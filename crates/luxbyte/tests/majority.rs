//! Proxies that agree across a majority of three regions rather than in
//! one authority region: losing any one region's copy of the shared state
//! stops nothing, a proxy that can reach only a minority fences itself
//! without holding up anyone else, two proxies claiming one key agree, and
//! a move goes on without a region that holds none of its keys.
//!
//! The three copies of the shared state are kept in memory here, one per
//! region; the regions themselves are wire-protocol test backends.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use luxbyte::peers::{MemoryPeers, Peers};
use luxbyte::{Config, Server, ServerHandle};
use luxbyte_core::directory::{Directory, KeyState, Location};
use luxbyte_pool::{Connection, ConnectionTarget, Credentials};
use luxbyte_proto::auth::ScramVerifier;
use luxbyte_testkit::{Backend, BackendConfig, Reply};

const WRITE: &str = "UPDATE users SET name = 'x' WHERE tenant_id = 'alice'";
const LEASE: Duration = Duration::from_millis(200);

fn auth() -> String {
    let ops = ScramVerifier::derive("ops-pw", b"salt-ops", 4096).unwrap();
    let app = ScramVerifier::derive("app-pw", b"salt-app", 4096).unwrap();
    format!(
        "[auth]\noperators = [\"ops\"]\n\n[[auth.users]]\nname = \"ops\"\nverifier = \"{}\"\n\n\
         [[auth.users]]\nname = \"app\"\nverifier = \"{}\"\n",
        ops.to_postgres_string(),
        app.to_postgres_string()
    )
}

fn config(node: &str, home: &str, ports: [u16; 3]) -> Config {
    let [eu, us, ap] = ports;
    Config::from_toml(&format!(
        r#"
[server]
listen = "127.0.0.1:0"
node = "{node}"

[backend]
user = "luxbyte"
database = "shop"

{auth}

[directory_store]
shared = true
authority = "majority"
poll_ms = 20
lease_ms = {lease}
lock_timeout_ms = 2000
hold_ms = 100

[topology]
home_region = "{home}"

[[topology.regions]]
id = "eu-west"
primary = {{ host = "127.0.0.1", port = {eu} }}

[[topology.regions]]
id = "us-east"
primary = {{ host = "127.0.0.1", port = {us} }}

[[topology.regions]]
id = "ap-south"
primary = {{ host = "127.0.0.1", port = {ap} }}

[[topology.tables]]
table = "users"
placement = "geo_partitioned"
key = "tenant_id"
"#,
        auth = auth(),
        lease = LEASE.as_millis(),
    ))
    .expect("config should parse")
}

/// Europe, holding alice's two users rows.
fn europe() -> BackendConfig {
    BackendConfig::new("eu-west")
        .reply_to(
            "FROM \"public\".\"users\" AS t",
            Reply::rows(
                &["json"],
                &[&[r#"[{"tenant_id":"alice","name":"Alice"},{"tenant_id":"alice","name":"Al"}]"#]],
            ),
        )
        .reply_to(
            "DELETE FROM \"public\".\"users\"",
            Reply::Done {
                tag: "DELETE 2".to_owned(),
            },
        )
}

/// The United States, where alice is going.
fn america() -> BackendConfig {
    BackendConfig::new("us-east")
        .reply_to(
            "\"public\".\"users\"'::regclass AND",
            Reply::rows(
                &["attname", "attidentity"],
                &[&["tenant_id", ""], &["name", ""]],
            ),
        )
        .reply_to(
            "INSERT INTO \"public\".\"users\"",
            Reply::Done {
                tag: "INSERT 0 2".to_owned(),
            },
        )
}

struct Fixture {
    eu: Backend,
    us: Backend,
    /// Absent when the test wants Asia's database unreachable.
    _ap: Option<Backend>,
    memory: Arc<MemoryPeers>,
    /// Home in Europe.
    a: ServerHandle,
    /// Home in the United States.
    b: ServerHandle,
}

async fn fixture(asia_up: bool) -> Fixture {
    let eu = Backend::start(europe()).await.unwrap();
    let us = Backend::start(america()).await.unwrap();
    let (ap, ap_port) = if asia_up {
        let ap = Backend::start(BackendConfig::new("ap-south"))
            .await
            .unwrap();
        let port = ap.port();
        (Some(ap), port)
    } else {
        (None, closed_port())
    };
    let ports = [eu.port(), us.port(), ap_port];
    let memory = MemoryPeers::with_regions(3);
    memory.place("users", "alice", "eu-west");
    let a = proxy("proxy-a", "eu-west", ports, &memory).await;
    let b = proxy("proxy-b", "us-east", ports, &memory).await;
    Fixture {
        eu,
        us,
        _ap: ap,
        memory,
        a,
        b,
    }
}

/// A port nothing listens on.
fn closed_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn proxy(node: &str, home: &str, ports: [u16; 3], memory: &Arc<MemoryPeers>) -> ServerHandle {
    Server::bind_sharing(&config(node, home, ports), Arc::clone(memory))
        .await
        .expect("should bind")
        .spawn()
        .expect("should serve")
}

async fn connect(proxy: &ServerHandle, user: &str) -> Connection {
    Connection::connect(
        &ConnectionTarget {
            host: "127.0.0.1".to_owned(),
            port: proxy.port(),
            database: "shop".to_owned(),
            user: user.to_owned(),
        },
        &Credentials::Password(format!("{user}-pw")),
        Duration::from_secs(5),
    )
    .await
    .expect("should log in")
}

fn location(proxy: &ServerHandle, key: &str) -> Location {
    proxy.shared().directory.locate("users", key)
}

fn resident(region: &str) -> Location {
    Location::Known(KeyState::Resident {
        region: region.to_owned(),
    })
}

fn fenced(proxy: &ServerHandle) -> bool {
    proxy.shared().peers.as_ref().is_some_and(Peers::fenced)
}

async fn until(what: &str, mut condition: impl FnMut() -> bool) {
    let started = Instant::now();
    while !condition() {
        assert!(started.elapsed() < Duration::from_secs(5), "never {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn losing_one_region_of_three_stops_nothing() {
    let f = fixture(true).await;
    // Asia's copy of the shared state is gone, for both proxies.
    for node in ["proxy-a", "proxy-b"] {
        f.memory.cut_off_region(node, 2, true);
    }
    // Several leases later both still route on their directories.
    tokio::time::sleep(LEASE.saturating_mul(3)).await;
    assert!(!fenced(&f.a) && !fenced(&f.b));
    let mut app = connect(&f.b, "app").await;
    app.simple_query(WRITE).await.unwrap();
    assert_eq!(f.eu.count_of(WRITE), 1);

    // A move through one is followed by the other.
    let mut ops = connect(&f.a, "ops").await;
    ops.simple_query("RELOCATE alice TO us-east")
        .await
        .expect("the move should succeed");
    until("followed by proxy-b", || {
        location(&f.b, "alice") == resident("us-east")
    })
    .await;
    app.simple_query(WRITE).await.unwrap();
    assert_eq!(f.us.count_of(WRITE), 1);

    // And a new key is claimed, once.
    let insert = "INSERT INTO users (tenant_id, name) VALUES ('carol', 'Carol')";
    app.simple_query(insert).await.unwrap();
    assert!(f.us.saw(insert), "proxy-b's home");
    let mut other = connect(&f.a, "app").await;
    let update = "UPDATE users SET name = 'C' WHERE tenant_id = 'carol'";
    other.simple_query(update).await.unwrap();
    assert!(f.us.saw(update), "proxy-a writes where the claim put carol");
    assert!(!f.eu.saw("carol"));
}

#[tokio::test]
async fn a_proxy_that_reaches_only_a_minority_fences_itself_and_holds_nobody_up() {
    let f = fixture(true).await;
    let mut app = connect(&f.b, "app").await;
    // proxy-b can still reach Europe's copy, and keeps leaving heartbeats
    // there, but not the other two.
    for index in [1, 2] {
        f.memory.cut_off_region("proxy-b", index, true);
    }
    until("proxy-b fenced", || fenced(&f.b)).await;
    let error = app.simple_query(WRITE).await.expect_err("fenced");
    assert_eq!(error.sqlstate(), Some("57P03"), "{error}");
    assert!(error.to_string().contains("majority"), "{error}");

    // Its heartbeats stopped granting when its visits stopped reaching a
    // majority, so a move waits only for its last grant to age out rather
    // than for ever.
    let started = Instant::now();
    let mut ops = connect(&f.a, "ops").await;
    ops.simple_query("RELOCATE alice TO us-east")
        .await
        .expect("moves without the fenced proxy");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(!fenced(&f.a));
    assert_eq!(location(&f.b, "alice"), resident("eu-west"), "stale");

    // Back in touch, it reads the directory whole before routing again.
    for index in [1, 2] {
        f.memory.cut_off_region("proxy-b", index, false);
    }
    until("proxy-b unfenced", || !fenced(&f.b)).await;
    assert_eq!(location(&f.b, "alice"), resident("us-east"));
    app.simple_query(WRITE).await.unwrap();
    assert_eq!(f.us.count_of(WRITE), 1);
    assert_eq!(f.eu.count_of(WRITE), 0);
}

#[tokio::test]
async fn two_proxies_claiming_one_key_at_once_agree_where_it_goes() {
    let f = fixture(true).await;
    for round in 0..10 {
        let key = format!("new-{round}");
        let insert =
            |name: &str| format!("INSERT INTO users (tenant_id, name) VALUES ('{key}', '{name}')");
        let (from_a, from_b) = (insert("A"), insert("B"));
        let mut a = connect(&f.a, "app").await;
        let mut b = connect(&f.b, "app").await;
        let (first, second) = tokio::join!(a.simple_query(&from_a), b.simple_query(&from_b));
        first.unwrap();
        second.unwrap();

        // The decision reaches each region's directory with the next
        // heartbeat rather than holding up the insert.
        until("recorded", || f.memory.region_of("users", &key).is_some()).await;
        let placed = f.memory.region_of("users", &key).unwrap();
        assert_eq!(location(&f.a, &key), resident(&placed), "{key}");
        assert_eq!(location(&f.b, &key), resident(&placed), "{key}");
        // Both inserts went to the one region the claim chose.
        let landed = if placed == "eu-west" { &f.eu } else { &f.us };
        assert!(landed.saw(&from_a) && landed.saw(&from_b), "{key}");
    }
}

#[tokio::test]
async fn losing_a_majority_fences_every_proxy() {
    let f = fixture(true).await;
    for node in ["proxy-a", "proxy-b"] {
        for index in [1, 2] {
            f.memory.cut_off_region(node, index, true);
        }
    }
    until("both fenced", || fenced(&f.a) && fenced(&f.b)).await;
    let mut app = connect(&f.a, "app").await;
    let error = app.simple_query(WRITE).await.expect_err("fenced");
    assert_eq!(error.sqlstate(), Some("57P03"), "{error}");
    // What does not depend on the directory carries on.
    app.simple_query("SELECT 1").await.unwrap();

    for node in ["proxy-a", "proxy-b"] {
        for index in [1, 2] {
            f.memory.cut_off_region(node, index, false);
        }
    }
    until("both unfenced", || !fenced(&f.a) && !fenced(&f.b)).await;
    app.simple_query(WRITE).await.unwrap();
}

#[tokio::test]
async fn a_move_goes_on_without_a_region_that_holds_none_of_its_keys() {
    // Asia's database cannot be reached at all; alice moves between the
    // other two, and the regions that took part outvote the one left out.
    let f = fixture(false).await;
    let mut ops = connect(&f.a, "ops").await;
    ops.simple_query("RELOCATE alice TO us-east")
        .await
        .expect("moves without Asia");
    assert!(f.us.saw("INSERT INTO \"public\".\"users\""));
    until("followed by proxy-b", || {
        location(&f.b, "alice") == resident("us-east")
    })
    .await;
    let mut app = connect(&f.b, "app").await;
    app.simple_query(WRITE).await.unwrap();
    assert_eq!(f.us.count_of(WRITE), 1);
    until("withdrawn", || f.memory.announced() == 0).await;
}

#[tokio::test]
async fn an_entry_only_one_region_holds_is_copied_to_the_others_at_start() {
    // Claimed while the proxies met in Europe alone: only Europe's table
    // has erin. Were Europe lost, a majority would not know her at all.
    let eu = Backend::start(europe()).await.unwrap();
    let us = Backend::start(america()).await.unwrap();
    let ap = Backend::start(BackendConfig::new("ap-south"))
        .await
        .unwrap();
    let memory = MemoryPeers::with_regions(3);
    memory.place_in(0, "users", "erin", "eu-west");
    let _a = proxy(
        "proxy-a",
        "eu-west",
        [eu.port(), us.port(), ap.port()],
        &memory,
    )
    .await;
    for index in 0..3 {
        assert_eq!(
            memory.region_in(index, "users", "erin").as_deref(),
            Some("eu-west"),
            "copy {index}"
        );
    }
}

#[tokio::test]
async fn a_region_that_comes_back_is_brought_up_to_date() {
    let f = fixture(true).await;
    // Asia's copy misses a move while it is unreachable...
    for node in ["proxy-a", "proxy-b"] {
        f.memory.cut_off_region(node, 2, true);
    }
    let mut ops = connect(&f.a, "ops").await;
    ops.simple_query("RELOCATE alice TO us-east").await.unwrap();
    // (The in-memory move records itself in every copy; put Asia's back
    // to what it held before, as a region the move left out would, at an
    // older version than the others.)
    f.memory.place_in(2, "users", "alice", "eu-west");
    f.memory.place_in(0, "users", "alice", "us-east");
    f.memory.place_in(1, "users", "alice", "us-east");
    assert_eq!(
        f.memory.region_in(2, "users", "alice").as_deref(),
        Some("eu-west")
    );

    // ...and is brought up to date once it answers again.
    for node in ["proxy-a", "proxy-b"] {
        f.memory.cut_off_region(node, 2, false);
    }
    until("repaired", || {
        f.memory.region_in(2, "users", "alice").as_deref() == Some("us-east")
    })
    .await;
}
