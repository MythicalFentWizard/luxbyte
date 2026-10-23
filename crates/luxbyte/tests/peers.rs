//! Two proxies sharing one directory: a move made through one is held and
//! then followed by the other, a proxy that cannot confirm stops a move, a
//! proxy cut off from the others stops routing on its directory and catches
//! up when it gets back, and new keys are placed once.
//!
//! The shared state is kept in memory here, standing in for the tables in
//! the authority region; the regions themselves are wire-protocol test
//! backends, as in the other end-to-end tests.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use luxbyte::peers::MemoryPeers;
use luxbyte::{Config, Server, ServerHandle};
use luxbyte_core::directory::{Directory, KeyState, Location};
use luxbyte_pool::{Connection, ConnectionTarget, Credentials, QueryResult};
use luxbyte_proto::auth::ScramVerifier;
use luxbyte_testkit::{Backend, BackendConfig, Reply};

const WRITE: &str = "UPDATE users SET name = 'x' WHERE tenant_id = 'alice'";

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

fn config(node: &str, home: &str, eu: &Backend, us: &Backend, store: &str) -> Config {
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
authority = "eu-west"
poll_ms = 20
lease_ms = 200
{store}

[topology]
home_region = "{home}"

[[topology.regions]]
id = "eu-west"
primary = {{ host = "127.0.0.1", port = {eu} }}

[[topology.regions]]
id = "us-east"
primary = {{ host = "127.0.0.1", port = {us} }}

[[topology.tables]]
table = "orders"
placement = "geo_partitioned"
key = "tenant_id"

[[topology.tables]]
table = "users"
placement = "geo_partitioned"
key = "tenant_id"

[[topology.tables]]
table = "currencies"
placement = "global_replicated"
"#,
        auth = auth(),
        eu = eu.port(),
        us = us.port(),
    ))
    .expect("config should parse")
}

/// Europe, holding alice: two users rows and one order.
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
            "FROM \"public\".\"orders\" AS t",
            Reply::rows(&["json"], &[&[r#"[{"tenant_id":"alice","id":7}]"#]]),
        )
        .reply_to(
            "DELETE FROM \"public\".\"users\"",
            Reply::Done {
                tag: "DELETE 2".to_owned(),
            },
        )
        .reply_to(
            "DELETE FROM \"public\".\"orders\"",
            Reply::Done {
                tag: "DELETE 1".to_owned(),
            },
        )
}

/// The United States, where alice is going.
fn america() -> BackendConfig {
    america_taking("INSERT 0 2")
}

/// The United States, answering the users insert with `tag`.
fn america_taking(tag: &str) -> BackendConfig {
    BackendConfig::new("us-east")
        .reply_to(
            "\"public\".\"users\"'::regclass AND",
            Reply::rows(
                &["attname", "attidentity"],
                &[&["tenant_id", ""], &["name", ""]],
            ),
        )
        .reply_to(
            "\"public\".\"orders\"'::regclass AND",
            Reply::rows(
                &["attname", "attidentity"],
                &[&["tenant_id", ""], &["id", "a"]],
            ),
        )
        .reply_to(
            "INSERT INTO \"public\".\"users\"",
            Reply::Done {
                tag: tag.to_owned(),
            },
        )
        .reply_to(
            "INSERT INTO \"public\".\"orders\"",
            Reply::Done {
                tag: "INSERT 0 1".to_owned(),
            },
        )
}

struct Fixture {
    eu: Backend,
    us: Backend,
    memory: Arc<MemoryPeers>,
    /// Home in Europe.
    a: ServerHandle,
    /// Home in the United States.
    b: ServerHandle,
}

async fn fixture(eu: BackendConfig, us: BackendConfig, store: &str) -> Fixture {
    let eu = Backend::start(eu).await.unwrap();
    let us = Backend::start(us).await.unwrap();
    let memory = MemoryPeers::new();
    memory.place("users", "alice", "eu-west");
    memory.place("orders", "alice", "eu-west");
    let a = proxy("proxy-a", "eu-west", &eu, &us, store, &memory).await;
    let b = proxy("proxy-b", "us-east", &eu, &us, store, &memory).await;
    Fixture {
        eu,
        us,
        memory,
        a,
        b,
    }
}

async fn proxy(
    node: &str,
    home: &str,
    eu: &Backend,
    us: &Backend,
    store: &str,
    memory: &Arc<MemoryPeers>,
) -> ServerHandle {
    Server::bind_sharing(&config(node, home, eu, us, store), Arc::clone(memory))
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

fn location(proxy: &ServerHandle, table: &str, key: &str) -> Location {
    proxy.shared().directory.locate(table, key)
}

fn resident(region: &str) -> Location {
    Location::Known(KeyState::Resident {
        region: region.to_owned(),
    })
}

fn rows(result: &QueryResult) -> Vec<Vec<String>> {
    result
        .rows
        .iter()
        .map(|row| row.iter().map(|v| v.clone().unwrap_or_default()).collect())
        .collect()
}

async fn until(what: &str, mut condition: impl FnMut() -> bool) {
    let started = Instant::now();
    while !condition() {
        assert!(started.elapsed() < Duration::from_secs(5), "never {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn a_move_through_one_proxy_is_held_by_the_other_and_then_followed() {
    let f = fixture(
        europe().slow_on("LOCK TABLE", Duration::from_millis(300)),
        america(),
        "lock_timeout_ms = 2000\nhold_ms = 5000",
    )
    .await;
    let mut app = connect(&f.b, "app").await;
    app.simple_query(WRITE).await.unwrap();
    assert_eq!(f.eu.count_of(WRITE), 1, "alice starts in Europe");

    let mut ops = connect(&f.a, "ops").await;
    let relocating =
        tokio::spawn(async move { ops.simple_query("RELOCATE alice TO us-east").await });
    // The other proxy holds the key once the move is announced...
    until("held by proxy-b", || {
        matches!(
            location(&f.b, "users", "alice"),
            Location::Known(KeyState::Relocating { .. })
        )
    })
    .await;
    // ...so a write through it waits for the move, then goes where alice
    // went rather than where she was.
    app.simple_query(WRITE).await.expect("waits, then runs");
    assert_eq!(f.eu.count_of(WRITE), 1);
    assert_eq!(f.us.count_of(WRITE), 1);

    let moved = relocating.await.unwrap().expect("the move should succeed");
    assert_eq!(rows(&moved).len(), 2);
    for table in ["users", "orders"] {
        assert_eq!(location(&f.a, table, "alice"), resident("us-east"));
        assert_eq!(location(&f.b, table, "alice"), resident("us-east"));
        assert_eq!(
            f.memory.region_of(table, "alice").as_deref(),
            Some("us-east")
        );
    }
    until("withdrawn", || f.memory.announced() == 0).await;
}

#[tokio::test]
async fn a_proxy_that_does_not_confirm_stops_the_move() {
    // proxy-b goes quiet but has visited recently, so it may still be
    // routing; the move gives up rather than copy rows under it.
    let f = fixture(europe(), america(), "lock_timeout_ms = 100").await;
    f.memory.cut_off("proxy-b", true);
    let mut ops = connect(&f.a, "ops").await;
    let error = ops
        .simple_query("RELOCATE alice TO us-east")
        .await
        .expect_err("proxy-b never confirmed");
    assert_eq!(error.sqlstate(), Some("55P03"), "{error}");
    assert!(error.to_string().contains("proxy-b"), "{error}");

    assert!(!f.us.saw("INSERT INTO \"public\".\"users\""));
    assert!(f.eu.prepared_transactions().is_empty());
    assert_eq!(location(&f.a, "users", "alice"), resident("eu-west"));
    assert_eq!(
        f.memory.region_of("users", "alice").as_deref(),
        Some("eu-west")
    );
    until("withdrawn", || f.memory.announced() == 0).await;
}

#[tokio::test]
async fn a_failed_move_lets_the_other_proxy_go_where_it_was() {
    // The destination takes one row of two, so the move is refused after
    // both proxies held the key.
    let f = fixture(
        europe(),
        america_taking("INSERT 0 1"),
        "lock_timeout_ms = 2000",
    )
    .await;
    let mut ops = connect(&f.a, "ops").await;
    let error = ops
        .simple_query("RELOCATE alice TO us-east")
        .await
        .expect_err("refused");
    assert_eq!(error.sqlstate(), Some("XX000"), "{error}");

    until("released by proxy-b", || {
        location(&f.b, "users", "alice") == resident("eu-west")
    })
    .await;
    let mut app = connect(&f.b, "app").await;
    app.simple_query(WRITE).await.unwrap();
    assert_eq!(f.eu.count_of(WRITE), 1);
    assert_eq!(f.us.count_of(WRITE), 0);
}

#[tokio::test]
async fn a_proxy_cut_off_stops_routing_on_its_directory_and_catches_up_after() {
    let f = fixture(europe(), america(), "lock_timeout_ms = 2000\nhold_ms = 100").await;
    let mut app = connect(&f.b, "app").await;
    f.memory.cut_off("proxy-b", true);
    until("fenced", || {
        f.b.shared()
            .peers
            .as_ref()
            .is_some_and(luxbyte::peers::Peers::fenced)
    })
    .await;

    // What depends on the directory is refused rather than guessed...
    let error = app.simple_query(WRITE).await.expect_err("fenced");
    assert_eq!(error.sqlstate(), Some("57P03"), "{error}");
    assert!(error.to_string().contains("eu-west"), "{error}");
    // ...and what does not carries on.
    app.simple_query("SELECT * FROM currencies").await.unwrap();
    app.simple_query("SELECT 1").await.unwrap();

    // Meanwhile alice moves without it, once it has been quiet long enough
    // to have stopped by itself.
    let mut ops = connect(&f.a, "ops").await;
    ops.simple_query("RELOCATE alice TO us-east")
        .await
        .expect("moves without the fenced proxy");
    assert_eq!(
        location(&f.b, "users", "alice"),
        resident("eu-west"),
        "stale"
    );

    // Back in touch, it reads the directory whole before routing again.
    f.memory.cut_off("proxy-b", false);
    until("unfenced", || {
        f.b.shared()
            .peers
            .as_ref()
            .is_some_and(|peers| !peers.fenced())
    })
    .await;
    assert_eq!(location(&f.b, "users", "alice"), resident("us-east"));
    app.simple_query(WRITE).await.unwrap();
    assert_eq!(f.us.count_of(WRITE), 1);
    assert_eq!(f.eu.count_of(WRITE), 0);
}

#[tokio::test]
async fn a_new_key_is_placed_once_whichever_proxy_writes_it_first() {
    let f = fixture(europe(), america(), "").await;
    let insert = "INSERT INTO users (tenant_id, name) VALUES ('carol', 'Carol')";
    let update = "UPDATE users SET name = 'C' WHERE tenant_id = 'carol'";

    // proxy-b is home in the United States, and gets there first.
    let mut b = connect(&f.b, "app").await;
    b.simple_query(insert).await.unwrap();
    assert!(f.us.saw(insert));
    assert_eq!(
        f.memory.region_of("users", "carol").as_deref(),
        Some("us-east")
    );

    // proxy-a, home in Europe, has never seen carol, and writes where the
    // claim put her rather than at its own home.
    let mut a = connect(&f.a, "app").await;
    a.simple_query(update).await.unwrap();
    assert!(f.us.saw(update));
    assert!(!f.eu.saw("carol"));
    assert_eq!(location(&f.a, "users", "carol"), resident("us-east"));
}

#[tokio::test]
async fn two_processes_cannot_share_a_name() {
    let f = fixture(europe(), america(), "").await;
    let error = Server::bind_sharing(
        &config("proxy-a", "eu-west", &f.eu, &f.us, ""),
        Arc::clone(&f.memory),
    )
    .await
    .expect_err("proxy-a is alive");
    assert!(error.to_string().contains("proxy-a"), "{error}");
}

#[tokio::test]
async fn show_peers_lists_every_proxy() {
    let f = fixture(europe(), america(), "").await;
    let mut app = connect(&f.a, "app").await;
    until("proxy-b seen", || {
        f.a.shared()
            .peers
            .as_ref()
            .is_some_and(|peers| peers.peers().len() == 2)
    })
    .await;
    let result = app.simple_query("SHOW PEERS").await.unwrap();
    assert_eq!(result.columns, ["node", "role", "age_ms", "detail"]);
    let rows = rows(&result);
    assert!(
        rows.iter()
            .any(|row| row[0] == "proxy-a" && row[1] == "this proxy"),
        "{rows:?}"
    );
    assert!(
        rows.iter()
            .any(|row| row[0] == "proxy-b" && row[1] == "peer"),
        "{rows:?}"
    );
}
