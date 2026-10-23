//! What an operator can do from psql: see where a statement would go, take
//! a replica out of service, and see what is wrong.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::time::{Duration, Instant};

use luxbyte::{Config, Server, ServerHandle};
use luxbyte_pool::{Connection, ConnectionTarget, Credentials, QueryResult};
use luxbyte_proto::auth::ScramVerifier;
use luxbyte_testkit::{Backend, BackendConfig};

const READ: &str = "SELECT * FROM users WHERE tenant_id = 'alice'";

struct Fixture {
    primary: Backend,
    replica: Backend,
    proxy: ServerHandle,
}

fn auth() -> String {
    let ops = ScramVerifier::derive("ops-pw", b"salt-ops", 4096).unwrap();
    let app = ScramVerifier::derive("app-pw", b"salt-app", 4096).unwrap();
    format!(
        "[auth]\noperators = [\"ops\"]\npassthrough = false\n\n[[auth.users]]\nname = \"ops\"\nverifier = \"{}\"\n\n\
         [[auth.users]]\nname = \"app\"\nverifier = \"{}\"\n",
        ops.to_postgres_string(),
        app.to_postgres_string()
    )
}

async fn fixture(extra: &str) -> Fixture {
    let primary = Backend::start(BackendConfig::new("eu-primary"))
        .await
        .unwrap();
    let replica = Backend::start(BackendConfig::new("eu-replica"))
        .await
        .unwrap();
    replica.set_in_recovery(true);
    primary.set_position(5_000);
    replica.set_position(5_000);
    let config = Config::from_toml(&format!(
        r#"
[server]
listen = "127.0.0.1:0"
node = "test-proxy"

[backend]
user = "luxbyte"
database = "shop"

{}

[health]
interval_ms = 20
max_replica_lag_bytes = 1000

{extra}

[topology]
home_region = "eu-west"

[[topology.regions]]
id = "eu-west"
primary = {{ host = "127.0.0.1", port = {} }}
replicas = [{{ host = "127.0.0.1", port = {} }}]

[[topology.tables]]
table = "users"
placement = "geo_partitioned"
key = "tenant_id"

[[directory]]
table = "users"
key = "alice"
region = "eu-west"
"#,
        auth(),
        primary.port(),
        replica.port()
    ))
    .unwrap();
    let proxy = Server::bind(&config).await.unwrap().spawn().unwrap();
    Fixture {
        primary,
        replica,
        proxy,
    }
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
    .unwrap()
}

fn rows(result: &QueryResult) -> Vec<Vec<String>> {
    result
        .rows
        .iter()
        .map(|row| row.iter().map(|v| v.clone().unwrap_or_default()).collect())
        .collect()
}

#[tokio::test]
async fn route_says_where_a_statement_would_go_without_running_it() {
    let f = fixture("").await;
    let mut client = connect(&f.proxy, "app").await;
    let result = client
        .simple_query("ROUTE UPDATE users SET name = 'x' WHERE tenant_id = 'alice'")
        .await
        .unwrap();
    assert_eq!(result.columns, ["statement", "step", "detail"]);
    let rows = rows(&result);
    assert!(
        rows.contains(&vec![
            "1".to_owned(),
            "plan".to_owned(),
            "one region: eu-west (ReadWrite)".to_owned()
        ]),
        "{rows:?}"
    );
    assert!(!f.primary.saw("UPDATE"));
    assert!(!f.replica.saw("UPDATE"));
}

#[tokio::test]
async fn a_drained_replica_takes_no_reads_until_it_is_undrained() {
    let f = fixture("").await;
    let endpoint = format!("127.0.0.1:{}", f.replica.port());
    let mut app = connect(&f.proxy, "app").await;
    app.simple_query(READ).await.unwrap();
    assert_eq!(f.replica.count_of(READ), 1);

    let mut ops = connect(&f.proxy, "ops").await;
    ops.simple_query(&format!("DRAIN {endpoint}"))
        .await
        .unwrap();
    app.simple_query(READ).await.unwrap();
    assert_eq!(f.primary.count_of(READ), 1, "drained, so the primary reads");
    let health = rows(&app.simple_query("SHOW HEALTH").await.unwrap());
    assert!(
        health
            .iter()
            .any(|row| row[1] == endpoint && row[3] == "up, drained"),
        "{health:?}"
    );

    ops.simple_query(&format!("UNDRAIN {endpoint}"))
        .await
        .unwrap();
    app.simple_query(READ).await.unwrap();
    assert_eq!(f.replica.count_of(READ), 2);
}

#[tokio::test]
async fn only_operators_drain_and_a_primary_cannot_be_drained() {
    let f = fixture("").await;
    let mut app = connect(&f.proxy, "app").await;
    let error = app
        .simple_query(&format!("DRAIN 127.0.0.1:{}", f.replica.port()))
        .await
        .expect_err("not an operator");
    assert_eq!(error.sqlstate(), Some("42501"), "{error}");

    let mut ops = connect(&f.proxy, "ops").await;
    let error = ops
        .simple_query(&format!("DRAIN 127.0.0.1:{}", f.primary.port()))
        .await
        .expect_err("a primary");
    assert_eq!(error.sqlstate(), Some("22023"), "{error}");
    let error = ops
        .simple_query("DRAIN nowhere:1")
        .await
        .expect_err("unknown");
    assert_eq!(error.sqlstate(), Some("22023"), "{error}");
}

#[tokio::test]
async fn a_lagging_replica_raises_an_alert_until_it_catches_up() {
    let f = fixture("").await;
    let endpoint = format!("127.0.0.1:{}", f.replica.port());
    let mut client = connect(&f.proxy, "app").await;
    assert!(
        rows(&client.simple_query("SHOW ALERTS").await.unwrap()).is_empty(),
        "nothing is wrong yet"
    );

    f.replica.set_position(1_000);
    let started = Instant::now();
    loop {
        let alerts = rows(&client.simple_query("SHOW ALERTS").await.unwrap());
        if alerts
            .iter()
            .any(|row| row[0] == "replica_lagging" && row[1] == endpoint)
        {
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(5), "{alerts:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    f.replica.set_position(5_000);
    let started = Instant::now();
    while !rows(&client.simple_query("SHOW ALERTS").await.unwrap()).is_empty() {
        assert!(started.elapsed() < Duration::from_secs(5));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn connections_are_warmed_before_the_first_client() {
    let f = fixture("[pool]\nwarm = 3").await;
    assert_eq!(f.primary.connections_accepted(), 3);
    assert_eq!(f.replica.connections_accepted(), 3);
    let mut client = connect(&f.proxy, "app").await;
    client.simple_query(READ).await.unwrap();
    // The read used a warm connection rather than opening one.
    assert_eq!(f.replica.connections_accepted(), 3);
}
