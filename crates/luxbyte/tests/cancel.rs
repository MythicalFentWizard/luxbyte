//! Query cancellation, end to end.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::time::{Duration, Instant};

use luxbyte::{Config, Server, ServerHandle};
use luxbyte_pool::{CancelTarget, Connection, ConnectionTarget, Credentials, send_cancel};
use luxbyte_proto::startup::CancelRequest;
use luxbyte_testkit::{Backend, BackendConfig};

const SLOW: Duration = Duration::from_secs(3);

async fn proxy(eu: &Backend, us: &Backend) -> ServerHandle {
    let config = Config::from_toml(&format!(
        r#"
[server]
listen = "127.0.0.1:0"
node = "test-proxy"

[backend]
user = "app"
database = "shop"

[topology]
home_region = "eu-west"

[[topology.regions]]
id = "eu-west"
primary = {{ host = "127.0.0.1", port = {eu} }}

[[topology.regions]]
id = "us-east"
primary = {{ host = "127.0.0.1", port = {us} }}

[[topology.tables]]
table = "users"
placement = "geo_partitioned"
key = "tenant_id"

[[directory]]
table = "users"
key = "alice"
region = "eu-west"
"#,
        eu = eu.port(),
        us = us.port(),
    ))
    .expect("config");
    Server::bind(&config)
        .await
        .expect("bind")
        .spawn()
        .expect("serve")
}

async fn client(proxy: &ServerHandle) -> Connection {
    Connection::connect(
        &ConnectionTarget {
            host: "127.0.0.1".to_owned(),
            port: proxy.port(),
            database: "shop".to_owned(),
            user: "app".to_owned(),
        },
        &Credentials::Trust,
        Duration::from_secs(5),
    )
    .await
    .expect("connect")
}

/// Sends a cancel to the proxy, the way libpq does: a new connection
/// carrying only the key the proxy handed out at startup.
async fn cancel(proxy: &ServerHandle, key: (i32, i32)) {
    send_cancel(
        &CancelTarget {
            host: "127.0.0.1".to_owned(),
            port: proxy.port(),
            key: CancelRequest {
                process_id: key.0,
                secret_key: key.1,
            },
        },
        Duration::from_secs(5),
    )
    .await
    .expect("deliver the cancel");
}

/// Two regions where reading or writing users is slow, and nothing else
/// is, so a cancel lands on the statement under test.
async fn slow_pair() -> (Backend, Backend) {
    (
        Backend::start(BackendConfig::new("eu-west").slow_on("users", SLOW))
            .await
            .unwrap(),
        Backend::start(BackendConfig::new("us-east").slow_on("users", SLOW))
            .await
            .unwrap(),
    )
}

#[tokio::test]
async fn the_proxy_hands_out_a_key_of_its_own() {
    let (eu, us) = slow_pair().await;
    let proxy = proxy(&eu, &us).await;
    let first = client(&proxy).await;
    let second = client(&proxy).await;
    assert_ne!(first.backend_key(), (0, 0));
    assert_ne!(first.backend_key(), second.backend_key());
}

#[tokio::test]
async fn one_cancel_stops_a_read_in_every_region_it_fanned_out_to() {
    let (eu, us) = slow_pair().await;
    let proxy = proxy(&eu, &us).await;
    let mut client = client(&proxy).await;
    let key = client.backend_key();

    let started = Instant::now();
    let query = tokio::spawn(async move {
        // No key, so it reads both regions, each of which takes SLOW.
        let outcome = client.simple_query("SELECT id FROM users").await;
        (client, outcome)
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    cancel(&proxy, key).await;

    let (mut client, outcome) = query.await.expect("task");
    let error = outcome.expect_err("the read was cancelled");
    assert_eq!(error.sqlstate(), Some("57014"), "{error}");
    assert!(started.elapsed() < SLOW, "the cancel did not cut it short");
    assert_eq!(eu.cancels(), 1);
    assert_eq!(us.cancels(), 1);

    // The cancelled connections were retired rather than pooled, so a late
    // cancel could never reach a query that is not this client's. eu-west
    // has now seen the read's connection, the cancel's, and a replacement.
    let _ = client.simple_query("SELECT 1").await;
    assert_eq!(eu.connections_accepted(), 3);
}

#[tokio::test]
async fn a_cancel_reaches_a_statement_inside_a_transaction() {
    let (eu, us) = slow_pair().await;
    let proxy = proxy(&eu, &us).await;
    let mut client = client(&proxy).await;
    let key = client.backend_key();

    client.simple_query("BEGIN").await.expect("begin");
    let query = tokio::spawn(async move {
        let outcome = client
            .simple_query("UPDATE users SET name = 'x' WHERE tenant_id = 'alice'")
            .await;
        (client, outcome)
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    cancel(&proxy, key).await;

    let (mut client, outcome) = query.await.expect("task");
    assert_eq!(outcome.expect_err("cancelled").sqlstate(), Some("57014"));
    assert_eq!(eu.cancels(), 1);
    assert_eq!(us.cancels(), 0);
    let rollback = client.simple_query("ROLLBACK").await;
    assert!(rollback.is_ok());
}

#[tokio::test]
async fn a_key_the_proxy_never_issued_cancels_nothing() {
    let (eu, us) = slow_pair().await;
    let proxy = proxy(&eu, &us).await;
    let mut client = client(&proxy).await;
    let (process, secret) = client.backend_key();

    let query = tokio::spawn(async move { client.simple_query("SELECT id FROM users").await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    cancel(&proxy, (process, secret.wrapping_add(1))).await;

    assert!(
        query.await.expect("task").is_ok(),
        "a wrong secret must not cancel"
    );
    assert_eq!(eu.cancels(), 0);
    assert_eq!(us.cancels(), 0);
}
