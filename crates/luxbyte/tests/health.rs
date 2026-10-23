//! Probing backends: replica lag, read-your-writes, and reads that survive
//! a replica going away.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::time::{Duration, Instant};

use luxbyte::{Config, Server, ServerHandle};
use luxbyte_pool::{Connection, ConnectionTarget, Credentials, QueryResult};
use luxbyte_testkit::{Backend, BackendConfig};

const READ: &str = "SELECT * FROM users WHERE tenant_id = 'alice'";
const WRITE: &str = "UPDATE users SET name = 'x' WHERE tenant_id = 'alice'";

struct Fixture {
    primary: Backend,
    replica: Backend,
    proxy: ServerHandle,
}

async fn fixture(health: &str) -> Fixture {
    let primary = Backend::start(BackendConfig::new("eu-primary"))
        .await
        .unwrap();
    let replica = Backend::start(BackendConfig::new("eu-replica"))
        .await
        .unwrap();
    let us = Backend::start(BackendConfig::new("us-east")).await.unwrap();
    replica.set_in_recovery(true);
    primary.set_position(5_000);
    replica.set_position(4_500);
    let config = Config::from_toml(&format!(
        r#"
[server]
listen = "127.0.0.1:0"
node = "test-proxy"

[backend]
user = "app"
database = "shop"

[health]
{health}

[topology]
home_region = "eu-west"

[[topology.regions]]
id = "eu-west"
primary = {{ host = "127.0.0.1", port = {} }}
replicas = [{{ host = "127.0.0.1", port = {} }}]

[[topology.regions]]
id = "us-east"
primary = {{ host = "127.0.0.1", port = {} }}

[[topology.tables]]
table = "users"
placement = "geo_partitioned"
key = "tenant_id"

[[directory]]
table = "users"
key = "alice"
region = "eu-west"
"#,
        primary.port(),
        replica.port(),
        us.port()
    ))
    .unwrap();
    let proxy = Server::bind(&config).await.unwrap().spawn().unwrap();
    Fixture {
        primary,
        replica,
        proxy,
    }
}

const FAST: &str = "interval_ms = 20\ntimeout_ms = 500\nmax_replica_lag_bytes = 1000";

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
    .unwrap()
}

/// Waits for the prober to report a replica position.
async fn probed(f: &Fixture, position: u64) {
    let started = Instant::now();
    let endpoint = format!("127.0.0.1:{}", f.replica.port());
    loop {
        let seen = f
            .proxy
            .shared()
            .health
            .snapshot()
            .into_iter()
            .find(|status| status.endpoint == endpoint)
            .and_then(|status| status.position)
            .map(|(position, _)| position);
        if seen == Some(position) {
            // And one more round, so the primary's position is fresh too.
            let probes = f.primary.probes();
            while f.primary.probes() <= probes {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "never probed {position}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn reads_of(backend: &Backend) -> usize {
    backend.count_of(READ)
}

#[tokio::test]
async fn reads_go_to_a_replica_close_enough_to_its_primary() {
    let f = fixture(FAST).await;
    let mut client = client(&f.proxy).await;
    client.simple_query(READ).await.unwrap();
    assert_eq!(reads_of(&f.replica), 1);
    assert_eq!(reads_of(&f.primary), 0);
}

#[tokio::test]
async fn a_replica_too_far_behind_serves_no_reads_until_it_catches_up() {
    let f = fixture(FAST).await;
    f.replica.set_position(1_000);
    probed(&f, 1_000).await;
    let mut client = client(&f.proxy).await;
    client.simple_query(READ).await.unwrap();
    assert_eq!(reads_of(&f.primary), 1, "4000 bytes behind is too far");
    assert!(f.proxy.shared().health.kept_off_replicas() >= 1);

    f.replica.set_position(4_900);
    probed(&f, 4_900).await;
    client.simple_query(READ).await.unwrap();
    assert_eq!(reads_of(&f.replica), 1);
}

#[tokio::test]
async fn a_session_reads_its_own_writes_and_others_carry_on_with_the_replica() {
    let f = fixture(FAST).await;
    let mut writer = client(&f.proxy).await;
    let mut other = client(&f.proxy).await;
    f.replica.set_position(5_000);
    probed(&f, 5_000).await;

    writer.simple_query(WRITE).await.unwrap();
    // The write's WAL is past anything the replica has replayed.
    f.primary.set_position(6_000);
    writer.simple_query(READ).await.unwrap();
    assert_eq!(reads_of(&f.primary), 1, "read straight after the write");

    // Once a probe after the write has seen the primary at 6000, a replica
    // short of that still cannot serve this session...
    probed(&f, 5_000).await;
    writer.simple_query(READ).await.unwrap();
    assert_eq!(reads_of(&f.primary), 2);
    // ...while a session that wrote nothing reads from it as before.
    other.simple_query(READ).await.unwrap();
    assert_eq!(reads_of(&f.replica), 1);

    // When the replica has replayed past the write, the writer reads from
    // it too.
    f.replica.set_position(6_000);
    probed(&f, 6_000).await;
    writer.simple_query(READ).await.unwrap();
    assert_eq!(reads_of(&f.replica), 2);
}

#[tokio::test]
async fn a_write_in_a_transaction_counts_once_it_commits() {
    let f = fixture(FAST).await;
    f.replica.set_position(5_000);
    probed(&f, 5_000).await;
    let mut client = client(&f.proxy).await;
    client.simple_query("BEGIN").await.unwrap();
    client.simple_query(WRITE).await.unwrap();
    client.simple_query("COMMIT").await.unwrap();
    f.primary.set_position(6_000);
    client.simple_query(READ).await.unwrap();
    assert_eq!(reads_of(&f.primary), 1);
    assert_eq!(reads_of(&f.replica), 0);
}

#[tokio::test]
async fn a_replica_that_cannot_be_reached_hands_its_reads_to_the_primary() {
    // Probes are far apart, so the replica still looks up when the read
    // arrives and the read itself finds out.
    let f = fixture("interval_ms = 60000\nmax_replica_lag_bytes = 1000").await;
    f.replica.stop();
    f.replica.close_connections();
    let mut client = client(&f.proxy).await;
    client
        .simple_query(READ)
        .await
        .expect("the read survives the replica");
    assert_eq!(reads_of(&f.primary), 1);
    assert_eq!(f.proxy.shared().health.failovers(), 1);
    // Marked down at once, so the next read does not try it.
    client.simple_query(READ).await.unwrap();
    assert_eq!(reads_of(&f.primary), 2);
    assert_eq!(f.proxy.shared().health.failovers(), 1);
}

#[tokio::test]
async fn failed_probes_mark_a_replica_down() {
    let f = fixture(FAST).await;
    f.replica.stop();
    f.replica.close_connections();
    let endpoint = format!("127.0.0.1:{}", f.replica.port());
    let started = Instant::now();
    while !f
        .proxy
        .shared()
        .health
        .snapshot()
        .iter()
        .any(|status| status.endpoint == endpoint && status.state.name() == "down")
    {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "never marked down"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut client = client(&f.proxy).await;
    client.simple_query(READ).await.unwrap();
    assert_eq!(reads_of(&f.primary), 1);
    assert_eq!(f.proxy.shared().health.failovers(), 0);
}

fn rows(result: &QueryResult) -> Vec<Vec<String>> {
    result
        .rows
        .iter()
        .map(|row| row.iter().map(|v| v.clone().unwrap_or_default()).collect())
        .collect()
}

#[tokio::test]
async fn show_health_says_what_each_backend_is_doing() {
    let f = fixture(FAST).await;
    f.primary.set_in_recovery(true);
    let started = Instant::now();
    loop {
        let mut client = client(&f.proxy).await;
        let result = client.simple_query("SHOW HEALTH").await.unwrap();
        assert_eq!(
            result.columns,
            [
                "region",
                "endpoint",
                "role",
                "state",
                "lag_bytes",
                "probe_ms",
                "last_error"
            ]
        );
        let rows = rows(&result);
        let primary = rows
            .iter()
            .find(|row| row[1] == format!("127.0.0.1:{}", f.primary.port()))
            .unwrap();
        let replica = rows
            .iter()
            .find(|row| row[1] == format!("127.0.0.1:{}", f.replica.port()))
            .unwrap();
        assert_eq!(replica[2], "replica");
        assert_eq!(replica[3], "up");
        if primary[2] == "primary, but in recovery" {
            assert_eq!(replica[4], "500");
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(5), "{rows:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn with_probing_off_replicas_are_used_as_configured_and_never_probed() {
    let f = fixture("interval_ms = 0").await;
    let mut client = client(&f.proxy).await;
    client.simple_query(READ).await.unwrap();
    assert_eq!(reads_of(&f.replica), 1);
    assert_eq!(f.replica.probes(), 0);
    assert_eq!(f.primary.probes(), 0);
}

#[tokio::test]
async fn probe_connections_stay_out_of_the_way() {
    let f = fixture(FAST).await;
    let started = Instant::now();
    while f.primary.probes() < 3 {
        assert!(started.elapsed() < Duration::from_secs(5));
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // One connection each, kept between probes, never in the pools.
    assert_eq!(f.primary.connections_accepted(), 0);
    assert!(f.primary.statements().is_empty());
}
