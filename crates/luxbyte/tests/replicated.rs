//! Replicated tables written in one region, as PostgreSQL's replication
//! then carries them, and a session that never misses its own write.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::time::Duration;

use luxbyte::{Config, Server, ServerHandle};
use luxbyte_pool::{Connection, ConnectionTarget, Credentials};
use luxbyte_testkit::{Backend, BackendConfig};

struct Fixture {
    eu: Backend,
    us: Backend,
    proxy: ServerHandle,
}

async fn fixture() -> Fixture {
    let eu = Backend::start(BackendConfig::new("eu-west")).await.unwrap();
    let us = Backend::start(BackendConfig::new("us-east")).await.unwrap();
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
primary = {{ host = "127.0.0.1", port = {} }}

[[topology.regions]]
id = "us-east"
primary = {{ host = "127.0.0.1", port = {} }}

[[topology.tables]]
table = "users"
placement = "geo_partitioned"
key = "tenant_id"

[[topology.tables]]
table = "rates"
placement = "global_replicated"
writer = "us-east"

[[topology.tables]]
table = "currencies"
placement = "global_replicated"

[[directory]]
table = "users"
key = "alice"
region = "eu-west"
"#,
        eu.port(),
        us.port()
    ))
    .unwrap();
    let proxy = Server::bind(&config).await.unwrap().spawn().unwrap();
    Fixture { eu, us, proxy }
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
    .unwrap()
}

const WRITE: &str = "UPDATE rates SET value = 2 WHERE code = 'EUR'";
const READ: &str = "SELECT value FROM rates WHERE code = 'EUR'";

#[tokio::test]
async fn a_write_goes_to_the_writer_alone_with_no_two_phase_commit() {
    let f = fixture().await;
    let mut client = client(&f.proxy).await;
    client.simple_query(WRITE).await.unwrap();
    assert!(f.us.saw(WRITE));
    assert!(!f.eu.saw(WRITE));
    assert!(f.us.prepared_transactions().is_empty());
}

#[tokio::test]
async fn a_session_that_wrote_reads_from_the_writer_and_others_read_nearby() {
    let f = fixture().await;
    let mut writer = client(&f.proxy).await;
    let mut other = client(&f.proxy).await;

    writer.simple_query(READ).await.unwrap();
    assert_eq!(f.eu.count_of(READ), 1, "before writing, the nearest copy");

    writer.simple_query(WRITE).await.unwrap();
    writer.simple_query(READ).await.unwrap();
    assert_eq!(f.us.count_of(READ), 1, "after writing, the writer's copy");

    other.simple_query(READ).await.unwrap();
    assert_eq!(f.eu.count_of(READ), 2, "a session that did not write");

    // Tables without a writer change everywhere at once, so it reads them
    // nearby as before.
    writer
        .simple_query("SELECT * FROM currencies")
        .await
        .unwrap();
    assert!(f.eu.saw("SELECT * FROM currencies"));
    assert!(!f.us.saw("SELECT * FROM currencies"));

    // A join with rows that live in Europe has to run there.
    let join = "SELECT * FROM users u JOIN rates r ON r.code = u.currency \
                WHERE u.tenant_id = 'alice'";
    writer.simple_query(join).await.unwrap();
    assert!(f.eu.saw(join));
}
