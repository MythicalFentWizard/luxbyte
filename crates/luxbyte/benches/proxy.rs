//! What the proxy costs a client, end to end.
//!
//! Each statement is sent twice: straight to a backend, and through a proxy
//! in front of the same backend. Both run in this process over loopback
//! TCP, and the backend is the testkit's, not PostgreSQL, so the difference
//! between the two is the proxy's own work — parsing, routing, relaying —
//! and nothing else. What a real PostgreSQL adds is the same with or
//! without the proxy, so it is left out rather than guessed at.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::missing_panics_doc)]
// `criterion_main!` generates an undocumented `main`.
#![allow(missing_docs)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use luxbyte::{Config, Server, ServerHandle};
use luxbyte_pool::{Connection, ConnectionTarget, Credentials, Step, StepKind};
use luxbyte_proto::frontend;
use luxbyte_testkit::{Backend, BackendConfig, Reply};
use tokio::sync::Mutex;

const POINT_READ: &str = "SELECT id, name FROM users WHERE tenant_id = 'alice'";
const PREPARED_READ: &str = "SELECT id, name FROM users WHERE tenant_id = $1";
const MANY_ROWS: &str = "SELECT * FROM currencies";
const ROWS: usize = 10_000;

struct Setup {
    _backends: Vec<Backend>,
    _proxy: ServerHandle,
    direct: Arc<Mutex<Connection>>,
    proxied: Arc<Mutex<Connection>>,
}

fn backend_config(name: &str) -> BackendConfig {
    let rows: Vec<Vec<Option<String>>> = (0..ROWS)
        .map(|n| {
            vec![
                Some(format!("CUR{n:05}")),
                Some("A currency with a reasonably long name".to_owned()),
            ]
        })
        .collect();
    BackendConfig::new(name)
        .reply_to(
            "FROM users",
            Reply::rows(&["id", "name"], &[&["1", "Alice"]]),
        )
        .reply_to(
            "FROM currencies",
            Reply::Rows {
                columns: vec!["code".to_owned(), "name".to_owned()],
                rows,
            },
        )
}

async fn connect(port: u16) -> Connection {
    Connection::connect(
        &ConnectionTarget {
            host: "127.0.0.1".to_owned(),
            port,
            database: "shop".to_owned(),
            user: "app".to_owned(),
        },
        &Credentials::Trust,
        Duration::from_secs(5),
    )
    .await
    .unwrap()
}

async fn setup() -> Setup {
    let eu = Backend::start(backend_config("eu-west")).await.unwrap();
    let us = Backend::start(backend_config("us-east")).await.unwrap();
    let ap = Backend::start(backend_config("ap-south")).await.unwrap();
    let config = Config::from_toml(&format!(
        r#"
[server]
listen = "127.0.0.1:0"

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

[[topology.regions]]
id = "ap-south"
primary = {{ host = "127.0.0.1", port = {} }}

[[topology.tables]]
table = "users"
placement = "geo_partitioned"
key = "tenant_id"

[[topology.tables]]
table = "currencies"
placement = "global_replicated"

[[directory]]
table = "users"
key = "alice"
region = "eu-west"
"#,
        eu.port(),
        us.port(),
        ap.port()
    ))
    .unwrap();
    let proxy = Server::bind(&config).await.unwrap().spawn().unwrap();
    let direct = connect(eu.port()).await;
    let proxied = connect(proxy.port()).await;
    Setup {
        _backends: vec![eu, us, ap],
        _proxy: proxy,
        direct: Arc::new(Mutex::new(direct)),
        proxied: Arc::new(Mutex::new(proxied)),
    }
}

fn prepared_steps() -> Vec<Step> {
    vec![
        Step::new(
            StepKind::Parse,
            frontend::parse("", PREPARED_READ, &[]).unwrap(),
        ),
        Step::new(
            StepKind::Bind,
            frontend::bind("", "", &[], &[Some(b"alice".as_slice())], &[]).unwrap(),
        ),
        Step::new(StepKind::Execute, frontend::execute("", 0).unwrap()),
        Step::new(StepKind::Sync, frontend::sync().unwrap()),
    ]
}

fn end_to_end(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let setup = runtime.block_on(setup());
    let paths = [("direct", &setup.direct), ("proxy", &setup.proxied)];

    let mut group = c.benchmark_group("point_read");
    for (path, connection) in paths {
        group.bench_function(BenchmarkId::from_parameter(path), |b| {
            b.to_async(&runtime).iter(|| async {
                let result = connection.lock().await.simple_query(POINT_READ).await;
                black_box(result.unwrap());
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("prepared_read");
    let steps = prepared_steps();
    for (path, connection) in paths {
        group.bench_function(BenchmarkId::from_parameter(path), |b| {
            b.to_async(&runtime).iter(|| async {
                let replies = connection.lock().await.pipeline(&steps).await;
                black_box(replies.unwrap());
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("ten_thousand_rows");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.sample_size(30);
    for (path, connection) in paths {
        group.bench_function(BenchmarkId::from_parameter(path), |b| {
            b.to_async(&runtime).iter(|| async {
                let result = connection.lock().await.simple_query(MANY_ROWS).await;
                black_box(result.unwrap());
            });
        });
    }
    group.finish();
}

criterion_group!(benches, end_to_end);
criterion_main!(benches);
