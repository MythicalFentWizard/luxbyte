//! The proxy, end to end.
//!
//! A real client connects over TCP to a real proxy, which connects over TCP
//! to real backends. Everything speaks the wire protocol. The only thing
//! that is not PostgreSQL is the backends, and they are only pretending so
//! that the interesting failures can be made to happen on purpose.
//!
//! The client is `luxbyte-pool`'s own `Connection`, which is a protocol
//! client already — so the proxy is exercised by the same code that talks
//! to backends, and nothing in the test knows it is not psql.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::time::Duration;

use luxbyte::{Config, ServerHandle};
use luxbyte_pool::{Connection, ConnectionTarget, Credentials, QueryResult};
use luxbyte_testkit::{Backend, BackendConfig, PreparedOutcome, Reply};

/// Three regions, each a fake backend, behind one proxy.
struct Fixture {
    eu: Backend,
    us: Backend,
    ap: Backend,
    proxy: ServerHandle,
}

async fn fixture_with(
    eu: BackendConfig,
    us: BackendConfig,
    ap: BackendConfig,
    directory: &[(&str, &str, &str)],
) -> Fixture {
    let eu = Backend::start(eu).await.expect("eu");
    let us = Backend::start(us).await.expect("us");
    let ap = Backend::start(ap).await.expect("ap");

    let entries = directory
        .iter()
        .fold(String::new(), |mut out, (table, key, region)| {
            use std::fmt::Write as _;
            let _ = write!(
                out,
                "[[directory]]\ntable = \"{table}\"\nkey = \"{key}\"\nregion = \"{region}\"\n"
            );
            out
        });

    let toml = format!(
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
primary = {{ host = "127.0.0.1", port = {eu_port} }}

[[topology.regions]]
id = "us-east"
primary = {{ host = "127.0.0.1", port = {us_port} }}

[[topology.regions]]
id = "ap-south"
primary = {{ host = "127.0.0.1", port = {ap_port} }}

[[topology.tables]]
table = "users"
placement = "geo_partitioned"
key = "tenant_id"

[[topology.tables]]
table = "orders"
placement = "geo_partitioned"
key = "tenant_id"

[[topology.tables]]
table = "currencies"
placement = "global_replicated"

{entries}
"#,
        eu_port = eu.port(),
        us_port = us.port(),
        ap_port = ap.port(),
    );

    let config = Config::from_toml(&toml).expect("config should parse");
    let server = luxbyte::Server::bind(&config).await.expect("should bind");
    let proxy = server.spawn().expect("should serve");
    Fixture { eu, us, ap, proxy }
}

async fn fixture(directory: &[(&str, &str, &str)]) -> Fixture {
    fixture_with(
        BackendConfig::new("eu-west"),
        BackendConfig::new("us-east"),
        BackendConfig::new("ap-south"),
        directory,
    )
    .await
}

fn placed() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("users", "alice", "eu-west"),
        ("users", "bob", "us-east"),
        ("orders", "alice", "eu-west"),
    ]
}

/// Connects to the proxy as an ordinary PostgreSQL client would.
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
    .expect("should connect to the proxy")
}

fn values(result: &QueryResult, column: usize) -> Vec<String> {
    result
        .rows
        .iter()
        .filter_map(|row| row.get(column)?.clone())
        .collect()
}

// ------------------------------------------------------------- the basics

#[tokio::test]
async fn a_client_can_connect_and_run_a_query() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;

    let result = client
        .simple_query("SELECT 1")
        .await
        .expect("query should succeed");
    assert_eq!(
        result.status,
        luxbyte_proto::backend::TransactionStatus::Idle
    );
    // `SELECT 1` touches no table, so it stays in the home region.
    assert!(f.eu.saw("SELECT 1"));
    assert!(!f.us.saw("SELECT 1"));
}

#[tokio::test]
async fn the_proxy_completes_a_startup_handshake() {
    let f = fixture(&placed()).await;
    let client = client(&f.proxy).await;
    assert_eq!(client.parameter("server_version"), Some("18.0 (luxbyte)"));
}

#[tokio::test]
async fn a_point_read_reaches_only_the_region_holding_the_key() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;

    client
        .simple_query("SELECT id FROM users WHERE tenant_id = 'alice'")
        .await
        .expect("should succeed");
    assert!(f.eu.saw("tenant_id = 'alice'"));
    assert!(!f.us.saw("tenant_id = 'alice'"));
    assert!(!f.ap.saw("tenant_id = 'alice'"));

    client
        .simple_query("SELECT id FROM users WHERE tenant_id = 'bob'")
        .await
        .expect("should succeed");
    assert!(f.us.saw("tenant_id = 'bob'"));
    assert!(!f.eu.saw("tenant_id = 'bob'"));
}

#[tokio::test]
async fn rows_come_back_through_the_proxy_unchanged() {
    let f = fixture_with(
        BackendConfig::new("eu-west").reply_to(
            "SELECT",
            Reply::Rows {
                columns: vec!["id".to_owned(), "name".to_owned()],
                rows: vec![
                    vec![Some("1".to_owned()), Some("ada".to_owned())],
                    vec![Some("2".to_owned()), None],
                ],
            },
        ),
        BackendConfig::new("us-east"),
        BackendConfig::new("ap-south"),
        &placed(),
    )
    .await;

    let mut client = client(&f.proxy).await;
    let result = client
        .simple_query("SELECT id, name FROM users WHERE tenant_id = 'alice'")
        .await
        .expect("should succeed");

    assert_eq!(result.columns, ["id", "name"]);
    // NULL survives the trip as NULL rather than becoming an empty string.
    assert_eq!(
        result.rows,
        vec![
            vec![Some("1".to_owned()), Some("ada".to_owned())],
            vec![Some("2".to_owned()), None],
        ]
    );
}

#[tokio::test]
async fn a_syntax_error_comes_back_as_a_syntax_error() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;
    let error = client
        .simple_query("SELEKT * FRM nowhere")
        .await
        .expect_err("should fail");
    assert_eq!(error.sqlstate(), Some("42601"));
    // The connection is still usable afterwards.
    assert!(client.simple_query("SELECT 1").await.is_ok());
}

// --------------------------------------------------------- scatter-gather

#[tokio::test]
async fn a_read_without_a_key_fans_out_and_the_rows_are_merged() {
    let rows =
        |value: &str| BackendConfig::new("x").reply_to("SELECT", Reply::rows(&["id"], &[&[value]]));
    let f = fixture_with(rows("1"), rows("2"), rows("3"), &placed()).await;

    let mut client = client(&f.proxy).await;
    let result = client
        .simple_query("SELECT id FROM users WHERE name = 'ada'")
        .await
        .expect("should succeed");

    let mut found = values(&result, 0);
    found.sort();
    assert_eq!(found, ["1", "2", "3"], "every region contributed");
    assert_eq!(result.tags, ["SELECT 3"]);
}

#[tokio::test]
async fn an_ordered_scatter_comes_back_in_order() {
    let f = fixture_with(
        BackendConfig::new("eu").reply_to("SELECT", Reply::rows(&["id"], &[&["1"], &["4"]])),
        BackendConfig::new("us").reply_to("SELECT", Reply::rows(&["id"], &[&["2"], &["5"]])),
        BackendConfig::new("ap").reply_to("SELECT", Reply::rows(&["id"], &[&["3"], &["6"]])),
        &placed(),
    )
    .await;

    let mut client = client(&f.proxy).await;
    let result = client
        .simple_query("SELECT id FROM users WHERE name = 'ada' ORDER BY id")
        .await
        .expect("should succeed");

    assert_eq!(values(&result, 0), ["1", "2", "3", "4", "5", "6"]);
}

#[tokio::test]
async fn an_in_list_touches_only_the_regions_it_names() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;
    client
        .simple_query("SELECT id FROM users WHERE tenant_id IN ('alice', 'bob')")
        .await
        .expect("should succeed");

    assert!(f.eu.saw("IN ('alice', 'bob')"));
    assert!(f.us.saw("IN ('alice', 'bob')"));
    assert!(!f.ap.saw("IN ('alice', 'bob')"), "ap holds neither key");
}

#[tokio::test]
async fn an_aggregate_across_regions_is_refused_with_a_reason() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;
    let error = client
        .simple_query("SELECT count(*) FROM users")
        .await
        .expect_err("should be refused");

    assert_eq!(error.sqlstate(), Some("0A000"));
    assert!(error.to_string().contains("aggregat"));
    // Nothing was sent to any backend.
    assert!(!f.eu.saw("count(*)"));
}

#[tokio::test]
async fn one_region_failing_fails_the_whole_scatter() {
    // A partial result that looks complete is worse than an error.
    let f = fixture_with(
        BackendConfig::new("eu").reply_to("SELECT", Reply::rows(&["id"], &[&["1"]])),
        BackendConfig::new("us").reply_to("SELECT", Reply::fail("53300", "too many clients")),
        BackendConfig::new("ap").reply_to("SELECT", Reply::rows(&["id"], &[&["3"]])),
        &placed(),
    )
    .await;

    let mut client = client(&f.proxy).await;
    let error = client
        .simple_query("SELECT id FROM users WHERE name = 'ada'")
        .await
        .expect_err("should fail");
    assert!(error.to_string().contains("us-east"));
}

// ------------------------------------------------------------- the writes

#[tokio::test]
async fn a_write_goes_to_the_region_holding_the_key() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;
    client
        .simple_query("UPDATE users SET name = 'x' WHERE tenant_id = 'bob'")
        .await
        .expect("should succeed");

    assert!(f.us.saw("UPDATE users"));
    assert!(!f.eu.saw("UPDATE users"));
}

#[tokio::test]
async fn a_multi_region_insert_is_split_so_each_row_lands_once() {
    // Broadcasting would write both rows in both regions, and nothing
    // would complain.
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;

    client
        .simple_query("INSERT INTO users (tenant_id, name) VALUES ('alice', 'a'), ('bob', 'b')")
        .await
        .expect("should succeed");

    assert!(f.eu.saw("'alice'"));
    assert!(
        !f.eu.saw("'bob'"),
        "alice's region must not receive bob's row"
    );
    assert!(f.us.saw("'bob'"));
    assert!(
        !f.us.saw("'alice'"),
        "bob's region must not receive alice's row"
    );
}

#[tokio::test]
async fn a_multi_region_write_commits_in_two_phases() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;

    client
        .simple_query("INSERT INTO users (tenant_id, name) VALUES ('alice', 'a'), ('bob', 'b')")
        .await
        .expect("should succeed");

    for backend in [&f.eu, &f.us] {
        assert!(backend.saw("PREPARE TRANSACTION"));
        let prepared = backend.prepared_transactions();
        assert!(
            prepared
                .iter()
                .all(|(_, outcome)| *outcome == PreparedOutcome::Committed),
            "{} should have committed its prepared transaction",
            backend.name()
        );
    }
    // ap-south was not involved, so it must not have been dragged in.
    assert!(!f.ap.saw("PREPARE TRANSACTION"));
}

#[tokio::test]
async fn a_single_region_write_does_not_pay_for_two_phase_commit() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;
    client
        .simple_query("INSERT INTO users (tenant_id, name) VALUES ('alice', 'a')")
        .await
        .expect("should succeed");

    assert!(f.eu.saw("INSERT"));
    assert!(!f.eu.saw("PREPARE TRANSACTION"));
}

#[tokio::test]
async fn a_participant_that_refuses_to_prepare_aborts_the_whole_write() {
    let f = fixture_with(
        BackendConfig::new("eu-west"),
        BackendConfig::new("us-east").refusing_prepare(),
        BackendConfig::new("ap-south"),
        &placed(),
    )
    .await;

    let mut client = client(&f.proxy).await;
    let error = client
        .simple_query("INSERT INTO users (tenant_id, name) VALUES ('alice', 'a'), ('bob', 'b')")
        .await
        .expect_err("should fail");

    assert_eq!(error.sqlstate(), Some("40001"), "the client should retry");
    // The region that did prepare must have rolled back, not been left
    // holding a prepared transaction.
    assert!(
        f.eu.prepared_transactions()
            .iter()
            .all(|(_, outcome)| *outcome == PreparedOutcome::RolledBack)
    );
}

#[tokio::test]
async fn a_write_to_a_replicated_table_reaches_every_region() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;
    client
        .simple_query("UPDATE currencies SET rate = 1.1 WHERE code = 'EUR'")
        .await
        .expect("should succeed");

    for backend in [&f.eu, &f.us, &f.ap] {
        assert!(backend.saw("UPDATE currencies"), "{}", backend.name());
        assert!(backend.saw("PREPARE TRANSACTION"));
    }
}

// ------------------------------------------------------ explicit transactions

#[tokio::test]
async fn an_explicit_transaction_in_one_region_commits_in_one_phase() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;

    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("UPDATE users SET name = 'x' WHERE tenant_id = 'alice'")
        .await
        .expect("update");
    let result = client.simple_query("COMMIT").await.expect("commit");

    assert_eq!(
        result.status,
        luxbyte_proto::backend::TransactionStatus::Idle
    );
    assert!(f.eu.saw("COMMIT"));
    assert!(!f.eu.saw("PREPARE TRANSACTION"));
}

#[tokio::test]
async fn an_explicit_transaction_across_regions_commits_in_two_phases() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;

    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("UPDATE users SET name = 'x' WHERE tenant_id = 'alice'")
        .await
        .expect("eu write");
    client
        .simple_query("UPDATE users SET name = 'y' WHERE tenant_id = 'bob'")
        .await
        .expect("us write");
    client.simple_query("COMMIT").await.expect("commit");

    for backend in [&f.eu, &f.us] {
        assert!(backend.saw("PREPARE TRANSACTION"), "{}", backend.name());
        assert!(backend.saw("COMMIT PREPARED"));
    }
}

#[tokio::test]
async fn rolling_back_reaches_every_region_that_was_touched() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;

    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("DELETE FROM users WHERE tenant_id = 'alice'")
        .await
        .expect("eu write");
    client
        .simple_query("DELETE FROM users WHERE tenant_id = 'bob'")
        .await
        .expect("us write");
    client.simple_query("ROLLBACK").await.expect("rollback");

    for backend in [&f.eu, &f.us] {
        assert!(backend.saw("ROLLBACK"), "{}", backend.name());
        assert!(!backend.saw("COMMIT"));
    }
}

#[tokio::test]
async fn a_failed_statement_aborts_the_transaction_block() {
    let f = fixture_with(
        BackendConfig::new("eu-west").reply_to("boom", Reply::fail("23505", "duplicate key")),
        BackendConfig::new("us-east"),
        BackendConfig::new("ap-south"),
        &placed(),
    )
    .await;
    let mut client = client(&f.proxy).await;

    client.simple_query("BEGIN").await.expect("begin");
    let _ = client
        .simple_query("INSERT INTO users (tenant_id, name) VALUES ('alice', 'boom')")
        .await
        .expect_err("should fail");

    // PostgreSQL refuses everything until the block ends, and so does this.
    let error = client
        .simple_query("SELECT id FROM users WHERE tenant_id = 'alice'")
        .await
        .expect_err("should be blocked");
    assert_eq!(error.sqlstate(), Some("25P02"));

    client.simple_query("ROLLBACK").await.expect("rollback");
    assert!(client.simple_query("SELECT 1").await.is_ok());
}

// ------------------------------------------------------------ session state

#[tokio::test]
async fn a_set_is_replayed_onto_whichever_region_is_used_next() {
    // A session that moves between regions has to keep the settings it
    // asked for, or it quietly behaves differently depending on where its
    // rows happen to live.
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;

    client
        .simple_query("SET search_path = analytics")
        .await
        .expect("set");
    client
        .simple_query("SELECT id FROM users WHERE tenant_id = 'alice'")
        .await
        .expect("eu read");
    client
        .simple_query("SELECT id FROM users WHERE tenant_id = 'bob'")
        .await
        .expect("us read");

    assert!(f.eu.saw("SET search_path = analytics"));
    assert!(f.us.saw("SET search_path = analytics"));
}

#[tokio::test]
async fn discard_forgets_the_settings() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;

    client
        .simple_query("SET work_mem = '64MB'")
        .await
        .expect("set");
    client.simple_query("DISCARD ALL").await.expect("discard");
    f.eu.clear();
    client
        .simple_query("SELECT id FROM users WHERE tenant_id = 'alice'")
        .await
        .expect("read");

    assert!(!f.eu.saw("work_mem"));
}

#[tokio::test]
async fn one_clients_settings_never_reach_another_client() {
    // Connections are pooled across clients, so a SET replayed onto one
    // would otherwise be inherited by whoever borrows it next.
    let f = fixture(&placed()).await;
    let mut first = client(&f.proxy).await;
    first
        .simple_query("SET search_path = tenant_a")
        .await
        .expect("set");
    first
        .simple_query("SELECT id FROM users WHERE tenant_id = 'alice'")
        .await
        .expect("read");

    // The reset runs as the connection is returned, off the client's path.
    for _ in 0..200 {
        if f.eu.saw("DISCARD ALL") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let mut second = client(&f.proxy).await;
    second
        .simple_query("SELECT id FROM users WHERE tenant_id = 'alice'")
        .await
        .expect("read");

    let statements = f.eu.statements();
    let reset = statements.iter().position(|sql| sql == "DISCARD ALL");
    let last = statements.len().checked_sub(1);
    assert!(reset.is_some() && reset < last, "{statements:?}");
    assert_eq!(
        statements.last().map(String::as_str),
        Some("SELECT id FROM users WHERE tenant_id = 'alice'"),
        "the second client's statement carries none of the first's settings"
    );
    // Same connection, reset rather than reopened.
    assert_eq!(f.eu.connections_accepted(), 1);
}

// ---------------------------------------------------------------- console

#[tokio::test]
async fn the_console_answers_over_the_same_connection() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;

    let regions = client.simple_query("SHOW REGIONS").await.expect("regions");
    assert_eq!(regions.columns, ["region", "home", "primary", "replicas"]);
    assert_eq!(values(&regions, 0), ["eu-west", "us-east", "ap-south"]);

    let placement = client
        .simple_query("SHOW PLACEMENT")
        .await
        .expect("placement");
    assert!(values(&placement, 0).contains(&"public.users".to_owned()));

    let where_is = client
        .simple_query("WHERE IS users alice")
        .await
        .expect("where is");
    assert_eq!(values(&where_is, 2), ["eu-west"]);

    // None of that reached a backend.
    assert!(!f.eu.saw("SHOW REGIONS"));
}

#[tokio::test]
async fn the_console_reports_an_unseen_key_as_unseen() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;
    let result = client
        .simple_query("WHERE IS users nobody")
        .await
        .expect("where is");
    assert_eq!(values(&result, 2), ["eu-west"]);
    assert!(
        values(&result, 3)
            .first()
            .is_some_and(|s| s.contains("unseen"))
    );
}

#[tokio::test]
async fn show_pools_reports_the_connections_actually_opened() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;
    client
        .simple_query("SELECT id FROM users WHERE tenant_id = 'alice'")
        .await
        .expect("read");

    let pools = client.simple_query("SHOW POOLS").await.expect("pools");
    assert_ne!(pools.rows.len(), 0);
    assert_eq!(f.eu.connections_accepted(), 1);
}

// ------------------------------------------------------------ refusals

#[tokio::test]
async fn a_cross_region_join_is_refused_rather_than_answered_wrongly() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;
    let error = client
        .simple_query(
            "SELECT * FROM users u JOIN orders o ON o.tenant_id = u.tenant_id \
             WHERE u.tenant_id = 'bob' AND o.tenant_id = 'alice'",
        )
        .await
        .expect_err("should be refused");
    assert_eq!(error.sqlstate(), Some("0A000"));
    assert!(error.to_string().contains("join"));
}

#[tokio::test]
async fn assigning_to_the_shard_key_is_refused() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;
    let error = client
        .simple_query("UPDATE users SET tenant_id = 'bob' WHERE tenant_id = 'alice'")
        .await
        .expect_err("should be refused");
    assert!(error.to_string().contains("shard key"));
    assert!(!f.eu.saw("UPDATE users"));
}

// ------------------------------------------------------------- pooling

#[tokio::test]
async fn many_client_queries_share_a_few_backend_connections() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;
    for _ in 0..25 {
        client
            .simple_query("SELECT id FROM users WHERE tenant_id = 'alice'")
            .await
            .expect("read");
    }
    assert_eq!(f.eu.connections_accepted(), 1);
}

#[tokio::test]
async fn several_clients_are_served_at_once() {
    let f = fixture(&placed()).await;
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let port = f.proxy.port();
        tasks.push(tokio::spawn(async move {
            let mut connection = Connection::connect(
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
            .expect("connect");
            connection
                .simple_query("SELECT id FROM users WHERE tenant_id = 'alice'")
                .await
                .map(|_| ())
        }));
    }
    for task in tasks {
        task.await.expect("task").expect("query");
    }
    assert!(f.eu.connections_accepted() <= 8);
}

#[tokio::test]
async fn each_statement_of_a_batch_runs_only_where_it_was_routed() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;
    client
        .simple_query(
            "UPDATE users SET name = 'a' WHERE tenant_id = 'alice'; \
             UPDATE users SET name = 'b' WHERE tenant_id = 'bob'",
        )
        .await
        .expect("batch");
    assert!(f.eu.saw("tenant_id = 'alice'"));
    assert!(f.us.saw("tenant_id = 'bob'"));
    // Bob's row lives in us-east; eu-west must never be sent his update.
    assert!(!f.eu.saw("tenant_id = 'bob'"), "{:?}", f.eu.statements());
    assert!(!f.us.saw("tenant_id = 'alice'"), "{:?}", f.us.statements());
}

#[tokio::test]
async fn a_batch_is_one_transaction_so_a_failure_undoes_what_came_before() {
    let f = fixture_with(
        BackendConfig::new("eu-west").reply_to("boom", Reply::fail("23505", "duplicate key")),
        BackendConfig::new("us-east"),
        BackendConfig::new("ap-south"),
        &placed(),
    )
    .await;
    let mut client = client(&f.proxy).await;
    let error = client
        .simple_query(
            "UPDATE users SET name = 'a' WHERE tenant_id = 'alice'; \
             UPDATE users SET name = 'boom' WHERE tenant_id = 'alice'",
        )
        .await
        .expect_err("the second statement fails");
    assert_eq!(error.sqlstate(), Some("23505"));
    // On PostgreSQL the first update would be rolled back with the second.
    assert!(f.eu.saw("BEGIN"));
    assert!(f.eu.saw("ROLLBACK"));
    assert!(!f.eu.saw("COMMIT"));
    assert_eq!(
        client.status(),
        luxbyte_proto::backend::TransactionStatus::Idle
    );
}

#[tokio::test]
async fn a_batch_across_regions_commits_in_two_phases() {
    let f = fixture(&placed()).await;
    let mut client = client(&f.proxy).await;
    client
        .simple_query(
            "UPDATE users SET name = 'a' WHERE tenant_id = 'alice'; \
             UPDATE users SET name = 'b' WHERE tenant_id = 'bob'",
        )
        .await
        .expect("batch");
    for backend in [&f.eu, &f.us] {
        let prepared = backend.prepared_transactions();
        assert_eq!(prepared.len(), 1, "{}", backend.name());
        assert!(
            prepared
                .iter()
                .all(|(_, outcome)| *outcome == PreparedOutcome::Committed)
        );
    }
}
