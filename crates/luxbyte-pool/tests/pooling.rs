//! Pooling against a real socket, with no database anywhere.
//!
//! Everything here runs against `luxbyte-testkit`, which speaks the wire
//! protocol over loopback. The connections are real, the handshake is real,
//! and so are the failures.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::time::Duration;

use luxbyte_pool::{ConnectionTarget, Credentials, Pool, PoolConfig, PoolError, PoolSet};
use luxbyte_testkit::{Backend, BackendConfig, Reply};

fn target(backend: &Backend) -> ConnectionTarget {
    ConnectionTarget {
        host: "127.0.0.1".to_owned(),
        port: backend.port(),
        database: "shop".to_owned(),
        user: "alice".to_owned(),
    }
}

fn pool_with(backend: &Backend, config: PoolConfig) -> Pool {
    Pool::new(target(backend), Credentials::Trust, config)
}

fn pool(backend: &Backend) -> Pool {
    pool_with(backend, PoolConfig::default())
}

async fn backend_with(config: BackendConfig) -> Backend {
    Backend::start(config).await.expect("backend should start")
}

// --------------------------------------------------------- the happy path

#[tokio::test]
async fn connects_authenticates_and_returns_rows() {
    let backend = backend_with(BackendConfig::new("eu-west").reply_to(
        "SELECT",
        Reply::rows(&["id", "name"], &[&["1", "ada"], &["2", "bob"]]),
    ))
    .await;

    let result = pool(&backend)
        .simple_query("SELECT id, name FROM users")
        .await
        .expect("query should succeed");

    assert_eq!(result.columns, ["id", "name"]);
    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        result.rows.first().and_then(|row| row.first().cloned()),
        Some(Some("1".to_owned()))
    );
    assert_eq!(backend.connections_accepted(), 1);
}

#[tokio::test]
async fn the_startup_handshake_is_completed_before_any_query() {
    let backend = backend_with(BackendConfig::new("eu-west")).await;
    let connection = pool(&backend).acquire().await.expect("should connect");
    let inner = connection.connection().expect("connection is held");

    // These only exist if the handshake was read to its ReadyForQuery.
    assert_eq!(
        inner.parameter("server_version"),
        Some("18.0 (luxbyte-testkit)")
    );
    assert_eq!(inner.backend_key(), (4_242, 2_424));
}

#[tokio::test]
async fn null_and_empty_survive_the_round_trip_distinctly() {
    let backend = backend_with(BackendConfig::new("eu-west").reply_to(
        "SELECT",
        Reply::Rows {
            columns: vec!["a".to_owned(), "b".to_owned()],
            rows: vec![vec![None, Some(String::new())]],
        },
    ))
    .await;

    let result = pool(&backend).simple_query("SELECT a, b").await.unwrap();
    assert_eq!(result.rows, vec![vec![None, Some(String::new())]]);
}

// ------------------------------------------------------------ reuse

#[tokio::test]
async fn a_connection_is_reused_rather_than_reopened() {
    let backend = backend_with(BackendConfig::new("eu-west")).await;
    let pool = pool(&backend);

    for _ in 0..20 {
        pool.simple_query("SELECT 1").await.expect("should succeed");
    }

    assert_eq!(backend.connections_accepted(), 1);
    assert_eq!(pool.stats().opened, 1);
    assert_eq!(pool.stats().reused, 19);
}

#[tokio::test]
async fn the_pool_never_exceeds_its_maximum_size() {
    let backend = backend_with(BackendConfig::new("eu-west")).await;
    let pool = pool_with(
        &backend,
        PoolConfig {
            max_size: 2,
            ..PoolConfig::default()
        },
    );

    // Hold two, then prove a third cannot be opened while they are held.
    let first = pool.acquire().await.expect("first");
    let second = pool.acquire().await.expect("second");
    assert_eq!(backend.connections_live(), 2);

    let third = tokio::time::timeout(Duration::from_millis(150), pool.acquire()).await;
    assert!(third.is_err(), "a third borrow must wait for a free slot");
    assert_eq!(backend.connections_live(), 2);

    drop(first);
    let third = pool.acquire().await.expect("third after a release");
    assert_eq!(backend.connections_accepted(), 2);
    drop((second, third));
}

#[tokio::test]
async fn waiting_for_a_connection_gives_up_rather_than_hanging() {
    let backend = backend_with(BackendConfig::new("eu-west")).await;
    let pool = pool_with(
        &backend,
        PoolConfig {
            max_size: 1,
            acquire_timeout: Duration::from_millis(50),
            ..PoolConfig::default()
        },
    );

    let held = pool.acquire().await.expect("first");
    let error = pool.acquire().await.expect_err("should time out");
    assert!(matches!(error, PoolError::AcquireTimeout { .. }));
    assert_eq!(pool.stats().timeouts, 1);
    drop(held);

    // The slot is usable again once released.
    assert!(pool.acquire().await.is_ok());
}

// ------------------------------------------- the rule about transaction state

#[tokio::test]
async fn a_connection_left_in_a_transaction_is_not_returned_to_the_pool() {
    // Returning it would hand the next borrower someone else's open
    // transaction, complete with its locks and its snapshot.
    let backend = backend_with(BackendConfig::new("eu-west")).await;
    let pool = pool(&backend);

    {
        let mut connection = pool.acquire().await.expect("should connect");
        connection.simple_query("BEGIN").await.expect("begin");
        // Dropped mid-transaction, as a cancelled request would.
    }

    assert_eq!(pool.stats().discarded, 1);
    assert_eq!(pool.stats().idle, 0);

    pool.simple_query("SELECT 1").await.expect("should succeed");
    assert_eq!(
        backend.connections_accepted(),
        2,
        "a fresh connection, not the poisoned one"
    );
}

#[tokio::test]
async fn a_committed_transaction_leaves_the_connection_reusable() {
    let backend = backend_with(BackendConfig::new("eu-west")).await;
    let pool = pool(&backend);

    {
        let mut connection = pool.acquire().await.expect("should connect");
        connection.simple_query("BEGIN").await.unwrap();
        connection
            .simple_query("UPDATE users SET a = 1")
            .await
            .unwrap();
        connection.simple_query("COMMIT").await.unwrap();
    }

    assert_eq!(pool.stats().discarded, 0);
    pool.simple_query("SELECT 1").await.expect("should succeed");
    assert_eq!(backend.connections_accepted(), 1);
}

#[tokio::test]
async fn a_failed_statement_inside_a_transaction_retires_the_connection() {
    let backend = backend_with(
        BackendConfig::new("eu-west").reply_to("boom", Reply::fail("23505", "duplicate key")),
    )
    .await;
    let pool = pool(&backend);

    {
        let mut connection = pool.acquire().await.expect("should connect");
        connection.simple_query("BEGIN").await.unwrap();
        let error = connection
            .simple_query("INSERT INTO boom VALUES (1)")
            .await
            .expect_err("should fail");
        assert_eq!(error.sqlstate(), Some("23505"));
    }

    // The transaction is in its failed state, so the connection cannot be
    // handed on.
    assert_eq!(pool.stats().discarded, 1);
}

// ---------------------------------------------------------------- failures

#[tokio::test]
async fn a_server_error_is_reported_with_its_sqlstate() {
    let backend = backend_with(BackendConfig::new("eu-west").reply_to(
        "SELECT bad",
        Reply::fail("42703", "column \"bad\" does not exist"),
    ))
    .await;

    let error = pool(&backend)
        .simple_query("SELECT bad FROM users")
        .await
        .expect_err("should fail");
    assert_eq!(error.sqlstate(), Some("42703"));
    assert!(error.to_string().contains("does not exist"));
    assert!(!error.connection_lost());
}

#[tokio::test]
async fn a_statement_error_leaves_the_connection_usable() {
    let backend = backend_with(
        BackendConfig::new("eu-west").reply_to("bad", Reply::fail("42703", "no such column")),
    )
    .await;
    let pool = pool(&backend);

    let _ = pool.simple_query("SELECT bad").await;
    pool.simple_query("SELECT 1").await.expect("should succeed");

    // An error outside a transaction is not a reason to throw the
    // connection away.
    assert_eq!(backend.connections_accepted(), 1);
}

#[tokio::test]
async fn a_rejected_authentication_is_reported_rather_than_retried() {
    let backend = backend_with(BackendConfig::new("eu-west").refusing_auth()).await;
    let error = pool(&backend)
        .simple_query("SELECT 1")
        .await
        .expect_err("should fail");
    assert_eq!(error.sqlstate(), Some("28P01"));
}

#[tokio::test]
async fn connecting_to_a_closed_port_fails_quickly_and_clearly() {
    let backend = backend_with(BackendConfig::new("eu-west")).await;
    let port = backend.port();
    // Dropping the backend stops its listener, so the port is genuinely
    // closed rather than merely unattended.
    drop(backend);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let pool = Pool::new(
        ConnectionTarget {
            host: "127.0.0.1".to_owned(),
            port,
            database: "shop".to_owned(),
            user: "alice".to_owned(),
        },
        Credentials::Trust,
        PoolConfig {
            connect_timeout: Duration::from_millis(500),
            ..PoolConfig::default()
        },
    );
    let error = pool
        .simple_query("SELECT 1")
        .await
        .expect_err("should fail");
    assert!(error.connection_lost());
}

#[tokio::test]
async fn a_backend_that_vanishes_mid_query_is_reported_not_hung() {
    let backend =
        backend_with(BackendConfig::new("eu-west").disconnecting_on("SELECT doomed")).await;

    let error = tokio::time::timeout(
        Duration::from_secs(2),
        pool(&backend).simple_query("SELECT doomed"),
    )
    .await
    .expect("must not hang")
    .expect_err("should fail");

    assert!(error.connection_lost());
}

// -------------------------------------------------- the retry, and its limit

#[tokio::test]
async fn an_idempotent_statement_retries_once_on_a_dead_pooled_connection() {
    // A pooled connection can die while it sits idle - a server restart, a
    // firewall, an idle timeout - and the pool only finds out when it tries
    // to use it.
    let backend = backend_with(BackendConfig::new("eu-west")).await;
    let pool = pool(&backend);

    pool.simple_query("SELECT 1").await.expect("warm the pool");
    assert_eq!(pool.stats().idle, 1);

    backend.close_connections();
    tokio::time::sleep(Duration::from_millis(80)).await;

    let result = pool
        .simple_query_idempotent("SELECT 1")
        .await
        .expect("the retry should succeed on a fresh connection");
    assert_eq!(
        result.status,
        luxbyte_proto::backend::TransactionStatus::Idle
    );
    assert_eq!(
        backend.connections_accepted(),
        2,
        "the dead connection was replaced, not reused"
    );
}

#[tokio::test]
async fn without_the_retry_a_dead_pooled_connection_surfaces_as_an_error() {
    // The same situation through the ordinary path: the caller is told,
    // rather than having the statement silently run again.
    let backend = backend_with(BackendConfig::new("eu-west")).await;
    let pool = pool(&backend);

    pool.simple_query("SELECT 1").await.expect("warm the pool");
    backend.close_connections();
    tokio::time::sleep(Duration::from_millis(80)).await;

    let error = pool
        .simple_query("SELECT 1")
        .await
        .expect_err("the dead connection should surface");
    assert!(error.connection_lost());
}

#[tokio::test]
async fn a_non_idempotent_statement_is_never_retried() {
    let backend = backend_with(BackendConfig::new("eu-west").disconnecting_on("INSERT")).await;
    let pool = pool(&backend);

    let error = pool
        .simple_query("INSERT INTO users VALUES (1)")
        .await
        .expect_err("should fail");
    assert!(error.connection_lost());

    // Exactly one attempt reached the server. Retrying a write whose fate
    // is unknown is how a row gets inserted twice.
    assert_eq!(backend.count_of("INSERT"), 1);
}

// ------------------------------------------------------------- lifecycle

#[tokio::test]
async fn a_connection_past_its_maximum_lifetime_is_retired() {
    let backend = backend_with(BackendConfig::new("eu-west")).await;
    let pool = pool_with(
        &backend,
        PoolConfig {
            max_lifetime: Duration::from_millis(80),
            ..PoolConfig::default()
        },
    );

    pool.simple_query("SELECT 1").await.expect("first");
    assert_eq!(backend.connections_accepted(), 1);
    tokio::time::sleep(Duration::from_millis(120)).await;
    pool.simple_query("SELECT 1").await.expect("second");

    assert_eq!(backend.connections_accepted(), 2);
    assert!(pool.stats().discarded >= 1);
}

#[tokio::test]
async fn an_idle_connection_past_its_timeout_is_retired() {
    let backend = backend_with(BackendConfig::new("eu-west")).await;
    let pool = pool_with(
        &backend,
        PoolConfig {
            idle_timeout: Duration::from_millis(80),
            ..PoolConfig::default()
        },
    );

    pool.simple_query("SELECT 1").await.expect("first");
    tokio::time::sleep(Duration::from_millis(120)).await;
    pool.simple_query("SELECT 1").await.expect("second");

    assert_eq!(backend.connections_accepted(), 2);
}

#[tokio::test]
async fn draining_closes_the_idle_connections() {
    let backend = backend_with(BackendConfig::new("eu-west")).await;
    let pool = pool(&backend);
    pool.simple_query("SELECT 1").await.expect("warm");
    assert_eq!(pool.stats().idle, 1);

    pool.drain().await;
    assert_eq!(pool.stats().idle, 0);
}

#[tokio::test]
async fn a_detached_connection_does_not_go_back_to_the_pool() {
    let backend = backend_with(BackendConfig::new("eu-west")).await;
    let pool = pool(&backend);

    let borrowed = pool.acquire().await.expect("should connect");
    let detached = borrowed.detach().expect("connection is held");
    assert_eq!(pool.stats().idle, 0);
    detached.close().await;
}

// ------------------------------------------------------------- pool sets

#[tokio::test]
async fn each_backend_database_and_role_gets_its_own_pool() {
    let eu = backend_with(BackendConfig::new("eu-west")).await;
    let us = backend_with(BackendConfig::new("us-east")).await;
    let set = PoolSet::new(PoolConfig::default());

    let eu_pool = set.pool(&target(&eu), &Credentials::Trust);
    let us_pool = set.pool(&target(&us), &Credentials::Trust);
    // Same endpoint, different role: a separate pool, because a connection
    // carries its role and must not be shared across them.
    let mut other_role = target(&eu);
    other_role.user = "bob".to_owned();
    let _ = set.pool(&other_role, &Credentials::Trust);

    assert_eq!(set.len(), 3);

    eu_pool.simple_query("SELECT 1").await.expect("eu");
    us_pool.simple_query("SELECT 1").await.expect("us");
    assert_eq!(eu.connections_accepted(), 1);
    assert_eq!(us.connections_accepted(), 1);

    // Asking again returns the existing pool rather than making another.
    let again = set.pool(&target(&eu), &Credentials::Trust);
    again.simple_query("SELECT 1").await.expect("eu again");
    assert_eq!(eu.connections_accepted(), 1);
    assert_eq!(set.len(), 3);
}

#[tokio::test]
async fn concurrent_borrowers_share_a_bounded_set_of_connections() {
    let backend = backend_with(BackendConfig::new("eu-west")).await;
    let pool = pool_with(
        &backend,
        PoolConfig {
            max_size: 4,
            ..PoolConfig::default()
        },
    );

    let mut tasks = Vec::new();
    for _ in 0..32 {
        let pool = pool.clone();
        tasks.push(tokio::spawn(async move {
            pool.simple_query("SELECT 1").await.map(|_| ())
        }));
    }
    for task in tasks {
        task.await
            .expect("task should finish")
            .expect("query should succeed");
    }

    assert!(
        backend.connections_accepted() <= 4,
        "opened {} connections for 32 queries",
        backend.connections_accepted()
    );
}
