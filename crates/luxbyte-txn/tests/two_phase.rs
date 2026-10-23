//! Two-phase commit against real sockets, including the failure paths.
//!
//! Every test here runs against `luxbyte-testkit`. No database, no Docker,
//! and the crash cases happen on demand rather than on a good day.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use luxbyte_pool::{ConnectionTarget, Credentials, Pool, PoolConfig};
use luxbyte_testkit::{Backend, BackendConfig, PreparedOutcome, Reply};
use luxbyte_txn::log::{CommitLog, CommitRecord};
use luxbyte_txn::{CommitOutcome, Coordinator, MemoryLog, TxnError};
use parking_lot::Mutex;

async fn backend(name: &str) -> Arc<Backend> {
    Arc::new(
        Backend::start(BackendConfig::new(name))
            .await
            .expect("backend should start"),
    )
}

async fn backend_with(config: BackendConfig) -> Arc<Backend> {
    Arc::new(Backend::start(config).await.expect("backend should start"))
}

fn pool_for(backend: &Backend) -> Pool {
    Pool::new(
        ConnectionTarget {
            host: "127.0.0.1".to_owned(),
            port: backend.port(),
            database: "shop".to_owned(),
            user: "alice".to_owned(),
        },
        Credentials::Trust,
        PoolConfig::default(),
    )
}

fn coordinator(log: Arc<dyn CommitLog>) -> Coordinator {
    Coordinator::new("eu-proxy-1", log)
}

// ------------------------------------------------------------ the fast path

#[tokio::test]
async fn one_participant_commits_in_a_single_phase() {
    // With nobody to disagree with, the prepare, the log write and the
    // in-doubt window are all pure cost.
    let eu = backend("eu-west").await;
    let log = Arc::new(MemoryLog::new());
    let coordinator = coordinator(log.clone());

    let mut transaction = coordinator.begin();
    transaction
        .enlist("eu-west", &pool_for(&eu))
        .await
        .expect("enlist");
    transaction
        .execute("eu-west", "UPDATE users SET name = 'ada'")
        .await
        .expect("execute");
    let outcome = transaction.commit().await.expect("commit");

    assert_eq!(
        outcome,
        CommitOutcome::SinglePhase {
            region: "eu-west".to_owned()
        }
    );
    assert!(eu.saw("COMMIT"));
    assert!(!eu.saw("PREPARE TRANSACTION"));
    assert!(log.is_empty(), "no decision needs recording");
}

#[tokio::test]
async fn a_transaction_with_no_participants_commits_trivially() {
    let coordinator = coordinator(Arc::new(MemoryLog::new()));
    let outcome = coordinator.begin().commit().await.expect("commit");
    assert_eq!(outcome, CommitOutcome::Empty);
    assert!(!outcome.is_committed());
}

// ------------------------------------------------------- the two-phase path

#[tokio::test]
async fn two_participants_prepare_then_commit() {
    let eu = backend("eu-west").await;
    let us = backend("us-east").await;
    let log = Arc::new(MemoryLog::new());
    let coordinator = coordinator(log.clone());

    let mut transaction = coordinator.begin();
    let gid = transaction.gid().to_owned();
    transaction.enlist("eu-west", &pool_for(&eu)).await.unwrap();
    transaction.enlist("us-east", &pool_for(&us)).await.unwrap();
    transaction
        .execute("eu-west", "INSERT INTO users VALUES ('alice')")
        .await
        .unwrap();
    transaction
        .execute("us-east", "INSERT INTO users VALUES ('bob')")
        .await
        .unwrap();

    let outcome = transaction.commit().await.expect("commit");
    assert_eq!(
        outcome,
        CommitOutcome::Committed {
            regions: vec!["eu-west".to_owned(), "us-east".to_owned()]
        }
    );

    for participant in [&eu, &us] {
        assert_eq!(
            participant.prepared(&gid),
            Some(PreparedOutcome::Committed),
            "{} should have committed its prepared transaction",
            participant.name()
        );
    }
    assert!(
        log.is_empty(),
        "the record is forgotten once everyone commits"
    );
}

/// A log that snapshots what the participants have seen at the moment the
/// decision is written.
#[derive(Debug)]
struct SpyLog {
    inner: MemoryLog,
    participants: Vec<Arc<Backend>>,
    at_decision: Mutex<Option<Vec<String>>>,
}

impl SpyLog {
    fn new(participants: Vec<Arc<Backend>>) -> Arc<Self> {
        Arc::new(Self {
            inner: MemoryLog::new(),
            participants,
            at_decision: Mutex::new(None),
        })
    }

    fn snapshot(&self) -> Vec<String> {
        self.at_decision.lock().clone().unwrap_or_default()
    }
}

impl CommitLog for SpyLog {
    fn record(&self, record: &CommitRecord) -> Result<(), TxnError> {
        let seen: Vec<String> = self
            .participants
            .iter()
            .flat_map(|backend| backend.statements())
            .collect();
        *self.at_decision.lock() = Some(seen);
        self.inner.record(record)
    }

    fn forget(&self, gid: &str) -> Result<(), TxnError> {
        self.inner.forget(gid)
    }

    fn pending(&self) -> Result<Vec<CommitRecord>, TxnError> {
        self.inner.pending()
    }
}

#[tokio::test]
async fn the_decision_is_recorded_after_every_prepare_and_before_any_commit() {
    // This ordering *is* the protocol. Record too early and a crash commits
    // a transaction a participant had refused; record too late and a crash
    // rolls back one a participant already committed.
    let eu = backend("eu-west").await;
    let us = backend("us-east").await;
    let log = SpyLog::new(vec![Arc::clone(&eu), Arc::clone(&us)]);
    let coordinator = coordinator(log.clone());

    let mut transaction = coordinator.begin();
    transaction.enlist("eu-west", &pool_for(&eu)).await.unwrap();
    transaction.enlist("us-east", &pool_for(&us)).await.unwrap();
    transaction.commit().await.expect("commit");

    let at_decision = log.snapshot();
    let prepares = at_decision
        .iter()
        .filter(|sql| sql.contains("PREPARE TRANSACTION"))
        .count();
    let commits = at_decision
        .iter()
        .filter(|sql| sql.contains("COMMIT PREPARED"))
        .count();

    assert_eq!(prepares, 2, "both participants must have prepared first");
    assert_eq!(commits, 0, "nobody may be told to commit before the record");
}

// ------------------------------------------------------- prepare that fails

#[tokio::test]
async fn a_refused_prepare_rolls_everything_back() {
    let eu = backend("eu-west").await;
    let us = backend_with(BackendConfig::new("us-east").refusing_prepare()).await;
    let log = Arc::new(MemoryLog::new());
    let coordinator = coordinator(log.clone());

    let mut transaction = coordinator.begin();
    let gid = transaction.gid().to_owned();
    transaction.enlist("eu-west", &pool_for(&eu)).await.unwrap();
    transaction.enlist("us-east", &pool_for(&us)).await.unwrap();

    let error = transaction.commit().await.expect_err("should fail");
    let TxnError::PrepareFailed { region, .. } = &error else {
        panic!("expected a prepare failure, got {error:?}");
    };
    assert_eq!(region, "us-east");
    assert!(error.retryable(), "nothing committed, so a retry is clean");

    // The one that did prepare must have been rolled back, not left in doubt.
    assert_eq!(eu.prepared(&gid), Some(PreparedOutcome::RolledBack));
    assert!(!eu.saw("COMMIT PREPARED"));
    assert!(log.is_empty(), "no decision was ever taken");
}

#[tokio::test]
async fn the_first_participant_to_refuse_stops_the_rest_preparing() {
    let eu = backend_with(BackendConfig::new("eu-west").refusing_prepare()).await;
    let us = backend("us-east").await;
    let coordinator = coordinator(Arc::new(MemoryLog::new()));

    let mut transaction = coordinator.begin();
    transaction.enlist("eu-west", &pool_for(&eu)).await.unwrap();
    transaction.enlist("us-east", &pool_for(&us)).await.unwrap();
    let _ = transaction.commit().await.expect_err("should fail");

    assert!(!us.saw("PREPARE TRANSACTION"));
    assert!(us.saw("ROLLBACK"));
}

// -------------------------------------------------------- commit that stalls

#[tokio::test]
async fn a_participant_that_dies_after_the_decision_leaves_it_committed() {
    // The decision is durable, so the transaction is committed even though
    // one participant has not acknowledged it. Reporting it as a failure
    // would be a lie, and rolling back would be a disaster.
    let eu = backend("eu-west").await;
    let us = backend_with(BackendConfig::new("us-east").disconnecting_on("COMMIT PREPARED")).await;
    let log = Arc::new(MemoryLog::new());
    let coordinator = coordinator(log.clone());

    let mut transaction = coordinator.begin();
    let gid = transaction.gid().to_owned();
    transaction.enlist("eu-west", &pool_for(&eu)).await.unwrap();
    transaction.enlist("us-east", &pool_for(&us)).await.unwrap();
    let outcome = transaction.commit().await.expect("the commit stands");

    assert!(outcome.is_committed());
    assert_eq!(outcome.unresolved(), ["us-east"]);
    assert_eq!(eu.prepared(&gid), Some(PreparedOutcome::Committed));

    // The record stays, because recovery still has work to do.
    assert_eq!(log.len(), 1);
}

// ------------------------------------------------------------- the rollback

#[tokio::test]
async fn rolling_back_before_preparing_uses_a_plain_rollback() {
    let eu = backend("eu-west").await;
    let us = backend("us-east").await;
    let coordinator = coordinator(Arc::new(MemoryLog::new()));

    let mut transaction = coordinator.begin();
    transaction.enlist("eu-west", &pool_for(&eu)).await.unwrap();
    transaction.enlist("us-east", &pool_for(&us)).await.unwrap();
    transaction.rollback().await.expect("rollback");

    for participant in [&eu, &us] {
        assert!(participant.saw("ROLLBACK"));
        assert!(!participant.saw("ROLLBACK PREPARED"));
    }
}

#[tokio::test]
async fn a_dropped_transaction_commits_nothing() {
    let eu = backend("eu-west").await;
    let pool = pool_for(&eu);
    let coordinator = coordinator(Arc::new(MemoryLog::new()));

    {
        let mut transaction = coordinator.begin();
        transaction.enlist("eu-west", &pool).await.unwrap();
        transaction
            .execute("eu-west", "DELETE FROM users")
            .await
            .unwrap();
        // Dropped without committing, as a cancelled request would be.
    }

    assert!(!eu.saw("COMMIT"));
    // The connection was still in a transaction, so it is retired rather
    // than handed to the next caller with the work still open on it.
    assert_eq!(pool.stats().discarded, 1);
}

// --------------------------------------------------------------- misuse

#[tokio::test]
async fn enlisting_the_same_region_twice_opens_one_transaction() {
    let eu = backend("eu-west").await;
    let pool = pool_for(&eu);
    let coordinator = coordinator(Arc::new(MemoryLog::new()));

    let mut transaction = coordinator.begin();
    transaction.enlist("eu-west", &pool).await.unwrap();
    transaction.enlist("eu-west", &pool).await.unwrap();

    assert_eq!(transaction.len(), 1);
    assert_eq!(eu.count_of("BEGIN"), 1);
}

#[tokio::test]
async fn a_statement_for_a_region_that_is_not_enlisted_is_refused() {
    let eu = backend("eu-west").await;
    let coordinator = coordinator(Arc::new(MemoryLog::new()));
    let mut transaction = coordinator.begin();
    transaction.enlist("eu-west", &pool_for(&eu)).await.unwrap();

    let error = transaction
        .execute("ap-south", "SELECT 1")
        .await
        .expect_err("should refuse");
    assert!(matches!(error, TxnError::UnknownRegion(region) if region == "ap-south"));
}

// ------------------------------------------------------------- recovery

#[tokio::test]
async fn recovery_finishes_a_transaction_the_log_says_was_decided() {
    let eu = backend("eu-west").await;
    let us = backend("us-east").await;
    let log = Arc::new(MemoryLog::new());

    // A coordinator that decided to commit and then died.
    let gid = "luxbyte-eu-proxy-1-7-1700000000000";
    for participant in [&eu, &us] {
        pool_for(participant)
            .simple_query(&format!("PREPARE TRANSACTION '{gid}'"))
            .await
            .expect("prepare");
    }
    log.record(&CommitRecord::new(
        gid,
        &["eu-west".to_owned(), "us-east".to_owned()],
    ))
    .expect("record");

    let pools = BTreeMap::from([
        ("eu-west".to_owned(), pool_for(&eu)),
        ("us-east".to_owned(), pool_for(&us)),
    ]);
    let report = coordinator(log.clone())
        .recover(&pools)
        .await
        .expect("recover");

    assert_eq!(report.committed, [gid]);
    assert_eq!(report.still_in_doubt, Vec::<String>::new());
    assert_eq!(eu.prepared(gid), Some(PreparedOutcome::Committed));
    assert_eq!(us.prepared(gid), Some(PreparedOutcome::Committed));
    assert!(log.is_empty());
}

#[tokio::test]
async fn recovery_treats_an_already_committed_participant_as_done() {
    // The coordinator died part-way through phase two, so one participant
    // has committed and no longer knows the identifier.
    let eu = backend("eu-west").await;
    let log = Arc::new(MemoryLog::new());
    let gid = "luxbyte-eu-proxy-1-9-1700000000000";
    log.record(&CommitRecord::new(gid, &["eu-west".to_owned()]))
        .expect("record");

    // Never prepared here, so `COMMIT PREPARED` reports 42704.
    let pools = BTreeMap::from([("eu-west".to_owned(), pool_for(&eu))]);
    let report = coordinator(log.clone())
        .recover(&pools)
        .await
        .expect("recover");

    assert_eq!(report.committed, [gid]);
    assert!(log.is_empty(), "nothing left to resolve");
}

#[tokio::test]
async fn recovery_leaves_a_record_alone_when_a_participant_is_unreachable() {
    let eu = backend("eu-west").await;
    let log = Arc::new(MemoryLog::new());
    let gid = "luxbyte-eu-proxy-1-3-1700000000000";
    log.record(&CommitRecord::new(
        gid,
        &["eu-west".to_owned(), "us-east".to_owned()],
    ))
    .expect("record");

    // Only one of the two participants is reachable.
    let pools = BTreeMap::from([("eu-west".to_owned(), pool_for(&eu))]);
    let report = coordinator(log.clone())
        .recover(&pools)
        .await
        .expect("recover");

    assert_eq!(report.still_in_doubt, [gid]);
    assert_eq!(log.len(), 1, "the decision must survive for the next pass");
}

#[tokio::test]
async fn recovery_is_safe_to_run_twice() {
    let eu = backend("eu-west").await;
    let log = Arc::new(MemoryLog::new());
    let gid = "luxbyte-eu-proxy-1-4-1700000000000";
    pool_for(&eu)
        .simple_query(&format!("PREPARE TRANSACTION '{gid}'"))
        .await
        .expect("prepare");
    log.record(&CommitRecord::new(gid, &["eu-west".to_owned()]))
        .expect("record");

    let pools = BTreeMap::from([("eu-west".to_owned(), pool_for(&eu))]);
    let coordinator = coordinator(log.clone());
    let first = coordinator.recover(&pools).await.expect("first pass");
    let second = coordinator.recover(&pools).await.expect("second pass");

    assert_eq!(first.committed, [gid]);
    assert!(second.is_clean());
}

#[tokio::test]
async fn recovery_rolls_back_a_prepared_transaction_the_log_never_decided() {
    // The dangerous case. This one was prepared and then abandoned, so it
    // holds locks and pins the transaction horizon until somebody removes
    // it. Nothing will, unless recovery goes looking.
    let orphan = "luxbyte-eu-proxy-1-99-1700000000000";
    let eu = backend_with(
        BackendConfig::new("eu-west")
            .reply_to("pg_prepared_xacts", Reply::rows(&["gid"], &[&[orphan]])),
    )
    .await;

    let log = Arc::new(MemoryLog::new());
    let pools = BTreeMap::from([("eu-west".to_owned(), pool_for(&eu))]);
    let report = coordinator(log).recover(&pools).await.expect("recover");

    assert_eq!(report.rolled_back.len(), 1);
    assert!(
        report
            .rolled_back
            .first()
            .is_some_and(|entry| entry.contains(orphan))
    );
    assert!(eu.saw(&format!("ROLLBACK PREPARED '{orphan}'")));
}

#[tokio::test]
async fn recovery_does_not_touch_a_prepared_transaction_it_has_decided_to_commit() {
    // The same query returns a gid the log *does* know about. Rolling that
    // one back would undo a committed transaction.
    let gid = "luxbyte-eu-proxy-1-11-1700000000000";
    let eu = backend_with(
        BackendConfig::new("eu-west")
            .reply_to("pg_prepared_xacts", Reply::rows(&["gid"], &[&[gid]])),
    )
    .await;
    let log = Arc::new(MemoryLog::new());
    log.record(&CommitRecord::new(gid, &["eu-west".to_owned()]))
        .expect("record");

    let pools = BTreeMap::from([("eu-west".to_owned(), pool_for(&eu))]);
    let report = coordinator(log).recover(&pools).await.expect("recover");

    assert_eq!(report.rolled_back, Vec::<String>::new());
    assert!(!eu.saw("ROLLBACK PREPARED"));
    assert!(eu.saw("COMMIT PREPARED"));
}

#[tokio::test]
async fn recovery_ignores_prepared_transactions_belonging_to_another_coordinator() {
    // Another proxy, or another system entirely, sharing the database.
    // Rolling back somebody else's prepared transaction would destroy data
    // this coordinator knows nothing about.
    let eu = backend_with(BackendConfig::new("eu-west")).await;
    let pools = BTreeMap::from([("eu-west".to_owned(), pool_for(&eu))]);
    let _ = coordinator(Arc::new(MemoryLog::new()))
        .recover(&pools)
        .await
        .expect("recover");

    // The scan is scoped to this coordinator's own identifier prefix.
    assert!(eu.saw("gid LIKE 'luxbyte-eu-proxy-1-%'"));
}

#[tokio::test]
async fn a_clean_start_finds_nothing_to_do() {
    let eu = backend("eu-west").await;
    let pools = BTreeMap::from([("eu-west".to_owned(), pool_for(&eu))]);
    let report = coordinator(Arc::new(MemoryLog::new()))
        .recover(&pools)
        .await
        .expect("recover");
    assert!(report.is_clean());
}
