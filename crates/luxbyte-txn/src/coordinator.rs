//! Driving two-phase commit across regions.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use luxbyte_pool::{CancelScope, Pool, PoolError, PooledConnection, QueryResult};

use crate::error::TxnError;
use crate::log::{CommitLog, CommitRecord};

/// Prefix on every identifier this coordinator issues.
///
/// Recovery uses it to tell its own prepared transactions apart from
/// anybody else's on the same server. Rolling back a prepared transaction
/// that belongs to another system would be a spectacular way to lose
/// somebody else's data.
pub const GID_PREFIX: &str = "luxbyte";

/// SQLSTATE PostgreSQL returns for a prepared transaction that is not there.
///
/// On the commit path this means somebody already committed it, which is
/// success, not failure. Treating it as an error is how recovery gets stuck
/// in a loop.
const UNDEFINED_OBJECT: &str = "42704";

/// How a distributed transaction ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOutcome {
    /// Nothing was enlisted, so there was nothing to commit.
    Empty,
    /// One region was involved, committed in a single phase.
    ///
    /// With one participant there is nobody to disagree with, so the
    /// prepare phase, the log write and the in-doubt window are all pure
    /// cost. This is the common case for a well-partitioned workload and it
    /// is worth the special case.
    SinglePhase {
        /// The region.
        region: String,
    },
    /// Every participant committed.
    Committed {
        /// The regions, in the order they were enlisted.
        regions: Vec<String>,
    },
    /// The decision was committed and recorded, but some participants have
    /// not yet acknowledged it.
    ///
    /// The transaction **is** committed. The remaining participants hold
    /// prepared transactions that recovery will finish. This is reported
    /// rather than hidden because those participants are holding locks
    /// until it happens.
    CommittedPendingAcknowledgement {
        /// Participants that confirmed.
        committed: Vec<String>,
        /// Participants still to confirm.
        unresolved: Vec<String>,
    },
}

impl CommitOutcome {
    /// Whether the transaction is durably committed, whatever remains to be
    /// tidied up.
    #[must_use]
    pub const fn is_committed(&self) -> bool {
        !matches!(self, Self::Empty)
    }

    /// Participants still holding a prepared transaction.
    #[must_use]
    pub fn unresolved(&self) -> &[String] {
        match self {
            Self::CommittedPendingAcknowledgement { unresolved, .. } => unresolved,
            _ => &[],
        }
    }
}

/// What a recovery pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Recovery {
    /// Transactions finished from the log.
    pub committed: Vec<String>,
    /// Transactions still unresolved, because a participant is unreachable.
    pub still_in_doubt: Vec<String>,
    /// Prepared transactions found on a participant with no commit record,
    /// and therefore rolled back.
    pub rolled_back: Vec<String>,
}

impl Recovery {
    /// Whether anything needed doing.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.committed.is_empty() && self.still_in_doubt.is_empty() && self.rolled_back.is_empty()
    }
}

/// Issues identifiers, drives commits, and resolves what a crash left behind.
#[derive(Debug)]
pub struct Coordinator {
    node: String,
    sequence: AtomicU64,
    log: Arc<dyn CommitLog>,
}

impl Coordinator {
    /// A coordinator for one proxy instance.
    ///
    /// `node` must be unique across the deployment: it is what keeps two
    /// proxies from issuing the same identifier and then resolving each
    /// other's transactions.
    #[must_use]
    pub fn new(node: &str, log: Arc<dyn CommitLog>) -> Self {
        Self {
            node: sanitise(node),
            sequence: AtomicU64::new(0),
            log,
        }
    }

    /// This coordinator's node name, as it appears in identifiers.
    #[must_use]
    pub fn node(&self) -> &str {
        &self.node
    }

    /// The commit log in use.
    #[must_use]
    pub fn log(&self) -> &Arc<dyn CommitLog> {
        &self.log
    }

    /// Mints an identifier for a new transaction.
    ///
    /// PostgreSQL allows 200 characters and treats the identifier as
    /// opaque. This one is `luxbyte-<node>-<sequence>-<millis>`: unique per
    /// coordinator, recognisable to recovery, and safe as a file name.
    #[must_use]
    pub fn next_gid(&self) -> String {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_millis());
        let gid = format!("{GID_PREFIX}-{}-{sequence}-{millis}", self.node);
        gid.chars().take(200).collect()
    }

    /// Starts a transaction. Nothing is sent until a region is enlisted.
    #[must_use]
    pub fn begin(&self) -> Transaction {
        Transaction {
            gid: self.next_gid(),
            log: Arc::clone(&self.log),
            participants: Vec::new(),
            finished: None,
            scope: None,
        }
    }

    /// Finishes what a crash left behind.
    ///
    /// Two jobs, and they pull in opposite directions:
    ///
    /// 1. Every transaction in the log was **decided to commit**, so commit
    ///    it everywhere. A participant that no longer has it has already
    ///    done so.
    /// 2. Every prepared transaction of ours on a participant that has
    ///    **no** log record was never decided, so roll it back. These are
    ///    the dangerous ones: PostgreSQL holds their locks and pins the
    ///    transaction horizon, so left alone they stop `VACUUM` reclaiming
    ///    anywhere in the cluster.
    pub async fn recover(&self, pools: &BTreeMap<String, Pool>) -> Result<Recovery, TxnError> {
        let mut report = Recovery::default();
        let pending = self.pending().await?;
        let decided: BTreeSet<String> = pending.iter().map(|r| r.gid.clone()).collect();

        for record in pending {
            let mut unresolved = Vec::new();
            for region in &record.participants {
                let Some(pool) = pools.get(region) else {
                    unresolved.push(region.clone());
                    continue;
                };
                if commit_prepared(pool, &record.gid).await.is_err() {
                    unresolved.push(region.clone());
                }
            }
            if unresolved.is_empty() {
                self.forget(&record.gid).await?;
                report.committed.push(record.gid);
            } else {
                report.still_in_doubt.push(record.gid);
            }
        }

        // Orphans: prepared here, never decided.
        for (region, pool) in pools {
            let Ok(found) = prepared_transactions(pool, &self.node).await else {
                continue;
            };
            for gid in found {
                if decided.contains(&gid) {
                    continue;
                }
                if rollback_prepared(pool, &gid).await.is_ok() {
                    report.rolled_back.push(format!("{gid}@{region}"));
                }
            }
        }

        Ok(report)
    }

    /// Finishes one transaction that was decided to commit, on the
    /// participants that have not yet acknowledged it. Returns whether all
    /// of them now have, in which case its record is gone.
    ///
    /// Safe while other transactions run, unlike [`Coordinator::recover`]:
    /// that also rolls back every prepared transaction it has no record of,
    /// which while sessions are live includes ones another session has
    /// prepared and is about to record.
    pub async fn finish(&self, gid: &str, pools: &[Pool]) -> Result<bool, TxnError> {
        let mut finished = true;
        for pool in pools {
            if commit_prepared(pool, gid).await.is_err() {
                finished = false;
            }
        }
        if finished {
            self.forget(gid).await?;
        }
        Ok(finished)
    }

    async fn pending(&self) -> Result<Vec<CommitRecord>, TxnError> {
        let log = Arc::clone(&self.log);
        tokio::task::spawn_blocking(move || log.pending())
            .await
            .map_err(|_| TxnError::LogTaskFailed)?
    }

    async fn forget(&self, gid: &str) -> Result<(), TxnError> {
        let log = Arc::clone(&self.log);
        let gid = gid.to_owned();
        tokio::task::spawn_blocking(move || log.forget(&gid))
            .await
            .map_err(|_| TxnError::LogTaskFailed)?
    }
}

/// Keeps an identifier to characters that are safe in a file name and in a
/// quoted SQL literal.
fn sanitise(node: &str) -> String {
    let cleaned: String = node
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .take(48)
        .collect();
    if cleaned.is_empty() {
        "node".to_owned()
    } else {
        cleaned
    }
}

struct Participant {
    region: String,
    connection: PooledConnection,
    prepared: bool,
}

/// A transaction spanning one or more regions.
///
/// Dropping one without committing or rolling back returns its connections
/// to the pool, which retires them because they are still inside a
/// transaction — so the work is abandoned rather than silently applied.
pub struct Transaction {
    gid: String,
    log: Arc<dyn CommitLog>,
    participants: Vec<Participant>,
    finished: Option<&'static str>,
    /// Where a client's cancel finds this transaction's connections.
    scope: Option<CancelScope>,
}

// The commit log is deliberately left out: it is shared with every other
// transaction, and printing it here would dump unrelated state into any log
// line that formats one of these.
#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transaction")
            .field("gid", &self.gid)
            .field(
                "participants",
                &self
                    .participants
                    .iter()
                    .map(|p| p.region.as_str())
                    .collect::<Vec<_>>(),
            )
            .field("finished", &self.finished)
            .finish()
    }
}

impl Transaction {
    /// Makes every participant, present and future, reachable by a cancel
    /// on `scope`.
    pub fn cancel_with(&mut self, scope: &CancelScope) {
        for participant in &mut self.participants {
            participant.connection.join(scope);
        }
        self.scope = Some(scope.clone());
    }

    /// The identifier used for `PREPARE TRANSACTION`.
    #[must_use]
    pub fn gid(&self) -> &str {
        &self.gid
    }

    /// The regions enlisted so far.
    #[must_use]
    pub fn regions(&self) -> Vec<&str> {
        self.participants
            .iter()
            .map(|p| p.region.as_str())
            .collect()
    }

    /// How many regions are involved.
    #[must_use]
    pub fn len(&self) -> usize {
        self.participants.len()
    }

    /// Whether no region has been enlisted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.participants.is_empty()
    }

    fn check_open(&self) -> Result<(), TxnError> {
        match self.finished {
            Some(state) => Err(TxnError::AlreadyFinished(state)),
            None => Ok(()),
        }
    }

    /// Brings a region into the transaction and opens one there.
    ///
    /// Enlisting twice is a no-op, so a planner that discovers the same
    /// region twice does not open two transactions on it.
    pub async fn enlist(&mut self, region: &str, pool: &Pool) -> Result<(), TxnError> {
        self.check_open()?;
        if self.participants.iter().any(|p| p.region == region) {
            return Ok(());
        }
        let mut connection = pool.acquire().await?;
        if let Some(scope) = &self.scope {
            connection.join(scope);
        }
        connection.simple_query("BEGIN").await?;
        self.participants.push(Participant {
            region: region.to_owned(),
            connection,
            prepared: false,
        });
        Ok(())
    }

    /// Runs a statement on one participant.
    pub async fn execute(&mut self, region: &str, sql: &str) -> Result<QueryResult, TxnError> {
        self.check_open()?;
        let participant = self
            .participants
            .iter_mut()
            .find(|p| p.region == region)
            .ok_or_else(|| TxnError::UnknownRegion(region.to_owned()))?;
        Ok(participant.connection.simple_query(sql).await?)
    }

    /// The connection a participant's work runs on.
    ///
    /// For callers that speak to the participant directly, such as a relay
    /// of the extended query protocol. Whatever they send is part of this
    /// transaction and is committed or rolled back with it.
    pub fn connection(&mut self, region: &str) -> Result<&mut PooledConnection, TxnError> {
        self.check_open()?;
        self.participants
            .iter_mut()
            .find(|p| p.region == region)
            .map(|participant| &mut participant.connection)
            .ok_or_else(|| TxnError::UnknownRegion(region.to_owned()))
    }

    /// Commits.
    ///
    /// With one participant this is an ordinary `COMMIT`. With more, it is
    /// prepare, record the decision, then commit — and the order of those
    /// three is the entire protocol.
    pub async fn commit(mut self) -> Result<CommitOutcome, TxnError> {
        self.check_open()?;

        match self.participants.len() {
            0 => {
                self.finished = Some("committed");
                Ok(CommitOutcome::Empty)
            }
            1 => {
                let participant = self
                    .participants
                    .first_mut()
                    .ok_or(TxnError::AlreadyFinished("committed"))?;
                participant.connection.simple_query("COMMIT").await?;
                let region = participant.region.clone();
                self.finished = Some("committed");
                Ok(CommitOutcome::SinglePhase { region })
            }
            _ => self.two_phase_commit().await,
        }
    }

    async fn two_phase_commit(&mut self) -> Result<CommitOutcome, TxnError> {
        let prepare = format!("PREPARE TRANSACTION '{}'", self.gid);

        // Phase one. Any refusal and nothing commits anywhere.
        for index in 0..self.participants.len() {
            let Some(participant) = self.participants.get_mut(index) else {
                continue;
            };
            match participant.connection.simple_query(&prepare).await {
                Ok(_) => participant.prepared = true,
                Err(source) => {
                    let region = participant.region.clone();
                    self.abort_all().await;
                    self.finished = Some("rolled back");
                    return Err(TxnError::PrepareFailed {
                        region,
                        source: Box::new(source),
                    });
                }
            }
        }

        // The decision. Everything before this can be abandoned; nothing
        // after it can.
        let regions: Vec<String> = self.participants.iter().map(|p| p.region.clone()).collect();
        let record = CommitRecord::new(&self.gid, &regions);
        let log = Arc::clone(&self.log);
        let to_write = record.clone();
        let recorded = tokio::task::spawn_blocking(move || log.record(&to_write))
            .await
            .map_err(|_| TxnError::LogTaskFailed)
            .and_then(|recorded| recorded);
        if let Err(error) = recorded {
            // No decision, so nothing may commit. Left prepared, the
            // participants would hold their locks until the next recovery.
            self.abort_all().await;
            self.finished = Some("rolled back");
            return Err(error);
        }

        // Phase two. Failures here delay the transaction; they cannot undo
        // it, because the decision is already durable. That includes a
        // client's cancel, which from here on would only leave a committed
        // transaction prepared until the next recovery, so it is no longer
        // allowed to reach the participants.
        for participant in &mut self.participants {
            participant.connection.leave_scope();
        }
        let commit = format!("COMMIT PREPARED '{}'", self.gid);
        let mut committed = Vec::new();
        let mut unresolved = Vec::new();
        for participant in &mut self.participants {
            match participant.connection.simple_query(&commit).await {
                Ok(_) => committed.push(participant.region.clone()),
                Err(error) if already_resolved(&error) => {
                    committed.push(participant.region.clone());
                }
                Err(_) => unresolved.push(participant.region.clone()),
            }
        }

        self.finished = Some("committed");

        if unresolved.is_empty() {
            let log = Arc::clone(&self.log);
            let gid = self.gid.clone();
            // The transaction has committed everywhere, and saying otherwise
            // would have a caller act as if it had not. A record left behind
            // costs recovery one `COMMIT PREPARED` that finds nothing.
            let forgotten = tokio::task::spawn_blocking(move || log.forget(&gid))
                .await
                .map_err(|_| TxnError::LogTaskFailed)
                .and_then(|forgotten| forgotten);
            if let Err(error) = forgotten {
                tracing::warn!(gid = %self.gid, %error, "a committed transaction's record was not removed");
            }
            return Ok(CommitOutcome::Committed { regions });
        }
        // The record stays so recovery finishes the job.
        Ok(CommitOutcome::CommittedPendingAcknowledgement {
            committed,
            unresolved,
        })
    }

    /// Rolls back every participant.
    pub async fn rollback(mut self) -> Result<(), TxnError> {
        self.check_open()?;
        self.abort_all().await;
        self.finished = Some("rolled back");
        Ok(())
    }

    /// Best-effort abort. Errors are swallowed deliberately: a participant
    /// that cannot be reached to roll back is resolved by recovery, and
    /// there is nothing useful the caller could do with the error.
    async fn abort_all(&mut self) {
        let rollback_prepared = format!("ROLLBACK PREPARED '{}'", self.gid);
        for participant in &mut self.participants {
            let statement = if participant.prepared {
                rollback_prepared.as_str()
            } else {
                "ROLLBACK"
            };
            if participant
                .connection
                .simple_query(statement)
                .await
                .is_err()
            {
                // The connection is unusable; make sure it is not pooled.
                participant.connection.poison();
            }
        }
    }
}

/// Whether an error means the prepared transaction is already gone.
fn already_resolved(error: &PoolError) -> bool {
    error.sqlstate() == Some(UNDEFINED_OBJECT)
}

async fn commit_prepared(pool: &Pool, gid: &str) -> Result<(), TxnError> {
    match pool.simple_query(&format!("COMMIT PREPARED '{gid}'")).await {
        Ok(_) => Ok(()),
        // Already committed by an earlier pass, or by another coordinator.
        Err(error) if already_resolved(&error) => Ok(()),
        Err(error) => Err(TxnError::Pool(error)),
    }
}

async fn rollback_prepared(pool: &Pool, gid: &str) -> Result<(), TxnError> {
    match pool
        .simple_query(&format!("ROLLBACK PREPARED '{gid}'"))
        .await
    {
        Ok(_) => Ok(()),
        Err(error) if already_resolved(&error) => Ok(()),
        Err(error) => Err(TxnError::Pool(error)),
    }
}

/// Asks a participant which of our prepared transactions it is holding.
async fn prepared_transactions(pool: &Pool, node: &str) -> Result<Vec<String>, TxnError> {
    let pattern = format!("{GID_PREFIX}-{node}-%");
    let result = pool
        .simple_query(&format!(
            "SELECT gid FROM pg_prepared_xacts WHERE gid LIKE '{pattern}'"
        ))
        .await?;
    Ok(result
        .rows
        .into_iter()
        .filter_map(|row| row.into_iter().next().flatten())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::MemoryLog;

    #[test]
    fn identifiers_are_unique_and_recognisable() {
        let coordinator = Coordinator::new("eu-proxy-1", Arc::new(MemoryLog::new()));
        let first = coordinator.next_gid();
        let second = coordinator.next_gid();
        assert_ne!(first, second);
        assert!(first.starts_with("luxbyte-eu-proxy-1-"));
        assert!(first.len() <= 200);
    }

    #[test]
    fn a_node_name_is_reduced_to_safe_characters() {
        // It becomes both a file name and part of a SQL literal.
        let coordinator =
            Coordinator::new("eu west/1'; DROP TABLE users--", Arc::new(MemoryLog::new()));
        assert!(
            coordinator
                .node()
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-')
        );
        assert!(!coordinator.next_gid().contains('\''));
    }

    #[test]
    fn an_empty_node_name_still_produces_an_identifier() {
        let coordinator = Coordinator::new("", Arc::new(MemoryLog::new()));
        assert_eq!(coordinator.node(), "node");
    }

    #[test]
    fn an_outcome_knows_whether_it_committed() {
        assert!(!CommitOutcome::Empty.is_committed());
        assert!(
            CommitOutcome::SinglePhase {
                region: "eu-west".to_owned()
            }
            .is_committed()
        );
        let pending = CommitOutcome::CommittedPendingAcknowledgement {
            committed: vec!["eu-west".to_owned()],
            unresolved: vec!["us-east".to_owned()],
        };
        assert!(pending.is_committed());
        assert_eq!(pending.unresolved(), ["us-east"]);
    }
}
