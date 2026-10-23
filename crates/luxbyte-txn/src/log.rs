//! The durable record of a commit decision.
//!
//! Two-phase commit has exactly one moment that matters. Before it, the
//! transaction can still be abandoned and nothing is lost. After it, every
//! participant **must** eventually commit, whatever happens to the machines
//! involved — including the coordinator.
//!
//! That moment is the write to this log. It happens after every participant
//! has answered `PREPARE TRANSACTION` successfully and before any of them is
//! told to commit. A coordinator that crashes one instruction either side of
//! it must come back and reach the same conclusion:
//!
//! - **No record** — nobody has been told to commit, so roll every prepared
//!   transaction back.
//! - **A record** — somebody may already have committed, so commit the rest.
//!
//! Get this backwards and you get the failure two-phase commit exists to
//! prevent: one region committed, another rolled back, and no way to tell
//! which until a customer notices.
//!
//! A prepared transaction that is never resolved is worse than a slow one:
//! PostgreSQL holds its locks and pins the transaction horizon, so `VACUUM`
//! stops reclaiming anywhere in the cluster. Recovery is not optional.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::error::TxnError;

/// The decision to commit one distributed transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRecord {
    /// The identifier given to `PREPARE TRANSACTION` on every participant.
    pub gid: String,
    /// The regions that prepared, and so must now commit.
    pub participants: Vec<String>,
    /// When the decision was taken, as milliseconds since the Unix epoch.
    ///
    /// Only used for reporting how long a transaction has been in doubt,
    /// which is the number an operator actually wants.
    pub decided_at_ms: u64,
}

impl CommitRecord {
    /// A record for `gid` over `participants`, decided now.
    #[must_use]
    pub fn new(gid: &str, participants: &[String]) -> Self {
        Self {
            gid: gid.to_owned(),
            participants: participants.to_vec(),
            decided_at_ms: now_ms(),
        }
    }

    /// How long this transaction has been in doubt.
    #[must_use]
    pub fn in_doubt_for(&self) -> std::time::Duration {
        std::time::Duration::from_millis(now_ms().saturating_sub(self.decided_at_ms))
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Where commit decisions are written.
///
/// Implementations must make [`CommitLog::record`] durable before returning:
/// the whole protocol rests on that one guarantee.
pub trait CommitLog: Send + Sync + std::fmt::Debug {
    /// Durably records a decision to commit.
    fn record(&self, record: &CommitRecord) -> Result<(), TxnError>;

    /// Forgets a decision, once every participant has committed.
    fn forget(&self, gid: &str) -> Result<(), TxnError>;

    /// Every decision not yet forgotten.
    fn pending(&self) -> Result<Vec<CommitRecord>, TxnError>;
}

/// A log that keeps nothing across a restart.
///
/// For tests, and for a deployment that has accepted it cannot recover an
/// in-doubt transaction automatically. Using it in production means a
/// coordinator crash leaves prepared transactions pinned on the
/// participants until somebody resolves them by hand.
#[derive(Debug, Default)]
pub struct MemoryLog {
    records: Mutex<BTreeMap<String, CommitRecord>>,
}

impl MemoryLog {
    /// An empty log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// How many decisions are outstanding.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.lock().len()
    }

    /// Whether anything is outstanding.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl CommitLog for MemoryLog {
    fn record(&self, record: &CommitRecord) -> Result<(), TxnError> {
        self.records
            .lock()
            .insert(record.gid.clone(), record.clone());
        Ok(())
    }

    fn forget(&self, gid: &str) -> Result<(), TxnError> {
        self.records.lock().remove(gid);
        Ok(())
    }

    fn pending(&self) -> Result<Vec<CommitRecord>, TxnError> {
        Ok(self.records.lock().values().cloned().collect())
    }
}

/// A log of one small file per in-doubt transaction.
///
/// A file is created when the decision is taken and removed when every
/// participant has committed, so the directory *is* the list of transactions
/// still in doubt. There is no append-only file to compact and no replay to
/// get wrong: the state is what you can see with `ls`.
///
/// Writes go to a temporary name and are renamed into place after
/// `sync_all`, so a crash during the write leaves either the old state or
/// the new one, never half a record.
#[derive(Debug)]
pub struct FileLog {
    directory: PathBuf,
}

impl FileLog {
    /// Opens, creating the directory if it does not exist.
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, TxnError> {
        let directory = directory.as_ref().to_path_buf();
        std::fs::create_dir_all(&directory).map_err(|source| TxnError::Log {
            action: "create the log directory",
            source,
        })?;
        Ok(Self { directory })
    }

    /// Where the log lives.
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    fn path_for(&self, gid: &str) -> PathBuf {
        self.directory.join(format!("{gid}.json"))
    }
}

impl CommitLog for FileLog {
    fn record(&self, record: &CommitRecord) -> Result<(), TxnError> {
        use std::io::Write as _;

        let encoded = serde_json::to_vec(record).map_err(|error| TxnError::Encode {
            reason: error.to_string(),
        })?;

        let temporary = self
            .directory
            .join(format!("{}.{}.tmp", record.gid, std::process::id()));
        let wrap =
            |action: &'static str| move |source: std::io::Error| TxnError::Log { action, source };

        let mut file = std::fs::File::create(&temporary).map_err(wrap("create the record"))?;
        file.write_all(&encoded).map_err(wrap("write the record"))?;
        // The decision is only real once it has reached the disk. Without
        // this, a power cut between here and `COMMIT PREPARED` loses the
        // decision while the participants keep their prepared transactions.
        file.sync_all().map_err(wrap("flush the record to disk"))?;
        drop(file);

        std::fs::rename(&temporary, self.path_for(&record.gid))
            .map_err(wrap("publish the record"))?;
        Ok(())
    }

    fn forget(&self, gid: &str) -> Result<(), TxnError> {
        match std::fs::remove_file(self.path_for(gid)) {
            Ok(()) => Ok(()),
            // Already gone: forgetting is meant to be idempotent, because
            // recovery and the commit path can both reach it.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(TxnError::Log {
                action: "remove the record",
                source,
            }),
        }
    }

    fn pending(&self) -> Result<Vec<CommitRecord>, TxnError> {
        let entries = std::fs::read_dir(&self.directory).map_err(|source| TxnError::Log {
            action: "read the log directory",
            source,
        })?;

        let mut records = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|source| TxnError::Log {
                action: "read a log entry",
                source,
            })?;
            let path = entry.path();
            if path.extension().is_none_or(|extension| extension != "json") {
                // A leftover temporary file from an interrupted write. It
                // was never renamed, so it was never a decision.
                continue;
            }
            let bytes = std::fs::read(&path).map_err(|source| TxnError::Log {
                action: "read a record",
                source,
            })?;
            let record: CommitRecord =
                serde_json::from_slice(&bytes).map_err(|error| TxnError::Encode {
                    reason: format!("{}: {error}", path.display()),
                })?;
            records.push(record);
        }
        records.sort_by_key(|record| record.decided_at_ms);
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(gid: &str) -> CommitRecord {
        CommitRecord::new(gid, &["eu-west".to_owned(), "us-east".to_owned()])
    }

    fn temp_dir(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("luxbyte-log-test-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        path
    }

    #[test]
    fn a_memory_log_remembers_until_told_to_forget() {
        let log = MemoryLog::new();
        log.record(&record("g1")).expect("record");
        assert_eq!(log.pending().expect("pending").len(), 1);
        log.forget("g1").expect("forget");
        assert!(log.is_empty());
    }

    #[test]
    fn forgetting_something_unknown_is_not_an_error() {
        // Recovery and the commit path can both reach `forget`, so it has
        // to be safe to call twice.
        let log = MemoryLog::new();
        assert!(log.forget("never-existed").is_ok());
    }

    #[test]
    fn a_file_log_survives_being_reopened() {
        let directory = temp_dir("reopen");
        {
            let log = FileLog::open(&directory).expect("open");
            log.record(&record("g1")).expect("record");
            log.record(&record("g2")).expect("record");
        }

        // A new process, reading what the old one left behind.
        let log = FileLog::open(&directory).expect("reopen");
        let pending = log.pending().expect("pending");
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().any(|r| r.gid == "g1"));
        assert_eq!(
            pending.first().map(|r| r.participants.clone()),
            Some(vec!["eu-west".to_owned(), "us-east".to_owned()])
        );

        log.forget("g1").expect("forget");
        let log = FileLog::open(&directory).expect("reopen again");
        assert_eq!(log.pending().expect("pending").len(), 1);
        let _ = std::fs::remove_dir_all(&directory);
    }

    #[test]
    fn an_interrupted_write_is_not_mistaken_for_a_decision() {
        // A crash between `create` and `rename` leaves a `.tmp` file. It
        // was never a decision, and reading it as one would commit a
        // transaction nobody decided to commit.
        let directory = temp_dir("interrupted");
        let log = FileLog::open(&directory).expect("open");
        std::fs::write(directory.join("g9.12345.tmp"), b"{\"not\": \"a record\"}")
            .expect("write a stray temporary");

        assert_eq!(log.pending().expect("pending").len(), 0);
        let _ = std::fs::remove_dir_all(&directory);
    }

    #[test]
    fn records_come_back_oldest_first() {
        let directory = temp_dir("order");
        let log = FileLog::open(&directory).expect("open");
        let mut first = record("old");
        first.decided_at_ms = 1_000;
        let mut second = record("new");
        second.decided_at_ms = 2_000;
        log.record(&second).expect("record");
        log.record(&first).expect("record");

        let pending = log.pending().expect("pending");
        assert_eq!(
            pending.iter().map(|r| r.gid.as_str()).collect::<Vec<_>>(),
            ["old", "new"]
        );
        let _ = std::fs::remove_dir_all(&directory);
    }

    #[test]
    fn in_doubt_duration_is_measured_from_the_decision() {
        let mut record = record("g1");
        record.decided_at_ms = now_ms().saturating_sub(5_000);
        assert!(record.in_doubt_for().as_secs() >= 4);
    }
}
