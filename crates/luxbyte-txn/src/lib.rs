//! Two-phase commit across regions.
//!
//! When a statement writes to more than one region, there is no single
//! server that can make it atomic. Two-phase commit is how the proxy does
//! it anyway: ask every participant whether it *can* commit, and only once
//! all of them have said yes, tell them to.
//!
//! The protocol is three steps and the order of them is the whole thing:
//!
//! 1. `PREPARE TRANSACTION '<gid>'` on every participant. Any refusal and
//!    the transaction is rolled back everywhere. Nothing has committed, so
//!    nothing is lost.
//! 2. **Write the decision to the commit log**, durably. This is the point
//!    of no return.
//! 3. `COMMIT PREPARED '<gid>'` on every participant. Failures here delay
//!    the transaction; they cannot undo it.
//!
//! A coordinator that crashes between 2 and 3 comes back, reads the log and
//! finishes. One that crashes between 1 and 2 comes back, finds no record,
//! and rolls back. Both are correct; which one happens is decided entirely
//! by whether that one write completed.
//!
//! # Why recovery is not optional
//!
//! A prepared transaction that is never resolved is worse than a slow one.
//! PostgreSQL keeps its locks and holds back the transaction horizon, so
//! `VACUUM` stops reclaiming dead rows — not just in that table, but across
//! the database. An abandoned prepared transaction is a slow outage.
//!
//! So [`Coordinator::recover`] does two things: it finishes the
//! transactions the log says were decided, and it rolls back prepared
//! transactions of its own that the log has no record of, which are the
//! ones that were never decided at all.
//!
//! # The single-participant case
//!
//! Most transactions touch one region, and with one participant there is
//! nobody to disagree with. Those commit in a single phase, with no prepare,
//! no log write and no window in which they are in doubt.

pub mod coordinator;
pub mod error;
pub mod log;

pub use coordinator::{CommitOutcome, Coordinator, GID_PREFIX, Recovery, Transaction};
pub use error::TxnError;
pub use log::{CommitLog, CommitRecord, FileLog, MemoryLog};
