//! What can go wrong committing across regions.

use luxbyte_pool::PoolError;

/// A distributed transaction failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TxnError {
    /// A backend connection failed.
    #[error("backend error: {0}")]
    Pool(#[from] PoolError),

    /// The commit log could not be read or written.
    ///
    /// Fatal on the write path by design: if the decision cannot be made
    /// durable, the transaction must not be committed, because a crash
    /// would then leave participants disagreeing with no record of who was
    /// right.
    #[error("could not {action} in the commit log: {source}")]
    Log {
        /// What was being attempted.
        action: &'static str,
        /// The underlying failure.
        source: std::io::Error,
    },

    /// A commit record could not be encoded or decoded.
    #[error("malformed commit record: {reason}")]
    Encode {
        /// What was wrong with it.
        reason: String,
    },

    /// The blocking task running a log operation did not finish.
    #[error("the commit log task failed to run")]
    LogTaskFailed,

    /// A statement named a region not enlisted in this transaction.
    #[error("region {0} is not part of this transaction")]
    UnknownRegion(String),

    /// The transaction had already finished.
    #[error("the transaction is already {0}")]
    AlreadyFinished(&'static str),

    /// A participant refused to prepare, so the whole transaction aborted.
    #[error("{region} could not prepare, so the transaction was rolled back: {source}")]
    PrepareFailed {
        /// Which region refused.
        region: String,
        /// Why.
        source: Box<PoolError>,
    },
}

impl TxnError {
    /// Whether the client can simply try the whole transaction again.
    #[must_use]
    pub fn retryable(&self) -> bool {
        match self {
            // Nothing was committed anywhere, so a retry is clean.
            Self::PrepareFailed { .. } => true,
            Self::Pool(error) => error.retryable(),
            _ => false,
        }
    }
}
