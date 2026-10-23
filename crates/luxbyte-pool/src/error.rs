//! What can go wrong borrowing or using a backend connection.

use std::time::Duration;

use luxbyte_proto::ProtocolError;

/// A pooling or connection failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PoolError {
    /// The socket failed.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// The protocol was violated, or the peer hung up.
    #[error("protocol error: {0}")]
    Protocol(ProtocolError),

    /// The connection could not be opened.
    #[error("could not connect to {target}: {source}")]
    Connect {
        /// Which backend.
        target: String,
        /// Why.
        source: std::io::Error,
    },

    /// The connection did not open in time.
    #[error("connecting to {target} timed out after {after:?}")]
    ConnectTimeout {
        /// Which backend.
        target: String,
        /// How long was allowed.
        after: Duration,
    },

    /// No connection became available in time.
    ///
    /// The pool is at capacity and everyone holding a connection is still
    /// using it. Raising `max_size` treats the symptom; usually the cause is
    /// a caller keeping a connection across an await it did not need to.
    #[error("no connection to {target} became available within {after:?}")]
    AcquireTimeout {
        /// Which backend.
        target: String,
        /// How long was allowed.
        after: Duration,
    },

    /// Authentication failed.
    #[error("authentication failed: {0}")]
    Auth(String),

    /// The server reported an error.
    #[error("{severity} {code}: {message}")]
    Server {
        /// `ERROR`, `FATAL` or `PANIC`.
        severity: String,
        /// The five-character SQLSTATE.
        code: String,
        /// The primary message.
        message: String,
    },

    /// The pool has been shut down.
    #[error("the pool is closed")]
    Closed,
}

impl From<ProtocolError> for PoolError {
    fn from(error: ProtocolError) -> Self {
        match error {
            // A server error is not a protocol failure; it is the server
            // doing its job. Keeping them apart matters because one means
            // "retry elsewhere" and the other means "this statement is
            // wrong".
            ProtocolError::Server {
                severity,
                code,
                message,
            } => Self::Server {
                severity,
                code,
                message,
            },
            ProtocolError::Auth(reason) => Self::Auth(reason),
            other => Self::Protocol(other),
        }
    }
}

impl PoolError {
    /// Whether the connection this happened on is now unusable.
    #[must_use]
    pub const fn connection_lost(&self) -> bool {
        match self {
            Self::Io(_) | Self::Connect { .. } | Self::ConnectTimeout { .. } => true,
            Self::Protocol(error) => error.is_fatal(),
            _ => false,
        }
    }

    /// The SQLSTATE, when the server supplied one.
    #[must_use]
    pub fn sqlstate(&self) -> Option<&str> {
        match self {
            Self::Server { code, .. } => Some(code),
            _ => None,
        }
    }

    /// Whether the server said the transaction may simply be retried.
    ///
    /// Class 40 is PostgreSQL's transaction-rollback class: serialization
    /// failures and deadlocks, both of which succeed on a second attempt.
    #[must_use]
    pub fn retryable(&self) -> bool {
        self.sqlstate().is_some_and(|code| code.starts_with("40"))
    }
}
