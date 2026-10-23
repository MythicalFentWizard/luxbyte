//! Connection pooling for backend PostgreSQL servers.
//!
//! A geo-router opens connections to every region it talks to, so pooling is
//! not an optimisation here but a requirement: without it, a query to a
//! distant region pays a TCP handshake, a TLS handshake and an
//! authentication round trip across the same ocean the routing exists to
//! avoid crossing.
//!
//! Pools are keyed by `(endpoint, database, role)`, because a PostgreSQL
//! connection carries all three. Handing a connection opened as one role to
//! a caller acting as another would be a privilege escalation rather than a
//! saving.
//!
//! # The rule that matters
//!
//! A connection goes back to the pool **only when the server has reported
//! it idle**. `ReadyForQuery` carries the transaction state on every
//! response, and anything other than idle means the connection still holds
//! a transaction — with its locks and its snapshot — which the next
//! borrower would inherit. Those connections are closed instead.

pub mod cancel;
pub mod connection;
pub mod error;
pub mod pipeline;
pub mod pool;

pub use cancel::{CancelScope, CancelTarget, send_cancel};
pub use connection::{Connection, ConnectionTarget, Credentials, QueryResult};
pub use error::PoolError;
pub use pipeline::{Step, StepKind, StepReply};
pub use pool::{Pool, PoolConfig, PoolSet, PoolStats, PooledConnection};
