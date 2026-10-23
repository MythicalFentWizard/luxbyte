//! PostgreSQL frontend/backend protocol, version 3.0.
//!
//! The crate is built around one observation: a proxy forwards far more bytes
//! than it inspects. A statement's parameters, its result rows and its `COPY`
//! streams are none of the proxy's business, and decoding them into owned Rust
//! values only to re-encode them wastes both time and allocations.
//!
//! So the unit of transport here is [`RawMessage`] — a tag byte plus an opaque
//! body — and typed decoding is a *view* over that body: borrowed,
//! allocation-free, and provided only for the handful of messages that routing
//! actually depends on. Everything else is relayed untouched.
//!
//! # Layout
//!
//! - [`buf`] — bounds-checked big-endian readers and writers.
//! - [`RawMessage`] and [`Framed`] — framing over any async byte stream.
//! - [`frontend`] — typed views of client-to-server messages.
//! - [`backend`] — typed views and builders for server-to-client messages.
//! - [`startup`] — the untagged handshake messages.
//! - [`auth`] — SCRAM-SHA-256, MD5 and cleartext, in both directions.

pub mod auth;
pub mod backend;
pub mod buf;
pub mod error;
pub mod frontend;
pub mod startup;
pub mod stream;
pub mod types;

mod framed;
mod raw;

pub use error::{Peer, ProtocolError};
pub use framed::Framed;
pub use raw::{RawMessage, tag};
pub use stream::Stream;

/// The protocol version this crate implements, in the wire's packed form:
/// major 3 in the high 16 bits, minor 0 in the low 16.
pub const PROTOCOL_VERSION_3: i32 = 196_608;

/// The largest message body this crate will accept before giving up.
///
/// PostgreSQL itself caps messages at 1 GiB. Accepting that much from a peer
/// that has not authenticated yet is a memory-exhaustion invitation, so the
/// default here is deliberately smaller; raise it only if you genuinely move
/// large `COPY` payloads through a single message.
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
