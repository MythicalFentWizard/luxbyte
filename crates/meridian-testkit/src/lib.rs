//! An in-process PostgreSQL backend for tests.
//!
//! Not yet implemented. It will speak the real wire protocol over an
//! in-memory duplex stream, so the proxy can be tested end to end with
//! `cargo test` and no database, no Docker and no network.
