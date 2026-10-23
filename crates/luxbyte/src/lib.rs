//! A geo-distributed PostgreSQL proxy.
//!
//! The library half of the `luxbyte` binary: configuration, the client
//! session, result merging, statement splitting and the operator console.
//! It is a library so the proxy can be driven from an integration test
//! without a process boundary.

pub mod admin;
pub mod alerts;
pub mod auth;
pub mod cancel;
pub mod config;
pub mod console;
pub mod explain;
pub mod health;
pub mod merge;
pub mod metrics;
pub mod peers;
pub mod relocate;
pub mod server;
pub mod session;
pub mod split;
pub mod tls;

pub use config::{Config, ConfigError};
pub use server::{Server, ServerHandle};
