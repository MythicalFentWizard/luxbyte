//! Connection pooling for backend PostgreSQL servers.
//!
//! Not yet implemented. The planner in `meridian-core` decides which region
//! a statement belongs to; this crate will hold the per-endpoint pools that
//! hand it a connection there, keyed by endpoint, database and role, and
//! returning a connection only when `ReadyForQuery` reports it idle.
