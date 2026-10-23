//! Two-phase commit across regions.
//!
//! Not yet implemented. `Plan::Distributed` already identifies the
//! statements that need it; this crate will drive `PREPARE TRANSACTION`,
//! `COMMIT PREPARED` and `ROLLBACK PREPARED` across the regions involved,
//! with a durable log so that a proxy restart between prepare and commit
//! resolves rather than abandons the transaction.
