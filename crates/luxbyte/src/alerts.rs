//! Conditions worth waking someone for.
//!
//! Each is a fact the proxy already knows, stated once, rather than a
//! threshold over a counter: a backend is down, a replica is too far behind
//! to serve reads, a server plays a role the topology does not give it, a
//! two-phase commit is decided but not acknowledged everywhere, a move has
//! committed but a region has not confirmed it. They are served as
//! `luxbyte_alert` series, one per condition and subject, and by
//! `SHOW ALERTS`.

use luxbyte_core::directory::{KeyState, RelocationPhase};

use crate::health::State;
use crate::session::Shared;

/// One condition, about one thing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Alert {
    /// The condition.
    pub name: &'static str,
    /// What it is about: an endpoint, a transaction, a key.
    pub subject: String,
    /// What is known about it.
    pub detail: String,
}

/// Everything wrong right now.
#[must_use]
pub fn current(shared: &Shared) -> Vec<Alert> {
    let mut alerts = Vec::new();
    let topology = shared.router.topology();
    let health = &shared.health;
    for status in health.snapshot() {
        if status.state == State::Down {
            alerts.push(Alert {
                name: "endpoint_down",
                subject: status.endpoint.clone(),
                detail: status
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "marked down".to_owned()),
            });
        }
        if !status.role_matches() {
            alerts.push(Alert {
                name: "role_mismatch",
                subject: status.endpoint.clone(),
                detail: if status.replica {
                    "configured as a replica, but not in recovery".to_owned()
                } else {
                    "configured as a primary, but in recovery".to_owned()
                },
            });
        }
        if status.replica && !status.drained {
            let lag = topology.region(&status.region).and_then(|region| {
                let replica = region
                    .replicas
                    .iter()
                    .find(|replica| crate::health::key(replica) == status.endpoint)?;
                health.lag(region, replica)
            });
            if let Some(lag) = lag.filter(|lag| *lag > health.max_replica_lag_bytes()) {
                alerts.push(Alert {
                    name: "replica_lagging",
                    subject: status.endpoint.clone(),
                    detail: format!("{lag} bytes behind its primary; serving no reads"),
                });
            }
        }
    }
    for record in shared.coordinator.log().pending().unwrap_or_default() {
        alerts.push(Alert {
            name: "transaction_in_doubt",
            subject: record.gid.clone(),
            detail: format!(
                "decided to commit {:?} ago, not yet acknowledged by all of {}",
                record.in_doubt_for(),
                record.participants.join(", ")
            ),
        });
    }
    for (table, key, state) in shared.directory.snapshot() {
        if let KeyState::Relocating {
            to,
            phase: RelocationPhase::Committing,
            ..
        } = state
        {
            alerts.push(Alert {
                name: "relocation_unconfirmed",
                subject: format!("{table} {key}"),
                detail: format!("moving to {to}; the key waits until every region confirms"),
            });
        }
    }
    alerts
}
