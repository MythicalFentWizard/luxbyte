//! Conditions worth waking someone for.
//!
//! Each is a fact the proxy already knows, stated once, rather than a
//! threshold over a counter: a backend is down, a replica is too far behind
//! to serve reads, a server plays a role the topology does not give it, a
//! two-phase commit is decided but not acknowledged everywhere, a move has
//! committed but a region has not confirmed it, the shared directory cannot
//! be reached, another proxy's move has held a key for a long time. They are served as
//! `luxbyte_alert` series, one per condition and subject, and by
//! `SHOW ALERTS`.

use luxbyte_core::directory::{KeyState, RelocationPhase};

use crate::health::State;
use crate::session::Shared;

/// How long another proxy's move may hold a key before it is worth a look:
/// a move that takes this long has usually lost its mover.
const STALLED: std::time::Duration = std::time::Duration::from_secs(60);

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
    if let Some(peers) = &shared.peers {
        if peers.fenced() {
            alerts.push(Alert {
                name: "directory_unreachable",
                subject: peers.authority().to_owned(),
                detail: peers.fence_refusal().message,
            });
        }
        for (key, to, _, held) in peers.following() {
            if held > STALLED {
                alerts.push(Alert {
                    name: "move_stalled",
                    subject: key,
                    detail: format!(
                        "another proxy has been moving it to {to} for {} s; writes to it wait \
                         until the move is withdrawn",
                        held.as_secs()
                    ),
                });
            }
        }
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
