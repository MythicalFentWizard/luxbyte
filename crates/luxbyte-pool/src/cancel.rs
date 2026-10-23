//! Cancelling what a client's connections are running.
//!
//! A `CancelRequest` names a backend process, so a proxy has to know which
//! backend connections one client is using at the moment the request
//! arrives. Through Luxbyte that can be several: a scatter-gather read runs
//! in every region at once, and a distributed transaction holds a connection
//! in each participant. A [`CancelScope`] tracks all of them, and cancelling
//! it reaches every one.
//!
//! # The race
//!
//! A cancel is a signal sent from a separate connection, so it can arrive
//! after the query it was meant for has finished. Against PostgreSQL directly
//! that is harmless. Through a pool it is not: by then the backend may be
//! running another client's query, and that is the one that gets cancelled.
//! So every connection a cancel was aimed at is retired rather than returned
//! to the pool. A cancel then either stops this client's work or reaches a
//! connection nobody will use again — never a stranger's query.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use luxbyte_proto::startup::{CancelRequest, encode_cancel};
use parking_lot::Mutex;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::error::PoolError;

/// Where to send the cancel for one backend connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelTarget {
    /// The backend's host.
    pub host: String,
    /// The backend's port.
    pub port: u16,
    /// The key the backend reported in `BackendKeyData`.
    pub key: CancelRequest,
}

#[derive(Debug, Default)]
struct Members {
    next: u64,
    live: HashMap<u64, (CancelTarget, Arc<AtomicBool>)>,
}

/// The backend connections one client is using right now.
#[derive(Debug, Clone, Default)]
pub struct CancelScope {
    members: Arc<Mutex<Members>>,
}

/// One connection's place in a scope, held by the connection.
#[derive(Debug)]
pub(crate) struct Membership {
    scope: CancelScope,
    id: u64,
    targeted: Arc<AtomicBool>,
}

impl Membership {
    /// Leaves the scope. Returns whether a cancel was aimed at the
    /// connection while it was a member.
    ///
    /// Leaving and cancelling take the same lock, so a cancel either sees
    /// the connection and marks it, or does not see it at all.
    pub(crate) fn leave(self) -> bool {
        self.scope.members.lock().live.remove(&self.id);
        self.targeted.load(Ordering::SeqCst)
    }
}

impl CancelScope {
    /// An empty scope.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn join(&self, target: CancelTarget) -> Membership {
        let targeted = Arc::new(AtomicBool::new(false));
        let mut members = self.members.lock();
        let id = members.next;
        members.next = members.next.wrapping_add(1);
        members.live.insert(id, (target, Arc::clone(&targeted)));
        Membership {
            scope: self.clone(),
            id,
            targeted,
        }
    }

    /// How many connections are in the scope.
    #[must_use]
    pub fn len(&self) -> usize {
        self.members.lock().live.len()
    }

    /// Whether no connection is in the scope.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Marks every member for retirement and returns where to send the
    /// cancels.
    #[must_use]
    pub fn targets(&self) -> Vec<CancelTarget> {
        self.members
            .lock()
            .live
            .values()
            .map(|(target, targeted)| {
                targeted.store(true, Ordering::SeqCst);
                target.clone()
            })
            .collect()
    }

    /// Cancels whatever every member is running, at once. Returns how many
    /// cancels were delivered.
    pub async fn cancel(&self, timeout: Duration) -> usize {
        let mut tasks = tokio::task::JoinSet::new();
        for target in self.targets() {
            tasks.spawn(async move { send_cancel(&target, timeout).await });
        }
        let mut delivered = 0_usize;
        while let Some(joined) = tasks.join_next().await {
            if matches!(joined, Ok(Ok(()))) {
                delivered = delivered.saturating_add(1);
            }
        }
        delivered
    }
}

/// Sends one `CancelRequest`.
///
/// The server never answers one; it acts on it and closes the connection.
/// So delivery is all that can be confirmed.
pub async fn send_cancel(target: &CancelTarget, timeout: Duration) -> Result<(), PoolError> {
    let address = format!("{}:{}", target.host, target.port);
    let mut stream = tokio::time::timeout(timeout, TcpStream::connect(&address))
        .await
        .map_err(|_| PoolError::ConnectTimeout {
            target: address.clone(),
            after: timeout,
        })?
        .map_err(|source| PoolError::Connect {
            target: address.clone(),
            source,
        })?;
    stream
        .write_all(&encode_cancel(target.key).map_err(PoolError::from)?)
        .await?;
    stream.shutdown().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(port: u16) -> CancelTarget {
        CancelTarget {
            host: "127.0.0.1".to_owned(),
            port,
            key: CancelRequest {
                process_id: 1,
                secret_key: 2,
            },
        }
    }

    #[test]
    fn a_connection_that_left_before_the_cancel_is_not_marked() {
        let scope = CancelScope::new();
        let early = scope.join(target(1));
        assert!(!early.leave());
        assert!(scope.targets().is_empty());
    }

    #[test]
    fn a_connection_present_at_the_cancel_learns_of_it_when_it_leaves() {
        let scope = CancelScope::new();
        let member = scope.join(target(1));
        let other = scope.join(target(2));
        assert_eq!(scope.targets().len(), 2);
        assert!(member.leave());
        assert!(other.leave());
        assert!(scope.is_empty());
    }
}
