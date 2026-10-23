//! Accepting clients.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use luxbyte_core::directory::Directory;
use luxbyte_core::{MemoryDirectory, Router};
use luxbyte_pool::PoolSet;
use luxbyte_sql::AnalysisCache;
use luxbyte_txn::{CommitLog, Coordinator, FileLog, MemoryLog, Recovery};
use tokio::net::TcpListener;

use crate::auth::{Authenticator, Method};
use crate::config::Config;
use crate::session::{Session, Shared};

/// Why the server could not start.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ServerError {
    /// The listening socket could not be opened.
    #[error("could not listen on {address}: {source}")]
    Listen {
        /// The address that was tried.
        address: String,
        /// Why it failed.
        source: std::io::Error,
    },
    /// The commit log could not be opened.
    #[error("could not open the commit log: {0}")]
    CommitLog(#[from] luxbyte_txn::TxnError),
    /// The `[auth]` section is invalid.
    #[error("{0}")]
    Auth(#[from] crate::auth::AuthError),
    /// The proxy would accept anyone, on an address other machines reach.
    #[error(
        "{address} is reachable from other machines and no client authentication is \
         configured; add [[auth.users]] or auth.query, or set auth.method = \"trust\" to \
         accept anyone on purpose"
    )]
    Unprotected {
        /// The listen address.
        address: String,
    },
}

/// Whether only this machine can reach `address`.
fn is_loopback(address: &str) -> bool {
    if let Ok(socket) = address.parse::<SocketAddr>() {
        return socket.ip().is_loopback();
    }
    address
        .rsplit_once(':')
        .is_some_and(|(host, _)| host.eq_ignore_ascii_case("localhost"))
}

/// A bound, not yet running, proxy.
#[derive(Debug)]
pub struct Server {
    shared: Arc<Shared>,
    listener: TcpListener,
}

impl Server {
    /// Binds the listener and prepares everything the sessions share.
    pub async fn bind(config: &Config) -> Result<Self, ServerError> {
        let auth = Authenticator::new(&config.auth)?;
        // Accepting anyone is a choice to be made out loud. Left as the
        // default, it is allowed only where nobody else can connect.
        if auth.method() == Method::Trust
            && config.auth.method.is_none()
            && !is_loopback(&config.server.listen)
        {
            return Err(ServerError::Unprotected {
                address: config.server.listen.clone(),
            });
        }

        let directory = Arc::new(MemoryDirectory::new());
        for entry in &config.directory {
            directory.place(&entry.table, &entry.key, &entry.region);
        }

        let log: Arc<dyn CommitLog> = match &config.server.commit_log {
            Some(path) => Arc::new(FileLog::open(path)?),
            // Without a durable log a crash between prepare and commit
            // leaves prepared transactions pinned on the participants.
            None => Arc::new(MemoryLog::new()),
        };

        let shared = Arc::new(Shared {
            router: Router::new(
                config.topology.clone(),
                Arc::clone(&directory) as Arc<dyn Directory>,
            ),
            cache: AnalysisCache::default(),
            pools: PoolSet::new(config.pool.to_pool_config()),
            coordinator: Coordinator::new(&config.server.node, log),
            directory,
            credentials: config.backend.credentials(),
            backend_user: config.backend.user.clone(),
            backend_database: config.backend.database.clone(),
            auth,
            cancels: crate::cancel::CancelRegistry::new(),
        });

        let listener = TcpListener::bind(&config.server.listen)
            .await
            .map_err(|source| ServerError::Listen {
                address: config.server.listen.clone(),
                source,
            })?;

        Ok(Self { shared, listener })
    }

    /// Where the proxy is listening.
    ///
    /// Resolved from the socket rather than the configuration, so a test
    /// can bind port 0 and find out what it got.
    pub fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.listener.local_addr()
    }

    /// What the sessions share.
    #[must_use]
    pub fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }

    /// Finishes whatever a previous run left in doubt.
    ///
    /// Run before accepting clients. A prepared transaction nobody resolves
    /// holds its locks and pins the transaction horizon, so `VACUUM` stops
    /// reclaiming across the whole database — starting to serve traffic
    /// while that is outstanding makes it worse, not better.
    pub async fn recover(&self) -> Result<Recovery, luxbyte_txn::TxnError> {
        let mut pools = BTreeMap::new();
        for region in self.shared.router.topology().regions.clone() {
            if let Some(pool) = self
                .shared
                .pool_for(&region.id, luxbyte_sql::Access::ReadWrite, 0)
            {
                pools.insert(region.id, pool);
            }
        }
        self.shared.coordinator.recover(&pools).await
    }

    /// Serves until the listener fails.
    pub async fn serve(self) {
        loop {
            let (stream, peer) = match self.listener.accept().await {
                Ok(accepted) => accepted,
                Err(error) => {
                    tracing::error!(%error, "could not accept a connection");
                    return;
                }
            };
            let shared = Arc::clone(&self.shared);
            tokio::spawn(async move {
                if let Err(error) = Session::run(shared, stream).await {
                    // One client's failure is not the server's: log it and
                    // carry on.
                    tracing::debug!(%peer, %error, "session ended");
                }
            });
        }
    }

    /// Serves in the background, returning a handle.
    pub fn spawn(self) -> Result<ServerHandle, std::io::Error> {
        let addr = self.local_addr()?;
        let shared = Arc::clone(&self.shared);
        let task = tokio::spawn(self.serve());
        Ok(ServerHandle { addr, shared, task })
    }
}

/// A running proxy.
///
/// Dropping it stops the listener, so a test cannot leak a server into the
/// next one.
#[derive(Debug)]
pub struct ServerHandle {
    addr: SocketAddr,
    shared: Arc<Shared>,
    task: tokio::task::JoinHandle<()>,
}

impl ServerHandle {
    /// Where it is listening.
    #[must_use]
    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Its port.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.addr.port()
    }

    /// What the sessions share, for assertions in tests.
    #[must_use]
    pub fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_loopback_addresses_count_as_private() {
        assert!(is_loopback("127.0.0.1:6432"));
        assert!(is_loopback("[::1]:6432"));
        assert!(is_loopback("localhost:6432"));
        assert!(!is_loopback("0.0.0.0:6432"));
        assert!(!is_loopback("10.0.0.5:6432"));
        assert!(!is_loopback("proxy.internal:6432"));
    }
}
