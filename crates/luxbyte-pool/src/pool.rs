//! Per-endpoint connection pooling.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::connection::{Connection, ConnectionTarget, Credentials, QueryResult};
use crate::error::PoolError;
use crate::pipeline::{Step, StepReply};

/// How a pool behaves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolConfig {
    /// Most connections to hold open at once.
    pub max_size: usize,
    /// How long to wait for one to become free.
    pub acquire_timeout: Duration,
    /// How long to wait for a new connection to open.
    pub connect_timeout: Duration,
    /// Retire a connection that has sat unused for longer than this.
    pub idle_timeout: Duration,
    /// Retire a connection older than this, however busy it has been.
    ///
    /// Bounds the damage from anything that accumulates per connection on
    /// the server — temporary tables, prepared statements, memory in a leaky
    /// extension — and gives a failed-over primary a point at which old
    /// connections stop being used.
    pub max_lifetime: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: 16,
            acquire_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(300),
            max_lifetime: Duration::from_secs(1_800),
        }
    }
}

/// What a pool has been doing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolStats {
    /// Connections opened since the pool was created.
    pub opened: u64,
    /// Connections closed, for any reason.
    pub closed: u64,
    /// Borrows served from the idle list rather than by connecting.
    pub reused: u64,
    /// Borrows that gave up waiting.
    pub timeouts: u64,
    /// Connections retired because they came back unusable.
    pub discarded: u64,
    /// Connections currently idle in the pool.
    pub idle: usize,
}

#[derive(Debug, Default)]
struct Counters {
    opened: AtomicU64,
    closed: AtomicU64,
    reused: AtomicU64,
    timeouts: AtomicU64,
    discarded: AtomicU64,
}

#[derive(Debug)]
struct Shared {
    target: ConnectionTarget,
    credentials: Credentials,
    config: PoolConfig,
    idle: Mutex<Vec<Connection>>,
    permits: Arc<Semaphore>,
    counters: Counters,
}

impl Shared {
    /// Takes a connection from the idle list, discarding any that have
    /// aged out.
    fn take_idle(&self) -> Option<Connection> {
        let mut idle = self.idle.lock();
        while let Some(mut connection) = idle.pop() {
            let expired = connection.idle_for() > self.config.idle_timeout
                || connection.age() > self.config.max_lifetime;
            if expired || !connection.is_reusable() {
                self.counters.closed.fetch_add(1, Ordering::Relaxed);
                self.counters.discarded.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            connection.mark_returned();
            return Some(connection);
        }
        None
    }

    /// Puts a connection back, or retires it.
    fn give_back(&self, mut connection: Connection) {
        // Only an idle connection goes back. One still in a transaction
        // would hand its locks and its snapshot to the next borrower.
        if !connection.is_reusable() || connection.age() > self.config.max_lifetime {
            self.counters.closed.fetch_add(1, Ordering::Relaxed);
            self.counters.discarded.fetch_add(1, Ordering::Relaxed);
            return;
        }
        connection.mark_returned();
        self.idle.lock().push(connection);
    }
}

/// A pool of connections to one backend, database and role.
#[derive(Debug, Clone)]
pub struct Pool {
    shared: Arc<Shared>,
}

impl Pool {
    /// Builds a pool. No connection is opened until one is borrowed.
    #[must_use]
    pub fn new(target: ConnectionTarget, credentials: Credentials, config: PoolConfig) -> Self {
        let permits = Arc::new(Semaphore::new(config.max_size.max(1)));
        Self {
            shared: Arc::new(Shared {
                target,
                credentials,
                config,
                idle: Mutex::new(Vec::new()),
                permits,
                counters: Counters::default(),
            }),
        }
    }

    /// Which backend this pool serves.
    #[must_use]
    pub fn target(&self) -> &ConnectionTarget {
        &self.shared.target
    }

    /// Current statistics.
    #[must_use]
    pub fn stats(&self) -> PoolStats {
        let counters = &self.shared.counters;
        PoolStats {
            opened: counters.opened.load(Ordering::Relaxed),
            closed: counters.closed.load(Ordering::Relaxed),
            reused: counters.reused.load(Ordering::Relaxed),
            timeouts: counters.timeouts.load(Ordering::Relaxed),
            discarded: counters.discarded.load(Ordering::Relaxed),
            idle: self.shared.idle.lock().len(),
        }
    }

    /// Borrows a connection, opening one if the pool is below capacity.
    ///
    /// The permit is taken before the idle list is consulted, so the pool
    /// can never exceed `max_size` connections however many callers arrive
    /// at once.
    pub async fn acquire(&self) -> Result<PooledConnection, PoolError> {
        let permits = Arc::clone(&self.shared.permits);
        let permit =
            tokio::time::timeout(self.shared.config.acquire_timeout, permits.acquire_owned())
                .await
                .map_err(|_| {
                    self.shared
                        .counters
                        .timeouts
                        .fetch_add(1, Ordering::Relaxed);
                    PoolError::AcquireTimeout {
                        target: self.shared.target.to_string(),
                        after: self.shared.config.acquire_timeout,
                    }
                })?
                .map_err(|_| PoolError::Closed)?;

        if let Some(connection) = self.shared.take_idle() {
            self.shared.counters.reused.fetch_add(1, Ordering::Relaxed);
            return Ok(PooledConnection {
                shared: Arc::clone(&self.shared),
                connection: Some(connection),
                permit: Some(permit),
                from_pool: true,
            });
        }

        let connection = Connection::connect(
            &self.shared.target,
            &self.shared.credentials,
            self.shared.config.connect_timeout,
        )
        .await?;
        self.shared.counters.opened.fetch_add(1, Ordering::Relaxed);
        Ok(PooledConnection {
            shared: Arc::clone(&self.shared),
            connection: Some(connection),
            permit: Some(permit),
            from_pool: false,
        })
    }

    /// Borrows a connection and runs one statement on it.
    pub async fn simple_query(&self, sql: &str) -> Result<QueryResult, PoolError> {
        let mut connection = self.acquire().await?;
        connection.simple_query(sql).await
    }

    /// Runs a statement that is safe to run twice, retrying once if the
    /// connection turns out to have died while it was idle.
    ///
    /// A pooled connection can be closed by the server — a restart, an idle
    /// timeout, a firewall — and the pool only finds out when it tries to
    /// use it. Retrying hides that, but **only** for a statement where
    /// running it twice is harmless, because a connection that dies after
    /// the server received the statement is indistinguishable from one that
    /// died before. That ambiguity is why this is a separate method with an
    /// awkward name rather than the default behaviour of
    /// [`Pool::simple_query`].
    ///
    /// Never use it for a write, and never for `COMMIT PREPARED`.
    pub async fn simple_query_idempotent(&self, sql: &str) -> Result<QueryResult, PoolError> {
        let mut connection = self.acquire().await?;
        let reused = connection.came_from_pool();
        match connection.simple_query(sql).await {
            Err(error) if reused && error.connection_lost() => {
                drop(connection);
                let mut fresh = self.acquire().await?;
                fresh.simple_query(sql).await
            }
            other => other,
        }
    }

    /// Closes every idle connection.
    ///
    /// Connections currently borrowed are not touched; they are retired
    /// when they come back.
    pub async fn drain(&self) {
        let connections = std::mem::take(&mut *self.shared.idle.lock());
        self.shared
            .counters
            .closed
            .fetch_add(connections.len() as u64, Ordering::Relaxed);
        for connection in connections {
            connection.close().await;
        }
    }
}

/// A borrowed connection, returned to the pool when dropped.
#[derive(Debug)]
pub struct PooledConnection {
    shared: Arc<Shared>,
    connection: Option<Connection>,
    permit: Option<OwnedSemaphorePermit>,
    from_pool: bool,
}

impl PooledConnection {
    /// Whether this came from the idle list rather than being opened now.
    #[must_use]
    pub const fn came_from_pool(&self) -> bool {
        self.from_pool
    }

    /// Runs a simple query.
    pub async fn simple_query(&mut self, sql: &str) -> Result<QueryResult, PoolError> {
        let connection = self.connection.as_mut().ok_or(PoolError::Closed)?;
        connection.simple_query(sql).await
    }

    /// Sends extended-query messages; see [`Connection::pipeline`].
    pub async fn pipeline(&mut self, steps: &[Step]) -> Result<Vec<StepReply>, PoolError> {
        let connection = self.connection.as_mut().ok_or(PoolError::Closed)?;
        connection.pipeline(steps).await
    }

    /// Which backend this connection goes to.
    #[must_use]
    pub fn target(&self) -> &ConnectionTarget {
        &self.shared.target
    }

    /// The underlying connection.
    #[must_use]
    pub fn connection(&self) -> Option<&Connection> {
        self.connection.as_ref()
    }

    /// Marks the connection unusable so it is retired rather than reused.
    pub fn poison(&mut self) {
        if let Some(connection) = self.connection.as_mut() {
            connection.poison();
        }
    }

    /// Takes the connection out of the pool's care entirely.
    ///
    /// Used by the commit coordinator, which holds a connection across a
    /// prepare and a commit and must not have it handed to anyone else in
    /// between.
    pub fn detach(mut self) -> Option<Connection> {
        self.connection.take()
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            self.shared.give_back(connection);
        }
        // Dropped after the connection is back, so a waiter that is woken
        // by the permit always finds it.
        drop(self.permit.take());
    }
}

/// Every pool, keyed by backend, database and role.
///
/// Pools are per `(endpoint, database, user)` because a connection carries
/// all three: handing a connection opened as one role to a caller acting as
/// another is a privilege escalation, not an optimisation.
#[derive(Debug, Default)]
pub struct PoolSet {
    config: PoolConfig,
    pools: Mutex<BTreeMap<ConnectionTarget, Pool>>,
}

impl PoolSet {
    /// An empty set.
    #[must_use]
    pub fn new(config: PoolConfig) -> Self {
        Self {
            config,
            pools: Mutex::new(BTreeMap::new()),
        }
    }

    /// The pool for a target, created on first use.
    ///
    /// A pool whose credentials no longer match is replaced. That happens
    /// when a role's password changes and its clients pass their keys
    /// through: the old pool's connections stay valid until they are
    /// retired, but it could never open another one.
    #[must_use]
    pub fn pool(&self, target: &ConnectionTarget, credentials: &Credentials) -> Pool {
        let mut pools = self.pools.lock();
        if let Some(existing) = pools.get(target) {
            if &existing.shared.credentials == credentials {
                return existing.clone();
            }
        }
        let pool = Pool::new(target.clone(), credentials.clone(), self.config.clone());
        pools.insert(target.clone(), pool.clone());
        pool
    }

    /// How many distinct pools exist.
    #[must_use]
    pub fn len(&self) -> usize {
        self.pools.lock().len()
    }

    /// Whether any pool exists.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Statistics for every pool.
    #[must_use]
    pub fn stats(&self) -> Vec<(ConnectionTarget, PoolStats)> {
        self.pools
            .lock()
            .iter()
            .map(|(target, pool)| (target.clone(), pool.stats()))
            .collect()
    }
}
