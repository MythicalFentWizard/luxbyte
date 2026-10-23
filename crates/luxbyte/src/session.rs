//! One client connection, from startup to disconnect.

use std::sync::Arc;

use luxbyte_core::directory::MemoryDirectory;
use luxbyte_core::topology::Endpoint;
use luxbyte_core::{LocalIntent, ParameterValues, Plan, Refusal, Router, Routing};
use luxbyte_pool::{
    CancelScope, ConnectionTarget, Credentials, Pool, PoolError, PoolSet, QueryResult,
};
use luxbyte_proto::backend::{TransactionStatus, build};
use luxbyte_proto::frontend::Frontend;
use luxbyte_proto::startup::{CancelRequest, SSL_REQUEST_CODE, StartupRequest};
use luxbyte_proto::{Framed, Peer, ProtocolError, Stream};
use luxbyte_sql::combine::{AggregateFinish, Aggregates, Finish, RowsFinish};
use luxbyte_sql::{Access, AnalysisCache, StatementAnalysis, TransactionOp};
use luxbyte_txn::{Coordinator, Transaction};
use tokio::net::TcpStream;

use crate::merge::{self, RegionResult};
use crate::split::{SplitError, split_insert};

mod extended;

/// How long to spend delivering one cancel to a backend.
const CANCEL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Everything shared between all sessions.
#[derive(Debug)]
pub struct Shared {
    /// Decides where statements go.
    pub router: Router,
    /// Reuses statement analyses across sessions.
    pub cache: AnalysisCache,
    /// Backend connections.
    pub pools: PoolSet,
    /// Issues and resolves distributed transactions.
    pub coordinator: Coordinator,
    /// The key directory, shared with the router.
    pub directory: Arc<MemoryDirectory>,
    /// How to authenticate to backends.
    pub credentials: Credentials,
    /// Role to connect to backends as.
    pub backend_user: String,
    /// Database to connect to.
    pub backend_database: String,
    /// How clients authenticate.
    pub auth: crate::auth::Authenticator,
    /// The cancel key of every live session.
    pub cancels: crate::cancel::CancelRegistry,
    /// What the proxy has been doing.
    pub metrics: crate::metrics::Metrics,
    /// Accepts TLS from clients, when a certificate is configured.
    pub tls: Option<crate::tls::Acceptor>,
    /// Refuse clients that do not ask for TLS.
    pub tls_required: bool,
    /// The aggregate functions the database lists, and when they were read.
    pub aggregates: parking_lot::Mutex<Option<(std::time::Instant, Arc<Aggregates>)>>,
    /// Keys moving between regions, and the writes a move waits for.
    pub moves: crate::relocate::Moves,
    /// Which backends are up, and how far replicas lag.
    pub health: crate::health::Health,
}

impl Shared {
    /// The pool for a region, as the proxy's own backend role, choosing a
    /// replica for reads where one exists.
    pub fn pool_for(&self, region: &str, access: Access, nth: usize) -> Option<Pool> {
        self.pool_as(
            region,
            access,
            nth,
            None,
            &self.backend_user,
            &self.credentials,
        )
    }

    /// The pool for a region, as a given role. A read goes to a replica
    /// that is up, not too far behind, and — after `wrote` — has replayed
    /// the session's last write there; otherwise to the primary.
    pub fn pool_as(
        &self,
        region: &str,
        access: Access,
        nth: usize,
        wrote: Option<&mut crate::health::Wrote>,
        user: &str,
        credentials: &Credentials,
    ) -> Option<Pool> {
        let region = self.router.topology().region(region)?;
        let endpoint: &Endpoint = if access.needs_primary() {
            &region.primary
        } else {
            self.health.read_endpoint(region, nth, wrote)
        };
        let target = ConnectionTarget::new(endpoint, &self.backend_database, user);
        Some(self.pools.pool(&target, credentials))
    }

    /// Borrows a connection from `pool`. A read whose replica cannot be
    /// reached goes to `primary` instead, which has the same rows; nothing
    /// was sent, so nothing can have run twice. An endpoint that could not
    /// be reached is marked down either way.
    pub async fn acquire(
        &self,
        pool: &Pool,
        primary: Option<&Pool>,
    ) -> Result<luxbyte_pool::PooledConnection, PoolError> {
        let error = match pool.acquire().await {
            Ok(connection) => return Ok(connection),
            Err(error) => error,
        };
        if error.connection_lost() || matches!(error, PoolError::Protocol(_)) {
            self.health.failed(&endpoint_key(pool.target()), &error);
        }
        match primary.filter(|primary| primary.target() != pool.target()) {
            Some(primary) => {
                self.health.failed_over();
                tracing::debug!(%error, "a replica could not be reached; reading from the primary");
                primary.acquire().await
            }
            None => Err(error),
        }
    }
}

/// The health key of the endpoint a pool connects to.
fn endpoint_key(target: &ConnectionTarget) -> String {
    format!("{}:{}", target.host, target.port)
}

/// What ended a session.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SessionError {
    /// The connection failed or the protocol was violated.
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    /// The socket failed.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

/// One client connection.
pub struct Session {
    shared: Arc<Shared>,
    client: Framed<Stream>,
    status: TransactionStatus,
    /// `SET` statements to replay onto every backend this session uses.
    settings: Vec<String>,
    transaction: Option<Transaction>,
    /// Advanced per read, to spread reads across a region's replicas.
    reads: usize,
    /// Prepared statements, portals and the batch in progress.
    extended: extended::State,
    /// The role this session's backend work runs as: the client's own, with
    /// passthrough, or the proxy's.
    backend_user: String,
    credentials: Credentials,
    /// Every backend connection this session is using, for a cancel.
    cancel: CancelScope,
    /// The cancel key this session was given, once it has one.
    key: Option<CancelRequest>,
    /// The role the client logged in as.
    user: String,
    /// When this session last wrote in each region, so that its reads there
    /// see what it wrote.
    wrote: parking_lot::Mutex<std::collections::BTreeMap<String, crate::health::Wrote>>,
    /// Regions the open transaction has written in, which count as written
    /// once it commits.
    txn_wrote: parking_lot::Mutex<std::collections::BTreeSet<String>>,
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.shared.cancels.forget(key);
        }
    }
}

impl Session {
    /// Serves one client until it disconnects.
    pub async fn run(shared: Arc<Shared>, stream: TcpStream) -> Result<(), SessionError> {
        let counted = Arc::clone(&shared);
        let _session = counted.metrics.session();
        stream.set_nodelay(true)?;
        let Some((client, first)) = negotiate(&shared, stream).await? else {
            return Ok(());
        };
        let mut session = Self {
            client,
            status: TransactionStatus::Idle,
            settings: Vec::new(),
            transaction: None,
            reads: 0,
            extended: extended::State::default(),
            backend_user: shared.backend_user.clone(),
            credentials: shared.credentials.clone(),
            cancel: CancelScope::new(),
            key: None,
            user: String::new(),
            wrote: parking_lot::Mutex::default(),
            txn_wrote: parking_lot::Mutex::default(),
            shared,
        };
        if session.startup(&first).await? {
            session.serve().await?;
        }
        Ok(())
    }

    /// Handles the startup phase. Returns whether to go on to serving.
    async fn startup(&mut self, first: &luxbyte_proto::RawMessage) -> Result<bool, SessionError> {
        match StartupRequest::decode(first)? {
            StartupRequest::Startup(parameters) => {
                if self.shared.tls_required && !self.client.get_ref().is_encrypted() {
                    self.fatal(
                        "28000",
                        "this proxy accepts only TLS connections; connect with sslmode=require \
                         or stricter",
                    )
                    .await?;
                    return Ok(false);
                }
                let Some(user) = parameters.get("user").map(ToOwned::to_owned) else {
                    self.fatal(
                        "28000",
                        "no PostgreSQL user name specified in startup packet",
                    )
                    .await?;
                    return Ok(false);
                };
                if !self.authenticate(&user).await? {
                    return Ok(false);
                }
                self.user.clone_from(&user);
                self.client.queue_raw(&build::authentication_ok()?);
                for (name, value) in [
                    ("server_version", "18.0 (luxbyte)"),
                    ("client_encoding", "UTF8"),
                    ("DateStyle", "ISO, MDY"),
                    ("integer_datetimes", "on"),
                    ("standard_conforming_strings", "on"),
                    (
                        "application_name",
                        parameters.get("application_name").unwrap_or(""),
                    ),
                ] {
                    self.client
                        .queue_raw(&build::parameter_status(name, value)?);
                }
                // The proxy's own key, not a backend's: a cancel request
                // arrives on a new connection and has to be routed, which
                // needs a key this proxy issued — and the work it cancels
                // may be running in several regions at once.
                let key = self.shared.cancels.register(self.cancel.clone());
                self.key = Some(key);
                self.client
                    .queue_raw(&build::backend_key_data(key.process_id, key.secret_key)?);
                self.ready().await?;
                Ok(true)
            }
            // A cancel arrives on its own connection and carries no
            // credentials; the key is the credential. One that matches no
            // session is dropped, as PostgreSQL drops it, without a reply
            // that would confirm or deny anything.
            StartupRequest::Cancel(request) => {
                if let Some(scope) = self.shared.cancels.lookup(request) {
                    let delivered = scope.cancel(CANCEL_TIMEOUT).await;
                    self.shared.metrics.cancelled(delivered);
                    tracing::debug!(delivered, "relayed a cancel");
                }
                Ok(false)
            }
            // A second encryption request after the first was answered is
            // a client out of step with the protocol.
            StartupRequest::GssEnc | StartupRequest::Ssl => {
                self.fatal("08P01", "encryption was already negotiated")
                    .await?;
                Ok(false)
            }
        }
    }

    /// Runs the client's authentication exchange. Returns whether it
    /// succeeded; on failure the client has already been told.
    async fn authenticate(&mut self, user: &str) -> Result<bool, SessionError> {
        use luxbyte_proto::auth::{SCRAM_SHA_256, ScramServer};

        if self.shared.auth.method() == crate::auth::Method::Trust {
            return Ok(true);
        }
        let verifier = match self.shared.auth.verifier(&self.shared, user).await {
            Ok(verifier) => verifier,
            Err(error) => {
                // Not the client's fault, and not a reason to tell it which
                // roles exist: an unavailable lookup is a connection failure.
                tracing::warn!(%error, "could not read a verifier");
                self.fatal("08006", "the proxy could not check credentials right now")
                    .await?;
                return Ok(false);
            }
        };
        let failed = format!("password authentication failed for user \"{user}\"");

        self.client
            .queue_raw(&build::authentication_sasl(SCRAM_SHA_256)?);
        self.client.flush().await?;
        let initial = self.client.read_message().await?;
        let Some((mechanism, client_first)) = crate::auth::sasl_initial(&initial.body) else {
            self.fatal("08P01", "malformed SASL initial response")
                .await?;
            return Ok(false);
        };
        if mechanism != SCRAM_SHA_256 {
            self.fatal(
                "28000",
                "the client chose a SASL mechanism this proxy does not offer",
            )
            .await?;
            return Ok(false);
        }

        let mut server = ScramServer::new(verifier);
        let Ok(server_first) = server.server_first(&client_first) else {
            self.shared.metrics.auth_failed();
            self.fatal("28P01", &failed).await?;
            return Ok(false);
        };
        self.client.queue_raw(&build::authentication_sasl_continue(
            server_first.as_bytes(),
        )?);
        self.client.flush().await?;

        let response = self.client.read_message().await?;
        let client_final = String::from_utf8_lossy(&response.body).into_owned();
        let Ok(server_final) = server.server_final(&client_final) else {
            self.shared.metrics.auth_failed();
            self.fatal("28P01", &failed).await?;
            return Ok(false);
        };
        self.client
            .queue_raw(&build::authentication_sasl_final(server_final.as_bytes())?);

        if self.shared.auth.passthrough() {
            if let Some(keys) = server.keys() {
                user.clone_into(&mut self.backend_user);
                self.credentials = Credentials::Scram(keys);
            }
        }
        Ok(true)
    }

    /// Commits, counting how the transaction ended.
    async fn commit(
        &self,
        transaction: Transaction,
    ) -> Result<luxbyte_txn::CommitOutcome, luxbyte_txn::TxnError> {
        use luxbyte_txn::CommitOutcome;
        let outcome = transaction.commit().await;
        let written = std::mem::take(&mut *self.txn_wrote.lock());
        if outcome.is_ok() {
            self.wrote_in(written.iter().map(String::as_str));
        }
        self.shared.metrics.transaction(match &outcome {
            Ok(CommitOutcome::SinglePhase { .. }) => "committed_one_phase",
            Ok(CommitOutcome::Committed { .. }) => "committed_two_phase",
            Ok(CommitOutcome::CommittedPendingAcknowledgement { .. }) => {
                "committed_pending_acknowledgement"
            }
            Ok(_) => "committed_empty",
            Err(_) => "failed",
        });
        outcome
    }

    /// Rolls back, counting it.
    async fn rollback(&self, transaction: Transaction) {
        let _ = transaction.rollback().await;
        self.txn_wrote.lock().clear();
        self.shared.metrics.transaction("rolled_back");
    }

    /// The pool for a region, as this session's backend role.
    fn pool_for(&self, region: &str, access: Access, nth: usize) -> Option<Pool> {
        let mut wrote = self.wrote.lock();
        self.shared.pool_as(
            region,
            access,
            nth,
            wrote.get_mut(region),
            &self.backend_user,
            &self.credentials,
        )
    }

    /// Records writes that have just succeeded: committed already outside a
    /// transaction, or once the open one commits.
    fn note_writes<'a>(&self, regions: impl IntoIterator<Item = &'a str>) {
        if self.transaction.is_some() {
            self.txn_wrote
                .lock()
                .extend(regions.into_iter().map(ToOwned::to_owned));
        } else {
            self.wrote_in(regions);
        }
    }

    /// Records that this session's writes in `regions` have committed.
    fn wrote_in<'a>(&self, regions: impl IntoIterator<Item = &'a str>) {
        let mut wrote = self.wrote.lock();
        for region in regions {
            wrote.insert(region.to_owned(), crate::health::Wrote::now());
        }
    }

    async fn serve(&mut self) -> Result<(), SessionError> {
        loop {
            let message = match self.client.read_message().await {
                Ok(message) => message,
                Err(ProtocolError::Closed(_)) => return Ok(()),
                Err(error) => return Err(error.into()),
            };

            match Frontend::decode(&message)? {
                Frontend::Query { sql } => {
                    let sql = sql.to_owned();
                    // A simple query ends an unfinished extended-query batch,
                    // as it does on PostgreSQL.
                    if self.extended.active() {
                        self.end_batch().await?;
                    }
                    self.handle_query(&sql).await?;
                }
                Frontend::Terminate => return Ok(()),
                Frontend::Parse(parse) => self.on_parse(&parse)?,
                Frontend::Bind(bind) => self.on_bind(&bind).await?,
                Frontend::Describe { target, name } => self.on_describe(target, name).await?,
                Frontend::Execute { portal, max_rows } => {
                    self.on_execute(portal, max_rows).await?;
                }
                Frontend::Close { target, name } => self.on_close(target, name).await?,
                Frontend::Sync => self.on_sync().await?,
                Frontend::Flush => self.on_flush().await?,
                // Anything else is acknowledged, so a stray message cannot
                // wedge the session.
                _ => self.ready().await?,
            }
        }
    }

    // --------------------------------------------------------- statements

    async fn handle_query(&mut self, sql: &str) -> Result<(), SessionError> {
        if sql.trim().is_empty() {
            self.client.queue_raw(&build::empty_query_response()?);
            return self.ready().await;
        }

        if let Some(command) = crate::relocate::parse(sql) {
            match self.relocate(command).await {
                Ok(result) => self.respond(&result)?,
                Err(refusal) => self.error(&refusal)?,
            }
            return self.ready().await;
        }

        if let Some(result) = crate::console::handle(&self.shared, sql) {
            self.respond(&result)?;
            return self.ready().await;
        }

        let prepared = match self.shared.cache.analyze(sql) {
            Ok(prepared) => prepared,
            Err(error) => {
                self.error(&Refusal {
                    sqlstate: "42601".to_owned(),
                    message: error.to_string(),
                    hint: None,
                    retryable: false,
                })?;
                return self.ready().await;
            }
        };

        // Each statement is routed on its own and must be *sent* on its own:
        // sending the whole batch to each statement's region would run the
        // other statements there too.
        let texts = luxbyte_sql::statements::split(sql);
        if texts.len() != prepared.analyses.len() {
            // The splitter and the parser disagree about where statements
            // end. Guessing could send a statement to the wrong region.
            self.error(&Refusal {
                sqlstate: "0A000".to_owned(),
                message: "the statements in this query could not be told apart reliably".to_owned(),
                hint: Some("send the statements one at a time".to_owned()),
                retryable: false,
            })?;
            return self.ready().await;
        }

        // PostgreSQL runs a batch of several statements as one implicit
        // transaction: if the third fails, the first two never happened. The
        // proxy keeps that promise with a transaction of its own, which
        // commits in two phases if the batch crossed regions.
        let implicit = prepared.analyses.len() > 1
            && self.transaction.is_none()
            && !prepared
                .analyses
                .iter()
                .any(StatementAnalysis::is_transaction_control);
        if implicit {
            let mut transaction = self.shared.coordinator.begin();
            transaction.cancel_with(&self.cancel);
            self.transaction = Some(transaction);
        }

        let parameters = ParameterValues::from_prepared(&prepared, &[]);
        let mut failed = false;
        for (analysis, text) in prepared.analyses.iter().zip(texts) {
            if self.blocked_by_failed_transaction(analysis) {
                self.error(&Refusal {
                    sqlstate: "25P02".to_owned(),
                    message: "current transaction is aborted, commands ignored until end of \
                              transaction block"
                        .to_owned(),
                    hint: Some("send ROLLBACK".to_owned()),
                    retryable: false,
                })?;
                break;
            }

            let started = std::time::Instant::now();
            let (plan, _write) = self.route(analysis, &parameters).await;
            let kind = plan_kind(&plan);
            let writes: Vec<String> = if analysis.access.needs_primary() {
                plan.regions().into_iter().map(ToOwned::to_owned).collect()
            } else {
                Vec::new()
            };
            let outcome = self.execute(analysis, text, plan).await?;
            self.shared.metrics.statement(kind, started.elapsed());
            if outcome.is_ok() {
                self.note_writes(writes.iter().map(String::as_str));
            }
            if let Err(failure) = outcome {
                if let Failed::Refused(refusal) = failure {
                    self.error(&refusal)?;
                }
                failed = true;
                // A failed statement inside a transaction poisons it, as it
                // would on a single server.
                if self.transaction.is_some() {
                    self.status = TransactionStatus::Failed;
                }
                break;
            }
        }

        if implicit {
            self.status = TransactionStatus::Idle;
            if let Some(transaction) = self.transaction.take() {
                self.extended.end_transaction();
                if failed {
                    self.rollback(transaction).await;
                } else if let Err(error) = self.commit(transaction).await {
                    self.error(&txn_refusal(&error))?;
                }
            }
        }

        self.ready().await
    }

    /// Plans a statement, waiting while it touches a key that is moving
    /// between regions. A write comes back with the ticket that makes a move
    /// wait for it, to be held until the write has been answered.
    async fn route(
        &self,
        analysis: &StatementAnalysis,
        parameters: &ParameterValues,
    ) -> (Plan, Option<crate::relocate::Ticket>) {
        let moves = &self.shared.moves;
        let started = std::time::Instant::now();
        let mut counted = false;
        loop {
            let changed = moves.changed();
            tokio::pin!(changed);
            changed.as_mut().enable();
            // Taken before planning, so that a move that begins after this
            // point either is seen by the planner or waits for this write.
            let ticket = analysis.access.needs_primary().then(|| moves.write());
            let refusal = match self.shared.router.route(analysis, Some(parameters)) {
                Routing::Ready(plan) => return (plan, ticket),
                Routing::Held(refusal) => refusal,
            };
            drop(ticket);
            // A transaction that has already done work may hold locks the
            // move is waiting for; waiting here would only stall them both.
            let busy = self
                .transaction
                .as_ref()
                .is_some_and(|transaction| !transaction.is_empty());
            let remaining = moves.hold().saturating_sub(started.elapsed());
            if busy || remaining.is_zero() {
                return (Plan::Refuse(refusal), None);
            }
            if !counted {
                self.shared.metrics.held();
                counted = true;
            }
            if tokio::time::timeout(remaining, changed).await.is_err() {
                return (Plan::Refuse(refusal), None);
            }
        }
    }

    /// Runs `RELOCATE`, for an operator.
    async fn relocate(
        &mut self,
        command: Result<(String, String), String>,
    ) -> Result<QueryResult, Refusal> {
        let refuse = |sqlstate: &str, message: String, hint: Option<&str>| Refusal {
            sqlstate: sqlstate.to_owned(),
            message,
            hint: hint.map(ToOwned::to_owned),
            retryable: false,
        };
        let (key, to) = command.map_err(|usage| refuse("42601", usage, None))?;
        let Some(store) = self.shared.moves.store() else {
            return Err(refuse(
                "55000",
                "RELOCATE needs a durable directory".to_owned(),
                Some("add a [directory_store] section"),
            ));
        };
        // Moving data is an operator's act. With trust authentication any
        // client can claim any role, so nobody qualifies.
        if self.shared.auth.method() == crate::auth::Method::Trust {
            return Err(refuse(
                "42501",
                "RELOCATE needs clients to authenticate".to_owned(),
                Some("configure [auth], and list the operator in directory_store.operators"),
            ));
        }
        if !store.operators.contains(&self.user) {
            return Err(refuse(
                "42501",
                format!("role \"{}\" may not run RELOCATE", self.user),
                Some("list it in directory_store.operators"),
            ));
        }
        if self.transaction.is_some() {
            return Err(refuse(
                "25001",
                "RELOCATE cannot run inside a transaction block".to_owned(),
                None,
            ));
        }
        let outcome =
            crate::relocate::relocate(Arc::clone(&self.shared), key.clone(), to.clone()).await;
        match &outcome {
            Ok(_) => tracing::info!(user = %self.user, %key, %to, "RELOCATE"),
            Err(refusal) => {
                tracing::warn!(user = %self.user, %key, %to, error = %refusal.message, "RELOCATE refused");
            }
        }
        outcome.map(|report| report.result())
    }

    fn blocked_by_failed_transaction(&self, analysis: &StatementAnalysis) -> bool {
        if self.status != TransactionStatus::Failed {
            return false;
        }
        // Only the statements that end the block are allowed through.
        !matches!(
            analysis.kind,
            luxbyte_sql::StatementKind::TransactionControl(
                TransactionOp::Commit | TransactionOp::Rollback
            )
        )
    }

    /// Runs one planned statement. `Ok(Err(refusal))` means the statement
    /// was refused; `Err(_)` means the session cannot continue.
    async fn execute(
        &mut self,
        analysis: &StatementAnalysis,
        sql: &str,
        plan: Plan,
    ) -> Result<Result<(), Failed>, SessionError> {
        let refused = |outcome: Result<(), Refusal>| outcome.map_err(Failed::Refused);
        match plan {
            Plan::Refuse(refusal) => Ok(Err(Failed::Refused(refusal))),
            Plan::NeedsParameters { .. } => Ok(Err(Failed::Refused(Refusal {
                sqlstate: "0A000".to_owned(),
                message: "the shard key is a placeholder, which needs the extended query \
                          protocol"
                    .to_owned(),
                hint: None,
                retryable: false,
            }))),
            Plan::Local(intent) => self.local(intent, sql).await,
            Plan::Single { region, access } => self.relay_on(&region, access, sql).await,
            Plan::ScatterGather { regions, .. } => Ok(refused(self.scatter(&regions, sql).await?)),
            Plan::Distributed { regions } => {
                Ok(refused(self.distributed(analysis, sql, &regions).await?))
            }
        }
    }

    /// Runs a statement in one region and passes the answer to the client
    /// as the backend sends it: rows are never decoded, a large result is
    /// never held in memory, and the column descriptions keep everything
    /// PostgreSQL put in them.
    async fn relay_on(
        &mut self,
        region: &str,
        access: Access,
        sql: &str,
    ) -> Result<Result<(), Failed>, SessionError> {
        let statement = self.with_settings(sql);
        let replayed = self.settings.len();
        self.reads = self.reads.wrapping_add(1);
        // Inside a transaction every region is enlisted on its primary.
        let access = if self.transaction.is_some() {
            Access::ReadWrite
        } else {
            access
        };
        let remote = region != self.shared.router.topology().home_region;
        self.shared.metrics.sent(region, remote);
        let Some(pool) = self.pool_for(region, access, self.reads) else {
            return Ok(Err(Failed::Refused(Refusal {
                sqlstate: "08000".to_owned(),
                message: format!("region {region} is not configured"),
                hint: None,
                retryable: false,
            })));
        };

        if let Some(transaction) = self.transaction.as_mut() {
            if let Err(error) = transaction.enlist(region, &pool).await {
                return Ok(Err(Failed::Refused(to_refusal(&backend_error(error)))));
            }
            let connection = match transaction.connection(region) {
                Ok(connection) => connection,
                Err(error) => return Ok(Err(Failed::Refused(txn_refusal(&error)))),
            };
            return relay(
                connection,
                &mut self.client,
                &statement,
                replayed,
                &self.shared.metrics,
            )
            .await;
        }
        let primary = if access.needs_primary() {
            None
        } else {
            self.pool_for(region, Access::ReadWrite, 0)
        };
        let mut connection = match self.shared.acquire(&pool, primary.as_ref()).await {
            Ok(connection) => connection,
            Err(error) => return Ok(Err(Failed::Refused(to_refusal(&error)))),
        };
        connection.join(&self.cancel);
        relay(
            &mut connection,
            &mut self.client,
            &statement,
            replayed,
            &self.shared.metrics,
        )
        .await
    }

    // ------------------------------------------------------- local work

    async fn local(
        &mut self,
        intent: LocalIntent,
        sql: &str,
    ) -> Result<Result<(), Failed>, SessionError> {
        match intent {
            LocalIntent::SessionState => {
                self.record_setting(sql);
                self.command_complete("SET")?;
                Ok(Ok(()))
            }
            LocalIntent::Show => {
                let home = self.shared.router.topology().home_region.clone();
                self.relay_on(&home, Access::ReadOnly, sql).await
            }
            LocalIntent::TransactionControl(op) => {
                Ok(self.transaction_control(op).await?.map_err(Failed::Refused))
            }
        }
    }

    fn record_setting(&mut self, sql: &str) {
        let trimmed = sql.trim().trim_end_matches(';');
        if trimmed.to_uppercase().starts_with("DISCARD") {
            self.settings.clear();
        } else {
            // Replayed onto every backend this session later uses, so that a
            // session which moves between regions keeps the settings it
            // asked for.
            self.settings.push(trimmed.to_owned());
        }
    }

    async fn transaction_control(
        &mut self,
        op: TransactionOp,
    ) -> Result<Result<(), Refusal>, SessionError> {
        match op {
            TransactionOp::Begin => {
                if self.transaction.is_none() {
                    let mut transaction = self.shared.coordinator.begin();
                    transaction.cancel_with(&self.cancel);
                    self.transaction = Some(transaction);
                    self.status = TransactionStatus::InTransaction;
                }
                self.command_complete("BEGIN")?;
                Ok(Ok(()))
            }
            TransactionOp::Commit => {
                let Some(transaction) = self.transaction.take() else {
                    // `COMMIT` with no transaction open is a warning in
                    // PostgreSQL, not an error.
                    self.command_complete("COMMIT")?;
                    return Ok(Ok(()));
                };
                // Portals end with their transaction, however it ends.
                self.extended.end_transaction();
                // A failed transaction cannot commit. PostgreSQL rolls it
                // back and says so in the tag; committing here would keep the
                // statements that ran before a refusal the client was told
                // had aborted the block.
                if self.status == TransactionStatus::Failed {
                    self.rollback(transaction).await;
                    self.status = TransactionStatus::Idle;
                    self.command_complete("ROLLBACK")?;
                    return Ok(Ok(()));
                }
                let outcome = self.commit(transaction).await;
                self.status = TransactionStatus::Idle;
                match outcome {
                    Ok(_) => {
                        self.command_complete("COMMIT")?;
                        Ok(Ok(()))
                    }
                    Err(error) => Ok(Err(Refusal {
                        sqlstate: if error.retryable() { "40001" } else { "40000" }.to_owned(),
                        message: error.to_string(),
                        hint: error
                            .retryable()
                            .then(|| "retry the transaction".to_owned()),
                        retryable: error.retryable(),
                    })),
                }
            }
            TransactionOp::Rollback => {
                if let Some(transaction) = self.transaction.take() {
                    self.extended.end_transaction();
                    self.rollback(transaction).await;
                }
                self.status = TransactionStatus::Idle;
                self.command_complete("ROLLBACK")?;
                Ok(Ok(()))
            }
            // A savepoint inside a distributed transaction would have to be
            // taken and released on every participant atomically, which is
            // a second consensus problem on top of the commit.
            // `TransactionOp` is non-exhaustive, so anything added later
            // is refused rather than silently mishandled.
            _ => Ok(Err(Refusal {
                sqlstate: "0A000".to_owned(),
                message: "savepoints are not supported inside a transaction that may span \
                          regions"
                    .to_owned(),
                hint: None,
                retryable: false,
            })),
        }
    }

    // ------------------------------------------------------- backend work

    /// Runs a statement on one region, through the open transaction if
    /// there is one.
    async fn run_on(
        &mut self,
        region: &str,
        access: Access,
        sql: &str,
    ) -> Result<QueryResult, PoolError> {
        let statement = self.with_settings(sql);
        self.reads = self.reads.wrapping_add(1);
        // Inside a transaction every region is enlisted on its primary: a
        // participant opened on a replica because the first statement was a
        // read would refuse every write after it.
        let access = if self.transaction.is_some() {
            Access::ReadWrite
        } else {
            access
        };
        let remote = region != self.shared.router.topology().home_region;
        self.shared.metrics.sent(region, remote);
        let pool = self
            .pool_for(region, access, self.reads)
            .ok_or_else(|| PoolError::Auth(format!("region {region} is not configured")))?;

        if let Some(transaction) = self.transaction.as_mut() {
            transaction
                .enlist(region, &pool)
                .await
                .map_err(backend_error)?;
            return transaction
                .execute(region, &statement)
                .await
                .map_err(backend_error);
        }
        let primary = if access.needs_primary() {
            None
        } else {
            self.pool_for(region, Access::ReadWrite, 0)
        };
        let mut connection = self.shared.acquire(&pool, primary.as_ref()).await?;
        connection.join(&self.cancel);
        connection.simple_query(&statement).await
    }

    /// Borrows a connection for `region`, outside any transaction: for a
    /// read, from a replica that qualifies, or the primary if that replica
    /// cannot be reached.
    async fn borrow(
        &mut self,
        region: &str,
        access: Access,
    ) -> Result<luxbyte_pool::PooledConnection, PoolError> {
        self.reads = self.reads.wrapping_add(1);
        let pool = self
            .pool_for(region, access, self.reads)
            .ok_or_else(|| PoolError::Auth(format!("region {region} is not configured")))?;
        let primary = if access.needs_primary() {
            None
        } else {
            self.pool_for(region, Access::ReadWrite, 0)
        };
        let mut connection = self.shared.acquire(&pool, primary.as_ref()).await?;
        connection.join(&self.cancel);
        Ok(connection)
    }

    /// Prefixes the session's settings so a freshly borrowed connection
    /// behaves like the one the client last used.
    ///
    /// Sent in the same round trip rather than separately, because a
    /// settings replay that costs an extra transatlantic round trip would
    /// undo the point of routing.
    fn with_settings(&self, sql: &str) -> String {
        if self.settings.is_empty() {
            return sql.to_owned();
        }
        format!("{}; {sql}", self.settings.join("; "))
    }

    async fn scatter(
        &mut self,
        regions: &[String],
        sql: &str,
    ) -> Result<Result<(), Refusal>, SessionError> {
        Ok(match self.scatter_result(regions, sql).await? {
            Ok(result) => {
                self.respond(&result)?;
                Ok(())
            }
            Err(refusal) => Err(refusal),
        })
    }

    /// Reads from several regions and combines the answers into one.
    pub(crate) async fn scatter_result(
        &mut self,
        regions: &[String],
        sql: &str,
    ) -> Result<Result<QueryResult, Refusal>, SessionError> {
        // Always planned, even when the analysis saw nothing to combine: a
        // function it did not recognise may be an aggregate the database
        // knows about, and computing that per region returns each region's
        // answer as if it were the whole.
        let aggregates = match self.aggregate_names().await {
            Ok(aggregates) => aggregates,
            Err(error) => return Ok(Err(to_refusal(&error))),
        };
        let plan = match luxbyte_sql::combine::plan(sql, &aggregates) {
            Ok(plan) => plan,
            Err(reason) => return Ok(Err(cannot_combine(regions.len(), &reason.0))),
        };
        // A plain union of rows needs nothing done afterwards, so it is sent
        // exactly as the client wrote it.
        let untouched = matches!(
            &plan.finish,
            Finish::Rows(rows) if rows.order.is_empty()
                && !rows.distinct
                && rows.hidden == 0
                && rows.limit.is_none()
                && rows.offset == 0
        );
        let region_sql = if untouched { sql } else { &plan.region_sql };

        let answers = match self.gather(regions, region_sql).await? {
            Ok(answers) => answers,
            Err(refusal) => return Ok(Err(refusal)),
        };
        Ok(match &plan.finish {
            Finish::Rows(finish) => self.finish_rows(answers, finish).await?,
            Finish::Aggregate(finish) => self.finish_aggregates(&answers, finish).await?,
        })
    }

    /// Runs one statement in every region and collects the answers, in
    /// region order.
    async fn gather(
        &mut self,
        regions: &[String],
        sql: &str,
    ) -> Result<Result<Vec<RegionResult>, Refusal>, SessionError> {
        let mut answers = Vec::with_capacity(regions.len());

        // Inside a transaction the read goes through its participants, so it
        // sees what the transaction has written; one region at a time, as
        // each participant is a single connection.
        if self.transaction.is_some() {
            for region in regions {
                match self.run_on(region, Access::ReadOnly, sql).await {
                    Ok(result) => answers.push(RegionResult {
                        region: region.clone(),
                        result,
                    }),
                    Err(error) => return Ok(Err(region_failed(region, &error))),
                }
            }
            return Ok(Ok(answers));
        }

        let statement = self.with_settings(sql);
        let mut tasks = tokio::task::JoinSet::new();
        for (index, region) in regions.iter().enumerate() {
            let Some(pool) =
                self.pool_for(region, Access::ReadOnly, self.reads.wrapping_add(index))
            else {
                // A region left out makes a result that looks whole and is
                // not, which is worse than no result.
                return Ok(Err(Refusal {
                    sqlstate: "08000".to_owned(),
                    message: format!("region {region} is not configured"),
                    hint: None,
                    retryable: false,
                }));
            };
            let primary = self.pool_for(region, Access::ReadWrite, 0);
            let remote = *region != self.shared.router.topology().home_region;
            self.shared.metrics.sent(region, remote);
            let region = region.clone();
            let statement = statement.clone();
            let scope = self.cancel.clone();
            let shared = Arc::clone(&self.shared);
            // Every region is queried at once: the whole point is to pay
            // the latency of the slowest region, not the sum of them all.
            tasks.spawn(async move {
                let outcome = match shared.acquire(&pool, primary.as_ref()).await {
                    Ok(mut connection) => {
                        connection.join(&scope);
                        connection.simple_query(&statement).await
                    }
                    Err(error) => Err(error),
                };
                (region, outcome)
            });
        }
        self.reads = self.reads.wrapping_add(regions.len());

        while let Some(joined) = tasks.join_next().await {
            let Ok((region, outcome)) = joined else {
                return Ok(Err(Refusal {
                    sqlstate: "XX000".to_owned(),
                    message: "a region's read was lost".to_owned(),
                    hint: None,
                    retryable: true,
                }));
            };
            match outcome {
                Ok(result) => answers.push(RegionResult { region, result }),
                // One region failing makes the result incomplete, and an
                // incomplete result that looks whole is worse than an error.
                Err(error) => return Ok(Err(region_failed(&region, &error))),
            }
        }
        // Regions answer in whatever order they finish; a merge has to see
        // them in a stable order or an equal-keyed row could move.
        answers.sort_by(|left, right| left.region.cmp(&right.region));
        Ok(Ok(answers))
    }

    /// Orders, deduplicates and limits rows that came from several regions:
    /// here when that can be done exactly, otherwise in the home region.
    async fn finish_rows(
        &mut self,
        answers: Vec<RegionResult>,
        finish: &RowsFinish,
    ) -> Result<Result<QueryResult, Refusal>, SessionError> {
        let (columns, type_oids) = merge::columns_of(&answers);
        let order = match finish.resolve(&columns) {
            Ok(order) => order,
            Err(reason) => return Ok(Err(cannot_combine(answers.len(), &reason.0))),
        };
        if !finish.distinct {
            let rows = if order.is_empty() {
                Some(merge::concatenate(answers.clone()).rows)
            } else {
                merge::sorted(&answers, &order)
            };
            if let Some(rows) = rows {
                self.shared.metrics.combined(if order.is_empty() {
                    "concatenated"
                } else {
                    "merged_in_proxy"
                });
                return Ok(Ok(merge::finish_rows(
                    columns,
                    type_oids,
                    rows,
                    finish.offset,
                    finish.limit,
                    finish.hidden,
                )));
            }
        }
        let source = match carried(&answers, &type_oids) {
            Ok(source) => source,
            Err(refusal) => return Ok(Err(refusal)),
        };
        self.shared.metrics.combined("finished_in_postgres");
        match finish.query(&columns, &source) {
            Ok(sql) => self.finish_in_home(&sql).await,
            Err(reason) => Ok(Err(cannot_combine(answers.len(), &reason.0))),
        }
    }

    /// Regroups partial aggregates in the home region.
    async fn finish_aggregates(
        &mut self,
        answers: &[RegionResult],
        finish: &AggregateFinish,
    ) -> Result<Result<QueryResult, Refusal>, SessionError> {
        let (columns, type_oids) = merge::columns_of(answers);
        if columns.len() != finish.columns {
            return Ok(Err(Refusal {
                sqlstate: "XX000".to_owned(),
                message: format!(
                    "the regions returned {} partial columns where {} were expected",
                    columns.len(),
                    finish.columns
                ),
                hint: None,
                retryable: false,
            }));
        }
        let types: Vec<String> = match type_names(&type_oids) {
            Ok(types) => types,
            Err(refusal) => return Ok(Err(refusal)),
        };
        let source = match carried(answers, &type_oids) {
            Ok(source) => source,
            Err(refusal) => return Ok(Err(refusal)),
        };
        self.shared.metrics.combined("aggregated_in_postgres");
        match finish.query(&types, &source) {
            Ok(sql) => self.finish_in_home(&sql).await,
            Err(reason) => Ok(Err(cannot_combine(answers.len(), &reason.0))),
        }
    }

    /// Runs a finishing statement in the home region: nearby, and any
    /// region computes the same answer from the same partial results.
    async fn finish_in_home(
        &mut self,
        sql: &str,
    ) -> Result<Result<QueryResult, Refusal>, SessionError> {
        let home = self.shared.router.topology().home_region.clone();
        Ok(self
            .run_on(&home, Access::ReadOnly, sql)
            .await
            .map_err(|error| to_refusal(&error)))
    }

    /// The names the database lists as aggregates, read from the home
    /// region and remembered for a minute.
    ///
    /// The read is part of the client's statement, so the client's cancel
    /// reaches it, and a cancelled read cancels the statement.
    async fn aggregate_names(&mut self) -> Result<Arc<Aggregates>, PoolError> {
        const FRESH_FOR: std::time::Duration = std::time::Duration::from_secs(60);
        if let Some((at, names)) = self.shared.aggregates.lock().as_ref() {
            if at.elapsed() < FRESH_FOR {
                return Ok(Arc::clone(names));
            }
        }
        let home = self.shared.router.topology().home_region.clone();
        let read =
            match self.borrow(&home, Access::ReadOnly).await {
                Ok(mut connection) => connection
                    .simple_query(
                        "SELECT DISTINCT proname::text FROM pg_catalog.pg_proc WHERE prokind = 'a'",
                    )
                    .await,
                Err(error) => Err(error),
            };
        match read {
            Err(error) if error.sqlstate() == Some("57014") => Err(error),
            Ok(result) => {
                let names = Arc::new(Aggregates::with_names(
                    result
                        .rows
                        .into_iter()
                        .filter_map(|row| row.into_iter().next().flatten()),
                ));
                *self.shared.aggregates.lock() =
                    Some((std::time::Instant::now(), Arc::clone(&names)));
                Ok(names)
            }
            // The built-in list still catches the standard aggregates; not
            // remembering the failure means the next read tries again.
            Err(error) => {
                tracing::warn!(%error, "could not read the aggregate catalog");
                Ok(Arc::new(Aggregates::default()))
            }
        }
    }

    async fn distributed(
        &mut self,
        analysis: &StatementAnalysis,
        sql: &str,
        regions: &[String],
    ) -> Result<Result<(), Refusal>, SessionError> {
        Ok(
            match self.distributed_result(analysis, sql, regions).await? {
                Ok(result) => {
                    self.respond(&result)?;
                    Ok(())
                }
                Err(refusal) => Err(refusal),
            },
        )
    }

    /// Writes to several regions: inside the open transaction if there is
    /// one, otherwise as a transaction of its own, committed in two phases.
    ///
    /// Rows a `RETURNING` clause produces come back from every region, in
    /// region order; PostgreSQL promises no order for them either.
    pub(crate) async fn distributed_result(
        &mut self,
        analysis: &StatementAnalysis,
        sql: &str,
        regions: &[String],
    ) -> Result<Result<QueryResult, Refusal>, SessionError> {
        // An INSERT has to be split: broadcasting it would write every row
        // in every region.
        let per_region = if analysis.kind == luxbyte_sql::StatementKind::Insert {
            match self.split_for_regions(analysis, sql) {
                Ok(parts) => parts,
                Err(refusal) => return Ok(Err(refusal)),
            }
        } else {
            // Every other write applies to the rows a region already holds,
            // so the same statement is correct everywhere.
            regions
                .iter()
                .map(|region| crate::split::RegionStatement {
                    region: region.clone(),
                    sql: sql.to_owned(),
                })
                .collect()
        };

        let mut combined = QueryResult::default();
        let mut affected = 0_usize;
        let mut gather = |result: QueryResult| {
            affected = affected.saturating_add(rows_affected(&result));
            if combined.columns.is_empty() && !result.columns.is_empty() {
                combined.columns = result.columns;
                combined.type_oids = result.type_oids;
            }
            combined.rows.extend(result.rows);
        };

        // Inside an explicit transaction the client decides when to commit.
        if self.transaction.is_some() {
            for part in &per_region {
                match self
                    .run_on(&part.region, Access::ReadWrite, &part.sql)
                    .await
                {
                    Ok(result) => gather(result),
                    Err(error) => return Ok(Err(to_refusal(&error))),
                }
            }
            combined.tags = vec![command_tag(analysis, affected)];
            return Ok(Ok(combined));
        }

        // Otherwise the statement is its own transaction.
        let mut transaction = self.shared.coordinator.begin();
        transaction.cancel_with(&self.cancel);
        for part in &per_region {
            let Some(pool) = self.pool_for(&part.region, Access::ReadWrite, 0) else {
                // Leaving a region out would commit the rest of the write
                // without it.
                self.rollback(transaction).await;
                return Ok(Err(Refusal {
                    sqlstate: "08000".to_owned(),
                    message: format!("region {} is not configured", part.region),
                    hint: None,
                    retryable: false,
                }));
            };
            let statement = self.with_settings(&part.sql);
            if let Err(error) = transaction.enlist(&part.region, &pool).await {
                self.rollback(transaction).await;
                return Ok(Err(txn_refusal(&error)));
            }
            match transaction.execute(&part.region, &statement).await {
                Ok(result) => gather(result),
                Err(error) => {
                    self.rollback(transaction).await;
                    return Ok(Err(txn_refusal(&error)));
                }
            }
        }

        match self.commit(transaction).await {
            Ok(_) => {
                combined.tags = vec![command_tag(analysis, affected)];
                Ok(Ok(combined))
            }
            Err(error) => Ok(Err(txn_refusal(&error))),
        }
    }

    fn split_for_regions(
        &self,
        analysis: &StatementAnalysis,
        sql: &str,
    ) -> Result<Vec<crate::split::RegionStatement>, Refusal> {
        let table = analysis.tables.first().ok_or_else(|| Refusal {
            sqlstate: "0A000".to_owned(),
            message: "the statement writes to no table".to_owned(),
            hint: None,
            retryable: false,
        })?;
        let key = match self
            .shared
            .router
            .topology()
            .placement(table.schema.as_deref(), &table.name)
        {
            Some(luxbyte_core::topology::Placement::GeoPartitioned { key }) => key.clone(),
            _ => {
                return Err(Refusal {
                    sqlstate: "0A000".to_owned(),
                    message: format!("\"{table}\" is not partitioned by a key"),
                    hint: None,
                    retryable: false,
                });
            }
        };

        let directory = Arc::clone(&self.shared.directory);
        let home = self.shared.router.topology().home_region.clone();
        let table_name = table.name.clone();
        split_insert(sql, &key, move |value| {
            use luxbyte_core::directory::{Directory, Location};
            match directory.locate(&table_name, value) {
                // A write planned before a move began still goes where the
                // rows were; the move waits for it before taking them.
                Location::Known(state) => state.source().unwrap_or(&home).to_owned(),
                Location::Unseen => home.clone(),
            }
        })
        .map_err(|error: SplitError| Refusal {
            sqlstate: "0A000".to_owned(),
            message: format!(
                "this write spans regions and cannot be split: {}",
                error.reason()
            ),
            hint: Some("insert the rows for each region in separate statements".to_owned()),
            retryable: false,
        })
    }

    // ---------------------------------------------------------- replying

    fn respond(&mut self, result: &QueryResult) -> Result<(), SessionError> {
        if !result.columns.is_empty() {
            let names: Vec<&str> = result.columns.iter().map(String::as_str).collect();
            self.client
                .queue_raw(&build::row_description_typed(&names, &result.type_oids)?);
            for row in &result.rows {
                let values: Vec<Option<&str>> = row.iter().map(|value| value.as_deref()).collect();
                self.client.queue_raw(&build::data_row(&values)?);
            }
        }
        // A batch that replayed settings produced several tags; the one the
        // client asked about is the last.
        let tag = result
            .tags
            .last()
            .cloned()
            .unwrap_or_else(|| format!("SELECT {}", result.rows.len()));
        self.client.queue_raw(&build::command_complete(&tag)?);
        Ok(())
    }

    fn command_complete(&mut self, tag: &str) -> Result<(), SessionError> {
        self.client.queue_raw(&build::command_complete(tag)?);
        Ok(())
    }

    fn error(&mut self, refusal: &Refusal) -> Result<(), SessionError> {
        self.shared.metrics.error(&refusal.sqlstate);
        let mut builder = build::ErrorBuilder::error(&refusal.sqlstate, refusal.message.clone());
        if let Some(hint) = &refusal.hint {
            builder = builder.hint(hint.clone());
        }
        self.client.queue_raw(&builder.build()?);
        Ok(())
    }

    async fn fatal(&mut self, sqlstate: &str, message: &str) -> Result<(), SessionError> {
        self.client
            .queue_raw(&build::ErrorBuilder::fatal(sqlstate, message).build()?);
        self.client.flush().await?;
        Ok(())
    }

    async fn ready(&mut self) -> Result<(), SessionError> {
        self.client.queue_raw(&build::ready_for_query(self.status)?);
        self.client.flush().await?;
        Ok(())
    }
}

/// Answers the client's encryption requests and returns the connection,
/// encrypted if it asked and a certificate is configured, with the first
/// message that follows. `None` when the client went away or broke the
/// protocol.
async fn negotiate(
    shared: &Shared,
    stream: TcpStream,
) -> Result<Option<(Framed<Stream>, luxbyte_proto::RawMessage)>, SessionError> {
    let mut client = Framed::new(Stream::Plain(stream), Peer::Frontend);
    let mut first = client.read_untagged().await?;
    // At most one GSSAPI request and one TLS request, in that order, as
    // libpq sends them.
    for _ in 0..2 {
        if is_request(&first, luxbyte_proto::startup::GSSENC_REQUEST_CODE) {
            // Declining is the protocol's own answer, and a client built
            // with GSSAPI tries TLS or plain text next.
            client.queue_raw(b"N");
            client.flush().await?;
            first = client.read_untagged().await?;
            continue;
        }
        if !is_request(&first, SSL_REQUEST_CODE) || client.get_ref().is_encrypted() {
            break;
        }
        let Some(acceptor) = shared.tls.clone() else {
            client.queue_raw(b"N");
            client.flush().await?;
            first = client.read_untagged().await?;
            continue;
        };
        // Bytes sent after the request arrived unencrypted; taking them as
        // part of the encrypted session is the injection CVE-2021-23214
        // describes.
        if client.buffered_input() > 0 {
            return Ok(None);
        }
        client.queue_raw(b"S");
        client.flush().await?;
        let Some(plain) = client.into_inner().into_plain() else {
            return Ok(None);
        };
        let encrypted = match acceptor.accept(plain).await {
            Ok(encrypted) => encrypted,
            Err(error) => {
                tracing::debug!(%error, "a client's TLS handshake failed");
                return Ok(None);
            }
        };
        client = Framed::new(Stream::Server(Box::new(encrypted)), Peer::Frontend);
        first = client.read_untagged().await?;
    }
    Ok(Some((client, first)))
}

fn is_request(message: &luxbyte_proto::RawMessage, code: i32) -> bool {
    message
        .body
        .get(..4)
        .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
        .is_some_and(|bytes| i32::from_be_bytes(bytes) == code)
}

/// How a statement that did not succeed ended.
pub(crate) enum Failed {
    /// Refused, or failed before anything reached the client, which has
    /// still to be told.
    Refused(Refusal),
    /// The backend's error has been passed to the client already.
    Reported,
}

/// How much of an answer to stage before writing it to the client, so a
/// large result streams rather than piling up.
const RELAY_FLUSH_BYTES: usize = 64 * 1024;

/// Passes a simple query's answer from `connection` to `client` as it
/// arrives, leaving out the answers to the `replayed` settings sent ahead
/// of it, which the client did not ask for.
async fn relay(
    connection: &mut luxbyte_pool::PooledConnection,
    client: &mut Framed<Stream>,
    sql: &str,
    replayed: usize,
    metrics: &crate::metrics::Metrics,
) -> Result<Result<(), Failed>, SessionError> {
    if let Err(error) = connection.send_query(sql).await {
        return Ok(Err(Failed::Refused(to_refusal(&error))));
    }
    let mut replayed = replayed;
    let mut answered = false;
    let mut failed = false;
    loop {
        let message = match connection.next_reply().await {
            Ok(Some(message)) => message,
            Ok(None) => break,
            Err(error) => {
                let refusal = to_refusal(&error);
                if !answered {
                    return Ok(Err(Failed::Refused(refusal)));
                }
                // Part of the answer is already with the client; an error
                // after rows is something every client handles.
                metrics.error(&refusal.sqlstate);
                client.queue_raw(
                    &build::ErrorBuilder::error(&refusal.sqlstate, refusal.message).build()?,
                );
                return Ok(Err(Failed::Reported));
            }
        };
        if replayed > 0 && message.is(luxbyte_proto::tag::CLOSE_OR_COMMAND_COMPLETE) {
            replayed = replayed.saturating_sub(1);
            continue;
        }
        if message.is(luxbyte_proto::tag::EXECUTE_OR_ERROR) {
            failed = true;
            if let Ok(luxbyte_proto::backend::Backend::Error(notice)) =
                luxbyte_proto::backend::Backend::decode(&message)
            {
                metrics.error(notice.code());
            }
        }
        client.queue(&message)?;
        answered = true;
        if client.pending_writes() >= RELAY_FLUSH_BYTES {
            client.flush().await?;
        }
    }
    Ok(if failed {
        Err(Failed::Reported)
    } else {
        Ok(())
    })
}

/// The label a plan is counted under.
const fn plan_kind(plan: &Plan) -> &'static str {
    match plan {
        Plan::Local(_) => "local",
        Plan::Single { .. } => "single",
        Plan::ScatterGather { .. } => "scatter",
        Plan::Distributed { .. } => "distributed",
        Plan::NeedsParameters { .. } => "deferred",
        Plan::Refuse(_) => "refused",
    }
}

/// Why a read across `regions` regions could not be combined.
fn cannot_combine(regions: usize, reason: &str) -> Refusal {
    Refusal {
        sqlstate: "0A000".to_owned(),
        message: format!("the statement reads {regions} regions and cannot be combined: {reason}"),
        hint: Some(
            "add the shard key to the WHERE clause so the statement reads one region".to_owned(),
        ),
        retryable: false,
    }
}

fn region_failed(region: &str, error: &PoolError) -> Refusal {
    Refusal {
        sqlstate: error.sqlstate().unwrap_or("08000").to_owned(),
        message: format!("{region} did not answer: {error}"),
        hint: None,
        retryable: error.retryable(),
    }
}

/// The SQL type of each column, or a refusal naming one that cannot be
/// carried to the home region intact.
fn type_names(type_oids: &[i32]) -> Result<Vec<String>, Refusal> {
    type_oids
        .iter()
        .map(|oid| {
            luxbyte_sql::combine::type_name(*oid)
                .map(ToOwned::to_owned)
                .ok_or_else(|| Refusal {
                    sqlstate: "0A000".to_owned(),
                    message: format!(
                        "a column of type oid {oid} cannot be combined across regions"
                    ),
                    hint: Some(
                        "cast the column to a built-in type, or add the shard key to the WHERE \
                         clause"
                            .to_owned(),
                    ),
                    retryable: false,
                })
        })
        .collect()
}

/// Every region's rows as typed arrays, for a finishing statement.
fn carried(answers: &[RegionResult], type_oids: &[i32]) -> Result<String, Refusal> {
    let types = type_names(type_oids)?;
    let values = merge::by_column(answers, types.len());
    Ok(luxbyte_sql::combine::source(
        &types.into_iter().zip(values).collect::<Vec<_>>(),
    ))
}

fn to_refusal(error: &PoolError) -> Refusal {
    Refusal {
        // `08000` is connection_exception, which is what a backend that
        // cannot be reached amounts to from the client's point of view.
        sqlstate: error.sqlstate().unwrap_or("08000").to_owned(),
        message: error.to_string(),
        hint: None,
        retryable: error.retryable(),
    }
}

/// A transaction's failure as the backend error it usually is, so its
/// SQLSTATE — a cancel's `57014`, a deadlock's `40P01` — reaches the client
/// rather than being flattened into a generic connection failure.
fn backend_error(error: luxbyte_txn::TxnError) -> PoolError {
    match error {
        luxbyte_txn::TxnError::Pool(error) => error,
        other => PoolError::Server {
            severity: "ERROR".to_owned(),
            code: "XX000".to_owned(),
            message: other.to_string(),
        },
    }
}

fn txn_refusal(error: &luxbyte_txn::TxnError) -> Refusal {
    Refusal {
        sqlstate: if error.retryable() { "40001" } else { "40000" }.to_owned(),
        message: error.to_string(),
        hint: error
            .retryable()
            .then(|| "retry the transaction".to_owned()),
        retryable: error.retryable(),
    }
}

/// How many rows a command tag says it changed.
fn rows_affected(result: &QueryResult) -> usize {
    result
        .tags
        .last()
        .and_then(|tag| tag.rsplit(' ').next())
        .and_then(|count| count.parse().ok())
        .unwrap_or(0)
}

fn command_tag(analysis: &StatementAnalysis, affected: usize) -> String {
    use luxbyte_sql::StatementKind;
    match analysis.kind {
        StatementKind::Insert => format!("INSERT 0 {affected}"),
        StatementKind::Update => format!("UPDATE {affected}"),
        StatementKind::Delete => format!("DELETE {affected}"),
        _ => format!("SELECT {affected}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(tags: &[&str]) -> QueryResult {
        QueryResult {
            tags: tags.iter().map(|t| (*t).to_owned()).collect(),
            ..QueryResult::default()
        }
    }

    #[test]
    fn row_counts_are_read_from_the_command_tag() {
        assert_eq!(rows_affected(&result(&["INSERT 0 3"])), 3);
        assert_eq!(rows_affected(&result(&["UPDATE 7"])), 7);
        assert_eq!(rows_affected(&result(&["DELETE 0"])), 0);
        assert_eq!(rows_affected(&result(&["BEGIN"])), 0);
        assert_eq!(rows_affected(&result(&[])), 0);
    }

    #[test]
    fn a_batch_reports_the_last_tag_not_the_first() {
        // Settings are replayed in the same batch, so the first tags belong
        // to the SETs rather than to the client's statement.
        assert_eq!(rows_affected(&result(&["SET", "SET", "UPDATE 4"])), 4);
    }
}
