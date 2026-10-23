//! An in-process PostgreSQL backend that speaks the real wire protocol.
//!
//! Testing a proxy properly needs a server at the other end. The usual
//! answer is Docker and a real PostgreSQL, which makes the test suite
//! something that runs on a good day on a machine someone set up — so the
//! interesting failure paths end up never being tested at all.
//!
//! This is the other answer: a backend that implements enough of the
//! protocol to be indistinguishable to a client, listening on a real
//! loopback socket, started in microseconds, with no external dependency of
//! any kind. The connection pool really connects to it over TCP, really
//! authenticates, and really has to handle it closing mid-query.
//!
//! The point is not to emulate PostgreSQL. It is to make the failures that
//! matter — a backend that dies between `PREPARE TRANSACTION` and
//! `COMMIT PREPARED`, one that refuses to prepare, one that hangs — happen
//! on demand, every run, in milliseconds.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use luxbyte_testkit::{Backend, BackendConfig};
//!
//! let backend = Backend::start(BackendConfig::new("eu-west")).await?;
//! println!("listening on {}", backend.addr());
//!
//! // ... point a pool at backend.addr() ...
//!
//! assert!(backend.statements().iter().any(|sql| sql.contains("SELECT")));
//! # Ok(())
//! # }
//! ```

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes;
use luxbyte_proto::backend::{TransactionStatus, build};
use luxbyte_proto::frontend::{Format, Frontend, Target};
use luxbyte_proto::startup::{SSL_REQUEST_CODE, StartupRequest};
use luxbyte_proto::{Framed, Peer, ProtocolError, Stream};
use parking_lot::Mutex;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

/// What a backend does with a statement it is asked to run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    /// Reply with a command tag and no rows.
    Done {
        /// The tag, such as `UPDATE 1`.
        tag: String,
    },
    /// Reply with rows.
    Rows {
        /// Column names.
        columns: Vec<String>,
        /// Row values, with `None` for SQL `NULL`.
        rows: Vec<Vec<Option<String>>>,
    },
    /// Reply with rows whose columns have the given type OIDs.
    TypedRows {
        /// Column names.
        columns: Vec<String>,
        /// Each column's type OID.
        types: Vec<i32>,
        /// Row values, with `None` for SQL `NULL`.
        rows: Vec<Vec<Option<String>>>,
    },
    /// Reply with an error.
    Fail {
        /// SQLSTATE to report.
        sqlstate: String,
        /// Message to report.
        message: String,
    },
    /// Stop answering and drop the connection without a reply.
    ///
    /// This is how a crash is simulated. A proxy that assumes every request
    /// gets a response hangs here, which is exactly what the test is for.
    Disconnect,
}

impl Reply {
    /// Rows from string literals, for brevity in tests.
    #[must_use]
    pub fn rows(columns: &[&str], rows: &[&[&str]]) -> Self {
        Self::Rows {
            columns: columns.iter().map(|c| (*c).to_owned()).collect(),
            rows: rows
                .iter()
                .map(|row| row.iter().map(|v| Some((*v).to_owned())).collect())
                .collect(),
        }
    }

    /// Typed rows from string literals: each column is a name and a type
    /// OID, such as `("id", 23)` for `int4`.
    #[must_use]
    pub fn typed(columns: &[(&str, i32)], rows: &[&[Option<&str>]]) -> Self {
        Self::TypedRows {
            columns: columns.iter().map(|(name, _)| (*name).to_owned()).collect(),
            types: columns.iter().map(|(_, oid)| *oid).collect(),
            rows: rows
                .iter()
                .map(|row| row.iter().map(|v| v.map(ToOwned::to_owned)).collect())
                .collect(),
        }
    }

    /// An error reply.
    #[must_use]
    pub fn fail(sqlstate: &str, message: &str) -> Self {
        Self::Fail {
            sqlstate: sqlstate.to_owned(),
            message: message.to_owned(),
        }
    }
}

/// A certificate authority and a server certificate it signed, made fresh
/// for a test.
///
/// Signed by a separate CA rather than self-signed, so a client verifying
/// the chain does the same work it would against a real deployment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestCertificates {
    /// The CA certificate, PEM: what a client trusts.
    pub ca_pem: String,
    /// The server certificate, PEM.
    pub cert_pem: String,
    /// The server's private key, PEM.
    pub key_pem: String,
}

impl TestCertificates {
    /// A CA, and a certificate from it naming each of `names` (host names
    /// or IP addresses).
    pub fn generate(names: &[&str]) -> Result<Self, rcgen::Error> {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
        let ca_key = KeyPair::generate()?;
        let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_cert = ca_params.self_signed(&ca_key)?;
        let issuer = Issuer::new(ca_params, ca_key);

        let server_key = KeyPair::generate()?;
        let names: Vec<String> = names.iter().map(|name| (*name).to_owned()).collect();
        let server_cert = CertificateParams::new(names)?.signed_by(&server_key, &issuer)?;
        Ok(Self {
            ca_pem: ca_cert.pem(),
            cert_pem: server_cert.pem(),
            key_pem: server_key.serialize_pem(),
        })
    }

    /// A TLS acceptor presenting the server certificate.
    pub fn acceptor(&self) -> Result<tokio_rustls::TlsAcceptor, ProtocolError> {
        use rustls_pki_types::pem::PemObject as _;
        let invalid = |error: &dyn std::fmt::Display| {
            ProtocolError::Io(std::io::Error::other(error.to_string()))
        };
        let certificates =
            rustls_pki_types::CertificateDer::pem_slice_iter(self.cert_pem.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| invalid(&error))?;
        let key = rustls_pki_types::PrivateKeyDer::from_pem_slice(self.key_pem.as_bytes())
            .map_err(|error| invalid(&error))?;
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|error| invalid(&error))?
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .map_err(|error| invalid(&error))?;
        Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
    }
}

/// How a prepared transaction ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedOutcome {
    /// `PREPARE TRANSACTION` succeeded; the transaction is in doubt.
    InDoubt,
    /// `COMMIT PREPARED` was received.
    Committed,
    /// `ROLLBACK PREPARED` was received.
    RolledBack,
}

/// How to behave.
#[derive(Debug, Clone)]
pub struct BackendConfig {
    /// A label, so a test can tell several backends apart.
    pub name: String,
    /// Replies scripted by substring match, in insertion order.
    pub scripted: Vec<(String, Reply)>,
    /// Refuse every `PREPARE TRANSACTION`.
    pub refuse_prepare: bool,
    /// Drop the connection when a statement matching this is received.
    pub disconnect_on: Option<String>,
    /// Delay every reply by this long.
    pub delay: Option<std::time::Duration>,
    /// Delay only statements containing this, when set.
    pub delay_only: Option<String>,
    /// Refuse to authenticate.
    pub refuse_auth: bool,
    /// Certificates to answer an `SSLRequest` with; without them the
    /// backend declines TLS.
    pub tls: Option<TestCertificates>,
    /// Roles that must authenticate with SCRAM, and their verifiers. When
    /// any are listed, every other role is refused.
    pub scram_users: Vec<(String, luxbyte_proto::auth::ScramVerifier)>,
}

impl BackendConfig {
    /// A backend that accepts everything and reports success.
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            scripted: Vec::new(),
            refuse_prepare: false,
            disconnect_on: None,
            delay: None,
            delay_only: None,
            refuse_auth: false,
            scram_users: Vec::new(),
            tls: None,
        }
    }

    /// Scripts a reply for statements containing `pattern`.
    #[must_use]
    pub fn reply_to(mut self, pattern: &str, reply: Reply) -> Self {
        self.scripted.push((pattern.to_owned(), reply));
        self
    }

    /// Makes every `PREPARE TRANSACTION` fail, so the commit path has to
    /// abort the whole distributed transaction.
    #[must_use]
    pub const fn refusing_prepare(mut self) -> Self {
        self.refuse_prepare = true;
        self
    }

    /// Drops the connection on a statement containing `pattern`.
    #[must_use]
    pub fn disconnecting_on(mut self, pattern: &str) -> Self {
        self.disconnect_on = Some(pattern.to_owned());
        self
    }

    /// Delays every reply, for timeout tests.
    #[must_use]
    pub const fn with_delay(mut self, delay: std::time::Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Accepts TLS with these certificates.
    #[must_use]
    pub fn with_tls(mut self, certificates: TestCertificates) -> Self {
        self.tls = Some(certificates);
        self
    }

    /// Requires SCRAM-SHA-256 from `user`, checked against `verifier`, the
    /// way a real server checks `pg_authid`.
    #[must_use]
    pub fn requiring_scram(
        mut self,
        user: &str,
        verifier: luxbyte_proto::auth::ScramVerifier,
    ) -> Self {
        self.scram_users.push((user.to_owned(), verifier));
        self
    }

    /// Delays only statements containing `pattern`, so a test can slow the
    /// statement it is about and nothing else.
    #[must_use]
    pub fn slow_on(mut self, pattern: &str, delay: std::time::Duration) -> Self {
        self.delay = Some(delay);
        self.delay_only = Some(pattern.to_owned());
        self
    }

    /// Rejects authentication, so the pool has to report it rather than
    /// retry forever.
    #[must_use]
    pub const fn refusing_auth(mut self) -> Self {
        self.refuse_auth = true;
        self
    }
}

/// Everything the backend observed, shared with the test.
#[derive(Debug)]
struct Observed {
    statements: Mutex<Vec<String>>,
    /// Parameters bound to each statement run over the extended protocol.
    bound: Mutex<Vec<(String, Vec<Option<String>>)>>,
    /// Statements run inside an implicit transaction whose connection closed
    /// before its `Sync`, which PostgreSQL rolls back.
    abandoned: Mutex<Vec<String>>,
    parses: AtomicU64,
    /// The role each accepted connection authenticated as.
    users: Mutex<Vec<String>>,
    tls_connections: AtomicU64,
    /// Wakes a connection's slow statement when a cancel names its key.
    cancel_waiters: Mutex<HashMap<(i32, i32), Arc<tokio::sync::Notify>>>,
    cancels: AtomicU64,
    next_process: AtomicU64,
    prepared: Mutex<BTreeMap<String, PreparedOutcome>>,
    connections: AtomicU64,
    live: AtomicU64,
    stopped: AtomicBool,
    /// Broadcast to every live session to make it hang up at once, which is
    /// how a server restart looks to a pool holding idle connections.
    kill: broadcast::Sender<()>,
}

impl Default for Observed {
    fn default() -> Self {
        let (kill, _) = broadcast::channel(16);
        Self {
            statements: Mutex::new(Vec::new()),
            bound: Mutex::new(Vec::new()),
            abandoned: Mutex::new(Vec::new()),
            parses: AtomicU64::new(0),
            users: Mutex::new(Vec::new()),
            tls_connections: AtomicU64::new(0),
            cancel_waiters: Mutex::new(HashMap::new()),
            cancels: AtomicU64::new(0),
            next_process: AtomicU64::new(1_000),
            prepared: Mutex::new(BTreeMap::new()),
            connections: AtomicU64::new(0),
            live: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
            kill,
        }
    }
}

/// A running backend.
///
/// Dropping it stops the listener and hangs up on every open connection, so
/// a test cannot leak a server into the next one.
#[derive(Debug)]
pub struct Backend {
    addr: SocketAddr,
    observed: Arc<Observed>,
    config: Arc<BackendConfig>,
    accept: tokio::task::JoinHandle<()>,
}

impl Drop for Backend {
    fn drop(&mut self) {
        self.observed.stopped.store(true, Ordering::SeqCst);
        let _ = self.observed.kill.send(());
        self.accept.abort();
    }
}

impl Backend {
    /// Starts a backend on an ephemeral loopback port.
    pub async fn start(config: BackendConfig) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let observed = Arc::new(Observed::default());
        let config = Arc::new(config);

        let accept_observed = Arc::clone(&observed);
        let accept_config = Arc::clone(&config);
        let accept = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                if accept_observed.stopped.load(Ordering::SeqCst) {
                    return;
                }
                accept_observed.connections.fetch_add(1, Ordering::SeqCst);
                accept_observed.live.fetch_add(1, Ordering::SeqCst);
                let session_observed = Arc::clone(&accept_observed);
                let session_config = Arc::clone(&accept_config);
                tokio::spawn(async move {
                    let _ = session(stream, &session_config, &session_observed).await;
                    session_observed.live.fetch_sub(1, Ordering::SeqCst);
                });
            }
        });

        Ok(Self {
            addr,
            observed,
            config,
            accept,
        })
    }

    /// Where to connect.
    #[must_use]
    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The port, for building an endpoint.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// This backend's label.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Every statement received, in order.
    #[must_use]
    pub fn statements(&self) -> Vec<String> {
        self.observed.statements.lock().clone()
    }

    /// Whether any statement contains `needle`.
    #[must_use]
    pub fn saw(&self, needle: &str) -> bool {
        self.observed
            .statements
            .lock()
            .iter()
            .any(|sql| sql.contains(needle))
    }

    /// How many statements contain `needle`.
    #[must_use]
    pub fn count_of(&self, needle: &str) -> usize {
        self.observed
            .statements
            .lock()
            .iter()
            .filter(|sql| sql.contains(needle))
            .count()
    }

    /// The parameters each extended-protocol statement was run with, in
    /// order. Binary values are rendered as hex, prefixed `\x` the way
    /// PostgreSQL prints a `bytea`.
    #[must_use]
    pub fn bound(&self) -> Vec<(String, Vec<Option<String>>)> {
        self.observed.bound.lock().clone()
    }

    /// Statements that ran inside an implicit transaction whose connection
    /// closed before the `Sync` that would have committed it.
    ///
    /// PostgreSQL rolls these back, so they are how a test observes that
    /// the proxy abandoned work rather than committing it.
    #[must_use]
    pub fn abandoned(&self) -> Vec<String> {
        self.observed.abandoned.lock().clone()
    }

    /// How many connections were encrypted.
    #[must_use]
    pub fn tls_connections(&self) -> u64 {
        self.observed.tls_connections.load(Ordering::SeqCst)
    }

    /// How many `CancelRequest`s have arrived.
    #[must_use]
    pub fn cancels(&self) -> u64 {
        self.observed.cancels.load(Ordering::SeqCst)
    }

    /// The role every accepted connection logged in as, in order.
    #[must_use]
    pub fn users(&self) -> Vec<String> {
        self.observed.users.lock().clone()
    }

    /// How many `Parse` messages have arrived.
    #[must_use]
    pub fn parses(&self) -> u64 {
        self.observed.parses.load(Ordering::SeqCst)
    }

    /// Forgets the statement log.
    pub fn clear(&self) {
        self.observed.statements.lock().clear();
    }

    /// How a prepared transaction ended, if it was ever prepared here.
    #[must_use]
    pub fn prepared(&self, gid: &str) -> Option<PreparedOutcome> {
        self.observed.prepared.lock().get(gid).copied()
    }

    /// Every prepared transaction this backend knows about.
    #[must_use]
    pub fn prepared_transactions(&self) -> Vec<(String, PreparedOutcome)> {
        self.observed
            .prepared
            .lock()
            .iter()
            .map(|(gid, outcome)| (gid.clone(), *outcome))
            .collect()
    }

    /// How many connections have ever been accepted.
    ///
    /// The number a pooling test cares about: it should stay small however
    /// many queries run.
    #[must_use]
    pub fn connections_accepted(&self) -> u64 {
        self.observed.connections.load(Ordering::SeqCst)
    }

    /// How many connections are open right now.
    #[must_use]
    pub fn connections_live(&self) -> u64 {
        self.observed.live.load(Ordering::SeqCst)
    }

    /// Stops accepting new connections.
    pub fn stop(&self) {
        self.observed.stopped.store(true, Ordering::SeqCst);
    }

    /// Hangs up on every open connection, without warning.
    ///
    /// What a pool sees when the server restarts, or when a firewall drops
    /// an idle connection: the sockets it holds are dead and it has no way
    /// to know until it uses one.
    pub fn close_connections(&self) {
        let _ = self.observed.kill.send(());
    }
}

/// Serves one connection.
async fn session(
    stream: TcpStream,
    config: &BackendConfig,
    observed: &Observed,
) -> Result<(), ProtocolError> {
    stream.set_nodelay(true)?;
    let mut framed = Framed::new(Stream::Plain(stream), Peer::Frontend);
    let mut first = framed.read_untagged().await?;
    if is_ssl_request(&first) {
        if let Some(certificates) = &config.tls {
            framed.queue_raw(b"S");
            framed.flush().await?;
            let Some(plain) = framed.into_inner().into_plain() else {
                return Ok(());
            };
            let Ok(encrypted) = certificates.acceptor()?.accept(plain).await else {
                return Ok(());
            };
            observed.tls_connections.fetch_add(1, Ordering::SeqCst);
            framed = Framed::new(Stream::Server(Box::new(encrypted)), Peer::Frontend);
        } else {
            // Decline TLS with the single byte the protocol specifies, then
            // wait for the real startup packet.
            framed.queue_raw(b"N");
            framed.flush().await?;
        }
        first = framed.read_untagged().await?;
    }
    let Some((key, cancelled)) = startup(&mut framed, &first, config, observed).await? else {
        return Ok(());
    };
    let outcome = serve(&mut framed, config, observed, &cancelled).await;
    observed.cancel_waiters.lock().remove(&key);
    if outcome? {
        // Vanish mid-statement, with no reply at all.
        let mut stream = framed.into_inner();
        let _ = stream.shutdown().await;
    }
    Ok(())
}

/// Serves statements until the client goes. Returns `true` when the backend
/// is meant to vanish mid-statement instead.
async fn serve(
    framed: &mut Framed<Stream>,
    config: &BackendConfig,
    observed: &Observed,
    cancelled: &tokio::sync::Notify,
) -> Result<bool, ProtocolError> {
    let mut status = TransactionStatus::Idle;
    let mut extended = Extended::default();
    let mut kill = observed.kill.subscribe();
    loop {
        let read = tokio::select! {
            result = framed.read_message() => result,
            // Hang up mid-wait. Cancelling the read is safe because the
            // connection is being abandoned either way.
            _ = kill.recv() => {
                extended.abandon(observed);
                return Ok(false);
            }
        };
        let message = match read {
            Ok(message) => message,
            // The client hung up, which is how a pooled connection is
            // normally retired.
            Err(ProtocolError::Closed(_)) => {
                extended.abandon(observed);
                return Ok(false);
            }
            Err(error) => return Err(error),
        };

        match Frontend::decode(&message)? {
            Frontend::Query { sql } => {
                observed.statements.lock().push(sql.to_owned());
                let replies = match pace(sql, config, cancelled).await {
                    Pace::Vanish => return Ok(true),
                    Pace::Cancelled => vec![canceled(&mut status)?],
                    Pace::Go => answer(sql, config, observed, &mut status)?,
                };
                for reply in replies {
                    framed.queue_raw(&reply);
                }
                framed.queue_raw(&build::ready_for_query(status)?);
                framed.flush().await?;
            }
            Frontend::Terminate => {
                extended.abandon(observed);
                return Ok(false);
            }
            Frontend::Sync => {
                extended.sync(status);
                framed.queue_raw(&build::ready_for_query(status)?);
                framed.flush().await?;
            }
            Frontend::Flush => framed.flush().await?,
            // After an error the server discards everything up to `Sync`.
            Frontend::Parse(_)
            | Frontend::Bind(_)
            | Frontend::Describe { .. }
            | Frontend::Execute { .. }
            | Frontend::Close { .. }
                if extended.skipping => {}
            Frontend::Execute { portal, max_rows } => {
                let Some(sql) = extended.portals.get(portal).map(|p| p.sql.clone()) else {
                    extended.fail(
                        framed,
                        "34000",
                        &format!("portal \"{portal}\" does not exist"),
                    )?;
                    continue;
                };
                match pace(&sql, config, cancelled).await {
                    Pace::Vanish => {
                        observed.statements.lock().push(sql);
                        extended.abandon(observed);
                        return Ok(true);
                    }
                    Pace::Cancelled => {
                        observed.statements.lock().push(sql);
                        extended.skipping = true;
                        framed.queue_raw(&canceled(&mut status)?);
                        continue;
                    }
                    Pace::Go => {}
                }
                for reply in extended.execute(portal, max_rows, config, observed, &mut status)? {
                    framed.queue_raw(&reply);
                }
            }
            Frontend::Parse(_)
            | Frontend::Bind(_)
            | Frontend::Describe { .. }
            | Frontend::Close { .. } => {
                observed_parse(&message, observed);
                for reply in extended.prepare(&message, config)? {
                    framed.queue_raw(&reply);
                }
            }
            // Everything else the pool or the commit driver might send is
            // acknowledged, so that a stray message cannot wedge a test.
            _ => {
                framed.queue_raw(&build::ready_for_query(status)?);
                framed.flush().await?;
            }
        }
    }
}

/// What to do with a statement once any configured delay is over.
enum Pace {
    Go,
    Vanish,
    Cancelled,
}

/// Applies the configured delay, during which a cancel can interrupt it, then
/// says whether this statement is the one to hang up on.
async fn pace(sql: &str, config: &BackendConfig, cancelled: &tokio::sync::Notify) -> Pace {
    let applies = config
        .delay_only
        .as_ref()
        .is_none_or(|pattern| sql.contains(pattern));
    if let Some(delay) = config.delay.filter(|_| applies) {
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            () = cancelled.notified() => return Pace::Cancelled,
        }
    }
    if config
        .disconnect_on
        .as_ref()
        .is_some_and(|pattern| sql.contains(pattern))
    {
        Pace::Vanish
    } else {
        Pace::Go
    }
}

/// The error PostgreSQL reports for a cancelled statement.
fn canceled(status: &mut TransactionStatus) -> Result<Bytes, ProtocolError> {
    if *status == TransactionStatus::InTransaction {
        *status = TransactionStatus::Failed;
    }
    build::ErrorBuilder::error("57014", "canceling statement due to user request").build()
}

/// Runs the startup phase. Returns the connection's cancel key and what a
/// cancel wakes, when the client went on to a session.
async fn startup(
    framed: &mut Framed<Stream>,
    startup: &luxbyte_proto::RawMessage,
    config: &BackendConfig,
    observed: &Observed,
) -> Result<Option<((i32, i32), Arc<tokio::sync::Notify>)>, ProtocolError> {
    let parameters = match StartupRequest::decode(startup)? {
        StartupRequest::Startup(parameters) => parameters,
        // A cancel names a connection by the key it was given, and wakes
        // whatever slow statement that connection is running.
        StartupRequest::Cancel(request) => {
            observed.cancels.fetch_add(1, Ordering::SeqCst);
            let key = (request.process_id, request.secret_key);
            if let Some(waiter) = observed.cancel_waiters.lock().get(&key) {
                waiter.notify_waiters();
            }
            return Ok(None);
        }
        _ => return Ok(None),
    };

    if config.refuse_auth {
        let error =
            build::ErrorBuilder::fatal("28P01", "password authentication failed").build()?;
        framed.queue_raw(&error);
        framed.flush().await?;
        return Ok(None);
    }

    let user = parameters.get("user").unwrap_or("").to_owned();
    if !config.scram_users.is_empty() {
        let verifier = config
            .scram_users
            .iter()
            .find(|(name, _)| *name == user)
            .map(|(_, verifier)| verifier.clone());
        let accepted = match verifier {
            Some(verifier) => scram(framed, verifier).await?,
            None => false,
        };
        if !accepted {
            let error = build::ErrorBuilder::fatal(
                "28P01",
                format!("password authentication failed for user \"{user}\""),
            )
            .build()?;
            framed.queue_raw(&error);
            framed.flush().await?;
            return Ok(None);
        }
    }
    observed.users.lock().push(user);

    // Otherwise trust authentication: enough to exercise the pool's
    // handshake without making every test carry a password.
    framed.queue_raw(&build::authentication_ok()?);
    for (name, value) in [
        ("server_version", "18.0 (luxbyte-testkit)"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
        (
            "application_name",
            parameters.get("application_name").unwrap_or(""),
        ),
    ] {
        framed.queue_raw(&build::parameter_status(name, value)?);
    }
    // A key of its own per connection, as PostgreSQL gives, so a cancel
    // reaches exactly one of them.
    let process = observed.next_process.fetch_add(1, Ordering::SeqCst);
    let process_id = i32::try_from(process).unwrap_or(i32::MAX);
    let key = (process_id, process_id ^ 0x5a5a_5a5a);
    let waiter = Arc::new(tokio::sync::Notify::new());
    observed
        .cancel_waiters
        .lock()
        .insert(key, Arc::clone(&waiter));
    framed.queue_raw(&build::backend_key_data(key.0, key.1)?);
    framed.queue_raw(&build::ready_for_query(TransactionStatus::Idle)?);
    framed.flush().await?;
    Ok(Some((key, waiter)))
}

/// Runs the server side of a SCRAM-SHA-256 exchange. Returns whether the
/// client proved it holds the password.
async fn scram(
    framed: &mut Framed<Stream>,
    verifier: luxbyte_proto::auth::ScramVerifier,
) -> Result<bool, ProtocolError> {
    use luxbyte_proto::auth::{SCRAM_SHA_256, ScramServer};

    framed.queue_raw(&build::authentication_sasl(SCRAM_SHA_256)?);
    framed.flush().await?;
    let initial = framed.read_message().await?;
    let mut reader = initial.reader();
    let _mechanism = reader.cstr()?;
    let _length = reader.i32()?;
    let client_first = String::from_utf8_lossy(reader.take_rest()).into_owned();

    let mut server = ScramServer::new(verifier);
    let Ok(server_first) = server.server_first(&client_first) else {
        return Ok(false);
    };
    framed.queue_raw(&build::authentication_sasl_continue(
        server_first.as_bytes(),
    )?);
    framed.flush().await?;

    let response = framed.read_message().await?;
    let client_final = String::from_utf8_lossy(&response.body).into_owned();
    match server.server_final(&client_final) {
        Ok(server_final) => {
            framed.queue_raw(&build::authentication_sasl_final(server_final.as_bytes())?);
            Ok(true)
        }
        Err(_) => Ok(false),
    }
}

fn observed_parse(message: &luxbyte_proto::RawMessage, observed: &Observed) {
    if matches!(Frontend::decode(message), Ok(Frontend::Parse(_))) {
        observed.parses.fetch_add(1, Ordering::SeqCst);
    }
}

/// A bound portal.
#[derive(Debug)]
struct Portal {
    sql: String,
    parameters: Vec<Option<String>>,
    /// For the proxy's query that renders parameters as typed text, the
    /// answer PostgreSQL would give.
    rendered: Option<Vec<Option<String>>>,
    /// Replies not yet sent, for an `Execute` with a row limit: the rows
    /// still to come, then whatever ends the result.
    cursor: Option<(VecDeque<Bytes>, Vec<Bytes>)>,
}

/// The extended-query state of one connection.
#[derive(Debug, Default)]
struct Extended {
    /// Statement text and declared parameter types, by name.
    statements: HashMap<String, (String, Vec<i32>)>,
    portals: HashMap<String, Portal>,
    /// Set after an error, until the next `Sync`.
    skipping: bool,
    /// Statements run since the last `Sync` outside an explicit
    /// transaction: PostgreSQL's implicit transaction, which commits at the
    /// `Sync` and is rolled back if the connection goes first.
    implicit: Vec<String>,
}

impl Extended {
    fn sync(&mut self, status: TransactionStatus) {
        self.skipping = false;
        self.implicit.clear();
        // Portals end with their transaction.
        if status == TransactionStatus::Idle {
            self.portals.clear();
        }
    }

    fn abandon(&mut self, observed: &Observed) {
        observed
            .abandoned
            .lock()
            .extend(std::mem::take(&mut self.implicit));
    }

    fn fail(
        &mut self,
        framed: &mut Framed<Stream>,
        sqlstate: &str,
        message: &str,
    ) -> Result<(), ProtocolError> {
        self.skipping = true;
        framed.queue_raw(&build::ErrorBuilder::error(sqlstate, message).build()?);
        Ok(())
    }

    /// Answers `Parse`, `Bind`, `Describe` and `Close`.
    fn prepare(
        &mut self,
        message: &luxbyte_proto::RawMessage,
        config: &BackendConfig,
    ) -> Result<Vec<Bytes>, ProtocolError> {
        let error = |this: &mut Self, sqlstate: &str, text: String| {
            this.skipping = true;
            Ok(vec![build::ErrorBuilder::error(sqlstate, text).build()?])
        };
        match Frontend::decode(message)? {
            Frontend::Parse(parse) => {
                if !parse.name.is_empty() && self.statements.contains_key(parse.name) {
                    return error(
                        self,
                        "42P05",
                        format!("prepared statement \"{}\" already exists", parse.name),
                    );
                }
                self.statements.insert(
                    parse.name.to_owned(),
                    (parse.sql.to_owned(), parse.parameter_types.clone()),
                );
                Ok(vec![build::parse_complete()?])
            }
            Frontend::Bind(bind) => {
                let Some((sql, types)) = self.statements.get(bind.statement).cloned() else {
                    return error(
                        self,
                        "26000",
                        format!("prepared statement \"{}\" does not exist", bind.statement),
                    );
                };
                let parameters = render_parameters(&bind);
                let rendered = sql
                    .starts_with(RENDER_PREFIX)
                    .then(|| render_typed(&bind, &types));
                self.portals.insert(
                    bind.portal.to_owned(),
                    Portal {
                        sql,
                        parameters,
                        rendered,
                        cursor: None,
                    },
                );
                Ok(vec![build::bind_complete()?])
            }
            Frontend::Describe { target, name } => {
                let sql = match target {
                    Target::Statement => self.statements.get(name).map(|(sql, _)| sql.clone()),
                    Target::Portal => self.portals.get(name).map(|p| p.sql.clone()),
                };
                let Some(sql) = sql else {
                    return error(self, "26000", format!("\"{name}\" does not exist"));
                };
                let mut out = Vec::new();
                if target == Target::Statement {
                    // Every placeholder is reported as `text`; a test that
                    // needs another type declares it in the `Parse`.
                    out.push(build::parameter_description(&vec![25; placeholders(&sql)])?);
                }
                out.push(describe_rows(&sql, config)?);
                Ok(out)
            }
            Frontend::Close { target, name } => {
                match target {
                    Target::Statement => self.statements.remove(name).map(|(sql, _)| sql),
                    Target::Portal => self.portals.remove(name).map(|p| p.sql),
                };
                Ok(vec![build::close_complete()?])
            }
            _ => Ok(Vec::new()),
        }
    }

    /// Answers an `Execute`.
    fn execute(
        &mut self,
        portal_name: &str,
        max_rows: i32,
        config: &BackendConfig,
        observed: &Observed,
        status: &mut TransactionStatus,
    ) -> Result<Vec<Bytes>, ProtocolError> {
        let Some(portal) = self.portals.get_mut(portal_name) else {
            return Ok(Vec::new());
        };
        if portal.cursor.is_none() {
            observed.statements.lock().push(portal.sql.clone());
            observed
                .bound
                .lock()
                .push((portal.sql.clone(), portal.parameters.clone()));
            if *status == TransactionStatus::Idle {
                self.implicit.push(portal.sql.clone());
            }
            let replies = match &portal.rendered {
                Some(values) => {
                    let values: Vec<Option<&str>> = values.iter().map(Option::as_deref).collect();
                    vec![
                        build::data_row(&values)?,
                        build::command_complete("SELECT 1")?,
                    ]
                }
                None => answer(&portal.sql, config, observed, status)?,
            };
            // An `Execute` never repeats the `RowDescription`; that is what
            // `Describe` is for.
            let (rows, tail): (Vec<Bytes>, Vec<Bytes>) = replies
                .into_iter()
                .filter(|reply| reply.first() != Some(&b'T'))
                .partition(|reply| reply.first() == Some(&b'D'));
            portal.cursor = Some((rows.into(), tail));
        }
        let Some((rows, tail)) = portal.cursor.as_mut() else {
            return Ok(Vec::new());
        };
        let limit = usize::try_from(max_rows).unwrap_or(0);
        let mut out = Vec::new();
        if limit > 0 && rows.len() > limit {
            out.extend(rows.drain(..limit));
            out.push(build::portal_suspended()?);
            return Ok(out);
        }
        out.extend(rows.drain(..));
        out.extend(tail.iter().cloned());
        if tail.iter().any(|reply| reply.first() == Some(&b'E')) {
            self.skipping = true;
        }
        Ok(out)
    }
}

/// How many placeholders a statement uses, taken as the highest `$n`.
fn placeholders(sql: &str) -> usize {
    let mut highest = 0;
    let mut rest = sql;
    while let Some(at) = rest.find('$') {
        let after = rest.get(at.saturating_add(1)..).unwrap_or("");
        let digits: String = after.chars().take_while(char::is_ascii_digit).collect();
        if let Ok(number) = digits.parse::<usize>() {
            highest = highest.max(number);
        }
        rest = after;
    }
    highest
}

/// What a `Describe` reports about a statement's result columns.
fn describe_rows(sql: &str, config: &BackendConfig) -> Result<Bytes, ProtocolError> {
    let scripted = config
        .scripted
        .iter()
        .find(|(pattern, _)| sql.contains(pattern.as_str()));
    match scripted {
        Some((_, Reply::Rows { columns, .. })) => {
            let names: Vec<&str> = columns.iter().map(String::as_str).collect();
            build::row_description(&names, 25)
        }
        Some((_, Reply::TypedRows { columns, types, .. })) => {
            let names: Vec<&str> = columns.iter().map(String::as_str).collect();
            build::row_description_typed(&names, types)
        }
        _ => build::no_data(),
    }
}

/// How the proxy's parameter-rendering query begins.
const RENDER_PREFIX: &str = "SELECT $1::text, pg_catalog.pg_typeof($1)::text";

/// What PostgreSQL answers to the rendering query: each parameter as text,
/// then its type's name. Binary values are decoded for the types the
/// testkit knows; a parameter with no declared type is `text`, as the
/// `Describe` reported.
fn render_typed(bind: &luxbyte_proto::frontend::Bind<'_>, types: &[i32]) -> Vec<Option<String>> {
    let mut out = Vec::with_capacity(bind.parameters.len().saturating_mul(2));
    for (index, value) in bind.parameters.iter().enumerate() {
        let oid = types.get(index).copied().unwrap_or(0);
        out.push(value.map(|bytes| {
            match bind.parameter_format(index) {
                Format::Text => String::from_utf8_lossy(bytes).into_owned(),
                Format::Binary => luxbyte_proto::types::text_from_binary(oid, bytes)
                    .unwrap_or_else(|| String::from_utf8_lossy(bytes).into_owned()),
            }
        }));
        out.push(Some(
            match oid {
                16 => "boolean",
                20 => "bigint",
                21 => "smallint",
                23 => "integer",
                1043 => "character varying",
                2950 => "uuid",
                _ => "text",
            }
            .to_owned(),
        ));
    }
    out
}

/// Renders bound parameters for the log: text as itself, binary as hex.
fn render_parameters(bind: &luxbyte_proto::frontend::Bind<'_>) -> Vec<Option<String>> {
    bind.parameters
        .iter()
        .enumerate()
        .map(|(index, value)| {
            value.map(|bytes| match bind.parameter_format(index) {
                Format::Text => String::from_utf8_lossy(bytes).into_owned(),
                Format::Binary => bytes.iter().fold(String::from("\\x"), |mut out, byte| {
                    use std::fmt::Write as _;
                    let _ = write!(out, "{byte:02x}");
                    out
                }),
            })
        })
        .collect()
}

fn is_ssl_request(message: &luxbyte_proto::RawMessage) -> bool {
    message
        .body
        .get(..4)
        .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
        .is_some_and(|bytes| i32::from_be_bytes(bytes) == SSL_REQUEST_CODE)
}

/// Works out the reply to one statement, updating transaction state.
fn answer(
    sql: &str,
    config: &BackendConfig,
    observed: &Observed,
    status: &mut TransactionStatus,
) -> Result<Vec<Bytes>, ProtocolError> {
    let trimmed = sql.trim();
    // `to_ascii_uppercase` keeps byte offsets aligned with the original, so
    // the verb can be matched case-insensitively while the identifier is
    // still sliced out of the text as the client wrote it. Uppercasing the
    // identifier too would make every gid fail to match on the way back.
    let upper = trimmed.to_ascii_uppercase();

    // Two-phase commit verbs, which the coordinator's tests assert on.
    if let Some(gid) = gid_after(&upper, trimmed, "PREPARE TRANSACTION") {
        if config.refuse_prepare {
            *status = TransactionStatus::Failed;
            return Ok(vec![
                build::ErrorBuilder::error("25P02", "prepare refused by this backend").build()?,
            ]);
        }
        observed
            .prepared
            .lock()
            .insert(gid, PreparedOutcome::InDoubt);
        *status = TransactionStatus::Idle;
        return Ok(vec![build::command_complete("PREPARE TRANSACTION")?]);
    }
    if let Some(gid) = gid_after(&upper, trimmed, "COMMIT PREPARED") {
        let known = observed
            .prepared
            .lock()
            .insert(gid.clone(), PreparedOutcome::Committed);
        *status = TransactionStatus::Idle;
        if known.is_none() {
            // PostgreSQL rejects a gid it never prepared, and a recovery
            // path that cannot cope with that will loop forever.
            observed.prepared.lock().remove(&gid);
            return Ok(vec![
                build::ErrorBuilder::error(
                    "42704",
                    format!("prepared transaction with identifier \"{gid}\" does not exist"),
                )
                .build()?,
            ]);
        }
        return Ok(vec![build::command_complete("COMMIT PREPARED")?]);
    }
    if let Some(gid) = gid_after(&upper, trimmed, "ROLLBACK PREPARED") {
        let known = observed
            .prepared
            .lock()
            .insert(gid.clone(), PreparedOutcome::RolledBack);
        *status = TransactionStatus::Idle;
        if known.is_none() {
            observed.prepared.lock().remove(&gid);
            return Ok(vec![
                build::ErrorBuilder::error(
                    "42704",
                    format!("prepared transaction with identifier \"{gid}\" does not exist"),
                )
                .build()?,
            ]);
        }
        return Ok(vec![build::command_complete("ROLLBACK PREPARED")?]);
    }

    if upper.starts_with("BEGIN") || upper.starts_with("START TRANSACTION") {
        *status = TransactionStatus::InTransaction;
        return Ok(vec![build::command_complete("BEGIN")?]);
    }
    if upper.starts_with("COMMIT") {
        *status = TransactionStatus::Idle;
        return Ok(vec![build::command_complete("COMMIT")?]);
    }
    if upper.starts_with("ROLLBACK") {
        *status = TransactionStatus::Idle;
        return Ok(vec![build::command_complete("ROLLBACK")?]);
    }

    // A scripted reply, if one matches.
    if let Some((_, reply)) = config
        .scripted
        .iter()
        .find(|(pattern, _)| sql.contains(pattern.as_str()))
    {
        return scripted_reply(reply, status);
    }

    // Session-level commands report their own tags, which is how a pool
    // tells that a connection needs resetting.
    for (verb, tag) in [
        ("SET ", "SET"),
        ("RESET", "RESET"),
        ("DISCARD", "DISCARD ALL"),
    ] {
        if upper.starts_with(verb) {
            return Ok(vec![build::command_complete(tag)?]);
        }
    }

    // Otherwise report success without rows, which is what a write does.
    if *status == TransactionStatus::InTransaction {
        // Stay in the transaction.
    }
    Ok(vec![build::command_complete("SELECT 0")?])
}

fn scripted_reply(
    reply: &Reply,
    status: &mut TransactionStatus,
) -> Result<Vec<Bytes>, ProtocolError> {
    match reply {
        Reply::Done { tag } => Ok(vec![build::command_complete(tag)?]),
        Reply::TypedRows {
            columns,
            types,
            rows,
        } => {
            let names: Vec<&str> = columns.iter().map(String::as_str).collect();
            let mut out = vec![build::row_description_typed(&names, types)?];
            for row in rows {
                let values: Vec<Option<&str>> = row.iter().map(|value| value.as_deref()).collect();
                out.push(build::data_row(&values)?);
            }
            out.push(build::command_complete(&format!("SELECT {}", rows.len()))?);
            Ok(out)
        }
        Reply::Rows { columns, rows } => {
            let names: Vec<&str> = columns.iter().map(String::as_str).collect();
            // 25 is `text`, which is what every value here is.
            let mut out = vec![build::row_description(&names, 25)?];
            for row in rows {
                let values: Vec<Option<&str>> = row.iter().map(|value| value.as_deref()).collect();
                out.push(build::data_row(&values)?);
            }
            out.push(build::command_complete(&format!("SELECT {}", rows.len()))?);
            Ok(out)
        }
        Reply::Fail { sqlstate, message } => {
            // A failed statement inside a transaction poisons it, exactly as
            // PostgreSQL does, so the proxy has to notice and roll back.
            if *status == TransactionStatus::InTransaction {
                *status = TransactionStatus::Failed;
            }
            Ok(vec![
                build::ErrorBuilder::error(sqlstate, message.clone()).build()?,
            ])
        }
        Reply::Disconnect => Ok(Vec::new()),
    }
}

/// Extracts the identifier from `PREPARE TRANSACTION 'gid'` and friends.
///
/// `upper` is matched against the verb; the identifier is sliced out of
/// `original` at the same offsets, so its case survives. Uppercasing the
/// identifier along with the verb is an easy mistake and makes every
/// `COMMIT PREPARED` fail to find what it prepared.
fn gid_after(upper: &str, original: &str, verb: &str) -> Option<String> {
    if !upper.starts_with(verb) {
        return None;
    }
    let open = upper.find('\'')?;
    let start = open.checked_add(1)?;
    let close = upper.get(start..)?.find('\'')?;
    let end = start.checked_add(close)?;
    original.get(start..end).map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_a_gid_from_each_two_phase_verb() {
        let statement = "PREPARE TRANSACTION 'luxbyte-eu-1-0'";
        assert_eq!(
            gid_after(
                &statement.to_ascii_uppercase(),
                statement,
                "PREPARE TRANSACTION"
            ),
            Some("luxbyte-eu-1-0".to_owned()),
            "the identifier keeps the case the client sent"
        );

        let statement = "COMMIT PREPARED 'MixedCase'";
        assert_eq!(
            gid_after(
                &statement.to_ascii_uppercase(),
                statement,
                "COMMIT PREPARED"
            ),
            Some("MixedCase".to_owned())
        );
        assert_eq!(gid_after("COMMIT", "COMMIT", "COMMIT PREPARED"), None);
        assert_eq!(
            gid_after(
                "PREPARE TRANSACTION",
                "PREPARE TRANSACTION",
                "PREPARE TRANSACTION"
            ),
            None
        );
    }

    #[tokio::test]
    async fn a_backend_starts_on_a_real_port_and_reports_it() {
        let backend = Backend::start(BackendConfig::new("eu-west"))
            .await
            .expect("should start");
        assert!(backend.port() > 0);
        assert_eq!(backend.name(), "eu-west");
        assert_eq!(backend.connections_accepted(), 0);
    }
}
