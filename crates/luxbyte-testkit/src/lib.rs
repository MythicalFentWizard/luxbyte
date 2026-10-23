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

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes;
use luxbyte_proto::backend::{TransactionStatus, build};
use luxbyte_proto::frontend::Frontend;
use luxbyte_proto::startup::{SSL_REQUEST_CODE, StartupRequest};
use luxbyte_proto::{Framed, Peer, ProtocolError};
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

    /// An error reply.
    #[must_use]
    pub fn fail(sqlstate: &str, message: &str) -> Self {
        Self::Fail {
            sqlstate: sqlstate.to_owned(),
            message: message.to_owned(),
        }
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
    /// Refuse to authenticate.
    pub refuse_auth: bool,
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
            refuse_auth: false,
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
    let mut framed = Framed::new(stream, Peer::Frontend);

    // The startup phase, including a possible TLS request first.
    let mut startup = framed.read_untagged().await?;
    if is_ssl_request(&startup) {
        // Decline TLS with the single byte the protocol specifies, then wait
        // for the real startup packet.
        framed.queue_raw(b"N");
        framed.flush().await?;
        startup = framed.read_untagged().await?;
    }
    let StartupRequest::Startup(parameters) = StartupRequest::decode(&startup)? else {
        return Ok(());
    };

    if config.refuse_auth {
        let error =
            build::ErrorBuilder::fatal("28P01", "password authentication failed").build()?;
        framed.queue_raw(&error);
        framed.flush().await?;
        return Ok(());
    }

    // Trust authentication: enough to exercise the pool's handshake without
    // making every test carry a password.
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
    framed.queue_raw(&build::backend_key_data(4_242, 2_424)?);
    framed.queue_raw(&build::ready_for_query(TransactionStatus::Idle)?);
    framed.flush().await?;

    let mut status = TransactionStatus::Idle;
    let mut kill = observed.kill.subscribe();
    loop {
        let read = tokio::select! {
            result = framed.read_message() => result,
            // Hang up mid-wait. Cancelling the read is safe because the
            // connection is being abandoned either way.
            _ = kill.recv() => return Ok(()),
        };
        let message = match read {
            Ok(message) => message,
            // The client hung up, which is how a pooled connection is
            // normally retired.
            Err(ProtocolError::Closed(_)) => return Ok(()),
            Err(error) => return Err(error),
        };

        match Frontend::decode(&message)? {
            Frontend::Query { sql } => {
                observed.statements.lock().push(sql.to_owned());

                if let Some(delay) = config.delay {
                    tokio::time::sleep(delay).await;
                }
                if config
                    .disconnect_on
                    .as_ref()
                    .is_some_and(|pattern| sql.contains(pattern))
                {
                    // Vanish mid-statement, with no reply at all.
                    let mut stream = framed.into_inner();
                    let _ = stream.shutdown().await;
                    return Ok(());
                }

                let replies = answer(sql, config, observed, &mut status)?;
                for reply in replies {
                    framed.queue_raw(&reply);
                }
                framed.queue_raw(&build::ready_for_query(status)?);
                framed.flush().await?;
            }
            Frontend::Terminate => return Ok(()),
            // `Sync` ends an extended-query batch, and everything else the
            // pool or the commit driver might send is acknowledged the same
            // way, so that a stray message cannot wedge a test.
            _ => {
                framed.queue_raw(&build::ready_for_query(status)?);
                framed.flush().await?;
            }
        }
    }
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
