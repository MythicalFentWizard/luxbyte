//! One connection to a backend PostgreSQL server.

use std::time::{Duration, Instant};

use luxbyte_core::topology::Endpoint;
use luxbyte_proto::auth::{ScramClient, ScramKeys, md5_password};
use luxbyte_proto::backend::{Authentication, Backend, TransactionStatus};
use luxbyte_proto::startup::StartupParameters;
use luxbyte_proto::tag;
use luxbyte_proto::{Framed, Peer, ProtocolError, frontend};
use tokio::net::TcpStream;

use crate::error::PoolError;
use crate::pipeline::{Step, StepKind, StepReply};

/// How a connection authenticates.
#[derive(Clone, PartialEq, Eq)]
pub enum Credentials {
    /// The server is expected not to ask for a password.
    Trust,
    /// A password, used for whichever method the server asks for.
    Password(String),
    /// Keys a client proved it holds when it authenticated to the proxy,
    /// used to log in as that client. SCRAM only: no other method can be
    /// answered without the password itself.
    Scram(ScramKeys),
}

// Hand-written so that formatting a pool or a target into a log line can
// never print something that logs in as somebody.
impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Trust => "Trust",
            Self::Password(_) => "Password(..)",
            Self::Scram(_) => "Scram(..)",
        })
    }
}

impl Credentials {
    fn password(&self) -> Option<&str> {
        match self {
            Self::Trust | Self::Scram(_) => None,
            Self::Password(password) => Some(password),
        }
    }
}

/// Everything needed to open a connection.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConnectionTarget {
    /// Where to connect.
    pub host: String,
    /// Which port.
    pub port: u16,
    /// Which database.
    pub database: String,
    /// Which role.
    pub user: String,
}

impl ConnectionTarget {
    /// Builds a target from a topology endpoint.
    #[must_use]
    pub fn new(endpoint: &Endpoint, database: &str, user: &str) -> Self {
        Self {
            host: endpoint.host.clone(),
            port: endpoint.port,
            database: database.to_owned(),
            user: user.to_owned(),
        }
    }
}

impl std::fmt::Display for ConnectionTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}@{}:{}/{}",
            self.user, self.host, self.port, self.database
        )
    }
}

/// The result of a simple query.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryResult {
    /// Column names, from the last `RowDescription` seen.
    pub columns: Vec<String>,
    /// Datatype OID of each column.
    ///
    /// Kept because merging two regions' results in `ORDER BY` order needs
    /// to know whether a column compares as text or as a number. Sorting
    /// integers lexicographically puts 10 before 9.
    pub type_oids: Vec<i32>,
    /// Result rows, with `None` for SQL `NULL`.
    pub rows: Vec<Vec<Option<String>>>,
    /// Command tags, one per statement in the query.
    pub tags: Vec<String>,
    /// The transaction state the server reported afterwards.
    pub status: TransactionStatus,
}

impl QueryResult {
    /// The single value of a one-row, one-column result.
    #[must_use]
    pub fn scalar(&self) -> Option<&str> {
        match self.rows.as_slice() {
            [row] => row.first()?.as_deref(),
            _ => None,
        }
    }
}

/// An open connection to a backend.
#[derive(Debug)]
pub struct Connection {
    framed: Framed<TcpStream>,
    target: ConnectionTarget,
    status: TransactionStatus,
    /// Reported by the server, needed to route a later `CancelRequest`.
    backend_key: (i32, i32),
    parameters: Vec<(String, String)>,
    opened_at: Instant,
    returned_at: Instant,
    /// Set when the connection is known to be unusable, so it is retired
    /// rather than handed to someone else.
    poisoned: bool,
    /// Set when a statement changed session state that would outlive it —
    /// a `SET`, a `LISTEN`, a named prepared statement — so the connection
    /// is reset before anyone else borrows it.
    needs_reset: bool,
    /// Set while extended-query messages have been sent without a `Sync`.
    ///
    /// `ReadyForQuery` only arrives after a `Sync`, so until then the last
    /// reported status is stale: the server may be holding an implicit
    /// transaction open, and the next borrower would inherit it.
    awaiting_sync: bool,
}

impl Connection {
    /// Opens and authenticates a connection.
    pub async fn connect(
        target: &ConnectionTarget,
        credentials: &Credentials,
        connect_timeout: Duration,
    ) -> Result<Self, PoolError> {
        let address = format!("{}:{}", target.host, target.port);
        let stream = tokio::time::timeout(connect_timeout, TcpStream::connect(&address))
            .await
            .map_err(|_| PoolError::ConnectTimeout {
                target: target.to_string(),
                after: connect_timeout,
            })?
            .map_err(|error| PoolError::Connect {
                target: target.to_string(),
                source: error,
            })?;
        // Routing decisions are small and latency-sensitive; waiting 40 ms
        // for Nagle to fill a segment is the wrong trade here.
        stream.set_nodelay(true)?;

        let mut connection = Self {
            framed: Framed::new(stream, Peer::Backend),
            target: target.clone(),
            status: TransactionStatus::Idle,
            backend_key: (0, 0),
            parameters: Vec::new(),
            opened_at: Instant::now(),
            returned_at: Instant::now(),
            poisoned: false,
            needs_reset: false,
            awaiting_sync: false,
        };
        connection.handshake(credentials).await?;
        Ok(connection)
    }

    async fn handshake(&mut self, credentials: &Credentials) -> Result<(), PoolError> {
        let mut parameters = StartupParameters::new();
        parameters
            .set("user", &self.target.user)
            .set("database", &self.target.database)
            .set("application_name", "luxbyte");
        self.framed.queue_raw(&parameters.encode()?);
        self.framed.flush().await?;

        let mut scram: Option<ScramClient> = None;
        loop {
            let message = self.framed.read_message().await?;
            match Backend::decode(&message)? {
                Backend::Authentication(Authentication::Ok) => {
                    // Accepted. The handshake is not over: parameters, the
                    // cancellation key and `ReadyForQuery` still follow.
                    tracing::debug!(target = %self.target, "backend accepted authentication");
                }
                Backend::Authentication(Authentication::CleartextPassword) => {
                    let password = self.require_password(credentials)?;
                    let mut payload = password.as_bytes().to_vec();
                    payload.push(0);
                    self.framed.queue_raw(&frontend::password(&payload)?);
                    self.framed.flush().await?;
                }
                Backend::Authentication(Authentication::Md5Password { salt }) => {
                    let password = self.require_password(credentials)?;
                    let hashed = md5_password(&self.target.user, password, salt);
                    let mut payload = hashed.into_bytes();
                    payload.push(0);
                    self.framed.queue_raw(&frontend::password(&payload)?);
                    self.framed.flush().await?;
                }
                Backend::Authentication(Authentication::Sasl { mechanisms }) => {
                    if !mechanisms.contains(&luxbyte_proto::auth::SCRAM_SHA_256) {
                        return Err(PoolError::Protocol(ProtocolError::UnsupportedAuth(
                            mechanisms.join(", "),
                        )));
                    }
                    let mut client = match credentials {
                        Credentials::Scram(keys) => ScramClient::with_keys(keys.clone()),
                        _ => ScramClient::new(self.require_password(credentials)?),
                    };
                    let first = client.client_first();
                    let mut payload = Vec::new();
                    payload.extend_from_slice(luxbyte_proto::auth::SCRAM_SHA_256.as_bytes());
                    payload.push(0);
                    let length = i32::try_from(first.len()).unwrap_or(0);
                    payload.extend_from_slice(&length.to_be_bytes());
                    payload.extend_from_slice(first.as_bytes());
                    self.framed.queue_raw(&frontend::password(&payload)?);
                    self.framed.flush().await?;
                    scram = Some(client);
                }
                Backend::Authentication(Authentication::SaslContinue { data }) => {
                    let client = scram
                        .as_mut()
                        .ok_or_else(|| PoolError::Auth("SASL continue before start".to_owned()))?;
                    let server_first = std::str::from_utf8(data)
                        .map_err(|_| PoolError::Auth("SASL message is not utf-8".to_owned()))?;
                    let final_message = client.client_final(server_first)?;
                    self.framed
                        .queue_raw(&frontend::password(final_message.as_bytes())?);
                    self.framed.flush().await?;
                }
                Backend::Authentication(Authentication::SaslFinal { data }) => {
                    let client = scram
                        .as_ref()
                        .ok_or_else(|| PoolError::Auth("SASL final before start".to_owned()))?;
                    let server_final = std::str::from_utf8(data)
                        .map_err(|_| PoolError::Auth("SASL message is not utf-8".to_owned()))?;
                    // Proves the server holds the verifier. Skipping this
                    // turns SCRAM into an unauthenticated key exchange.
                    client.verify_server_final(server_final)?;
                }
                Backend::Authentication(Authentication::Unsupported { kind }) => {
                    return Err(PoolError::Protocol(ProtocolError::UnsupportedAuth(
                        format!("authentication method {kind}"),
                    )));
                }
                Backend::ParameterStatus { name, value } => {
                    self.parameters.push((name.to_owned(), value.to_owned()));
                }
                Backend::BackendKeyData {
                    process_id,
                    secret_key,
                } => self.backend_key = (process_id, secret_key),
                Backend::ReadyForQuery { status } => {
                    self.status = status;
                    return Ok(());
                }
                Backend::Error(notice) => return Err(PoolError::from(notice.to_error())),
                // Notices during startup are informational.
                _ => {}
            }
        }
    }

    fn require_password<'a>(&self, credentials: &'a Credentials) -> Result<&'a str, PoolError> {
        credentials.password().ok_or_else(|| {
            PoolError::Auth(match credentials {
                Credentials::Scram(_) => format!(
                    "{} asked for a password rather than SCRAM, and a client's password is \
                     never known to the proxy; set password_encryption = scram-sha-256 there",
                    self.target
                ),
                _ => format!(
                    "{} asked for a password and none was configured",
                    self.target
                ),
            })
        })
    }

    /// Runs a simple query and collects the whole response.
    ///
    /// The response is read to its `ReadyForQuery` whatever happens, because
    /// a connection left part-way through a response is unusable by anyone
    /// else — and returning it to the pool in that state is how one failed
    /// query corrupts the next unrelated one.
    pub async fn simple_query(&mut self, sql: &str) -> Result<QueryResult, PoolError> {
        self.framed.queue_raw(&frontend::query(sql)?);
        self.framed.flush().await?;
        self.collect_response().await
    }

    async fn collect_response(&mut self) -> Result<QueryResult, PoolError> {
        let mut result = QueryResult::default();
        let mut failure: Option<ProtocolError> = None;

        loop {
            let message = match self.framed.read_message().await {
                Ok(message) => message,
                Err(error) => {
                    // The stream is gone; nothing can be recovered from it.
                    self.poisoned = true;
                    return Err(PoolError::from(error));
                }
            };
            match Backend::decode(&message)? {
                Backend::RowDescription { fields } => {
                    result.columns = fields.iter().map(|field| field.name.to_owned()).collect();
                    result.type_oids = fields.iter().map(|field| field.type_oid).collect();
                }
                Backend::DataRow { columns } => {
                    result.rows.push(
                        columns
                            .iter()
                            .map(|value| {
                                value.map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                            })
                            .collect(),
                    );
                }
                Backend::CommandComplete { tag } => {
                    self.note(tag);
                    result.tags.push(tag.to_owned());
                }
                Backend::EmptyQueryResponse => result.tags.push(String::new()),
                Backend::Error(notice) => failure = Some(notice.to_error()),
                Backend::ReadyForQuery { status } => {
                    self.status = status;
                    result.status = status;
                    return match failure {
                        Some(error) => Err(PoolError::from(error)),
                        None => Ok(result),
                    };
                }
                // Notices, parameter changes and notifications can arrive at
                // any time and do not end the response.
                _ => {}
            }
        }
    }

    /// Sends extended-query messages in one write and reads the answer to
    /// each.
    ///
    /// Unless the last step is a `Sync`, a `Flush` is appended so the server
    /// answers what it has without ending the implicit transaction. That is
    /// what lets a caller hold a portal open across several exchanges; it is
    /// also why the connection is not reusable until a later `Sync`.
    ///
    /// Returns one [`StepReply`] per step. An error answer is a reply, not an
    /// `Err`: the server skips everything after it up to the next `Sync`, and
    /// those steps come back empty. `Err` means the connection itself failed
    /// or answered out of turn, and it has been poisoned.
    pub async fn pipeline(&mut self, steps: &[Step]) -> Result<Vec<StepReply>, PoolError> {
        for step in steps {
            self.framed.queue_raw(&step.message);
        }
        if steps.last().is_none_or(|step| step.kind != StepKind::Sync) {
            self.framed.queue_raw(&frontend::flush()?);
        }
        self.awaiting_sync = true;
        if let Err(error) = self.framed.flush().await {
            self.poisoned = true;
            return Err(PoolError::from(error));
        }

        let mut replies = Vec::with_capacity(steps.len());
        let mut skipping = false;
        for step in steps {
            let mut reply = StepReply::default();
            if skipping && step.kind != StepKind::Sync {
                replies.push(reply);
                continue;
            }
            loop {
                let message = match self.framed.read_message().await {
                    Ok(message) => message,
                    Err(error) => {
                        self.poisoned = true;
                        return Err(PoolError::from(error));
                    }
                };
                let kind = message.tag.unwrap_or(0);
                match kind {
                    // A notice belongs to whatever caused it, so it travels
                    // with that step's answer.
                    tag::NOTICE_RESPONSE => reply.messages.push(message),
                    // Asynchronous and not the client's business here: the
                    // proxy reports its own parameters, and LISTEN is not
                    // relayed.
                    tag::SYNC_OR_PARAMETER_STATUS | tag::NOTIFICATION_RESPONSE => {}
                    tag::EXECUTE_OR_ERROR => {
                        reply.failed = true;
                        reply.messages.push(message);
                        // A `Sync` can fail too — a deferred constraint is
                        // checked when the implicit transaction commits — and
                        // `ReadyForQuery` still follows.
                        if step.kind != StepKind::Sync {
                            skipping = true;
                            break;
                        }
                    }
                    tag::READY_FOR_QUERY if step.kind == StepKind::Sync => {
                        if let Backend::ReadyForQuery { status } = Backend::decode(&message)? {
                            self.status = status;
                        }
                        self.awaiting_sync = false;
                        skipping = false;
                        reply.messages.push(message);
                        break;
                    }
                    other if step.kind.ends_with(other) => {
                        if let Ok(Backend::CommandComplete { tag }) = Backend::decode(&message) {
                            self.note(tag);
                        }
                        reply.messages.push(message);
                        break;
                    }
                    other if step.kind.continues_with(other) => reply.messages.push(message),
                    // Anything else — `CopyInResponse` from an Execute, a
                    // stray `ReadyForQuery` — means the two sides disagree
                    // about where they are, and nothing read after this could
                    // be trusted.
                    other => {
                        self.poisoned = true;
                        return Err(PoolError::Protocol(ProtocolError::UnexpectedTag {
                            tag: char::from(other),
                            peer: Peer::Backend,
                        }));
                    }
                }
            }
            replies.push(reply);
        }
        Ok(replies)
    }

    /// Records what a completed command did to the session.
    ///
    /// Read from the command tag because that is where every route ends up:
    /// a `SET` the proxy replayed, one the client sent inside a transaction,
    /// one sent through the extended protocol. State that leaves no tag of
    /// its own, such as a temporary table, needs `reset = always`.
    fn note(&mut self, tag: &str) {
        let verb = tag.split(' ').next().unwrap_or("");
        let changes_session = matches!(
            verb,
            "SET" | "RESET" | "DISCARD" | "LISTEN" | "PREPARE" | "DECLARE"
        ) && tag != "PREPARE TRANSACTION";
        self.needs_reset |= changes_session;
    }

    /// Whether a statement left session state behind.
    #[must_use]
    pub const fn needs_reset(&self) -> bool {
        self.needs_reset
    }

    /// Marks the connection as needing a reset before reuse.
    pub const fn mark_for_reset(&mut self) {
        self.needs_reset = true;
    }

    pub(crate) const fn reset_done(&mut self) {
        self.needs_reset = false;
    }

    /// The transaction state last reported by the server.
    #[must_use]
    pub const fn status(&self) -> TransactionStatus {
        self.status
    }

    /// Where this connection goes.
    #[must_use]
    pub const fn target(&self) -> &ConnectionTarget {
        &self.target
    }

    /// The server's cancellation key.
    #[must_use]
    pub const fn backend_key(&self) -> (i32, i32) {
        self.backend_key
    }

    /// A parameter the server reported at startup.
    #[must_use]
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.parameters
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    /// Marks the connection unusable, so the pool retires it.
    pub const fn poison(&mut self) {
        self.poisoned = true;
    }

    /// Whether this connection can be handed to another caller.
    ///
    /// Only an idle connection qualifies. One still inside a transaction
    /// would leak that transaction — and its locks, and its snapshot — to
    /// whoever borrows it next.
    #[must_use]
    pub const fn is_reusable(&self) -> bool {
        !self.poisoned && !self.awaiting_sync && self.status.is_poolable()
    }

    /// How long since the connection was opened.
    #[must_use]
    pub fn age(&self) -> Duration {
        self.opened_at.elapsed()
    }

    /// How long the connection has been sitting in the pool.
    #[must_use]
    pub fn idle_for(&self) -> Duration {
        self.returned_at.elapsed()
    }

    pub(crate) fn mark_returned(&mut self) {
        self.returned_at = Instant::now();
    }

    /// Asks the server to close the connection politely.
    pub async fn close(mut self) {
        let _ = frontend::terminate().map(|bytes| self.framed.queue_raw(&bytes));
        let _ = self.framed.shutdown().await;
    }
}
