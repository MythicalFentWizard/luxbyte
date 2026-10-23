//! Authenticating clients.
//!
//! Clients authenticate with SCRAM-SHA-256 against a verifier — the value
//! PostgreSQL keeps in `pg_authid.rolpassword` — so the proxy never holds a
//! password, and a compromised proxy leaks nothing a compromised database
//! would not have leaked already.
//!
//! Verifiers come from the configuration, from a query against the home
//! region, or both. A successful exchange also proves which keys the client
//! holds, and with `passthrough` the proxy logs in to every backend as that
//! client, with those keys. Privileges, row-level security and audit logs on
//! the backends then see the real role, not one shared superuser.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bytes::Bytes;
use luxbyte_pool::{Step, StepKind};
use luxbyte_proto::auth::{ScramVerifier, stand_in_verifier};
use luxbyte_proto::backend::Backend;
use luxbyte_proto::frontend;
use luxbyte_sql::Access;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::session::Shared;

/// How clients prove who they are.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Method {
    /// Anyone may connect as anyone.
    Trust,
    /// SCRAM-SHA-256 against a stored verifier.
    Scram,
}

/// One role and its stored verifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    /// The role name.
    pub name: String,
    /// Its verifier, in PostgreSQL's stored form:
    /// `SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>`.
    pub verifier: String,
}

/// The `[auth]` section.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
// Every field defaults, so a section can name only what it changes.
#[serde(default)]
pub struct AuthConfig {
    /// `trust` or `scram`. Unset means `scram` when any verifier source is
    /// configured and `trust` otherwise.
    pub method: Option<Method>,
    /// Roles with verifiers kept in the configuration.
    pub users: Vec<User>,
    /// A statement that returns a role's verifier, with the role name as
    /// `$1`, run on the home region's primary as the backend role. For
    /// example `SELECT rolpassword FROM pg_authid WHERE rolname = $1`, or a
    /// `SECURITY DEFINER` function that does the same for a role without
    /// superuser.
    pub query: Option<String>,
    /// How long a verifier read by `query` is trusted, in seconds.
    pub cache_ttl_s: u64,
    /// Log in to backends as the client's own role, using the keys its
    /// SCRAM exchange proved. Needs the role to have the same verifier on
    /// every backend, which `CREATE ROLE ... PASSWORD 'SCRAM-SHA-256$...'`
    /// makes easy.
    pub passthrough: bool,
    /// Roles allowed the operator's commands, `RELOCATE`, `DRAIN` and
    /// `UNDRAIN`. With `trust` nobody qualifies, since any client could
    /// claim any role.
    pub operators: Vec<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            method: None,
            users: Vec::new(),
            query: None,
            cache_ttl_s: 60,
            passthrough: true,
            operators: Vec::new(),
        }
    }
}

impl AuthConfig {
    /// The method in force once the defaults are applied.
    #[must_use]
    pub fn effective_method(&self) -> Method {
        self.method
            .unwrap_or(if self.users.is_empty() && self.query.is_none() {
                Method::Trust
            } else {
                Method::Scram
            })
    }
}

/// Why a verifier could not be found.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AuthError {
    /// A configured verifier is malformed.
    #[error("the verifier for \"{user}\" is malformed: {reason}")]
    BadVerifier {
        /// Whose.
        user: String,
        /// Why.
        reason: String,
    },
    /// The verifier query failed.
    #[error("could not read the verifier for \"{user}\": {reason}")]
    Lookup {
        /// Whose.
        user: String,
        /// Why.
        reason: String,
    },
}

/// Finds verifiers and remembers the ones it read.
pub struct Authenticator {
    config: AuthConfig,
    method: Method,
    users: HashMap<String, ScramVerifier>,
    cache: Mutex<HashMap<String, (Instant, Option<ScramVerifier>)>>,
    /// Keys the stand-in verifiers for roles that do not exist, so their
    /// salts are stable for this process and unpredictable to anyone else.
    secret: [u8; 32],
}

impl std::fmt::Debug for Authenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authenticator")
            .field("method", &self.method)
            .field("users", &self.users.len())
            .field("query", &self.config.query.is_some())
            .finish_non_exhaustive()
    }
}

impl Authenticator {
    /// Whether `role`, having authenticated, may run the operator's
    /// commands. Never under `trust`.
    #[must_use]
    pub fn is_operator(&self, role: &str) -> bool {
        self.method != Method::Trust && self.config.operators.iter().any(|name| name == role)
    }

    /// Validates the configuration, so a malformed verifier stops the proxy
    /// at startup rather than locking out a role at its first login.
    pub fn new(config: &AuthConfig) -> Result<Self, AuthError> {
        let mut users = HashMap::new();
        for user in &config.users {
            let verifier =
                ScramVerifier::parse(&user.verifier).map_err(|error| AuthError::BadVerifier {
                    user: user.name.clone(),
                    reason: error.to_string(),
                })?;
            users.insert(user.name.clone(), verifier);
        }
        let mut secret = [0_u8; 32];
        {
            use rand::Rng as _;
            rand::rng().fill_bytes(&mut secret);
        }
        Ok(Self {
            config: config.clone(),
            method: config.effective_method(),
            users,
            cache: Mutex::new(HashMap::new()),
            secret,
        })
    }

    /// The method in force.
    #[must_use]
    pub const fn method(&self) -> Method {
        self.method
    }

    /// Whether to log in to backends as the client.
    #[must_use]
    pub const fn passthrough(&self) -> bool {
        self.config.passthrough
    }

    /// The verifier to check `user` against.
    ///
    /// A role with no verifier gets a stand-in, so the exchange that follows
    /// fails exactly as a wrong password would, and does not tell the client
    /// which roles exist.
    pub async fn verifier(&self, shared: &Shared, user: &str) -> Result<ScramVerifier, AuthError> {
        if let Some(verifier) = self.users.get(user) {
            return Ok(verifier.clone());
        }
        if let Some(query) = &self.config.query {
            if let Some(verifier) = self.lookup(shared, query, user).await? {
                return Ok(verifier);
            }
        }
        stand_in_verifier(user, &self.secret).map_err(|error| AuthError::Lookup {
            user: user.to_owned(),
            reason: error.to_string(),
        })
    }

    async fn lookup(
        &self,
        shared: &Shared,
        query: &str,
        user: &str,
    ) -> Result<Option<ScramVerifier>, AuthError> {
        let ttl = Duration::from_secs(self.config.cache_ttl_s);
        if let Some((at, verifier)) = self.cache.lock().get(user) {
            if at.elapsed() < ttl {
                return Ok(verifier.clone());
            }
        }

        let failed = |reason: String| AuthError::Lookup {
            user: user.to_owned(),
            reason,
        };
        let home = shared.router.topology().home_region.clone();
        let pool = shared
            .pool_for(&home, Access::ReadWrite, 0)
            .ok_or_else(|| failed(format!("region {home} is not configured")))?;
        // A bound parameter, not text spliced into the statement: the role
        // name comes from an unauthenticated client.
        let steps = encode_lookup(query, user).map_err(|error| failed(error.to_string()))?;
        let replies = match pool.acquire().await {
            Ok(mut connection) => connection.pipeline(&steps).await,
            Err(error) => Err(error),
        }
        .map_err(|error| failed(error.to_string()))?;

        let mut text = None;
        for message in replies.iter().flat_map(|reply| &reply.messages) {
            match Backend::decode(message) {
                Ok(Backend::DataRow { columns }) => {
                    text = columns
                        .first()
                        .copied()
                        .flatten()
                        .map(|bytes| String::from_utf8_lossy(bytes).into_owned());
                }
                Ok(Backend::Error(notice)) => return Err(failed(notice.message().to_owned())),
                _ => {}
            }
        }
        // A role whose stored password is MD5 or absent cannot use SCRAM,
        // which is the same as not being able to log in at all.
        let verifier = text.and_then(|text| ScramVerifier::parse(&text).ok());
        self.cache
            .lock()
            .insert(user.to_owned(), (Instant::now(), verifier.clone()));
        Ok(verifier)
    }
}

fn encode_lookup(query: &str, user: &str) -> Result<Vec<Step>, luxbyte_proto::ProtocolError> {
    let parameter: [Option<&[u8]>; 1] = [Some(user.as_bytes())];
    Ok(vec![
        Step::new(StepKind::Parse, frontend::parse("", query, &[])?),
        Step::new(
            StepKind::Bind,
            frontend::bind("", "", &[], &parameter, &[])?,
        ),
        Step::new(StepKind::Execute, frontend::execute("", 2)?),
        Step::new(StepKind::Sync, frontend::sync()?),
    ])
}

/// Splits a `SASLInitialResponse` into its mechanism and its data.
pub fn sasl_initial(body: &Bytes) -> Option<(String, String)> {
    let mut reader = luxbyte_proto::buf::Reader::new(body);
    let mechanism = reader.cstr().ok()?.to_owned();
    let length = reader.i32().ok()?;
    let data = if length < 0 {
        String::new()
    } else {
        String::from_utf8(reader.take_rest().to_vec()).ok()?
    };
    Some((mechanism, data))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trust_is_the_default_only_when_nothing_could_be_checked() {
        assert_eq!(AuthConfig::default().effective_method(), Method::Trust);
        let with_query = AuthConfig {
            query: Some("SELECT 1".to_owned()),
            ..AuthConfig::default()
        };
        assert_eq!(with_query.effective_method(), Method::Scram);
    }

    #[test]
    fn a_malformed_verifier_is_caught_at_startup() {
        let config = AuthConfig {
            users: vec![User {
                name: "app".to_owned(),
                verifier: "md5abc".to_owned(),
            }],
            ..AuthConfig::default()
        };
        assert!(matches!(
            Authenticator::new(&config),
            Err(AuthError::BadVerifier { .. })
        ));
    }

    #[test]
    fn the_initial_response_is_split_into_mechanism_and_data() {
        let mut body = b"SCRAM-SHA-256\0".to_vec();
        body.extend_from_slice(&5_i32.to_be_bytes());
        body.extend_from_slice(b"n,,r=");
        let parsed = sasl_initial(&Bytes::from(body));
        assert_eq!(
            parsed,
            Some(("SCRAM-SHA-256".to_owned(), "n,,r=".to_owned()))
        );
    }
}
