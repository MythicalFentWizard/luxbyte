//! Reading the proxy's configuration.

use std::path::PathBuf;
use std::time::Duration;

use luxbyte_core::Topology;
use luxbyte_pool::{Credentials, PoolConfig, ResetPolicy};
use serde::{Deserialize, Serialize};

/// The whole configuration file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    /// How the proxy listens.
    #[serde(default)]
    pub server: ServerConfig,
    /// How the proxy connects to backends.
    #[serde(default)]
    pub backend: BackendConfig,
    /// How connections are pooled.
    #[serde(default)]
    pub pool: PoolSettings,
    /// How clients authenticate.
    #[serde(default)]
    pub auth: crate::auth::AuthConfig,
    /// Regions and table placement.
    pub topology: Topology,
    /// Keys to seed the directory with, for a deployment that does not yet
    /// have a directory of its own.
    #[serde(default)]
    pub directory: Vec<DirectoryEntry>,
}

/// One pre-placed key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectoryEntry {
    /// The table the key belongs to.
    pub table: String,
    /// The key value, as text.
    pub key: String,
    /// The region holding it.
    pub region: String,
}

/// Listener settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
// Every field defaults, so a section can name only what it changes
// rather than having to restate the whole block.
#[serde(default)]
pub struct ServerConfig {
    /// Address to listen on.
    pub listen: String,
    /// This proxy's name.
    ///
    /// Must be unique across the deployment: it goes into every
    /// `PREPARE TRANSACTION` identifier, and it is what keeps two proxies
    /// from resolving each other's in-doubt transactions.
    pub node: String,
    /// Where commit decisions are written.
    ///
    /// Without it, a crash between prepare and commit leaves prepared
    /// transactions pinned on the participants until someone resolves them
    /// by hand, so the proxy says so loudly at startup.
    pub commit_log: Option<PathBuf>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:6432".to_owned(),
            node: "luxbyte".to_owned(),
            commit_log: None,
        }
    }
}

/// How to reach the backends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
// Every field defaults, so a section can name only what it changes
// rather than having to restate the whole block.
#[serde(default)]
pub struct BackendConfig {
    /// Role to connect as.
    pub user: String,
    /// Password, if the backends ask for one.
    pub password: Option<String>,
    /// Database to connect to.
    pub database: String,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            user: "postgres".to_owned(),
            password: None,
            database: "postgres".to_owned(),
        }
    }
}

impl BackendConfig {
    /// The credentials to authenticate with.
    #[must_use]
    pub fn credentials(&self) -> Credentials {
        self.password
            .as_ref()
            .map_or(Credentials::Trust, |password| {
                Credentials::Password(password.clone())
            })
    }
}

/// Pool sizing and timeouts, in units a configuration file can express.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
// Every field defaults, so a section can name only what it changes
// rather than having to restate the whole block.
#[serde(default)]
pub struct PoolSettings {
    /// Most connections per backend, database and role.
    pub max_size: usize,
    /// Milliseconds to wait for a free connection.
    pub acquire_timeout_ms: u64,
    /// Milliseconds to wait for a new connection to open.
    pub connect_timeout_ms: u64,
    /// Seconds a connection may sit unused.
    pub idle_timeout_s: u64,
    /// Seconds a connection may live.
    pub max_lifetime_s: u64,
    /// When a returned connection is reset: `when_changed`, `always` or
    /// `never`.
    pub reset: Reset,
    /// The statement that resets one.
    pub reset_query: String,
}

/// When a returned connection is reset, as the configuration spells it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reset {
    /// Never.
    Never,
    /// When a statement visibly changed the session, such as a `SET`.
    WhenChanged,
    /// Every time, which also catches temporary tables.
    Always,
}

impl Default for PoolSettings {
    fn default() -> Self {
        Self {
            max_size: 16,
            acquire_timeout_ms: 5_000,
            connect_timeout_ms: 5_000,
            idle_timeout_s: 300,
            max_lifetime_s: 1_800,
            reset: Reset::WhenChanged,
            reset_query: "DISCARD ALL".to_owned(),
        }
    }
}

impl PoolSettings {
    /// Converts to the pool's own configuration.
    #[must_use]
    pub fn to_pool_config(&self) -> PoolConfig {
        PoolConfig {
            max_size: self.max_size,
            acquire_timeout: Duration::from_millis(self.acquire_timeout_ms),
            connect_timeout: Duration::from_millis(self.connect_timeout_ms),
            idle_timeout: Duration::from_secs(self.idle_timeout_s),
            max_lifetime: Duration::from_secs(self.max_lifetime_s),
            reset: match self.reset {
                Reset::Never => ResetPolicy::Never,
                Reset::WhenChanged => ResetPolicy::WhenChanged,
                Reset::Always => ResetPolicy::Always,
            },
            reset_query: self.reset_query.clone(),
        }
    }
}

/// Why a configuration was rejected.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// The file could not be read.
    #[error("could not read {path}: {source}")]
    Read {
        /// Which file.
        path: String,
        /// Why.
        source: std::io::Error,
    },
    /// The file was not valid TOML, or did not match the schema.
    #[error("could not parse {path}: {source}")]
    Parse {
        /// Which file.
        path: String,
        /// Why.
        source: toml::de::Error,
    },
    /// The topology itself is inconsistent.
    #[error("{0}")]
    Topology(#[from] luxbyte_core::TopologyError),
}

impl Config {
    /// Parses and validates a configuration from TOML text.
    pub fn from_toml(text: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(text).map_err(|source| ConfigError::Parse {
            path: "<inline>".to_owned(),
            source,
        })?;
        config.topology.validate()?;
        Ok(config)
    }

    /// Reads and validates a configuration file.
    pub fn load(path: &str) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        let config: Self = toml::from_str(&text).map_err(|source| ConfigError::Parse {
            path: path.to_owned(),
            source,
        })?;
        // Every invariant the router relies on is checked once, here,
        // rather than per query.
        config.topology.validate()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shipped_example_loads_and_its_auth_is_valid() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../examples/luxbyte.toml");
        let config = Config::load(path).expect("the example should load");
        let auth = crate::auth::Authenticator::new(&config.auth).expect("auth should validate");
        assert_eq!(auth.method(), crate::auth::Method::Scram);
    }

    const SAMPLE: &str = r#"
[server]
listen = "0.0.0.0:6432"
node = "eu-proxy-1"
commit_log = "/var/lib/luxbyte/commit-log"

[backend]
user = "app"
password = "hunter2"
database = "shop"

[pool]
max_size = 32

[topology]
home_region = "eu-west"

[[topology.regions]]
id = "eu-west"
primary = { host = "eu.db" }

[[topology.tables]]
table = "users"
placement = "geo_partitioned"
key = "tenant_id"

[[directory]]
table = "users"
key = "alice"
region = "eu-west"
"#;

    #[test]
    fn reads_every_section() {
        let config: Config = toml::from_str(SAMPLE).expect("should parse");
        assert_eq!(config.server.listen, "0.0.0.0:6432");
        assert_eq!(config.server.node, "eu-proxy-1");
        assert_eq!(config.backend.database, "shop");
        assert_eq!(config.pool.max_size, 32);
        assert_eq!(config.topology.home_region, "eu-west");
        assert_eq!(config.directory.len(), 1);
    }

    #[test]
    fn a_section_may_set_one_field_and_leave_the_rest() {
        let partial = r#"
[pool]
max_size = 64

[server]
node = "eu-1"

[topology]
home_region = "eu-west"
[[topology.regions]]
id = "eu-west"
primary = { host = "eu.db" }
"#;
        let config: Config = toml::from_str(partial).expect("should parse");
        assert_eq!(config.pool.max_size, 64);
        assert_eq!(config.pool.acquire_timeout_ms, 5_000, "untouched default");
        assert_eq!(config.server.node, "eu-1");
        assert_eq!(config.server.listen, "127.0.0.1:6432", "untouched default");
    }

    #[test]
    fn omitted_sections_take_their_defaults() {
        let minimal = r#"
[topology]
home_region = "eu-west"
[[topology.regions]]
id = "eu-west"
primary = { host = "eu.db" }
"#;
        let config: Config = toml::from_str(minimal).expect("should parse");
        assert_eq!(config.server.listen, "127.0.0.1:6432");
        assert_eq!(config.backend.user, "postgres");
        assert_eq!(config.pool.max_size, 16);
        assert_eq!(config.directory, Vec::new());
    }

    #[test]
    fn a_password_becomes_credentials_and_its_absence_becomes_trust() {
        let config: Config = toml::from_str(SAMPLE).expect("should parse");
        assert_eq!(
            config.backend.credentials(),
            Credentials::Password("hunter2".to_owned())
        );
        assert_eq!(BackendConfig::default().credentials(), Credentials::Trust);
    }

    #[test]
    fn pool_settings_convert_to_durations() {
        let settings = PoolSettings {
            acquire_timeout_ms: 1_500,
            idle_timeout_s: 60,
            ..PoolSettings::default()
        };
        let config = settings.to_pool_config();
        assert_eq!(config.acquire_timeout, Duration::from_millis(1_500));
        assert_eq!(config.idle_timeout, Duration::from_secs(60));
    }

    #[test]
    fn a_missing_file_is_reported_with_its_name() {
        let error = Config::load("definitely-not-here.toml").expect_err("should fail");
        assert!(error.to_string().contains("definitely-not-here.toml"));
    }
}
