//! Shared fixtures for the head-to-head comparison.
//!
//! Both analysers are given the same statements and the same placement
//! policy, expressed in each project's own configuration vocabulary:
//!
//! | | Luxbyte | shahrah |
//! |---|---|---|
//! | sharded by a key | `geo_partitioned` | `with_key` |
//! | copy in every region | `global_replicated` | `with_replicated` |
//!
//! The statements use literals rather than placeholders, so that neither
//! analyser is waiting on a value the other already has.

use std::sync::Arc;

use luxbyte_core::{MemoryDirectory, ParameterValues, Plan, Router, Topology};
use luxbyte_sql::AnalysisCache;
use shahrah_sql::analysis::{Access as ShahrahAccess, KeyType, Policy, Routing, TableClass};
use shahrah_sql::cache::Cache;

/// Luxbyte's view of the deployment.
pub const TOPOLOGY: &str = r#"
home_region = "eu-west"

[[regions]]
id = "eu-west"
primary = { host = "eu" }

[[regions]]
id = "us-east"
primary = { host = "us" }

[[regions]]
id = "ap-south"
primary = { host = "ap" }

[[tables]]
table = "users"
placement = "geo_partitioned"
key = "tenant_id"

[[tables]]
table = "orders"
placement = "geo_partitioned"
key = "tenant_id"

[[tables]]
table = "currencies"
placement = "global_replicated"
"#;

/// shahrah's view of the same deployment.
#[must_use]
pub fn shahrah_policy() -> Policy {
    Policy::new()
        .with_key("users", "tenant_id", KeyType::Text)
        .with_key("orders", "tenant_id", KeyType::Text)
        .with_replicated("currencies", "eu-west")
}

/// Luxbyte's router over the same deployment.
#[must_use]
pub fn luxbyte_router() -> Router {
    let topology = Topology::from_toml(TOPOLOGY).expect("topology should parse");
    let directory = MemoryDirectory::with_entries([
        ("users", "alice", "eu-west"),
        ("users", "bob", "us-east"),
        ("orders", "alice", "eu-west"),
        ("orders", "bob", "us-east"),
        // Present so the negative-key row tests sign handling rather than a
        // key the directory happens not to know.
        ("users", "-1", "ap-south"),
    ]);
    Router::new(topology, Arc::new(directory))
}

/// The statements both analysers are asked about.
///
/// Ordered from "every router handles this" to "this is where routers
/// differ", because the second half is the interesting part.
pub const STATEMENTS: &[(&str, &str)] = &[
    ("point read", "SELECT id, name FROM users WHERE tenant_id = 'alice'"),
    ("point read, reversed", "SELECT id FROM users WHERE 'alice' = tenant_id"),
    ("point write", "UPDATE users SET name = 'ada' WHERE tenant_id = 'alice'"),
    ("insert", "INSERT INTO users (tenant_id, name) VALUES ('alice', 'ada')"),
    ("delete", "DELETE FROM users WHERE tenant_id = 'alice'"),
    ("aliased key", "SELECT u.id FROM users u WHERE u.tenant_id = 'alice'"),
    ("cast key", "SELECT id FROM users WHERE tenant_id = 'alice'::text"),
    ("negative key", "SELECT id FROM users WHERE tenant_id = -1"),
    ("no key at all", "SELECT id FROM users WHERE name = 'ada'"),
    ("key in an IN list", "SELECT id FROM users WHERE tenant_id IN ('alice', 'bob')"),
    (
        "key under AND",
        "SELECT id FROM users WHERE name = 'ada' AND tenant_id = 'alice'",
    ),
    (
        "same key under OR",
        "SELECT id FROM users WHERE tenant_id = 'alice' OR tenant_id = 'bob'",
    ),
    (
        "key OR unconstrained",
        "SELECT id FROM users WHERE tenant_id = 'alice' OR name = 'ada'",
    ),
    (
        "multi-region insert",
        "INSERT INTO users (tenant_id, name) VALUES ('alice', 'a'), ('bob', 'b')",
    ),
    ("count across regions", "SELECT count(*) FROM users"),
    ("count within one region", "SELECT count(*) FROM users WHERE tenant_id = 'alice'"),
    ("sum across regions", "SELECT sum(id) FROM users"),
    ("distinct across regions", "SELECT DISTINCT name FROM users"),
    (
        "ordered scatter",
        "SELECT id FROM users WHERE name = 'ada' ORDER BY created_at DESC",
    ),
    (
        "join within one region",
        "SELECT u.id FROM users u JOIN orders o ON o.tenant_id = u.tenant_id \
         WHERE u.tenant_id = 'alice' AND o.tenant_id = 'alice'",
    ),
    (
        "join across regions",
        "SELECT u.id FROM users u JOIN orders o ON o.tenant_id = u.tenant_id \
         WHERE u.tenant_id = 'alice' AND o.tenant_id = 'bob'",
    ),
    ("replicated read", "SELECT * FROM currencies"),
    ("replicated write", "UPDATE currencies SET rate = 1.1 WHERE code = 'EUR'"),
];

/// A routing decision, reduced to the part both projects can be compared on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// Goes to exactly one region.
    OneRegion,
    /// Goes to several regions and the results are combined.
    FanOut,
    /// Will not be run.
    Refused(String),
    /// Needs a value that has not arrived yet.
    Deferred,
    /// Answered without a backend.
    Local,
    /// The analyser failed.
    Failed(String),
}

impl Decision {
    /// A short symbol for the comparison table.
    #[must_use]
    pub fn symbol(&self) -> &'static str {
        match self {
            Self::OneRegion => "one region",
            Self::FanOut => "fan out",
            Self::Refused(_) => "REFUSED",
            Self::Deferred => "deferred",
            Self::Local => "local",
            Self::Failed(_) => "FAILED",
        }
    }

    /// Whether the statement would actually run.
    #[must_use]
    pub const fn runs(&self) -> bool {
        matches!(self, Self::OneRegion | Self::FanOut | Self::Local)
    }

    /// The reason, where there is one.
    #[must_use]
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Refused(why) | Self::Failed(why) => Some(why),
            _ => None,
        }
    }
}

/// Runs a statement through Luxbyte.
#[must_use]
pub fn luxbyte_decision(router: &Router, cache: &AnalysisCache, sql: &str) -> Decision {
    let prepared = match cache.analyze(sql) {
        Ok(prepared) => prepared,
        Err(error) => return Decision::Failed(error.to_string()),
    };
    let Some(analysis) = prepared.analyses.first() else {
        return Decision::Failed("no statement".to_owned());
    };
    let parameters = ParameterValues::from_prepared(&prepared, &[]);
    match router.plan(analysis, Some(&parameters)) {
        Plan::Single { .. } => Decision::OneRegion,
        Plan::ScatterGather { .. } | Plan::Distributed { .. } => Decision::FanOut,
        Plan::Local(_) => Decision::Local,
        Plan::NeedsParameters { .. } => Decision::Deferred,
        Plan::Refuse(refusal) => Decision::Refused(refusal.message),
    }
}

/// Runs a statement through shahrah.
#[must_use]
pub fn shahrah_decision(cache: &Cache, policy: &Policy, sql: &str) -> Decision {
    let analysis = match cache.analyse(sql, policy) {
        Ok(analysis) => analysis,
        Err(error) => return Decision::Failed(error.to_string()),
    };
    match &analysis.routing {
        Routing::Single { .. } | Routing::Pinned { .. } => Decision::OneRegion,
        // `NoShardedTable` covers both "nothing interesting here" and a
        // replicated table, which shahrah routes by its configured writer
        // region rather than by a key. Collapsing the second into "local"
        // would understate it.
        Routing::NoShardedTable => {
            let replicated = analysis
                .classes
                .iter()
                .any(|(_, class)| *class == Some(TableClass::Replicated));
            if replicated {
                Decision::OneRegion
            } else {
                match analysis.access {
                    ShahrahAccess::Read | ShahrahAccess::Write => Decision::OneRegion,
                    _ => Decision::Local,
                }
            }
        }
        Routing::EveryShard { what, .. } => {
            // A fan-out it cannot merge is a fan-out it will not perform.
            if analysis.mergeable {
                Decision::FanOut
            } else {
                Decision::Refused(format!("every shard, not mergeable: {what}"))
            }
        }
        Routing::KeyMissing { table } => {
            Decision::Refused(format!("no shard key for \"{table}\""))
        }
        Routing::Unsupported(why) => Decision::Refused((*why).to_owned()),
    }
}
