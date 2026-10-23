//! Regions, their endpoints, and the table placement policy.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// A database endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Endpoint {
    /// Host name or address.
    pub host: String,
    /// TCP port.
    #[serde(default = "default_port")]
    pub port: u16,
}

const fn default_port() -> u16 {
    5432
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

/// One region: a primary and any read replicas beside it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Region {
    /// Stable identifier, such as `eu-west`.
    pub id: String,
    /// The writable endpoint.
    pub primary: Endpoint,
    /// Read-only endpoints, which may lag.
    #[serde(default)]
    pub replicas: Vec<Endpoint>,
}

impl Region {
    /// Picks an endpoint for a read, falling back to the primary when there
    /// is no replica.
    ///
    /// `nth` is a counter the caller advances per query, which spreads reads
    /// across replicas without the bookkeeping a least-connections policy
    /// would need.
    #[must_use]
    pub fn read_endpoint(&self, nth: usize) -> &Endpoint {
        if self.replicas.is_empty() {
            return &self.primary;
        }
        let index = nth.checked_rem(self.replicas.len()).unwrap_or(0);
        self.replicas.get(index).unwrap_or(&self.primary)
    }
}

/// How a table's rows are distributed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "placement", rename_all = "snake_case")]
pub enum Placement {
    /// Rows live in whichever region their shard key maps to.
    GeoPartitioned {
        /// The column that decides the region.
        key: String,
    },
    /// Every region holds a full copy.
    ///
    /// Suitable for small, rarely written reference data — currency tables,
    /// feature flags. Writes must reach every region, so this is expensive
    /// to write and free to read.
    GlobalReplicated,
    /// The table exists in exactly one region.
    Pinned {
        /// The region that holds it.
        region: String,
    },
}

/// A table and how it is placed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TablePolicy {
    /// Table name, unqualified.
    pub table: String,
    /// Schema, defaulting to `public`.
    #[serde(default = "default_schema")]
    pub schema: String,
    /// How its rows are distributed.
    #[serde(flatten)]
    pub placement: Placement,
}

fn default_schema() -> String {
    "public".to_owned()
}

/// What to do about a table with no placement policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnconfiguredTables {
    /// Refuse the statement and name the table in the error.
    ///
    /// The default, because the alternative is to quietly route a table to
    /// one region and leave someone to discover months later that half
    /// their data is unreachable from everywhere else.
    #[default]
    Refuse,
    /// Treat the table as living in the home region.
    Home,
}

/// The whole deployment: which regions exist, and how tables sit across them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Topology {
    /// The region this proxy instance sits in.
    ///
    /// Used as the tie-breaker for anything that could run anywhere, which
    /// is where most of the latency win comes from: a globally replicated
    /// read never leaves the building.
    pub home_region: String,
    /// Every region, keyed by id.
    pub regions: Vec<Region>,
    /// Table placement policy.
    #[serde(default)]
    pub tables: Vec<TablePolicy>,
    /// How to treat a table no policy covers.
    #[serde(default)]
    pub unconfigured_tables: UnconfiguredTables,
}

/// Problems with a topology that make it unusable.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum TopologyError {
    /// No regions were configured.
    #[error("the topology has no regions")]
    NoRegions,
    /// Two regions share an id.
    #[error("region {0} is defined more than once")]
    DuplicateRegion(String),
    /// Two policies cover the same table.
    #[error("table {0} has more than one placement policy")]
    DuplicateTable(String),
    /// `home_region` names a region that does not exist.
    #[error("home region {0} is not one of the configured regions")]
    UnknownHomeRegion(String),
    /// A pinned table names a region that does not exist.
    #[error("table {table} is pinned to unknown region {region}")]
    UnknownPinnedRegion {
        /// The table.
        table: String,
        /// The region it named.
        region: String,
    },
    /// The configuration file was not valid TOML.
    #[error("could not parse the topology: {0}")]
    Parse(String),
}

impl Topology {
    /// Parses a topology from TOML and validates it.
    pub fn from_toml(text: &str) -> Result<Self, TopologyError> {
        let topology: Self =
            toml::from_str(text).map_err(|error| TopologyError::Parse(error.to_string()))?;
        topology.validate()?;
        Ok(topology)
    }

    /// Checks every invariant the router later relies on.
    ///
    /// Done once at startup rather than per query, so that routing can treat
    /// a missing region as a bug rather than a case to handle.
    pub fn validate(&self) -> Result<(), TopologyError> {
        if self.regions.is_empty() {
            return Err(TopologyError::NoRegions);
        }
        let mut seen = BTreeSet::new();
        for region in &self.regions {
            if !seen.insert(region.id.clone()) {
                return Err(TopologyError::DuplicateRegion(region.id.clone()));
            }
        }
        if !seen.contains(&self.home_region) {
            return Err(TopologyError::UnknownHomeRegion(self.home_region.clone()));
        }
        let mut tables = BTreeSet::new();
        for policy in &self.tables {
            let qualified = format!("{}.{}", policy.schema, policy.table);
            if !tables.insert(qualified.clone()) {
                return Err(TopologyError::DuplicateTable(qualified));
            }
            if let Placement::Pinned { region } = &policy.placement
                && !seen.contains(region)
            {
                return Err(TopologyError::UnknownPinnedRegion {
                    table: qualified,
                    region: region.clone(),
                });
            }
        }
        Ok(())
    }

    /// Looks a region up by id.
    #[must_use]
    pub fn region(&self, id: &str) -> Option<&Region> {
        self.regions.iter().find(|region| region.id == id)
    }

    /// Every region id, in configuration order.
    pub fn region_ids(&self) -> impl Iterator<Item = &str> {
        self.regions.iter().map(|region| region.id.as_str())
    }

    /// The placement policy for a table.
    ///
    /// Matching ignores the schema when the statement did not give one,
    /// because an unqualified name resolves through `search_path`, which the
    /// proxy does not track. Configuring two same-named tables in different
    /// schemas with different placements is therefore refused at startup.
    #[must_use]
    pub fn placement(&self, schema: Option<&str>, table: &str) -> Option<&Placement> {
        self.tables
            .iter()
            .find(|policy| {
                policy.table == table && schema.is_none_or(|schema| policy.schema == schema)
            })
            .map(|policy| &policy.placement)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> &'static str {
        r#"
home_region = "eu-west"

[[regions]]
id = "eu-west"
primary = { host = "eu.db.internal" }
replicas = [{ host = "eu-replica-1.db.internal" }, { host = "eu-replica-2.db.internal" }]

[[regions]]
id = "us-east"
primary = { host = "us.db.internal", port = 6432 }

[[tables]]
table = "users"
placement = "geo_partitioned"
key = "tenant_id"

[[tables]]
table = "currencies"
placement = "global_replicated"

[[tables]]
table = "eu_audit_log"
placement = "pinned"
region = "eu-west"
"#
    }

    #[test]
    fn parses_a_complete_topology() {
        let topology = Topology::from_toml(sample()).expect("should parse");
        assert_eq!(topology.home_region, "eu-west");
        assert_eq!(topology.regions.len(), 2);
        assert_eq!(
            topology.region("us-east").map(|r| r.primary.port),
            Some(6432)
        );
        // The default port is applied where none was given.
        assert_eq!(
            topology.region("eu-west").map(|r| r.primary.port),
            Some(5432)
        );
    }

    #[test]
    fn reads_the_three_placement_kinds() {
        let topology = Topology::from_toml(sample()).expect("should parse");
        assert_eq!(
            topology.placement(None, "users"),
            Some(&Placement::GeoPartitioned {
                key: "tenant_id".to_owned()
            })
        );
        assert_eq!(
            topology.placement(None, "currencies"),
            Some(&Placement::GlobalReplicated)
        );
        assert_eq!(
            topology.placement(None, "eu_audit_log"),
            Some(&Placement::Pinned {
                region: "eu-west".to_owned()
            })
        );
        assert_eq!(topology.placement(None, "unknown_table"), None);
    }

    #[test]
    fn a_schema_qualified_lookup_must_match_the_schema() {
        let topology = Topology::from_toml(sample()).expect("should parse");
        assert!(topology.placement(Some("public"), "users").is_some());
        assert!(topology.placement(Some("archive"), "users").is_none());
    }

    #[test]
    fn replica_selection_rotates_and_never_leaves_the_region() {
        let topology = Topology::from_toml(sample()).expect("should parse");
        let eu = topology.region("eu-west").expect("eu-west exists");
        assert_eq!(eu.read_endpoint(0).host, "eu-replica-1.db.internal");
        assert_eq!(eu.read_endpoint(1).host, "eu-replica-2.db.internal");
        assert_eq!(eu.read_endpoint(2).host, "eu-replica-1.db.internal");
    }

    #[test]
    fn a_region_with_no_replicas_reads_from_its_primary() {
        let topology = Topology::from_toml(sample()).expect("should parse");
        let us = topology.region("us-east").expect("us-east exists");
        assert_eq!(us.read_endpoint(0).host, "us.db.internal");
        assert_eq!(us.read_endpoint(7).host, "us.db.internal");
    }

    #[test]
    fn an_empty_topology_is_refused() {
        let error = Topology::from_toml("home_region = \"eu\"\nregions = []");
        assert_eq!(error.err(), Some(TopologyError::NoRegions));
    }

    #[test]
    fn a_home_region_that_does_not_exist_is_refused() {
        let text = r#"
home_region = "ap-south"
[[regions]]
id = "eu-west"
primary = { host = "eu" }
"#;
        assert_eq!(
            Topology::from_toml(text).err(),
            Some(TopologyError::UnknownHomeRegion("ap-south".to_owned()))
        );
    }

    #[test]
    fn a_duplicate_region_is_refused() {
        let text = r#"
home_region = "eu-west"
[[regions]]
id = "eu-west"
primary = { host = "a" }
[[regions]]
id = "eu-west"
primary = { host = "b" }
"#;
        assert_eq!(
            Topology::from_toml(text).err(),
            Some(TopologyError::DuplicateRegion("eu-west".to_owned()))
        );
    }

    #[test]
    fn a_duplicate_table_policy_is_refused() {
        let text = r#"
home_region = "eu-west"
[[regions]]
id = "eu-west"
primary = { host = "a" }
[[tables]]
table = "users"
placement = "global_replicated"
[[tables]]
table = "users"
placement = "geo_partitioned"
key = "tenant_id"
"#;
        assert_eq!(
            Topology::from_toml(text).err(),
            Some(TopologyError::DuplicateTable("public.users".to_owned()))
        );
    }

    #[test]
    fn pinning_a_table_to_a_region_that_does_not_exist_is_refused() {
        let text = r#"
home_region = "eu-west"
[[regions]]
id = "eu-west"
primary = { host = "a" }
[[tables]]
table = "logs"
placement = "pinned"
region = "mars-one"
"#;
        assert_eq!(
            Topology::from_toml(text).err(),
            Some(TopologyError::UnknownPinnedRegion {
                table: "public.logs".to_owned(),
                region: "mars-one".to_owned(),
            })
        );
    }

    #[test]
    fn unconfigured_tables_are_refused_unless_configured_otherwise() {
        let topology = Topology::from_toml(sample()).expect("should parse");
        assert_eq!(topology.unconfigured_tables, UnconfiguredTables::Refuse);

        // The key must precede the `[[tables]]` arrays, or TOML reads it
        // as a field of the last table rather than of the topology.
        let permissive = format!(
            "unconfigured_tables = \"home\"
{}",
            sample()
        );
        let topology = Topology::from_toml(&permissive).expect("should parse");
        assert_eq!(topology.unconfigured_tables, UnconfiguredTables::Home);
    }

    #[test]
    fn malformed_toml_is_an_error_not_a_panic() {
        assert!(matches!(
            Topology::from_toml("this is not toml ["),
            Err(TopologyError::Parse(_))
        ));
    }
}
