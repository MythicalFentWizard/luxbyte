//! Turning an analysed statement into a plan.
//!
//! The planner is a pure function of three things — the statement, the
//! topology, and the directory — and it has no I/O of its own. That is what
//! makes the interesting cases testable: a cross-region join, a write during
//! a cutover, a `count(*)` over every region, all without a database
//! anywhere in sight.
//!
//! Where it differs from refusing everything it cannot pin down:
//!
//! - A read whose key is not known fans out and the results are combined —
//!   ordered, limited, deduplicated or re-aggregated as the statement asks —
//!   rather than failing.
//! - A write that touches several regions becomes a distributed transaction,
//!   rather than failing.
//! - A statement whose key arrives in a `Bind` is deferred, not refused.
//!
//! What it still refuses, it refuses for a reason it can state: a join across
//! regions would silently return the wrong rows. A wrong answer is worse than
//! an error, and an error gets a real SQLSTATE so a client can tell it apart.

use std::collections::BTreeSet;
use std::sync::Arc;

use luxbyte_sql::{
    Access, KeyValue, MergeStrategy, Prepared, StatementAnalysis, StatementKind, TableRef,
    TransactionOp,
};

use crate::directory::{Directory, Location};
use crate::topology::{Placement, Topology, UnconfiguredTables};

/// Values bound to a statement's placeholders.
///
/// Indexed from one, the way the wire numbers them, so that `$1` is
/// `get(1)` and no caller has to remember to subtract.
#[derive(Debug, Clone, Default)]
pub struct ParameterValues {
    values: Vec<Option<String>>,
}

impl ParameterValues {
    /// Wraps a list of bound values, in parameter order.
    #[must_use]
    pub fn new(values: Vec<Option<String>>) -> Self {
        Self { values }
    }

    /// The value bound to `$index`, or `None` if it is `NULL`, absent, or
    /// was sent in binary format.
    #[must_use]
    pub fn get(&self, index: u16) -> Option<&str> {
        let position = usize::from(index).checked_sub(1)?;
        self.values.get(position)?.as_deref()
    }

    /// Combines values bound over the wire with literals the analysis cache
    /// lifted out of the statement text.
    ///
    /// Both arrive as positional values, and after this they are
    /// indistinguishable to the planner — which is the point. An inlined
    /// literal and a bound parameter take exactly the same path, so there is
    /// one routing path to get right rather than two.
    #[must_use]
    pub fn from_prepared(prepared: &Prepared, bound: &[Option<String>]) -> Self {
        let base = usize::from(prepared.first_placeholder).saturating_sub(1);
        let mut values = Vec::with_capacity(base.saturating_add(prepared.literals.len()));
        for index in 0..base {
            values.push(bound.get(index).cloned().flatten());
        }
        values.extend(prepared.literals.iter().cloned());
        Self::new(values)
    }

    /// How many values are bound.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether no values are bound.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Something the session handles itself, without touching a backend yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalIntent {
    /// `BEGIN`, `COMMIT`, `ROLLBACK` and friends.
    TransactionControl(TransactionOp),
    /// `SET`, `RESET`, `DISCARD`: recorded and replayed onto every backend
    /// the session later borrows, so that a session which moves between
    /// regions keeps the settings it asked for.
    SessionState,
    /// `SHOW` and other read-only utility statements.
    Show,
}

/// Why a statement will not be run, phrased for the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    /// A real PostgreSQL SQLSTATE, so existing error handling applies.
    pub sqlstate: String,
    /// The primary message.
    pub message: String,
    /// What to do about it, where there is something to do.
    pub hint: Option<String>,
    /// Whether retrying the transaction unchanged is likely to succeed.
    pub retryable: bool,
}

impl Refusal {
    fn new(sqlstate: &str, message: impl Into<String>) -> Self {
        Self {
            sqlstate: sqlstate.to_owned(),
            message: message.into(),
            hint: None,
            retryable: false,
        }
    }

    fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    fn retryable(mut self) -> Self {
        self.retryable = true;
        self
    }
}

/// What the executor should do with a statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// Handle it in the session, without a backend.
    Local(LocalIntent),
    /// Send it to one region.
    Single {
        /// The region.
        region: String,
        /// Whether a primary is required.
        access: Access,
    },
    /// Read from several regions and merge the results.
    ScatterGather {
        /// The regions, in a stable order.
        regions: Vec<String>,
        /// How the results combine.
        merge: MergeStrategy,
    },
    /// Write to several regions under two-phase commit.
    Distributed {
        /// The regions, in a stable order.
        regions: Vec<String>,
    },
    /// The shard key is a placeholder whose value has not arrived yet.
    ///
    /// Returned at `Parse` time; the session plans again at `Bind`, when the
    /// values are known. Deferring is what keeps prepared statements as
    /// precisely routed as literal ones.
    NeedsParameters {
        /// The placeholders whose values would settle the routing.
        parameters: Vec<u16>,
    },
    /// Do not run it.
    Refuse(Refusal),
}

impl Plan {
    /// Every region this plan touches.
    #[must_use]
    pub fn regions(&self) -> Vec<&str> {
        match self {
            Self::Single { region, .. } => vec![region.as_str()],
            Self::ScatterGather { regions, .. } | Self::Distributed { regions } => {
                regions.iter().map(String::as_str).collect()
            }
            Self::Local(_) | Self::NeedsParameters { .. } | Self::Refuse(_) => Vec::new(),
        }
    }

    /// Whether the plan needs a distributed transaction.
    #[must_use]
    pub const fn is_distributed(&self) -> bool {
        matches!(self, Self::Distributed { .. })
    }
}

/// Decides where statements go.
pub struct Router {
    topology: Topology,
    directory: Arc<dyn Directory>,
}

impl std::fmt::Debug for Router {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Router")
            .field("home_region", &self.topology.home_region)
            .field("regions", &self.topology.regions.len())
            .field("directory_entries", &self.directory.len())
            .finish()
    }
}

/// Schemas whose tables exist identically in every region.
///
/// A client library issues a surprising number of catalog queries before it
/// will run anything of yours; routing those as if they were user data would
/// make `psql` fan out across continents to draw a prompt.
const CATALOG_SCHEMAS: &[&str] = &["pg_catalog", "information_schema", "pg_toast"];

fn is_catalog_table(table: &TableRef) -> bool {
    table
        .schema
        .as_deref()
        .is_some_and(|schema| CATALOG_SCHEMAS.contains(&schema))
        || table.name.starts_with("pg_")
}

impl Router {
    /// Builds a router over a validated topology.
    #[must_use]
    pub fn new(topology: Topology, directory: Arc<dyn Directory>) -> Self {
        Self {
            topology,
            directory,
        }
    }

    /// The topology this router plans against.
    #[must_use]
    pub const fn topology(&self) -> &Topology {
        &self.topology
    }

    /// The directory this router consults.
    #[must_use]
    pub fn directory(&self) -> &Arc<dyn Directory> {
        &self.directory
    }

    fn all_regions(&self) -> Vec<String> {
        self.topology.region_ids().map(ToOwned::to_owned).collect()
    }

    /// Plans a statement.
    ///
    /// `parameters` is `None` before a `Bind` has arrived. A statement whose
    /// routing depends on a placeholder then yields
    /// [`Plan::NeedsParameters`] rather than a guess.
    #[must_use]
    pub fn plan(&self, analysis: &StatementAnalysis, parameters: Option<&ParameterValues>) -> Plan {
        match analysis.kind {
            StatementKind::TransactionControl(op) => {
                Plan::Local(LocalIntent::TransactionControl(op))
            }
            StatementKind::SessionState => Plan::Local(LocalIntent::SessionState),
            StatementKind::Show => Plan::Local(LocalIntent::Show),
            // Schema has to be the same everywhere or a relocated key lands
            // in a region whose table does not match. So DDL goes to every
            // region, atomically.
            StatementKind::Ddl => self.distributed_over_all_regions(),
            _ => self.plan_data_statement(analysis, parameters),
        }
    }

    fn distributed_over_all_regions(&self) -> Plan {
        let regions = self.all_regions();
        if regions.len() == 1 {
            return Plan::Single {
                region: self.topology.home_region.clone(),
                access: Access::ReadWrite,
            };
        }
        Plan::Distributed { regions }
    }

    #[allow(clippy::too_many_lines)]
    fn plan_data_statement(
        &self,
        analysis: &StatementAnalysis,
        parameters: Option<&ParameterValues>,
    ) -> Plan {
        let user_tables: Vec<&TableRef> = analysis
            .tables
            .iter()
            .filter(|table| !is_catalog_table(table))
            .collect();

        // `SELECT 1`, `SELECT now()`, or a pure catalog query: the home
        // region answers, and the answer is the same everywhere.
        if user_tables.is_empty() {
            return Plan::Single {
                region: self.topology.home_region.clone(),
                access: analysis.access,
            };
        }

        let mut regions: BTreeSet<String> = BTreeSet::new();
        let mut pending: BTreeSet<u16> = BTreeSet::new();

        for table in &user_tables {
            let Some(placement) = self
                .topology
                .placement(table.schema.as_deref(), &table.name)
            else {
                match self.topology.unconfigured_tables {
                    UnconfiguredTables::Refuse => {
                        return Plan::Refuse(
                            Refusal::new(
                                "0A000",
                                format!("table \"{table}\" has no placement policy"),
                            )
                            .with_hint(format!(
                                "add a [[tables]] entry for \"{table}\" to the topology, or set \
                                 unconfigured_tables = \"home\""
                            )),
                        );
                    }
                    UnconfiguredTables::Home => {
                        regions.insert(self.topology.home_region.clone());
                        continue;
                    }
                }
            };

            match placement {
                Placement::Pinned { region } => {
                    regions.insert(region.clone());
                }
                Placement::GlobalReplicated => {
                    if analysis.access.needs_primary() {
                        // Every copy has to change, or the copies diverge.
                        regions.extend(self.all_regions());
                    } else {
                        // Any copy will do, so read the nearest one.
                        regions.insert(self.topology.home_region.clone());
                    }
                }
                Placement::GeoPartitioned { key } => {
                    // Moving a row's shard key moves the row between
                    // regions. Writing it in place would leave the row in a
                    // region the directory no longer points at, so it is a
                    // relocation, not an update.
                    if analysis.kind == StatementKind::Update && analysis.assigns_to(key) {
                        return Plan::Refuse(
                            Refusal::new(
                                "0A000",
                                format!("cannot assign to \"{key}\", the shard key of \"{table}\""),
                            )
                            .with_hint(
                                "use RELOCATE to move the key to another region, then update \
                                 the remaining columns"
                                    .to_owned(),
                            ),
                        );
                    }

                    match self.resolve_key(analysis, table, key, parameters, &mut pending) {
                        KeyResolution::Regions(found) => regions.extend(found),
                        KeyResolution::Deferred => {}
                        KeyResolution::Refused(refusal) => return Plan::Refuse(refusal),
                    }
                }
            }
        }

        if !pending.is_empty() {
            return Plan::NeedsParameters {
                parameters: pending.into_iter().collect(),
            };
        }

        let regions: Vec<String> = regions.into_iter().collect();
        match regions.len() {
            0 => Plan::Single {
                region: self.topology.home_region.clone(),
                access: analysis.access,
            },
            1 => Plan::Single {
                region: regions.into_iter().next().unwrap_or_default(),
                access: analysis.access,
            },
            _ => Self::plan_multi_region(analysis, &user_tables, regions),
        }
    }

    fn plan_multi_region(
        analysis: &StatementAnalysis,
        user_tables: &[&TableRef],
        regions: Vec<String>,
    ) -> Plan {
        // A join whose sides are in different regions cannot be answered by
        // running it separately in each: the rows that should match across
        // the boundary would simply be missing from both results. A set
        // operation's branches are read independently, so only tables within
        // one branch count as joined.
        let joined = if analysis.branches.is_empty() {
            user_tables.len() > 1
        } else {
            analysis.branches.iter().any(|branch| {
                branch
                    .iter()
                    .filter(|table| !is_catalog_table(table))
                    .count()
                    > 1
            })
        };
        if joined {
            let names: Vec<String> = user_tables.iter().map(ToString::to_string).collect();
            return Plan::Refuse(
                Refusal::new(
                    "0A000",
                    format!(
                        "the statement joins {} across regions {}",
                        names.join(" and "),
                        regions.join(", ")
                    ),
                )
                .with_hint(
                    "constrain every table to the same shard key, or make the smaller table \
                     global_replicated"
                        .to_owned(),
                ),
            );
        }

        if analysis.access.needs_primary() {
            return Plan::Distributed { regions };
        }

        // Whether the combined result can be computed is decided from the
        // statement's text when it runs; see `luxbyte_sql::combine`.
        Plan::ScatterGather {
            regions,
            merge: analysis.post_processing.merge_strategy(),
        }
    }

    fn resolve_key(
        &self,
        analysis: &StatementAnalysis,
        table: &TableRef,
        key: &str,
        parameters: Option<&ParameterValues>,
        pending: &mut BTreeSet<u16>,
    ) -> KeyResolution {
        let Some(values) = analysis.shard_key_values(&table.name, key) else {
            // No usable predicate on the key. A read fans out; a write that
            // cannot name its rows is a cross-region write, which is slow
            // but well defined — except for an INSERT, which would have to
            // invent a region for a row it is creating.
            if analysis.kind == StatementKind::Insert {
                return KeyResolution::Refused(
                    Refusal::new(
                        "23502",
                        format!("INSERT into \"{table}\" does not supply \"{key}\", its shard key"),
                    )
                    .with_hint(format!("include \"{key}\" in the column list")),
                );
            }
            return KeyResolution::Regions(self.all_regions());
        };

        let mut found = Vec::new();
        for value in values {
            let text = match value {
                KeyValue::Literal(text) => text.clone(),
                KeyValue::Parameter(index) => match parameters.and_then(|p| p.get(*index)) {
                    Some(text) => text.to_owned(),
                    None if parameters.is_none() => {
                        pending.insert(*index);
                        continue;
                    }
                    // The parameter is bound, but to NULL or to a binary
                    // value this proxy will not reinterpret. Either way the
                    // key is unknown, so fan out rather than guess.
                    None => return KeyResolution::Regions(self.all_regions()),
                },
            };

            match self.directory.locate(&table.name, &text) {
                Location::Unseen => {
                    if analysis.access.needs_primary() {
                        // A key nobody has seen is created where the client
                        // is. This is the whole premise: new rows land in
                        // the region of whoever made them.
                        found.push(self.topology.home_region.clone());
                    } else {
                        // Reading a key with no directory entry: it may
                        // genuinely not exist, or the entry may be missing.
                        // Fanning out returns the right answer either way.
                        return KeyResolution::Regions(self.all_regions());
                    }
                }
                Location::Known(state) => {
                    if analysis.access.needs_primary() {
                        let Some(region) = state.write_region() else {
                            return KeyResolution::Refused(
                                Refusal::new(
                                    "40001",
                                    format!(
                                        "\"{key}\" = {text} is being relocated; writes are paused \
                                         for the cutover"
                                    ),
                                )
                                .with_hint("retry the transaction".to_owned())
                                .retryable(),
                            );
                        };
                        found.push(region.to_owned());
                    } else {
                        found.push(state.read_region().to_owned());
                    }
                }
            }
        }

        if found.is_empty() && !pending.is_empty() {
            return KeyResolution::Deferred;
        }
        KeyResolution::Regions(found)
    }
}

enum KeyResolution {
    Regions(Vec<String>),
    Deferred,
    Refused(Refusal),
}
