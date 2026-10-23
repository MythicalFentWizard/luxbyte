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
    Access, ColumnRef, Hint, KeyConstraints, KeyValue, MergeStrategy, Prepared, StatementAnalysis,
    StatementKind, TableRef, TransactionOp,
};

use crate::directory::{Directory, Location, Reads, RelocationPhase};
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
    /// A refusal with a SQLSTATE and a message.
    #[must_use]
    pub fn new(sqlstate: &str, message: impl Into<String>) -> Self {
        Self {
            sqlstate: sqlstate.to_owned(),
            message: message.into(),
            hint: None,
            retryable: false,
        }
    }

    /// Adds what to do about it.
    #[must_use]
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
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

/// A plan, or the reason a statement has to wait before it can have one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Routing {
    /// Run it.
    Ready(Plan),
    /// It touches a key that is moving between regions. Planned again once
    /// the move is over it routes normally; the refusal is what to say if
    /// it cannot wait.
    Held(Refusal),
    /// It writes keys the directory has never seen, as `(table, key)`, and
    /// the directory is shared: another proxy may be placing the same key
    /// in its own home region, so the keys are claimed in the shared
    /// directory first and the statement is planned again.
    Claim(Vec<(String, String)>),
}

/// Decides where statements go.
pub struct Router {
    topology: Topology,
    directory: Arc<dyn Directory>,
    /// Whether a write to a key nobody has seen is claimed rather than
    /// placed in the home region outright.
    claims: bool,
}

impl std::fmt::Debug for Router {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Router")
            .field("home_region", &self.topology.home_region)
            .field("regions", &self.topology.regions.len())
            .field("directory_entries", &self.directory.len())
            .field("claims", &self.claims)
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
            claims: false,
        }
    }

    /// Asks for new keys to be claimed before they are written, as a
    /// directory several proxies share needs; see [`Routing::Claim`].
    #[must_use]
    pub const fn claiming(mut self, claims: bool) -> Self {
        self.claims = claims;
        self
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
    ///
    /// A statement that would have to wait for a move is refused with
    /// `40001`; [`Router::route`] tells the two apart, for a caller that can
    /// wait.
    #[must_use]
    ///
    /// A new key is shown in the home region, where it is placed unless
    /// another proxy claimed it first.
    pub fn plan(&self, analysis: &StatementAnalysis, parameters: Option<&ParameterValues>) -> Plan {
        self.plan_hinted(analysis, parameters, None)
    }

    /// [`Router::plan`], following a hint the statement carries.
    #[must_use]
    pub fn plan_hinted(
        &self,
        analysis: &StatementAnalysis,
        parameters: Option<&ParameterValues>,
        hint: Option<&Hint>,
    ) -> Plan {
        match self.follow_hint(analysis, parameters, hint, false) {
            Routing::Ready(plan) => plan,
            Routing::Held(refusal) => Plan::Refuse(refusal),
            // Never asked for when planning without claims.
            Routing::Claim(_) => Plan::Refuse(Refusal::new(
                "XX000",
                "a statement planned without claims asked for one",
            )),
        }
    }

    /// Plans a statement, or says that it touches a moving key and has to
    /// wait.
    #[must_use]
    pub fn route(
        &self,
        analysis: &StatementAnalysis,
        parameters: Option<&ParameterValues>,
    ) -> Routing {
        self.route_hinted(analysis, parameters, None)
    }

    /// [`Router::route`], following a hint the statement carries: see
    /// [`luxbyte_sql::hint`].
    #[must_use]
    pub fn route_hinted(
        &self,
        analysis: &StatementAnalysis,
        parameters: Option<&ParameterValues>,
        hint: Option<&Hint>,
    ) -> Routing {
        self.follow_hint(analysis, parameters, hint, self.claims)
    }

    fn follow_hint(
        &self,
        analysis: &StatementAnalysis,
        parameters: Option<&ParameterValues>,
        hint: Option<&Hint>,
        claims: bool,
    ) -> Routing {
        let hint = match (hint, analysis.kind) {
            // What the proxy answers itself, or keeps per session, goes
            // nowhere a hint could point.
            (
                Some(_),
                StatementKind::TransactionControl(_)
                | StatementKind::SessionState
                | StatementKind::Show,
            )
            | (None, _) => return self.route_with(analysis, parameters, claims),
            (Some(hint), _) => hint,
        };
        let geo: Vec<(&TableRef, &str)> = analysis
            .tables
            .iter()
            .filter(|table| !is_catalog_table(table))
            .filter_map(|table| {
                match self
                    .topology
                    .placement(table.schema.as_deref(), &table.name)
                {
                    Some(Placement::GeoPartitioned { key }) => Some((table, key.as_str())),
                    _ => None,
                }
            })
            .collect();
        match hint {
            Hint::Region(region) => {
                if self.topology.region(region).is_none() {
                    return Routing::Ready(Plan::Refuse(Refusal::new(
                        "22023",
                        format!(
                            "a luxbyte hint names region \"{region}\", which is not configured"
                        ),
                    )));
                }
                // The hint says where, but a move still has to be waited
                // for: a write landing in a key's old region after its rows
                // left would be lost to every later read.
                for (table, _) in &geo {
                    match self.directory.moving_in(&table.name) {
                        Some(_) if analysis.access.needs_primary() => {
                            return Routing::Held(moving(
                                &format!("a key of \"{table}\" is moving between regions"),
                                "a write sent by a hint waits for it",
                            ));
                        }
                        Some(RelocationPhase::Committing) => {
                            return Routing::Held(moving(
                                &format!("a key of \"{table}\" is moving between regions"),
                                "a read sent by a hint waits while the move commits",
                            ));
                        }
                        _ => {}
                    }
                }
                Routing::Ready(Plan::Single {
                    region: region.clone(),
                    access: analysis.access,
                })
            }
            Hint::Key(key) => {
                if geo.is_empty() {
                    return Routing::Ready(Plan::Refuse(
                        Refusal::new(
                            "0A000",
                            "a luxbyte key hint needs a geo-partitioned table in the statement, \
                             to know which key it names",
                        )
                        .with_hint("name the region instead, as /* luxbyte: region=... */"),
                    ));
                }
                // Every geo-partitioned table, pinned to the key by its own
                // name, which is how a statement over several tables is
                // matched; whatever the statement said of the key gives way.
                let mut hinted = analysis.clone();
                hinted.constraints = geo.iter().fold(
                    KeyConstraints::unconstrained(),
                    |constraints, (table, column)| {
                        constraints.and(KeyConstraints::pinned(
                            ColumnRef {
                                qualifier: Some(table.name.clone()),
                                column: (*column).to_owned(),
                            },
                            KeyValue::Literal(key.clone()),
                        ))
                    },
                );
                self.route_with(&hinted, parameters, claims)
            }
        }
    }

    fn route_with(
        &self,
        analysis: &StatementAnalysis,
        parameters: Option<&ParameterValues>,
        claims: bool,
    ) -> Routing {
        match analysis.kind {
            StatementKind::TransactionControl(op) => {
                Routing::Ready(Plan::Local(LocalIntent::TransactionControl(op)))
            }
            StatementKind::SessionState => Routing::Ready(Plan::Local(LocalIntent::SessionState)),
            StatementKind::Show => Routing::Ready(Plan::Local(LocalIntent::Show)),
            // Schema has to be the same everywhere or a relocated key lands
            // in a region whose table does not match. So DDL goes to every
            // region, atomically.
            StatementKind::Ddl => Routing::Ready(self.distributed_over_all_regions()),
            // The rows would arrive through a relay that does not carry them,
            // and each would have to be placed by its key.
            StatementKind::CopyIn => Routing::Ready(Plan::Refuse(
                Refusal::new("0A000", "COPY FROM is not supported through the proxy")
                    .with_hint("insert the rows with INSERT, which places each by its key"),
            )),
            // Each region would answer for its own rows only, and their
            // answers cannot be joined into one.
            StatementKind::Explain | StatementKind::CopyOut => {
                match self.plan_data_statement(analysis, parameters, claims) {
                    Routing::Ready(
                        Plan::ScatterGather { regions, .. } | Plan::Distributed { regions },
                    ) => Routing::Ready(Plan::Refuse(
                        Refusal::new(
                            "0A000",
                            format!(
                                "{} of a statement spanning regions {} cannot be combined",
                                if analysis.kind == StatementKind::Explain {
                                    "EXPLAIN"
                                } else {
                                    "COPY"
                                },
                                regions.join(", ")
                            ),
                        )
                        .with_hint("constrain it to one shard key"),
                    )),
                    other => other,
                }
            }
            _ => self.plan_data_statement(analysis, parameters, claims),
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
        claims: bool,
    ) -> Routing {
        let user_tables: Vec<&TableRef> = analysis
            .tables
            .iter()
            .filter(|table| !is_catalog_table(table))
            .collect();

        // `SELECT 1`, `SELECT now()`, or a pure catalog query: the home
        // region answers, and the answer is the same everywhere.
        if user_tables.is_empty() {
            return Routing::Ready(Plan::Single {
                region: self.topology.home_region.clone(),
                access: analysis.access,
            });
        }

        let mut regions: BTreeSet<String> = BTreeSet::new();
        let mut pending: BTreeSet<u16> = BTreeSet::new();
        let mut unclaimed: Option<Vec<(String, String)>> = claims.then(Vec::new);
        // Read alone, a replicated table is read nearest; read with other
        // tables, from the copy where those are.
        let mut reads_a_copy = false;

        for table in &user_tables {
            let Some(placement) = self
                .topology
                .placement(table.schema.as_deref(), &table.name)
            else {
                match self.topology.unconfigured_tables {
                    UnconfiguredTables::Refuse => {
                        return Routing::Ready(Plan::Refuse(
                            Refusal::new(
                                "0A000",
                                format!("table \"{table}\" has no placement policy"),
                            )
                            .with_hint(format!(
                                "add a [[tables]] entry for \"{table}\" to the topology, or set \
                                 unconfigured_tables = \"home\""
                            )),
                        ));
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
                Placement::GlobalReplicated { writer } => {
                    if analysis.access.needs_primary() {
                        match writer {
                            // Replication carries it from there.
                            Some(writer) => {
                                regions.insert(writer.clone());
                            }
                            // Every copy has to change, or the copies
                            // diverge.
                            None => regions.extend(self.all_regions()),
                        }
                    } else {
                        // Any copy will do.
                        reads_a_copy = true;
                    }
                }
                Placement::GeoPartitioned { key } => {
                    // Moving a row's shard key moves the row between
                    // regions. Writing it in place would leave the row in a
                    // region the directory no longer points at, so it is a
                    // relocation, not an update.
                    if analysis.kind == StatementKind::Update && analysis.assigns_to(key) {
                        return Routing::Ready(Plan::Refuse(
                            Refusal::new(
                                "0A000",
                                format!("cannot assign to \"{key}\", the shard key of \"{table}\""),
                            )
                            .with_hint(
                                "use RELOCATE to move the key to another region, then update \
                                 the remaining columns"
                                    .to_owned(),
                            ),
                        ));
                    }

                    match self.resolve_key(
                        analysis,
                        table,
                        key,
                        parameters,
                        &mut pending,
                        unclaimed.as_mut(),
                    ) {
                        KeyResolution::Regions(found) => regions.extend(found),
                        KeyResolution::Deferred => {}
                        KeyResolution::Refused(refusal) => {
                            return Routing::Ready(Plan::Refuse(refusal));
                        }
                        KeyResolution::Held(refusal) => return Routing::Held(refusal),
                    }
                }
            }
        }

        if let Some(keys) = unclaimed.filter(|keys| !keys.is_empty()) {
            return Routing::Claim(keys);
        }
        if !pending.is_empty() {
            return Routing::Ready(Plan::NeedsParameters {
                parameters: pending.into_iter().collect(),
            });
        }

        if regions.is_empty() && reads_a_copy {
            regions.insert(self.topology.home_region.clone());
        }
        if let Some(refusal) = self.copies_written_from_partial_rows(analysis, &regions) {
            return Routing::Ready(Plan::Refuse(refusal));
        }
        let regions: Vec<String> = regions.into_iter().collect();
        Routing::Ready(match regions.len() {
            0 => Plan::Single {
                region: self.topology.home_region.clone(),
                access: analysis.access,
            },
            1 => Plan::Single {
                region: regions.into_iter().next().unwrap_or_default(),
                access: analysis.access,
            },
            _ => self.plan_multi_region(analysis, &user_tables, regions),
        })
    }

    /// Whether an `INSERT ... SELECT` takes the key of the table it writes,
    /// key for key, from the rows of a geo-partitioned table it reads: each
    /// region then writes the rows it read, in the region they belong to.
    fn copies_keys(&self, analysis: &StatementAnalysis, key: &str) -> bool {
        analysis.links.iter().any(|link| {
            let (other, other_column) = match (link.left, link.right) {
                (0, other) if link.left_column == key => (other, &link.right_column),
                (other, 0) if link.right_column == key => (other, &link.left_column),
                _ => return false,
            };
            analysis.tables.get(other).is_some_and(|read| {
                matches!(
                    self.topology.placement(read.schema.as_deref(), &read.name),
                    Some(Placement::GeoPartitioned { key }) if key == other_column
                )
            })
        })
    }

    /// A write to a replicated table that reads rows only some regions
    /// hold: each copy would be changed from different rows, and the copies
    /// would no longer agree. Fine only when one region writes, for a writer
    /// whose replication carries the change, and holds every row read.
    fn copies_written_from_partial_rows(
        &self,
        analysis: &StatementAnalysis,
        regions: &BTreeSet<String>,
    ) -> Option<Refusal> {
        if !matches!(
            analysis.kind,
            StatementKind::Insert | StatementKind::Update | StatementKind::Delete
        ) {
            return None;
        }
        let target = analysis.tables.first()?;
        let Some(Placement::GlobalReplicated { writer }) = self
            .topology
            .placement(target.schema.as_deref(), &target.name)
        else {
            return None;
        };
        let partial = analysis
            .tables
            .iter()
            .skip(1)
            .filter(|table| !is_catalog_table(table))
            .find(|table| {
                !matches!(
                    self.topology
                        .placement(table.schema.as_deref(), &table.name),
                    Some(Placement::GlobalReplicated { .. })
                )
            })?;
        let one_writer = writer
            .as_ref()
            .is_some_and(|writer| regions.len() == 1 && regions.contains(writer));
        if one_writer {
            return None;
        }
        Some(
            Refusal::new(
                "0A000",
                format!(
                    "this statement changes every copy of \"{target}\" from rows of \"{partial}\", \
                     which each region holds only some of, so the copies would differ"
                ),
            )
            .with_hint(
                "read the rows first and write the values, or give the table a writer and \
                 constrain the rows read to that region",
            ),
        )
    }

    /// Whether running a statement in each region on its own rows gives
    /// the whole answer: every geo-partitioned read is tied to the others
    /// shard key to shard key, so rows only ever meet in the region they
    /// share, and every other table is replicated, so each region has all
    /// of it.
    fn colocated(&self, analysis: &StatementAnalysis) -> bool {
        let mut geo: Vec<(usize, &str)> = Vec::new();
        for (index, table) in analysis.tables.iter().enumerate() {
            if is_catalog_table(table) {
                continue;
            }
            match self
                .topology
                .placement(table.schema.as_deref(), &table.name)
            {
                Some(Placement::GeoPartitioned { key }) => geo.push((index, key.as_str())),
                Some(Placement::GlobalReplicated { .. }) => {}
                // A pinned or unplaced table lives in one region only.
                _ => return false,
            }
        }
        // Replicated tables only: every region has all of every one.
        if geo.is_empty() {
            return true;
        }
        let key_of = |read: usize| {
            geo.iter()
                .find(|(index, _)| *index == read)
                .map(|(_, key)| *key)
        };
        // Union-find over the geo-partitioned reads.
        let root = |parent: &[usize], mut at: usize| {
            while let Some(&up) = parent.get(at) {
                if up == at {
                    break;
                }
                at = up;
            }
            at
        };
        let mut parent: Vec<usize> = (0..analysis.tables.len()).collect();
        for link in &analysis.links {
            if key_of(link.left) == Some(link.left_column.as_str())
                && key_of(link.right) == Some(link.right_column.as_str())
            {
                let (a, b) = (root(&parent, link.left), root(&parent, link.right));
                if let Some(slot) = parent.get_mut(a) {
                    *slot = b;
                }
            }
        }
        let mut roots = geo.iter().map(|(index, _)| root(&parent, *index));
        let first = roots.next();
        // A result row drawn only from replicated tables would come back
        // once from every region.
        let anchored = analysis
            .anchors
            .iter()
            .any(|anchor| key_of(*anchor).is_some());
        anchored && roots.all(|other| Some(other) == first)
    }

    fn plan_multi_region(
        &self,
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
            user_tables.len() > 1 && !self.colocated(analysis)
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
        mut unclaimed: Option<&mut Vec<(String, String)>>,
    ) -> KeyResolution {
        let Some(values) = analysis.shard_key_values(&table.name, key) else {
            // A statement over every key touches the moving ones too: a write
            // could reach a moving key's rows in neither region, and a read
            // while a move commits could see them in both.
            match self.directory.moving_in(&table.name) {
                Some(_) if analysis.access.needs_primary() => {
                    return KeyResolution::Held(moving(
                        &format!("a key of \"{table}\" is moving between regions"),
                        "writes to every key of a table wait",
                    ));
                }
                Some(RelocationPhase::Committing) => {
                    return KeyResolution::Held(moving(
                        &format!("a key of \"{table}\" is moving between regions"),
                        "reads of every key of a table wait while the move commits",
                    ));
                }
                _ => {}
            }
            // No usable predicate on the key. A read fans out; a write that
            // cannot name its rows is a cross-region write, which is slow
            // but well defined — except for an INSERT, which would have to
            // invent a region for a row it is creating.
            let written = analysis
                .tables
                .first()
                .is_some_and(|first| std::ptr::eq(first, table));
            if analysis.kind == StatementKind::Insert && written && !self.copies_keys(analysis, key)
            {
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
                        // the region of whoever made them — in one of its
                        // shards, if it has several. With proxies in several
                        // regions, the first to claim it decides.
                        match unclaimed.as_deref_mut() {
                            Some(keys) => keys.push((table.name.clone(), text)),
                            None => found.push(self.topology.new_key_region(&text).to_owned()),
                        }
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
                            return KeyResolution::Held(moving(
                                &format!("\"{key}\" = {text} is moving between regions"),
                                "writes to it wait",
                            ));
                        };
                        found.push(region.to_owned());
                    } else {
                        match state.reads() {
                            Reads::Region(region) => found.push(region.to_owned()),
                            Reads::Everywhere => return KeyResolution::Regions(self.all_regions()),
                            Reads::Wait => {
                                return KeyResolution::Held(moving(
                                    &format!("\"{key}\" = {text} is moving between regions"),
                                    "reads of it wait while the move commits",
                                ));
                            }
                        }
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
    Held(Refusal),
}

/// What a statement that waited too long for a move is told.
///
/// `40001` is `serialization_failure`, which client libraries already
/// retry, so even a statement that cannot wait costs a correct application
/// nothing but the retry.
fn moving(what: &str, why: &str) -> Refusal {
    Refusal::new("40001", format!("{what}; {why} until the move is over"))
        .with_hint("retry the transaction".to_owned())
        .retryable()
}
