//! Statement analysis for routing.
//!
//! Luxbyte decides where a statement goes by parsing it, not by matching it
//! against patterns. The difference shows up immediately: `WHERE tenant_id =
//! $1` and `WHERE $1 = tenant_id` and `WHERE u.tenant_id IN ($1, $2)` are the
//! same question to a parser and three different problems to a regular
//! expression.
//!
//! What comes out is a [`StatementAnalysis`]: what kind of statement it is,
//! whether it writes, which tables it touches, and — the part that matters —
//! a [`KeyConstraints`] lattice describing the values its shard key can take.
//!
//! Anything the analyser does not understand produces *no* constraint rather
//! than a wrong one. The router then broadcasts, which is slow and correct,
//! instead of guessing, which is fast and wrong.

mod analyze;
pub mod cache;
pub mod combine;
pub mod hint;
pub mod key;
pub mod merge;
pub mod shape;
pub mod statements;

pub use cache::{AnalysisCache, CacheStats, Prepared};
pub use hint::Hint;
pub use key::{ColumnRef, KeyConstraints, KeyValue};
pub use merge::{MergeStrategy, PostProcessing, SortKey};
pub use shape::{Literal, Shape, normalize};

/// Parsing failures.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SqlError {
    /// The text was not valid PostgreSQL.
    #[error("could not parse the statement: {0}")]
    Parse(String),
}

/// What kind of statement this is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StatementKind {
    /// A `SELECT`, or anything else that only reads.
    Select,
    /// An `INSERT`.
    Insert,
    /// An `UPDATE`.
    Update,
    /// A `DELETE`.
    Delete,
    /// `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT` and friends.
    TransactionControl(TransactionOp),
    /// `SET`, `RESET`, `DISCARD` — state the proxy must replay on every
    /// backend a session later touches, or the session silently diverges
    /// between regions.
    SessionState,
    /// `SHOW`, and other read-only utility statements.
    Show,
    /// `CREATE`, `ALTER`, `DROP`, `TRUNCATE`.
    Ddl,
    /// `EXPLAIN`, of the statement whose tables and keys the analysis
    /// describes; with `ANALYZE` it runs that statement.
    Explain,
    /// `COPY ... TO`, of a table or a query.
    CopyOut,
    /// `COPY ... FROM`, into a table.
    CopyIn,
    /// Anything else.
    Other,
}

/// Which transaction-control statement this is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransactionOp {
    /// `BEGIN` or `START TRANSACTION`.
    Begin,
    /// `COMMIT`.
    Commit,
    /// `ROLLBACK`, with no savepoint named.
    Rollback,
    /// `SAVEPOINT name`.
    Savepoint,
    /// `ROLLBACK TO SAVEPOINT name`.
    RollbackToSavepoint,
    /// `RELEASE SAVEPOINT name`.
    ReleaseSavepoint,
}

/// Whether a statement can be sent to a replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// Reads only, so a replica will do.
    ReadOnly,
    /// Writes, or takes row locks, so it must go to a primary.
    ReadWrite,
}

impl Access {
    /// Whether a primary is required.
    #[must_use]
    pub const fn needs_primary(self) -> bool {
        matches!(self, Self::ReadWrite)
    }
}

/// A table the statement refers to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    /// Schema, if the statement qualified it.
    pub schema: Option<String>,
    /// Table name, folded to lower case unless it was quoted.
    pub name: String,
    /// Alias, if one was given.
    pub alias: Option<String>,
}

impl std::fmt::Display for TableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.schema {
            Some(schema) => write!(f, "{schema}.{}", self.name),
            None => write!(f, "{}", self.name),
        }
    }
}

/// Everything the router needs to know about one statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementAnalysis {
    /// What kind of statement it is.
    pub kind: StatementKind,
    /// Whether it needs a primary.
    pub access: Access,
    /// Tables it reads or writes, in the order they appear.
    pub tables: Vec<TableRef>,
    /// What its predicates pin each column to.
    pub constraints: KeyConstraints,
    /// Columns an `UPDATE` assigns to.
    ///
    /// Assigning to the shard key moves a row between regions, which is a
    /// relocation rather than an update, and the router refuses it rather
    /// than writing a row into a region the directory will not look in.
    pub assigned_columns: Vec<String>,
    /// Highest `$n` placeholder anywhere in the statement text.
    pub highest_parameter: u16,
    /// What the statement does to its rows after collecting them, which
    /// decides whether a result can be assembled from several regions.
    pub post_processing: PostProcessing,
    /// For a set operation, the tables each branch reads; empty otherwise.
    ///
    /// Branches are read independently, so tables in different branches are
    /// not joined — only tables within one branch are.
    pub branches: Vec<Vec<TableRef>>,
}

impl StatementAnalysis {
    /// The values the statement pins `column` of `table` to.
    ///
    /// An unqualified predicate is only trusted when the statement touches a
    /// single table; with a join, `WHERE tenant_id = $1` is genuinely
    /// ambiguous, and PostgreSQL would reject it as ambiguous too.
    #[must_use]
    pub fn shard_key_values(
        &self,
        table: &str,
        column: &str,
    ) -> Option<&std::collections::BTreeSet<KeyValue>> {
        if self.tables.len() > 1 {
            // Fall back to a qualified match only.
            return self.constraints.columns().find_map(|(reference, values)| {
                (reference.qualifier.as_deref() == Some(table) && reference.column == column)
                    .then_some(values)
            });
        }
        self.constraints.values_for(table, column)
    }

    /// Whether the statement assigns to `column`.
    #[must_use]
    pub fn assigns_to(&self, column: &str) -> bool {
        self.assigned_columns.iter().any(|name| name == column)
    }

    /// Whether this statement only manipulates transaction state.
    #[must_use]
    pub const fn is_transaction_control(&self) -> bool {
        matches!(self.kind, StatementKind::TransactionControl(_))
    }
}

/// Parses SQL text and analyses every statement in it.
///
/// A simple query can carry several statements separated by semicolons, and
/// each is analysed on its own, because a batch may well touch more than one
/// region.
pub fn analyze(sql: &str) -> Result<Vec<StatementAnalysis>, SqlError> {
    analyze::analyze(sql)
}
