//! What a statement says about its shard key, expressed as a lattice.
//!
//! The question a geo-router has to answer is narrow: *which values can the
//! shard key take for the rows this statement touches?* The answer is one of
//! three things — a finite set, nothing at all, or unknown — and the useful
//! part is that these compose through `AND` and `OR` without ever needing to
//! evaluate the predicate.
//!
//! The rules are short enough to state in full:
//!
//! - `AND` **narrows**. If either side pins a column, the result is pinned;
//!   if both do, the answer is the intersection.
//! - `OR` **widens**. A column survives only if *both* sides pin it, and the
//!   answer is the union. One unconstrained branch loses the column entirely,
//!   which is correct: `tenant = 1 OR created_at > now()` really can match
//!   any tenant.
//! - Anything not understood — a function call, a subquery, a `NOT` — yields
//!   no constraint rather than a guess.
//!
//! Every rule errs towards *more* candidate values, never fewer. An
//! over-approximation costs an extra region in the fan-out; an
//! under-approximation silently returns the wrong rows.

use std::collections::{BTreeMap, BTreeSet};

/// A value a shard key is compared against.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KeyValue {
    /// A literal written into the statement, normalised to its text form.
    Literal(String),
    /// A placeholder such as `$1`, carrying its one-based position.
    ///
    /// The value arrives later, in a `Bind`. Carrying the position rather
    /// than giving up is what lets a prepared statement be routed as
    /// precisely as a literal one.
    Parameter(u16),
}

impl KeyValue {
    /// The literal text, if this is not a placeholder.
    #[must_use]
    pub fn as_literal(&self) -> Option<&str> {
        match self {
            Self::Literal(text) => Some(text),
            Self::Parameter(_) => None,
        }
    }

    /// The one-based placeholder position, if this is a placeholder.
    #[must_use]
    pub const fn as_parameter(&self) -> Option<u16> {
        match self {
            Self::Parameter(index) => Some(*index),
            Self::Literal(_) => None,
        }
    }
}

impl std::fmt::Display for KeyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Literal(text) => write!(f, "{text}"),
            Self::Parameter(index) => write!(f, "${index}"),
        }
    }
}

/// A column, with its table qualifier if the statement gave one.
///
/// Names are folded to lower case because PostgreSQL folds unquoted
/// identifiers that way. A quoted `"TenantId"` is a different column from
/// `TenantId`, and treating them alike would route some statements to the
/// wrong place — so quoting is preserved by the parser and respected here.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColumnRef {
    /// Table name or alias, if the column was written qualified.
    pub qualifier: Option<String>,
    /// The column name.
    pub column: String,
}

impl ColumnRef {
    /// An unqualified column.
    #[must_use]
    pub fn bare(column: impl Into<String>) -> Self {
        Self {
            qualifier: None,
            column: column.into(),
        }
    }

    /// A qualified column.
    #[must_use]
    pub fn qualified(qualifier: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            qualifier: Some(qualifier.into()),
            column: column.into(),
        }
    }

    /// Whether this reference could denote `column` of `table`.
    ///
    /// An unqualified reference matches any table, which is sound only
    /// because the caller checks that the statement touches one table — see
    /// [`crate::StatementAnalysis::shard_key_values`].
    #[must_use]
    pub fn matches(&self, table: &str, column: &str) -> bool {
        self.column == column
            && self
                .qualifier
                .as_ref()
                .is_none_or(|qualifier| qualifier == table)
    }
}

impl std::fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.qualifier {
            Some(qualifier) => write!(f, "{qualifier}.{}", self.column),
            None => write!(f, "{}", self.column),
        }
    }
}

/// The set of values a statement pins each column to.
///
/// A column that is absent is unconstrained. A column that is present always
/// maps to a non-empty set; the empty set would mean "matches nothing", which
/// this type never produces, because an unsatisfiable predicate is the
/// database's business and not the router's.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyConstraints {
    columns: BTreeMap<ColumnRef, BTreeSet<KeyValue>>,
}

impl KeyConstraints {
    /// No constraints at all: every column may take any value.
    #[must_use]
    pub fn unconstrained() -> Self {
        Self::default()
    }

    /// One column pinned to one value.
    #[must_use]
    pub fn pinned(column: ColumnRef, value: KeyValue) -> Self {
        let mut values = BTreeSet::new();
        values.insert(value);
        Self {
            columns: BTreeMap::from([(column, values)]),
        }
    }

    /// One column pinned to a set of values, as `IN (...)` produces.
    ///
    /// An empty set is treated as no constraint, since `IN ()` is not
    /// something to reason about.
    #[must_use]
    pub fn pinned_to_any_of(column: ColumnRef, values: BTreeSet<KeyValue>) -> Self {
        if values.is_empty() {
            return Self::unconstrained();
        }
        Self {
            columns: BTreeMap::from([(column, values)]),
        }
    }

    /// Whether nothing is constrained.
    #[must_use]
    pub fn is_unconstrained(&self) -> bool {
        self.columns.is_empty()
    }

    /// Every constrained column.
    pub fn columns(&self) -> impl Iterator<Item = (&ColumnRef, &BTreeSet<KeyValue>)> {
        self.columns.iter()
    }

    /// The values pinned for `column` of `table`, if any.
    ///
    /// A qualified match is preferred over an unqualified one; when a
    /// statement says both `u.tenant = $1` and `tenant = 2`, the qualified
    /// form is the one that definitely refers to this table.
    #[must_use]
    pub fn values_for(&self, table: &str, column: &str) -> Option<&BTreeSet<KeyValue>> {
        let qualified = self
            .columns
            .iter()
            .find(|(key, _)| key.qualifier.as_deref() == Some(table) && key.column == column);
        let unqualified = || {
            self.columns
                .iter()
                .find(|(key, _)| key.qualifier.is_none() && key.column == column)
        };
        qualified.or_else(unqualified).map(|(_, values)| values)
    }

    /// Combines two sides of an `AND`.
    #[must_use]
    pub fn and(mut self, other: Self) -> Self {
        for (column, values) in other.columns {
            match self.columns.get_mut(&column) {
                // Both sides pin the column, so only values satisfying both
                // can occur.
                Some(existing) => {
                    let narrowed: BTreeSet<KeyValue> =
                        existing.intersection(&values).cloned().collect();
                    // An empty intersection means the predicate is
                    // unsatisfiable. Routing it anywhere is harmless and
                    // lets the database return the empty result itself, so
                    // keep the narrower side rather than inventing a state.
                    if !narrowed.is_empty() {
                        *existing = narrowed;
                    }
                }
                None => {
                    self.columns.insert(column, values);
                }
            }
        }
        self
    }

    /// Combines two sides of an `OR`.
    #[must_use]
    pub fn or(self, mut other: Self) -> Self {
        let mut merged = BTreeMap::new();
        for (column, mut values) in self.columns {
            // A column only survives if the other branch pins it too;
            // otherwise that branch admits every value.
            if let Some(other_values) = other.columns.remove(&column) {
                values.extend(other_values);
                merged.insert(column, values);
            }
        }
        Self { columns: merged }
    }

    /// Rewrites qualifiers through an alias map, so that `u.tenant` becomes
    /// `users.tenant`.
    #[must_use]
    pub fn resolve_aliases(self, aliases: &BTreeMap<String, String>) -> Self {
        let columns = self
            .columns
            .into_iter()
            .map(|(mut column, values)| {
                if let Some(qualifier) = &column.qualifier
                    && let Some(table) = aliases.get(qualifier)
                {
                    column.qualifier = Some(table.clone());
                }
                (column, values)
            })
            .collect();
        Self { columns }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn literal(text: &str) -> KeyValue {
        KeyValue::Literal(text.to_owned())
    }

    fn set(values: &[&str]) -> BTreeSet<KeyValue> {
        values.iter().map(|v| literal(v)).collect()
    }

    fn tenant() -> ColumnRef {
        ColumnRef::bare("tenant_id")
    }

    #[test]
    fn and_keeps_a_constraint_from_either_side() {
        let left = KeyConstraints::pinned(tenant(), literal("7"));
        let right = KeyConstraints::pinned(ColumnRef::bare("status"), literal("'live'"));
        let combined = left.and(right);
        assert_eq!(
            combined.values_for("users", "tenant_id"),
            Some(&set(&["7"]))
        );
        assert_eq!(
            combined.values_for("users", "status"),
            Some(&set(&["'live'"]))
        );
    }

    #[test]
    fn and_intersects_when_both_sides_pin_the_same_column() {
        let left = KeyConstraints::pinned_to_any_of(tenant(), set(&["1", "2", "3"]));
        let right = KeyConstraints::pinned_to_any_of(tenant(), set(&["2", "3", "4"]));
        assert_eq!(
            left.and(right).values_for("users", "tenant_id"),
            Some(&set(&["2", "3"]))
        );
    }

    #[test]
    fn an_unsatisfiable_and_does_not_collapse_to_nothing() {
        // `tenant = 1 AND tenant = 2` matches no rows. The router still has
        // to send it somewhere to get an empty result back, so a constraint
        // must survive.
        let left = KeyConstraints::pinned(tenant(), literal("1"));
        let right = KeyConstraints::pinned(tenant(), literal("2"));
        let combined = left.and(right);
        assert!(combined.values_for("users", "tenant_id").is_some());
    }

    #[test]
    fn or_unions_when_both_sides_pin_the_column() {
        let left = KeyConstraints::pinned(tenant(), literal("1"));
        let right = KeyConstraints::pinned(tenant(), literal("2"));
        assert_eq!(
            left.or(right).values_for("users", "tenant_id"),
            Some(&set(&["1", "2"]))
        );
    }

    #[test]
    fn or_drops_a_column_the_other_branch_leaves_free() {
        // The classic trap: one branch pins the tenant, the other does not,
        // so the statement can match any tenant at all.
        let pinned = KeyConstraints::pinned(tenant(), literal("1"));
        let free = KeyConstraints::pinned(ColumnRef::bare("created_at"), literal("'2026-01-01'"));
        assert!(pinned.or(free).is_unconstrained());
    }

    #[test]
    fn or_is_commutative_for_the_dropped_column_case() {
        let pinned = KeyConstraints::pinned(tenant(), literal("1"));
        let free = KeyConstraints::unconstrained();
        assert!(pinned.clone().or(free.clone()).is_unconstrained());
        assert!(free.or(pinned).is_unconstrained());
    }

    #[test]
    fn a_qualified_constraint_beats_an_unqualified_one() {
        let combined =
            KeyConstraints::pinned(ColumnRef::qualified("users", "tenant_id"), literal("1"))
                .and(KeyConstraints::pinned(tenant(), literal("2")));
        assert_eq!(
            combined.values_for("users", "tenant_id"),
            Some(&set(&["1"]))
        );
    }

    #[test]
    fn a_constraint_on_another_table_does_not_match() {
        let combined =
            KeyConstraints::pinned(ColumnRef::qualified("orders", "tenant_id"), literal("1"));
        assert_eq!(combined.values_for("users", "tenant_id"), None);
    }

    #[test]
    fn aliases_resolve_to_real_table_names() {
        let aliases = BTreeMap::from([("u".to_owned(), "users".to_owned())]);
        let resolved = KeyConstraints::pinned(ColumnRef::qualified("u", "tenant_id"), literal("1"))
            .resolve_aliases(&aliases);
        assert_eq!(
            resolved.values_for("users", "tenant_id"),
            Some(&set(&["1"]))
        );
    }

    #[test]
    fn an_empty_in_list_constrains_nothing() {
        assert!(KeyConstraints::pinned_to_any_of(tenant(), BTreeSet::new()).is_unconstrained());
    }

    #[test]
    fn placeholders_and_literals_are_distinct_values() {
        let combined = KeyConstraints::pinned(tenant(), KeyValue::Parameter(1))
            .or(KeyConstraints::pinned(tenant(), literal("1")));
        assert_eq!(
            combined.values_for("users", "tenant_id").map(BTreeSet::len),
            Some(2)
        );
    }
}
