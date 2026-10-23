//! What a statement does to its rows after collecting them.
//!
//! A read that fans out is only answered by concatenating the regions'
//! rows when it is a plain union of them. Anything computed over the rows —
//! an order, a limit, a deduplication, an aggregate — has to be redone over
//! the combined rows, which [`crate::combine`] plans from the statement's
//! text when it runs. This records, at analysis time, whether that is
//! needed at all.

use serde::{Deserialize, Serialize};

/// One `ORDER BY` term, as text plus direction.
///
/// The expression is kept as rendered SQL rather than a parse tree: the merge
/// only needs to know *which* output column to compare and in which
/// direction, and positional terms (`ORDER BY 1`) render as themselves.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortKey {
    /// The ordering expression, as written.
    pub expression: String,
    /// Whether the order is descending.
    pub descending: bool,
    /// Whether nulls sort first, if the statement said.
    pub nulls_first: Option<bool>,
}

/// What a statement does to its rows after collecting them.
// Independent facts about one statement, any combination of which can be
// true at once; an enum of states would have to list every combination.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostProcessing {
    /// An aggregate function, `GROUP BY`, or `HAVING` is present.
    pub aggregates: bool,
    /// `DISTINCT` is present.
    pub distinct: bool,
    /// `ORDER BY` terms, in order.
    pub order_by: Vec<SortKey>,
    /// A `LIMIT` or `OFFSET` is present.
    pub limited: bool,
    /// The statement is a `UNION`, `INTERSECT` or `EXCEPT`.
    #[serde(default)]
    pub set_operation: bool,
}

/// How a scattered result can be put back together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeStrategy {
    /// Concatenate the regions' rows in any order.
    Concatenate,
    /// Rewrite the statement for the regions and finish the combined
    /// result: see [`crate::combine`].
    Combine,
}

impl PostProcessing {
    /// How results from several regions are put back together.
    #[must_use]
    pub fn merge_strategy(&self) -> MergeStrategy {
        if self.is_trivial() {
            MergeStrategy::Concatenate
        } else {
            MergeStrategy::Combine
        }
    }

    /// Whether anything at all has to happen after collecting rows.
    #[must_use]
    pub fn is_trivial(&self) -> bool {
        !self.aggregates
            && !self.distinct
            && self.order_by.is_empty()
            && !self.limited
            && !self.set_operation
    }
}

/// Function names treated as aggregates.
///
/// A deliberately conservative list of the standard PostgreSQL aggregates. An
/// unknown function is *not* assumed to be scalar — see
/// [`is_aggregate_name`] — because a user-defined aggregate that was guessed
/// wrong would return quietly wrong numbers.
const AGGREGATES: &[&str] = &[
    "array_agg",
    "avg",
    "bit_and",
    "bit_or",
    "bool_and",
    "bool_or",
    "corr",
    "count",
    "covar_pop",
    "covar_samp",
    "every",
    "json_agg",
    "json_object_agg",
    "jsonb_agg",
    "jsonb_object_agg",
    "max",
    "min",
    "range_agg",
    "regr_slope",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "string_agg",
    "sum",
    "var_pop",
    "var_samp",
    "variance",
    "xmlagg",
];

/// Whether a function name is a known aggregate.
#[must_use]
pub fn is_aggregate_name(name: &str) -> bool {
    AGGREGATES
        .binary_search(&name.to_lowercase().as_str())
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plain_query_concatenates() {
        assert_eq!(
            PostProcessing::default().merge_strategy(),
            MergeStrategy::Concatenate
        );
    }

    #[test]
    fn anything_computed_over_the_rows_needs_combining() {
        for post in [
            PostProcessing {
                aggregates: true,
                ..PostProcessing::default()
            },
            PostProcessing {
                distinct: true,
                ..PostProcessing::default()
            },
            PostProcessing {
                limited: true,
                ..PostProcessing::default()
            },
            PostProcessing {
                set_operation: true,
                ..PostProcessing::default()
            },
            PostProcessing {
                order_by: vec![SortKey {
                    expression: "id".to_owned(),
                    descending: false,
                    nulls_first: None,
                }],
                ..PostProcessing::default()
            },
        ] {
            assert_eq!(post.merge_strategy(), MergeStrategy::Combine, "{post:?}");
        }
    }

    #[test]
    fn the_aggregate_list_is_sorted_so_the_binary_search_is_valid() {
        let mut sorted = AGGREGATES.to_vec();
        sorted.sort_unstable();
        assert_eq!(sorted.as_slice(), AGGREGATES);
    }

    #[test]
    fn aggregate_names_are_matched_without_regard_to_case() {
        assert!(is_aggregate_name("count"));
        assert!(is_aggregate_name("COUNT"));
        assert!(is_aggregate_name("Sum"));
        assert!(!is_aggregate_name("lower"));
        assert!(!is_aggregate_name("my_custom_thing"));
    }
}
