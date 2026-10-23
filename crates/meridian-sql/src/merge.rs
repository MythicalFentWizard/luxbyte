//! Whether a result can be assembled from several regions.
//!
//! Fanning a query out and concatenating what comes back is only correct for
//! queries whose answer is a plain union of their inputs. It is wrong the
//! moment the query computes something *over* its rows:
//!
//! - `count(*)` returns one row per region, not one row.
//! - `GROUP BY` produces per-region groups that need re-grouping.
//! - `DISTINCT` deduplicates within a region but not between regions.
//! - `ORDER BY` holds within each region and not across them.
//! - `LIMIT 10` returns ten rows from *each* region.
//!
//! The first three need real re-aggregation, which means knowing each
//! function's combine step; Meridian does not attempt that and refuses
//! instead. The last two are mechanical: a k-way merge fixes the order, and
//! the limit is then applied to the merged stream. So they are allowed, and
//! this type records what the executor has to do.

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
}

/// How a scattered result can be put back together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeStrategy {
    /// Concatenate the regions' rows in any order.
    Concatenate,
    /// Merge the regions' already-sorted streams on these keys.
    SortedMerge(Vec<SortKey>),
}

/// Why a result cannot be assembled across regions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeObstacle {
    /// Re-aggregating across regions needs each function's combine step.
    Aggregate,
    /// Deduplicating across regions needs the whole result in one place.
    Distinct,
}

impl MergeObstacle {
    /// A short explanation for the error sent to the client.
    #[must_use]
    pub const fn reason(self) -> &'static str {
        match self {
            Self::Aggregate => {
                "the statement aggregates, and the partial results from each region cannot be \
                 combined without knowing how to merge each aggregate"
            }
            Self::Distinct => {
                "the statement uses DISTINCT, which cannot deduplicate rows that are in \
                 different regions"
            }
        }
    }
}

impl PostProcessing {
    /// Decides how, or whether, results from several regions can be merged.
    ///
    /// A `LIMIT` is not an obstacle: each region is asked for the full
    /// result and the limit is applied once, after merging. That is more
    /// data on the wire than strictly necessary, and still correct, which is
    /// the right order of priorities.
    pub fn merge_strategy(&self) -> Result<MergeStrategy, MergeObstacle> {
        if self.aggregates {
            return Err(MergeObstacle::Aggregate);
        }
        if self.distinct {
            return Err(MergeObstacle::Distinct);
        }
        if self.order_by.is_empty() {
            Ok(MergeStrategy::Concatenate)
        } else {
            Ok(MergeStrategy::SortedMerge(self.order_by.clone()))
        }
    }

    /// Whether anything at all has to happen after collecting rows.
    #[must_use]
    pub fn is_trivial(&self) -> bool {
        !self.aggregates && !self.distinct && self.order_by.is_empty() && !self.limited
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

    fn sort_key(expression: &str, descending: bool) -> SortKey {
        SortKey {
            expression: expression.to_owned(),
            descending,
            nulls_first: None,
        }
    }

    #[test]
    fn a_plain_query_concatenates() {
        assert_eq!(
            PostProcessing::default().merge_strategy(),
            Ok(MergeStrategy::Concatenate)
        );
    }

    #[test]
    fn an_ordered_query_merges_on_its_sort_keys() {
        let post = PostProcessing {
            order_by: vec![sort_key("created_at", true)],
            ..PostProcessing::default()
        };
        assert_eq!(
            post.merge_strategy(),
            Ok(MergeStrategy::SortedMerge(vec![sort_key(
                "created_at",
                true
            )]))
        );
    }

    #[test]
    fn an_aggregate_cannot_be_merged() {
        let post = PostProcessing {
            aggregates: true,
            ..PostProcessing::default()
        };
        assert_eq!(post.merge_strategy(), Err(MergeObstacle::Aggregate));
    }

    #[test]
    fn distinct_cannot_be_merged() {
        let post = PostProcessing {
            distinct: true,
            ..PostProcessing::default()
        };
        assert_eq!(post.merge_strategy(), Err(MergeObstacle::Distinct));
    }

    #[test]
    fn a_limit_alone_does_not_block_a_merge() {
        let post = PostProcessing {
            limited: true,
            ..PostProcessing::default()
        };
        assert_eq!(post.merge_strategy(), Ok(MergeStrategy::Concatenate));
        assert!(!post.is_trivial());
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
