//! Combining results that came back from several regions.
//!
//! The planner has already decided that a merge is sound — it refuses
//! aggregates and `DISTINCT` rather than letting them get this far. What is
//! left is mechanical: concatenate, or merge already-sorted streams.
//!
//! The part worth getting right is the comparison. Every value arrives as
//! text, and comparing text is wrong for numbers: `"10" < "9"`. So the
//! column's datatype OID decides whether a key compares numerically or
//! lexicographically.

use luxbyte_pool::QueryResult;
use luxbyte_sql::{MergeStrategy, SortKey};

/// Datatype OIDs that must compare as numbers rather than as text.
///
/// From `pg_type`: the integer, floating point and numeric families.
const NUMERIC_OIDS: &[i32] = &[
    20,   // int8
    21,   // int2
    23,   // int4
    26,   // oid
    700,  // float4
    701,  // float8
    1700, // numeric
];

fn is_numeric(oid: i32) -> bool {
    NUMERIC_OIDS.contains(&oid)
}

/// One region's answer, kept with its region so errors can name it.
#[derive(Debug, Clone)]
pub struct RegionResult {
    /// Which region produced it.
    pub region: String,
    /// What it produced.
    pub result: QueryResult,
}

/// Combines results from several regions into one.
///
/// Column names and types come from the first region that returned any, so
/// a region that matched no rows does not erase the shape of the result.
#[must_use]
pub fn merge(results: Vec<RegionResult>, strategy: &MergeStrategy) -> QueryResult {
    let mut merged = QueryResult::default();

    for answer in &results {
        if !answer.result.columns.is_empty() {
            merged.columns.clone_from(&answer.result.columns);
            merged.type_oids.clone_from(&answer.result.type_oids);
            break;
        }
    }

    match strategy {
        MergeStrategy::Concatenate => {
            for answer in results {
                merged.rows.extend(answer.result.rows);
            }
        }
        MergeStrategy::SortedMerge(keys) => {
            let indices = key_indices(&merged.columns, keys);
            if indices.is_empty() {
                // The sort keys are expressions rather than output columns,
                // so there is nothing here to compare. Concatenating keeps
                // every row; it just does not promise an order.
                for answer in results {
                    merged.rows.extend(answer.result.rows);
                }
            } else {
                merged.rows = sorted_merge(results, &indices, &merged.type_oids);
            }
        }
    }

    // A merged result spans several command tags; one summarising tag is
    // what a client expects to see.
    merged.tags = vec![format!("SELECT {}", merged.rows.len())];
    merged
}

/// A sort key resolved to an output column.
#[derive(Debug, Clone, Copy)]
struct ResolvedKey {
    column: usize,
    descending: bool,
    nulls_first: bool,
}

fn key_indices(columns: &[String], keys: &[SortKey]) -> Vec<ResolvedKey> {
    keys.iter()
        .filter_map(|key| {
            let wanted = key.expression.trim().to_lowercase();
            let column = columns
                .iter()
                .position(|name| name.to_lowercase() == wanted)
                // `ORDER BY 2` names an output column by position.
                .or_else(|| {
                    wanted
                        .parse::<usize>()
                        .ok()
                        .and_then(|ordinal| ordinal.checked_sub(1))
                        .filter(|index| *index < columns.len())
                })?;
            Some(ResolvedKey {
                column,
                descending: key.descending,
                // PostgreSQL defaults to nulls last for ascending order and
                // nulls first for descending.
                nulls_first: key.nulls_first.unwrap_or(key.descending),
            })
        })
        .collect()
}

type Row = Vec<Option<String>>;

/// Merges each region's already-ordered rows into one ordered stream.
///
/// Each region sorted its own rows; this interleaves them, which is a k-way
/// merge rather than a re-sort and keeps the work proportional to the rows
/// actually returned.
fn sorted_merge(results: Vec<RegionResult>, keys: &[ResolvedKey], type_oids: &[i32]) -> Vec<Row> {
    let mut streams: Vec<std::vec::IntoIter<Row>> = results
        .into_iter()
        .map(|answer| answer.result.rows.into_iter())
        .collect();
    let mut heads: Vec<Option<Row>> = streams.iter_mut().map(Iterator::next).collect();

    let total: usize = heads.iter().filter(|head| head.is_some()).count();
    let mut out: Vec<Row> = Vec::with_capacity(total);

    loop {
        let mut best: Option<usize> = None;
        for (index, head) in heads.iter().enumerate() {
            let Some(row) = head.as_ref() else { continue };
            let is_better = match best {
                None => true,
                Some(current) => heads
                    .get(current)
                    .and_then(|slot| slot.as_ref())
                    .is_some_and(|incumbent| compare(row, incumbent, keys, type_oids).is_lt()),
            };
            if is_better {
                best = Some(index);
            }
        }

        let Some(index) = best else { return out };
        let Some(slot) = heads.get_mut(index) else {
            return out;
        };
        if let Some(row) = slot.take() {
            out.push(row);
        }
        if let Some(stream) = streams.get_mut(index)
            && let Some(slot) = heads.get_mut(index)
        {
            *slot = stream.next();
        }
    }
}

fn compare(left: &Row, right: &Row, keys: &[ResolvedKey], type_oids: &[i32]) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    for key in keys {
        let a = left.get(key.column).and_then(Option::as_deref);
        let b = right.get(key.column).and_then(Option::as_deref);
        let numeric = type_oids.get(key.column).copied().is_some_and(is_numeric);

        let ordering = match (a, b) {
            (None, None) => Ordering::Equal,
            // A null sorts to whichever end the clause asked for, and that
            // is not affected by the ascending/descending flip below.
            (None, Some(_)) => {
                return if key.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }
            (Some(_), None) => {
                return if key.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
            }
            (Some(a), Some(b)) => compare_values(a, b, numeric),
        };

        if ordering != Ordering::Equal {
            return if key.descending {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }
    Ordering::Equal
}

fn compare_values(a: &str, b: &str, numeric: bool) -> std::cmp::Ordering {
    if numeric {
        // Both parse, or neither comparison is meaningful as a number and
        // text is the honest fallback.
        if let (Ok(a), Ok(b)) = (a.parse::<f64>(), b.parse::<f64>()) {
            return a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal);
        }
    }
    a.cmp(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(columns: &[&str], oids: &[i32], rows: &[&[Option<&str>]]) -> QueryResult {
        QueryResult {
            columns: columns.iter().map(|c| (*c).to_owned()).collect(),
            type_oids: oids.to_vec(),
            rows: rows
                .iter()
                .map(|row| row.iter().map(|v| v.map(ToOwned::to_owned)).collect())
                .collect(),
            tags: vec!["SELECT".to_owned()],
            status: luxbyte_proto::backend::TransactionStatus::Idle,
        }
    }

    fn region(name: &str, result: QueryResult) -> RegionResult {
        RegionResult {
            region: name.to_owned(),
            result,
        }
    }

    fn column(merged: &QueryResult, index: usize) -> Vec<Option<&str>> {
        merged
            .rows
            .iter()
            .map(|row| row.get(index).and_then(Option::as_deref))
            .collect()
    }

    fn text_key(name: &str, descending: bool) -> SortKey {
        SortKey {
            expression: name.to_owned(),
            descending,
            nulls_first: None,
        }
    }

    #[test]
    fn concatenating_keeps_every_row_and_the_column_names() {
        let merged = merge(
            vec![
                region("eu", result(&["id"], &[23], &[&[Some("1")], &[Some("2")]])),
                region("us", result(&["id"], &[23], &[&[Some("3")]])),
            ],
            &MergeStrategy::Concatenate,
        );
        assert_eq!(merged.columns, ["id"]);
        assert_eq!(merged.rows.len(), 3);
        assert_eq!(merged.tags, ["SELECT 3"]);
    }

    #[test]
    fn a_region_with_no_rows_does_not_erase_the_result_shape() {
        let merged = merge(
            vec![
                region("eu", result(&[], &[], &[])),
                region(
                    "us",
                    result(&["id", "name"], &[23, 25], &[&[Some("1"), Some("a")]]),
                ),
            ],
            &MergeStrategy::Concatenate,
        );
        assert_eq!(merged.columns, ["id", "name"]);
        assert_eq!(merged.type_oids, [23, 25]);
    }

    #[test]
    fn a_sorted_merge_interleaves_the_regions() {
        let merged = merge(
            vec![
                region("eu", result(&["id"], &[23], &[&[Some("1")], &[Some("4")]])),
                region("us", result(&["id"], &[23], &[&[Some("2")], &[Some("3")]])),
            ],
            &MergeStrategy::SortedMerge(vec![text_key("id", false)]),
        );
        assert_eq!(
            column(&merged, 0),
            [Some("1"), Some("2"), Some("3"), Some("4")]
        );
    }

    #[test]
    fn numeric_columns_compare_as_numbers_not_as_text() {
        // The bug this guards against: "10" sorting before "9".
        let merged = merge(
            vec![
                region("eu", result(&["id"], &[23], &[&[Some("9")]])),
                region(
                    "us",
                    result(&["id"], &[23], &[&[Some("10")], &[Some("100")]]),
                ),
            ],
            &MergeStrategy::SortedMerge(vec![text_key("id", false)]),
        );
        assert_eq!(column(&merged, 0), [Some("9"), Some("10"), Some("100")]);
    }

    #[test]
    fn text_columns_still_compare_as_text() {
        let merged = merge(
            vec![
                region("eu", result(&["name"], &[25], &[&[Some("alice")]])),
                region("us", result(&["name"], &[25], &[&[Some("bob")]])),
            ],
            &MergeStrategy::SortedMerge(vec![text_key("name", false)]),
        );
        assert_eq!(column(&merged, 0), [Some("alice"), Some("bob")]);
    }

    #[test]
    fn descending_order_is_respected() {
        let merged = merge(
            vec![
                region("eu", result(&["id"], &[23], &[&[Some("4")], &[Some("1")]])),
                region("us", result(&["id"], &[23], &[&[Some("3")], &[Some("2")]])),
            ],
            &MergeStrategy::SortedMerge(vec![text_key("id", true)]),
        );
        assert_eq!(
            column(&merged, 0),
            [Some("4"), Some("3"), Some("2"), Some("1")]
        );
    }

    #[test]
    fn a_second_sort_key_breaks_ties() {
        let merged = merge(
            vec![
                region(
                    "eu",
                    result(&["grp", "id"], &[23, 23], &[&[Some("1"), Some("2")]]),
                ),
                region(
                    "us",
                    result(&["grp", "id"], &[23, 23], &[&[Some("1"), Some("1")]]),
                ),
            ],
            &MergeStrategy::SortedMerge(vec![text_key("grp", false), text_key("id", false)]),
        );
        assert_eq!(column(&merged, 1), [Some("1"), Some("2")]);
    }

    #[test]
    fn nulls_sort_last_ascending_and_first_descending() {
        let rows = || {
            vec![
                region("eu", result(&["id"], &[23], &[&[None]])),
                region("us", result(&["id"], &[23], &[&[Some("1")]])),
            ]
        };
        let ascending = merge(
            rows(),
            &MergeStrategy::SortedMerge(vec![text_key("id", false)]),
        );
        assert_eq!(column(&ascending, 0), [Some("1"), None]);

        let descending = merge(
            rows(),
            &MergeStrategy::SortedMerge(vec![text_key("id", true)]),
        );
        assert_eq!(column(&descending, 0), [None, Some("1")]);
    }

    #[test]
    fn an_explicit_nulls_first_overrides_the_default() {
        let merged = merge(
            vec![
                region("eu", result(&["id"], &[23], &[&[None]])),
                region("us", result(&["id"], &[23], &[&[Some("1")]])),
            ],
            &MergeStrategy::SortedMerge(vec![SortKey {
                expression: "id".to_owned(),
                descending: false,
                nulls_first: Some(true),
            }]),
        );
        assert_eq!(column(&merged, 0), [None, Some("1")]);
    }

    #[test]
    fn ordering_by_column_position_works() {
        let merged = merge(
            vec![
                region(
                    "eu",
                    result(&["a", "b"], &[25, 23], &[&[Some("x"), Some("2")]]),
                ),
                region(
                    "us",
                    result(&["a", "b"], &[25, 23], &[&[Some("x"), Some("1")]]),
                ),
            ],
            &MergeStrategy::SortedMerge(vec![text_key("2", false)]),
        );
        assert_eq!(column(&merged, 1), [Some("1"), Some("2")]);
    }

    #[test]
    fn a_sort_key_that_is_not_an_output_column_falls_back_to_concatenating() {
        // `ORDER BY lower(name)` does not name a column in the result, so
        // there is nothing here to compare. Every row still comes back.
        let merged = merge(
            vec![
                region("eu", result(&["name"], &[25], &[&[Some("b")]])),
                region("us", result(&["name"], &[25], &[&[Some("a")]])),
            ],
            &MergeStrategy::SortedMerge(vec![text_key("lower(name)", false)]),
        );
        assert_eq!(merged.rows.len(), 2);
    }

    #[test]
    fn merging_nothing_produces_an_empty_result() {
        let merged = merge(Vec::new(), &MergeStrategy::Concatenate);
        assert_eq!(merged.rows.len(), 0);
        assert_eq!(merged.tags, ["SELECT 0"]);
    }

    #[test]
    fn regions_of_wildly_different_sizes_merge_correctly() {
        let many: Vec<Vec<Option<String>>> =
            (0..100).map(|n| vec![Some((n * 2).to_string())]).collect();
        let few = vec![vec![Some("1".to_owned())], vec![Some("199".to_owned())]];
        let merged = merge(
            vec![
                region(
                    "eu",
                    QueryResult {
                        columns: vec!["id".to_owned()],
                        type_oids: vec![23],
                        rows: many,
                        ..QueryResult::default()
                    },
                ),
                region(
                    "us",
                    QueryResult {
                        columns: vec!["id".to_owned()],
                        type_oids: vec![23],
                        rows: few,
                        ..QueryResult::default()
                    },
                ),
            ],
            &MergeStrategy::SortedMerge(vec![text_key("id", false)]),
        );
        assert_eq!(merged.rows.len(), 102);
        let values: Vec<i64> = merged
            .rows
            .iter()
            .filter_map(|row| row.first()?.as_deref()?.parse().ok())
            .collect();
        assert!(values.windows(2).all(|pair| pair[0] <= pair[1]));
    }
}
