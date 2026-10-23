//! Combining results that came back from several regions.
//!
//! [`luxbyte_sql::combine`] decides what each region runs and what has to
//! happen afterwards. This module does the part that can be done here,
//! exactly: concatenating, merging already-sorted streams, and applying a
//! limit. Anything it cannot do exactly — sorting text in a collation's
//! order, deduplicating, aggregating — is finished in PostgreSQL instead.
//!
//! The comparison is where a merge goes quietly wrong. Every value arrives as
//! text, and text order is wrong for numbers (`"10" < "9"`), and `f64` is
//! wrong for big ones (it cannot tell 9007199254740993 from
//! 9007199254740992). So only types with an exact comparison here are merged
//! here, and each is compared the way PostgreSQL compares it.

use std::cmp::Ordering;

use luxbyte_pool::QueryResult;

/// One region's answer, kept with its region so errors can name it.
#[derive(Debug, Clone)]
pub struct RegionResult {
    /// Which region produced it.
    pub region: String,
    /// What it produced.
    pub result: QueryResult,
}

type Row = Vec<Option<String>>;

/// How a column's values compare outside PostgreSQL, if they can.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Comparison {
    Integer,
    Numeric,
    Float,
    Bool,
    /// Canonical lowercase hex: text order is byte order is uuid order.
    Uuid,
}

fn comparison(oid: i32) -> Option<Comparison> {
    match oid {
        20 | 21 | 23 | 26 => Some(Comparison::Integer),
        1700 => Some(Comparison::Numeric),
        700 | 701 => Some(Comparison::Float),
        16 => Some(Comparison::Bool),
        2950 => Some(Comparison::Uuid),
        _ => None,
    }
}

/// The column names and types of the first region that reported any, so a
/// region that matched no rows does not erase the shape of the result.
fn shape(results: &[RegionResult]) -> (Vec<String>, Vec<i32>) {
    results
        .iter()
        .find(|answer| !answer.result.columns.is_empty())
        .map(|answer| {
            (
                answer.result.columns.clone(),
                answer.result.type_oids.clone(),
            )
        })
        .unwrap_or_default()
}

/// Concatenates the regions' rows.
#[must_use]
pub fn concatenate(results: Vec<RegionResult>) -> QueryResult {
    let (columns, type_oids) = shape(&results);
    let rows = results
        .into_iter()
        .flat_map(|answer| answer.result.rows)
        .collect();
    finished(columns, type_oids, rows)
}

/// One sort key: column, descending, nulls first.
pub type SortColumn = (usize, bool, bool);

/// Merges already-sorted regions into one sorted result, if every sort
/// column can be compared exactly here. `None` means it cannot, and the
/// result has to be finished in PostgreSQL.
#[must_use]
pub fn sorted(results: &[RegionResult], keys: &[SortColumn]) -> Option<Vec<Row>> {
    let (_, type_oids) = shape(results);
    let comparisons: Vec<(SortColumn, Comparison)> = keys
        .iter()
        .map(|key| Some((*key, comparison(*type_oids.get(key.0)?)?)))
        .collect::<Option<_>>()?;

    let mut streams: Vec<std::slice::Iter<'_, Row>> = results
        .iter()
        .map(|answer| answer.result.rows.iter())
        .collect();
    let mut heads: Vec<Option<&Row>> = streams.iter_mut().map(Iterator::next).collect();
    let total = results.iter().map(|answer| answer.result.rows.len()).sum();
    let mut out: Vec<Row> = Vec::with_capacity(total);

    // A k-way merge: each region sorted its own rows, so the next row
    // overall is the least of the regions' next rows.
    loop {
        let mut best: Option<usize> = None;
        for (index, head) in heads.iter().enumerate() {
            let Some(row) = head else { continue };
            let better = match best.and_then(|current| heads.get(current).copied().flatten()) {
                None => true,
                Some(incumbent) => compare(row, incumbent, &comparisons).is_lt(),
            };
            if better {
                best = Some(index);
            }
        }
        let Some(index) = best else { return Some(out) };
        if let Some(row) = heads.get(index).copied().flatten() {
            out.push(row.clone());
        }
        let next = streams.get_mut(index).and_then(Iterator::next);
        if let Some(slot) = heads.get_mut(index) {
            *slot = next;
        }
    }
}

/// Skips `offset` rows, keeps at most `limit`, and drops the last `hidden`
/// columns, which were only there to sort on.
#[must_use]
pub fn finish_rows(
    columns: Vec<String>,
    type_oids: Vec<i32>,
    rows: Vec<Row>,
    offset: u64,
    limit: Option<u64>,
    hidden: usize,
) -> QueryResult {
    let skip = usize::try_from(offset).unwrap_or(usize::MAX);
    let take = limit.map_or(usize::MAX, |limit| {
        usize::try_from(limit).unwrap_or(usize::MAX)
    });
    let visible = columns.len().saturating_sub(hidden);
    let rows = rows
        .into_iter()
        .skip(skip)
        .take(take)
        .map(|mut row| {
            row.truncate(visible);
            row
        })
        .collect();
    let mut columns = columns;
    columns.truncate(visible);
    let mut type_oids = type_oids;
    type_oids.truncate(visible);
    finished(columns, type_oids, rows)
}

/// The columns and types the regions reported, for the final statement.
#[must_use]
pub fn columns_of(results: &[RegionResult]) -> (Vec<String>, Vec<i32>) {
    shape(results)
}

/// Each column's values across every region, for carrying to PostgreSQL.
#[must_use]
pub fn by_column(results: &[RegionResult], width: usize) -> Vec<Vec<Option<String>>> {
    let mut columns: Vec<Vec<Option<String>>> = vec![Vec::new(); width];
    for row in results.iter().flat_map(|answer| &answer.result.rows) {
        for (index, column) in columns.iter_mut().enumerate() {
            column.push(row.get(index).cloned().flatten());
        }
    }
    columns
}

fn finished(columns: Vec<String>, type_oids: Vec<i32>, rows: Vec<Row>) -> QueryResult {
    // A merged result spans several command tags; one summarising tag is
    // what a client expects to see.
    let tags = vec![format!("SELECT {}", rows.len())];
    QueryResult {
        columns,
        type_oids,
        rows,
        tags,
        status: luxbyte_proto::backend::TransactionStatus::Idle,
    }
}

fn compare(left: &Row, right: &Row, keys: &[(SortColumn, Comparison)]) -> Ordering {
    for ((column, descending, nulls_first), how) in keys {
        let a = left.get(*column).and_then(Option::as_deref);
        let b = right.get(*column).and_then(Option::as_deref);
        let ordering = match (a, b) {
            (None, None) => Ordering::Equal,
            // A null goes to whichever end the clause asked for, and that is
            // not affected by the ascending/descending flip below.
            (None, Some(_)) => {
                return if *nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }
            (Some(_), None) => {
                return if *nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
            }
            (Some(a), Some(b)) => compare_values(a, b, *how),
        };
        if ordering != Ordering::Equal {
            return if *descending {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }
    Ordering::Equal
}

fn compare_values(a: &str, b: &str, how: Comparison) -> Ordering {
    match how {
        Comparison::Integer => match (a.parse::<i128>(), b.parse::<i128>()) {
            (Ok(a), Ok(b)) => a.cmp(&b),
            _ => a.cmp(b),
        },
        Comparison::Numeric => compare_numeric(a, b),
        Comparison::Float => compare_float(a, b),
        Comparison::Bool | Comparison::Uuid => a.cmp(b),
    }
}

/// PostgreSQL's float order: every number, then infinity, then `NaN`, which
/// equals itself.
fn compare_float(a: &str, b: &str) -> Ordering {
    match (a.parse::<f64>(), b.parse::<f64>()) {
        (Ok(a), Ok(b)) => match (a.is_nan(), b.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
        },
        _ => a.cmp(b),
    }
}

/// `numeric` compared digit by digit, never through a float: `NaN` above
/// everything, then infinity, and `-Infinity` below every number.
fn compare_numeric(a: &str, b: &str) -> Ordering {
    fn rank(text: &str) -> Option<i8> {
        match text {
            "NaN" => Some(3),
            "Infinity" => Some(2),
            "-Infinity" => Some(-2),
            _ => None,
        }
    }
    match (rank(a), rank(b)) {
        (Some(x), Some(y)) => return x.cmp(&y),
        (Some(x), None) => {
            return if x > 0 {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        (None, Some(y)) => {
            return if y > 0 {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        (None, None) => {}
    }

    let split = |text: &str| -> (bool, String, String) {
        let (negative, digits) = text
            .strip_prefix('-')
            .map_or((false, text), |rest| (true, rest));
        let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
        let whole = whole.trim_start_matches('0').to_owned();
        let fraction = fraction.trim_end_matches('0').to_owned();
        let zero = whole.is_empty() && fraction.is_empty();
        (negative && !zero, whole, fraction)
    };
    let (a_negative, a_whole, a_fraction) = split(a);
    let (b_negative, b_whole, b_fraction) = split(b);
    match (a_negative, b_negative) {
        (true, false) => return Ordering::Less,
        (false, true) => return Ordering::Greater,
        _ => {}
    }
    // With leading zeros gone, a longer whole part is a larger magnitude;
    // with trailing zeros gone, fractions compare as text.
    let magnitude = a_whole
        .len()
        .cmp(&b_whole.len())
        .then_with(|| a_whole.cmp(&b_whole))
        .then_with(|| a_fraction.cmp(&b_fraction));
    if a_negative {
        magnitude.reverse()
    } else {
        magnitude
    }
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

    fn first_column(rows: &[Row]) -> Vec<Option<&str>> {
        rows.iter()
            .map(|row| row.first().and_then(Option::as_deref))
            .collect()
    }

    #[test]
    fn concatenating_keeps_every_row_and_the_column_names() {
        let merged = concatenate(vec![
            region("eu", result(&["id"], &[23], &[&[Some("1")], &[Some("2")]])),
            region("us", result(&["id"], &[23], &[&[Some("3")]])),
        ]);
        assert_eq!(merged.columns, ["id"]);
        assert_eq!(merged.rows.len(), 3);
        assert_eq!(merged.tags, ["SELECT 3"]);
    }

    #[test]
    fn a_region_with_no_rows_does_not_erase_the_result_shape() {
        let merged = concatenate(vec![
            region("eu", result(&[], &[], &[])),
            region(
                "us",
                result(&["id", "name"], &[23, 25], &[&[Some("1"), Some("a")]]),
            ),
        ]);
        assert_eq!(merged.columns, ["id", "name"]);
        assert_eq!(merged.type_oids, [23, 25]);
    }

    #[test]
    fn a_sorted_merge_interleaves_the_regions() {
        let rows = sorted(
            &[
                region("eu", result(&["id"], &[23], &[&[Some("1")], &[Some("4")]])),
                region("us", result(&["id"], &[23], &[&[Some("2")], &[Some("3")]])),
            ],
            &[(0, false, false)],
        )
        .expect("integers merge here");
        assert_eq!(
            first_column(&rows),
            [Some("1"), Some("2"), Some("3"), Some("4")]
        );
    }

    #[test]
    fn integers_too_big_for_a_float_still_compare_exactly() {
        // 2^53 + 1 and 2^53 are the same f64.
        let rows = sorted(
            &[
                region("eu", result(&["id"], &[20], &[&[Some("9007199254740993")]])),
                region("us", result(&["id"], &[20], &[&[Some("9007199254740992")]])),
            ],
            &[(0, false, false)],
        )
        .expect("int8 merges here");
        assert_eq!(
            first_column(&rows),
            [Some("9007199254740992"), Some("9007199254740993")]
        );
    }

    #[test]
    fn numerics_compare_digit_by_digit() {
        use Ordering::{Equal, Greater, Less};
        for (a, b, expected) in [
            ("10", "9", Greater),
            ("0.45", "0.5", Less),
            ("1.50", "1.5", Equal),
            ("-2", "-10", Greater),
            ("-0.5", "0", Less),
            (
                "123456789012345678901234567890.1",
                "123456789012345678901234567890.01",
                Greater,
            ),
            ("NaN", "Infinity", Greater),
            ("Infinity", "99999999999999999999", Greater),
            ("-Infinity", "-99999999999999999999", Less),
        ] {
            assert_eq!(compare_numeric(a, b), expected, "{a} vs {b}");
        }
    }

    #[test]
    fn floats_put_nan_after_infinity_as_postgres_does() {
        assert_eq!(compare_float("NaN", "Infinity"), Ordering::Greater);
        assert_eq!(compare_float("NaN", "NaN"), Ordering::Equal);
        assert_eq!(compare_float("-0", "0"), Ordering::Equal);
        assert_eq!(compare_float("1e+100", "9e99"), Ordering::Greater);
    }

    #[test]
    fn text_is_not_merged_here() {
        // Text sorts by collation, which is PostgreSQL's to apply.
        assert!(
            sorted(
                &[region("eu", result(&["name"], &[25], &[&[Some("b")]]))],
                &[(0, false, false)]
            )
            .is_none()
        );
    }

    #[test]
    fn descending_order_and_nulls_are_respected() {
        let rows = sorted(
            &[
                region(
                    "eu",
                    result(&["id"], &[23], &[&[None], &[Some("3")], &[Some("1")]]),
                ),
                region("us", result(&["id"], &[23], &[&[Some("2")]])),
            ],
            // DESC puts nulls first by default.
            &[(0, true, true)],
        )
        .expect("merge");
        assert_eq!(first_column(&rows), [None, Some("3"), Some("2"), Some("1")]);
    }

    #[test]
    fn a_second_sort_key_breaks_ties() {
        let rows = sorted(
            &[
                region(
                    "eu",
                    result(
                        &["grp", "id"],
                        &[23, 23],
                        &[&[Some("1"), Some("2")], &[Some("2"), Some("1")]],
                    ),
                ),
                region(
                    "us",
                    result(&["grp", "id"], &[23, 23], &[&[Some("1"), Some("1")]]),
                ),
            ],
            &[(0, false, false), (1, false, false)],
        )
        .expect("merge");
        let pairs: Vec<(Option<&str>, Option<&str>)> = rows
            .iter()
            .map(|row| {
                (
                    row.first().and_then(Option::as_deref),
                    row.get(1).and_then(Option::as_deref),
                )
            })
            .collect();
        assert_eq!(
            pairs,
            [
                (Some("1"), Some("1")),
                (Some("1"), Some("2")),
                (Some("2"), Some("1"))
            ]
        );
    }

    #[test]
    fn limits_offsets_and_hidden_columns_are_applied_once() {
        let rows: Vec<Row> = (1..=5)
            .map(|n| vec![Some(n.to_string()), Some(format!("sort{n}"))])
            .collect();
        let finished = finish_rows(
            vec!["id".to_owned(), "__luxbyte_sort_0".to_owned()],
            vec![23, 25],
            rows,
            1,
            Some(2),
            1,
        );
        assert_eq!(finished.columns, ["id"]);
        assert_eq!(finished.type_oids, [23]);
        assert_eq!(first_column(&finished.rows), [Some("2"), Some("3")]);
        assert!(finished.rows.iter().all(|row| row.len() == 1));
        assert_eq!(finished.tags, ["SELECT 2"]);
    }

    #[test]
    fn values_are_regrouped_by_column_for_carrying() {
        let columns = by_column(
            &[
                region("eu", result(&["a", "b"], &[23, 25], &[&[Some("1"), None]])),
                region(
                    "us",
                    result(&["a", "b"], &[23, 25], &[&[Some("2"), Some("x")]]),
                ),
            ],
            2,
        );
        assert_eq!(
            columns,
            [
                vec![Some("1".to_owned()), Some("2".to_owned())],
                vec![None, Some("x".to_owned())]
            ]
        );
    }
}
