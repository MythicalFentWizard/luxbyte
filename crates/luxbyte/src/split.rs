//! Splitting one statement into the statements each region should run.
//!
//! Most multi-region writes need no splitting. `UPDATE users SET active =
//! false WHERE last_seen < $1` can be sent verbatim to every region,
//! because each one applies it to the rows it holds and the union is
//! exactly what a single server would have done.
//!
//! `INSERT ... VALUES` is the exception, and getting it wrong is silent.
//! Broadcasting
//!
//! ```sql
//! INSERT INTO users (tenant_id, name) VALUES ('alice', 'a'), ('bob', 'b')
//! ```
//!
//! to two regions does not write one row in each. It writes *both* rows in
//! *both* regions, and nothing complains — the duplicate lives in a region
//! the directory will never look in, so it is invisible until the day
//! somebody counts.
//!
//! So the rows are partitioned by where their key belongs, and each region
//! gets an `INSERT` containing only its own. The statement is rebuilt from
//! the parse tree rather than by cutting up the text, because the text has
//! commas inside string literals and the parse tree does not.
//!
//! Anything not understood returns `None`, and the caller refuses the
//! statement rather than guessing.

use std::collections::BTreeMap;

use sqlparser::ast::{Expr, Parens, SetExpr, Statement, TableObject, UnaryOperator, Value, Values};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// A statement, and the region it should run on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionStatement {
    /// Where to run it.
    pub region: String,
    /// What to run.
    pub sql: String,
}

/// Why a statement could not be split.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SplitError {
    /// Not an `INSERT ... VALUES` with an explicit column list.
    NotSplittable,
    /// A row's key was an expression rather than a value, so where it
    /// belongs cannot be known without running it.
    KeyNotLiteral,
    /// The statement does not mention its shard key at all.
    KeyNotPresent,
}

impl SplitError {
    /// What to tell the client.
    #[must_use]
    pub const fn reason(self) -> &'static str {
        match self {
            Self::NotSplittable => {
                "only INSERT ... VALUES with an explicit column list can be split across regions"
            }
            Self::KeyNotLiteral => {
                "a row's shard key is an expression, so which region it belongs to is not known \
                 until it is evaluated"
            }
            Self::KeyNotPresent => "the statement does not supply its shard key",
        }
    }
}

/// Splits a multi-row `INSERT` so each region receives only its own rows.
///
/// `route` maps a key value to a region.
pub fn split_insert(
    sql: &str,
    key_column: &str,
    route: impl Fn(&str) -> String,
) -> Result<Vec<RegionStatement>, SplitError> {
    let statements =
        Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|_| SplitError::NotSplittable)?;
    let [Statement::Insert(insert)] = statements.as_slice() else {
        return Err(SplitError::NotSplittable);
    };
    let TableObject::TableName(_) = &insert.table else {
        return Err(SplitError::NotSplittable);
    };

    let key_index = insert
        .columns
        .iter()
        .position(|name| {
            name.0
                .first()
                .and_then(|part| part.as_ident())
                .is_some_and(|ident| fold(&ident.value, ident.quote_style.is_some()) == key_column)
        })
        .ok_or(SplitError::KeyNotPresent)?;

    let source = insert.source.as_ref().ok_or(SplitError::NotSplittable)?;
    let SetExpr::Values(Values {
        rows,
        explicit_row,
        value_keyword,
    }) = source.body.as_ref()
    else {
        return Err(SplitError::NotSplittable);
    };
    if rows.len() < 2 {
        // One row goes wherever its key goes; there is nothing to split.
        return Err(SplitError::NotSplittable);
    }

    // Group rows by region, keeping the order they were written in so the
    // statement a region receives matches the one the client sent.
    let mut grouped: BTreeMap<String, Vec<Parens<Vec<Expr>>>> = BTreeMap::new();
    for row in rows {
        let key = row
            .content
            .get(key_index)
            .ok_or(SplitError::KeyNotLiteral)?;
        let text = literal_text(key).ok_or(SplitError::KeyNotLiteral)?;
        grouped.entry(route(&text)).or_default().push(row.clone());
    }

    Ok(grouped
        .into_iter()
        .map(|(region, rows)| {
            let mut per_region = insert.clone();
            let mut query = source.as_ref().clone();
            query.body = Box::new(SetExpr::Values(Values {
                explicit_row: *explicit_row,
                value_keyword: *value_keyword,
                rows,
            }));
            per_region.source = Some(Box::new(query));
            RegionStatement {
                region,
                sql: Statement::Insert(per_region).to_string(),
            }
        })
        .collect())
}

fn fold(value: &str, quoted: bool) -> String {
    if quoted {
        value.to_owned()
    } else {
        value.to_lowercase()
    }
}

/// The text of a literal, matching how the analyser normalises shard keys.
fn literal_text(expr: &Expr) -> Option<String> {
    match expr {
        // A cast does not change which row a value selects, and nor do
        // brackets or a leading plus.
        Expr::Nested(inner)
        | Expr::Cast { expr: inner, .. }
        | Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr: inner,
        } => literal_text(inner),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => literal_text(inner).map(|text| format!("-{text}")),
        Expr::Value(value) => match &value.value {
            Value::Number(number, _) => Some(number.clone()),
            Value::SingleQuotedString(text)
            | Value::DoubleQuotedString(text)
            | Value::EscapedStringLiteral(text)
            | Value::NationalStringLiteral(text)
            | Value::UnicodeStringLiteral(text) => Some(text.clone()),
            Value::Boolean(flag) => Some(flag.to_string()),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// alice lives in Europe, everyone else in the US.
    fn route(key: &str) -> String {
        if key == "alice" {
            "eu-west".to_owned()
        } else {
            "us-east".to_owned()
        }
    }

    #[test]
    fn each_region_receives_only_its_own_rows() {
        let parts = split_insert(
            "INSERT INTO users (tenant_id, name) VALUES ('alice', 'a'), ('bob', 'b')",
            "tenant_id",
            route,
        )
        .expect("should split");

        assert_eq!(parts.len(), 2);
        let eu = parts.iter().find(|p| p.region == "eu-west").expect("eu");
        let us = parts.iter().find(|p| p.region == "us-east").expect("us");

        assert!(eu.sql.contains("'alice'"));
        assert!(!eu.sql.contains("'bob'"), "eu must not receive bob's row");
        assert!(us.sql.contains("'bob'"));
        assert!(
            !us.sql.contains("'alice'"),
            "us must not receive alice's row"
        );
    }

    #[test]
    fn rows_for_one_region_stay_in_one_statement() {
        let parts = split_insert(
            "INSERT INTO users (tenant_id, name) VALUES ('bob', 'b'), ('carol', 'c')",
            "tenant_id",
            route,
        )
        .expect("should split");
        assert_eq!(parts.len(), 1);
        let only = parts.first().expect("one part");
        assert_eq!(only.region, "us-east");
        assert!(only.sql.contains("'bob'") && only.sql.contains("'carol'"));
    }

    #[test]
    fn the_rebuilt_statement_still_parses() {
        // Rebuilt from the parse tree, so it has to be valid SQL again.
        let parts = split_insert(
            "INSERT INTO users (tenant_id, name) VALUES ('alice', 'a'), ('bob', 'b')",
            "tenant_id",
            route,
        )
        .expect("should split");
        for part in parts {
            Parser::parse_sql(&PostgreSqlDialect {}, &part.sql)
                .unwrap_or_else(|error| panic!("{} did not parse: {error}", part.sql));
        }
    }

    #[test]
    fn a_comma_inside_a_literal_does_not_confuse_the_split() {
        // The reason this works on the parse tree and not on the text.
        let parts = split_insert(
            "INSERT INTO users (tenant_id, name) VALUES ('alice', 'Smith, Ada'), ('bob', 'b')",
            "tenant_id",
            route,
        )
        .expect("should split");
        let eu = parts.iter().find(|p| p.region == "eu-west").expect("eu");
        assert!(eu.sql.contains("Smith, Ada"));
        assert_eq!(parts.len(), 2);
    }

    #[test]
    fn a_key_that_is_not_the_first_column_is_found() {
        let parts = split_insert(
            "INSERT INTO users (name, tenant_id) VALUES ('a', 'alice'), ('b', 'bob')",
            "tenant_id",
            route,
        )
        .expect("should split");
        assert_eq!(parts.len(), 2);
    }

    #[test]
    fn a_negative_key_keeps_its_sign() {
        let parts = split_insert(
            "INSERT INTO t (k, v) VALUES (-1, 'a'), (1, 'b')",
            "k",
            |key| {
                if key.starts_with('-') {
                    "ap-south".to_owned()
                } else {
                    "eu-west".to_owned()
                }
            },
        )
        .expect("should split");
        assert_eq!(parts.len(), 2);
        assert!(parts.iter().any(|p| p.region == "ap-south"));
    }

    #[test]
    fn a_single_row_insert_is_not_split() {
        assert_eq!(
            split_insert(
                "INSERT INTO users (tenant_id, name) VALUES ('alice', 'a')",
                "tenant_id",
                route
            ),
            Err(SplitError::NotSplittable)
        );
    }

    #[test]
    fn an_insert_without_the_key_column_is_refused() {
        assert_eq!(
            split_insert(
                "INSERT INTO users (name) VALUES ('a'), ('b')",
                "tenant_id",
                route
            ),
            Err(SplitError::KeyNotPresent)
        );
    }

    #[test]
    fn a_computed_key_is_refused_rather_than_guessed() {
        assert_eq!(
            split_insert(
                "INSERT INTO users (tenant_id, name) VALUES (gen_tenant(), 'a'), ('bob', 'b')",
                "tenant_id",
                route
            ),
            Err(SplitError::KeyNotLiteral)
        );
    }

    #[test]
    fn an_insert_from_a_select_is_refused() {
        assert_eq!(
            split_insert(
                "INSERT INTO users (tenant_id) SELECT tenant_id FROM staging",
                "tenant_id",
                route
            ),
            Err(SplitError::NotSplittable)
        );
    }

    #[test]
    fn an_update_is_not_an_insert() {
        assert_eq!(
            split_insert("UPDATE users SET a = 1", "tenant_id", route),
            Err(SplitError::NotSplittable)
        );
    }

    #[test]
    fn every_refusal_can_explain_itself() {
        for error in [
            SplitError::NotSplittable,
            SplitError::KeyNotLiteral,
            SplitError::KeyNotPresent,
        ] {
            assert_ne!(error.reason(), "");
        }
    }
}
