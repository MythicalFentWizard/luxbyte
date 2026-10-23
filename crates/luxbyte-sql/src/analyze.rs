//! Walking the parse tree.

use std::collections::{BTreeMap, BTreeSet};

use sqlparser::ast::{
    BinaryOperator, CopySource, Delete, Expr, FromTable, GroupByExpr, Ident, Insert, ObjectName,
    OrderByKind, OrderBySort, Query, SetExpr, Statement, TableFactor, TableObject, TableWithJoins,
    UnaryOperator, Update, Value, Values, Visit, visit_expressions,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::key::{ColumnRef, KeyConstraints, KeyValue};
use crate::merge::{PostProcessing, SortKey, is_aggregate_name};
use crate::{Access, SqlError, StatementAnalysis, StatementKind, TableRef, TransactionOp};

pub(crate) fn analyze(sql: &str) -> Result<Vec<StatementAnalysis>, SqlError> {
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .map_err(|error| SqlError::Parse(error.to_string()))?;
    let highest_parameter = scan_highest_placeholder(sql);
    Ok(statements
        .iter()
        .map(|statement| analyze_statement(statement, highest_parameter))
        .collect())
}

/// Folds an identifier the way PostgreSQL does: unquoted names become lower
/// case, quoted ones are taken literally.
fn fold(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

/// Splits an object name into its schema and table parts.
fn table_from_name(name: &ObjectName, alias: Option<String>) -> TableRef {
    let parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|part| part.as_ident().map(fold))
        .collect();
    match parts.as_slice() {
        [schema, table] => TableRef {
            schema: Some(schema.clone()),
            name: table.clone(),
            alias,
        },
        [table] => TableRef {
            schema: None,
            name: table.clone(),
            alias,
        },
        // Three-part names (database.schema.table) keep the last two; a
        // cross-database reference is not something this proxy routes.
        parts => TableRef {
            schema: parts.get(parts.len().saturating_sub(2)).cloned(),
            name: parts.last().cloned().unwrap_or_default(),
            alias,
        },
    }
}

/// Normalises a literal to the text form the directory stores keys in.
///
/// `'eu-west'` and the bind parameter `eu-west` have to compare equal, so the
/// quotes come off here rather than at every lookup.
fn literal(value: &Value) -> Option<KeyValue> {
    let text = match value {
        Value::Number(number, _) => number.clone(),
        Value::SingleQuotedString(text)
        | Value::DoubleQuotedString(text)
        | Value::EscapedStringLiteral(text)
        | Value::NationalStringLiteral(text)
        | Value::UnicodeStringLiteral(text) => text.clone(),
        Value::Boolean(flag) => flag.to_string(),
        Value::Placeholder(name) => {
            // `$1` is a parameter; `?` and named placeholders are not
            // PostgreSQL and carry no position to report.
            let index: u16 = name.strip_prefix('$')?.parse().ok()?;
            return Some(KeyValue::Parameter(index));
        }
        // `NULL` is never equal to anything, including itself, so it cannot
        // pin a shard key.
        _ => return None,
    };
    Some(KeyValue::Literal(text))
}

/// Reads an expression as a column reference, if that is what it is.
fn column_of(expr: &Expr) -> Option<ColumnRef> {
    match expr {
        Expr::Identifier(ident) => Some(ColumnRef::bare(fold(ident))),
        // `table.column` and `schema.table.column` both qualify by the
        // table, which is the last part before the column.
        Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            [.., qualifier, column] => Some(ColumnRef::qualified(fold(qualifier), fold(column))),
            _ => None,
        },
        Expr::Nested(inner) => column_of(inner),
        _ => None,
    }
}

/// Reads an expression as a constant, if that is what it is.
fn value_of(expr: &Expr) -> Option<KeyValue> {
    match expr {
        Expr::Value(value) => literal(&value.value),
        Expr::Nested(inner) => value_of(inner),
        // `tenant_id = $1::uuid` is still `$1`; a cast does not change which
        // row the predicate selects.
        Expr::Cast { expr, .. } => value_of(expr),
        // A negative number reaches the parser as a unary minus applied to a
        // positive one. Treating that as "not a literal" would send every
        // `id = -1` on a fan-out; treating it as the positive value would be
        // very much worse.
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match value_of(expr)? {
            KeyValue::Literal(text) if is_number(&text) => {
                Some(KeyValue::Literal(format!("-{text}")))
            }
            // A negated placeholder has no value here to negate.
            _ => None,
        },
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => match value_of(expr)? {
            KeyValue::Literal(text) if is_number(&text) => Some(KeyValue::Literal(text)),
            _ => None,
        },
        _ => None,
    }
}

/// Whether a literal's text is a plain number, and so safe to negate.
fn is_number(text: &str) -> bool {
    !text.is_empty()
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'.')
}

/// Handles `column = value` written in either order.
fn equality(left: &Expr, right: &Expr) -> KeyConstraints {
    if let (Some(column), Some(value)) = (column_of(left), value_of(right)) {
        return KeyConstraints::pinned(column, value);
    }
    if let (Some(value), Some(column)) = (value_of(left), column_of(right)) {
        return KeyConstraints::pinned(column, value);
    }
    KeyConstraints::unconstrained()
}

/// Derives the key constraints a predicate implies.
fn constraints_of(expr: &Expr) -> KeyConstraints {
    match expr {
        Expr::Nested(inner) => constraints_of(inner),
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => constraints_of(left).and(constraints_of(right)),
            BinaryOperator::Or => constraints_of(left).or(constraints_of(right)),
            BinaryOperator::Eq => equality(left, right),
            // Ranges, inequalities and pattern matches do not pin a key.
            // `tenant_id > 5` is a scan however it is written.
            _ => KeyConstraints::unconstrained(),
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let Some(column) = column_of(expr) else {
                return KeyConstraints::unconstrained();
            };
            let mut values = BTreeSet::new();
            for item in list {
                // One unreadable element makes the whole list unusable: the
                // rows it selects are not known, so neither is the region.
                let Some(value) = value_of(item) else {
                    return KeyConstraints::unconstrained();
                };
                values.insert(value);
            }
            KeyConstraints::pinned_to_any_of(column, values)
        }
        _ => KeyConstraints::unconstrained(),
    }
}

/// Collects every table named in a `FROM` clause, including joins.
fn tables_of_from(from: &[TableWithJoins], out: &mut Vec<TableRef>) {
    for item in from {
        table_factor(&item.relation, out);
        for join in &item.joins {
            table_factor(&join.relation, out);
        }
    }
}

fn table_factor(factor: &TableFactor, out: &mut Vec<TableRef>) {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let alias = alias.as_ref().map(|alias| fold(&alias.name));
            out.push(table_from_name(name, alias));
        }
        TableFactor::Derived { subquery, .. } => tables_of_query(subquery, out),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => tables_of_from(std::slice::from_ref(table_with_joins), out),
        _ => {}
    }
}

/// The tables each branch of a set operation reads, one list per branch.
/// Empty for anything that is not a set operation.
///
/// A `UNION` reads its branches independently, so two tables in different
/// branches are not a join, and each region can answer its part.
fn branches_of(body: &SetExpr, out: &mut Vec<Vec<TableRef>>) {
    if let SetExpr::SetOperation { left, right, .. } = body {
        for side in [left, right] {
            if matches!(side.as_ref(), SetExpr::SetOperation { .. }) {
                branches_of(side, out);
            } else {
                let mut tables = Vec::new();
                tables_of_set_expr(side, &mut tables);
                out.push(tables);
            }
        }
    }
}

fn tables_of_query(query: &Query, out: &mut Vec<TableRef>) {
    tables_of_set_expr(&query.body, out);
}

fn tables_of_set_expr(body: &SetExpr, out: &mut Vec<TableRef>) {
    match body {
        SetExpr::Select(select) => tables_of_from(&select.from, out),
        SetExpr::Query(query) => tables_of_query(query, out),
        SetExpr::SetOperation { left, right, .. } => {
            tables_of_set_expr(left, out);
            tables_of_set_expr(right, out);
        }
        _ => {}
    }
}

/// The `WHERE` clause of a query, when it has exactly one.
///
/// A set operation has two, which may disagree, so the honest answer for one
/// is no constraint at all.
fn selection_of_query(query: &Query) -> Option<&Expr> {
    match query.body.as_ref() {
        SetExpr::Select(select) => select.selection.as_ref(),
        SetExpr::Query(inner) => selection_of_query(inner),
        _ => None,
    }
}

fn alias_map(tables: &[TableRef]) -> BTreeMap<String, String> {
    tables
        .iter()
        .filter_map(|table| {
            table
                .alias
                .as_ref()
                .map(|alias| (alias.clone(), table.name.clone()))
        })
        .collect()
}

/// Reads the key constraints an `INSERT ... VALUES` implies.
///
/// Each row pins the shard key to its own value, so a multi-row insert can
/// legitimately span regions — and the router needs to see every value to
/// know that.
fn insert_constraints(insert: &Insert, table: &TableRef) -> KeyConstraints {
    let columns: Vec<String> = insert
        .columns
        .iter()
        .filter_map(|name| name.0.first().and_then(|part| part.as_ident()).map(fold))
        .collect();
    if columns.is_empty() {
        // No column list means positional insert against the real table
        // shape, which the proxy does not know. Nothing can be concluded.
        return KeyConstraints::unconstrained();
    }

    let Some(source) = insert.source.as_ref() else {
        return KeyConstraints::unconstrained();
    };
    let SetExpr::Values(Values { rows, .. }) = source.body.as_ref() else {
        // `INSERT ... SELECT` draws its keys from a query the proxy would
        // have to run to know the answer.
        return KeyConstraints::unconstrained();
    };

    let mut per_column: BTreeMap<String, BTreeSet<KeyValue>> = BTreeMap::new();
    for row in rows {
        for (index, name) in columns.iter().enumerate() {
            let Some(expr) = row.get(index) else { continue };
            let Some(value) = value_of(expr) else {
                // An expression such as `DEFAULT` or `now()` makes this
                // column unusable as a key across the whole statement.
                per_column.remove(name);
                continue;
            };
            if let Some(values) = per_column.get_mut(name) {
                values.insert(value);
            } else {
                per_column.insert(name.clone(), BTreeSet::from([value]));
            }
        }
    }

    per_column
        .into_iter()
        .fold(KeyConstraints::unconstrained(), |acc, (column, values)| {
            acc.and(KeyConstraints::pinned_to_any_of(
                ColumnRef::qualified(table.name.clone(), column),
                values,
            ))
        })
}

/// Whether any aggregate function appears anywhere under `node`.
///
/// Uses the parser's own visitor rather than a hand-written walk, so a
/// function buried in a `CASE` arm or a cast is still found. Missing one
/// would mean merging partial aggregates as if they were whole rows, and
/// returning a number that is quietly wrong.
fn contains_aggregate<V: Visit>(node: &V) -> bool {
    visit_expressions(node, |expr| {
        if let Expr::Function(function) = expr
            && let Some(ident) = function.name.0.last().and_then(|part| part.as_ident())
            && is_aggregate_name(&ident.value)
        {
            return std::ops::ControlFlow::Break(());
        }
        std::ops::ControlFlow::<()>::Continue(())
    })
    .is_break()
}

/// Works out what a query does to its rows after collecting them.
fn post_processing_of(query: &Query) -> PostProcessing {
    let mut post = PostProcessing {
        limited: query.limit_clause.is_some() || query.fetch.is_some(),
        ..PostProcessing::default()
    };

    match query.order_by.as_ref().map(|order_by| &order_by.kind) {
        Some(OrderByKind::Expressions(terms)) => {
            post.order_by = terms
                .iter()
                .map(|term| SortKey {
                    expression: term.expr.to_string(),
                    descending: matches!(term.options.sort, Some(OrderBySort::Desc)),
                    nulls_first: term.options.nulls_first,
                })
                .collect();
        }
        // `ORDER BY ALL` is not PostgreSQL, and guessing its key list would
        // produce a merge order that is not the one the user asked for.
        Some(_) => post.aggregates = true,
        None => {}
    }

    post.set_operation = matches!(query.body.as_ref(), SetExpr::SetOperation { .. });
    if let SetExpr::Select(select) = query.body.as_ref() {
        post.distinct = select.distinct.is_some();
        let grouped = match &select.group_by {
            GroupByExpr::All(_) => true,
            GroupByExpr::Expressions(expressions, _) => !expressions.is_empty(),
        };
        // Only the projection is searched for aggregates: an aggregate in a
        // subquery under `WHERE` is computed per row, not over the result,
        // so it does not stop the outer rows being concatenated.
        let projected = select.projection.iter().any(contains_aggregate);
        post.aggregates = post.aggregates || grouped || select.having.is_some() || projected;
    }

    post
}

fn select_is_locking(query: &Query) -> bool {
    // `SELECT ... FOR UPDATE` takes row locks, so a replica will not do.
    !query.locks.is_empty()
}

#[allow(clippy::too_many_lines)]
fn analyze_statement(statement: &Statement, highest_parameter: u16) -> StatementAnalysis {
    // EXPLAIN and COPY wrap a statement or a table; what they touch is what
    // that does, and a router that saw no tables in them would send them
    // anywhere.
    match statement {
        Statement::Explain {
            statement: inner,
            analyze,
            options,
            ..
        } => {
            let mut analysis = analyze_statement(inner, highest_parameter);
            let runs = *analyze
                || options.as_ref().is_some_and(|options| {
                    options
                        .iter()
                        .any(|option| option.name.value.eq_ignore_ascii_case("analyze"))
                });
            if !runs {
                analysis.access = Access::ReadOnly;
            }
            analysis.kind = StatementKind::Explain;
            return analysis;
        }
        Statement::Copy { source, to, .. } => {
            let mut analysis = match source {
                CopySource::Query(query) => {
                    analyze_statement(&Statement::Query(query.clone()), highest_parameter)
                }
                CopySource::Table { table_name, .. } => StatementAnalysis {
                    kind: StatementKind::Other,
                    access: Access::ReadOnly,
                    tables: vec![table_from_name(table_name, None)],
                    constraints: KeyConstraints::unconstrained(),
                    assigned_columns: Vec::new(),
                    highest_parameter,
                    post_processing: PostProcessing::default(),
                    branches: Vec::new(),
                },
            };
            (analysis.kind, analysis.access) = if *to {
                (StatementKind::CopyOut, Access::ReadOnly)
            } else {
                (StatementKind::CopyIn, Access::ReadWrite)
            };
            return analysis;
        }
        _ => {}
    }

    let mut tables = Vec::new();
    let mut constraints = KeyConstraints::unconstrained();
    let mut assigned_columns = Vec::new();
    let mut post_processing = PostProcessing::default();
    let mut branches = Vec::new();

    let (kind, access) = match statement {
        Statement::Query(query) => {
            tables_of_query(query, &mut tables);
            branches_of(&query.body, &mut branches);
            if let Some(selection) = selection_of_query(query) {
                constraints = constraints_of(selection);
            }
            post_processing = post_processing_of(query);
            let access = if select_is_locking(query) {
                Access::ReadWrite
            } else {
                Access::ReadOnly
            };
            (StatementKind::Select, access)
        }
        Statement::Insert(insert) => {
            if let TableObject::TableName(name) = &insert.table {
                let table = table_from_name(name, None);
                constraints = insert_constraints(insert, &table);
                tables.push(table);
            }
            (StatementKind::Insert, Access::ReadWrite)
        }
        Statement::Update(Update {
            table,
            assignments,
            from,
            selection,
            ..
        }) => {
            table_factor(&table.relation, &mut tables);
            if let Some(from) = from {
                match from {
                    sqlparser::ast::UpdateTableFromKind::BeforeSet(items)
                    | sqlparser::ast::UpdateTableFromKind::AfterSet(items) => {
                        tables_of_from(items, &mut tables);
                    }
                }
            }
            for assignment in assignments {
                if let sqlparser::ast::AssignmentTarget::ColumnName(name) = &assignment.target {
                    if let Some(ident) = name.0.last().and_then(|part| part.as_ident()) {
                        assigned_columns.push(fold(ident));
                    }
                }
            }
            if let Some(selection) = selection {
                constraints = constraints_of(selection);
            }
            (StatementKind::Update, Access::ReadWrite)
        }
        Statement::Delete(Delete {
            from, selection, ..
        }) => {
            match from {
                FromTable::WithFromKeyword(items) | FromTable::WithoutKeyword(items) => {
                    tables_of_from(items, &mut tables);
                }
            }
            if let Some(selection) = selection {
                constraints = constraints_of(selection);
            }
            (StatementKind::Delete, Access::ReadWrite)
        }
        Statement::StartTransaction { .. } => (
            StatementKind::TransactionControl(TransactionOp::Begin),
            Access::ReadOnly,
        ),
        Statement::Commit { .. } => (
            StatementKind::TransactionControl(TransactionOp::Commit),
            Access::ReadWrite,
        ),
        Statement::Rollback { savepoint, .. } => {
            let op = if savepoint.is_some() {
                TransactionOp::RollbackToSavepoint
            } else {
                TransactionOp::Rollback
            };
            (StatementKind::TransactionControl(op), Access::ReadWrite)
        }
        Statement::Savepoint { .. } => (
            StatementKind::TransactionControl(TransactionOp::Savepoint),
            Access::ReadWrite,
        ),
        Statement::ReleaseSavepoint { .. } => (
            StatementKind::TransactionControl(TransactionOp::ReleaseSavepoint),
            Access::ReadWrite,
        ),
        Statement::Set(_) | Statement::Discard { .. } => {
            (StatementKind::SessionState, Access::ReadOnly)
        }
        Statement::ShowVariable { .. }
        | Statement::ShowVariables { .. }
        | Statement::ShowStatus { .. } => (StatementKind::Show, Access::ReadOnly),
        Statement::CreateTable(_)
        | Statement::CreateIndex(_)
        | Statement::CreateSchema { .. }
        | Statement::CreateView { .. }
        | Statement::AlterTable { .. }
        | Statement::Drop { .. }
        | Statement::Truncate { .. } => (StatementKind::Ddl, Access::ReadWrite),
        _ => (StatementKind::Other, Access::ReadWrite),
    };

    let aliases = alias_map(&tables);
    StatementAnalysis {
        kind,
        access,
        constraints: constraints.resolve_aliases(&aliases),
        tables,
        assigned_columns,
        highest_parameter,
        post_processing,
        branches,
    }
}

/// Finds the highest `$n` placeholder in raw SQL text.
///
/// Done over the text rather than the parse tree because a placeholder can
/// appear anywhere — projection, `LIMIT`, `RETURNING` — and the count has to
/// be right regardless. String literals, dollar-quoted bodies, identifiers
/// and comments are skipped, which is exactly where a naive scan goes wrong:
/// `SELECT '$9'` has no parameters at all.
pub(crate) fn scan_highest_placeholder(sql: &str) -> u16 {
    let bytes = sql.as_bytes();
    let mut highest = 0_u16;
    let mut index = 0_usize;

    let at = |position: usize| bytes.get(position).copied();
    let advance = |position: usize| position.saturating_add(1);

    while index < bytes.len() {
        match at(index) {
            Some(quote @ (b'\'' | b'"')) => {
                index = advance(index);
                while index < bytes.len() && at(index) != Some(quote) {
                    index = advance(index);
                }
                index = advance(index);
            }
            Some(b'-') if at(advance(index)) == Some(b'-') => {
                while index < bytes.len() && at(index) != Some(b'\n') {
                    index = advance(index);
                }
            }
            Some(b'/') if at(advance(index)) == Some(b'*') => {
                index = advance(advance(index));
                while index < bytes.len()
                    && !(at(index) == Some(b'*') && at(advance(index)) == Some(b'/'))
                {
                    index = advance(index);
                }
                index = advance(advance(index));
            }
            Some(b'$') => {
                let mut cursor = advance(index);
                let start = cursor;
                while at(cursor).is_some_and(|byte| byte.is_ascii_digit()) {
                    cursor = advance(cursor);
                }
                if cursor > start {
                    if let Some(number) = sql
                        .get(start..cursor)
                        .and_then(|text| text.parse::<u16>().ok())
                    {
                        highest = highest.max(number);
                    }
                    index = cursor;
                } else {
                    // A dollar-quoted string: `$tag$ ... $tag$`. Skip to the
                    // matching closing tag so its body is never scanned.
                    let mut tag_end = cursor;
                    while at(tag_end).is_some_and(|byte| byte != b'$') {
                        tag_end = advance(tag_end);
                    }
                    match (sql.get(index..advance(tag_end)), at(tag_end)) {
                        (Some(tag), Some(b'$')) => {
                            let body_start = advance(tag_end);
                            let closing = sql.get(body_start..).and_then(|rest| rest.find(tag));
                            index = closing.map_or(bytes.len(), |offset| {
                                body_start.saturating_add(offset).saturating_add(tag.len())
                            });
                        }
                        _ => index = advance(index),
                    }
                }
            }
            _ => index = advance(index),
        }
    }
    highest
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(sql: &str) -> StatementAnalysis {
        let mut analyses = analyze(sql).expect("statement should parse");
        assert_eq!(analyses.len(), 1, "expected exactly one statement");
        analyses.remove(0)
    }

    fn values(sql: &str, table: &str, column: &str) -> Vec<String> {
        let analysis = one(sql);
        analysis
            .shard_key_values(table, column)
            .map(|set| set.iter().map(ToString::to_string).collect())
            .unwrap_or_default()
    }

    #[test]
    fn finds_a_key_in_a_simple_equality() {
        assert_eq!(
            values(
                "SELECT * FROM users WHERE tenant_id = 42",
                "users",
                "tenant_id"
            ),
            ["42"]
        );
    }

    #[test]
    fn finds_a_key_written_the_other_way_round() {
        assert_eq!(
            values(
                "SELECT * FROM users WHERE 42 = tenant_id",
                "users",
                "tenant_id"
            ),
            ["42"]
        );
    }

    #[test]
    fn finds_a_key_given_as_a_placeholder() {
        let analysis = one("SELECT * FROM users WHERE tenant_id = $1");
        let found = analysis.shard_key_values("users", "tenant_id");
        assert_eq!(
            found.map(|set| set.iter().cloned().collect::<Vec<_>>()),
            Some(vec![KeyValue::Parameter(1)])
        );
    }

    #[test]
    fn strips_quotes_so_literals_compare_with_bind_values() {
        assert_eq!(
            values(
                "SELECT * FROM users WHERE region = 'eu-west'",
                "users",
                "region"
            ),
            ["eu-west"]
        );
    }

    #[test]
    fn sees_through_a_cast() {
        let analysis = one("SELECT * FROM users WHERE tenant_id = $1::uuid");
        assert!(analysis.shard_key_values("users", "tenant_id").is_some());
    }

    #[test]
    fn collects_every_value_of_an_in_list() {
        let mut found = values(
            "SELECT * FROM users WHERE tenant_id IN (1, 2, 3)",
            "users",
            "tenant_id",
        );
        found.sort();
        assert_eq!(found, ["1", "2", "3"]);
    }

    #[test]
    fn a_not_in_list_pins_nothing() {
        assert_eq!(
            values(
                "SELECT * FROM users WHERE tenant_id NOT IN (1, 2)",
                "users",
                "tenant_id"
            ),
            Vec::<String>::new()
        );
    }

    #[test]
    fn an_in_subquery_pins_nothing() {
        assert_eq!(
            values(
                "SELECT * FROM users WHERE tenant_id IN (SELECT id FROM t)",
                "users",
                "tenant_id"
            ),
            Vec::<String>::new()
        );
    }

    #[test]
    fn an_or_with_one_free_branch_pins_nothing() {
        assert_eq!(
            values(
                "SELECT * FROM users WHERE tenant_id = 1 OR created_at > now()",
                "users",
                "tenant_id"
            ),
            Vec::<String>::new()
        );
    }

    #[test]
    fn an_or_over_the_same_column_keeps_both_values() {
        let mut found = values(
            "SELECT * FROM users WHERE tenant_id = 1 OR tenant_id = 2",
            "users",
            "tenant_id",
        );
        found.sort();
        assert_eq!(found, ["1", "2"]);
    }

    #[test]
    fn a_range_predicate_pins_nothing() {
        assert_eq!(
            values(
                "SELECT * FROM users WHERE tenant_id > 5",
                "users",
                "tenant_id"
            ),
            Vec::<String>::new()
        );
    }

    #[test]
    fn resolves_an_alias_to_its_table() {
        assert_eq!(
            values(
                "SELECT * FROM users u WHERE u.tenant_id = 9",
                "users",
                "tenant_id"
            ),
            ["9"]
        );
    }

    #[test]
    fn an_unqualified_key_is_not_trusted_across_a_join() {
        // Two tables, so `tenant_id` could belong to either. PostgreSQL
        // would call this ambiguous; so does the router.
        let analysis =
            one("SELECT * FROM users u JOIN orders o ON o.user_id = u.id WHERE tenant_id = 1");
        assert_eq!(analysis.tables.len(), 2);
        assert!(analysis.shard_key_values("users", "tenant_id").is_none());
    }

    #[test]
    fn a_qualified_key_is_trusted_across_a_join() {
        assert_eq!(
            values(
                "SELECT * FROM users u JOIN orders o ON o.user_id = u.id WHERE u.tenant_id = 1",
                "users",
                "tenant_id"
            ),
            ["1"]
        );
    }

    #[test]
    fn an_insert_pins_the_key_from_its_values() {
        assert_eq!(
            values(
                "INSERT INTO users (tenant_id, name) VALUES (7, 'ada')",
                "users",
                "tenant_id"
            ),
            ["7"]
        );
    }

    #[test]
    fn a_multi_row_insert_reports_every_key_it_touches() {
        let mut found = values(
            "INSERT INTO users (tenant_id, name) VALUES (1, 'ada'), (2, 'bob')",
            "users",
            "tenant_id",
        );
        found.sort();
        assert_eq!(found, ["1", "2"]);
    }

    #[test]
    fn an_insert_select_pins_nothing() {
        assert_eq!(
            values(
                "INSERT INTO users (tenant_id) SELECT tenant_id FROM staging",
                "users",
                "tenant_id"
            ),
            Vec::<String>::new()
        );
    }

    #[test]
    fn an_insert_without_a_column_list_pins_nothing() {
        assert_eq!(
            values("INSERT INTO users VALUES (1, 'ada')", "users", "tenant_id"),
            Vec::<String>::new()
        );
    }

    #[test]
    fn an_update_reports_the_columns_it_assigns() {
        let analysis = one("UPDATE users SET name = 'ada', tenant_id = 2 WHERE tenant_id = 1");
        assert!(analysis.assigns_to("tenant_id"));
        assert!(analysis.assigns_to("name"));
        assert!(!analysis.assigns_to("email"));
    }

    #[test]
    fn a_delete_takes_its_key_from_the_where_clause() {
        assert_eq!(
            values(
                "DELETE FROM users WHERE tenant_id = 3",
                "users",
                "tenant_id"
            ),
            ["3"]
        );
    }

    #[test]
    fn reads_and_writes_are_told_apart() {
        assert_eq!(one("SELECT 1").access, Access::ReadOnly);
        assert_eq!(one("UPDATE t SET a = 1").access, Access::ReadWrite);
        assert_eq!(one("DELETE FROM t").access, Access::ReadWrite);
        assert_eq!(
            one("INSERT INTO t (a) VALUES (1)").access,
            Access::ReadWrite
        );
    }

    #[test]
    fn a_locking_select_is_treated_as_a_write() {
        // It takes row locks, so a replica cannot serve it.
        assert_eq!(
            one("SELECT * FROM users WHERE id = 1 FOR UPDATE").access,
            Access::ReadWrite
        );
    }

    #[test]
    fn transaction_control_is_recognised() {
        assert_eq!(
            one("BEGIN").kind,
            StatementKind::TransactionControl(TransactionOp::Begin)
        );
        assert_eq!(
            one("COMMIT").kind,
            StatementKind::TransactionControl(TransactionOp::Commit)
        );
        assert_eq!(
            one("ROLLBACK").kind,
            StatementKind::TransactionControl(TransactionOp::Rollback)
        );
        assert_eq!(
            one("SAVEPOINT a").kind,
            StatementKind::TransactionControl(TransactionOp::Savepoint)
        );
        assert_eq!(
            one("ROLLBACK TO SAVEPOINT a").kind,
            StatementKind::TransactionControl(TransactionOp::RollbackToSavepoint)
        );
    }

    #[test]
    fn session_state_is_recognised_so_it_can_be_replayed() {
        assert_eq!(
            one("SET search_path = public").kind,
            StatementKind::SessionState
        );
        assert_eq!(one("DISCARD ALL").kind, StatementKind::SessionState);
    }

    #[test]
    fn a_batch_is_analysed_statement_by_statement() {
        let analyses = analyze("SELECT 1; UPDATE t SET a = 1").expect("should parse");
        assert_eq!(analyses.len(), 2);
        assert_eq!(analyses.first().map(|a| a.access), Some(Access::ReadOnly));
        assert_eq!(analyses.get(1).map(|a| a.access), Some(Access::ReadWrite));
    }

    #[test]
    fn unparsable_sql_is_an_error_not_a_guess() {
        assert!(analyze("SELEKT * FRM").is_err());
    }

    #[test]
    fn a_plain_select_can_be_concatenated_across_regions() {
        let analysis = one("SELECT id FROM users WHERE tenant_id = 1");
        assert!(analysis.post_processing.is_trivial());
        assert_eq!(
            analysis.post_processing.merge_strategy(),
            crate::MergeStrategy::Concatenate
        );
    }

    #[test]
    fn an_aggregate_in_the_projection_needs_combining() {
        let analysis = one("SELECT count(*) FROM users");
        assert!(analysis.post_processing.aggregates);
        assert_eq!(
            analysis.post_processing.merge_strategy(),
            crate::MergeStrategy::Combine
        );
    }

    #[test]
    fn an_aggregate_hidden_inside_an_expression_is_still_found() {
        // A hand-written walk that only looked at the top level would miss
        // both of these and return a wrong number.
        assert!(
            one("SELECT sum(amount) / 2 FROM orders")
                .post_processing
                .aggregates
        );
        assert!(
            one("SELECT CASE WHEN true THEN max(id) ELSE 0 END FROM orders")
                .post_processing
                .aggregates
        );
    }

    #[test]
    fn group_by_and_having_are_aggregation() {
        assert!(
            one("SELECT region FROM users GROUP BY region")
                .post_processing
                .aggregates
        );
        assert!(
            one("SELECT region FROM users GROUP BY region HAVING count(*) > 1")
                .post_processing
                .aggregates
        );
    }

    #[test]
    fn distinct_and_set_operations_need_combining() {
        for sql in [
            "SELECT DISTINCT region FROM users",
            "SELECT id FROM users UNION SELECT id FROM orders",
        ] {
            assert_eq!(
                one(sql).post_processing.merge_strategy(),
                crate::MergeStrategy::Combine,
                "{sql}"
            );
        }
    }

    #[test]
    fn an_aggregate_in_a_where_subquery_does_not_block_the_outer_merge() {
        // The aggregate is computed inside the subquery, per row; the outer
        // rows are still a plain union across regions.
        let analysis = one("SELECT id FROM users WHERE id = (SELECT max(id) FROM seeds)");
        assert!(!analysis.post_processing.aggregates);
    }

    #[test]
    fn order_by_is_recorded_with_direction_preserved() {
        let analysis = one("SELECT id FROM users ORDER BY created_at DESC, id");
        assert_eq!(
            analysis.post_processing.merge_strategy(),
            crate::MergeStrategy::Combine
        );
        let keys = &analysis.post_processing.order_by;
        assert_eq!(keys.len(), 2);
        assert_eq!(
            keys.first().map(|k| k.expression.as_str()),
            Some("created_at")
        );
        assert_eq!(keys.first().map(|k| k.descending), Some(true));
        assert_eq!(keys.get(1).map(|k| k.descending), Some(false));
    }

    #[test]
    fn each_branch_of_a_union_has_its_own_tables() {
        let analysis =
            one("SELECT id FROM users UNION SELECT u.id FROM users u JOIN orders o ON true");
        let counts: Vec<usize> = analysis.branches.iter().map(Vec::len).collect();
        assert_eq!(counts, [1, 2]);
        assert!(one("SELECT id FROM users").branches.is_empty());
    }

    #[test]
    fn a_limit_has_to_be_applied_again_after_merging() {
        // Each region returns up to ten rows; the answer is ten in all.
        let analysis = one("SELECT id FROM users LIMIT 10");
        assert!(analysis.post_processing.limited);
        assert_eq!(
            analysis.post_processing.merge_strategy(),
            crate::MergeStrategy::Combine
        );
    }

    #[test]
    fn counts_the_highest_placeholder() {
        assert_eq!(scan_highest_placeholder("SELECT $1, $3, $2"), 3);
        assert_eq!(scan_highest_placeholder("SELECT 1"), 0);
    }

    #[test]
    fn placeholders_inside_strings_and_comments_are_not_counted() {
        assert_eq!(scan_highest_placeholder("SELECT '$9'"), 0);
        assert_eq!(scan_highest_placeholder("SELECT 1 -- $9"), 0);
        assert_eq!(scan_highest_placeholder("SELECT 1 /* $9 */"), 0);
        assert_eq!(scan_highest_placeholder("SELECT \"$9\""), 0);
        assert_eq!(scan_highest_placeholder("SELECT $tag$ $9 $tag$"), 0);
        assert_eq!(scan_highest_placeholder("SELECT $tag$ $9 $tag$, $2"), 2);
    }

    #[test]
    fn a_dollar_quoted_body_that_is_never_closed_does_not_loop() {
        assert_eq!(scan_highest_placeholder("SELECT $tag$ unterminated"), 0);
    }
}
