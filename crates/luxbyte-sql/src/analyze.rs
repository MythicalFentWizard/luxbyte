//! Walking the parse tree.

use std::collections::{BTreeMap, BTreeSet};

use sqlparser::ast::{
    BinaryOperator, CopySource, Expr, GroupByExpr, Ident, Insert, ObjectName, OrderByKind,
    OrderBySort, Query, SetExpr, Statement, UnaryOperator, Update, Value, Values, Visit,
    visit_expressions,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::key::{ColumnRef, KeyConstraints, KeyValue};
use crate::merge::{PostProcessing, SortKey, is_aggregate_name};
use crate::scope::Walker;
use crate::{Access, SqlError, StatementAnalysis, StatementKind, TableRef, TransactionOp};

pub(crate) fn analyze(sql: &str) -> Result<Vec<StatementAnalysis>, SqlError> {
    let ours = analyze_with_sqlparser(sql);
    // PostgreSQL's own parser checks what sqlparser read, and reads what it
    // could not: see `crate::pg`.
    #[cfg(feature = "pg-parser")]
    {
        let checked = match &ours {
            Ok(analyses) => crate::pg::agrees(sql, analyses),
            Err(_) => false,
        };
        if !checked {
            if let Ok(theirs) = crate::pg::analyze(sql) {
                return Ok(theirs);
            }
        }
    }
    ours
}

/// Analyses a statement with sqlparser alone.
pub(crate) fn analyze_with_sqlparser(sql: &str) -> Result<Vec<StatementAnalysis>, SqlError> {
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
pub(crate) fn fold(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

/// Splits an object name into its schema and table parts.
pub(crate) fn table_from_name(name: &ObjectName, alias: Option<String>) -> TableRef {
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
        Value::DollarQuotedString(quoted) => quoted.value.clone(),
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
pub(crate) fn column_of(expr: &Expr) -> Option<ColumnRef> {
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
pub(crate) fn constraints_of(expr: &Expr) -> KeyConstraints {
    match expr {
        Expr::Nested(inner) => constraints_of(inner),
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => constraints_of(left).and(constraints_of(right)),
            BinaryOperator::Or => constraints_of(left).or(constraints_of(right)),
            BinaryOperator::Eq => match (left.as_ref(), right.as_ref()) {
                // `(a, b) = (1, 2)` is `a = 1 AND b = 2`.
                (Expr::Tuple(lefts), Expr::Tuple(rights)) if lefts.len() == rights.len() => lefts
                    .iter()
                    .zip(rights)
                    .fold(KeyConstraints::unconstrained(), |all, (left, right)| {
                        all.and(equality(left, right))
                    }),
                _ => equality(left, right),
            },
            // Ranges, inequalities and pattern matches do not pin a key.
            // `tenant_id > 5` is a scan however it is written.
            _ => KeyConstraints::unconstrained(),
        },
        // `tenant_id = ANY (ARRAY['a', 'b'])` is an IN list.
        Expr::AnyOp {
            left,
            compare_op: BinaryOperator::Eq,
            right,
            ..
        } => match (column_of(left), right.as_ref()) {
            (Some(column), Expr::Array(array)) => {
                let mut values = BTreeSet::new();
                for item in &array.elem {
                    let Some(value) = value_of(item) else {
                        return KeyConstraints::unconstrained();
                    };
                    values.insert(value);
                }
                KeyConstraints::pinned_to_any_of(column, values)
            }
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

/// Reads the key constraints an `INSERT ... VALUES` implies.
///
/// Each row pins the shard key to its own value, so a multi-row insert can
/// legitimately span regions — and the router needs to see every value to
/// know that.
pub(crate) fn insert_constraints(insert: &Insert, table: &TableRef) -> KeyConstraints {
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
                    links: Vec::new(),
                    anchors: vec![0],
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

    let mut walker = Walker::over(statement);
    let mut assigned_columns = Vec::new();
    let mut post_processing = PostProcessing::default();
    let mut branches = Vec::new();
    let reads_rows = matches!(
        statement,
        Statement::Query(_)
            | Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_)
            | Statement::Merge(_)
    );

    let (kind, access) = match statement {
        Statement::Query(query) => match query.body.as_ref() {
            // `WITH ... INSERT/UPDATE/DELETE`: the statement it ends in is
            // what it does, and its table comes first.
            SetExpr::Insert(inner) | SetExpr::Update(inner) | SetExpr::Delete(inner) => {
                walker.name_ctes(query);
                walker.statement(inner);
                walker.walk_ctes(query);
                if let Statement::Update(update) = inner {
                    assigned_columns = assigned(update);
                }
                (data_kind(inner), Access::ReadWrite)
            }
            body => {
                post_processing = post_processing_of(query);
                if matches!(body, SetExpr::SetOperation { .. }) {
                    branches = walk_branches(&mut walker, query);
                } else {
                    walker.result_query(query);
                }
                let access = if select_is_locking(query) || walker.writes {
                    Access::ReadWrite
                } else {
                    Access::ReadOnly
                };
                (StatementKind::Select, access)
            }
        },
        Statement::Insert(_) | Statement::Delete(_) => {
            walker.statement(statement);
            (data_kind(statement), Access::ReadWrite)
        }
        Statement::Update(update) => {
            walker.statement(statement);
            assigned_columns = assigned(update);
            (StatementKind::Update, Access::ReadWrite)
        }
        // A MERGE that may insert is an INSERT whose key it does not name:
        // run in several regions it would insert the row in each one that
        // has no match. One that only updates or deletes is an UPDATE.
        Statement::Merge(merge) => {
            walker.statement(statement);
            let inserts = merge
                .clauses
                .iter()
                .any(|clause| matches!(clause.action, sqlparser::ast::MergeAction::Insert(_)));
            for clause in &merge.clauses {
                if let sqlparser::ast::MergeAction::Update(update) = &clause.action {
                    if let sqlparser::ast::MergeUpdateKind::Set(assignments) = &update.kind {
                        assigned_columns.extend(assignments.iter().filter_map(|assignment| {
                            match &assignment.target {
                                sqlparser::ast::AssignmentTarget::ColumnName(name) => {
                                    name.0.last().and_then(|part| part.as_ident()).map(fold)
                                }
                                sqlparser::ast::AssignmentTarget::Tuple(_) => None,
                            }
                        }));
                    }
                }
            }
            let kind = if inserts {
                StatementKind::Insert
            } else {
                StatementKind::Update
            };
            (kind, Access::ReadWrite)
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

    // Whatever the walk missed is still read.
    if reads_rows {
        walker.sweep();
    }
    let (tables, constraints, links, anchors) = walker.finish();
    StatementAnalysis {
        kind,
        access,
        tables,
        constraints,
        assigned_columns,
        highest_parameter,
        post_processing,
        links,
        anchors,
        branches,
    }
}

/// What kind a data-modifying statement is.
fn data_kind(statement: &Statement) -> StatementKind {
    match statement {
        Statement::Insert(_) => StatementKind::Insert,
        Statement::Update(_) => StatementKind::Update,
        Statement::Delete(_) => StatementKind::Delete,
        _ => StatementKind::Other,
    }
}

/// The columns an `UPDATE` assigns to.
fn assigned(update: &Update) -> Vec<String> {
    update
        .assignments
        .iter()
        .filter_map(|assignment| match &assignment.target {
            sqlparser::ast::AssignmentTarget::ColumnName(name) => {
                name.0.last().and_then(|part| part.as_ident()).map(fold)
            }
            sqlparser::ast::AssignmentTarget::Tuple(_) => None,
        })
        .collect()
}

/// Walks a set operation branch by branch, and says which tables each
/// branch reads: its own, and those of the `WITH` queries it may use.
fn walk_branches(walker: &mut Walker, query: &Query) -> Vec<Vec<TableRef>> {
    fn leaves<'a>(body: &'a SetExpr, out: &mut Vec<&'a SetExpr>) {
        match body {
            SetExpr::SetOperation { left, right, .. } => {
                leaves(left, out);
                leaves(right, out);
            }
            other => out.push(other),
        }
    }
    walker.name_ctes(query);
    walker.walk_ctes(query);
    let shared = walker.tables().len();
    let mut found = Vec::new();
    leaves(&query.body, &mut found);
    let mut branches = Vec::with_capacity(found.len());
    for leaf in found {
        let start = walker.tables().len();
        walker.set_expr(leaf);
        let tables = walker.tables();
        let mut branch: Vec<TableRef> = tables.get(..shared).unwrap_or_default().to_vec();
        branch.extend(tables.get(start..).unwrap_or_default().iter().cloned());
        branches.push(branch);
    }
    walker.ordering(query);
    branches
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

    // ------------------------------------------------- every block read

    fn names(sql: &str) -> Vec<String> {
        one(sql).tables.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn tables_hidden_in_subqueries_and_with_queries_are_found() {
        assert_eq!(
            names("WITH u AS (SELECT * FROM users) SELECT * FROM u"),
            ["users"],
            "a WITH query's name is not a table; what it reads is"
        );
        for sql in [
            "SELECT * FROM currencies WHERE code IN (SELECT currency FROM orders)",
            "SELECT *, (SELECT count(*) FROM orders o WHERE o.code = c.code) FROM currencies c",
            "SELECT * FROM currencies WHERE EXISTS (SELECT 1 FROM orders)",
            "UPDATE currencies SET rate = (SELECT max(rate) FROM orders)",
            "INSERT INTO currencies SELECT * FROM orders",
            "DELETE FROM currencies USING orders WHERE orders.code = currencies.code",
            "SELECT * FROM currencies c JOIN LATERAL (SELECT * FROM orders) o ON true",
        ] {
            assert_eq!(names(sql), ["currencies", "orders"], "{sql}");
        }
        // Set-returning functions are not tables.
        assert_eq!(
            names("SELECT * FROM generate_series(1, 3)"),
            Vec::<String>::new()
        );
    }

    #[test]
    fn a_subquery_pins_its_own_tables_and_never_the_outer_ones() {
        let analysis = one("SELECT * FROM users WHERE NOT EXISTS \
             (SELECT 1 FROM orders WHERE tenant_id = 'b' AND users.tenant_id = 'a')");
        assert_eq!(
            analysis
                .shard_key_values("orders", "tenant_id")
                .map(|set| set.iter().map(ToString::to_string).collect::<Vec<_>>()),
            Some(vec!["b".to_owned()])
        );
        // Under NOT EXISTS the subquery's predicate on users says nothing
        // about which users rows are returned.
        assert!(analysis.shard_key_values("users", "tenant_id").is_none());
    }

    #[test]
    fn a_key_flows_into_a_correlated_subquery_and_along_inner_joins() {
        for sql in [
            "SELECT * FROM users u WHERE u.tenant_id = 'a' AND EXISTS \
             (SELECT 1 FROM orders o WHERE o.tenant_id = u.tenant_id)",
            "SELECT * FROM users u JOIN orders o ON o.tenant_id = u.tenant_id \
             WHERE u.tenant_id = 'a'",
            "SELECT * FROM users u, orders o WHERE o.tenant_id = u.tenant_id \
             AND u.tenant_id = 'a'",
            "SELECT * FROM users u LEFT JOIN orders o ON o.tenant_id = u.tenant_id \
             WHERE u.tenant_id = 'a'",
            "SELECT * FROM users u JOIN orders o USING (tenant_id) WHERE u.tenant_id = 'a'",
        ] {
            assert_eq!(values(sql, "orders", "tenant_id"), ["a"], "{sql}");
            assert_eq!(values(sql, "users", "tenant_id"), ["a"], "{sql}");
        }
    }

    #[test]
    fn an_outer_join_restricts_only_the_side_it_may_leave_out() {
        // Every users row comes back whatever the ON clause asks of orders.
        let sql = "SELECT * FROM users u LEFT JOIN orders o \
                   ON o.tenant_id = u.tenant_id AND o.tenant_id = 'b'";
        assert_eq!(values(sql, "orders", "tenant_id"), ["b"]);
        assert!(one(sql).shard_key_values("users", "tenant_id").is_none());
        assert!(
            one(
                "SELECT * FROM users u FULL JOIN orders o ON o.tenant_id = u.tenant_id \
                 WHERE true AND o.tenant_id IS NULL"
            )
            .shard_key_values("orders", "tenant_id")
            .is_none()
        );
    }

    #[test]
    fn a_table_read_twice_is_restricted_only_as_far_as_both_reads_are() {
        assert_eq!(
            values(
                "SELECT * FROM users WHERE tenant_id = 'a' \
                 UNION ALL SELECT * FROM users WHERE tenant_id = 'b'",
                "users",
                "tenant_id"
            ),
            ["a", "b"]
        );
        assert!(
            one("SELECT * FROM users WHERE tenant_id = 'a' AND manager IN \
                 (SELECT id FROM users)")
            .shard_key_values("users", "tenant_id")
            .is_none()
        );
    }

    #[test]
    fn a_statement_that_writes_through_with_is_a_write() {
        let analysis = one(
            "WITH moved AS (UPDATE orders SET total = 0 WHERE tenant_id = 'a' RETURNING *) \
             SELECT * FROM moved",
        );
        assert_eq!(analysis.access, Access::ReadWrite);
        let analysis = one("WITH gone AS (SELECT id FROM users WHERE tenant_id = 'a') \
             UPDATE orders SET tenant_id = 'b' WHERE tenant_id = 'a'");
        assert_eq!(analysis.kind, StatementKind::Update);
        assert_eq!(
            analysis.tables.first().map(ToString::to_string).as_deref(),
            Some("orders"),
            "the written table first"
        );
        assert!(analysis.assigns_to("tenant_id"));
        assert_eq!(
            one("SELECT * INTO copy FROM users").access,
            Access::ReadWrite
        );
    }

    #[test]
    fn links_and_anchors_say_how_rows_meet_and_where_results_come_from() {
        let analysis = one("SELECT * FROM users u WHERE EXISTS \
             (SELECT 1 FROM orders o WHERE o.tenant_id = u.tenant_id)");
        assert_eq!(
            analysis.links,
            [crate::Link {
                left: 1,
                left_column: "tenant_id".to_owned(),
                right: 0,
                right_column: "tenant_id".to_owned(),
            }]
        );
        assert_eq!(analysis.anchors, [0], "only users rows are returned");
        // The side an outer join may leave out anchors nothing.
        assert_eq!(
            one("SELECT * FROM currencies c LEFT JOIN orders o ON o.code = c.code").anchors,
            [0]
        );
        assert_eq!(
            one("SELECT * FROM (SELECT * FROM orders) s, currencies c").anchors,
            [0, 1]
        );
        assert_eq!(one("UPDATE orders SET x = 1 FROM users").anchors, [0]);
    }

    #[test]
    fn an_insert_select_writes_the_key_it_reads() {
        let sql = "INSERT INTO orders (tenant_id, id, total) \
                   SELECT tenant_id, id + 1000, total FROM orders WHERE tenant_id = 'a'";
        let analysis = one(sql);
        assert_eq!(names(sql), ["orders", "orders"]);
        assert_eq!(values(sql, "orders", "tenant_id"), ["a"]);
        assert!(analysis.links.iter().any(|link| link.left == 0
            && link.left_column == "tenant_id"
            && link.right == 1
            && link.right_column == "tenant_id"));
        // Without a column list nothing says which column is which.
        assert!(
            one("INSERT INTO orders SELECT * FROM orders")
                .links
                .is_empty()
        );
    }

    #[test]
    fn a_merge_that_only_updates_is_joined_on_its_condition() {
        let analysis = one(
            "MERGE INTO users u USING orders o ON u.tenant_id = o.tenant_id \
             WHEN MATCHED THEN UPDATE SET vip = true",
        );
        assert_eq!(analysis.kind, StatementKind::Update);
        assert_eq!(
            names(
                "MERGE INTO users u USING orders o ON u.tenant_id = o.tenant_id \
                          WHEN MATCHED THEN DELETE"
            ),
            ["users", "orders"]
        );
        assert_eq!(analysis.anchors, [0]);
        assert_eq!(analysis.links.len(), 1);
        let inserting = one(
            "MERGE INTO users u USING orders o ON u.tenant_id = o.tenant_id \
             WHEN NOT MATCHED THEN INSERT (tenant_id) VALUES (o.tenant_id)",
        );
        assert_eq!(inserting.kind, StatementKind::Insert);
        assert!(inserting.links.is_empty(), "it has to name its key");
    }
}
