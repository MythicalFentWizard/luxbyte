//! PostgreSQL's own parser, behind sqlparser.
//!
//! sqlparser is fast and reads nearly everything an application sends, but
//! it is a reimplementation of PostgreSQL's grammar, not the grammar. Where
//! the two part ways it either rejects a valid statement or, worse, reads
//! one differently: `FROM ONLY users` comes out as a table named `only`.
//! So PostgreSQL's parser (`libpg_query`, compiled in) is used twice:
//!
//! - **To check.** For every statement sqlparser reads, PostgreSQL's parser
//!   names the tables it reads. That runs in C and does not build the whole
//!   tree, a few microseconds, and only the first time a statement's shape
//!   is seen. If the two disagree, PostgreSQL's reading is the one used.
//! - **To read.** A statement sqlparser cannot parse at all is analysed from
//!   PostgreSQL's parse tree.
//!
//! The analysis from PostgreSQL's tree is deliberately plainer than the
//! main one: it finds every table wherever it is read, and pins a key only
//! where the statement reads a single table. A statement it cannot place
//! in one region therefore spans regions or is refused; it is never sent
//! to a region chosen on a guess.

use std::collections::BTreeSet;

use pg_query::NodeEnum;
use pg_query::protobuf::{
    AExprKind, BoolExprType, CmdType, Node, SetOperation, TransactionStmtKind, a_const,
};

use crate::analyze::scan_highest_placeholder;
use crate::key::{ColumnRef, KeyConstraints, KeyValue};
use crate::merge::{PostProcessing, is_aggregate_name};
use crate::{Access, SqlError, StatementAnalysis, StatementKind, TableRef, TransactionOp};

/// Analyses a statement from PostgreSQL's parse tree.
///
/// # Errors
///
/// When PostgreSQL rejects it, or it is a kind of statement this analysis
/// does not place: a maintenance command such as `REINDEX` cannot run in
/// the transaction a statement over several regions needs, so it stays
/// unreadable and the application names its region.
pub(crate) fn analyze(sql: &str) -> Result<Vec<StatementAnalysis>, SqlError> {
    let parsed = pg_query::parse(sql).map_err(|error| SqlError::Parse(error.to_string()))?;
    let highest_parameter = scan_highest_placeholder(sql);
    parsed
        .protobuf
        .stmts
        .iter()
        .map(|raw| {
            raw.stmt
                .as_ref()
                .and_then(|stmt| stmt.node.as_ref())
                .and_then(|node| statement(node, highest_parameter))
                .ok_or_else(|| {
                    SqlError::Parse("PostgreSQL reads it, but the proxy cannot place it".to_owned())
                })
        })
        .collect()
}

/// Whether every table PostgreSQL's parser says a statement reads is one
/// `analyses` read too. More on sqlparser's side only makes the routing
/// more careful, and PostgreSQL's summary itself leaves some out (a MERGE's
/// source); a table it names that sqlparser did not is a misreading, as
/// with `FROM ONLY users`. A statement PostgreSQL rejects agrees: it will
/// reject it again when it runs.
pub(crate) fn agrees(sql: &str, analyses: &[StatementAnalysis]) -> bool {
    let Ok(summary) = pg_query::summary(sql, -1) else {
        return true;
    };
    let theirs: BTreeSet<(Option<String>, String)> = summary
        .tables
        .iter()
        .filter(|table| {
            !matches!(
                table.context,
                pg_query::protobuf::summary_result::Context::Ddl
            )
        })
        .map(|table| {
            (
                (!table.schema_name.is_empty()).then(|| table.schema_name.clone()),
                table.table_name.clone(),
            )
        })
        .collect();
    let ours: BTreeSet<(Option<String>, String)> = analyses
        .iter()
        .filter(|analysis| analysis.kind != StatementKind::Ddl)
        .flat_map(|analysis| &analysis.tables)
        .map(|table| (table.schema.clone(), table.name.clone()))
        .collect();
    theirs.is_subset(&ours)
}

fn plain(kind: StatementKind, access: Access, highest_parameter: u16) -> StatementAnalysis {
    StatementAnalysis {
        kind,
        access,
        tables: Vec::new(),
        constraints: KeyConstraints::unconstrained(),
        assigned_columns: Vec::new(),
        highest_parameter,
        post_processing: PostProcessing::default(),
        links: Vec::new(),
        anchors: Vec::new(),
        branches: Vec::new(),
    }
}

fn table_of(range: &pg_query::protobuf::RangeVar) -> TableRef {
    TableRef {
        schema: (!range.schemaname.is_empty()).then(|| range.schemaname.clone()),
        name: range.relname.clone(),
        alias: range.alias.as_ref().map(|alias| alias.aliasname.clone()),
    }
}

/// Every table read anywhere under `node`, as often as it is read, leaving
/// out the names `WITH` gives.
fn reads(node: &NodeEnum) -> Vec<TableRef> {
    let nodes = node.nodes();
    let ctes: BTreeSet<&str> = nodes
        .iter()
        .filter_map(|(node, ..)| match node {
            pg_query::NodeRef::CommonTableExpr(cte) => Some(cte.ctename.as_str()),
            _ => None,
        })
        .collect();
    nodes
        .iter()
        .filter_map(|(node, ..)| match node {
            pg_query::NodeRef::RangeVar(range)
                if !(range.schemaname.is_empty() && ctes.contains(range.relname.as_str())) =>
            {
                Some(table_of(range))
            }
            _ => None,
        })
        .collect()
}

/// The written table first, then everything else read.
fn target_first(
    target: Option<&pg_query::protobuf::RangeVar>,
    all: Vec<TableRef>,
) -> Vec<TableRef> {
    let Some(target) = target.map(table_of) else {
        return all;
    };
    let mut rest = all;
    if let Some(position) = rest.iter().position(|table| *table == target) {
        rest.remove(position);
    }
    let mut tables = vec![target];
    tables.extend(rest);
    tables
}

/// Whether a `WITH` holds a query that writes.
fn writes_through_with(with: Option<&pg_query::protobuf::WithClause>) -> bool {
    with.is_some_and(|with| {
        with.ctes.iter().any(|cte| match cte.node.as_ref() {
            Some(NodeEnum::CommonTableExpr(cte)) => matches!(
                cte.ctequery.as_ref().and_then(|query| query.node.as_ref()),
                Some(
                    NodeEnum::InsertStmt(_)
                        | NodeEnum::UpdateStmt(_)
                        | NodeEnum::DeleteStmt(_)
                        | NodeEnum::MergeStmt(_)
                )
            ),
            _ => false,
        })
    })
}

#[allow(clippy::too_many_lines)]
fn statement(node: &NodeEnum, highest_parameter: u16) -> Option<StatementAnalysis> {
    let mut analysis = plain(StatementKind::Other, Access::ReadWrite, highest_parameter);
    match node {
        NodeEnum::SelectStmt(select) => {
            analysis.kind = StatementKind::Select;
            analysis.access = if select.locking_clause.is_empty()
                && select.into_clause.is_none()
                && !writes_through_with(select.with_clause.as_ref())
            {
                Access::ReadOnly
            } else {
                Access::ReadWrite
            };
            analysis.tables = reads(node);
            analysis.post_processing = post_processing(select);
            let direct = select
                .from_clause
                .iter()
                .filter(|item| matches!(item.node, Some(NodeEnum::RangeVar(_))))
                .count();
            if select.op == SetOperation::SetopNone as i32
                && analysis.tables.len() == 1
                && direct == 1
            {
                analysis.anchors = vec![0];
                if let Some(predicate) = select.where_clause.as_deref() {
                    analysis.constraints = single_table(&analysis.tables, predicate);
                }
            }
        }
        NodeEnum::InsertStmt(insert) => {
            analysis.kind = StatementKind::Insert;
            analysis.tables = target_first(insert.relation.as_ref(), reads(node));
            if let (Some(table), Some(source)) = (
                analysis.tables.first(),
                insert
                    .select_stmt
                    .as_ref()
                    .and_then(|source| source.node.as_ref()),
            ) {
                analysis.constraints = inserted(table, &insert.cols, source);
            }
        }
        NodeEnum::UpdateStmt(update) => {
            analysis.kind = StatementKind::Update;
            analysis.tables = target_first(update.relation.as_ref(), reads(node));
            analysis.assigned_columns = assigned(&update.target_list);
            analysis.anchors = vec![0];
            if let (1, Some(predicate)) = (analysis.tables.len(), update.where_clause.as_deref()) {
                analysis.constraints = single_table(&analysis.tables, predicate);
            }
        }
        NodeEnum::DeleteStmt(delete) => {
            analysis.kind = StatementKind::Delete;
            analysis.tables = target_first(delete.relation.as_ref(), reads(node));
            analysis.anchors = vec![0];
            if let (1, Some(predicate)) = (analysis.tables.len(), delete.where_clause.as_deref()) {
                analysis.constraints = single_table(&analysis.tables, predicate);
            }
        }
        NodeEnum::MergeStmt(merge) => {
            // A MERGE that may insert is an INSERT whose key it does not
            // name: run in several regions it would insert the row in each
            // one that has no match. One that only updates or deletes is an
            // UPDATE of its table.
            let inserts = merge.merge_when_clauses.iter().any(|clause| {
                matches!(&clause.node, Some(NodeEnum::MergeWhenClause(when))
                    if when.command_type == CmdType::CmdInsert as i32)
            });
            analysis.kind = if inserts {
                StatementKind::Insert
            } else {
                StatementKind::Update
            };
            analysis.tables = target_first(merge.relation.as_ref(), reads(node));
            analysis.anchors = vec![0];
            for clause in &merge.merge_when_clauses {
                if let Some(NodeEnum::MergeWhenClause(when)) = &clause.node {
                    analysis
                        .assigned_columns
                        .extend(assigned(&when.target_list));
                }
            }
        }
        NodeEnum::TransactionStmt(transaction) => {
            let op = match TransactionStmtKind::try_from(transaction.kind) {
                Ok(TransactionStmtKind::TransStmtBegin | TransactionStmtKind::TransStmtStart) => {
                    Some(TransactionOp::Begin)
                }
                Ok(TransactionStmtKind::TransStmtCommit) => Some(TransactionOp::Commit),
                Ok(TransactionStmtKind::TransStmtRollback) => Some(TransactionOp::Rollback),
                Ok(TransactionStmtKind::TransStmtSavepoint) => Some(TransactionOp::Savepoint),
                Ok(TransactionStmtKind::TransStmtRelease) => Some(TransactionOp::ReleaseSavepoint),
                Ok(TransactionStmtKind::TransStmtRollbackTo) => {
                    Some(TransactionOp::RollbackToSavepoint)
                }
                // Two-phase commit is the proxy's own business.
                _ => None,
            };
            if let Some(op) = op {
                analysis.kind = StatementKind::TransactionControl(op);
                if op == TransactionOp::Begin {
                    analysis.access = Access::ReadOnly;
                }
            }
        }
        NodeEnum::VariableSetStmt(_) | NodeEnum::DiscardStmt(_) => {
            analysis.kind = StatementKind::SessionState;
            analysis.access = Access::ReadOnly;
        }
        NodeEnum::VariableShowStmt(_) => {
            analysis.kind = StatementKind::Show;
            analysis.access = Access::ReadOnly;
        }
        NodeEnum::ExplainStmt(explain) => {
            let inner = explain
                .query
                .as_ref()
                .and_then(|query| query.node.as_ref())?;
            analysis = statement(inner, highest_parameter)?;
            let runs = explain.options.iter().any(|option| {
                matches!(&option.node, Some(NodeEnum::DefElem(element))
                    if element.defname.eq_ignore_ascii_case("analyze"))
            });
            if !runs {
                analysis.access = Access::ReadOnly;
            }
            analysis.kind = StatementKind::Explain;
        }
        NodeEnum::CopyStmt(copy) => {
            if let Some(inner) = copy.query.as_ref().and_then(|query| query.node.as_ref()) {
                analysis = statement(inner, highest_parameter)?;
            } else if let Some(relation) = &copy.relation {
                analysis.tables = vec![table_of(relation)];
            }
            (analysis.kind, analysis.access) = if copy.is_from {
                (StatementKind::CopyIn, Access::ReadWrite)
            } else {
                (StatementKind::CopyOut, Access::ReadOnly)
            };
        }
        NodeEnum::CreateStmt(_)
        | NodeEnum::CreateTableAsStmt(_)
        | NodeEnum::AlterTableStmt(_)
        | NodeEnum::DropStmt(_)
        | NodeEnum::TruncateStmt(_)
        | NodeEnum::IndexStmt(_)
        | NodeEnum::CreateSchemaStmt(_)
        | NodeEnum::ViewStmt(_)
        | NodeEnum::RenameStmt(_) => analysis.kind = StatementKind::Ddl,
        _ => return None,
    }
    Some(analysis)
}

/// What a query does to its rows after collecting them.
fn post_processing(select: &pg_query::protobuf::SelectStmt) -> PostProcessing {
    let aggregate_called = select.target_list.iter().any(|target| {
        let Some(target) = target.node.as_ref() else {
            return false;
        };
        target.nodes().iter().any(|(node, ..)| match node {
            pg_query::NodeRef::FuncCall(call) => {
                call.agg_star
                    || call.funcname.last().is_some_and(|name| {
                        matches!(&name.node, Some(NodeEnum::String(text))
                            if is_aggregate_name(&text.sval))
                    })
            }
            _ => false,
        })
    });
    PostProcessing {
        aggregates: aggregate_called
            || !select.group_clause.is_empty()
            || select.having_clause.is_some()
            // Its sort keys are not written in a form the merge reads; the
            // combined result is planned from the statement's text, which
            // decides whether it can be built.
            || !select.sort_clause.is_empty(),
        distinct: !select.distinct_clause.is_empty(),
        order_by: Vec::new(),
        limited: select.limit_count.is_some() || select.limit_offset.is_some(),
        set_operation: select.op != SetOperation::SetopNone as i32,
    }
}

/// The columns an assignment list sets.
fn assigned(targets: &[Node]) -> Vec<String> {
    targets
        .iter()
        .filter_map(|target| match &target.node {
            Some(NodeEnum::ResTarget(target)) if !target.name.is_empty() => {
                Some(target.name.clone())
            }
            _ => None,
        })
        .collect()
}

/// A predicate's constraints on the one table a statement reads.
fn single_table(tables: &[TableRef], predicate: &Node) -> KeyConstraints {
    let Some(table) = tables.first() else {
        return KeyConstraints::unconstrained();
    };
    let names = [Some(table.name.as_str()), table.alias.as_deref()];
    constraints(predicate)
        .columns()
        .filter(|(column, _)| {
            column
                .qualifier
                .as_deref()
                .is_none_or(|qualifier| names.contains(&Some(qualifier)))
        })
        .fold(KeyConstraints::unconstrained(), |all, (column, values)| {
            all.and(KeyConstraints::pinned_to_any_of(
                ColumnRef::qualified(table.name.clone(), column.column.clone()),
                values.clone(),
            ))
        })
}

fn constraints(node: &Node) -> KeyConstraints {
    match node.node.as_ref() {
        Some(NodeEnum::BoolExpr(expr)) => match BoolExprType::try_from(expr.boolop) {
            Ok(BoolExprType::AndExpr) => expr
                .args
                .iter()
                .fold(KeyConstraints::unconstrained(), |all, arg| {
                    all.and(constraints(arg))
                }),
            Ok(BoolExprType::OrExpr) => {
                let mut branches = expr.args.iter().map(constraints);
                let first = branches.next().unwrap_or_default();
                branches.fold(first, KeyConstraints::or)
            }
            _ => KeyConstraints::unconstrained(),
        },
        Some(NodeEnum::AExpr(expr)) => {
            let equals = matches!(expr.name.as_slice(), [only]
                if matches!(&only.node, Some(NodeEnum::String(text)) if text.sval == "="));
            if !equals {
                return KeyConstraints::unconstrained();
            }
            let (Some(left), Some(right)) = (expr.lexpr.as_deref(), expr.rexpr.as_deref()) else {
                return KeyConstraints::unconstrained();
            };
            match AExprKind::try_from(expr.kind) {
                Ok(AExprKind::AexprOp) => {
                    if let (Some(column), Some(value)) = (column(left), value(right)) {
                        KeyConstraints::pinned(column, value)
                    } else if let (Some(value), Some(column)) = (value(left), column(right)) {
                        KeyConstraints::pinned(column, value)
                    } else {
                        KeyConstraints::unconstrained()
                    }
                }
                Ok(AExprKind::AexprIn) => match (column(left), &right.node) {
                    (Some(column), Some(NodeEnum::List(list))) => any_of(column, &list.items),
                    _ => KeyConstraints::unconstrained(),
                },
                Ok(AExprKind::AexprOpAny) => match (column(left), &right.node) {
                    (Some(column), Some(NodeEnum::AArrayExpr(array))) => {
                        any_of(column, &array.elements)
                    }
                    _ => KeyConstraints::unconstrained(),
                },
                _ => KeyConstraints::unconstrained(),
            }
        }
        _ => KeyConstraints::unconstrained(),
    }
}

fn any_of(column: ColumnRef, items: &[Node]) -> KeyConstraints {
    let mut values = BTreeSet::new();
    for item in items {
        // One element that is not a constant leaves the rows unknown.
        let Some(found) = value(item) else {
            return KeyConstraints::unconstrained();
        };
        values.insert(found);
    }
    KeyConstraints::pinned_to_any_of(column, values)
}

fn column(node: &Node) -> Option<ColumnRef> {
    let Some(NodeEnum::ColumnRef(reference)) = &node.node else {
        return None;
    };
    let names: Vec<&str> = reference
        .fields
        .iter()
        .map(|field| match &field.node {
            Some(NodeEnum::String(text)) => Some(text.sval.as_str()),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    match names.as_slice() {
        [column] => Some(ColumnRef::bare(*column)),
        [.., qualifier, column] => Some(ColumnRef::qualified(*qualifier, *column)),
        [] => None,
    }
}

fn value(node: &Node) -> Option<KeyValue> {
    match node.node.as_ref()? {
        NodeEnum::AConst(constant) => {
            if constant.isnull {
                return None;
            }
            Some(KeyValue::Literal(match constant.val.as_ref()? {
                a_const::Val::Ival(number) => number.ival.to_string(),
                a_const::Val::Fval(number) => number.fval.clone(),
                a_const::Val::Sval(text) => text.sval.clone(),
                a_const::Val::Boolval(flag) => flag.boolval.to_string(),
                a_const::Val::Bsval(_) => return None,
            }))
        }
        NodeEnum::ParamRef(parameter) => u16::try_from(parameter.number)
            .ok()
            .map(KeyValue::Parameter),
        // A cast does not change which rows match.
        NodeEnum::TypeCast(cast) => value(cast.arg.as_deref()?),
        _ => None,
    }
}

/// The keys an `INSERT ... VALUES` gives, by the column list.
fn inserted(table: &TableRef, columns: &[Node], source: &NodeEnum) -> KeyConstraints {
    let NodeEnum::SelectStmt(select) = source else {
        return KeyConstraints::unconstrained();
    };
    let names: Vec<Option<&str>> = columns
        .iter()
        .map(|column| match &column.node {
            Some(NodeEnum::ResTarget(target)) => Some(target.name.as_str()),
            _ => None,
        })
        .collect();
    if names.is_empty() || select.values_lists.is_empty() {
        return KeyConstraints::unconstrained();
    }
    let mut all = KeyConstraints::unconstrained();
    for (index, name) in names.iter().enumerate() {
        let Some(name) = name else {
            continue;
        };
        let mut values = BTreeSet::new();
        let mut usable = true;
        for row in &select.values_lists {
            let found = match &row.node {
                Some(NodeEnum::List(list)) => list.items.get(index).and_then(value),
                _ => None,
            };
            match found {
                Some(found) => {
                    values.insert(found);
                }
                None => usable = false,
            }
        }
        if usable {
            all = all.and(KeyConstraints::pinned_to_any_of(
                ColumnRef::qualified(table.name.clone(), *name),
                values,
            ));
        }
    }
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(sql: &str) -> StatementAnalysis {
        let mut all = analyze(sql).expect("PostgreSQL parses it");
        assert_eq!(all.len(), 1);
        all.remove(0)
    }

    fn key(analysis: &StatementAnalysis, table: &str) -> Vec<String> {
        analysis
            .shard_key_values(table, "tenant_id")
            .map(|values| values.iter().map(ToString::to_string).collect())
            .unwrap_or_default()
    }

    #[test]
    fn what_sqlparser_cannot_read_is_read_from_postgresql_s_tree() {
        let analysis = one("SELECT * FROM users* WHERE tenant_id = 'a'");
        assert_eq!(analysis.kind, StatementKind::Select);
        assert_eq!(analysis.access, Access::ReadOnly);
        assert_eq!(key(&analysis, "users"), ["a"]);

        let analysis = one(
            "INSERT INTO users (tenant_id, name) OVERRIDING SYSTEM VALUE \
             VALUES ('a', 'x'), ($1, 'y')",
        );
        assert_eq!(analysis.kind, StatementKind::Insert);
        assert_eq!(key(&analysis, "users"), ["a", "$1"]);

        let analysis = one("DELETE FROM users WHERE tenant_id = ANY(ARRAY['a', 'b'])");
        assert_eq!(key(&analysis, "users"), ["a", "b"]);

        let analysis = one("SELECT * FROM users WHERE tenant_id BETWEEN SYMMETRIC 'a' AND 'b'");
        assert!(analysis.shard_key_values("users", "tenant_id").is_none());
    }

    #[test]
    fn every_table_is_found_and_a_key_is_pinned_only_for_one() {
        let analysis = one(
            "WITH f AS (SELECT * FROM orders) SELECT * FROM ONLY users u \
             WHERE u.tenant_id = 'a' AND EXISTS (SELECT 1 FROM f)",
        );
        let names: Vec<String> = analysis.tables.iter().map(ToString::to_string).collect();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"users".to_owned()) && names.contains(&"orders".to_owned()));
        assert!(analysis.shard_key_values("users", "tenant_id").is_none());
    }

    #[test]
    fn merge_is_an_insert_when_it_may_insert() {
        let analysis = one(
            "MERGE INTO users u USING orders o ON u.tenant_id = o.tenant_id \
             WHEN MATCHED THEN UPDATE SET name = 'x' WHEN NOT MATCHED THEN DO NOTHING",
        );
        assert_eq!(analysis.kind, StatementKind::Update);
        assert_eq!(analysis.assigned_columns, ["name"]);
        let analysis = one(
            "MERGE INTO users u USING orders o ON u.tenant_id = o.tenant_id \
             WHEN NOT MATCHED THEN INSERT (tenant_id) VALUES (o.tenant_id)",
        );
        assert_eq!(analysis.kind, StatementKind::Insert);
    }

    #[test]
    fn the_check_catches_a_table_read_differently() {
        // sqlparser reads ONLY as the table's name.
        let ours = crate::analyze::analyze_with_sqlparser("SELECT * FROM ONLY users").unwrap();
        assert!(!agrees("SELECT * FROM ONLY users", &ours));
        let ours = crate::analyze::analyze_with_sqlparser("SELECT * FROM users").unwrap();
        assert!(agrees("SELECT * FROM users", &ours));
    }
}
