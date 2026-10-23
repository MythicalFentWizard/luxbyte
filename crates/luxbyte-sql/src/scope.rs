//! Every table a statement reads or writes, and what each is pinned to,
//! worked out block by block.
//!
//! A statement is a tree of query blocks: the outer query, each subquery
//! under `WHERE`, in the select list or in `FROM`, each `WITH` query, each
//! branch of a `UNION`. A table hidden in any of them is still read, and a
//! router that does not know about it sends the statement somewhere that
//! table's rows may not be. So every block is walked, and a predicate is
//! credited only to the block it belongs to:
//!
//! - A predicate in a block's `WHERE` restricts that block's own tables: a
//!   row that fails it cannot contribute, whatever the outer query does with
//!   the block. It says nothing of an outer table, since the block may sit
//!   under `NOT EXISTS` or an `OR`.
//! - An equality between two columns is a link along which a restriction
//!   flows: from an outer table into a correlated subquery, between tables
//!   of an inner join, from the preserved side of an outer join to the side
//!   that may be missing, never back.
//! - The same table read in two blocks is restricted to the union of the
//!   two, and not at all if either is unrestricted.
//!
//! Links between shard keys also say that rows only ever meet when their
//! keys are equal, so a statement that joins on the key can run in every
//! region at once: matching rows always live together.
//!
//! Whatever the walk does not reach is still caught: every relation the
//! parser saw is counted, and one the walk missed is added unrestricted.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

use sqlparser::ast::{
    Assignment, Delete, Expr, FromTable, Insert, JoinConstraint, JoinOperator, OnConflictAction,
    OnInsert, Query, Select, SetExpr, Statement, TableFactor, TableObject, TableWithJoins, Update,
    UpdateTableFromKind, Visit, Visitor,
};

use crate::analyze::{column_of, constraints_of, fold, insert_constraints, table_from_name};
use crate::key::{ColumnRef, KeyConstraints, KeyValue};
use crate::{Link, TableRef};

/// A name a block can use to refer to a relation in it.
#[derive(Debug, Clone)]
struct Visible {
    name: String,
    /// The table it is, or `None` for a subquery, a function or a `WITH`
    /// query, whose columns are not a table's.
    instance: Option<usize>,
}

/// One query block's relations.
#[derive(Debug, Default)]
struct Scope {
    visible: Vec<Visible>,
    /// The tables read in this block itself, not in its subqueries.
    own: Vec<usize>,
    /// Every row of this block is part of the statement's result.
    anchoring: bool,
}

/// How a join treats rows without a match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Join {
    Inner,
    /// Keeps every row of the left side.
    Left,
    /// Keeps every row of the right side.
    Right,
    /// Keeps every row of both.
    Full,
}

/// What a walk found.
#[derive(Debug, Default)]
pub(crate) struct Walker {
    /// Each read of a table, in the order met.
    tables: Vec<TableRef>,
    /// What each read's own columns are pinned to, unqualified.
    pins: Vec<KeyConstraints>,
    /// Restrictions that flow from one read's column to another's.
    flows: Vec<(usize, String, usize, String)>,
    links: Vec<Link>,
    /// Names given by `WITH`, which are not tables.
    ctes: BTreeSet<String>,
    stack: Vec<Scope>,
    /// Reads every result row is drawn from.
    anchors: Vec<usize>,
    /// The next block walked is one whose rows make up the result.
    next_anchoring: bool,
    /// A data-modifying `WITH` query, or `SELECT ... INTO`, writes.
    pub(crate) writes: bool,
    /// What the parser saw, from one pass over the statement.
    seen: Seen,
}

/// One pass over a whole statement: every relation named, the names that
/// are functions rather than tables, and how many queries it holds.
#[derive(Debug, Default)]
struct Seen {
    relations: Vec<TableRef>,
    functions: Vec<TableRef>,
    queries: usize,
    /// Whether any query in it is a subquery.
    subqueries: bool,
}

impl Visitor for Seen {
    type Break = ();
    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
        self.queries = self.queries.saturating_add(1);
        ControlFlow::Continue(())
    }
    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
        if let TableFactor::Table {
            name,
            args: Some(_),
            ..
        } = factor
        {
            self.functions.push(table_from_name(name, None));
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_relation(&mut self, name: &sqlparser::ast::ObjectName) -> ControlFlow<()> {
        self.relations.push(table_from_name(name, None));
        ControlFlow::Continue(())
    }
}

/// The subqueries directly under a node, not those nested in them.
fn subqueries<V: Visit>(node: &V) -> Vec<Query> {
    #[derive(Default)]
    struct Collect {
        depth: usize,
        found: Vec<Query>,
    }
    impl Visitor for Collect {
        type Break = ();
        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
            if self.depth == 0 {
                self.found.push(query.clone());
            }
            self.depth = self.depth.saturating_add(1);
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
            self.depth = self.depth.saturating_sub(1);
            ControlFlow::Continue(())
        }
    }
    let mut collect = Collect::default();
    let _ = node.visit(&mut collect);
    collect.found
}

/// The conjuncts of an `AND` chain.
fn conjuncts(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: sqlparser::ast::BinaryOperator::And,
            right,
        } => {
            let mut all = conjuncts(left);
            all.extend(conjuncts(right));
            all
        }
        Expr::Nested(inner) => conjuncts(inner),
        other => vec![other],
    }
}

/// `a = b` between two columns.
fn column_equality(expr: &Expr) -> Option<(ColumnRef, ColumnRef)> {
    match expr {
        Expr::BinaryOp {
            left,
            op: sqlparser::ast::BinaryOperator::Eq,
            right,
        } => Some((column_of(left)?, column_of(right)?)),
        Expr::Nested(inner) => column_equality(inner),
        _ => None,
    }
}

impl Walker {
    /// The reads of tables, in the order met.
    pub(crate) fn tables(&self) -> &[TableRef] {
        &self.tables
    }

    fn scope(&mut self) -> Option<&mut Scope> {
        if self.stack.is_empty() {
            self.stack.push(Scope::default());
        }
        self.stack.last_mut()
    }

    fn add_table(&mut self, table: TableRef, visible: Option<String>) -> usize {
        let index = self.tables.len();
        let name = visible.unwrap_or_else(|| table.alias.clone().unwrap_or(table.name.clone()));
        self.tables.push(table);
        self.pins.push(KeyConstraints::unconstrained());
        if self.stack.last().is_some_and(|scope| scope.anchoring) {
            self.anchors.push(index);
        }
        if let Some(scope) = self.scope() {
            scope.own.push(index);
            scope.visible.push(Visible {
                name,
                instance: Some(index),
            });
        }
        index
    }

    fn add_opaque(&mut self, name: Option<String>) {
        if let (Some(name), Some(scope)) = (name, self.scope()) {
            scope.visible.push(Visible {
                name,
                instance: None,
            });
        }
    }

    /// The table a qualifier names, searching this block, then the blocks
    /// around it. `None` when it is not a table, or not known.
    fn resolve(&self, qualifier: &str) -> Option<usize> {
        for scope in self.stack.iter().rev() {
            if let Some(visible) = scope.visible.iter().rev().find(|v| v.name == qualifier) {
                return visible.instance;
            }
        }
        None
    }

    /// The table a column belongs to: by its qualifier, or, unqualified, the
    /// only relation of this block if that is a table. An unqualified
    /// column in a block of several relations is ambiguous to the proxy,
    /// which does not know their columns, so it is credited to none.
    fn owner(&self, column: &ColumnRef) -> Option<usize> {
        if let Some(qualifier) = &column.qualifier {
            return self.resolve(qualifier);
        }
        match self.stack.last()?.visible.as_slice() {
            [only] => only.instance,
            _ => None,
        }
    }

    fn is_own(&self, instance: usize) -> bool {
        self.stack
            .last()
            .is_some_and(|scope| scope.own.contains(&instance))
    }

    fn pin(&mut self, instance: usize, column: &str, values: &BTreeSet<KeyValue>) {
        if let Some(pins) = self.pins.get_mut(instance) {
            let current = std::mem::take(pins);
            *pins = current.and(KeyConstraints::pinned_to_any_of(
                ColumnRef::bare(column),
                values.clone(),
            ));
        }
    }

    fn flow(&mut self, from: (usize, &str), to: (usize, &str)) {
        self.flows
            .push((from.0, from.1.to_owned(), to.0, to.1.to_owned()));
    }

    fn link(&mut self, left: (usize, &str), right: (usize, &str)) {
        self.links.push(Link {
            left: left.0,
            left_column: left.1.to_owned(),
            right: right.0,
            right_column: right.1.to_owned(),
        });
    }

    fn nested(&mut self, node: &impl Visit) {
        // Most statements hold no subquery, and then there is none to look
        // for anywhere.
        if !self.seen.subqueries {
            return;
        }
        for query in subqueries(node) {
            self.query(&query);
        }
    }

    /// A walker for `statement`, having looked over it once.
    pub(crate) fn over(statement: &Statement) -> Self {
        let mut seen = Seen::default();
        let _ = statement.visit(&mut seen);
        // The query a SELECT is, or an INSERT takes its rows from, is walked
        // directly; any other is a subquery.
        let direct = match statement {
            Statement::Query(_) => 1,
            Statement::Insert(insert) if insert.source.is_some() => 1,
            _ => 0,
        };
        seen.subqueries = seen.queries > direct;
        Self {
            seen,
            ..Self::default()
        }
    }

    // ---------------------------------------------------------- queries

    /// A query: its `WITH` queries, its body, and its own ordering.
    pub(crate) fn query(&mut self, query: &Query) {
        let anchoring = std::mem::take(&mut self.next_anchoring);
        self.name_ctes(query);
        self.walk_ctes(query);
        self.next_anchoring = anchoring;
        self.set_expr(&query.body);
        self.next_anchoring = false;
        self.ordering(query);
    }

    /// Walks a statement's outer query, whose rows are its result.
    pub(crate) fn result_query(&mut self, query: &Query) {
        self.next_anchoring = true;
        self.query(query);
    }

    /// The names a query's `WITH` gives, which are not tables from here on.
    pub(crate) fn name_ctes(&mut self, query: &Query) {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.ctes.insert(fold(&cte.alias.name));
            }
        }
    }

    /// The queries a `WITH` defines.
    pub(crate) fn walk_ctes(&mut self, query: &Query) {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.query(&cte.query);
            }
        }
    }

    /// Subqueries in a query's own `ORDER BY` and `LIMIT`.
    pub(crate) fn ordering(&mut self, query: &Query) {
        if let Some(order_by) = &query.order_by {
            self.nested(order_by);
        }
        if let Some(limit) = &query.limit_clause {
            self.nested(limit);
        }
    }

    /// A query body, by itself or as one branch of a set operation.
    pub(crate) fn set_expr(&mut self, body: &SetExpr) {
        match body {
            SetExpr::Select(select) => self.select(select),
            SetExpr::Query(query) => self.query(query),
            // Branches are not followed for result rows: a set operation is
            // routed branch by branch instead.
            SetExpr::SetOperation { left, right, .. } => {
                self.next_anchoring = false;
                self.set_expr(left);
                self.set_expr(right);
            }
            SetExpr::Values(values) => self.nested(values),
            SetExpr::Insert(statement)
            | SetExpr::Update(statement)
            | SetExpr::Delete(statement)
            | SetExpr::Merge(statement) => {
                self.writes = true;
                self.statement(statement);
            }
            SetExpr::Table(table) => {
                if let Some(name) = &table.table_name {
                    let table = TableRef {
                        schema: table.schema_name.clone(),
                        name: name.clone(),
                        alias: None,
                    };
                    self.stack.push(Scope::default());
                    if !(table.schema.is_none() && self.ctes.contains(&table.name)) {
                        self.add_table(table, None);
                    }
                    self.stack.pop();
                }
            }
        }
    }

    fn select(&mut self, select: &Select) {
        self.stack.push(Scope {
            anchoring: std::mem::take(&mut self.next_anchoring),
            ..Scope::default()
        });
        if select.into.is_some() {
            self.writes = true;
        }
        for item in &select.from {
            self.table_with_joins(item);
        }
        if let Some(selection) = &select.selection {
            self.condition(selection);
        }
        for item in &select.projection {
            self.nested(item);
        }
        self.nested(&select.group_by);
        if let Some(having) = &select.having {
            self.nested(having);
        }
        if let Some(qualify) = &select.qualify {
            self.nested(qualify);
        }
        self.stack.pop();
    }

    // ----------------------------------------------------------- tables

    fn table_with_joins(&mut self, item: &TableWithJoins) {
        self.factor(&item.relation);
        for join in &item.joins {
            let left: Vec<usize> = self
                .stack
                .last()
                .map(|scope| scope.own.clone())
                .unwrap_or_default();
            self.factor(&join.relation);
            let right: Vec<usize> = self
                .stack
                .last()
                .map(|scope| {
                    scope
                        .own
                        .iter()
                        .copied()
                        .filter(|own| !left.contains(own))
                        .collect()
                })
                .unwrap_or_default();
            let (kind, constraint) = match &join.join_operator {
                JoinOperator::Join(constraint)
                | JoinOperator::Inner(constraint)
                | JoinOperator::CrossJoin(constraint)
                | JoinOperator::StraightJoin(constraint) => (Join::Inner, constraint),
                JoinOperator::Left(constraint)
                | JoinOperator::LeftOuter(constraint)
                | JoinOperator::Semi(constraint)
                | JoinOperator::LeftSemi(constraint)
                | JoinOperator::Anti(constraint)
                | JoinOperator::LeftAnti(constraint) => (Join::Left, constraint),
                JoinOperator::Right(constraint)
                | JoinOperator::RightOuter(constraint)
                | JoinOperator::RightSemi(constraint)
                | JoinOperator::RightAnti(constraint) => (Join::Right, constraint),
                JoinOperator::FullOuter(constraint) => (Join::Full, constraint),
                // Not PostgreSQL; their tables are still read, unpinned.
                _ => {
                    self.anchors.retain(|anchor| !right.contains(anchor));
                    continue;
                }
            };
            // A side an outer join may leave out does not anchor a row.
            let nullable: Vec<usize> = match kind {
                Join::Inner => Vec::new(),
                Join::Left => right.clone(),
                Join::Right => left.clone(),
                Join::Full => left.iter().chain(&right).copied().collect(),
            };
            self.anchors.retain(|anchor| !nullable.contains(anchor));
            self.join(kind, constraint, &left, &right);
        }
    }

    fn factor(&mut self, factor: &TableFactor) {
        match factor {
            TableFactor::Table {
                name, alias, args, ..
            } => {
                let alias = alias.as_ref().map(|alias| fold(&alias.name));
                if let Some(args) = args {
                    // A set-returning function, not a table.
                    self.nested(args);
                    self.add_opaque(alias.or_else(|| Some(table_from_name(name, None).name)));
                    return;
                }
                let table = table_from_name(name, alias.clone());
                if table.schema.is_none() && self.ctes.contains(&table.name) {
                    self.add_opaque(Some(alias.unwrap_or(table.name)));
                } else {
                    self.add_table(table, alias);
                }
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                // A subquery in FROM gives rows to its block's rows.
                self.next_anchoring = self.stack.last().is_some_and(|scope| scope.anchoring);
                self.query(subquery);
                self.add_opaque(alias.as_ref().map(|alias| fold(&alias.name)));
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => self.table_with_joins(table_with_joins),
            other => {
                self.nested(other);
                let alias = match other {
                    TableFactor::TableFunction { alias, .. }
                    | TableFactor::Function { alias, .. }
                    | TableFactor::UNNEST { alias, .. } => alias.as_ref(),
                    _ => None,
                };
                self.add_opaque(alias.map(|alias| fold(&alias.name)));
            }
        }
    }

    // ------------------------------------------------------- predicates

    /// A block's `WHERE`: restricts the block's own tables, and links them
    /// with each other and with the blocks around it.
    fn condition(&mut self, expr: &Expr) {
        self.nested(expr);
        for (column, values) in constraints_of(expr).columns() {
            if let Some(instance) = self.owner(column) {
                if self.is_own(instance) {
                    self.pin(instance, &column.column, values);
                }
            }
        }
        for conjunct in conjuncts(expr) {
            let Some((a, b)) = column_equality(conjunct) else {
                continue;
            };
            let (Some(left), Some(right)) = (self.owner(&a), self.owner(&b)) else {
                continue;
            };
            let a = (left, a.column.as_str());
            let b = (right, b.column.as_str());
            match (self.is_own(left), self.is_own(right)) {
                (true, true) => {
                    self.flow(a, b);
                    self.flow(b, a);
                    self.link(a, b);
                }
                // A correlated subquery reads only the rows that match the
                // outer row it is asked about.
                (true, false) => {
                    self.flow(b, a);
                    self.link(a, b);
                }
                (false, true) => {
                    self.flow(a, b);
                    self.link(a, b);
                }
                (false, false) => {}
            }
        }
    }

    fn join(&mut self, kind: Join, constraint: &JoinConstraint, left: &[usize], right: &[usize]) {
        match constraint {
            JoinConstraint::On(expr) => {
                self.nested(expr);
                // Only the side that may be missing is restricted by what
                // the join asks of it: the other keeps every row anyway.
                let restricted = |instance: usize| match kind {
                    Join::Inner => true,
                    Join::Left => right.contains(&instance),
                    Join::Right => left.contains(&instance),
                    Join::Full => false,
                };
                for conjunct in conjuncts(expr) {
                    for (column, values) in constraints_of(conjunct).columns() {
                        if let Some(instance) = self.owner(column) {
                            if self.is_own(instance) && restricted(instance) {
                                self.pin(instance, &column.column, values);
                            }
                        }
                    }
                    if let Some((a, b)) = column_equality(conjunct) {
                        if let (Some(first), Some(second)) = (self.owner(&a), self.owner(&b)) {
                            self.join_equality(
                                kind,
                                (first, &a.column),
                                (second, &b.column),
                                left,
                                right,
                            );
                        }
                    }
                }
            }
            JoinConstraint::Using(names) => {
                // `USING (c)` is left.c = right.c, readable when each side
                // is one table.
                let ([only_left], [only_right]) = (left, right) else {
                    return;
                };
                for name in names {
                    if let Some(column) = name.0.last().and_then(|part| part.as_ident()) {
                        let column = fold(column);
                        self.join_equality(
                            kind,
                            (*only_left, &column),
                            (*only_right, &column),
                            left,
                            right,
                        );
                    }
                }
            }
            JoinConstraint::Natural | JoinConstraint::None => {}
        }
    }

    fn join_equality(
        &mut self,
        kind: Join,
        a: (usize, &str),
        b: (usize, &str),
        left: &[usize],
        right: &[usize],
    ) {
        let side = |instance: usize| {
            if left.contains(&instance) {
                Some(true)
            } else if right.contains(&instance) {
                Some(false)
            } else {
                None
            }
        };
        let (Some(a_left), Some(b_left)) = (side(a.0), side(b.0)) else {
            return;
        };
        if a_left == b_left {
            // Both on one side: a condition on the match, which an inner
            // join applies to every row it returns.
            if kind == Join::Inner {
                self.flow(a, b);
                self.flow(b, a);
                self.link(a, b);
            }
            return;
        }
        let (on_left, on_right) = if a_left { (a, b) } else { (b, a) };
        // Rows meet only where the columns are equal, whatever the join
        // keeps besides.
        self.link(on_left, on_right);
        match kind {
            Join::Inner => {
                self.flow(on_left, on_right);
                self.flow(on_right, on_left);
            }
            Join::Left => self.flow(on_left, on_right),
            Join::Right => self.flow(on_right, on_left),
            Join::Full => {}
        }
    }

    // --------------------------------------------------- data statements

    /// `INSERT`, `UPDATE`, `DELETE`: the table written first, then the
    /// tables read.
    pub(crate) fn statement(&mut self, statement: &Statement) {
        match statement {
            Statement::Insert(insert) => self.insert(insert),
            Statement::Update(update) => self.update(update),
            Statement::Delete(delete) => self.delete(delete),
            Statement::Query(query) => self.query(query),
            other => self.nested(other),
        }
    }

    fn insert(&mut self, insert: &Insert) {
        self.stack.push(Scope::default());
        if let TableObject::TableName(name) = &insert.table {
            let alias = insert.table_alias.as_ref().map(|alias| fold(&alias.alias));
            let table = table_from_name(name, alias.clone());
            let pins = insert_constraints(insert, &table);
            let index = self.add_table(table, alias);
            if let Some(slot) = self.pins.get_mut(index) {
                *slot = pins.unqualified();
            }
        }
        if let Some(source) = &insert.source {
            self.query(source);
        }
        if let Some(OnInsert::OnConflict(conflict)) = &insert.on {
            if let OnConflictAction::DoUpdate(update) = &conflict.action {
                self.assignments(&update.assignments);
                if let Some(selection) = &update.selection {
                    self.nested(selection);
                }
            }
        }
        if let Some(returning) = &insert.returning {
            self.nested(returning);
        }
        self.stack.pop();
    }

    fn update(&mut self, update: &Update) {
        self.stack.push(Scope {
            anchoring: true,
            ..Scope::default()
        });
        self.factor(&update.table.relation);
        if let Some(scope) = self.stack.last_mut() {
            scope.anchoring = false;
        }
        for join in &update.table.joins {
            self.factor(&join.relation);
        }
        if let Some(UpdateTableFromKind::BeforeSet(items) | UpdateTableFromKind::AfterSet(items)) =
            &update.from
        {
            for item in items {
                self.table_with_joins(item);
            }
        }
        self.assignments(&update.assignments);
        if let Some(selection) = &update.selection {
            self.condition(selection);
        }
        if let Some(returning) = &update.returning {
            self.nested(returning);
        }
        self.stack.pop();
    }

    fn delete(&mut self, delete: &Delete) {
        self.stack.push(Scope {
            anchoring: true,
            ..Scope::default()
        });
        match &delete.from {
            FromTable::WithFromKeyword(items) | FromTable::WithoutKeyword(items) => {
                for item in items {
                    self.table_with_joins(item);
                    if let Some(scope) = self.stack.last_mut() {
                        scope.anchoring = false;
                    }
                }
            }
        }
        if let Some(using) = &delete.using {
            for item in using {
                self.table_with_joins(item);
            }
        }
        if let Some(selection) = &delete.selection {
            self.condition(selection);
        }
        if let Some(returning) = &delete.returning {
            self.nested(returning);
        }
        self.stack.pop();
    }

    fn assignments(&mut self, assignments: &[Assignment]) {
        for assignment in assignments {
            self.nested(&assignment.value);
        }
    }

    // ------------------------------------------------------- the result

    /// Adds, unrestricted, any table the parser saw that the walk did not
    /// reach, so that nothing a statement reads is ever left out.
    pub(crate) fn sweep(&mut self) {
        let seen = std::mem::take(&mut self.seen);
        let key = |table: &TableRef| (table.schema.clone(), table.name.clone());
        let mut missing: BTreeMap<(Option<String>, String), i64> = BTreeMap::new();
        for table in &seen.relations {
            if table.schema.is_none() && self.ctes.contains(&table.name) {
                continue;
            }
            let count = missing.entry(key(table)).or_insert(0);
            *count = count.saturating_add(1);
        }
        for table in seen.functions.iter().chain(&self.tables) {
            if let Some(count) = missing.get_mut(&key(table)) {
                *count = count.saturating_sub(1);
            }
        }
        self.stack.push(Scope::default());
        for ((schema, name), count) in missing {
            for _ in 0..count.max(0) {
                self.add_table(
                    TableRef {
                        schema: schema.clone(),
                        name: name.clone(),
                        alias: None,
                    },
                    Some(String::new()),
                );
            }
        }
        self.stack.pop();
    }

    /// The tables, what the statement pins their columns to, qualified by
    /// table, and the links between them.
    pub(crate) fn finish(mut self) -> (Vec<TableRef>, KeyConstraints, Vec<Link>, Vec<usize>) {
        // Restrictions flow along links until nothing changes; each pass
        // can only narrow or add, so as many passes as links suffice.
        for _ in 0..=self.flows.len() {
            let mut changed = false;
            for (from, from_column, to, to_column) in &self.flows {
                let Some(values) = self
                    .pins
                    .get(*from)
                    .and_then(|pins| pins.values_for("", from_column))
                    .cloned()
                else {
                    continue;
                };
                if let Some(pins) = self.pins.get_mut(*to) {
                    let before = pins.clone();
                    *pins = before.clone().and(KeyConstraints::pinned_to_any_of(
                        ColumnRef::bare(to_column.clone()),
                        values,
                    ));
                    changed |= *pins != before;
                }
            }
            if !changed {
                break;
            }
        }

        // A table read in several places is restricted only as far as all
        // of them are.
        let mut reads: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
        for (index, table) in self.tables.iter().enumerate() {
            reads.entry(table.name.as_str()).or_default().push(index);
        }
        let mut constraints = KeyConstraints::unconstrained();
        for (name, instances) in reads {
            let mut columns: Option<BTreeMap<String, BTreeSet<KeyValue>>> = None;
            for instance in instances {
                let pinned: BTreeMap<String, BTreeSet<KeyValue>> = self
                    .pins
                    .get(instance)
                    .map(|pins| {
                        pins.columns()
                            .map(|(column, values)| (column.column.clone(), values.clone()))
                            .collect()
                    })
                    .unwrap_or_default();
                columns = Some(match columns {
                    None => pinned,
                    Some(mut so_far) => {
                        so_far.retain(|column, _| pinned.contains_key(column));
                        for (column, values) in &mut so_far {
                            if let Some(more) = pinned.get(column) {
                                values.extend(more.iter().cloned());
                            }
                        }
                        so_far
                    }
                });
            }
            for (column, values) in columns.unwrap_or_default() {
                constraints = constraints.and(KeyConstraints::pinned_to_any_of(
                    ColumnRef::qualified(name, column),
                    values,
                ));
            }
        }
        (self.tables, constraints, self.links, self.anchors)
    }
}
