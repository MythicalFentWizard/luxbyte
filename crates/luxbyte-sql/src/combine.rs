//! Turning one statement into a per-region statement and a finishing step.
//!
//! A read that fans out returns one result per region, and concatenating
//! them is only the right answer for a plain union of rows. Everything a
//! statement computes *over* its rows has to be redone over the combined
//! rows:
//!
//! - `ORDER BY` holds within each region, not across them;
//! - `LIMIT` and `OFFSET` apply to the whole result, not to each part;
//! - `DISTINCT` and `UNION` deduplicate within a region, not between them;
//! - an aggregate returns one partial answer per region, which has to be
//!   combined — and `avg` cannot be combined from averages at all.
//!
//! Each region runs a rewritten statement: ordered and limited rows for a
//! plain read, or partial aggregates per group. Then the parts are finished:
//!
//! - **Rows** whose sort columns compare exactly outside PostgreSQL —
//!   integers, `numeric`, floats, `bool`, `uuid` — are merged in order by the
//!   proxy, and the limit applied there.
//! - **Everything else** is finished *in PostgreSQL*: the parts are sent to
//!   the home region as typed arrays, and a final statement does the last
//!   step there. Collation order, `numeric` precision, `HAVING`, expressions
//!   over aggregates and the exact type of every result column are then
//!   PostgreSQL's own, not a reimplementation of them. A combine step written
//!   by hand would be one more place for a number to be quietly wrong.
//!
//! What cannot be combined is refused with the reason: `count(DISTINCT ...)`,
//! ordered-set and statistical aggregates, window functions, `INTERSECT`
//! and `EXCEPT`.

use std::fmt::Write as _;
use std::ops::ControlFlow;

use sqlparser::ast::{
    Distinct, Expr, Fetch, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident,
    LimitClause, Offset, OrderBy, OrderByKind, OrderBySort, Query, Select, SelectItem, SetExpr,
    SetOperator, SetQuantifier, Statement, Value, VisitMut, VisitorMut, visit_expressions,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// Which function names are aggregates.
///
/// The built-in list knows the standard aggregates. A user-defined one, or
/// one from an extension, looks like any other function in SQL text, and
/// treating it as one would compute it per region and return each region's
/// answer as if it were the whole. So the caller adds the names the
/// database itself lists as aggregates.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Aggregates {
    names: std::collections::HashSet<String>,
}

impl Aggregates {
    /// The built-in list plus `names`, as listed in `pg_proc`.
    #[must_use]
    pub fn with_names(names: impl IntoIterator<Item = String>) -> Self {
        Self {
            names: names.into_iter().map(|name| name.to_lowercase()).collect(),
        }
    }

    /// Whether `name` is an aggregate.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        crate::merge::is_aggregate_name(name) || self.names.contains(&name.to_lowercase())
    }

    /// Whether `function` aggregates rather than computing per row.
    fn aggregates(&self, function: &sqlparser::ast::Function) -> bool {
        function.over.is_none()
            && (!function.within_group.is_empty()
                || function_name(function).is_some_and(|name| self.contains(&name)))
    }
}

/// Marks generated names, so user text containing one can be refused.
const MARKER: &str = "__luxbyte_";

/// Why a statement cannot be assembled from several regions.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct Unsupported(pub String);

fn unsupported<T>(reason: impl Into<String>) -> Result<T, Unsupported> {
    Err(Unsupported(reason.into()))
}

/// How to answer a fanned-out read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CombinePlan {
    /// What every region runs.
    pub region_sql: String,
    /// What to do with what they return.
    pub finish: Finish,
}

/// The step after every region has answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Finish {
    /// Rows, possibly ordered, deduplicated and limited.
    Rows(RowsFinish),
    /// Partial aggregates, regrouped and finished in PostgreSQL.
    Aggregate(AggregateFinish),
}

/// One `ORDER BY` term, resolved to something the result can be sorted on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderRef {
    /// A visible output column, by position from zero.
    Column(usize),
    /// A visible output column, by name, resolved once the columns are known.
    Name(String),
    /// A column added for sorting, by position among the added ones.
    Hidden(usize),
}

/// One resolved sort term.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderTerm {
    /// What it sorts on.
    pub on: OrderRef,
    /// Descending.
    pub descending: bool,
    /// Where nulls go; `None` takes PostgreSQL's default for the direction.
    pub nulls_first: Option<bool>,
}

impl OrderTerm {
    /// Where nulls go, with PostgreSQL's defaults applied: last when
    /// ascending, first when descending.
    #[must_use]
    pub fn nulls_first(&self) -> bool {
        self.nulls_first.unwrap_or(self.descending)
    }
}

/// Finishing rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowsFinish {
    /// Columns appended to each region's rows for sorting, to drop after.
    pub hidden: usize,
    /// The sort, if any.
    pub order: Vec<OrderTerm>,
    /// Deduplicate across regions.
    pub distinct: bool,
    /// Rows to return at most.
    pub limit: Option<u64>,
    /// Rows to skip first.
    pub offset: u64,
}

/// The function that combines one partial aggregate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Combiner {
    /// `count`: the sum of the counts.
    Count,
    /// `sum`: the sum of the sums, in the original result type.
    Sum,
    /// `min`.
    Min,
    /// `max`.
    Max,
    /// `avg`: the sum of the sums over the sum of the counts.
    Avg,
    /// `bool_and` and `every`.
    BoolAnd,
    /// `bool_or`.
    BoolOr,
    /// `bit_and`.
    BitAnd,
    /// `bit_or`.
    BitOr,
}

/// One aggregate in the final statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalAggregate {
    /// How it combines.
    pub combiner: Combiner,
    /// The partial column it reads, from zero; for `avg`, the sums.
    pub column: usize,
    /// For `avg`, the partial column of counts.
    pub counts: Option<usize>,
}

/// Finishing partial aggregates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateFinish {
    /// How many columns each region returns.
    pub columns: usize,
    /// The final statement before `FROM`, with each aggregate as a marker.
    select: String,
    /// The final statement after `FROM`.
    rest: String,
    /// The aggregates the markers stand for, in marker order.
    pub aggregates: Vec<FinalAggregate>,
}

/// Plans how to answer `sql` from several regions.
pub fn plan(sql: &str, aggregates: &Aggregates) -> Result<CombinePlan, Unsupported> {
    if sql.contains(MARKER) {
        return unsupported("the statement contains text this proxy reserves for its own use");
    }
    let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .map_err(|error| Unsupported(error.to_string()))?;
    let (Some(Statement::Query(query)), None) = (statements.pop(), statements.pop()) else {
        return unsupported("only a single SELECT can be combined across regions");
    };
    let mut query = *query;

    if let SetExpr::Select(select) = query.body.as_ref() {
        if needs_aggregation(select, aggregates) {
            return plan_aggregate(query, aggregates);
        }
    }
    plan_rows(&mut query, aggregates)
}

// ------------------------------------------------------------------ rows

fn plan_rows(query: &mut Query, aggregates: &Aggregates) -> Result<CombinePlan, Unsupported> {
    let (limit, offset) = take_limits(query)?;
    let mut distinct = false;
    let mut hidden: Vec<Expr> = Vec::new();
    let mut order = Vec::new();

    let terms: Vec<_> = match query.order_by.as_ref().map(|order_by| &order_by.kind) {
        Some(OrderByKind::Expressions(terms)) => terms.clone(),
        Some(OrderByKind::All(_)) => return unsupported("ORDER BY ALL is not PostgreSQL"),
        None => Vec::new(),
    };
    // A window function sees the rows of its own region only, so its answer
    // depends on how the rows happen to be spread.
    if has_window_function(&query.body) || terms.iter().any(|t| has_window_function(&t.expr)) {
        return unsupported(
            "window functions see only one region's rows, so they cannot be combined across \
             regions",
        );
    }

    match query.body.as_mut() {
        SetExpr::Select(select) => {
            match &select.distinct {
                None | Some(Distinct::All) => {}
                Some(Distinct::Distinct) => distinct = true,
                Some(Distinct::On(_)) => {
                    return unsupported(
                        "DISTINCT ON keeps one row per group from each region, and which one \
                         survives across regions depends on rows the others never see",
                    );
                }
            }
            let aliases = output_aliases(select);
            for term in &terms {
                let on = if let Some(ordinal) = ordinal(&term.expr) {
                    OrderRef::Column(ordinal)
                } else if let Some(name) = bare_name(&term.expr).filter(|n| aliases.contains(n)) {
                    OrderRef::Name(name)
                } else if let Some(position) = {
                    // An expression the client selected sorts on that column.
                    let wanted = canonical(&term.expr);
                    select.projection.iter().position(|item| {
                        item_expr(item).is_some_and(|expr| canonical(expr) == wanted)
                    })
                } {
                    OrderRef::Column(position)
                } else if distinct {
                    return unsupported(
                        "for SELECT DISTINCT, ORDER BY expressions must appear in the select list",
                    );
                } else {
                    // Sorting on something the client did not select: carry
                    // it as an extra column, sort on it, then drop it.
                    hidden.push(term.expr.clone());
                    OrderRef::Hidden(hidden.len().saturating_sub(1))
                };
                order.push(OrderTerm {
                    on,
                    descending: matches!(term.options.sort, Some(OrderBySort::Desc)),
                    nulls_first: term.options.nulls_first,
                });
            }
            for (index, expr) in hidden.iter().enumerate() {
                select.projection.push(SelectItem::ExprWithAlias {
                    expr: expr.clone(),
                    alias: Ident::with_quote('"', format!("{MARKER}sort_{index}")),
                });
            }
        }
        body @ SetExpr::SetOperation { .. } => {
            distinct = union_distinct(body, aggregates)?;
            for term in &terms {
                let on = if let Some(ordinal) = ordinal(&term.expr) {
                    OrderRef::Column(ordinal)
                } else if let Some(name) = bare_name(&term.expr) {
                    OrderRef::Name(name)
                } else {
                    return unsupported(
                        "ORDER BY on a UNION must name an output column or its position",
                    );
                };
                order.push(OrderTerm {
                    on,
                    descending: matches!(term.options.sort, Some(OrderBySort::Desc)),
                    nulls_first: term.options.nulls_first,
                });
            }
        }
        _ => return unsupported("this kind of query cannot be combined across regions"),
    }

    // Each region returns at most the rows the whole answer could need.
    if let Some(limit) = limit {
        let per_region = limit.saturating_add(offset);
        query.limit_clause = Some(LimitClause::LimitOffset {
            limit: Some(Expr::value(Value::Number(per_region.to_string(), false))),
            offset: None,
            limit_by: Vec::new(),
        });
    }

    Ok(CombinePlan {
        region_sql: query.to_string(),
        finish: Finish::Rows(RowsFinish {
            hidden: hidden.len(),
            order,
            distinct,
            limit,
            offset,
        }),
    })
}

/// Whether a set operation deduplicates. Only one kind throughout is
/// accepted: `UNION ALL` under a `UNION` would deduplicate part of the
/// result and not the rest, which one deduplicating step cannot reproduce.
fn union_distinct(body: &SetExpr, aggregates: &Aggregates) -> Result<bool, Unsupported> {
    let mut kinds = Vec::new();
    collect_unions(body, &mut kinds, aggregates)?;
    let all_distinct = kinds.iter().all(|distinct| *distinct);
    let all_all = kinds.iter().all(|distinct| !*distinct);
    if all_distinct || all_all {
        Ok(all_distinct)
    } else {
        unsupported("UNION and UNION ALL mixed in one statement cannot be combined across regions")
    }
}

fn collect_unions(
    body: &SetExpr,
    kinds: &mut Vec<bool>,
    aggregates: &Aggregates,
) -> Result<(), Unsupported> {
    match body {
        SetExpr::SetOperation {
            left,
            op,
            set_quantifier,
            right,
        } => {
            if *op != SetOperator::Union {
                return unsupported(format!(
                    "{op} compares rows between its two sides, and rows in different regions \
                     never meet"
                ));
            }
            kinds.push(!matches!(
                set_quantifier,
                SetQuantifier::All | SetQuantifier::AllByName
            ));
            collect_unions(left, kinds, aggregates)?;
            collect_unions(right, kinds, aggregates)
        }
        SetExpr::Select(select) if needs_aggregation(select, aggregates) => {
            unsupported("an aggregate inside a UNION cannot be combined across regions")
        }
        SetExpr::Select(_) | SetExpr::Query(_) | SetExpr::Values(_) | SetExpr::Table(_) => Ok(()),
        _ => unsupported("this kind of query cannot be combined across regions"),
    }
}

/// Removes `LIMIT`, `OFFSET` and `FETCH` from the query, returning them.
fn take_limits(query: &mut Query) -> Result<(Option<u64>, u64), Unsupported> {
    let mut limit = None;
    let mut offset = 0_u64;
    match query.limit_clause.take() {
        None => {}
        Some(LimitClause::LimitOffset {
            limit: count,
            offset: skip,
            limit_by,
        }) => {
            if !limit_by.is_empty() {
                return unsupported("LIMIT BY is not PostgreSQL");
            }
            limit = match count {
                // `LIMIT ALL` and `LIMIT NULL` are no limit at all.
                None => None,
                Some(expr) if is_null(&expr) => None,
                Some(expr) => Some(constant(&expr, "LIMIT")?),
            };
            if let Some(Offset { value, .. }) = skip {
                offset = constant(&value, "OFFSET")?;
            }
        }
        Some(LimitClause::OffsetCommaLimit { .. }) => {
            return unsupported("LIMIT offset, count is not PostgreSQL");
        }
    }
    if let Some(Fetch {
        with_ties,
        percent,
        quantity,
    }) = query.fetch.take()
    {
        if with_ties || percent {
            return unsupported("FETCH ... WITH TIES or PERCENT cannot be combined across regions");
        }
        let count = match quantity {
            Some(expr) => constant(&expr, "FETCH")?,
            None => 1,
        };
        limit = Some(limit.map_or(count, |existing: u64| existing.min(count)));
    }
    Ok((limit, offset))
}

fn constant(expr: &Expr, clause: &str) -> Result<u64, Unsupported> {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::Number(text, _) => text
                .parse::<u64>()
                .map_err(|_| Unsupported(format!("{clause} must be a non-negative whole number"))),
            _ => unsupported(format!("{clause} must be a constant")),
        },
        _ => unsupported(format!(
            "{clause} must be a constant to be applied across regions"
        )),
    }
}

fn is_null(expr: &Expr) -> bool {
    matches!(expr, Expr::Value(value) if value.value == Value::Null)
}

// ------------------------------------------------------------ aggregates

/// The aggregates this module can split into a partial and a final step.
fn combiner(name: &str) -> Option<Combiner> {
    Some(match name {
        "count" => Combiner::Count,
        "sum" => Combiner::Sum,
        "min" => Combiner::Min,
        "max" => Combiner::Max,
        "avg" => Combiner::Avg,
        "bool_and" | "every" => Combiner::BoolAnd,
        "bool_or" => Combiner::BoolOr,
        "bit_and" => Combiner::BitAnd,
        "bit_or" => Combiner::BitOr,
        _ => return None,
    })
}

fn needs_aggregation(select: &Select, aggregates: &Aggregates) -> bool {
    let grouped = match &select.group_by {
        GroupByExpr::All(_) => true,
        GroupByExpr::Expressions(expressions, _) => !expressions.is_empty(),
    };
    grouped
        || select.having.is_some()
        || select
            .projection
            .iter()
            .any(|item| item_expr(item).is_some_and(|expr| has_aggregate(expr, aggregates)))
}

fn function_name(function: &sqlparser::ast::Function) -> Option<String> {
    function
        .name
        .0
        .last()
        .and_then(|part| part.as_ident())
        .map(fold)
}

fn has_aggregate(expr: &Expr, aggregates: &Aggregates) -> bool {
    visit_expressions(expr, |node| {
        if let Expr::Function(function) = node
            && aggregates.aggregates(function)
        {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    })
    .is_break()
}

fn has_window_function<V: sqlparser::ast::Visit>(node: &V) -> bool {
    visit_expressions(node, |expr| {
        if let Expr::Function(function) = expr
            && function.over.is_some()
        {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    })
    .is_break()
}

/// One partial column a region returns for the aggregates.
struct Partial {
    /// The canonical text of the aggregate call it came from.
    key: String,
    /// What the region computes.
    expr: Expr,
}

struct Rewriter<'a> {
    aggregates_known: &'a Aggregates,
    /// Canonical text of each group key, by partial column.
    keys: Vec<String>,
    partials: Vec<Partial>,
    aggregates: Vec<FinalAggregate>,
    /// Canonical text of each aggregate call already given a marker.
    seen: Vec<String>,
    failure: Option<Unsupported>,
}

impl Rewriter<'_> {
    /// The expression computed from the partial columns instead, or why
    /// it cannot be.
    fn rewrite(&mut self, expr: &Expr) -> Result<Expr, Unsupported> {
        let mut rewritten = expr.clone();
        let _ = rewritten.visit(self);
        if let Some(failure) = self.failure.take() {
            return Err(failure);
        }
        unresolved(&rewritten)?;
        Ok(rewritten)
    }

    fn column(index: usize) -> Expr {
        Expr::CompoundIdentifier(vec![
            Ident::new("p"),
            Ident::new(format!("c{}", index.saturating_add(1))),
        ])
    }

    fn partial(&mut self, key: String, expr: Expr) -> usize {
        if let Some(position) = self.partials.iter().position(|p| p.key == key) {
            return self.keys.len().saturating_add(position);
        }
        self.partials.push(Partial { key, expr });
        self.keys
            .len()
            .saturating_add(self.partials.len().saturating_sub(1))
    }

    /// Replaces one aggregate call with a marker, recording its partials.
    fn aggregate(&mut self, function: &sqlparser::ast::Function) -> Result<Expr, Unsupported> {
        let name = function_name(function).unwrap_or_default();
        let Some(combiner) = combiner(&name) else {
            return unsupported(format!(
                "{name}() cannot be computed from per-region partial results"
            ));
        };
        if function.over.is_some() {
            return unsupported("window functions cannot be combined across regions");
        }
        if !function.within_group.is_empty() {
            return unsupported(format!(
                "{name}() WITHIN GROUP cannot be combined across regions"
            ));
        }
        let FunctionArguments::List(list) = &function.args else {
            return unsupported(format!("{name}() needs an argument list"));
        };
        if list.duplicate_treatment.is_some_and(|treatment| {
            matches!(treatment, sqlparser::ast::DuplicateTreatment::Distinct)
        }) {
            return unsupported(format!(
                "{name}(DISTINCT ...) counts each value once, and the same value in two regions \
                 cannot be recognised from their partial results"
            ));
        }
        if !list.clauses.is_empty() {
            return unsupported(format!(
                "{name}() with clauses inside its arguments cannot be combined across regions"
            ));
        }
        let star = matches!(
            list.args.as_slice(),
            [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
        );
        let single = matches!(
            list.args.as_slice(),
            [FunctionArg::Unnamed(FunctionArgExpr::Expr(_))]
        );
        if !(single || (star && combiner == Combiner::Count)) {
            return unsupported(format!("{name}() with these arguments cannot be combined"));
        }

        // The partials keep the call's own arguments and FILTER; only the
        // function changes, and only for avg.
        let renamed = |to: &str| {
            let mut call = function.clone();
            call.name = sqlparser::ast::ObjectName::from(vec![Ident::new(to)]);
            Expr::Function(call)
        };
        let key = canonical(&Expr::Function(function.clone()));
        let marker = if let Some(index) = self.seen.iter().position(|seen| *seen == key) {
            index
        } else {
            {
                let entry = if combiner == Combiner::Avg {
                    let sums = self.partial(format!("sum:{key}"), renamed("sum"));
                    let counts = self.partial(format!("count:{key}"), renamed("count"));
                    FinalAggregate {
                        combiner,
                        column: sums,
                        counts: Some(counts),
                    }
                } else {
                    let column = self.partial(key.clone(), Expr::Function(function.clone()));
                    FinalAggregate {
                        combiner,
                        column,
                        counts: None,
                    }
                };
                self.aggregates.push(entry);
                self.seen.push(key);
                self.seen.len().saturating_sub(1)
            }
        };
        Ok(Expr::Identifier(Ident::new(format!(
            "{MARKER}agg_{marker}"
        ))))
    }
}

impl VisitorMut for Rewriter<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if self.failure.is_some() {
            return ControlFlow::Break(());
        }
        // A group key, however deep, becomes a reference to its column.
        let text = canonical(expr);
        if let Some(index) = self.keys.iter().position(|key| *key == text) {
            *expr = Self::column(index);
            return ControlFlow::Continue(());
        }
        if let Expr::Function(function) = expr
            && (self.aggregates_known.aggregates(function)
                || (function.over.is_some()
                    && function_name(function)
                        .is_some_and(|name| self.aggregates_known.contains(&name))))
        {
            match self.aggregate(function) {
                Ok(marker) => *expr = marker,
                Err(error) => {
                    self.failure = Some(error);
                    return ControlFlow::Break(());
                }
            }
        }
        ControlFlow::Continue(())
    }
}

/// Each GROUP BY key as an expression a select list can carry: a position
/// or an output alias in GROUP BY names a select item.
fn group_keys(select: &Select, aliases: &[String]) -> Result<Vec<Expr>, Unsupported> {
    let group_exprs: &[Expr] = match &select.group_by {
        GroupByExpr::All(_) => return unsupported("GROUP BY ALL is not PostgreSQL"),
        GroupByExpr::Expressions(expressions, modifiers) => {
            if !modifiers.is_empty() {
                return unsupported(
                    "ROLLUP, CUBE and GROUPING SETS cannot be combined across regions",
                );
            }
            expressions
        }
    };
    let mut keys = Vec::with_capacity(group_exprs.len());
    for expr in group_exprs {
        if matches!(
            expr,
            Expr::Rollup(_) | Expr::Cube(_) | Expr::GroupingSets(_)
        ) {
            return unsupported("ROLLUP, CUBE and GROUPING SETS cannot be combined across regions");
        }
        let resolved = if let Some(position) = ordinal(expr) {
            select
                .projection
                .get(position)
                .and_then(item_expr)
                .cloned()
                .ok_or_else(|| Unsupported("GROUP BY position is out of range".to_owned()))?
        } else if let Some(name) = bare_name(expr).filter(|name| aliases.contains(name)) {
            select
                .projection
                .iter()
                .find(|item| item_alias(item).is_some_and(|alias| alias == name))
                .and_then(item_expr)
                .cloned()
                .unwrap_or_else(|| expr.clone())
        } else {
            expr.clone()
        };
        keys.push(resolved);
    }
    Ok(keys)
}

fn plan_aggregate(mut query: Query, aggregates: &Aggregates) -> Result<CombinePlan, Unsupported> {
    if !query.locks.is_empty() {
        return unsupported("FOR UPDATE cannot be used with aggregates");
    }
    let SetExpr::Select(select) = query.body.as_mut() else {
        return unsupported("this kind of query cannot be combined across regions");
    };
    if select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
        )
    }) {
        return unsupported("SELECT * with aggregates cannot be combined across regions");
    }

    let aliases = output_aliases(select);
    let keys = group_keys(select, &aliases)?;

    let mut rewriter = Rewriter {
        aggregates_known: aggregates,
        keys: keys.iter().map(canonical).collect(),
        partials: Vec::new(),
        aggregates: Vec::new(),
        seen: Vec::new(),
        failure: None,
    };

    // The final select list: every item computed from the partial columns,
    // named as PostgreSQL would have named the original.
    let mut final_items = Vec::new();
    for item in &select.projection {
        let Some(expr) = item_expr(item) else {
            return unsupported("this select list cannot be combined across regions");
        };
        let name = item_alias(item).unwrap_or_else(|| column_name(expr));
        let rewritten = rewriter.rewrite(expr)?;
        final_items.push(format!("{rewritten} AS {}", quote_ident(&name)));
    }

    let having = select
        .having
        .as_ref()
        .map(|having| rewriter.rewrite(having))
        .transpose()?;

    let distinct = match &select.distinct {
        None | Some(Distinct::All) => String::new(),
        Some(Distinct::Distinct) => "DISTINCT ".to_owned(),
        Some(Distinct::On(on)) => {
            let mut rewritten = Vec::new();
            for expr in on {
                rewritten.push(rewriter.rewrite(expr)?.to_string());
            }
            format!("DISTINCT ON ({}) ", rewritten.join(", "))
        }
    };

    // ORDER BY a position or an output name refers to the final output,
    // which has the same positions and names; anything else is rewritten.
    let mut order = Vec::new();
    let terms = match query.order_by.take() {
        None => Vec::new(),
        Some(OrderBy {
            kind: OrderByKind::Expressions(terms),
            ..
        }) => terms,
        Some(_) => return unsupported("ORDER BY ALL is not PostgreSQL"),
    };
    for mut term in terms {
        let refers_to_output = ordinal(&term.expr).is_some()
            || bare_name(&term.expr).is_some_and(|name| aliases.contains(&name));
        if !refers_to_output {
            term.expr = rewriter.rewrite(&term.expr)?;
        }
        order.push(term.to_string());
    }

    // The limits apply to the final result only: a region cannot know
    // which of its groups will make the cut.
    let limit = query.limit_clause.take().map(|clause| clause.to_string());
    let fetch = query.fetch.take().map(|fetch| fetch.to_string());

    // What every region runs: the group keys, then the partials, grouped as
    // the original was, with nothing applied after grouping.
    let mut region_items: Vec<SelectItem> =
        keys.iter().cloned().map(SelectItem::UnnamedExpr).collect();
    region_items.extend(
        rewriter
            .partials
            .iter()
            .map(|partial| SelectItem::UnnamedExpr(partial.expr.clone())),
    );
    if region_items.is_empty() {
        return unsupported("the statement selects nothing that can be combined");
    }
    select.projection = region_items;
    select.having = None;
    select.distinct = None;
    let region_sql = query.to_string();

    let columns = keys.len().saturating_add(rewriter.partials.len());
    let rest = final_clauses(keys.len(), having.as_ref(), &order, limit, fetch);
    Ok(CombinePlan {
        region_sql,
        finish: Finish::Aggregate(AggregateFinish {
            columns,
            select: format!("SELECT {distinct}{}", final_items.join(", ")),
            rest,
            aggregates: rewriter.aggregates,
        }),
    })
}

/// Everything after `FROM` in the final statement: regrouping on the key
/// columns, then the original HAVING, ORDER BY and limits, applied once.
fn final_clauses(
    keys: usize,
    having: Option<&Expr>,
    order: &[String],
    limit: Option<String>,
    fetch: Option<String>,
) -> String {
    let mut rest = String::new();
    if keys > 0 {
        let grouping: Vec<String> = (1..=keys).map(|n| format!("p.c{n}")).collect();
        let _ = write!(rest, " GROUP BY {}", grouping.join(", "));
    }
    if let Some(having) = having {
        let _ = write!(rest, " HAVING {having}");
    }
    if !order.is_empty() {
        let _ = write!(rest, " ORDER BY {}", order.join(", "));
    }
    if let Some(limit) = limit {
        rest.push_str(&limit);
    }
    if let Some(fetch) = fetch {
        let _ = write!(rest, " {fetch}");
    }
    rest
}

/// Refuses an expression that still reads a column of the original table
/// after rewriting: it is neither grouped nor inside an aggregate, so the
/// regions' partial results do not carry it.
fn unresolved(expr: &Expr) -> Result<(), Unsupported> {
    let mut stray = None;
    let _ = visit_expressions(expr, |node| {
        match node {
            Expr::Identifier(ident) if !ident.value.starts_with(MARKER) => {
                stray = Some(ident.value.clone());
                return ControlFlow::Break(());
            }
            Expr::CompoundIdentifier(parts) => {
                let ours = parts.len() == 2
                    && parts.first().is_some_and(|part| part.value == "p")
                    && parts.get(1).is_some_and(|part| part.value.starts_with('c'));
                if !ours {
                    stray = Some(
                        parts
                            .iter()
                            .map(|part| part.value.clone())
                            .collect::<Vec<_>>()
                            .join("."),
                    );
                    return ControlFlow::Break(());
                }
            }
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
                stray = Some("a subquery".to_owned());
                return ControlFlow::Break(());
            }
            _ => {}
        }
        ControlFlow::Continue(())
    });
    match stray {
        Some(name) => unsupported(format!(
            "{name} is used outside an aggregate without being a GROUP BY expression, so the \
             per-region results do not carry it"
        )),
        None => Ok(()),
    }
}

// ------------------------------------------------------------- finishing

impl AggregateFinish {
    /// The final statement over the partial columns, given each column's
    /// SQL type, reading from `source`. Refuses an aggregate that cannot be
    /// finished exactly over the type its partials arrived as.
    pub fn query(&self, types: &[String], source: &str) -> Result<String, Unsupported> {
        let mut select = self.select.clone();
        let mut rest = self.rest.clone();
        for (index, aggregate) in self.aggregates.iter().enumerate() {
            let column = |position: usize| format!("p.c{}", position.saturating_add(1));
            let type_of = |position: usize| {
                types
                    .get(position)
                    .cloned()
                    .ok_or_else(|| Unsupported("a region returned too few columns".to_owned()))
            };
            let column_sql = column(aggregate.column);
            let sql = match aggregate.combiner {
                Combiner::Count => format!("sum({column_sql})::int8"),
                // sum() of an int8 is numeric; the original sum of an int4
                // was int8, so the type it arrived as is the type it keeps.
                Combiner::Sum => format!("sum({column_sql})::{}", type_of(aggregate.column)?),
                Combiner::Min => format!("min({column_sql})"),
                Combiner::Max => format!("max({column_sql})"),
                Combiner::BoolAnd => format!("bool_and({column_sql})"),
                Combiner::BoolOr => format!("bool_or({column_sql})"),
                Combiner::BitAnd => format!("bit_and({column_sql})"),
                Combiner::BitOr => format!("bit_or({column_sql})"),
                Combiner::Avg => {
                    let counts = column(aggregate.counts.unwrap_or(aggregate.column));
                    match type_of(aggregate.column)?.as_str() {
                        // avg of an integer or numeric is a numeric
                        // division of the total by the count, exactly as
                        // PostgreSQL's own avg finishes.
                        "int8" | "numeric" => {
                            format!("(sum({column_sql})::numeric / nullif(sum({counts}), 0))")
                        }
                        "float8" | "interval" => {
                            format!("(sum({column_sql}) / nullif(sum({counts}), 0)::float8)")
                        }
                        other => {
                            return unsupported(format!(
                                "avg() over {other} cannot be combined exactly across regions; \
                                 cast its argument to float8 or numeric"
                            ));
                        }
                    }
                }
            };
            // HAVING and ORDER BY may use an aggregate too. The markers
            // are names this module generated and the original text was
            // checked not to contain, so replacing them is exact.
            let marker = format!("{MARKER}agg_{index}");
            select = select.replace(&marker, &sql);
            rest = rest.replace(&marker, &sql);
        }
        Ok(format!("{select} FROM {source}{rest}"))
    }
}

impl RowsFinish {
    /// Resolves the sort to column positions in the regions' results, once
    /// their column names are known. `visible` is how many columns the
    /// client sees; hidden sort columns follow them.
    pub fn resolve(&self, names: &[String]) -> Result<Vec<(usize, bool, bool)>, Unsupported> {
        let visible = names.len().saturating_sub(self.hidden);
        self.order
            .iter()
            .map(|term| {
                let column = match &term.on {
                    OrderRef::Column(position) if *position < visible => *position,
                    OrderRef::Column(_) => {
                        return unsupported("ORDER BY position is out of range");
                    }
                    OrderRef::Name(name) => names
                        .iter()
                        .take(visible)
                        .position(|candidate| candidate == name)
                        .ok_or_else(|| {
                            Unsupported(format!("ORDER BY {name} is not an output column"))
                        })?,
                    OrderRef::Hidden(index) => visible.saturating_add(*index),
                };
                Ok((column, term.descending, term.nulls_first()))
            })
            .collect()
    }

    /// The final statement for rows that cannot be merged by the proxy.
    pub fn query(&self, names: &[String], source: &str) -> Result<String, Unsupported> {
        let visible = names.len().saturating_sub(self.hidden);
        let columns: Vec<String> = names
            .iter()
            .take(visible)
            .enumerate()
            .map(|(index, name)| format!("p.c{} AS {}", index.saturating_add(1), quote_ident(name)))
            .collect();
        let mut sql = format!(
            "SELECT {}{} FROM {source}",
            if self.distinct { "DISTINCT " } else { "" },
            columns.join(", ")
        );
        let order = self.resolve(names)?;
        if !order.is_empty() {
            let terms: Vec<String> = order
                .iter()
                .map(|(column, descending, nulls_first)| {
                    format!(
                        "p.c{}{}{}",
                        column.saturating_add(1),
                        if *descending { " DESC" } else { " ASC" },
                        if *nulls_first {
                            " NULLS FIRST"
                        } else {
                            " NULLS LAST"
                        }
                    )
                })
                .collect();
            let _ = write!(sql, " ORDER BY {}", terms.join(", "));
        }
        if let Some(limit) = self.limit {
            let _ = write!(sql, " LIMIT {limit}");
        }
        if self.offset > 0 {
            let _ = write!(sql, " OFFSET {}", self.offset);
        }
        Ok(sql)
    }
}

/// `unnest(...) AS p(c1, ...)` over one array per column, each column's
/// values given as text and cast to its type.
#[must_use]
pub fn source(columns: &[(String, Vec<Option<String>>)]) -> String {
    let arrays: Vec<String> = columns
        .iter()
        .map(|(type_name, values)| format!("{}::{type_name}[]", array_literal(values)))
        .collect();
    let names: Vec<String> = (1..=columns.len()).map(|n| format!("c{n}")).collect();
    format!("unnest({}) AS p({})", arrays.join(", "), names.join(", "))
}

/// An array literal of text values, as an escape string so that it means
/// the same whatever `standard_conforming_strings` is set to.
fn array_literal(values: &[Option<String>]) -> String {
    let mut out = String::from("E'{");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        match value {
            None => out.push_str("NULL"),
            Some(value) => {
                out.push('"');
                for character in value.chars() {
                    match character {
                        // Escaped once for the array, then each backslash
                        // again for the escape string.
                        '\\' => out.push_str("\\\\\\\\"),
                        '"' => out.push_str("\\\\\""),
                        '\'' => out.push_str("''"),
                        other => out.push(other),
                    }
                }
                out.push('"');
            }
        }
    }
    out.push_str("}'");
    out
}

/// The SQL name of a built-in type, for the types a combined result may
/// carry. Arrays and composites are left out: `unnest` would flatten them.
#[must_use]
pub fn type_name(oid: i32) -> Option<&'static str> {
    Some(match oid {
        16 => "bool",
        17 => "bytea",
        18 => "\"char\"",
        19 => "name",
        20 => "int8",
        21 => "int2",
        23 => "int4",
        25 => "text",
        26 => "oid",
        114 => "json",
        650 => "cidr",
        700 => "float4",
        701 => "float8",
        790 => "money",
        829 => "macaddr",
        869 => "inet",
        1042 => "bpchar",
        1043 => "varchar",
        1082 => "date",
        1083 => "time",
        1114 => "timestamp",
        1184 => "timestamptz",
        1186 => "interval",
        1266 => "timetz",
        1560 => "bit",
        1562 => "varbit",
        1700 => "numeric",
        2950 => "uuid",
        3802 => "jsonb",
        _ => return None,
    })
}

// --------------------------------------------------------------- helpers

/// Folds an identifier as PostgreSQL does.
fn fold(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

/// An expression's text with unquoted identifiers folded, so two spellings
/// of the same expression compare equal.
fn canonical(expr: &Expr) -> String {
    let mut copy = expr.clone();
    let _ = sqlparser::ast::visit_expressions_mut(&mut copy, |node| {
        match node {
            Expr::Identifier(ident) if ident.quote_style.is_none() => {
                ident.value = ident.value.to_lowercase();
            }
            Expr::CompoundIdentifier(parts) => {
                for part in parts.iter_mut().filter(|part| part.quote_style.is_none()) {
                    part.value = part.value.to_lowercase();
                }
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
    copy.to_string()
}

fn item_expr(item: &SelectItem) -> Option<&Expr> {
    match item {
        SelectItem::UnnamedExpr(expr)
        | SelectItem::ExprWithAlias { expr, .. }
        | SelectItem::ExprWithAliases { expr, .. } => Some(expr),
        _ => None,
    }
}

fn item_alias(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => Some(fold(alias)),
        _ => None,
    }
}

fn output_aliases(select: &Select) -> Vec<String> {
    select.projection.iter().filter_map(item_alias).collect()
}

/// `ORDER BY 2` and `GROUP BY 2` name a select item by position.
fn ordinal(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::Number(text, _) => text.parse::<usize>().ok()?.checked_sub(1),
            _ => None,
        },
        _ => None,
    }
}

fn bare_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(fold(ident)),
        _ => None,
    }
}

/// The name PostgreSQL gives an unaliased select item: a column's name, a
/// function's name, a cast's inner name or else its type, and `?column?`
/// for anything else.
fn column_name(expr: &Expr) -> String {
    figure(expr).unwrap_or_else(|| "?column?".to_owned())
}

fn figure(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(fold(ident)),
        Expr::CompoundIdentifier(parts) => parts.last().map(fold),
        Expr::Function(function) => function_name(function),
        Expr::Nested(inner) => figure(inner),
        Expr::Cast {
            expr, data_type, ..
        } => figure(expr).or_else(|| Some(internal_type_name(&data_type.to_string()))),
        Expr::Case { .. } => Some("case".to_owned()),
        _ => None,
    }
}

/// The name PostgreSQL uses internally for a type spelled in SQL, which is
/// what it names a column that is just a cast.
fn internal_type_name(spelled: &str) -> String {
    let lower = spelled.to_lowercase();
    let base = lower.split('(').next().unwrap_or(&lower).trim();
    match base {
        "int" | "integer" | "int4" => "int4",
        "bigint" | "int8" => "int8",
        "smallint" | "int2" => "int2",
        "real" | "float4" => "float4",
        "double precision" | "float8" | "float" => "float8",
        "boolean" | "bool" => "bool",
        "character varying" | "varchar" => "varchar",
        "character" | "char" => "bpchar",
        "decimal" | "numeric" => "numeric",
        "timestamp with time zone" | "timestamptz" => "timestamptz",
        "timestamp without time zone" | "timestamp" => "timestamp",
        "time with time zone" | "timetz" => "timetz",
        "time without time zone" | "time" => "time",
        other => other,
    }
    .to_owned()
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests;
