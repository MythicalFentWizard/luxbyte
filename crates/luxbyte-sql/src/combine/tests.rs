use super::*;

fn plan(sql: &str) -> Result<CombinePlan, Unsupported> {
    super::plan(sql, &Aggregates::default())
}

fn rows(sql: &str) -> (String, RowsFinish) {
    match plan(sql) {
        Ok(CombinePlan {
            region_sql,
            finish: Finish::Rows(finish),
        }) => (region_sql, finish),
        other => panic!("expected rows for {sql}: {other:?}"),
    }
}

fn aggregate(sql: &str) -> (String, AggregateFinish) {
    match plan(sql) {
        Ok(CombinePlan {
            region_sql,
            finish: Finish::Aggregate(finish),
        }) => (region_sql, finish),
        other => panic!("expected aggregates for {sql}: {other:?}"),
    }
}

fn refused(sql: &str) -> String {
    match plan(sql) {
        Err(Unsupported(reason)) => reason,
        Ok(plan) => panic!("expected {sql} to be refused, got {plan:?}"),
    }
}

fn types(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

// ------------------------------------------------------------------ rows

#[test]
fn each_region_is_asked_for_limit_plus_offset_and_the_rest_is_done_once() {
    let (region, finish) = rows("SELECT id FROM users ORDER BY id LIMIT 10 OFFSET 5");
    assert_eq!(region, "SELECT id FROM users ORDER BY id LIMIT 15");
    assert_eq!(finish.limit, Some(10));
    assert_eq!(finish.offset, 5);
}

#[test]
fn an_offset_without_a_limit_is_removed_from_the_regions() {
    let (region, finish) = rows("SELECT id FROM users ORDER BY id OFFSET 5");
    assert_eq!(region, "SELECT id FROM users ORDER BY id");
    assert_eq!((finish.limit, finish.offset), (None, 5));
}

#[test]
fn fetch_first_is_a_limit() {
    let (region, finish) = rows("SELECT id FROM users ORDER BY id FETCH FIRST 3 ROWS ONLY");
    assert_eq!(region, "SELECT id FROM users ORDER BY id LIMIT 3");
    assert_eq!(finish.limit, Some(3));
}

#[test]
fn a_sort_on_something_not_selected_travels_as_a_hidden_column() {
    let (region, finish) = rows("SELECT id FROM users ORDER BY lower(name) DESC NULLS LAST");
    assert_eq!(
        region,
        "SELECT id, lower(name) AS \"__luxbyte_sort_0\" FROM users ORDER BY lower(name) DESC NULLS LAST"
    );
    assert_eq!(finish.hidden, 1);
    let names = types(&["id", "__luxbyte_sort_0"]);
    assert_eq!(finish.resolve(&names), Ok(vec![(1, true, false)]));
}

#[test]
fn positions_and_aliases_sort_on_the_visible_columns() {
    let (_, finish) = rows("SELECT id, name AS who FROM users ORDER BY 2, who DESC");
    let names = types(&["id", "who"]);
    assert_eq!(
        finish.resolve(&names),
        Ok(vec![(1, false, false), (1, true, true)])
    );
}

#[test]
fn distinct_is_redone_across_regions() {
    let (region, finish) = rows("SELECT DISTINCT country FROM users ORDER BY country");
    assert_eq!(
        region,
        "SELECT DISTINCT country FROM users ORDER BY country"
    );
    assert!(finish.distinct);
    assert_eq!(
        finish.order.first().map(|term| &term.on),
        Some(&OrderRef::Column(0))
    );
}

#[test]
fn union_deduplicates_across_regions_and_union_all_does_not() {
    assert!(
        rows("SELECT id FROM users UNION SELECT id FROM orders")
            .1
            .distinct
    );
    assert!(
        !rows("SELECT id FROM users UNION ALL SELECT id FROM orders")
            .1
            .distinct
    );
    assert!(refused("SELECT 1 UNION SELECT 2 UNION ALL SELECT 3").contains("mixed"));
}

#[test]
fn the_final_rows_query_sorts_dedupes_and_limits_in_postgres() {
    let (_, finish) = rows("SELECT DISTINCT name FROM users ORDER BY name DESC LIMIT 2 OFFSET 1");
    let sql = finish
        .query(&types(&["name"]), "unnest(E'{}'::text[]) AS p(c1)")
        .expect("query");
    assert_eq!(
        sql,
        "SELECT DISTINCT p.c1 AS \"name\" FROM unnest(E'{}'::text[]) AS p(c1) ORDER BY p.c1 DESC \
         NULLS FIRST LIMIT 2 OFFSET 1"
    );
}

#[test]
fn what_rows_cannot_be_combined_is_refused_with_a_reason() {
    assert!(refused("SELECT id FROM users INTERSECT SELECT id FROM orders").contains("INTERSECT"));
    assert!(refused("SELECT id FROM users EXCEPT SELECT id FROM orders").contains("EXCEPT"));
    assert!(refused("SELECT row_number() OVER () FROM users").contains("window"));
    assert!(refused("SELECT DISTINCT ON (a) a, b FROM t ORDER BY a, b").contains("DISTINCT ON"));
    assert!(refused("SELECT id FROM users LIMIT $1").contains("constant"));
    assert!(
        refused("SELECT id FROM users ORDER BY id FETCH FIRST 1 ROW WITH TIES").contains("TIES")
    );
}

// ------------------------------------------------------------ aggregates

#[test]
fn a_count_is_the_sum_of_the_counts() {
    let (region, finish) = aggregate("SELECT count(*) FROM users");
    assert_eq!(region, "SELECT count(*) FROM users");
    let sql = finish.query(&types(&["int8"]), "src").expect("query");
    assert_eq!(sql, "SELECT sum(p.c1)::int8 AS \"count\" FROM src");
}

#[test]
fn groups_are_regrouped_and_having_order_and_limit_apply_once() {
    let (region, finish) = aggregate(
        "SELECT country, count(*), avg(age) FROM users GROUP BY country HAVING count(*) > 10 \
         ORDER BY 2 DESC LIMIT 5",
    );
    // No HAVING, ORDER BY or LIMIT in the regions: a region cannot know
    // which of its groups will survive them.
    assert_eq!(
        region,
        "SELECT country, count(*), sum(age), count(age) FROM users GROUP BY country"
    );
    let sql = finish
        .query(&types(&["text", "int8", "int8", "int8"]), "src")
        .expect("query");
    assert_eq!(
        sql,
        "SELECT p.c1 AS \"country\", sum(p.c2)::int8 AS \"count\", (sum(p.c3)::numeric / \
         nullif(sum(p.c4), 0)) AS \"avg\" FROM src GROUP BY p.c1 HAVING sum(p.c2)::int8 > 10 \
         ORDER BY 2 DESC LIMIT 5"
    );
}

#[test]
fn an_aggregate_used_twice_is_computed_once() {
    let (region, finish) = aggregate("SELECT sum(x), sum(x) * 2 FROM t HAVING sum(x) > 0");
    assert_eq!(region, "SELECT sum(x) FROM t");
    assert_eq!(finish.aggregates.len(), 1);
}

#[test]
fn group_keys_by_position_or_alias_and_expressions_over_them_work() {
    let (region, finish) = aggregate(
        "SELECT upper(country) AS c, sum(age) FILTER (WHERE age > 1) AS total FROM users u \
         GROUP BY country ORDER BY c",
    );
    assert_eq!(
        region,
        "SELECT country, sum(age) FILTER (WHERE age > 1) FROM users u GROUP BY country"
    );
    let sql = finish
        .query(&types(&["text", "int8"]), "src")
        .expect("query");
    assert_eq!(
        sql,
        "SELECT upper(p.c1) AS \"c\", sum(p.c2)::int8 AS \"total\" FROM src GROUP BY p.c1 \
         ORDER BY c"
    );

    let (region, _) = aggregate("SELECT date_trunc('day', ts) AS day, count(*) FROM e GROUP BY 1");
    assert_eq!(
        region,
        "SELECT date_trunc('day', ts), count(*) FROM e GROUP BY 1"
    );
    let (region, _) =
        aggregate("SELECT date_trunc('day', ts) AS day, count(*) FROM e GROUP BY day");
    assert_eq!(
        region,
        "SELECT date_trunc('day', ts), count(*) FROM e GROUP BY day"
    );
}

#[test]
fn avg_finishes_exactly_for_each_kind_of_partial_sum() {
    let (_, finish) = aggregate("SELECT avg(x) FROM t");
    for (sum_type, expected) in [
        ("int8", "(sum(p.c1)::numeric / nullif(sum(p.c2), 0))"),
        ("numeric", "(sum(p.c1)::numeric / nullif(sum(p.c2), 0))"),
        ("float8", "(sum(p.c1) / nullif(sum(p.c2), 0)::float8)"),
    ] {
        let sql = finish
            .query(&types(&[sum_type, "int8"]), "src")
            .expect("query");
        assert!(sql.contains(expected), "{sql}");
    }
    // sum(float4) accumulates in float4 where avg(float4) uses float8, so
    // the combined answer would differ in its last digits.
    assert!(finish.query(&types(&["float4", "int8"]), "src").is_err());
}

#[test]
fn a_sum_keeps_the_type_the_regions_returned() {
    let (_, finish) = aggregate("SELECT sum(x) FROM t");
    let sql = finish.query(&types(&["int8"]), "src").expect("query");
    // sum() over int8 partials is numeric; the original sum of an int4
    // was int8, and so is this.
    assert!(sql.starts_with("SELECT sum(p.c1)::int8"), "{sql}");
}

#[test]
fn what_aggregates_cannot_be_combined_is_refused_with_a_reason() {
    assert!(refused("SELECT count(DISTINCT country) FROM users").contains("DISTINCT"));
    assert!(refused("SELECT string_agg(name, ',') FROM users").contains("string_agg"));
    assert!(refused("SELECT stddev(age) FROM users").contains("stddev"));
    assert!(
        refused("SELECT country, count(*) FROM users GROUP BY ROLLUP (country)").contains("ROLLUP")
    );
    assert!(refused("SELECT * FROM users GROUP BY id").contains("SELECT *"));
    // A column neither grouped nor aggregated is not in the partial results.
    assert!(refused("SELECT country, name, count(*) FROM users GROUP BY country").contains("name"));
    assert!(
        refused("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY age) FROM users")
            .contains("percentile_cont")
    );
}

#[test]
fn an_aggregate_the_database_lists_is_never_mistaken_for_a_column() {
    // Without the catalog, my_median looks like any scalar function, and
    // each region's median would come back as a separate row.
    let known = Aggregates::with_names(["my_median".to_owned()]);
    let error = super::plan("SELECT my_median(age) FROM users", &known).expect_err("refused");
    assert!(error.0.contains("my_median"), "{error}");
    // An ordered-set aggregate is one whatever it is called.
    assert!(
        refused("SELECT anything(0.5) WITHIN GROUP (ORDER BY age) FROM users").contains("anything")
    );
}

#[test]
fn text_reserved_for_generated_names_is_refused() {
    assert!(refused("SELECT count(*) AS __luxbyte_agg_0 FROM users").contains("reserves"));
}

#[test]
fn unaliased_columns_are_named_as_postgres_names_them() {
    assert_eq!(column_name(&parse_expr("count(*)")), "count");
    assert_eq!(column_name(&parse_expr("u.name")), "name");
    assert_eq!(column_name(&parse_expr("sum(x)::numeric(10, 2)")), "sum");
    assert_eq!(column_name(&parse_expr("'a'::integer")), "int4");
    assert_eq!(column_name(&parse_expr("sum(x) / count(*)")), "?column?");
    assert_eq!(column_name(&parse_expr("CASE WHEN a THEN 1 END")), "case");
}

fn parse_expr(text: &str) -> Expr {
    sqlparser::parser::Parser::new(&PostgreSqlDialect {})
        .try_with_sql(text)
        .and_then(|mut parser| parser.parse_expr())
        .expect("expression")
}

// ------------------------------------------------------------- carrying

#[test]
fn values_survive_the_array_literal_whatever_they_contain() {
    let sql = source(&[(
        "text".to_owned(),
        vec![
            Some("it's".to_owned()),
            Some(r#"quote " and \ slash"#.to_owned()),
            Some("NULL".to_owned()),
            None,
        ],
    )]);
    assert_eq!(
        sql,
        r#"unnest(E'{"it''s","quote \\" and \\\\ slash","NULL",NULL}'::text[]) AS p(c1)"#
    );
}

#[test]
fn only_types_with_a_flat_text_form_are_carried() {
    assert_eq!(type_name(1700), Some("numeric"));
    assert_eq!(type_name(1007), None); // int4[]: unnest would flatten it
}
