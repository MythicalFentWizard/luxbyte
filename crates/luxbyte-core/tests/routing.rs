//! End-to-end routing decisions: SQL text in, plan out.
//!
//! These are the cases that decide whether the proxy is correct, and none of
//! them needs a database to check.

// A failed assertion in a test is supposed to abort the test; the workspace
// bans these in library code, where a panic would take a proxy down.
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::sync::Arc;

use luxbyte_core::{MemoryDirectory, ParameterValues, Plan, Router, Routing, Topology};
use luxbyte_sql::{Access, MergeStrategy};

const TOPOLOGY: &str = r#"
home_region = "eu-west"

[[regions]]
id = "eu-west"
primary = { host = "eu" }

[[regions]]
id = "us-east"
primary = { host = "us" }

[[regions]]
id = "ap-south"
primary = { host = "ap" }

[[tables]]
table = "users"
placement = "geo_partitioned"
key = "tenant_id"

[[tables]]
table = "orders"
placement = "geo_partitioned"
key = "tenant_id"

[[tables]]
table = "currencies"
placement = "global_replicated"

[[tables]]
table = "eu_audit"
placement = "pinned"
region = "eu-west"
"#;

/// A router over three regions, with `alice` in Europe and `bob` in the US.
fn router() -> Router {
    let directory = MemoryDirectory::with_entries([
        ("users", "alice", "eu-west"),
        ("users", "bob", "us-east"),
        ("orders", "alice", "eu-west"),
        ("orders", "bob", "us-east"),
    ]);
    router_with(directory)
}

fn router_with(directory: MemoryDirectory) -> Router {
    let topology = Topology::from_toml(TOPOLOGY).expect("topology should parse");
    Router::new(topology, Arc::new(directory))
}

fn plan_with(router: &Router, sql: &str, parameters: Option<&ParameterValues>) -> Plan {
    let mut analyses = luxbyte_sql::analyze(sql).expect("sql should parse");
    assert_eq!(analyses.len(), 1, "expected a single statement");
    router.plan(&analyses.remove(0), parameters)
}

fn plan(sql: &str) -> Plan {
    plan_with(&router(), sql, None)
}

fn single(sql: &str) -> String {
    match plan(sql) {
        Plan::Single { region, .. } => region,
        other => panic!("expected a single-region plan, got {other:?}"),
    }
}

// ---------------------------------------------------------------- point reads

#[test]
fn a_point_read_goes_to_the_region_that_holds_the_key() {
    assert_eq!(
        single("SELECT * FROM users WHERE tenant_id = 'alice'"),
        "eu-west"
    );
    assert_eq!(
        single("SELECT * FROM users WHERE tenant_id = 'bob'"),
        "us-east"
    );
}

#[test]
fn a_point_read_is_served_from_a_replica() {
    let Plan::Single { access, .. } = plan("SELECT * FROM users WHERE tenant_id = 'alice'") else {
        panic!("expected a single-region plan");
    };
    assert_eq!(access, Access::ReadOnly);
}

#[test]
fn a_locking_read_demands_a_primary() {
    let Plan::Single { access, .. } =
        plan("SELECT * FROM users WHERE tenant_id = 'alice' FOR UPDATE")
    else {
        panic!("expected a single-region plan");
    };
    assert_eq!(access, Access::ReadWrite);
}

#[test]
fn a_statement_with_no_tables_stays_home() {
    assert_eq!(single("SELECT 1"), "eu-west");
}

#[test]
fn catalog_queries_stay_home_instead_of_fanning_out() {
    // A client library fires a stack of these before it will run anything
    // of yours. Fanning them out would put three continents in the path of
    // drawing a prompt.
    assert_eq!(single("SELECT * FROM pg_catalog.pg_type"), "eu-west");
    assert_eq!(single("SELECT * FROM pg_settings"), "eu-west");
    assert_eq!(
        single("SELECT * FROM information_schema.columns"),
        "eu-west"
    );
}

// ----------------------------------------------------------- scatter and gather

#[test]
fn a_read_without_a_key_fans_out_rather_than_failing() {
    let Plan::ScatterGather { regions, merge } = plan("SELECT * FROM users WHERE name = 'ada'")
    else {
        panic!("expected a scatter-gather plan");
    };
    assert_eq!(regions, ["ap-south", "eu-west", "us-east"]);
    assert_eq!(merge, MergeStrategy::Concatenate);
}

#[test]
fn a_read_of_a_key_nobody_has_seen_fans_out() {
    // The row may not exist, or the directory entry may be missing. Fanning
    // out gives the right answer either way; assuming it is local does not.
    let Plan::ScatterGather { .. } = plan("SELECT * FROM users WHERE tenant_id = 'nobody'") else {
        panic!("expected a scatter-gather plan");
    };
}

#[test]
fn an_in_list_spanning_regions_touches_only_those_regions() {
    let Plan::ScatterGather { regions, .. } =
        plan("SELECT * FROM users WHERE tenant_id IN ('alice', 'bob')")
    else {
        panic!("expected a scatter-gather plan");
    };
    // Not ap-south: no key in the list lives there.
    assert_eq!(regions, ["eu-west", "us-east"]);
}

#[test]
fn an_in_list_within_one_region_is_not_a_scatter() {
    let directory =
        MemoryDirectory::with_entries([("users", "alice", "eu-west"), ("users", "ada", "eu-west")]);
    let router = router_with(directory);
    let Plan::Single { region, .. } = plan_with(
        &router,
        "SELECT * FROM users WHERE tenant_id IN ('alice', 'ada')",
        None,
    ) else {
        panic!("expected a single-region plan");
    };
    assert_eq!(region, "eu-west");
}

#[test]
fn an_ordered_scatter_is_combined_rather_than_concatenated() {
    // Each region's order holds only within that region.
    let Plan::ScatterGather { merge, .. } =
        plan("SELECT * FROM users WHERE name = 'ada' ORDER BY created_at DESC")
    else {
        panic!("expected a scatter-gather plan");
    };
    assert_eq!(merge, MergeStrategy::Combine);
}

#[test]
fn an_aggregate_across_regions_is_combined_rather_than_concatenated() {
    // Concatenating three partial counts would return three rows, each
    // wrong; the counts are combined into one instead.
    let Plan::ScatterGather { regions, merge } = plan("SELECT count(*) FROM users") else {
        panic!("expected a scatter-gather plan");
    };
    assert_eq!(regions.len(), 3);
    assert_eq!(merge, MergeStrategy::Combine);
}

#[test]
fn an_aggregate_within_one_region_is_fine() {
    assert_eq!(
        single("SELECT count(*) FROM users WHERE tenant_id = 'alice'"),
        "eu-west"
    );
}

#[test]
fn distinct_across_regions_is_combined() {
    let Plan::ScatterGather { merge, .. } = plan("SELECT DISTINCT name FROM users") else {
        panic!("expected a scatter-gather plan");
    };
    assert_eq!(merge, MergeStrategy::Combine);
}

// --------------------------------------------------------------------- writes

#[test]
fn an_insert_goes_to_the_region_of_its_key() {
    assert_eq!(
        single("INSERT INTO users (tenant_id, name) VALUES ('bob', 'bob')"),
        "us-east"
    );
}

#[test]
fn an_insert_for_a_new_key_lands_in_the_home_region() {
    // The premise of the whole system: rows are created where their owner is.
    assert_eq!(
        single("INSERT INTO users (tenant_id, name) VALUES ('carol', 'carol')"),
        "eu-west"
    );
}

#[test]
fn an_insert_without_the_shard_key_is_refused_with_a_usable_message() {
    let Plan::Refuse(refusal) = plan("INSERT INTO users (name) VALUES ('ada')") else {
        panic!("expected a refusal");
    };
    assert_eq!(refusal.sqlstate, "23502");
    assert!(refusal.message.contains("tenant_id"));
}

#[test]
fn a_multi_region_insert_becomes_a_distributed_transaction() {
    // This is the case a proxy that refuses what it cannot pin down has to
    // reject outright.
    let Plan::Distributed { regions } =
        plan("INSERT INTO users (tenant_id, name) VALUES ('alice', 'a'), ('bob', 'b')")
    else {
        panic!("expected a distributed plan");
    };
    assert_eq!(regions, ["eu-west", "us-east"]);
}

#[test]
fn an_update_without_a_key_becomes_a_distributed_write_rather_than_an_error() {
    let Plan::Distributed { regions } = plan("UPDATE users SET name = 'x' WHERE name = 'ada'")
    else {
        panic!("expected a distributed plan");
    };
    assert_eq!(regions.len(), 3);
}

#[test]
fn assigning_to_the_shard_key_is_refused_as_a_relocation() {
    let Plan::Refuse(refusal) =
        plan("UPDATE users SET tenant_id = 'bob' WHERE tenant_id = 'alice'")
    else {
        panic!("expected a refusal");
    };
    assert!(refusal.message.contains("shard key"));
    assert!(
        refusal
            .hint
            .as_deref()
            .is_some_and(|hint| hint.contains("RELOCATE"))
    );
}

#[test]
fn a_write_to_a_globally_replicated_table_reaches_every_region() {
    let Plan::Distributed { regions } = plan("UPDATE currencies SET rate = 1.1 WHERE code = 'EUR'")
    else {
        panic!("expected a distributed plan");
    };
    assert_eq!(regions, ["ap-south", "eu-west", "us-east"]);
}

#[test]
fn a_read_of_a_globally_replicated_table_never_leaves_home() {
    assert_eq!(single("SELECT * FROM currencies"), "eu-west");
}

#[test]
fn a_pinned_table_is_reached_wherever_it_lives() {
    assert_eq!(single("SELECT * FROM eu_audit"), "eu-west");
    assert_eq!(
        single("INSERT INTO eu_audit (what) VALUES ('x')"),
        "eu-west"
    );
}

#[test]
fn ddl_reaches_every_region_atomically() {
    // Schema that differs between regions turns a later relocation into a
    // row that no longer fits its new home.
    let Plan::Distributed { regions } = plan("ALTER TABLE users ADD COLUMN nickname text") else {
        panic!("expected a distributed plan");
    };
    assert_eq!(regions.len(), 3);
}

// ----------------------------------------------------------------- relocation

fn moving_alice(phase: luxbyte_core::RelocationPhase) -> Router {
    let directory = MemoryDirectory::with_entries([("users", "alice", "eu-west")]);
    directory
        .begin_relocation("users", "alice", "us-east")
        .expect("relocation should start");
    if phase == luxbyte_core::RelocationPhase::Committing {
        directory
            .begin_committing("users", "alice")
            .expect("commit should start");
    }
    router_with(directory)
}

fn route(router: &Router, sql: &str) -> Routing {
    let parsed = luxbyte_sql::analyze(sql).expect("parse");
    let statement = parsed.first().expect("one statement");
    router.route(statement, Some(&ParameterValues::default()))
}

#[test]
fn reads_continue_from_the_source_during_cutover() {
    let router = moving_alice(luxbyte_core::RelocationPhase::Cutover);
    let Plan::Single { region, .. } = plan_with(
        &router,
        "SELECT * FROM users WHERE tenant_id = 'alice'",
        None,
    ) else {
        panic!("expected a single-region plan");
    };
    assert_eq!(region, "eu-west");
}

#[test]
fn a_write_during_cutover_is_held_and_refused_retryably_if_it_cannot_wait() {
    // `40001` is serialization_failure, which every client library already
    // retries. A statement that cannot wait for a move therefore costs a
    // correctly written application nothing, and no code change at all.
    let router = moving_alice(luxbyte_core::RelocationPhase::Cutover);
    let sql = "UPDATE users SET name = 'x' WHERE tenant_id = 'alice'";
    let Routing::Held(refusal) = route(&router, sql) else {
        panic!("expected the write to be held");
    };
    assert_eq!(refusal.sqlstate, "40001");
    assert!(refusal.retryable);
    let Plan::Refuse(refusal) = plan_with(&router, sql, None) else {
        panic!("expected a refusal from plan()");
    };
    assert_eq!(refusal.sqlstate, "40001");
}

#[test]
fn reads_are_held_while_a_move_commits() {
    let router = moving_alice(luxbyte_core::RelocationPhase::Committing);
    assert!(matches!(
        route(&router, "SELECT * FROM users WHERE tenant_id = 'alice'"),
        Routing::Held(_)
    ));
    // Other keys are not affected.
    assert!(matches!(
        route(&router, "SELECT * FROM users WHERE tenant_id = 'bob'"),
        Routing::Ready(_)
    ));
}

#[test]
fn a_write_to_every_key_waits_for_a_moving_key_but_a_read_of_every_key_waits_only_for_the_commit() {
    let router = moving_alice(luxbyte_core::RelocationPhase::Cutover);
    assert!(matches!(
        route(&router, "UPDATE users SET name = 'x'"),
        Routing::Held(_)
    ));
    assert!(matches!(
        route(&router, "SELECT count(*) FROM users"),
        Routing::Ready(_)
    ));
    let router = moving_alice(luxbyte_core::RelocationPhase::Committing);
    assert!(matches!(
        route(&router, "SELECT count(*) FROM users"),
        Routing::Held(_)
    ));
    // A table with no moving key carries on.
    assert!(matches!(
        route(&router, "UPDATE orders SET total = 0"),
        Routing::Ready(_)
    ));
}

#[test]
fn a_finished_relocation_sends_traffic_to_the_new_region() {
    let directory = MemoryDirectory::with_entries([("users", "alice", "eu-west")]);
    directory
        .begin_relocation("users", "alice", "us-east")
        .expect("relocation should start");
    directory
        .finish_relocation("users", "alice")
        .expect("relocation should finish");
    let router = router_with(directory);
    let Plan::Single { region, .. } = plan_with(
        &router,
        "SELECT * FROM users WHERE tenant_id = 'alice'",
        None,
    ) else {
        panic!("expected a single-region plan");
    };
    assert_eq!(region, "us-east");
}

// ---------------------------------------------------------- prepared statements

#[test]
fn a_placeholder_key_defers_routing_until_the_values_arrive() {
    let Plan::NeedsParameters { parameters } = plan("SELECT * FROM users WHERE tenant_id = $1")
    else {
        panic!("expected routing to be deferred");
    };
    assert_eq!(parameters, [1]);
}

#[test]
fn a_bound_placeholder_routes_as_precisely_as_a_literal() {
    let router = router();
    let bound = ParameterValues::new(vec![Some("bob".to_owned())]);
    let Plan::Single { region, .. } = plan_with(
        &router,
        "SELECT * FROM users WHERE tenant_id = $1",
        Some(&bound),
    ) else {
        panic!("expected a single-region plan");
    };
    assert_eq!(region, "us-east");
}

#[test]
fn a_placeholder_bound_to_null_fans_out_instead_of_guessing() {
    let router = router();
    let bound = ParameterValues::new(vec![None]);
    let Plan::ScatterGather { .. } = plan_with(
        &router,
        "SELECT * FROM users WHERE tenant_id = $1",
        Some(&bound),
    ) else {
        panic!("expected a scatter-gather plan");
    };
}

#[test]
fn parameters_are_indexed_from_one_like_the_wire() {
    let bound = ParameterValues::new(vec![Some("first".to_owned()), Some("second".to_owned())]);
    assert_eq!(bound.get(1), Some("first"));
    assert_eq!(bound.get(2), Some("second"));
    assert_eq!(bound.get(0), None);
    assert_eq!(bound.get(3), None);
}

// ----------------------------------------------------------------------- joins

#[test]
fn a_join_within_one_region_is_a_single_plan() {
    assert_eq!(
        single(
            "SELECT * FROM users u JOIN orders o ON o.tenant_id = u.tenant_id \
             WHERE u.tenant_id = 'alice' AND o.tenant_id = 'alice'"
        ),
        "eu-west"
    );
}

#[test]
fn a_join_across_regions_is_refused_rather_than_silently_losing_rows() {
    // Running this separately in each region would drop exactly the rows
    // that match across the boundary, and return a plausible-looking result.
    let Plan::Refuse(refusal) = plan(
        "SELECT * FROM users u JOIN orders o ON o.buyer = u.id \
         WHERE u.tenant_id = 'alice' AND o.tenant_id = 'bob'",
    ) else {
        panic!("expected a refusal");
    };
    assert_eq!(refusal.sqlstate, "0A000");
    assert!(refusal.message.contains("join"));
}

#[test]
fn joining_a_partitioned_table_to_a_global_one_stays_in_one_region() {
    assert_eq!(
        single(
            "SELECT * FROM orders o JOIN currencies c ON c.code = o.currency \
             WHERE o.tenant_id = 'alice'"
        ),
        "eu-west"
    );
}

// ------------------------------------------------ joins on the shard key

#[test]
fn a_join_on_the_shard_key_runs_in_every_region_at_once() {
    // Rows only meet where their keys are equal, so each region has every
    // pair it returns, and no pair comes back from two regions.
    for sql in [
        "SELECT * FROM users u JOIN orders o ON o.tenant_id = u.tenant_id",
        "SELECT * FROM users u WHERE EXISTS \
         (SELECT 1 FROM orders o WHERE o.tenant_id = u.tenant_id AND o.total > 100)",
        "SELECT u.name, (SELECT count(*) FROM orders o WHERE o.tenant_id = u.tenant_id) \
         FROM users u",
        "SELECT * FROM orders o JOIN currencies c ON c.code = o.currency",
    ] {
        let Plan::ScatterGather { regions, .. } = plan(sql) else {
            panic!("expected a scatter, got {:?} for {sql}", plan(sql));
        };
        assert_eq!(regions.len(), 3, "{sql}");
    }
    let Plan::Distributed { regions } = plan(
        "UPDATE orders SET total = 0 WHERE EXISTS \
         (SELECT 1 FROM users u WHERE u.tenant_id = orders.tenant_id AND u.closed)",
    ) else {
        panic!("expected a distributed write");
    };
    assert_eq!(regions.len(), 3);
}

#[test]
fn a_key_reaches_a_correlated_subquery_so_one_region_answers() {
    assert_eq!(
        single(
            "SELECT * FROM users u WHERE u.tenant_id = 'bob' AND EXISTS \
             (SELECT 1 FROM orders o WHERE o.tenant_id = u.tenant_id)"
        ),
        "us-east"
    );
    assert_eq!(
        single(
            "WITH recent AS (SELECT * FROM orders WHERE tenant_id = 'alice') \
             SELECT * FROM recent"
        ),
        "eu-west"
    );
}

#[test]
fn rows_that_could_meet_across_regions_are_refused_rather_than_lost() {
    for sql in [
        // Not tied by the key: alice's user could match bob's orders.
        "SELECT * FROM users WHERE id IN (SELECT buyer FROM orders)",
        // Every region would return the same currency, once per region.
        "SELECT * FROM currencies WHERE code IN (SELECT currency FROM orders)",
        "SELECT * FROM currencies c LEFT JOIN orders o ON o.currency = c.code",
        // A pinned table lives in one region only.
        "SELECT * FROM orders o JOIN eu_audit a ON a.tenant_id = o.tenant_id",
    ] {
        let Plan::Refuse(refusal) = plan(sql) else {
            panic!("expected a refusal, got {:?} for {sql}", plan(sql));
        };
        assert_eq!(refusal.sqlstate, "0A000", "{sql}");
    }
}

#[test]
fn copies_of_a_replicated_table_are_never_written_from_partial_rows() {
    for sql in [
        "UPDATE currencies SET used = true WHERE code IN (SELECT currency FROM orders)",
        "UPDATE currencies SET used = true WHERE code IN \
         (SELECT currency FROM orders WHERE tenant_id = 'alice')",
        "INSERT INTO currencies SELECT currency FROM orders WHERE tenant_id = 'alice'",
    ] {
        let Plan::Refuse(refusal) = plan(sql) else {
            panic!("expected a refusal, got {:?} for {sql}", plan(sql));
        };
        assert!(refusal.message.contains("copies would differ"), "{sql}");
    }
    // From other replicated tables, every copy changes alike.
    assert!(matches!(
        plan("UPDATE currencies SET rate = (SELECT max(rate) FROM currencies)"),
        Plan::Distributed { .. }
    ));
}

#[test]
fn an_insert_select_copying_keys_writes_where_it_reads() {
    // Pinned by the rows it reads.
    assert_eq!(
        single(
            "INSERT INTO orders (tenant_id, id) SELECT tenant_id, id + 1 FROM orders \
             WHERE tenant_id = 'bob'"
        ),
        "us-east"
    );
    // Every key: each region copies its own rows.
    let copied = plan("INSERT INTO orders (tenant_id, id) SELECT tenant_id, id + 1 FROM orders");
    let Plan::Distributed { regions } = copied else {
        panic!("expected a distributed write, got {copied:?}");
    };
    assert_eq!(regions.len(), 3);
    // A key not taken from a key could land anywhere.
    assert!(matches!(
        plan("INSERT INTO orders (tenant_id, id) SELECT name, id FROM users"),
        Plan::Refuse(_)
    ));
}

#[test]
fn a_merge_runs_where_its_rows_are_unless_it_may_insert() {
    assert!(matches!(
        plan(
            "MERGE INTO users u USING orders o ON u.tenant_id = o.tenant_id \
             WHEN MATCHED THEN UPDATE SET name = 'x'"
        ),
        Plan::Distributed { .. }
    ));
    assert!(matches!(
        plan(
            "MERGE INTO users u USING orders o ON u.tenant_id = o.tenant_id \
             WHEN NOT MATCHED THEN INSERT (tenant_id) VALUES (o.tenant_id)"
        ),
        Plan::Refuse(_)
    ));
}

// ------------------------------------------------------------------ local work

#[test]
fn transaction_control_and_session_state_are_handled_in_the_session() {
    assert!(matches!(plan("BEGIN"), Plan::Local(_)));
    assert!(matches!(plan("COMMIT"), Plan::Local(_)));
    assert!(matches!(plan("SET search_path = public"), Plan::Local(_)));
    assert!(matches!(plan("SHOW search_path"), Plan::Local(_)));
}

// --------------------------------------------------------- unconfigured tables

#[test]
fn an_unconfigured_table_is_refused_and_the_error_says_what_to_add() {
    let Plan::Refuse(refusal) = plan("SELECT * FROM invoices") else {
        panic!("expected a refusal");
    };
    assert!(refusal.message.contains("invoices"));
    assert!(
        refusal
            .hint
            .as_deref()
            .is_some_and(|hint| hint.contains("[[tables]]"))
    );
}

#[test]
fn unconfigured_tables_can_be_sent_home_instead() {
    // The key has to precede the `[[tables]]` arrays, or TOML reads it as a
    // field of the last table rather than of the topology.
    let permissive = format!("unconfigured_tables = \"home\"\n{TOPOLOGY}");
    let topology = Topology::from_toml(&permissive).expect("topology should parse");
    let router = Router::new(topology, Arc::new(MemoryDirectory::new()));
    let actual = plan_with(&router, "SELECT * FROM invoices", None);
    let Plan::Single { region, .. } = actual else {
        panic!("expected a single-region plan, got {actual:?}");
    };
    assert_eq!(region, "eu-west");
}

// --------------------------------------------------------------- cached path

/// The cache lifts literals into placeholders, so a statement with an inlined
/// value has to route exactly as the same statement with a bound one.
#[test]
fn an_inlined_literal_routes_the_same_as_a_bound_parameter() {
    use luxbyte_sql::AnalysisCache;

    let router = router();
    let cache = AnalysisCache::default();

    for (sql, expected) in [
        ("SELECT * FROM users WHERE tenant_id = 'alice'", "eu-west"),
        ("SELECT * FROM users WHERE tenant_id = 'bob'", "us-east"),
    ] {
        let prepared = cache.analyze(sql).expect("should analyse");
        let parameters = ParameterValues::from_prepared(&prepared, &[]);
        let analysis = prepared.analyses.first().expect("one statement");
        let Plan::Single { region, .. } = router.plan(analysis, Some(&parameters)) else {
            panic!("expected a single-region plan for {sql}");
        };
        assert_eq!(region, expected);
    }

    // The second statement reused the first one's analysis.
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.stats().hits, 1);
}

#[test]
fn a_negative_inlined_key_is_not_routed_as_its_absolute_value() {
    use luxbyte_sql::AnalysisCache;

    let directory =
        MemoryDirectory::with_entries([("users", "-1", "ap-south"), ("users", "1", "us-east")]);
    let router = router_with(directory);
    let cache = AnalysisCache::default();

    let mut seen = Vec::new();
    for sql in [
        "SELECT * FROM users WHERE tenant_id = -1",
        "SELECT * FROM users WHERE tenant_id = 1",
    ] {
        let prepared = cache.analyze(sql).expect("should analyse");
        let parameters = ParameterValues::from_prepared(&prepared, &[]);
        let analysis = prepared.analyses.first().expect("one statement");
        let Plan::Single { region, .. } = router.plan(analysis, Some(&parameters)) else {
            panic!("expected a single-region plan for {sql}");
        };
        seen.push(region);
    }
    assert_eq!(seen, ["ap-south", "us-east"]);
    // One shape served both, so the sign survived the cache rather than the
    // shape.
    assert_eq!(cache.len(), 1);
}

#[test]
fn literals_are_numbered_after_parameters_the_client_already_bound() {
    use luxbyte_sql::AnalysisCache;

    let router = router();
    let cache = AnalysisCache::default();
    let prepared = cache
        .analyze("SELECT * FROM users WHERE name = $1 AND tenant_id = 'bob'")
        .expect("should analyse");
    assert_eq!(prepared.first_placeholder, 2);

    let parameters = ParameterValues::from_prepared(&prepared, &[Some("ada".to_owned())]);
    assert_eq!(parameters.get(1), Some("ada"));
    assert_eq!(parameters.get(2), Some("bob"));

    let analysis = prepared.analyses.first().expect("one statement");
    let Plan::Single { region, .. } = router.plan(analysis, Some(&parameters)) else {
        panic!("expected a single-region plan");
    };
    assert_eq!(region, "us-east");
}

// ------------------------------------------------------------ COPY, EXPLAIN

#[test]
fn copy_and_explain_go_where_what_they_wrap_goes() {
    assert_eq!(
        single("COPY (SELECT * FROM users WHERE tenant_id = 'bob') TO STDOUT"),
        "us-east"
    );
    assert_eq!(
        single("EXPLAIN SELECT * FROM users WHERE tenant_id = 'bob'"),
        "us-east"
    );
    // EXPLAIN ANALYZE runs the statement, so a write needs the primary.
    let Plan::Single { access, .. } =
        plan("EXPLAIN ANALYZE UPDATE users SET name = 'x' WHERE tenant_id = 'bob'")
    else {
        panic!("expected one region");
    };
    assert_eq!(access, Access::ReadWrite);
    let Plan::Single { access, .. } =
        plan("EXPLAIN UPDATE users SET name = 'x' WHERE tenant_id = 'bob'")
    else {
        panic!("expected one region");
    };
    assert_eq!(access, Access::ReadOnly);
}

#[test]
fn copy_and_explain_of_several_regions_are_refused_rather_than_answered_for_one() {
    for sql in [
        "COPY users TO STDOUT",
        "EXPLAIN SELECT count(*) FROM users",
        "EXPLAIN (ANALYZE) INSERT INTO users (tenant_id) VALUES ('alice'), ('bob')",
    ] {
        let Plan::Refuse(refusal) = plan(sql) else {
            panic!("{sql} should be refused");
        };
        assert!(
            refusal.message.contains("cannot be combined"),
            "{sql}: {refusal:?}"
        );
    }
    let Plan::Refuse(refusal) = plan("COPY users FROM STDIN") else {
        panic!("COPY FROM should be refused");
    };
    assert!(refusal.message.contains("COPY FROM"));
    // A global table's copy is the same in every region.
    assert_eq!(single("COPY currencies TO STDOUT"), "eu-west");
}

// ------------------------------------------------------- shared directory

#[test]
fn with_a_shared_directory_a_write_to_a_new_key_asks_for_a_claim() {
    let router = router().claiming(true);
    let Routing::Claim(keys) = route(
        &router,
        "INSERT INTO users (tenant_id, name) VALUES ('carol', 'C'), ('alice', 'A'), ('dave', 'D')",
    ) else {
        panic!("expected a claim");
    };
    // Only the keys nobody has placed; alice is known.
    assert_eq!(
        keys,
        [
            ("users".to_owned(), "carol".to_owned()),
            ("users".to_owned(), "dave".to_owned())
        ]
    );
}

#[test]
fn with_a_shared_directory_reads_and_known_keys_need_no_claim() {
    let router = router().claiming(true);
    // A read of a key nobody placed may find rows anywhere, and asks
    // everywhere rather than claiming it.
    assert!(matches!(
        route(&router, "SELECT * FROM users WHERE tenant_id = 'carol'"),
        Routing::Ready(Plan::ScatterGather { .. })
    ));
    assert!(matches!(
        route(
            &router,
            "UPDATE users SET name = 'x' WHERE tenant_id = 'bob'"
        ),
        Routing::Ready(Plan::Single { .. })
    ));
    // Planning without waiting, as ROUTE and EXPLAIN do, shows the home
    // region a new key would be claimed for.
    let Plan::Single { region, .. } = plan_with(
        &router,
        "INSERT INTO users (tenant_id, name) VALUES ('carol', 'C')",
        None,
    ) else {
        panic!("expected a single-region plan");
    };
    assert_eq!(region, "eu-west");
}

#[test]
fn a_new_key_is_created_in_the_home_shard_it_hashes_to() {
    let topology = Topology::from_toml(
        r#"
home_region = "eu-west-1"

[[regions]]
id = "eu-west-1"
location = "eu-west"
primary = { host = "eu1" }

[[regions]]
id = "eu-west-2"
location = "eu-west"
primary = { host = "eu2" }

[[regions]]
id = "us-east"
primary = { host = "us" }

[[tables]]
table = "users"
placement = "geo_partitioned"
key = "tenant_id"
"#,
    )
    .expect("topology should parse");
    let router = Router::new(topology, Arc::new(MemoryDirectory::new()));
    let region_of = |key: &str| {
        let sql = format!("INSERT INTO users (tenant_id) VALUES ('{key}')");
        match plan_with(&router, &sql, None) {
            Plan::Single { region, .. } => region,
            other => panic!("expected a single region for {key}, got {other:?}"),
        }
    };
    // As the topology's own tests pin: alice hashes odd, bob even.
    assert_eq!(region_of("alice"), "eu-west-2");
    assert_eq!(region_of("bob"), "eu-west-1");
    // A key nobody placed is read from every shard and every region.
    let Plan::ScatterGather { regions, .. } = plan_with(
        &router,
        "SELECT * FROM users WHERE tenant_id = 'alice'",
        None,
    ) else {
        panic!("expected a fan-out");
    };
    assert_eq!(regions.len(), 3);
}

// ------------------------------------------------------------------ hints

fn hinted(router: &Router, sql: &str) -> Routing {
    let hint = luxbyte_sql::hint::find(sql).expect("a readable hint");
    let parsed = luxbyte_sql::analyze(sql).expect("parse");
    let statement = parsed.first().expect("one statement");
    router.route_hinted(statement, Some(&ParameterValues::default()), hint.as_ref())
}

fn ready_region(routing: Routing) -> String {
    match routing {
        Routing::Ready(Plan::Single { region, .. }) => region,
        other => panic!("expected one region, got {other:?}"),
    }
}

#[test]
fn a_key_hint_routes_a_statement_whose_key_the_analyser_cannot_see() {
    let router = router();
    // The key is inside a function call, so the analysis sees no key.
    assert_eq!(
        ready_region(hinted(
            &router,
            "/* luxbyte: key=bob */ SELECT * FROM users WHERE lower(tenant_id) = lower('BOB')"
        )),
        "us-east"
    );
    // Every geo-partitioned table in the statement is pinned, joins included.
    assert_eq!(
        ready_region(hinted(
            &router,
            "/* luxbyte: key=alice */ SELECT * FROM users u JOIN orders o ON o.uid = u.id"
        )),
        "eu-west"
    );
    // A statement naming no geo-partitioned table has no key to route on.
    let Routing::Ready(Plan::Refuse(refusal)) =
        hinted(&router, "/* luxbyte: key=bob */ SELECT * FROM currencies")
    else {
        panic!("expected a refusal");
    };
    assert_eq!(refusal.sqlstate, "0A000");
}

#[test]
fn a_region_hint_sends_a_statement_there_but_still_waits_for_a_move() {
    let router = router();
    assert_eq!(
        ready_region(hinted(
            &router,
            "/* luxbyte: region=ap-south */ SELECT count(*) FROM users"
        )),
        "ap-south"
    );
    let Routing::Ready(Plan::Refuse(refusal)) =
        hinted(&router, "/* luxbyte: region=mars */ SELECT 1")
    else {
        panic!("expected a refusal");
    };
    assert_eq!(refusal.sqlstate, "22023");

    // A write the hint sends where a moving key's rows were could be lost.
    let router = moving_alice(luxbyte_core::RelocationPhase::Cutover);
    assert!(matches!(
        hinted(
            &router,
            "/* luxbyte: region=eu-west */ UPDATE users SET name = 'x' WHERE id = 1"
        ),
        Routing::Held(_)
    ));
    assert!(matches!(
        hinted(&router, "/* luxbyte: region=eu-west */ SELECT * FROM users"),
        Routing::Ready(_)
    ));
}

#[test]
fn hints_leave_alone_what_the_proxy_answers_itself() {
    assert!(matches!(
        hinted(&router(), "/* luxbyte: region=ap-south */ BEGIN"),
        Routing::Ready(Plan::Local(_))
    ));
}

#[test]
fn a_replicated_table_with_a_writer_is_written_there_alone_and_read_nearby() {
    let topology = Topology::from_toml(&TOPOLOGY.replace(
        "table = \"currencies\"\nplacement = \"global_replicated\"",
        "table = \"currencies\"\nplacement = \"global_replicated\"\nwriter = \"us-east\"",
    ))
    .expect("topology should parse");
    let router = Router::new(topology, Arc::new(MemoryDirectory::new()));
    let Plan::Single { region, .. } = plan_with(
        &router,
        "UPDATE currencies SET rate = 1.1 WHERE code = 'EUR'",
        None,
    ) else {
        panic!("expected the writer alone");
    };
    assert_eq!(region, "us-east");
    let Plan::Single { region, .. } = plan_with(&router, "SELECT * FROM currencies", None) else {
        panic!("expected one region");
    };
    assert_eq!(region, "eu-west", "read from the nearest copy");

    // Without a writer, every copy changes together.
    assert!(matches!(
        plan("UPDATE currencies SET rate = 1.1 WHERE code = 'EUR'"),
        Plan::Distributed { .. }
    ));
}

#[test]
fn a_writer_must_be_a_region() {
    let error = Topology::from_toml(&TOPOLOGY.replace(
        "table = \"currencies\"\nplacement = \"global_replicated\"",
        "table = \"currencies\"\nplacement = \"global_replicated\"\nwriter = \"mars\"",
    ))
    .expect_err("unknown writer");
    assert!(error.to_string().contains("mars"), "{error}");
}
