//! End-to-end routing decisions: SQL text in, plan out.
//!
//! These are the cases that decide whether the proxy is correct, and none of
//! them needs a database to check.

// A failed assertion in a test is supposed to abort the test; the workspace
// bans these in library code, where a panic would take a proxy down.
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::sync::Arc;

use meridian_core::directory::Directory;
use meridian_core::{MemoryDirectory, ParameterValues, Plan, Router, Topology};
use meridian_sql::{Access, MergeStrategy};

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
    let mut analyses = meridian_sql::analyze(sql).expect("sql should parse");
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
fn an_ordered_scatter_becomes_a_sorted_merge() {
    let Plan::ScatterGather { merge, .. } =
        plan("SELECT * FROM users WHERE name = 'ada' ORDER BY created_at DESC")
    else {
        panic!("expected a scatter-gather plan");
    };
    let MergeStrategy::SortedMerge(keys) = merge else {
        panic!("expected a sorted merge");
    };
    assert_eq!(keys.len(), 1);
    assert!(keys.first().is_some_and(|key| key.descending));
}

#[test]
fn an_aggregate_across_regions_is_refused_rather_than_answered_wrongly() {
    // Concatenating three partial counts would return three rows, each
    // wrong. Returning an error is the only honest option short of
    // implementing per-aggregate combine steps.
    let Plan::Refuse(refusal) = plan("SELECT count(*) FROM users") else {
        panic!("expected a refusal");
    };
    assert_eq!(refusal.sqlstate, "0A000");
    assert!(refusal.message.contains("aggregat"));
    assert!(refusal.hint.is_some());
}

#[test]
fn an_aggregate_within_one_region_is_fine() {
    assert_eq!(
        single("SELECT count(*) FROM users WHERE tenant_id = 'alice'"),
        "eu-west"
    );
}

#[test]
fn distinct_across_regions_is_refused() {
    let Plan::Refuse(refusal) = plan("SELECT DISTINCT name FROM users") else {
        panic!("expected a refusal");
    };
    assert!(refusal.message.contains("DISTINCT"));
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

#[test]
fn reads_continue_from_the_source_while_a_key_is_copying() {
    let directory = MemoryDirectory::with_entries([("users", "alice", "eu-west")]);
    directory
        .begin_relocation("users", "alice", "us-east")
        .expect("relocation should start");
    let router = router_with(directory);
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
fn writes_continue_to_the_source_while_a_key_is_copying() {
    let directory = MemoryDirectory::with_entries([("users", "alice", "eu-west")]);
    directory
        .begin_relocation("users", "alice", "us-east")
        .expect("relocation should start");
    let router = router_with(directory);
    let Plan::Single { region, .. } = plan_with(
        &router,
        "UPDATE users SET name = 'x' WHERE tenant_id = 'alice'",
        None,
    ) else {
        panic!("expected a single-region plan");
    };
    assert_eq!(region, "eu-west");
}

#[test]
fn a_write_during_cutover_is_refused_with_a_retryable_sqlstate() {
    // `40001` is serialization_failure, which every client library already
    // retries. A relocation therefore costs a correctly written application
    // nothing, and no code change at all.
    let directory = MemoryDirectory::with_entries([("users", "alice", "eu-west")]);
    directory
        .begin_relocation("users", "alice", "us-east")
        .expect("relocation should start");
    directory
        .begin_cutover("users", "alice")
        .expect("cutover should start");
    let router = router_with(directory);
    let Plan::Refuse(refusal) = plan_with(
        &router,
        "UPDATE users SET name = 'x' WHERE tenant_id = 'alice'",
        None,
    ) else {
        panic!("expected a refusal");
    };
    assert_eq!(refusal.sqlstate, "40001");
    assert!(refusal.retryable);
}

#[test]
fn reads_are_not_paused_during_cutover() {
    let directory = MemoryDirectory::with_entries([("users", "alice", "eu-west")]);
    directory
        .begin_relocation("users", "alice", "us-east")
        .expect("relocation should start");
    directory
        .begin_cutover("users", "alice")
        .expect("cutover should start");
    let router = router_with(directory);
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
        "SELECT * FROM users u JOIN orders o ON o.tenant_id = u.tenant_id \
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
    use meridian_sql::AnalysisCache;

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
    use meridian_sql::AnalysisCache;

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
    use meridian_sql::AnalysisCache;

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
