//! Cost of deciding where a statement runs.
//!
//! `end_to_end` is the number that matters: SQL text in, routing decision
//! out, which is the whole of the work a proxy adds on top of forwarding
//! bytes. It is the figure directly comparable with other routing proxies,
//! because it is the same job measured from the same starting point.
//!
//! `plan_only` separates the planner from the parser, so that when the
//! end-to-end number moves it is obvious which half moved.

#![allow(clippy::expect_used, clippy::unwrap_used)]
// `criterion_main!` generates an undocumented `main`.
#![allow(missing_docs)]

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use luxbyte_core::directory::Directory;
use luxbyte_core::{MemoryDirectory, ParameterValues, Router, Topology};
use luxbyte_sql::{AnalysisCache, StatementAnalysis};
use std::hint::black_box;

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
"#;

/// A directory with enough keys that lookups are not hitting a one-entry map.
fn router(keys: usize) -> Router {
    let topology = Topology::from_toml(TOPOLOGY).expect("topology should parse");
    let directory = MemoryDirectory::new();
    let regions = ["eu-west", "us-east", "ap-south"];
    for index in 0..keys {
        let region = regions
            .get(index.checked_rem(regions.len()).unwrap_or(0))
            .copied()
            .unwrap_or("eu-west");
        directory.place("users", &format!("tenant-{index}"), region);
        directory.place("orders", &format!("tenant-{index}"), region);
    }
    // Named keys the benchmark statements actually use.
    directory.place("users", "alice", "eu-west");
    directory.place("users", "bob", "us-east");
    directory.place("orders", "alice", "eu-west");
    Router::new(topology, Arc::new(directory))
}

const STATEMENTS: &[(&str, &str)] = &[
    ("trivial", "SELECT 1"),
    (
        "point_read",
        "SELECT id, name, email FROM users WHERE tenant_id = 'alice'",
    ),
    (
        "point_write",
        "UPDATE users SET name = 'ada' WHERE tenant_id = 'alice'",
    ),
    (
        "scatter_two_regions",
        "SELECT id FROM users WHERE tenant_id IN ('alice', 'bob')",
    ),
    (
        "scatter_all_regions",
        "SELECT id FROM users WHERE name = 'ada'",
    ),
    (
        "distributed_write",
        "INSERT INTO users (tenant_id, name) VALUES ('alice', 'a'), ('bob', 'b')",
    ),
    (
        "join_one_region",
        "SELECT u.name, o.total FROM users u JOIN orders o ON o.tenant_id = u.tenant_id WHERE u.tenant_id = 'alice' AND o.tenant_id = 'alice'",
    ),
    ("refused_aggregate", "SELECT count(*) FROM users"),
];

/// SQL text in, routing decision out: the full cost of the proxy's thinking.
fn end_to_end(c: &mut Criterion) {
    let router = router(10_000);
    let mut group = c.benchmark_group("route_end_to_end");
    for (name, sql) in STATEMENTS {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(name), sql, |bencher, text| {
            bencher.iter(|| {
                let analyses =
                    luxbyte_sql::analyze(black_box(text)).expect("statement should parse");
                let first = analyses.first().expect("one statement");
                black_box(router.plan(first, None))
            });
        });
    }
    group.finish();
}

/// Planning alone, with the statement already analysed.
fn plan_only(c: &mut Criterion) {
    let router = router(10_000);
    let analysed: Vec<(&str, StatementAnalysis)> = STATEMENTS
        .iter()
        .map(|(name, sql)| {
            let mut analyses = luxbyte_sql::analyze(sql).expect("statement should parse");
            (*name, analyses.remove(0))
        })
        .collect();

    let mut group = c.benchmark_group("plan_only");
    for (name, analysis) in &analysed {
        group.bench_with_input(
            BenchmarkId::from_parameter(name),
            analysis,
            |bencher, item| {
                bencher.iter(|| black_box(router.plan(black_box(item), None)));
            },
        );
    }
    group.finish();
}

/// A prepared statement planned at `Bind`, which is the hot path for any
/// application that prepares its queries.
fn bound_parameter(c: &mut Criterion) {
    let router = router(10_000);
    let mut parsed =
        luxbyte_sql::analyze("SELECT id FROM users WHERE tenant_id = $1").expect("should parse");
    let statement = parsed.remove(0);
    let bound = ParameterValues::new(vec![Some("tenant-4242".to_owned())]);

    c.bench_function("plan_bound_parameter", |bencher| {
        bencher.iter(|| black_box(router.plan(black_box(&statement), Some(black_box(&bound)))));
    });
}

/// How directory size affects a lookup, which is the one part of routing
/// that grows with the deployment.
fn directory_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("directory_lookup");
    for keys in [1_000_usize, 100_000, 1_000_000] {
        let directory = MemoryDirectory::new();
        for index in 0..keys {
            directory.place("users", &format!("tenant-{index}"), "eu-west");
        }
        let probe = format!("tenant-{}", keys / 2);
        group.bench_with_input(BenchmarkId::from_parameter(keys), &probe, |bencher, key| {
            bencher.iter(|| black_box(directory.locate("users", black_box(key))));
        });
    }
    group.finish();
}

/// The hot path: SQL text in, routing decision out, with the analysis cache
/// warm.
///
/// This is what a proxy actually costs in steady state, because an
/// application issues the same few statement shapes forever. The literals
/// differ on every call, so this measures a real cache hit rather than a
/// repeated identical string.
fn end_to_end_cached(c: &mut Criterion) {
    let router = router(10_000);
    let cache = AnalysisCache::default();

    // Warm the shapes.
    for (_, sql) in STATEMENTS {
        let _ = cache.analyze(sql);
    }

    let mut group = c.benchmark_group("route_cached");
    for (name, sql) in STATEMENTS {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(name), sql, |bencher, text| {
            bencher.iter(|| {
                let prepared = cache.analyze(black_box(text)).expect("should analyse");
                let parameters = ParameterValues::from_prepared(&prepared, &[]);
                let first = prepared.analyses.first().expect("one statement");
                black_box(router.plan(first, Some(&parameters)))
            });
        });
    }
    group.finish();
}

/// A workload of inlined literals, which is the case a text-keyed cache
/// misses on every single call.
fn end_to_end_cached_varying_literals(c: &mut Criterion) {
    let router = router(10_000);
    let cache = AnalysisCache::default();
    let statements: Vec<String> = (0..256)
        .map(|index| format!("SELECT id, name FROM users WHERE tenant_id = 'tenant-{index}'"))
        .collect();
    for sql in &statements {
        let _ = cache.analyze(sql);
    }

    let mut cursor = 0_usize;
    c.bench_function("route_cached_varying_literals", |bencher| {
        bencher.iter(|| {
            cursor = cursor.wrapping_add(1);
            let sql = statements
                .get(cursor.checked_rem(statements.len()).unwrap_or(0))
                .map_or("SELECT 1", String::as_str);
            let prepared = cache.analyze(black_box(sql)).expect("should analyse");
            let parameters = ParameterValues::from_prepared(&prepared, &[]);
            let first = prepared.analyses.first().expect("one statement");
            black_box(router.plan(first, Some(&parameters)))
        });
    });
}

criterion_group!(
    benches,
    end_to_end,
    end_to_end_cached,
    end_to_end_cached_varying_literals,
    plan_only,
    bound_parameter,
    directory_scaling
);
criterion_main!(benches);
