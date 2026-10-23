//! Cost of analysing one statement.
//!
//! This is the work a proxy adds to every query that is not already cached:
//! parse the SQL, find the shard key, classify the access, and decide whether
//! the result could be merged across regions.
//!
//! Statements are grouped by what makes them expensive rather than by what
//! they do, because that is what the numbers vary with: length, predicate
//! shape, and how many tables have to be resolved.

#![allow(clippy::expect_used, clippy::unwrap_used)]
// `criterion_main!` generates an undocumented `main`.
#![allow(missing_docs)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

/// Representative statements, from cheapest to most awkward.
const STATEMENTS: &[(&str, &str)] = &[
    ("trivial", "SELECT 1"),
    (
        "point_read_literal",
        "SELECT id, name, email FROM users WHERE tenant_id = 'alice'",
    ),
    (
        "point_read_parameter",
        "SELECT id, name, email FROM users WHERE tenant_id = $1",
    ),
    (
        "in_list",
        "SELECT id FROM users WHERE tenant_id IN ('alice', 'bob', 'chandra', 'dev', 'erin')",
    ),
    (
        "conjunction",
        "SELECT id FROM users WHERE tenant_id = $1 AND status = 'live' AND created_at > $2",
    ),
    (
        "disjunction",
        "SELECT id FROM users WHERE (tenant_id = $1 AND status = 'live') OR (tenant_id = $2 AND status = 'trial')",
    ),
    (
        "join",
        "SELECT u.name, o.total FROM users u JOIN orders o ON o.tenant_id = u.tenant_id WHERE u.tenant_id = $1",
    ),
    (
        "insert_multi_row",
        "INSERT INTO users (tenant_id, name, email) VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9)",
    ),
    (
        "update",
        "UPDATE users SET name = $1, email = $2 WHERE tenant_id = $3 AND id = $4",
    ),
    (
        "aggregate_ordered",
        "SELECT region, count(*) FROM users WHERE created_at > $1 GROUP BY region ORDER BY count(*) DESC LIMIT 10",
    ),
];

fn analysis(c: &mut Criterion) {
    let mut group = c.benchmark_group("analyse");
    for (name, sql) in STATEMENTS {
        // Throughput in bytes makes the length-driven part of the cost
        // visible, rather than hiding it in a single per-call figure.
        group.throughput(Throughput::Bytes(sql.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), sql, |b, sql| {
            b.iter(|| luxbyte_sql::analyze(black_box(sql)).expect("statement should parse"));
        });
    }
    group.finish();
}

/// The same statements, measured as one batch, to give a single
/// representative figure for a mixed workload.
fn mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("analyse_mixed");
    group.throughput(Throughput::Elements(STATEMENTS.len() as u64));
    group.bench_function("ten_statements", |b| {
        b.iter(|| {
            for (_, sql) in STATEMENTS {
                let _ = black_box(luxbyte_sql::analyze(black_box(sql)));
            }
        });
    });
    group.finish();
}

criterion_group!(benches, analysis, mixed_workload);
criterion_main!(benches);
