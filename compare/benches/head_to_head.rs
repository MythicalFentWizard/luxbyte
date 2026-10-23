//! Both analysers, one process, one machine, interleaved by criterion.
//!
//! The comparison is like for like in both directions:
//!
//! - **cold** is each project's uncached entry point, parsing every time.
//! - **cached** is each project's own statement cache, warmed first, then
//!   measured with a *different literal on every call* — so both caches are
//!   doing the work they were built for rather than returning a hit on an
//!   identical string.
//!
//! Benchmarking one project's cold path against the other's warm one would
//! produce a bigger number and mean nothing.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use meridian_compare::{STATEMENTS, meridian_router, shahrah_policy};
use meridian_core::ParameterValues;
use meridian_sql::AnalysisCache;
use shahrah_sql::analysis::Policy;
use shahrah_sql::cache::Cache;
use std::hint::black_box;

/// Statements that differ only in their literal, which is what a real
/// application issues and what a shape-keyed cache exists to handle.
fn varying_literals() -> Vec<String> {
    (0..256)
        .map(|index| format!("SELECT id, name FROM users WHERE tenant_id = 'tenant-{index}'"))
        .collect()
}

/// Parse and analyse, with no cache on either side.
fn cold(c: &mut Criterion) {
    let router = meridian_router();
    let policy = shahrah_policy();

    let mut group = c.benchmark_group("cold");
    for (name, sql) in STATEMENTS {
        group.bench_with_input(
            BenchmarkId::new("meridian", name),
            sql,
            |bencher, text| {
                bencher.iter(|| {
                    let analyses =
                        meridian_sql::analyze(black_box(text)).expect("should analyse");
                    let first = analyses.first().expect("one statement");
                    black_box(router.plan(first, None))
                });
            },
        );
        group.bench_with_input(BenchmarkId::new("shahrah", name), sql, |bencher, text| {
            bencher.iter(|| black_box(shahrah_sql::analysis::analyse(black_box(text), &policy)));
        });
    }
    group.finish();
}

/// Each project's own cache, warm, with a different literal every call.
fn cached(c: &mut Criterion) {
    let router = meridian_router();
    let policy = shahrah_policy();
    let statements = varying_literals();

    let meridian_cache = AnalysisCache::default();
    let shahrah_cache = Cache::new(4_096);
    for sql in &statements {
        let _ = meridian_cache.analyze(sql);
        let _ = shahrah_cache.analyse(sql, &policy);
    }

    let mut group = c.benchmark_group("cached_varying_literals");

    let mut cursor = 0_usize;
    group.bench_function("meridian", |bencher| {
        bencher.iter(|| {
            cursor = cursor.wrapping_add(1);
            let sql = pick(&statements, cursor);
            let prepared = meridian_cache.analyze(black_box(sql)).expect("should analyse");
            let parameters = ParameterValues::from_prepared(&prepared, &[]);
            let first = prepared.analyses.first().expect("one statement");
            black_box(router.plan(first, Some(&parameters)))
        });
    });

    let mut cursor = 0_usize;
    group.bench_function("shahrah", |bencher| {
        bencher.iter(|| {
            cursor = cursor.wrapping_add(1);
            let sql = pick(&statements, cursor);
            black_box(shahrah_cache.analyse(black_box(sql), &policy))
        });
    });

    group.finish();
}

/// The same statement text every call: the easiest case for any cache, and
/// the one a prepared-statement workload actually produces.
fn cached_repeated(c: &mut Criterion) {
    let router = meridian_router();
    let policy = shahrah_policy();
    let sql = "SELECT id, name FROM users WHERE tenant_id = 'alice'";

    let meridian_cache = AnalysisCache::default();
    let shahrah_cache = Cache::new(4_096);
    let _ = meridian_cache.analyze(sql);
    let _ = shahrah_cache.analyse(sql, &policy);

    let mut group = c.benchmark_group("cached_repeated");
    group.bench_function("meridian", |bencher| {
        bencher.iter(|| {
            let prepared = meridian_cache.analyze(black_box(sql)).expect("should analyse");
            let parameters = ParameterValues::from_prepared(&prepared, &[]);
            let first = prepared.analyses.first().expect("one statement");
            black_box(router.plan(first, Some(&parameters)))
        });
    });
    group.bench_function("shahrah", |bencher| {
        bencher.iter(|| black_box(shahrah_cache.analyse(black_box(sql), &policy)));
    });
    group.finish();
}

fn pick(statements: &[String], cursor: usize) -> &str {
    statements
        .get(cursor.checked_rem(statements.len()).unwrap_or(0))
        .map_or("SELECT 1", String::as_str)
}

/// Keeps the import honest: `Policy` is named in a signature above.
const _: Option<fn(&Policy)> = None;

criterion_group!(benches, cold, cached, cached_repeated);
criterion_main!(benches);
