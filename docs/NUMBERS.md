# Numbers

What a proxy costs is the work it adds on top of forwarding bytes. For a
geo-router that is one question — *where does this statement go?* — and this
document measures exactly that, and nothing else.

Reproduce everything here with:

```bash
cargo bench
```

No database, no Docker, no containers, no three-region testbed. That is
deliberate. A benchmark nobody can run is a benchmark nobody checks.

## Method

- **Machine:** Intel Core i5-12600K, 16 logical cores, Windows 11.
- **Harness:** [criterion](https://github.com/bheisler/criterion.rs) 0.7, which
  warms up, takes 100 samples per benchmark, and reports a confidence interval
  rather than a single figure.
- **Build:** `--release` with thin LTO and one codegen unit.
- **What is timed:** wall-clock, single-threaded, one statement per iteration.
- **Variance:** reruns of the same build moved individual figures by up to
  **15%** (`route_end_to_end/point_read` measured 7.57 µs and 8.66 µs on two
  consecutive runs). Treat anything under about 20% as noise. Where a number
  matters below, the slower of the two runs is quoted.

## Routing decision cost

The full job: SQL text in, routing decision out.

| Statement | Cold | **Cached** | Speed-up |
|---|---|---|---|
| `SELECT 1` | 2.24 µs | **352 ns** | 6.4× |
| point read, `WHERE tenant_id = 'alice'` | 8.66 µs | **781 ns** | 11.1× |
| point write, `UPDATE ... WHERE tenant_id = 'alice'` | 5.25 µs | **909 ns** | 5.8× |
| scatter over two regions, `IN ('alice','bob')` | 7.65 µs | **1.15 µs** | 6.7× |
| scatter over three regions, no key | 5.44 µs | **613 ns** | 8.9× |
| distributed write, multi-row `INSERT` | 7.53 µs | **1.41 µs** | 5.3× |
| join inside one region | 15.05 µs | **1.29 µs** | 11.7× |
| refused aggregate | 4.90 µs | **517 ns** | 9.5× |

**Inlined literals, 256 distinct values, cache warm: 891 ns.**

That last row is the one that matters for a real application. Every statement
has a different literal in it, so a cache keyed on SQL text would miss on
every single call. This one hits, because the key is the statement's *shape*.

## Where the time goes

| | |
|---|---|
| Parsing (`sqlparser`) | 7–13 µs |
| Planning (topology, directory, merge safety) | **40–635 ns** |
| Directory lookup, 1 000 keys | 198 ns |
| Directory lookup, 100 000 keys | 221 ns |
| Directory lookup, 1 000 000 keys | **316 ns** |
| Planning a bound parameter at `Bind` | 436 ns |

Parsing is roughly 95% of the cold cost and planning is the rest, which is why
the analysis cache is worth more than any amount of tuning in the planner. The
directory is a `BTreeMap`, so lookups grow logarithmically: a thousand-fold
increase in keys costs 118 ns.

## Comparison with shahrah

[shahrah](https://github.com/rs-alireza/shahrah) is the closest comparable
project, and its
[NUMBERS.md](https://github.com/rs-alireza/shahrah/blob/main/docs/NUMBERS.md)
is a genuinely careful document: interleaved rounds, distributions rather than
single figures, a retracted claim, and an honest statement that it remains
about 1.5x pgbouncer's cost. It is worth reading.

Rather than compare against its published figures across machines, both
analysers are compiled into **one process** and interleaved by criterion on
**one machine**. `shahrah_sql::analysis::analyse` and `shahrah_sql::cache::Cache`
are public, so no estimation is involved. See [`compare/`](../compare).

```bash
cargo run   --release --manifest-path compare/Cargo.toml   # decisions
cargo bench          --manifest-path compare/Cargo.toml   # speed
```

shahrah is pinned at `ac057a6`. Each project is given the same statements and
an equivalent placement policy, written in its own vocabulary.

### Speed, measured

Both caches are warmed first and then measured with a different literal on
every call, so each is doing the work it was built for. Benchmarking one
project's cold path against the other's warm one would produce a larger number
and mean nothing.

| Workload | Luxbyte | shahrah | Ratio |
|---|---|---|---|
| Cached, 256 varying literals | **802 ns** | 2.75 µs | **3.4x** |
| Cached, repeated statement | **718 ns** | 2.85 µs | **4.0x** |
| Cold, median across 23 statements | — | — | **~1.7x** |

Cold, Luxbyte is faster on **all 23** statements, between 1.18x and 2.05x.
The widest gaps are where predicates get complicated — `tenant_id = 'alice' OR
name = 'ada'` is 7.56 µs against 15.27 µs — and the narrowest is a plain
single-row `INSERT`, at 6.68 µs against 7.88 µs.

The gap widens with the cache because the two caches are doing different
amounts of work on a hit, not because either is badly written.

### A correction

An earlier draft of this document compared Luxbyte's measured 0.78 µs against
shahrah's **published** 13 µs and described the resulting 15x as "indicated but
not proven". That extrapolation was wrong by about a factor of four.

Running his code here gives 2.75 µs, not 13 µs. The published figure is CPU-µs
per transaction under 64 concurrent clients on an i3-1315U; this is
single-threaded wall-clock on an i5-12600K. Those are not the same quantity,
and the difference between them was most of the apparent gap.

The real factor is **3.4x to 4.0x**. It is kept here because a benchmark
document that quietly drops its own overstatements is not worth reading.

### Decisions, measured

Speed is the easy comparison and the less interesting one. Given identical
input, which statements does each project agree to run?

| Statement | Luxbyte | shahrah |
|---|---|---|
| point read, including reversed, aliased, cast and negative keys | one region | one region |
| point write, insert, delete | one region | one region |
| key under `AND` | one region | one region |
| `count(*)` within one region | one region | one region |
| replicated read | one region | one region |
| **no key at all** | fan out | refused |
| **key in an `IN` list** | fan out, 2 of 3 regions | refused |
| **same key under `OR`** | fan out, 2 of 3 regions | refused |
| **key `OR` unconstrained** | fan out | refused |
| **multi-region insert** | distributed write | refused |
| **ordered scatter** | fan out, sorted merge | refused |
| **join within one region** | one region | refused |
| join across regions | refused | refused |
| `count(*)`, `sum()`, `DISTINCT` across regions | refused | refused |

**Luxbyte runs 19 of the 23; shahrah runs 12.**

Two of those rows are worth separating out:

- **`join within one region`.** shahrah refuses *any* statement touching two
  sharded tables — "more than one sharded table in one statement" — even when
  both are constrained to the same key and therefore provably in one place.
  That is a large class of ordinary application queries.
- **Aggregates across regions.** Both refuse, but for different reasons.
  shahrah refuses for want of a shard key; Luxbyte gets far enough to say
  that the statement *reads three regions and cannot be merged because it
  aggregates*. Same outcome, and one of the two errors tells you what to do.

### Where shahrah is better

- **Parser fidelity.** shahrah uses [`pg_query`](https://github.com/pganalyze/pg_query.rs),
  PostgreSQL's own C parser, so it accepts precisely what PostgreSQL accepts —
  every operator, every extension, every corner of the grammar. Luxbyte uses
  pure-Rust `sqlparser`: faster, builds anywhere, and a *reimplementation* of
  the grammar rather than the grammar itself. Sufficiently exotic PostgreSQL
  will parse differently for us, or not at all. That is the price of the speed
  above, and it is a real one. (A statement Luxbyte cannot parse is refused
  rather than guessed at, so the failure mode is an error and not a wrong
  region.)
- **Replicated writes.** shahrah sends a write to a replicated table to one
  configured writer region and lets PostgreSQL replication carry it. Luxbyte
  broadcasts to every region under two-phase commit. Ours is synchronous and
  therefore consistent on read-your-writes from any region; his is much
  cheaper. Which is correct depends on the application, and his default is the
  more economical one.

## A note on cache correctness

Both projects cache analyses keyed on statement shape with literals stripped.
shahrah's NUMBERS.md records a bug this introduced:

> This caught a bug where `where id = -1` would route to the shard for the
> absolute value.

That is an easy bug to write and a hard one to notice: it is silent, it only
affects negative keys, and the query still returns rows. Luxbyte guards
against the whole class rather than that one instance. On every cache miss it
analyses the original statement *and* the normalised one and compares them,
and caches the shape only if they agree. A shape that fails is marked
unusable and always analysed in full.

So a bug in the normaliser costs a cache miss, never a wrong answer — and the
check also catches cases that are not about signs at all, such as `ORDER BY 1`
becoming `ORDER BY $1`, which would merge a scattered result in the wrong
order. Both are covered by tests.

The verification runs once per shape, so its cost disappears under any
repeating workload. It is included in every number in this document.

## What is not measured here

- End-to-end proxy throughput, latency and CPU per transaction. There is no
  server yet.
- Connection pooling.
- Two-phase commit.
- Anything against a real PostgreSQL.

Those land with the server, measured the same way: reproducible on one machine,
with the method written down.
