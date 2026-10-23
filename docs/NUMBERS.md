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

- **Machine:** Intel Core i5-12600K, 16 logical cores, Windows 11, for the
  routing figures; the end-to-end figures and the second set of comparison
  figures below were measured on a 4-vCPU Linux VM (Intel Xeon @ 2.80 GHz).
  Figures from the two machines are never mixed in one ratio.
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

Re-measured later on the Linux VM, with the analyser grown since (it now sees
inside `COPY` and `EXPLAIN`) and criterion's measurement time cut to three
seconds per benchmark:

| Workload | Luxbyte | shahrah | Ratio |
|---|---|---|---|
| Cached, 256 varying literals | **983 ns** | 3.15 µs | **3.2x** |
| Cached, repeated statement | **933 ns** | 3.43 µs | **3.7x** |
| Cold, median across 23 statements | — | — | **2.2x** |

Cold, Luxbyte is again faster on all 23, between 1.70x (a multi-row `INSERT`,
11.0 µs against 18.7 µs) and 3.06x (a key under `AND`, 10.4 µs against
31.9 µs). The cold gap is wider on this machine than on the first; both are
reported rather than either being picked.

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
input, which statements does each project agree to run? shahrah's proxy can
also read a sharded table from every shard when the table opts into
*broadcast*, so it is shown both ways: as configured by default, and with both
sharded tables opted in.

| Statement | Luxbyte | shahrah | shahrah, broadcast on |
|---|---|---|---|
| point read, including reversed, aliased, cast and negative keys | one region | one region | one region |
| point write, insert, delete | one region | one region | one region |
| key under `AND` | one region | one region | one region |
| `count(*)` within one region | one region | one region | one region |
| replicated read | one region | one region | one region |
| **no key at all** | fan out | refused | fan out |
| **key in an `IN` list** | fan out, 2 of 3 regions | refused | fan out, every shard |
| **same key under `OR`** | fan out, 2 of 3 regions | refused | fan out, every shard |
| **key `OR` unconstrained** | fan out | refused | fan out |
| **`count(*)` across regions** | fan out, combined | refused | fan out, combined |
| **ordered scatter** | fan out, sorted merge | refused | fan out, sorted merge |
| **`sum()` across regions** | fan out, combined | refused | refused |
| **`DISTINCT` across regions** | fan out, combined | refused | refused |
| **multi-region insert** | distributed write | refused | refused |
| **join within one region** | one region | refused | refused |
| replicated write | every region, two-phase commit | writer region | writer region |
| join across regions | refused | refused | refused |

**Of 23 statements, Luxbyte runs 22; shahrah runs 12, or 18 with broadcast
on.** Reproduce with `cargo run --release --manifest-path compare/Cargo.toml`.

Three rows are worth separating out:

- **`join within one region`.** shahrah refuses *any* statement touching two
  sharded tables — "more than one sharded table in one statement" — even when
  both are constrained to the same key and therefore provably in one place.
  That is a large class of ordinary application queries.
- **Aggregates across regions.** Luxbyte splits `count`, `sum`, `avg`, `min`,
  `max`, `bool_and`/`bool_or`, `bit_and`/`bit_or` and `DISTINCT` into per-region
  partials and finishes them exactly, in the proxy or in the home region's
  PostgreSQL; aggregates it cannot split, such as `count(DISTINCT)` or a
  percentile, are refused with the reason. shahrah's broadcast merges
  `count(*)`, sorts and limits, and refuses the rest.
- **`IN` and `OR` over keys.** Both fan out, but Luxbyte asks only the regions
  holding the named keys; a broadcast asks every shard.

### A second correction

The previous version of this table showed shahrah refusing every read without
a key, and Luxbyte running 19 statements to its 12. That was measured with
shahrah's broadcast left off, because the harness called shahrah's analyser
and broadcast is decided later, in its proxy. With broadcast on, shahrah runs
18. The harness now applies the rule shahrah's proxy applies
(`crates/proxy/src/session.rs` at the pinned revision) and reports both
columns. Luxbyte's own count has also moved, to 22, because it now combines
aggregates across regions that it used to refuse.

### Where shahrah is better

- **Parser fidelity.** shahrah uses [`pg_query`](https://github.com/pganalyze/pg_query.rs),
  PostgreSQL's own C parser, so it accepts precisely what PostgreSQL accepts —
  every operator, every extension, every corner of the grammar. Luxbyte uses
  pure-Rust `sqlparser`: faster, builds anywhere, and a *reimplementation* of
  the grammar rather than the grammar itself. Sufficiently exotic PostgreSQL
  will parse differently for us, or not at all. That is the price of the speed
  above, and it is a real one. (A statement Luxbyte cannot parse is refused
  rather than guessed at, so the failure mode is an error and not a wrong
  region; and one the application marks `/* luxbyte: region=... */` runs
  there unread, which covers the gap but puts the routing in the
  application's hands.)
- **Replicated writes.** No longer ahead. shahrah sends a write to a
  replicated table to one writer region and lets PostgreSQL replication
  carry it. Luxbyte does that too when a table names a `writer`, and then
  sends a session that wrote such a table to the writer for its reads of it,
  so it never reads a copy that has not caught up with its own write;
  without a `writer`, it keeps the synchronous two-phase commit to every
  region. Both ways are there, and the cheap one keeps read-your-writes.
- **Several proxies.** shahrah's proxies form a Raft group that agrees on the
  topology and hears about a move at once. Luxbyte's proxies share a
  directory through leases and confirmations in one authority region (see
  the README): a move waits for every live proxy, and a proxy that loses the
  authority stops routing on the directory. That makes the authority region
  a dependency Raft does not have. The window a write delayed past half a
  lease leaves open is closed by the database instead: with `guard = true`
  PostgreSQL refuses a write for a key in a region it has left, which no
  agreement between proxies can do for a write already in flight (it costs
  24–34% on single-row writes, so it is opt-in). In exchange there is no
  quorum to keep: one proxy per region is enough.
- **Sharding within a region.** shahrah hashes keys onto logical shards and
  moves whole ranges of them with `REBALANCE`. Luxbyte's shards are regions
  sharing a `location`: new keys spread across the home location's shards by
  hash, and its `REBALANCE` moves keys one atomic `RELOCATE` at a time. Per
  key is finer and keeps a key's tables together; per range moves many keys
  in fewer steps, and a range's keys never need a directory entry each.
- **Operating it.** Close to even now. Both have live dashboards (shahrah
  over a WebSocket, Luxbyte over server-sent events) that list every proxy,
  operator verbs (`DRAIN`, `REBALANCE`; Luxbyte also `ROUTE` and `RELOCATE`),
  warm connections, alerts, and routing hints. shahrah's dashboard shows
  more about each other proxy than the summary Luxbyte's heartbeat carries.
- **Measured across regions.** shahrah reports end-to-end numbers from a
  three-region bed with real latencies, against pgbouncer, pgcat and PgDog.
  Luxbyte's figures against PostgreSQL below are from one machine, over
  loopback, against shahrah only.

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

## End to end

The same statements sent straight to a backend and through the proxy in front
of it, both in this process over loopback TCP, on the Linux VM. The backend is
the testkit's, not PostgreSQL, so the difference is the proxy's own work —
parsing, routing, relaying, one extra hop on loopback — and what a real server
adds is left out rather than guessed at.

```bash
cargo bench -p luxbyte --bench proxy
```

| | direct | through the proxy | added |
|---|---|---|---|
| point read, simple protocol | 65.2 µs | 86.4 µs | **21 µs** |
| point read, prepared (`Parse`/`Bind`/`Execute`/`Sync`) | 66.2 µs | 95.3 µs | **29 µs** |
| 10 000 rows | 5.03 ms | 6.93 ms | **1.9 ms** |

Before answers from one region were relayed as they arrive, the 10 000-row
result was decoded and re-encoded in the proxy: measured the same way on the
same machine, it added **5.1 ms** (6.06 ms direct, 11.12 ms through the proxy).
Point reads did not move outside noise.

## Against PostgreSQL, through both proxies

PostgreSQL 16.15 on the same 4-vCPU Linux VM: three clusters standing in for
three regions, plus a streaming replica of one, all on loopback. Each proxy
was given the same topology and authenticated clients with SCRAM; Luxbyte was
also probing every backend twice a second. The statement is a one-row point
read, `SELECT name FROM users WHERE tenant_id = 'alice'`, which both proxies
send to the same primary, and *direct* is pgbench connected to that primary.
pgbench 16.15, six seconds per run, `-j` equal to `-c`, with direct, Luxbyte
and shahrah (`ac057a6`) taking turns in each of three rounds. Medians, in
transactions per second, with the range across rounds:

| | direct | Luxbyte | shahrah | Luxbyte / shahrah |
|---|---|---|---|---|
| simple, 1 client | 7 039 (7 035–7 119) | **4 266** (4 133–4 337) | 3 465 (3 358–3 953) | **1.23x** |
| simple, 16 clients | 19 503 (18 288–19 662) | **15 008** (14 053–16 075) | 12 919 (12 450–13 177) | **1.16x** |
| prepared, 1 client | 9 037 (8 783–9 502) | **5 305** (5 160–5 441) | 4 537 (4 484–4 811) | **1.17x** |
| prepared, 16 clients | 21 032 (20 873–21 218) | **16 801** (16 481–17 475) | 14 604 (14 443–14 976) | **1.15x** |

The first time this was run, Luxbyte **lost** on prepared statements: 4 347
against 4 748 with one client, 15 030 against 15 364 with sixteen. It held
every statement itself and parsed it again on the backend in every batch, so
a prepared statement cost a parse it would not have cost against PostgreSQL.
It now prepares a statement the client named on each pooled connection once,
under a name derived from its text, and binds it there after that. A copy is
only used if it was parsed after the client's own `Parse`, so a schema change
reaches the client exactly as it would directly; that was checked against
this PostgreSQL by altering the table under pgbench, which got the same
`cached plan must not change result type` through Luxbyte as it did direct.

Read these with their limits in mind:

- pgbench, both proxies and every PostgreSQL shared four vCPUs, so each
  figure includes contention a real deployment would not have, and neither
  proxy was tuned beyond its defaults.
- Loopback hides the network. What a region away costs is a round trip per
  statement, the same through either proxy, and not measured here.
- A proxy is a large share of a fast query's time on this machine: direct
  PostgreSQL ran 1.25x to 1.7x the transactions it ran through Luxbyte, and
  1.44x to 2.03x those through shahrah.

## What is not measured here

- Anything across real regions, with real latency between them.
- CPU per transaction, and throughput beyond sixteen clients.
- How long two-phase commit and `RELOCATE` take against real backends. Both
  were run against the PostgreSQL above to check they are correct — rows,
  identity columns and the directory all moved, no prepared transaction left
  behind — but not timed.

Each of those needs a testbed this document does not yet have; until then it
says nothing about them.
