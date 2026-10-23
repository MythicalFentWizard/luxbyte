# Luxbyte — a geo-distributed PostgreSQL proxy for multi-region Postgres

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-wire%20protocol%20v3-336791.svg)](https://www.postgresql.org/docs/current/protocol.html)

**Luxbyte is an open-source PostgreSQL proxy, written in Rust, that keeps each
user's data in their own region and routes every query to the region that
holds its rows.** Applications speak the ordinary PostgreSQL wire protocol to
it, with any driver or ORM; Luxbyte works out which region holds the rows a
statement touches and sends it there. It does geo-partitioning, sharding by
tenant or user key, data residency, connection pooling, read/write splitting
and cross-region transactions in front of plain PostgreSQL, with no extension
and no change to the database.

Sharding by a hash puts a user's data wherever the hash lands. Luxbyte puts it
where the user is, and keeps a directory so it can be moved later when they
move. A read in your home region is a local round trip. The same read against a
single distant cluster is the speed of light and back.

**Status: it runs.** `luxbyte serve` accepts PostgreSQL clients over both the
simple and the extended query protocol, authenticates them with SCRAM, routes
their statements, merges scattered reads and commits across regions in two
phases. See [Where it stands](#where-it-stands) for what is still missing.

Use it for:

- **Multi-region PostgreSQL** without a new database: plain PostgreSQL in
  each region, one proxy endpoint for the application.
- **Data residency and sovereignty** (GDPR and similar): a European user's
  rows stay in the European region, and a key can be moved to another
  region online with `RELOCATE`.
- **Sharding PostgreSQL by tenant or user**, with a directory of where each
  key lives instead of a fixed hash, and shards within a region.
- **Low-latency reads and writes near the user**, scatter-gather queries
  across regions with exact aggregates, `ORDER BY ... LIMIT` and joins on the
  shard key, and two-phase commit when a write spans regions.
- **Connection pooling, read replicas and failover-aware routing**, with
  SCRAM authentication, TLS, Prometheus metrics and a live dashboard.

---

## The part that is different

Most routing proxies take the same shape: find a shard key, send the statement
to one shard, and refuse anything that does not fit. Refusing is easy to
implement and easy to defend, and it pushes the hard half of the problem back
onto the application.

Luxbyte handles the hard half.

| Statement | A proxy that refuses what it cannot pin down | Luxbyte |
|---|---|---|
| `WHERE tenant_id = $1` | no value yet — refuse, or broadcast | defer the decision to `Bind`, then route to one region |
| `WHERE tenant_id IN ('alice','bob')` | two shards — refuse | scatter to those two regions, merge the results |
| `WHERE name = 'ada'` (no key) | refuse | scatter to every region, merge |
| `... ORDER BY created_at DESC LIMIT 20` across regions | refuse | each region sends its top 20; a k-way merge keeps the overall top 20 |
| `INSERT ... VALUES ('alice'),('bob')` | two shards — refuse | one distributed transaction across both |
| `SELECT country, count(*), avg(age) ... GROUP BY country HAVING ...` across regions | refuse | partial aggregates per region, regrouped by PostgreSQL in the home region |
| `count(DISTINCT ...)`, `stddev()` across regions | refuse | **refuse** — and say exactly why |
| `users JOIN orders ON o.tenant_id = u.tenant_id`, no key | refuse | every region at once: rows only meet where their keys match |
| `users JOIN orders ON o.buyer = u.id` across regions | refuse | **refuse** — and say exactly why |

See [docs/NUMBERS.md](docs/NUMBERS.md) for a measured head-to-head against
another geo-aware PostgreSQL proxy on the same machine with the same
statements.

The last two rows matter as much as the others. A `count(DISTINCT country)`
cannot be rebuilt from per-region answers: the same country in two regions
looks like two. A join on anything but the shard key, run separately in two
regions, silently drops exactly the rows that match across the boundary. Both would look like they worked.
Luxbyte refuses them with a real SQLSTATE and a hint, because **a wrong answer
is worse than an error**.

What it can combine, it combines exactly. Plain ordered reads are merged in
the proxy only where every sort column compares exactly outside PostgreSQL —
integers, `numeric` digit by digit, floats, `bool`, `uuid`. Everything else —
text in a collation's order, `DISTINCT`, `UNION`, every aggregate — is
finished *in PostgreSQL*: the regions' parts go to the home region as typed
arrays and a rewritten statement does the last step there, so collation,
`numeric` precision, `HAVING` and result types are PostgreSQL's own rather
than a reimplementation of them. Aggregates the database lists in `pg_proc`
are recognised as aggregates even when the built-in list does not know them,
so a user-defined one is never computed per region and concatenated.

Everything else errs the other way. Any predicate the analyser does not
understand produces *no* constraint rather than a guessed one, so the statement
fans out — slower, and correct.

## Try it

```bash
luxbyte serve --config examples/luxbyte.toml
psql -h 127.0.0.1 -p 6432 -d shop
```

```
shop=> SHOW REGIONS;
  region  | home |        primary        |         replicas
----------+------+-----------------------+---------------------------
 eu-west  | yes  | eu.db.internal:5432   | eu-replica.db.internal:5432
 us-east  | no   | us.db.internal:5432   |
 ap-south | no   | ap.db.internal:5432   |

shop=> WHERE IS users alice;
 table | key   | region  |  state
-------+-------+---------+----------
 users | alice | eu-west | resident
```

The console answers over the same connection, so the only tool needed to see
what the router is doing is the one already on the machine. It also has
`SHOW PLACEMENT`, `SHOW POOLS`, `SHOW DIRECTORY`, `SHOW IN DOUBT`,
`SHOW HEALTH`, `SHOW ALERTS`, and `ROUTE <statement>`, which says where a
statement would run and how its answer would be combined, using the live
directory, without running it. Roles listed in `auth.operators`, once
authenticated, also have `RELOCATE`, and `DRAIN`/`UNDRAIN <host:port>` to take
a replica out of service and back.

To see a routing decision without starting anything:

```bash
cargo run -p luxbyte -- explain \
  --topology examples/topology.toml \
  --directory examples/keys.tsv \
  "SELECT * FROM users WHERE tenant_id = 'alice'"
```

```
  kind    Select, ReadOnly, tables: users
  plan    one region: eu-west (ReadOnly)
```

A read whose key spans two regions, with an ordering and a limit to keep:

```
$ luxbyte explain "SELECT * FROM users WHERE tenant_id IN ('alice','bob') ORDER BY created_at DESC LIMIT 20"
  kind    Select, ReadOnly, tables: users
  plan    scatter-gather over eu-west, us-east
  regions SELECT *, created_at AS "__luxbyte_sort_0" FROM users WHERE tenant_id IN ('alice', 'bob') ORDER BY created_at DESC LIMIT 20
  finish  rows: merged in order, limit 20
```

An aggregate over every region:

```
$ luxbyte explain "SELECT country, count(*), avg(age) FROM users GROUP BY country HAVING count(*) > 100"
  kind    Select, ReadOnly, tables: users
  plan    scatter-gather over ap-south, eu-west, us-east
  regions SELECT country, count(*), sum(age), count(age) FROM users GROUP BY country
  finish  2 partial aggregate(s) regrouped in the home region
```

A write that spans regions:

```
$ luxbyte explain "INSERT INTO users (tenant_id, name) VALUES ('alice','a'), ('bob','b')"
  kind    Insert, ReadWrite, tables: users
  plan    distributed write over eu-west, us-east (two-phase commit)
```

A refusal, with its reason:

```
$ luxbyte explain "SELECT * FROM users u JOIN orders o ON o.tenant_id = u.tenant_id
                    WHERE u.tenant_id = 'alice' AND o.tenant_id = 'bob'"
  kind    Select, ReadOnly, tables: users, orders
  plan    refused [0A000]
  reason  the statement joins users and orders across regions eu-west, us-east
  hint    constrain every table to the same shard key, or make the smaller table global_replicated
```

And a prepared statement, before and after its parameter is known:

```
$ luxbyte explain "SELECT * FROM users WHERE tenant_id = $1"
  plan    deferred until $1 is bound

$ luxbyte explain --param bob "SELECT * FROM users WHERE tenant_id = $1"
  plan    one region: us-east (ReadOnly)
```

## How routing works

**Placement** is declared per table, in TOML:

```toml
[[tables]]
table = "users"
placement = "geo_partitioned"   # rows live where their key lives
key = "tenant_id"

[[tables]]
table = "currencies"
placement = "global_replicated" # a full copy in every region

[[tables]]
table = "fx_rates"
placement = "global_replicated"
writer = "us-east"              # written there; replication copies it on

[[tables]]
table = "eu_audit"
placement = "pinned"            # one region, and only that one
region = "eu-west"
```

A replicated table without a `writer` is written in every region in one
two-phase commit: every copy is current the moment it commits. With a
`writer`, a write goes there alone and PostgreSQL's own replication carries
it to the other copies, which costs one round trip instead of one to every
region. Reads use the nearest copy — except for a session that has written
one: from then on it reads those tables from the writer, so it never misses
its own write while replication catches up.

A table with no policy is refused by default, naming itself in the error. The
alternative — quietly sending it somewhere — is how half your data ends up
unreachable from everywhere else, discovered months later.

**Key constraints** come from parsing, not pattern matching. The analyser
builds a small lattice over the `WHERE` clause:

- `AND` narrows: if either side pins a column the result is pinned, and if both
  do the answer is their intersection.
- `OR` widens: a column survives only if *both* sides pin it, and the answer is
  their union. So `tenant_id = 1 OR created_at > now()` pins nothing, which is
  correct — it really can match any tenant.
- Anything else — a function, a `NOT`, a range — yields no constraint.

Every rule errs towards more candidate regions, never fewer. Over-approximating
costs an extra region in the fan-out; under-approximating returns the wrong
rows.

**Every query block** is read, not just the outer one. A table in a
subquery, a `WITH` query, a `LATERAL` join or the `SELECT` of an `INSERT` is
a table the statement reads, and a predicate restricts only the tables of the
block it is written in: under `NOT EXISTS`, a subquery's `WHERE` says nothing
about the outer rows. Keys flow along join conditions and into correlated
subqueries, so in

```sql
SELECT * FROM users u WHERE u.tenant_id = 'alice'
  AND EXISTS (SELECT 1 FROM orders o WHERE o.tenant_id = u.tenant_id)
```

both tables are alice's and one region answers. Without a key, a statement
whose geo-partitioned tables are joined **shard key to shard key** runs in
every region at once: matching rows have equal keys, equal keys live
together, so each region finds every pair it holds and no pair comes back
twice. That covers joins, correlated subqueries, an `INSERT ... SELECT` that
copies keys, and a `MERGE` that only updates. A join on anything else, or a
replicated table filtered by rows spread over regions, is refused; so is a
write that would change each copy of a replicated table from different rows.

**PostgreSQL's own parser** stands behind `sqlparser`, which is fast but a
reimplementation of the grammar. For each statement shape not seen before,
PostgreSQL's parser names the tables the statement reads, in a few
microseconds; if it names one `sqlparser` did not — `FROM ONLY users` reads
to `sqlparser` as a table called `only` — PostgreSQL's reading is used. A
statement `sqlparser` cannot parse at all (`users*`, `ROWS FROM`, `TABLE`,
`OVERRIDING SYSTEM VALUE`, ...) is read from PostgreSQL's parse tree. This is
the `pg-parser` feature, on by default; it needs a C compiler and `libclang`
to build. On 51 statements over real data in three regions, Luxbyte gives
PostgreSQL's answer 46 times, refuses 5, and is never wrong
([`compare/fidelity`](compare/fidelity)).

**The directory** maps each key to a region, and is the reason this is not just
hash sharding. A key with no entry is created in the proxy's home region: new
rows land where the person who made them is.

With a `[directory_store]` section the directory lives in a table in every
region, loaded at startup, and keys move with `RELOCATE`. A key written for
the first time is recorded there too, in the same round trip to the home
region that every proxy-placed key costs once, so its reads go straight to
its region instead of asking all of them (`record_new_keys = false` keeps
the old fan-out, for rows the directory does not list yet):

```
=> RELOCATE alice TO 'us-east';
 table  |  from   |   to    | rows | state
--------+---------+---------+------+-------
 orders | eu-west | us-east |   14 | moved
 users  | eu-west | us-east |    1 | moved
```

The key moves in every geo-partitioned table at once, since rows that share a
key are joined on it. The rows are inserted in the destination, deleted from
the source and the directory changed in every region, all in **one two-phase
commit**: either all of it happens or none of it does, so there is no
half-moved key to repair afterwards and no moment, once it commits, when the
rows are in two regions or none. Each count is checked against the last
(rows read, rows inserted, rows deleted), and a mismatch abandons the move.

| Phase | Reads of the key | Writes to the key |
|---|---|---|
| cutover: writes already under way finish; rows copied under a lock | source | wait |
| commit: regions commit one after another | wait | wait |
| done | destination | destination |

Statements *wait* rather than fail, so a move costs an application a pause,
not an error. One that waits longer than `hold_ms`, or one inside a
transaction that has already done work (it may hold locks the move needs),
is refused with `40001`, which client libraries already retry. In the source
the tables are locked against writers, not readers, for the length of the
copy, which also waits for any open transaction that wrote to them; a move
that cannot get its locks within `lock_timeout_ms` gives up and leaves
everything as it was.

Only roles listed in `auth.operators` may run it, and only after authenticating:
with `trust` authentication nobody can. A key with no entry is looked for in
every region, and a key with rows in two regions is refused rather than
guessed. A table outside the move with a foreign key onto one inside it is
refused too, since the move would break it; foreign keys between the moved
tables are followed, parents inserted first and deleted last.

### Hints

Sometimes the key is there but the proxy cannot see it — inside a function
call, a subquery, a view — and sometimes the proxy cannot read the
statement at all, because its parser does not know that corner of
PostgreSQL's grammar. The application can say where the statement goes:

```sql
/* luxbyte: key=alice */ SELECT * FROM users WHERE lower(tenant_id) = lower($1);
/* luxbyte: region=eu-west */ REINDEX TABLE users;
```

`key=` routes as if every geo-partitioned table in the statement were
constrained to that key, through the directory as usual. `region=` sends
the statement to one region; a statement the proxy cannot parse runs there
too, treated as a write that may touch any geo-partitioned table. Either
way a move in progress is still waited for. A hint is only read from a
comment that begins with `luxbyte:`, never from inside a string; one that
cannot be read is an error rather than ignored; and hints are left out of
the analysis cache's key, so a key per statement costs no cache entries.
`ROUTE` shows where a hinted statement would go.

### Shards within a region

One primary per region is not always enough. Regions that share a
`location` are shards of it:

```toml
[[topology.regions]]
id = "eu-west"
location = "eu-west"
primary = { host = "eu1.db.internal" }

[[topology.regions]]
id = "eu-west-2"
location = "eu-west"
primary = { host = "eu2.db.internal" }
```

A key nobody has placed is created in the home location's shard its hash
picks (FNV-1a, written out rather than taken from the standard library, so
proxies built at different times agree), and every one of its tables'
rows goes there with it. `REBALANCE eu-west TO eu-west-2 LIMIT 100` moves
up to 100 of a shard's keys, read from its rows, 25 to a move by default
(`BATCH n`): each move is one atomic transaction — one lock, one read, one
insert and one delete per table, one directory change, one two-phase
commit — for all its keys. `RELOCATE alice, bob TO us-east` does the same
for keys named by hand. A rebalance stops at the first move that is refused
and reports what moved before it; `BATCH 1` goes key by key, so one
troublesome key stops only itself. Against PostgreSQL 16 with two shards,
100 keys with their orders moved in 1.31 s key by key and 0.15 s in batches
of 25, every row accounted for. Everything else — the directory, moves,
reads that fan out — treats a shard as the region it is.

### Several proxies

With `shared = true` under `[directory_store]`, several proxies — one per
region, say — share the directory. They agree either in one region, the
`authority`, or, with `authority = "majority"`, across a majority of all the
regions:

- **Leases.** Each proxy records a heartbeat every `poll_ms` and routes on its
  directory only while its lease, renewed by visits that got through, is
  under `lease_ms` old. Past that it
  refuses statements on geo-partitioned tables with `57P03` rather than route
  on a directory that may be out of date (other tables carry on), and when it
  gets through again it reads the directory whole before routing.
- **Moves.** `RELOCATE` is announced there first. Every live proxy holds the
  key's writes, waits for its own writes under way, and confirms; only then
  are rows copied. Before the commit each holds reads too and confirms again.
  A proxy that does not confirm stops the move, unless its heartbeat is more
  than half again `lease_ms` old, by when it has stopped routing by itself.
  If the mover dies, the others keep holding the key until it restarts,
  finishes what it left in doubt and withdraws the announcement.
- **New keys.** A first write to a key nobody has placed claims it, so two
  proxies homed in different regions cannot each create it at home.

With `authority = "majority"` there is no region whose loss stops the
others. Every region keeps the shared tables and each step counts once a
majority has it: any two majorities share a region, so a proxy reading a
majority sees every move announced to one, and a withdrawn move leaves a
marker so that a region which missed the withdrawal cannot bring it back.
New keys are decided by single-decree Paxos with each region's database as
an acceptor, two round trips to a majority. Directory entries carry a
version every move raises, so the newest answer wins. A lease needs one
more step: a heartbeat grants only when the proxy's previous visit reached a
majority, and the proxy routes on a grant only after a later visit reached
one too, so a proxy cut off with a single region cannot keep a mover
waiting by heartbeating there. `lease_ms` must be at least three times
`poll_ms` for that.

Losing a minority of regions then stops nothing that does not need them:
every proxy keeps its lease, claims carry on, and a move goes on without a
region that holds none of its keys (the regions that took part outvote it).
When a region answers again, the proxies copy it the entries it missed; a
proxy starting up does the same, which also carries entries an authority
recorded alone over to the other regions. Against PostgreSQL 16 with two
proxies and three regions, stopping one region's database left both proxies
routing: a key moved between the other two in 0.49 s, a new key was claimed,
and 43,515 transactions through both proxies across the outage and the
restart had no failures. The same outage with the authority in that region
refused every statement on a geo-partitioned table. Three regions survive
losing one; two regions need both.

`SHOW PEERS` lists the proxies and the moves this one is holding keys for,
and `SHOW ALERTS` reports `directory_unreachable` and `move_stalled`. Each
proxy needs its own `server.node`; a second process with a name in use is
refused at startup.

What it costs: a small query to the authority, or to every region at once,
every `poll_ms` from each proxy; on the first write of each new key a round
trip to the authority, or two to a majority; and holds up to two polls
longer while a key moves. What leases alone do not cover: a
write a proxy sent before its lease ran out that reaches its region more than
`lease_ms / 2` later can land after a move it never heard of. Consensus
between proxies does not close that either, since the write is already on
its way to the database; the database can. With `guard = true` every region
gets a trigger on each geo-partitioned table that refuses a write for a key
its own directory places elsewhere — and a move leaves every region's
directory saying where the key went — so such a write is refused by
PostgreSQL (`40001`, which clients retry) instead of stranded where no read
will look:

```
ERROR:  key alice of orders lives in eu-west, not in us-east
HINT:  the key has moved; retry, and the proxy sends it where it is
```

The move itself is let through by a setting only its own transaction turns
on. Guards cost one index lookup per written row: on single-row updates over
loopback, the worst case, 24% of throughput with one client and 34% with
four (PostgreSQL 16, vacuum between runs, three interleaved rounds). They
are off by default, and need the backend role to own the tables.

Against PostgreSQL 16 (two proxies, three clusters), a move through one
proxy while the other ran pgbench incrementing the moving key's rows lost
nothing: the rows' sum rose by exactly two per committed transaction, 43 647
of them, none failed and none retried, and the move took 0.39 s.

## Speed

A routing decision, with the analysis cache warm:

| | |
|---|---|
| Point read | **853 ns** |
| Inlined literals, 256 distinct values | **1.06 µs** |
| Distributed write across two regions | **1.59 µs** |
| Directory lookup among 1 000 000 keys | **316 ns** |

Parsing is about 95% of the cold cost, so the analysis cache is where the
speed comes from. Its key is the statement's *shape* — the text with literals
lifted into placeholders — so `WHERE tenant_id = 'alice'` and
`WHERE tenant_id = 'bob'` are analysed once between them. An application that
inlines its literals gets the same hit rate as one that prepares.

Stripping literals to build a cache key is also an easy way to introduce a
silent bug: `where id = -1` must not end up routed to the region for `1`.
Rather than enumerate those cases, the cache **verifies its own key** — on
every miss it analyses both the original and the normalised statement and
caches the shape only if they agree. A bug in the normaliser costs a cache
miss, never a wrong answer.

Against the closest of the other alternatives, another open-source
geo-aware PostgreSQL proxy, with both analysers compiled into one process on
one machine rather than compared across published figures: **3.3-4.3x faster
cached**, and faster on all 23 statements cold even with PostgreSQL's parser
checking each new one. Luxbyte runs all 23 of those statements; the
alternative runs 12, or 18 with its opt-in broadcast reads. On 51 statements
over real data in three regions, Luxbyte gives PostgreSQL's answer 46 times
and the alternative 22; neither gives a wrong one. In front of real
PostgreSQL 16 under pgbench, on one machine, Luxbyte ran **1.15x to 1.23x**
its transactions per second, simple and prepared, one client and sixteen.
Through the proxy end to end, a point read costs 21 µs over a direct
connection on loopback. With three regions 75 and 110 ms apart, Luxbyte reads
and writes at home 1.08–1.23x as fast, matches it across a region, counts
over every region in 113 ms to its 190, and inserts new keys it refuses.
Method, variance, and the places other alternatives are better:
[docs/NUMBERS.md](docs/NUMBERS.md), reproducible with `cargo bench` and the
kits in [`compare/`](compare).

```bash
cargo bench   # no database, no Docker, no testbed
```

## Committing across regions

A write that touches one region is an ordinary `COMMIT`. One that touches
several is two-phase commit, and the order of three steps is the entire
protocol:

1. `PREPARE TRANSACTION` on every participant. Any refusal and everything
   rolls back. Nothing committed, so nothing is lost, and the client gets a
   retryable error.
2. **Write the decision to the commit log, durably.** The point of no return.
3. `COMMIT PREPARED` on every participant. Failures here delay the
   transaction; they cannot undo it.

A coordinator that dies between 2 and 3 restarts, reads the log and finishes.
One that dies between 1 and 2 restarts, finds no record, and rolls back. Both
are correct, and which happens is decided entirely by whether that one write
completed. There is a test that asserts the ordering directly: at the moment
the record is written, every participant has prepared and none has been told
to commit.

Recovery also does the job that is easy to forget. A prepared transaction
nobody resolves is worse than a slow one — PostgreSQL holds its locks and
pins the transaction horizon, so `VACUUM` stops reclaiming across the whole
database. So recovery scans each participant's `pg_prepared_xacts` for
identifiers this coordinator issued, and rolls back any the log never decided
on. It is scoped to its own identifier prefix, because rolling back another
system's prepared transaction would destroy data it knows nothing about.

## Splitting a write, rather than broadcasting it

Most multi-region writes need no special handling. `UPDATE users SET active =
false WHERE last_seen < '2026-01-01'` can go verbatim to every region: each
applies it to the rows it holds, and the union is what one server would have
done.

`INSERT ... VALUES` is the exception, and getting it wrong is silent.
Broadcasting

```sql
INSERT INTO users (tenant_id, name) VALUES ('alice', 'a'), ('bob', 'b')
```

to two regions does not write one row in each — it writes **both rows in
both**, and nothing complains. The duplicate sits in a region the directory
will never look in, so it stays invisible until somebody counts.

So the rows are partitioned by where their key belongs and each region gets an
`INSERT` containing only its own, rebuilt from the parse tree rather than by
cutting up the text — because the text has commas inside string literals and
the parse tree does not. Anything that cannot be split that way is refused.

## Engineering rules

The workspace denies, not warns:

```toml
unsafe_code       = "forbid"
unwrap_used       = "deny"
expect_used       = "deny"
panic             = "deny"
indexing_slicing  = "deny"   # no buf[i] — malformed input cannot panic a proxy
arithmetic_side_effects = "deny"  # no silent overflow in a length calculation
```

plus `clippy::pedantic` and `missing_docs`. The whole workspace builds with zero
warnings under all of it.

Two consequences worth stating, because they are the point rather than a side
effect:

- **A malformed message from the network cannot panic the proxy.** Every read
  is bounds-checked and returns a `Result`; a negative length, a truncated
  body, or a string with no terminator is an error that closes one connection.
- **The code is commented.** Not narration of what the next line does — the
  reasoning behind decisions that are not obvious from the code: why `NULL` and
  the empty string must stay distinct, why the server nonce has to extend the
  client's, why reads keep going to the source during a cutover.

### Tests

595 tests, no database required, `cargo test` and nothing else:

```
luxbyte-proto    62   framing, truncation, bad lengths, SCRAM both directions
luxbyte-sql     124   every query block, keys and links, PostgreSQL's parser, the cache
luxbyte-core     83   topology, relocation state machine, end-to-end routing
luxbyte-pool     39   handshake, reuse, resets, TLS, limits, lost connections
luxbyte-txn      31   two-phase commit and recovery, including the crash cases
luxbyte-testkit   2   the fake backend itself
luxbyte         253   merging, splitting, peers, config, and 175 through the proxy
```

The last 131 are the proxy as a whole: a real client connects over TCP to a
real proxy, which connects over TCP to real backends, and everything speaks
the wire protocol. The only thing that is not PostgreSQL is the backends, and
they are only pretending so that a participant can be made to refuse a prepare,
or vanish mid-commit, on purpose rather than on a bad day.

They cover the cases that decide whether the thing is correct: a write during a
cutover, an `OR` with one unconstrained branch, an aggregate hidden inside a
`CASE` arm, a SCRAM server that does not hold the verifier, a `$9` inside a
dollar-quoted string. None of them needs a running PostgreSQL, because a
benchmark harness nobody can stand up is a benchmark nobody checks.

## Similar tools, and how Luxbyte differs

If you are looking for a PostgreSQL proxy, pooler or sharding layer, these
are the usual alternatives, and where Luxbyte sits among them:

- **Connection poolers** such as PgBouncer pool connections to one
  PostgreSQL server. Luxbyte pools too, but its job is deciding *which*
  server, in which region, a statement belongs to.
- **Sharding proxies** such as pgcat and PgDog spread data over shards by a
  hash of the key. Luxbyte keeps a directory of where each key lives, so a
  key is placed in its user's region and can be moved later, and it hashes
  only within a region's shards.
- **Distributed PostgreSQL extensions** such as Citus spread tables across
  the nodes of one cluster. Luxbyte needs no extension and treats each region
  as its own ordinary PostgreSQL, which keeps every PostgreSQL feature and
  tool working in each region.
- **Distributed SQL databases** such as CockroachDB and YugabyteDB offer
  multi-region placement as a different database that speaks the PostgreSQL
  protocol. Luxbyte keeps PostgreSQL itself.
- **Other geo-aware PostgreSQL proxies** keep a directory of home regions as
  Luxbyte does; [docs/NUMBERS.md](docs/NUMBERS.md) compares Luxbyte with the
  closest of them, statement by statement and under load, including where
  it is still behind.

## Where it stands

Done, tested, and working:

- **`luxbyte-proto`** — PostgreSQL wire protocol v3. Framing over any async
  stream, startup and cancellation, SCRAM-SHA-256 as both client and server,
  MD5, and borrowed zero-copy views of the messages routing depends on. A proxy
  forwards far more bytes than it inspects, so the transport unit is an opaque
  tag-plus-body and typed decoding is a view over it.
- **`luxbyte-sql`** — statement analysis. Shard-key extraction, read/write
  classification, transaction and session-state detection, and merge-safety
  analysis.
- **`luxbyte-core`** — topology, placement, the key directory with its
  relocation state machine, and the routing planner.
- **`luxbyte-pool`** — per-endpoint connection pooling, keyed by endpoint,
  database and role. A connection goes back to the pool only once the server
  has reported it idle, and one that changed session state — a `SET`, a
  `LISTEN`, a named statement — is reset in the background first, so one
  client's settings never reach another.
- **`luxbyte-txn`** — the two-phase commit coordinator, with a durable
  decision log and recovery in both directions.
- **`luxbyte-testkit`** — an in-process PostgreSQL backend speaking the real
  wire protocol on a real socket, so all of the above is tested with
  `cargo test` and no database.
- **`luxbyte`** — the proxy server itself: listener, session, scatter-gather
  execution with sorted merge, statement splitting, the operator console, and
  the `explain` command.

The extended query protocol — what psycopg3, asyncpg and JDBC use by default —
routes each statement on the key bound to it at `Bind`, including keys bound in
binary once their type is declared or reported by a `Describe`. The proxy keeps
prepared statements itself and prepares them on whichever backend connection a
`Bind` is routed to, so a statement prepared once runs in any region. A
statement the client named stays prepared on each pooled connection it has
run on, so binding it again costs no parse there; a copy older than the
client's own `Parse` is never used, so a schema change behaves as it would on
one server. An error discards the rest of the batch and rolls it back, as on
one server.

A prepared statement that spans regions — a read to combine, a write to commit
in two phases, a multi-row `INSERT` to split by key — runs through the same
code as a simple query, with each parameter written into the text as a literal
of its own type. PostgreSQL renders the values and names their types
(`$1::text, pg_typeof($1)`), so the literal means exactly what the parameter
did, binary or text, whatever its type. The answer goes back in the formats
the client asked for, binary ones encoded by the home region, and a
row-limited `Execute` gets it a part at a time. A batch outside a transaction
that writes across regions runs, from that statement on, in a transaction the
proxy commits in two phases at the `Sync`: still all or nothing. What it
refuses, it refuses with a reason:

- a batch outside a transaction that has already written to one region and
  then writes to, or reads across, others: what it wrote is not in any
  transaction the proxy could commit with the rest, or would read back —
  inside `BEGIN`/`COMMIT` both work;
- `COPY`.

A statement for one region is relayed as the backend sends it: rows are never
decoded or held, so a large result streams through in bounded memory, and the
column descriptions keep the table and column numbers and type modifiers that
drivers such as JDBC read. `COPY ... TO` and `EXPLAIN` route by the table and
key inside them, and are refused when they would span regions, since each
region could answer only for its own rows; `COPY ... FROM` is refused, since
its rows would have to be placed by key.

Clients authenticate with SCRAM-SHA-256 against PostgreSQL's own stored
verifiers, from the configuration or read from the home region with a bound
parameter. With `passthrough`, the keys a client's login proved are used to
log in to every backend *as that client*: privileges and row-level security
on the backends see the real role, and no shared superuser is needed. Unknown
roles fail exactly as a wrong password does, so a login attempt does not
reveal which roles exist. Left unconfigured, the proxy trusts every client
and refuses to listen beyond loopback until told that is intended.

A `CancelRequest` reaches every backend connection the client is using at
that moment: each region of a scatter-gather read, each participant of a
transaction. Every connection a cancel was aimed at is retired rather than
pooled, so a cancel that arrives late can never stop another client's query
on a reused connection. Once a two-phase commit's decision is durable, cancels
no longer reach its participants, so a cancel cannot leave a committed
transaction prepared.

Every backend is probed on a connection of its own (`application_name =
luxbyte health`, so it is easy to tell apart in `pg_stat_activity`) for whether
it is in recovery and its WAL position. A replica more than
`max_replica_lag_bytes` behind its primary serves no reads. After a session
writes in a region, its reads there go to the primary until a replica has
replayed past the write: the primary's position at its first probe after the
write is at or past it, so a replica that has reached that position has it —
read-your-writes with no extra query. Three failed probes mark a backend down,
a failed connection on the data path marks it down at once, and a read whose
replica cannot be reached goes to the primary instead, since nothing was sent
yet. A stale replica is never used because the primary is down: a read that
fails says so, where a stale one would look like an answer. The proxy observes
and never promotes; a primary that reports being in recovery is shown as such
in `SHOW HEALTH` and the admin port.

Alerts are facts the proxy already knows, stated once rather than inferred
from thresholds: a backend down, a replica too far behind to serve reads, a
server playing a role the topology does not give it, a two-phase commit not
yet acknowledged everywhere, a move awaiting a region's confirmation. They are
`luxbyte_alert` series on the admin port and rows in `SHOW ALERTS`. With
`pool.warm` set, connections to every backend are opened before the first
client arrives.

An admin port, off unless configured, serves Prometheus metrics at
`/metrics` — statement latency by plan, errors by SQLSTATE, statements per
region and how many left the home region, how each cross-region read was
finished, transaction outcomes, pool occupancy — plus a JSON `/snapshot`,
`/key/<table>/<key>` to say which region holds a user's rows, `/healthz`, and
a dashboard at `/`: rendered on the server, live each second over
server-sent events (`/events`, at most 16 open at once), and reloading
itself every five seconds where script is off. Its one inline script is
allowed by its hash in the Content-Security-Policy, and every other page
allows none. With a shared directory it lists every proxy and what each says
of itself with its heartbeat: sessions, statements, holds, moves followed,
whether it is fenced. It refuses to listen beyond
loopback without a password; the password is compared in constant time, and
repeated wrong answers lock an address out for longer each time.

TLS runs both ways. To backends it takes libpq's `sslmode` names —
`disable`, `prefer` (the default), `require`, `verify-ca`, `verify-full` —
with their libpq meanings, against a configured CA or Mozilla's roots. To
clients it answers `SSLRequest` with a configured certificate, can refuse
clients that do not ask, closes a connection that sends bytes between its
`SSLRequest` and the handshake (CVE-2021-23214), and declines a GSSAPI
encryption request with the `N` that libpq expects rather than an error.
The admin port can serve HTTPS with the same certificate; it then drops a
plain request unanswered rather than reply in the clear.

Alongside, not part of the proxy:

- **[`compare/geo`](compare/geo)** — three regions a real round trip apart
  on one machine, a proxy per region, pgbench latency percentiles per
  workload, for Luxbyte and any other proxy.
- **[`compare/fidelity`](compare/fidelity)** — 51 statements run through
  proxies on real data split across three PostgreSQL databases, each answer
  graded against one database holding every row.

See [docs/NUMBERS.md](docs/NUMBERS.md) for what is measured, how, and what
is not measured yet.

## Licence

MIT.
