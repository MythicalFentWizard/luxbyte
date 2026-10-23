# Luxbyte

A geo-distributed PostgreSQL proxy. Applications speak the ordinary PostgreSQL
wire protocol to it; Luxbyte works out which region holds the rows a statement
touches and sends it there.

Sharding by a hash puts a user's data wherever the hash lands. Luxbyte puts it
where the user is, and keeps a directory so it can be moved later when they
move. A read in your home region is a local round trip. The same read against a
single distant cluster is the speed of light and back.

**Status: it runs.** `luxbyte serve` accepts PostgreSQL clients over both the
simple and the extended query protocol, authenticates them with SCRAM, routes
their statements, merges scattered reads and commits across regions in two
phases. See [Where it stands](#where-it-stands) for what is still missing.

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
| `users JOIN orders` across regions | refuse | **refuse** — and say exactly why |

See [docs/NUMBERS.md](docs/NUMBERS.md) for a measured head-to-head against
shahrah on the same machine with the same statements.

The last two rows matter as much as the others. A `count(DISTINCT country)`
cannot be rebuilt from per-region answers: the same country in two regions
looks like two. A join run separately in two regions silently drops exactly
the rows that match across the boundary. Both would look like they worked.
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
what the router is doing is the one already on the machine.

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
table = "eu_audit"
placement = "pinned"            # one region, and only that one
region = "eu-west"
```

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
- Anything else — a function, a subquery, a `NOT`, a range — yields no
  constraint.

Every rule errs towards more candidate regions, never fewer. Over-approximating
costs an extra region in the fan-out; under-approximating returns the wrong
rows.

**The directory** maps each key to a region, and is the reason this is not just
hash sharding. A key with no entry is created in the proxy's home region: new
rows land where the person who made them is. Keys move with an explicit
two-phase relocation:

| Phase | Reads | Writes |
|---|---|---|
| `Copying` — rows streaming to the destination | source | source |
| `Cutover` — destination caught up, mapping about to flip | source | paused, SQLSTATE `40001` |
| settled | destination | destination |

`40001` is `serialization_failure`, which every PostgreSQL client library
already retries. A relocation therefore costs a correctly written application
nothing, and no code change at all.

## Speed

A routing decision, with the analysis cache warm:

| | |
|---|---|
| Point read | **781 ns** |
| Inlined literals, 256 distinct values | **891 ns** |
| Distributed write across two regions | **1.41 µs** |
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

Against shahrah, compiled into one process and run on one machine rather than
compared across published figures: **3.4-4.0x faster cached**, and faster on
all 23 statements cold. It also runs 19 of those 23 statements where shahrah
runs 12. Method, variance, and the places shahrah is better:
[docs/NUMBERS.md](docs/NUMBERS.md), reproducible via [`compare/`](compare).

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

321 tests, no database required, `cargo test` and nothing else:

```
luxbyte-proto    49   framing, truncation, bad lengths, SCRAM both directions
luxbyte-sql      81   key extraction, the AND/OR lattice, merge safety, cache
luxbyte-core     64   topology, relocation state machine, end-to-end routing
luxbyte-pool     23   handshake, reuse, limits, lifetimes, lost connections
luxbyte-txn      29   two-phase commit and recovery, including the crash cases
luxbyte-testkit   3   the fake backend itself
luxbyte          72   merging, splitting, config, and 30 through the proxy
```

The last thirty are the proxy as a whole: a real client connects over TCP to a
real proxy, which connects over TCP to real backends, and everything speaks
the wire protocol. The only thing that is not PostgreSQL is the backends, and
they are only pretending so that a participant can be made to refuse a prepare,
or vanish mid-commit, on purpose rather than on a bad day.

They cover the cases that decide whether the thing is correct: a write during a
cutover, an `OR` with one unconstrained branch, an aggregate hidden inside a
`CASE` arm, a SCRAM server that does not hold the verifier, a `$9` inside a
dollar-quoted string. None of them needs a running PostgreSQL, because a
benchmark harness nobody can stand up is a benchmark nobody checks.

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
prepared statements itself and re-parses them on whichever backend connection a
`Bind` is routed to, so a statement prepared once runs in any region. An error
discards the rest of the batch and rolls it back, as on one server. What it
refuses, it refuses with a reason:

- a single statement whose plan spans regions (scatter-gather and split writes
  are simple-protocol only for now);
- a batch that writes to two regions outside a transaction, which could not
  commit atomically — inside `BEGIN`/`COMMIT` it commits in two phases;
- `COPY`.

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

An admin port, off unless configured, serves Prometheus metrics at
`/metrics` — statement latency by plan, errors by SQLSTATE, statements per
region and how many left the home region, how each cross-region read was
finished, transaction outcomes, pool occupancy — plus a JSON `/snapshot`,
`/key/<table>/<key>` to say which region holds a user's rows, a dashboard at
`/` that needs no JavaScript, and `/healthz`. It refuses to listen beyond
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

- **[`compare/`](compare)** — both Luxbyte's and shahrah's routing analysis
  compiled into one process, for a head-to-head with no cross-machine
  arithmetic in it. Kept out of the workspace because shahrah needs `libclang`
  to build and Luxbyte does not.

Benchmarks for the server will land with it. See
[docs/NUMBERS.md](docs/NUMBERS.md) for what is measured today.

## Licence

MIT.
