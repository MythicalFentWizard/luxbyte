# Meridian

A geo-distributed PostgreSQL proxy. Applications speak the ordinary PostgreSQL
wire protocol to it; Meridian works out which region holds the rows a statement
touches and sends it there.

Sharding by a hash puts a user's data wherever the hash lands. Meridian puts it
where the user is, and keeps a directory so it can be moved later when they
move. A read in your home region is a local round trip. The same read against a
single distant cluster is the speed of light and back.

**Status: in progress.** The protocol codec, the statement analyser and the
routing planner are complete, tested and usable through the `meridian explain`
command. The proxy server, connection pooling and the two-phase commit driver
are not written yet. See [Where it stands](#where-it-stands).

---

## The part that is different

Most routing proxies take the same shape: find a shard key, send the statement
to one shard, and refuse anything that does not fit. Refusing is easy to
implement and easy to defend, and it pushes the hard half of the problem back
onto the application.

Meridian handles the hard half.

| Statement | A proxy that refuses what it cannot pin down | Meridian |
|---|---|---|
| `WHERE tenant_id = $1` | no value yet — refuse, or broadcast | defer the decision to `Bind`, then route to one region |
| `WHERE tenant_id IN ('alice','bob')` | two shards — refuse | scatter to those two regions, merge the results |
| `WHERE name = 'ada'` (no key) | refuse | scatter to every region, merge |
| `... ORDER BY created_at DESC` across regions | refuse | k-way sorted merge of the regions' streams |
| `INSERT ... VALUES ('alice'),('bob')` | two shards — refuse | one distributed transaction across both |
| `SELECT count(*)` across regions | refuse | **refuse** — and say exactly why |
| `users JOIN orders` across regions | refuse | **refuse** — and say exactly why |

Measured against shahrah on the same machine with the same statements,
Meridian runs **19 of 23** where shahrah runs **12** — including a join
between two sharded tables provably in the same region, which shahrah refuses
outright.

The last two rows matter as much as the others. A `count(*)` fanned out to
three regions returns three partial counts; concatenating them gives you three
rows, each wrong. A join run separately in two regions silently drops exactly
the rows that match across the boundary. Both would look like they worked.
Meridian refuses them with a real SQLSTATE and a hint, because **a wrong answer
is worse than an error**.

Everything else errs the other way. Any predicate the analyser does not
understand produces *no* constraint rather than a guessed one, so the statement
fans out — slower, and correct.

## Try it

```bash
cargo run -p meridian -- explain \
  --topology examples/topology.toml \
  --directory examples/keys.tsv \
  "SELECT * FROM users WHERE tenant_id = 'alice'"
```

```
  kind    Select, ReadOnly, tables: users
  plan    one region: eu-west (ReadOnly)
```

A read whose key spans two regions, with an ordering to preserve:

```
$ meridian explain "SELECT * FROM users WHERE tenant_id IN ('alice','bob') ORDER BY created_at DESC"
  kind    Select, ReadOnly, tables: users
  plan    scatter-gather over eu-west, us-east
  merge   sorted merge on created_at desc
```

A write that spans regions:

```
$ meridian explain "INSERT INTO users (tenant_id, name) VALUES ('alice','a'), ('bob','b')"
  kind    Insert, ReadWrite, tables: users
  plan    distributed write over eu-west, us-east (two-phase commit)
```

A refusal, with its reason:

```
$ meridian explain "SELECT * FROM users u JOIN orders o ON o.tenant_id = u.tenant_id
                    WHERE u.tenant_id = 'alice' AND o.tenant_id = 'bob'"
  kind    Select, ReadOnly, tables: users, orders
  plan    refused [0A000]
  reason  the statement joins users and orders across regions eu-west, us-east
  hint    constrain every table to the same shard key, or make the smaller table global_replicated
```

And a prepared statement, before and after its parameter is known:

```
$ meridian explain "SELECT * FROM users WHERE tenant_id = $1"
  plan    deferred until $1 is bound

$ meridian explain --param bob "SELECT * FROM users WHERE tenant_id = $1"
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

200 tests, no database required, `cargo test` and nothing else:

```
meridian-proto   49   framing, truncation, bad lengths, SCRAM both directions
meridian-sql     81   key extraction, the AND/OR lattice, merge safety, cache
meridian-core    64   topology, relocation state machine, end-to-end routing
meridian          6   command line
```

They cover the cases that decide whether the thing is correct: a write during a
cutover, an `OR` with one unconstrained branch, an aggregate hidden inside a
`CASE` arm, a SCRAM server that does not hold the verifier, a `$9` inside a
dollar-quoted string. None of them needs a running PostgreSQL, because a
benchmark harness nobody can stand up is a benchmark nobody checks.

## Where it stands

Done, tested, and working:

- **`meridian-proto`** — PostgreSQL wire protocol v3. Framing over any async
  stream, startup and cancellation, SCRAM-SHA-256 as both client and server,
  MD5, and borrowed zero-copy views of the messages routing depends on. A proxy
  forwards far more bytes than it inspects, so the transport unit is an opaque
  tag-plus-body and typed decoding is a view over it.
- **`meridian-sql`** — statement analysis. Shard-key extraction, read/write
  classification, transaction and session-state detection, and merge-safety
  analysis.
- **`meridian-core`** — topology, placement, the key directory with its
  relocation state machine, and the routing planner.
- **`meridian`** — the `explain` command.

Not written yet:

- **`meridian-pool`** — per-endpoint connection pooling.
- **`meridian-txn`** — the two-phase commit driver. `Plan::Distributed` already
  identifies the statements that need it.
- **`meridian-testkit`** — an in-process PostgreSQL backend speaking the real
  wire protocol, so the proxy can be tested end to end with no database.
- The proxy server itself: listener, session, scatter-gather execution, the
  operator console, metrics.

Alongside, not part of the proxy:

- **[`compare/`](compare)** — both Meridian's and shahrah's routing analysis
  compiled into one process, for a head-to-head with no cross-machine
  arithmetic in it. Kept out of the workspace because shahrah needs `libclang`
  to build and Meridian does not.

Benchmarks for the server will land with it. See
[docs/NUMBERS.md](docs/NUMBERS.md) for what is measured today.

## Licence

MIT.
