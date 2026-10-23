# Comparing Luxbyte with other PostgreSQL proxies

Two kits for measuring a geo-distributed PostgreSQL proxy against PostgreSQL
itself and against other alternatives, on real PostgreSQL databases. Both
talk to proxies only through the PostgreSQL wire protocol, so any proxy that
speaks it can be put in the same run.

- **[`fidelity/`](fidelity)** — correctness. 51 statements PostgreSQL
  accepts (point and scatter reads, joins, subqueries, `WITH`, set
  operations, replicated tables, writes, and grammar many SQL parsers miss)
  run through each proxy on data split across three regions. Every answer is
  graded against one database holding every row: correct, refused, or wrong.
- **[`geo/`](geo)** — latency across regions. Three PostgreSQL databases a
  real round trip apart on one machine, a proxy per region, and pgbench
  percentiles (p50, p95, p99) and throughput per workload: home-region
  reads and writes, reads in a distant region, scatter over every region,
  and new keys.

Results and method are in [`docs/NUMBERS.md`](../docs/NUMBERS.md).
