# Fidelity

Whether a proxy gives the answer PostgreSQL would, statement by statement,
on real data split across three regions.

`setup.py` lays out twelve tenants over three PostgreSQL databases (`fid`)
and every row in one more (`fid_truth`), prints Luxbyte's directory entries
for that placement, and with `--csv` writes the same placement as plain
`table,key,region` lines for any other proxy. `corpus.sql` holds 51 statements PostgreSQL
accepts: point and scatter reads, grammar sqlparser does not know, subqueries
and `WITH`, joins, set operations, replicated tables, and writes. `run.py`
runs each through every proxy and against `fid_truth`, and grades the answer:

- **correct**: the same rows in any order, or for a write (between `BEGIN`
  and `ROLLBACK`) the same command tag;
- **refused**: the proxy returned an error;
- **wrong**: anything else.

```bash
python3 setup.py 55431 55432 55433 --csv placement.csv > directory.toml   # eu, us, ap
python3 run.py 55431 luxbyte=6450 other=6451 --json results.json
```

The proxies connect as `app`/`app-pw` to database `fid`; `fid_truth` is read
as `postgres`. Give every proxy the same placement, `currencies` as a
replicated table, and whatever option it has for reading a sharded table
from every shard, or it may refuse reads without a key for want of
configuration rather than of ability.

A command tag cannot show that the copies of a replicated table were
changed differently in each region, so a write judged correct here may still
have done that; Luxbyte refuses such writes, which the grading counts
against it.
