# Fidelity

Whether a proxy gives the answer PostgreSQL would, statement by statement,
on real data split across three regions.

`setup.py` lays out twelve tenants over three PostgreSQL databases (`fid`)
and every row in one more (`fid_truth`), and writes the same placement for
both proxies: Luxbyte's directory entries, printed for its config, and
shahrah's directory table. `corpus.sql` holds 51 statements PostgreSQL
accepts: point and scatter reads, grammar sqlparser does not know, subqueries
and `WITH`, joins, set operations, replicated tables, and writes. `run.py`
runs each through every proxy and against `fid_truth`, and grades the answer:

- **correct**: the same rows in any order, or for a write (between `BEGIN`
  and `ROLLBACK`) the same command tag;
- **refused**: the proxy returned an error;
- **wrong**: anything else.

```bash
python3 setup.py 55431 55432 55433 > directory.toml   # eu, us, ap ports
python3 run.py 55431 luxbyte=6450 shahrah=6451 --json results.json
```

The proxies connect as `app`/`app-pw` to database `fid`; `fid_truth` is read
as `postgres`. Give shahrah `broadcast = ["users", "orders"]` and
`currencies` as replicated, or it refuses every read without a key for want
of configuration rather than of ability.

A command tag cannot show that the copies of a replicated table were
changed differently in each region, so a write judged correct here may still
have done that; Luxbyte refuses such writes, which the grading counts
against it.
