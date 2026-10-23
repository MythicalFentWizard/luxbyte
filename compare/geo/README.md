# Across regions

Latency through Luxbyte and other PostgreSQL proxies with three regions a
real round trip apart, on one machine.

- **Regions:** three PostgreSQL databases (eu-west, us-east, ap-south),
  laid out by [`../fidelity/setup.py`](../fidelity/setup.py).
- **Distance:** `relay.py` sits between each proxy and the other two regions'
  databases and holds every chunk for half the round trip, each way:
  eu-west to us-east 75 ms, eu-west to ap-south 110 ms, us-east to ap-south
  42 ms. A proxy reaches its own region's database directly.
- **Proxies:** one Luxbyte per region, configured by `configs.py`, and one
  of each proxy it is compared with, given the same placement, the same
  replicated table and the same writer region. `configs.py` also writes
  `reach.csv`, the host and port each region's proxy uses for every region,
  so any proxy can be wired the same way. Each proxy runs on its own, and
  each opens one connection to every region at startup.
- **Client:** pgbench beside the eu-west proxies, four clients, one
  connection each for the whole run, every transaction logged. `run.py` runs
  each workload through each proxy in turn, twice, and pools the latencies.

```bash
python3 ../fidelity/setup.py 55431 55432 55433 --csv placement.csv > directory.toml
python3 configs.py out 55431 55432 55433 directory.toml
python3 relay.py $(cat out/relays) &
# start luxbyte serve --config out/luxbyte-<region>.toml for each region, and
# each other proxy per region from placement.csv and out/reach.csv
python3 run.py luxbyte=6460 other=6470 --seconds 12 --rounds 2
```

The new-key workload inserts rows into `fid`; delete them (`name = 'new'`)
before running the fidelity corpus again.

What it is not: a wide-area network. The delay is constant, with no jitter
and no loss, and the proxies, the relays and all three databases share one
machine's CPUs.
