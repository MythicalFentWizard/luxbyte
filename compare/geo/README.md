# Across regions

Latency through Luxbyte and shahrah with three regions a real round trip
apart, on one machine.

- **Regions:** three PostgreSQL databases (eu-west, us-east, ap-south),
  laid out by [`../fidelity/setup.py`](../fidelity/setup.py).
- **Distance:** `relay.py` sits between each proxy and the other two regions'
  databases and holds every chunk for half the round trip, each way:
  eu-west to us-east 75 ms, eu-west to ap-south 110 ms, us-east to ap-south
  42 ms. A proxy reaches its own region's database directly.
- **Proxies:** one Luxbyte and one shahrah per region, written by
  `configs.py` with the same placement, the same replicated table and the
  same writer region. Each proxy runs on its own (shahrah without its Raft
  group, Luxbyte without a shared directory), and each opens one connection
  to every region at startup.
- **Client:** pgbench beside the eu-west proxies, four clients, one
  connection each for the whole run, every transaction logged. `run.py` runs
  each workload through each proxy in turn, twice, and pools the latencies.

```bash
python3 ../fidelity/setup.py 55431 55432 55433 > directory.toml
python3 configs.py out 55431 55432 55433 directory.toml
python3 relay.py $(cat out/relays) &
# start luxbyte serve --config out/luxbyte-<region>.toml for each region,
# and shahrah with SHAHRAH_CONFIG=out/shahrah-<region>.toml
python3 run.py luxbyte=6460 shahrah=6470 --seconds 12 --rounds 2
```

The new-key workload inserts rows into `fid`; delete them (`name = 'new'`)
before running the fidelity corpus again.

What it is not: a wide-area network. The delay is constant, with no jitter
and no loss, and the proxies, the relays and all three databases share one
machine's CPUs.
