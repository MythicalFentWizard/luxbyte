#!/usr/bin/env python3
"""Writes one Luxbyte and one shahrah config per region, and the relay map.

Usage: configs.py OUT_DIR EU_PORT US_PORT AP_PORT DIRECTORY_TOML

Each proxy reaches its own region's database directly and the other two
through `relay.py`, which adds the round trip between the two regions. The
placement is the one `../fidelity/setup.py` lays out, in database `fid`.
"""

import pathlib
import sys

REGIONS = ["eu-west", "us-east", "ap-south"]
ROUND_TRIP_MS = {(0, 1): 75, (0, 2): 110, (1, 2): 42}
LUXBYTE_PORT = 6460
SHAHRAH_PORT = 6470


def round_trip(a, b):
    return ROUND_TRIP_MS[(min(a, b), max(a, b))]


def relay_port(source, target):
    return 15600 + 10 * source + target


def main():
    out = pathlib.Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    ports = [int(port) for port in sys.argv[2:5]]
    directory = pathlib.Path(sys.argv[5]).read_text()

    def reach(source, target):
        return ports[target] if source == target else relay_port(source, target)

    mappings = [
        f"{relay_port(source, target)}:{ports[target]}:{round_trip(source, target)}"
        for source in range(3) for target in range(3) if source != target
    ]
    (out / "relays").write_text(" ".join(mappings) + "\n")

    for index, region in enumerate(REGIONS):
        regions = "".join(
            f'\n[[topology.regions]]\nid = "{other}"\n'
            f'primary = {{ host = "127.0.0.1", port = {reach(index, other_index)} }}\n'
            for other_index, other in enumerate(REGIONS))
        (out / f"luxbyte-{region}.toml").write_text(f"""[server]
listen = "127.0.0.1:{LUXBYTE_PORT + index}"
node = "geo-{region}"

[backend]
user = "postgres"
database = "fid"
tls = "disable"

[auth]
query = "SELECT rolpassword FROM pg_authid WHERE rolname = $1"
passthrough = false

# One connection to every region opened at startup, as shahrah does.
[pool]
warm = 1

[topology]
home_region = "{region}"
unconfigured_tables = "home"
{regions}
[[topology.tables]]
table = "users"
placement = "geo_partitioned"
key = "tenant_id"

[[topology.tables]]
table = "orders"
placement = "geo_partitioned"
key = "tenant_id"

[[topology.tables]]
table = "currencies"
placement = "global_replicated"
writer = "eu-west"

{directory}""")
        shards = "".join(
            f"""
[[shards]]
number = {other_index + 1}
region = "{other}"
primary = {{ address = "127.0.0.1:{reach(index, other_index)}", region = "{other}" }}
logical = [{{ start = 0, end = 65535 }}]
""" for other_index, other in enumerate(REGIONS))
        (out / f"shahrah-{region}.toml").write_text(f"""region = "{region}"

[policy]
broadcast = ["users", "orders"]

[policy.replicated.currencies]
writer_region = "eu-west"

[policy.keys.users]
column = "tenant_id"
type = "text"

[policy.keys.orders]
column = "tenant_id"
type = "text"
{shards}""")


if __name__ == "__main__":
    main()
