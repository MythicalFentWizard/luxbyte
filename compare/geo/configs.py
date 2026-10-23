#!/usr/bin/env python3
"""Writes a Luxbyte config per region, the relay map, and how to reach
every region from each, for configuring any other proxy the same way.

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

    lines = []
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

# One connection to every region opened at startup.
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
        # How this region's proxy reaches every region, for configuring any
        # other proxy the same way: directly at home, through a relay away.
        lines.extend(
            f"{region},{other},127.0.0.1,{reach(index, other_index)}"
            for other_index, other in enumerate(REGIONS))
    (out / "reach.csv").write_text(
        "proxy_region,database_region,host,port\n" + "\n".join(lines) + "\n")

if __name__ == "__main__":
    main()
