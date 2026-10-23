#!/usr/bin/env python3
"""Lays out the fidelity dataset: twelve tenants over three regions.

Usage: setup.py EU_PORT US_PORT AP_PORT [--csv PLACEMENT_CSV]

Creates database `fid` in each region and `fid_truth` (every row, in one
place) in the first. Tenants t01-t04 live in eu-west, t05-t08 in us-east,
t09-t12 in ap-south. Each has two users and three orders; the third order of
each tenant was bought by a user of the next tenant, so joins that are not on
the shard key cross regions. `currencies` is the same everywhere.

Luxbyte's directory entries for this placement are printed, for its config.
With --csv, the same placement is also written as `table,key,region` lines,
for giving any other proxy the same layout in its own format.
"""

import subprocess
import sys

REGIONS = ["eu-west", "us-east", "ap-south"]
CURRENCIES = [("EUR", 100), ("USD", 92), ("INR", 1), ("JPY", 6), ("GBP", 117)]


def tenant_region(n):
    return REGIONS[(n - 1) // 4]


def rows():
    users, orders = [], []
    for n in range(1, 13):
        t = f"t{n:02d}"
        users.append((t, n * 10 + 1, f"user {n}-1", n % 3 == 0, None))
        users.append((t, n * 10 + 2, f"user {n}-2", False, n * 10 + 1))
        home_currency = {"eu-west": "EUR", "us-east": "USD", "ap-south": "INR"}[tenant_region(n)]
        nxt = n % 12 + 1
        orders.append((t, n * 100 + 1, n * 10 + 1, home_currency, n * 10 + 1))
        orders.append((t, n * 100 + 2, n * 10 + 2, "USD", n * 7))
        orders.append((t, n * 100 + 3, nxt * 10 + 1, "JPY" if n == 7 else "EUR", 5))
    return users, orders


def literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def values(table_rows):
    return ", ".join("(" + ", ".join(literal(v) for v in row) + ")" for row in table_rows)


SCHEMA = """
CREATE TABLE users (tenant_id text NOT NULL, id int NOT NULL, name text, vip bool,
                    manager int, PRIMARY KEY (tenant_id, id));
CREATE TABLE orders (tenant_id text NOT NULL, id int NOT NULL, buyer int, currency text,
                     total int, PRIMARY KEY (tenant_id, id));
CREATE TABLE currencies (code text PRIMARY KEY, rate int);
"""


def psql(port, database, sql):
    subprocess.run(
        ["psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1", "-p", str(port),
         "-U", "postgres", "-d", database],
        input=sql, text=True, check=True)


def main():
    ports = [int(p) for p in sys.argv[1:4]]
    users, orders = rows()
    for index, (region, port) in enumerate(zip(REGIONS, ports)):
        psql(port, "postgres", "DROP DATABASE IF EXISTS fid; CREATE DATABASE fid;")
        mine_users = [u for u in users if tenant_region(int(u[0][1:])) == region]
        mine_orders = [o for o in orders if tenant_region(int(o[0][1:])) == region]
        sql = SCHEMA
        sql += f"INSERT INTO users VALUES {values(mine_users)};\n"
        sql += f"INSERT INTO orders VALUES {values(mine_orders)};\n"
        sql += f"INSERT INTO currencies VALUES {values(CURRENCIES)};\n"
        sql += "GRANT ALL ON ALL TABLES IN SCHEMA public TO app;\n"
        psql(port, "fid", sql)
    psql(ports[0], "postgres", "DROP DATABASE IF EXISTS fid_truth; CREATE DATABASE fid_truth;")
    psql(ports[0], "fid_truth", SCHEMA
         + f"INSERT INTO users VALUES {values(users)};\n"
         + f"INSERT INTO orders VALUES {values(orders)};\n"
         + f"INSERT INTO currencies VALUES {values(CURRENCIES)};\n")
    if "--csv" in sys.argv:
        path = sys.argv[sys.argv.index("--csv") + 1]
        with open(path, "w", encoding="utf-8") as placement:
            for n in range(1, 13):
                for table in ("users", "orders"):
                    placement.write(f"{table},t{n:02d},{tenant_region(n)}\n")
    # Luxbyte's directory entries, for its config.
    for n in range(1, 13):
        for table in ("users", "orders"):
            print(f'[[directory]]\ntable = "{table}"\nkey = "t{n:02d}"\nregion = "{tenant_region(n)}"\n')


if __name__ == "__main__":
    main()
