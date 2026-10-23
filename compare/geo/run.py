#!/usr/bin/env python3
"""Latency across three regions, Luxbyte against shahrah.

Usage: run.py NAME=PORT [NAME=PORT ...] [--seconds N] [--rounds N] [--clients N]
              [--only TEXT] [--json FILE]

Every workload runs through each named proxy in turn, for each round, with
pgbench writing a line per transaction; the latencies of all rounds are
pooled per proxy and workload. The proxies are expected to sit in eu-west
beside the client, with us-east 75 ms and ap-south 110 ms away (see
configs.py and relay.py). Connections are opened once per client, so
connecting is not timed.
"""

import glob
import json
import os
import pathlib
import subprocess
import sys
import tempfile

WORKLOADS = {
    "point read, home region": ("SELECT * FROM users WHERE tenant_id = 't01';", "simple"),
    "point read, 75 ms away": ("SELECT * FROM users WHERE tenant_id = 't05';", "simple"),
    "point read, 110 ms away": ("SELECT * FROM users WHERE tenant_id = 't09';", "simple"),
    "prepared point read, home": ("SELECT * FROM users WHERE tenant_id = :key;", "prepared"),
    "point write, home region": (
        "UPDATE orders SET total = total WHERE tenant_id = 't01' AND id = 101;", "simple"),
    "point write, 75 ms away": (
        "UPDATE orders SET total = total WHERE tenant_id = 't05' AND id = 501;", "simple"),
    "replicated read": ("SELECT rate FROM currencies WHERE code = 'EUR';", "simple"),
    "count over every region": ("SELECT count(*) FROM orders;", "simple"),
    "new key, insert": (
        "\\set k random(1000000000000, 9000000000000000)\n"
        "INSERT INTO users (tenant_id, id, name) VALUES (:k, 1, 'new');", "prepared"),
}


def percentile(sorted_values, fraction):
    if not sorted_values:
        return float("nan")
    index = min(len(sorted_values) - 1, int(round(fraction * (len(sorted_values) - 1))))
    return sorted_values[index]


def run(port, script, mode, seconds, clients):
    with tempfile.TemporaryDirectory() as work:
        path = pathlib.Path(work) / "script.sql"
        path.write_text(script + "\n")
        done = subprocess.run(
            ["pgbench", "-n", "-h", "127.0.0.1", "-p", str(port), "-U", "app",
             "-c", str(clients), "-j", str(min(clients, 4)), "-T", str(seconds),
             "-M", mode, "-D", "key=t01", "-f", str(path), "-l", "--log-prefix", f"{work}/log",
             "fid"],
            env={**os.environ, "PGPASSWORD": "app-pw"}, capture_output=True, text=True,
            cwd=work)
        latencies, failed = [], 0
        for log in glob.glob(f"{work}/log*"):
            for line in pathlib.Path(log).read_text().splitlines():
                fields = line.split()
                if len(fields) >= 3 and fields[2].isdigit():
                    latencies.append(int(fields[2]) / 1000.0)
                elif len(fields) >= 3:
                    failed += 1
        errors = [line for line in done.stderr.splitlines() if "ERROR" in line or "error" in line]
        return latencies, failed, errors[:1]


def main():
    arguments = sys.argv[1:]
    options = {"--seconds": 10, "--rounds": 2, "--clients": 4}
    report = None
    for option in list(options):
        if option in arguments:
            index = arguments.index(option)
            options[option] = int(arguments[index + 1])
            del arguments[index:index + 2]
    if "--json" in arguments:
        index = arguments.index("--json")
        report = arguments[index + 1]
        del arguments[index:index + 2]
    only = None
    if "--only" in arguments:
        index = arguments.index("--only")
        only = arguments[index + 1]
        del arguments[index:index + 2]
    proxies = [argument.split("=") for argument in arguments]
    results = {}
    for name, (script, mode) in WORKLOADS.items():
        if only and only not in name:
            continue
        pooled = {proxy: ([], 0, []) for proxy, _ in proxies}
        for _ in range(options["--rounds"]):
            for proxy, port in proxies:
                latencies, failed, errors = run(
                    port, script, mode, options["--seconds"], options["--clients"])
                kept = pooled[proxy]
                pooled[proxy] = (kept[0] + latencies, kept[1] + failed, kept[2] or errors)
        row = {}
        for proxy, (latencies, failed, errors) in pooled.items():
            latencies.sort()
            row[proxy] = {
                "transactions": len(latencies),
                "p50_ms": round(percentile(latencies, 0.50), 2),
                "p95_ms": round(percentile(latencies, 0.95), 2),
                "p99_ms": round(percentile(latencies, 0.99), 2),
                "tps": round(len(latencies) / (options["--seconds"] * options["--rounds"]), 1),
                "error": errors[0] if errors and not latencies else None,
            }
        results[name] = row
        print(name)
        for proxy, numbers in row.items():
            if numbers["error"]:
                print(f"  {proxy:10} refused: {numbers['error'][:100]}")
            else:
                print(f"  {proxy:10} p50 {numbers['p50_ms']:8.2f} ms  p95 {numbers['p95_ms']:8.2f} ms"
                      f"  p99 {numbers['p99_ms']:8.2f} ms  {numbers['tps']:8.1f} tps")
        sys.stdout.flush()
    if report:
        pathlib.Path(report).write_text(json.dumps(results, indent=1))


if __name__ == "__main__":
    main()
