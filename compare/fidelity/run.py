#!/usr/bin/env python3
"""Runs the fidelity corpus through proxies and grades every answer.

Usage: run.py TRUTH_PORT NAME=PORT [NAME=PORT ...] [--json FILE]

Each statement in corpus.sql runs once directly against `fid_truth` (as
postgres, which every row is in) and once through each named proxy (as
app/app-pw, database `fid`). A read is correct when the proxy returns the
same rows, in any order; a write, which runs between BEGIN and ROLLBACK,
when it reports the same command tag. An error from the proxy is a refusal.
Anything else is wrong: an answer that is not the answer.
"""

import json
import os
import pathlib
import re
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent


def corpus():
    entries = []
    for block in re.split(r"(?m)^-- name: ", (HERE / "corpus.sql").read_text())[1:]:
        header, _, body = block.partition("\n")
        name, group, kind = (part.strip() for part in header.split("|"))
        sql = "\n".join(line for line in body.splitlines() if not line.startswith("--")).strip()
        entries.append({"name": name, "group": group, "write": kind == "write", "sql": sql})
    return entries


def run(port, user, password, database, sql, write):
    commands = ["-c", "BEGIN", "-c", sql, "-c", "ROLLBACK"] if write else ["-c", sql]
    done = subprocess.run(
        ["psql", "-X", "-A", "-t", "-F", "|", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1",
         "-p", str(port), "-U", user, "-d", database] + commands,
        env={**os.environ, "PGPASSWORD": password}, capture_output=True, text=True, timeout=60)
    if done.returncode != 0:
        message = next((line for line in done.stderr.splitlines() if "ERROR" in line), done.stderr)
        return {"error": message.strip()}
    lines = [line for line in done.stdout.splitlines() if line not in ("BEGIN", "ROLLBACK")]
    return {"rows": sorted(lines)}


def main():
    arguments = sys.argv[1:]
    report = None
    if "--json" in arguments:
        index = arguments.index("--json")
        report = arguments[index + 1]
        del arguments[index:index + 2]
    truth_port, proxies = arguments[0], [argument.split("=") for argument in arguments[1:]]
    results = []
    totals = {name: {"correct": 0, "refused": 0, "wrong": 0} for name, _ in proxies}
    for entry in corpus():
        truth = run(truth_port, "postgres", "", "fid_truth", entry["sql"], entry["write"])
        if "error" in truth:
            raise SystemExit(f"{entry['name']}: PostgreSQL itself rejects it: {truth['error']}")
        line = {"name": entry["name"], "group": entry["group"]}
        for name, port in proxies:
            answer = run(port, "app", "app-pw", "fid", entry["sql"], entry["write"])
            if "error" in answer:
                verdict = "refused"
            elif answer["rows"] == truth["rows"]:
                verdict = "correct"
            else:
                verdict = "wrong"
            totals[name][verdict] += 1
            line[name] = {"verdict": verdict, **answer}
        results.append(line)
        print(f"{entry['name']:40}" + "".join(
            f"{line[name]['verdict']:>10}" for name, _ in proxies))
    print()
    for name, counts in totals.items():
        print(f"{name}: {counts['correct']} correct, {counts['refused']} refused, "
              f"{counts['wrong']} wrong, of {len(results)}")
    if report:
        pathlib.Path(report).write_text(json.dumps({"totals": totals, "results": results}, indent=1))


if __name__ == "__main__":
    main()
