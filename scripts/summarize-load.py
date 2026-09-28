#!/usr/bin/env python3
"""Summarize actual sampler observations; CPU 100% means one logical core."""
import datetime as dt
import json
import re
import sys
from pathlib import Path


def seconds(value):
    parts = value.split(":")
    return sum(float(part) * (60 ** i) for i, part in enumerate(reversed(parts)))


def summarize(path):
    path = Path(path)
    metadata = json.loads((path / "metadata.json").read_text())
    samples = [json.loads(line) for line in (path / "system.jsonl").read_text().splitlines()]
    for sample in samples:
        if any(isinstance(sample[name], dict) and "error" in sample[name] for name in ["processes", "postgres_cpu", "postgres"]):
            raise ValueError(f"sample failure: {sample}")
    elapsed = (dt.datetime.fromisoformat(samples[-1]["at"]) - dt.datetime.fromisoformat(samples[0]["at"])).total_seconds()
    cpu = lambda row: int(re.search(r"^usage_usec (\d+)$", row["postgres_cpu"], re.M)[1])
    times = lambda row: {int(line.split()[0]): seconds(line.split()[2]) for line in row["processes"].splitlines()}
    before, after = times(samples[0]), times(samples[-1])
    pids = [metadata["serverPID"], *metadata["workerPIDs"]]
    cpu_percent = [(after[pid] - before[pid]) / elapsed * 100 for pid in pids]
    own = metadata["database"]
    waits = {}
    other = set()
    pool_occupancy = []
    for sample in samples:
        occupancy = 0
        for activity in sample["postgres"]["activity"]:
            if activity["datname"] == own:
                label = f'{activity["state"]} / {activity["wait_event_type"]} / {activity["wait_event"]}'
                waits[label] = waits.get(label, 0) + activity["n"]
                if activity["state"] in ("active", "idle in transaction"):
                    occupancy += activity["n"]
            elif activity["state"] in ("active", "idle in transaction"):
                other.add(activity["datname"])
        pool_occupancy.append(occupancy)
    return {
        "command": metadata["command"], "date": metadata["date"], "raw_directory": str(path),
        "result": json.loads((path / "result.json").read_text()), "audit": json.loads((path / "audit.json").read_text()),
        "sampler": {
            "samples": len(samples), "window_seconds": elapsed,
            "postgres_cpu_percent": (cpu(samples[-1])-cpu(samples[0])) / elapsed / 10000,
            "server_cpu_percent": cpu_percent[0], "worker_cpu_percent": cpu_percent[1:],
            "max_waiting_locks": max(s["postgres"]["waiting_locks"] for s in samples),
            "max_ready_tasks": max(s["postgres"]["tasks"]["ready"] for s in samples),
            "max_leased_tasks": max(s["postgres"]["tasks"]["leased"] for s in samples),
            "max_active_or_in_transaction": max(pool_occupancy),
            "backend_observations": waits, "other_active_databases": sorted(other),
            "deadlocks_delta": samples[-1]["postgres"]["database"]["deadlocks"]-samples[0]["postgres"]["database"]["deadlocks"],
            "transactions_delta": samples[-1]["postgres"]["database"]["xact_commit"]-samples[0]["postgres"]["database"]["xact_commit"],
        },
    }

if __name__ == "__main__":
    print(json.dumps([summarize(path) for path in sys.argv[1:]], indent=2))
