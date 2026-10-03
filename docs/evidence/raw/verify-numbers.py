#!/usr/bin/env python3
"""Recompute evidence from committed observations; fail closed on missing data.

--historical verifies the recovered archive alone. --write also renders final
sections; the normal gate only checks and never modifies documentation.
Only the Python standard library is needed.
"""
import argparse
from collections import Counter
import datetime as dt
from decimal import Decimal, ROUND_DOWN
import gzip
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import re
import struct
import subprocess

ROOT = Path(__file__).resolve().parents[3]
EVIDENCE = ROOT / "docs/evidence"
RAW = Path(__file__).resolve().parent
SHA = "904cb6c41da4f57ac0399d1524989288e3ededb1"
FINAL = RAW / SHA[:7]
POLISH_SHA = "0a80a08f7a0a3bb88afd4221e1fe9b085c48afb1"
POLISH = RAW / "polish-2026-09-28"
REVIEW_SHA = "59065273ba73f66109eea66779383eb58286f9a6"
# Wording pass after the review: comment-only source edits and documentation. Pinned so
# the source-drift and audit-report checks below still accept exactly those edits.
WORDING_SHA = "0000000000000000000000000000000000000000"
WORDING_FILES = {
    "Makefile", "cmd/capstan-lab/mutate.go", "conformance/README.md",
    "conformance/workflows.ts", "gen/capstan/v1/capstan.pb.go",
    "gen/capstan/v1/capstanv1connect/capstan.connect.go",
    "internal/store/pgstore/pgstore.go", "internal/store/store.go",
    "proto/capstan/v1/capstan.proto", "scripts/demo-idle-wait.mjs",
    "sdk/src/client/index.ts", "sdk/src/gen/capstan/v1/capstan_pb.ts",
    "sdk/src/testing/index.ts", "sdk/src/worker/index.ts",
    "sdk/src/workflow/agent.ts", "sdk/src/workflow/index.ts",
    "sdk/test/agent-live.test.ts",
}
REVIEW = RAW / "polish-review-2026-09-28"
SOURCE_PATHS = ["cmd", "internal", "sdk", "gen", "proto", "examples", "scripts",
                "Makefile", "go.mod", "go.sum", "compose.yaml", "conformance",
                ":(exclude)examples/codex-lanes", ":(exclude)scripts/demo-codex-lanes*"]
ORDER = [(50,1000,4),(200,1000,4),(800,1600,4),(50,1000,1),
         (50,1000,8),(25,1000,4),(10,500,4),(100,1000,4),
         (50,1000,4),(200,1000,4),(800,1600,4)]


def read(path):
    path = Path(path)
    if not path.exists() and Path(str(path) + ".gz").exists():
        path = Path(str(path) + ".gz")
    data = path.read_bytes()
    return (gzip.decompress(data) if path.suffix == ".gz" else data).decode()


def obj(path):
    return json.loads(read(path))


def jsonl(path):
    return [json.loads(line) for line in read(path).splitlines() if line]


def same(actual, expected, label="value"):
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys(), (label, actual.keys(), expected.keys())
        for key, value in expected.items():
            same(actual[key], value, f"{label}.{key}")
    elif isinstance(expected, list):
        assert len(actual) == len(expected), label
        for index, value in enumerate(expected):
            same(actual[index], value, f"{label}[{index}]")
    else:
        assert actual == expected, (label, actual, expected)


def trunc(value):
    return format(Decimal(str(value)).quantize(Decimal("0.0001"), rounding=ROUND_DOWN), ".4f")


def cells(line):
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def duration_seconds(value):
    units = {"h": 3600, "m": 60, "s": 1, "ms": .001, "µs": .000001, "ns": .000000001}
    parts = re.findall(r"([\d.]+)(ms|µs|ns|h|m|s)", value)
    assert "".join(a + b for a, b in parts) == value, value
    return sum(Decimal(a) * Decimal(str(units[b])) for a, b in parts)


def verify_load(folder):
    folder = Path(folder)
    meta, emitted, audit = [obj(folder / name) for name in ("metadata.json", "result.json", "audit.json")]
    rows, samples = jsonl(folder / "runs.jsonl"), jsonl(folder / "system.jsonl")
    assert len({r["run_id"] for r in rows}) == len(rows), "duplicate run sample"
    success = [r for r in rows if not r.get("error")]
    latencies = sorted(r["latency_ms"] for r in success)
    completed = len(success)
    assert completed > 0 and emitted["elapsed_seconds"] > 0
    derived = dict(runs=len(rows), concurrency=int(meta["command"].split()[-3]),
                   completed=completed, errors=len(rows)-completed,
                   activities=sum(r["activities"] for r in rows),
                   elapsed_seconds=emitted["elapsed_seconds"],
                   runs_per_second=completed/emitted["elapsed_seconds"],
                   activities_per_second=sum(r["activities"] for r in rows)/emitted["elapsed_seconds"],
                   p50_ms=latencies[math.ceil(completed*.5)-1],
                   p99_ms=latencies[math.ceil(completed*.99)-1])
    same(derived, emitted, str(folder))
    same(audit, {"runs": len(rows), "completed": completed, "other": len(rows)-completed,
                 "activity_completions": derived["activities"], "task_failures": 0, "remaining_tasks": 0}, "SQL")
    assert derived["activities"] == 5*completed and derived["errors"] == 0
    assert len(samples) >= 2
    for sample in samples:
        assert all(not (isinstance(sample[k], dict) and "error" in sample[k])
                   for k in ("processes", "postgres_cpu", "postgres")), sample
    elapsed = (dt.datetime.fromisoformat(samples[-1]["at"]) - dt.datetime.fromisoformat(samples[0]["at"])).total_seconds()
    assert elapsed > 0
    def cpu(row):
        return int(re.search(r"^usage_usec (\d+)$", row["postgres_cpu"], re.M)[1])
    def times(row):
        return {int(s.split()[0]): sum(float(p)*60**i for i,p in enumerate(reversed(s.split()[2].split(":"))))
                for s in row["processes"].splitlines()}
    before, after = times(samples[0]), times(samples[-1])
    cpu_percent = [(after[p]-before[p])/elapsed*100 for p in [meta["serverPID"], *meta["workerPIDs"]]]
    waits, other, occupancy = Counter(), set(), []
    for sample in samples:
        active = 0
        for a in sample["postgres"]["activity"]:
            if a["datname"] == meta["database"]:
                waits[f'{a["state"]} / {a["wait_event_type"]} / {a["wait_event"]}'] += a["n"]
                if a["state"] in ("active", "idle in transaction"):
                    active += a["n"]
            elif a["state"] in ("active", "idle in transaction"):
                other.add(a["datname"])
        occupancy.append(active)
    sampler = {
        "samples": len(samples), "window_seconds": elapsed,
        "postgres_cpu_percent": (cpu(samples[-1])-cpu(samples[0]))/elapsed/10000,
        "server_cpu_percent": cpu_percent[0], "worker_cpu_percent": cpu_percent[1:],
        "max_waiting_locks": max(s["postgres"]["waiting_locks"] for s in samples),
        "max_ready_tasks": max(s["postgres"]["tasks"]["ready"] for s in samples),
        "max_leased_tasks": max(s["postgres"]["tasks"]["leased"] for s in samples),
        "max_active_or_in_transaction": max(occupancy),
        "backend_observations": dict(waits), "other_active_databases": sorted(other),
        "deadlocks_delta": samples[-1]["postgres"]["database"]["deadlocks"]-samples[0]["postgres"]["database"]["deadlocks"],
        "transactions_delta": samples[-1]["postgres"]["database"]["xact_commit"]-samples[0]["postgres"]["database"]["xact_commit"],
    }
    return {"command": meta["command"], "date": meta["date"],
            "raw_directory": str(folder.relative_to(ROOT)) if folder.is_relative_to(ROOT) else str(folder),
            "result": derived, "audit": audit, "sampler": sampler}


def historical_load_tables(data):
    lines = read(EVIDENCE / "load.md").split("## Final code", 1)[0].splitlines()
    original = data[:11]
    after = [r for r in data if r.get("phase") in ("after", "after-repeat")]
    def result(r):
        d = r["result"]
        return [str(d["concurrency"]),str(d["runs"]),r["command"].split()[-1],
                *[trunc(d[k]) for k in ("runs_per_second","activities_per_second","p50_ms","p99_ms")],str(d["errors"])]
    def cpu(r):
        s = r["sampler"]
        return [str(s["samples"]),trunc(s["postgres_cpu_percent"]),trunc(s["server_cpu_percent"]),trunc(sum(s["worker_cpu_percent"]))]
    got = [cells(l) for l in lines if re.match(r"^\| (10|25|50|100|200|800) \| \d+ \| [148] \|", l)]
    same(got, [result(d) for d in original], "historical result table")
    got = [cells(l) for l in lines if re.match(r"^\| (10|25|50|100|200|800) / [148] \| \d+ \|", l)]
    same(got, [[f'{d["result"]["concurrency"]} / {d["command"].split()[-1]}', *cpu(d),
                str(d["sampler"]["max_ready_tasks"]),str(d["sampler"]["max_waiting_locks"])] for d in original])
    got = [cells(l) for l in lines if re.match(r"^\| after(?:-repeat)? / \d+ \| \d+ \|", l)]
    same(got, [[f'{d["phase"]} / {i%11+1}',*result(d),"yes" if d["sampler"]["other_active_databases"] else "no"]
               for i,d in enumerate(after)])
    got = [cells(l) for l in lines if re.match(r"^\| after(?:-repeat)? / \d+ \| \d+ / \d+ \|", l)]
    same(got, [[f'{d["phase"]} / {i%11+1}',f'{d["result"]["concurrency"]} / {d["command"].split()[-1]}',*cpu(d),
                *[str(d["sampler"][k]) for k in ("max_active_or_in_transaction","max_ready_tasks","max_waiting_locks","deadlocks_delta")]]
               for i,d in enumerate(after)])
    got = [cells(l)[1:] for l in lines if re.match(r"^\| (Original,|D30,|Targeted queue|Run/child)", l)]
    expected = []
    for ix, iy in [(11,12),(18,19),(20,21),(33,34),(36,37)]:
        x,y = data[ix],data[iy]
        expected.append([trunc(x["result"]["runs_per_second"]),trunc(y["result"]["runs_per_second"]),
                         trunc(x["result"]["p99_ms"]),trunc(y["result"]["p99_ms"]),
                         f'{x["sampler"]["transactions_delta"]:,} / {y["sampler"]["transactions_delta"]:,}'])
    same(got, expected)
    got = [cells(l) for l in lines if re.match(r"^\| (10|20|40|60) \| [\d.]+",l) and len(cells(l))==3]
    same(got, [[str(pool),trunc(data[x]["result"]["runs_per_second"])+(" *" if data[x]["sampler"]["other_active_databases"] else ""),
                trunc(data[y]["result"]["runs_per_second"])] for pool,x,y in [(10,24,25),(20,18,19),(40,20,21),(60,22,23)]])
    def span(values):
        low, high = min(values), max(values)
        return trunc(low) if low == high else f"{trunc(low)}–{trunc(high)}"
    got = [cells(l) for l in lines if re.match(r"^\| (10|25|50|100|200|800) / [148] \|", l) and len(cells(l))==5]
    expected = []
    for c,w in [(10,4),(25,4),(50,4),(100,4),(200,4),(800,4),(50,1),(50,8)]:
        groups = [[d for d in group if d["result"]["concurrency"]==c and int(d["command"].split()[-1])==w] for group in [original,after]]
        expected.append([f"{c} / {w}",*[span([d["result"][k] for d in group]) for k in ("runs_per_second","p99_ms") for group in groups]])
    same(got, expected)
    same([len(original),sum(d["result"]["runs"] for d in original),sum(d["result"]["activities"] for d in original)], [11,11700,58500])
    same([len(after),sum(d["result"]["runs"] for d in after),sum(d["result"]["activities"] for d in after)], [22,23400,117000])
    samples = [s for d in after for s in jsonl(ROOT / d["raw_directory"] / "system.jsonl")]
    locks = Counter(w["wait_event"] for s in samples for w in s["postgres"]["lock_waits"])
    same([len(samples),sum(s["postgres"]["waiting_locks"]>0 for s in samples),locks["transactionid"],locks["object"]], [331,17,17,6])
    assert sum(bool(d["sampler"]["other_active_databases"]) for d in after)==22
    assert all(d["sampler"]["deadlocks_delta"]==0 for d in after)
    original_samples = [s for d in original for s in jsonl(ROOT / d["raw_directory"] / "system.jsonl")]
    lwlocks = Counter()
    for sample in original_samples:
        for a in sample["postgres"]["activity"]:
            if a["wait_event_type"]=="LWLock":
                lwlocks[a["wait_event"]] += a["n"]
    same(dict(lwlocks), {"LockManager":1,"SubtransSLRU":1,"BufferContent":2})
    print("PASS historical load: every table cell, range, percentile, CPU delta and lock observation")


def fields(text):
    return dict(re.findall(r"^- ([^:]+): (.+)$", text, re.M))


def table_counts(text):
    return {m[0]:int(m[1]) for m in re.findall(r"^\| ([a-z][a-z0-9-]+) \| (\d+) \|$", text, re.M)}


def verify_mutations(folder, require_current=False, patch_revision=None):
    report = obj(folder / "results.json")
    results = report["results"]
    assert len(results)==26 and report["seeds"]==2000
    statuses = Counter()
    baseline = jsonl(folder / "baseline.jsonl")
    assert any(e.get("Action")=="pass" and e.get("Test")=="TestLab" for e in baseline)
    traces = [int(n) for e in baseline for n in re.findall(r"mutation seed (\d+)",e.get("Output",""))]
    same(traces,list(range(report["seeds"])))
    for row in results:
        name = row["mutant"]["id"]
        events = jsonl(folder / f"{name}.jsonl")
        text = "".join(e.get("Output","") for e in events)
        seeds = [int(n) for n in re.findall(r"mutation seed (\d+)",text)]
        assert seeds == list(range(len(seeds))), (name,"noncontiguous seed trace")
        if row["status"]=="caught":
            assert any(e.get("Action")=="fail" and e.get("Test")=="TestLab" for e in events)
            failures = re.findall(r"lab_test.go:\d+: seed (\d+):? (.+)", text)
            assert failures, name
            same(row["seed"],int(failures[0][0]),name)
            same(row["check"],failures[0][1].strip(),name)
            assert seeds[-1]==row["seed"]
        elif row["status"]=="equivalent":
            assert name=="M003" and row["mutant"]["equivalent_reason"]
            assert any(e.get("Action")=="pass" and e.get("Test")=="TestLab" for e in events)
            assert len(seeds)==report["seeds"]
        else:
            raise AssertionError((name,row["status"]))
        if require_current or patch_revision:
            patch = "internal/lab/mutants/" + Path(row["mutant"]["patch"]).name
            data = (ROOT / patch).read_bytes() if require_current else subprocess.check_output(
                ["git", "show", f"{patch_revision}:{patch}"], cwd=ROOT)
            same(hashlib.sha256(data).hexdigest(),row["patch_sha256"],name)
        statuses[row["status"]] += 1
    same(dict(statuses), {"caught":25,"equivalent":1})
    return report


def historical():
    manifest = obj(RAW / "historical/manifest.json")
    for file in manifest:
        data = (ROOT / file["path"]).read_bytes()
        same(len(data),file["bytes"],file["path"])
        same(hashlib.sha256(data).hexdigest(),file["sha256"],file["path"])
        original = gzip.decompress(data) if file["path"].endswith(".gz") else data
        same(len(original),file["source_bytes"],file["path"])
        same(hashlib.sha256(original).hexdigest(),file["source_sha256"],file["path"])
    data = obj(EVIDENCE / "load-results.json")
    assert len(data)==61 and len({r["raw_directory"] for r in data})==61
    for row in data:
        assert row["original_raw_directory"] != row["raw_directory"]
        derived = verify_load(ROOT / row["raw_directory"])
        for key,value in derived.items():
            same(row[key],value,f'{row["raw_directory"]}.{key}')
    historical_load_tables(data)
    for report_name,log_name in [("lab-2026-09-28.md","l2-campaign-200000.log"),("lab-delta1-2026-09-28.md","delta-campaign.log")]:
        original = read(RAW / "historical/lab" / report_name)
        current = read(EVIDENCE / report_name).split("## Final code",1)[0]
        same(fields(current),fields(original),report_name)
        same(table_counts(current),table_counts(original),report_name)
        f = fields(original)
        log = read(RAW / "historical/lab" / log_name)
        assert f'Runs: {f["Runs"]}; passed: {f["Passed"]}; failing seeds: {f["Failing seeds"]}' in log
        assert f'elapsed: {f["Elapsed"]}' in log
    delta = fields(read(RAW / "historical/lab/lab-delta1-2026-09-28.md"))
    assert int(delta["Root runs"])==2*int(delta["Runs"])
    mutation = verify_mutations(RAW / "historical/lab/mutations")
    doc = read(EVIDENCE / "lab-mutation.md").split("## Final code",1)[0]
    table = {cells(l)[0]:cells(l) for l in doc.splitlines() if re.match(r"^\| M\d{3} \|",l)}
    for row in mutation["results"]:
        c = table[row["mutant"]["id"]]
        same(c[2],row["status"]); same(-1 if c[3]=="—" else int(c[3]),row["seed"])
    caught = sum(r["status"]=="caught" for r in mutation["results"])
    equivalents = sum(r["status"]=="equivalent" for r in mutation["results"])
    assert f"Caught **{caught}/{len(mutation['results'])} valid mutants ({caught/len(mutation['results'])*100:.2f}%)**; equivalent survivors: **{equivalents}**." in doc
    assert f"**{caught}/{len(mutation['results'])-equivalents} (100.00%)**" in doc
    checks = {
        "historical/agent/agent-delta1-gate-e2e.log": [
            "✓ test/agent-e2e.test.ts > agent end to end with the real AgentOps Gate > ALLOW executes the registered tool once",
            "✓ test/agent-e2e.test.ts > agent end to end with the real AgentOps Gate > DENY completes without executing the registered tool",
            "✓ test/agent-e2e.test.ts > agent end to end with the real AgentOps Gate > a real Gate approval waits durably while the single-slot worker runs other work",
            "✓ test/agent-e2e.test.ts > agent end to end with the real AgentOps Gate > survives SIGKILL during approval, observes approval with no worker, and resumes once",
            "Test Files  1 passed (1)", "Tests  4 passed (4)",
        ],
        "historical/evidence/verify.log": ["295 passed", "13 passed"],
        "historical/lab/gate-make-verify-l2-final.log": ["293 passed"],
        "historical/lab/l2-2000-final.log": ["validated 2000 seeds", "(16.87s)"],
        "historical/lab/delta-make-verify-final.log": ["52.109s", "369 passed", "5 skipped"],
        "historical/perf/merged-verify-reviewed.log": ["369 passed", "18 passed", "2 skipped", "5 skipped"],
        "historical/perf/targeted-before.log": ["claim_attempts=40 claimed=1", "control_before=8 control_after=9 control_delta=1", "xact_commit_before=9 xact_commit_after=50 xact_commit_delta=41"],
        "historical/perf/targeted-after-reviewed.log": ["claim_attempts=1 claimed=1", "control_delta=1", "xact_commit_before=9 xact_commit_after=11 xact_commit_delta=2", "residual_commits=0"],
        "historical/perf/lock-red-race.log": ["deadlock retries = 1"],
        "historical/lab/delta-pg-unscaled-probe.log": ["1000 steps", "elapsed=3.771993333s"],
        "historical/lab/delta-pg-green.log": ["steps=975 duplicate_acks=149 database_errors=5 server_crashes=5 elapsed=6.313"],
        "historical/lab/delta-pg-500.log": ["steps=53835 duplicate_acks=7589 database_errors=250 server_crashes=250 elapsed=9m12.646540041s"],
        "historical/lab/delta-pg-500-final.log": ["seed 377", "context deadline exceeded", "600.208"],
        "historical/lab/delta-pg-500-uncontended.log": ["steps=53835 duplicate_acks=7589 database_errors=250 server_crashes=250 elapsed=5m22.944687959s", "(323.05s)", "323.388s"],
    }
    for path, snippets in checks.items():
        text = read(RAW / path)
        for snippet in snippets:
            assert snippet in text, (path,snippet)
    # Check historical timing prose against its actual log, including rounding.
    pg_doc = read(EVIDENCE / "lab-pg.md").split("## Final code",1)[0]
    for filename, phrase in [
        ("delta-pg-unscaled-probe.log","3.772 seconds"),
        ("delta-pg-green.log","6.313 seconds"),
        ("delta-pg-500.log","9 minutes 12.647 seconds"),
        ("delta-pg-500-uncontended.log","5 minutes 22.945 seconds"),
    ]:
        elapsed = re.search(r"elapsed[=:](\S+)",read(RAW/"historical/lab"/filename))[1]
        seconds = duration_seconds(elapsed)
        if "minutes" in phrase:
            actual = f"{int(seconds//60)} minutes {seconds%60:.3f} seconds"
        else:
            actual = f"{seconds:.3f} seconds"
        same(actual,phrase)
        assert phrase in pg_doc
    for snippet in ("975 actor steps","149 duplicate acknowledgements","53,835 actor steps",
                    "7,589 duplicate acknowledgements","250 database errors","250 server crashes",
                    "600.208 seconds","seed 377","2,000 completed run executions"):
        assert snippet in pg_doc,snippet
    l2 = read(EVIDENCE/"lab-l2.md").split("## Final code",1)[0]
    for snippet in ("16.87 seconds","52.109 seconds","46,179,964","148,554","9,870,634","293 SDK tests"):
        assert snippet in l2,snippet
    old_races = subprocess.check_output(["git","show","df11bda:internal/engine/pg_poll_lock_test.go"],cwd=ROOT,text=True)
    kinds = re.search(r'for _, kind := range \[\]string\{([^}]+)\}',old_races.split("func TestPostgresTerminateAgainstTaskOperations",1)[1])[1]
    orders = re.search(r'for _, first := range \[\]string\{([^}]+)\}',old_races)[1]
    assert len(re.findall(r'"[^"]+"',kinds))*len(re.findall(r'"[^"]+"',orders))==16
    assert "16 ordered termination races" in read(EVIDENCE/"load.md")
    print(f"PASS historical archive: {len(manifest)} hashes; lab aggregates, mutation seeds, gates and timing/control logs")
    print("LIMIT historical lab fault/step totals have emitted aggregate reports, not per-seed records")
    return data


def final_load():
    rows = []
    for i,point in enumerate(ORDER,1):
        log = read(FINAL / f"load-{i:02}.log")
        assert f"Measured source SHA: {SHA}" in log and "Exit: 0" in log
        folder = re.search(r"^Raw logs: .lane/(.+)$",log,re.M)[1]
        row = verify_load(FINAL / "load" / folder)
        meta = obj(FINAL / "load" / folder / "metadata.json")
        same(tuple(map(int,meta["command"].split()[-3:])),point)
        same(meta["phase"],"final"); same(meta["port"],7701)
        assert meta["database"].startswith("capstan_final_")
        assert meta["dbMaxConns"]=="default"
        row["source_sha"] = SHA
        row["measurement"] = i
        rows.append(row)
    return rows


def final_lab():
    records = jsonl(FINAL / "lab-seeds.jsonl")
    assert sorted(r["seed"] for r in records)==list(range(200000))
    failures = [r for r in records if r["error"]]
    assert not failures, failures[:3]
    assert len({r["scenario"] for r in records})==12
    assert all(len(r["faults"])<=6 and len(set(r["faults"]))==len(r["faults"]) for r in records)
    faults = Counter(f for r in records for f in r["faults"])
    responses = Counter(g for r in records for g in (r["gate_responses"] or []))
    report = read(FINAL / "lab-report.md")
    f = fields(report)
    counts = {"Runs":len(records),"Passed":len(records)-len(failures),"Failing seeds":len(failures),
              "Recognized known failures":0,"Unrecognized failures":0,
              "Total steps":sum(r["steps"] for r in records),
              "Root runs":sum(r["root_runs"] for r in records),
              "Store transaction steps":sum(r["transaction_steps"] for r in records)}
    for key,value in counts.items():
        same(int(f[key]),value,key)
    same(table_counts(report),dict(faults|responses))
    assert len(faults)==9 and len(responses)==5
    assert f["Seed range"]=="0..199999" and f["Fault selection"]=="all"
    assert f["Parallelism"]=="8" and f["Workers per seed"]=="3" and f["Maximum steps per seed"]=="1000"
    assert f["Scheduling stopped"]=="seed limit reached" and f["Time budget"]=="unlimited"
    log = read(FINAL / "lab.log")
    assert f"elapsed: {f['Elapsed']}" in log and "Exit: 0" in log and SHA in log
    return dict(counts, elapsed=f["Elapsed"], faults=dict(faults), gate_responses=dict(responses))


def final_pg():
    text = read(FINAL / "pg.log")
    rows = [json.loads(s) for s in re.findall(r'(\{"seed":\d+,[^\n]+\})', text)]
    assert [r["seed"] for r in rows]==list(range(500))
    total = {k:sum(r[k] for r in rows) for k in ("steps","duplicate_acks","database_errors","server_crashes")}
    m = re.search(r"validated (\d+) PostgreSQL seeds; two shared-queue runs/seed; steps=(\d+) duplicate_acks=(\d+) database_errors=(\d+) server_crashes=(\d+) elapsed=(\S+)",text)
    assert m and int(m[1])==len(rows)
    same(list(total.values()),list(map(int,m.group(2,3,4,5))))
    assert "--- PASS: TestPostgresCampaign" in text and "Exit: 0" in text and SHA in text
    return dict(total,seeds=len(rows),executions=4*len(rows),elapsed=m[6])


def artifacts():
    inspection = obj(EVIDENCE/"audit-2026-09-28/evidence/artifact-inspection.json")
    for name,info in inspection["images"].items():
        # The blocked screenshot was refreshed by polish. Keep checking the
        # auditor's original geometry against its original image, not the new UI.
        if name == "ui-blocked.png":
            data = subprocess.check_output(
                ["git", "show", f"{SHA}:docs/evidence/{name}"], cwd=ROOT)
        else:
            data = (EVIDENCE/name).read_bytes()
        if name.endswith(".png"):
            assert data[:8]==b"\x89PNG\r\n\x1a\n"
            same(list(struct.unpack(">II",data[16:24])),[info["width"],info["height"]],name)
        elif name.endswith(".gif"):
            assert data[:6] in (b"GIF87a",b"GIF89a")
            same(list(struct.unpack("<HH",data[6:10])),[info["width"],info["height"]],name)
            pos = 13 + (3*(2**((data[10]&7)+1)) if data[10]&128 else 0)
            frames,centiseconds,delay = 0,0,0
            def subblocks(pos):
                while data[pos]:
                    pos += 1+data[pos]
                return pos+1
            while data[pos]!=0x3b:
                block = data[pos]; pos += 1
                if block==0x21:
                    kind = data[pos]; pos += 1
                    if kind==0xf9:
                        assert data[pos]==4
                        delay = struct.unpack("<H",data[pos+2:pos+4])[0]
                    pos = subblocks(pos)
                elif block==0x2c:
                    packed = data[pos+8]; pos += 9
                    if packed&128:
                        pos += 3*2**((packed&7)+1)
                    pos += 1
                    pos = subblocks(pos)
                    frames += 1; centiseconds += delay; delay = 0
                else:
                    raise AssertionError((name,pos,block))
            same(frames,info["frames"])
            same(str(Decimal(centiseconds)/100),info["duration_seconds"])
    for path in EVIDENCE.glob("*.cast"):
        events = [json.loads(line,parse_float=Decimal) for line in read(path).splitlines()]
        assert events[0]["version"]==2 and set(events[0]["env"])=={"TERM"}
        assert all(a[0]<=b[0] for a,b in zip(events[1:],events[2:]))
        if path.name not in inspection["recordings"]:
            # Recorded after the audit's inspection (workload lane); structure checked above.
            print(f"recording {path.name}: structure OK; not in the audit inspection manifest")
            continue
        reported = inspection["recordings"][path.name]
        same(len(events)-1,reported["events"])
        same(str(events[-1][0]),reported["duration_seconds"])
        for pause in reported["announced_pauses"]:
            index = next(i for i,e in enumerate(events[1:],1)
                         if pause["marker"] in e[2] and str(e[0])==pause["at_seconds"])
            same(str(events[index+1][0]-events[index][0]),pause["gap_seconds"])
        text = "".join(e[2] for e in events[1:] if e[1]=="o")
        assert "PASS:" in text
        if path.stem=="demo-cost":
            assert "|     0.010000 | 0.000034 |           14 |             4 |" in text
            assert Decimal(14)/1000000+Decimal(4)*5/1000000==Decimal("0.000034")
    fixtures = [obj(p) for p in (ROOT / "conformance/fixtures").glob("*.json")]
    shared = [f for f in fixtures if f.get("only")!=["ts"]]
    same([len(fixtures),len(shared),sum("commands" in f["expect"] for f in shared),sum("mismatch" in f["expect"] for f in shared)],[54,53,39,14])
    exports = set(re.findall(r"^export (?:async )?function (\w+)",read(ROOT / "conformance/workflows.ts"),re.M))
    registry = read(ROOT/"internal/lab/scenarios/conformance.go").split("return map[string]labworker.Workflow{",1)[1].split("\n\t}",1)[0]
    assert len(exports)==29 and exports==set(re.findall(r'"(\w+)":',registry))
    tx = read(ROOT / "internal/store/store.go").split("type Tx interface {",1)[1].split("\n}",1)[0]
    methods = set(re.findall(r"^\s*([A-Z]\w*)\(",tx,re.M))
    wrappers = set()
    for path in (ROOT / "internal/lab").glob("*.go"):
        if not path.name.endswith("_test.go"):
            wrappers.update(re.findall(r"^func \(\w+ \*faultTx\) (\w+)\(",read(path),re.M))
    assert len(methods)==35 and not methods-wrappers
    audit = read(EVIDENCE / "audit-2026-09-28.md").split("## Post-audit resolution",1)[0]
    counts = Counter(re.findall(r"^\| [SDECR]\d+ \| \*\*(VERIFIED|FAILED|NOT CHECKED)\*\*",audit,re.M))
    same(dict(counts),{"VERIFIED":114,"FAILED":16,"NOT CHECKED":16})
    for c in (50,200):
        verify_load(EVIDENCE/f"audit-2026-09-28/evidence/load-c{c}")
    verify_mutations(EVIDENCE/"audit-2026-09-28/evidence/mutations-fixed")
    print("PASS image geometry/timing, recordings/cost arithmetic, source inventories and preserved audit measurements")


def final_demos():
    for name in ("crash","blocked","idle-wait"):
        text = read(FINAL / f"demo-{name}.log")
        assert SHA in text and "Exit: 0" in text and "PASS:" in text
    before = obj(FINAL / "demo-crash/history-before.json")
    after = obj(FINAL / "demo-crash/history-after.json")
    same(after[:len(before)],before)
    same(sum("activityCompleted" in e for e in after),4)
    timers = [e["timerFired"]["startedEventId"] for e in after if "timerFired" in e]
    assert len(timers)==len(set(timers))==3
    decoder = json.JSONDecoder()
    def cli_objects(text):
        return [decoder.raw_decode(text[m.start():])[0] for m in re.finditer(r"^\{\n",text,re.M)]
    blocked = read(FINAL/"demo-blocked.log")
    descriptions = cli_objects(blocked)
    old = [r for r in descriptions if r.get("runId")=="evidence-patch-old"]
    new = [r for r in descriptions if r.get("runId")=="evidence-patch-new"]
    assert any(r.get("status")=="blocked" and "event 5" in r["failure"]["message"] for r in old)
    assert any(r.get("status")=="completed" and r.get("result")=={"value":1} for r in old)
    assert any(r.get("status")=="completed" and r.get("result")=={"value":10} for r in new)
    assert "value 1, one activity completion" in blocked
    idle = read(FINAL/"demo-idle-wait.log")
    assert idle.count("Worker processes: 0")==2
    assert len(re.findall(r"^\s*0\s*\|\s*0\s*$",idle,re.M))==2
    assert "Waiting 5 real seconds" in idle
    assert any(r.get("status")=="completed" and r.get("result")=={"approved":True} for r in cli_objects(idle))
    return len(before)


def operational_proofs():
    gate = read(FINAL/"verify.log")
    assert SHA in gate and "Exit: 0" in gate
    assert all(command in gate for command in ("buf generate","buf lint","go vet","go test -race","-tags pgengine","tsc --noEmit","vitest run"))
    assert re.search(r"Tests\s+\d+ passed",gate)
    human = read(FINAL/"human-seven-days.log")
    assert "PASS: TestAuditPostgresHumanWaitSevenDays" in human and "Exit: 0" in human and SHA in human
    controls = read(FINAL/"controls.log")
    assert "claim_attempts=1 claimed=1" in controls and "Exit: 0" in controls and SHA in controls
    assert len(re.findall(r"--- PASS: TestPostgresTerminateAgainstTaskOperations/[^ ]+ \(",controls))==16
    quickstart = read(FINAL/"quickstart.log")
    assert SHA in quickstart and "Clean shell: env -i" in quickstart
    assert '"status": "completed"' in quickstart and '"result": 5' in quickstart and "/ui/ HTTP 200" in quickstart
    assert "owned database dropped; shared PostgreSQL left running" in quickstart
    browser = read(FINAL/"quickstart-ui.log")
    assert "quickstart-1" in browser and "Run completed" in browser and '\\"value\\": 5' in browser
    assert "final-quickstart' closed" in browser
    readme = read(ROOT/"README.md")
    setup = re.search(r"<!-- quickstart:start -->\n```bash\n(.*?)```",readme,re.S)[1]
    cleanup = re.search(r"<!-- quickstart:cleanup -->\n```bash\n(.*?)```",readme,re.S)[1]
    same(read(FINAL/"quickstart-commands.sh"),setup+"\n"+cleanup,"tested README commands")
    # The report text is pinned at the wording pass; its row statuses are counted against the
    # measured revision above, so measurements cannot drift behind a wording change.
    original = subprocess.check_output(["git","show",f"{WORDING_SHA}:docs/evidence/audit-2026-09-28.md"],cwd=ROOT,text=True)
    original = original.split("## Post-audit resolution",1)[0]
    current = read(EVIDENCE/"audit-2026-09-28.md")
    assert current.startswith(original), "auditor's original report changed"
    failed = set(re.findall(r"^\| ([A-Z]\d+) \| \*\*FAILED\*\*",original,re.M))
    resolution = current.split("## Post-audit resolution",1)[1].split("\n## ",1)[0]
    assert failed==set(re.findall(r"^\| ([A-Z]\d+) \|",resolution,re.M))
    print("PASS final gate, human wait, 16 ordered races, quickstart/browser and all FAILED-row resolutions")


def polish_provenance():
    # Historical observations still describe SHA. Pin the reviewed refactors and
    # viewer fix separately; no unlisted source drift is accepted by this gate.
    changed = subprocess.check_output(
        ["git", "diff", "--name-only", SHA, POLISH_SHA, "--", *SOURCE_PATHS],
        cwd=ROOT, text=True).splitlines()
    same(set(changed), {
        "internal/engine/commands.go", "internal/engine/workflow_task.go",
        "internal/lab/mutants/003-started-token-check.patch",
        "internal/lab/mutants/016-expired-workflow-completion.patch",
        "internal/server/ui/static/app.js", "internal/store/pgstore/tasks.go",
        # Pre-existing 71601cf waits for test cleanup before cancelling its context.
        "internal/store/pgstore/edges_test.go",
        "scripts/check-ui.mjs", "scripts/evidence-lib.mjs",
        "sdk/src/replay/activation.ts", "sdk/src/replay/runtime.ts",
        "sdk/src/sandbox/globals.ts", "sdk/src/worker/builtins/model.ts",
    }, "reviewed source changes since the measured revision")
    reviewed = subprocess.check_output(
        ["git", "diff", "--name-only", POLISH_SHA, REVIEW_SHA, "--", *SOURCE_PATHS],
        cwd=ROOT, text=True).splitlines()
    same(set(reviewed), {
        "internal/engine/workflow_task.go", "internal/engine/activity_task.go",
        "internal/engine/commands.go",
        "internal/lab/mutants/003-started-token-check.patch",
        "internal/lab/mutants/016-expired-workflow-completion.patch",
        "internal/lab/mutants/017-expired-activity-completion.patch",
    }, "grouped token fences and refreshed mutant contexts")
    # 6d7db69 changed only the held-transaction test helper after the review; no campaign runs it.
    worded = subprocess.check_output(
        ["git", "diff", "--name-only", REVIEW_SHA, WORDING_SHA, "--", *SOURCE_PATHS,
         ":(exclude)internal/store/pgstore/edges_test.go"],
        cwd=ROOT, text=True).splitlines()
    same(set(worded), WORDING_FILES, "comment and wording edits after the review")
    subprocess.run(["git", "diff", "--exit-code", WORDING_SHA, "--", *SOURCE_PATHS,
                    ":(exclude)internal/store/pgstore/edges_test.go"],
                   cwd=ROOT, check=True)


def polish_proofs():
    before_folder = POLISH / "mutations-before"
    before_revision = obj(before_folder / "results.json")["revision"]
    before = verify_mutations(before_folder, patch_revision=before_revision)
    after = verify_mutations(POLISH / "mutations-after", patch_revision=POLISH_SHA)
    # The mutation runner snapshots committed production code. Keep this campaign
    # tied to the initial polish source; the review campaign below covers the new fences.
    subprocess.run(["git", "diff", "--exit-code", after["revision"], POLISH_SHA,
                    "--", "internal/engine", "internal/store", "internal/lab", "cmd/capstan-lab"],
                   cwd=ROOT, check=True)
    for old, new in zip(before["results"], after["results"]):
        same(new["mutant"]["id"], old["mutant"]["id"])
        same(new["status"], old["status"])
        same(new["seed"], old["seed"])
        # Protobuf diagnostic formatting can vary whitespace between executions.
        same(re.sub(r"\s+", " ", new["check"]), re.sub(r"\s+", " ", old["check"]))
    ui = obj(POLISH / "ui-check.json")
    same(ui["check"], "PASS")
    assert "blocked mismatch event 5 and timeline link" in ui["checks"]
    assert "old-history fallback event 14 and resume command" in ui["checks"]
    assert "mismatch link loads subsequent history pages" in ui["checks"]
    assert "mismatch link opens the correct event in a new tab" in ui["checks"]
    screenshot = (EVIDENCE / "ui-blocked.png").read_bytes()
    inspected_screenshot = subprocess.check_output(
        ["git", "show", f"{POLISH_SHA}:docs/evidence/ui-blocked.png"], cwd=ROOT)
    same(screenshot, inspected_screenshot, "inspected polish screenshot")
    same(list(struct.unpack(">II", screenshot[16:24])), [1280, 2120], "polish screenshot geometry")
    gate = read(POLISH / "verify.txt")
    assert POLISH_SHA in gate and "Exit: 0" in gate
    commands = ("buf generate", "buf lint", "go vet", "go test -race",
                "-tags pgengine", "tsc --noEmit", "vitest run")
    assert all(command in gate for command in commands)
    print("PASS polish: pinned source, merge gate, viewer event 5; 26 valid mutants, 25 caught, 1 equivalent; unchanged seeds/checks")

    reviewed = verify_mutations(REVIEW / "mutations", require_current=True)
    same(reviewed["revision"], REVIEW_SHA, "grouped fences mutation source")
    for old, new in zip(after["results"], reviewed["results"]):
        same(new["mutant"]["id"], old["mutant"]["id"])
        same(new["status"], old["status"])
        same(new["seed"], old["seed"])
        same(re.sub(r"\s+", " ", new["check"]), re.sub(r"\s+", " ", old["check"]))
    gate = read(REVIEW / "verify.txt")
    assert REVIEW_SHA in gate and "Exit: 0" in gate
    assert all(command in gate for command in commands)
    print("PASS polish review: grouped fences, pinned source and merge gate; 26 valid mutants, 25 caught, 1 equivalent; unchanged seeds/checks")


def recorded_session_resolution():
    audit = read(EVIDENCE / "audit-2026-09-28.md")
    original = audit.split("## Post-audit resolution", 1)[0]
    recorded = audit.split("## Post-audit resolution (recorded sessions) — 2026-09-28", 1)[1]
    rows = re.findall(r"^\| ([A-Z]\d+) \|", recorded, re.M)
    expected = {"S05", "S30", "S34", *(f"R{i:02}" for i in range(3, 11))}
    same(set(rows), expected)
    same(len(rows), len(expected))
    unchecked = set(re.findall(r"^\| ([A-Z]\d+) \| \*\*NOT CHECKED\*\*", original, re.M))
    assert expected <= unchecked
    gate_row = next(line for line in recorded.splitlines() if line.startswith("| S30 |"))
    assert "**VERIFIED for four recorded local Gate cases**" in gate_row
    assert "raw/historical/agent/agent-delta1-gate-e2e.log" in gate_row
    assert "not a deployed Gate" in gate_row
    # Read only the retained receipts: this is not a new cloud or Gate session.
    proof = obj(EVIDENCE / "aws-2026-09-28-attempt-2-proof-summary.json")
    history = obj(EVIDENCE / "aws-2026-09-28-attempt-2-history.json")
    assert proof["prefixMatches"] and proof["taskBefore"] != proof["taskAfter"]
    same(len(history["before"]), proof["prefixEvents"])
    same(len(history["after"]), proof["finalEvents"])
    same(history["before"], history["after"][:proof["prefixEvents"]])
    final = obj(EVIDENCE / "aws-2026-09-28-attempt-2-final-verification.json")
    assert final["standingResourcesEmpty"] and not final["rawInventoryEmpty"]
    subscription = obj(EVIDENCE / "aws-2026-09-28-attempt-2-alarm-subscription.json")
    assert subscription and all(row["SubscriptionArn"] == "PendingConfirmation" for row in subscription)
    billing = obj(EVIDENCE / "aws-2026-09-28-attempt-2-billing-after.json")
    assert billing["ResultsByTime"][0]["Estimated"]
    print("PASS recorded-session resolution: all 11 open rows mapped; four local Gate cases and retained AWS receipt fields checked; no live service calls")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--historical",action="store_true")
    parser.add_argument("--write",action="store_true")
    args = parser.parse_args()
    polish_provenance()
    historical()
    artifacts()
    if args.historical:
        print("PASS historical numeric verification")
        return
    loads,lab,pg = final_load(),final_lab(),final_pg()
    mutations = verify_mutations(FINAL / "mutations", patch_revision=SHA)
    prefix = final_demos()
    operational_proofs()
    polish_proofs()
    recorded_session_resolution()
    output = {"source_sha":SHA,"load":loads,"lab":lab,"pg":pg,"crash_prefix":prefix,
              "mutation_statuses":dict(Counter(r["status"] for r in mutations["results"]))}
    destination = FINAL / "numbers.json"
    if args.write:
        destination.write_text(json.dumps(output,indent=2)+"\n")
    else:
        same(obj(destination),output,"final numbers")
    spec = importlib.util.spec_from_file_location("render_final",RAW / "render-final.py")
    render = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(render)
    render.check_documents(output,mutations,write=args.write)
    print(f"PASS final load: {len(loads)} measurements, {sum(r['result']['completed'] for r in loads)} completed, {sum(r['result']['activities'] for r in loads)} activities, 0 errors")
    print(f"PASS final lab: {lab['Passed']} seeds, {lab['Total steps']} steps, {lab['Store transaction steps']} transaction steps, 0 failures")
    print(f"PASS mutation catalogue: {len(mutations['results'])} valid, 25 caught, 1 equivalent; every first failing seed checked")
    print(f"PASS PostgreSQL: {pg['seeds']} seeds, {pg['steps']} steps, {pg['duplicate_acks']} duplicate acknowledgements")
    print("PASS every generated final numeric section matches committed observations")


if __name__ == "__main__":
    main()
