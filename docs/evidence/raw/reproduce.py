#!/usr/bin/env python3
"""Record final measurements without changing the measured engine or SDK.

Local JS copies change only resource ownership, displayed provenance, and metrics
authentication. Go overlays emit per-seed observations; all assertions remain intact.
"""
import gzip
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
RAW = Path(__file__).resolve().parent
SOURCE = "904cb6c41da4f57ac0399d1524989288e3ededb1"
OUT = RAW / SOURCE[:7]
SCRATCH = ROOT / ".lane/final-scripts"
ENV = dict(os.environ, GOTOOLCHAIN="go1.26.4", CAPSTAN_LOAD_PHASE="final",
           CAPSTAN_LOAD_PORT="7701", CAPSTAN_LOAD_DB_PREFIX="capstan_final",
           CAPSTAN_E2E_PORT="7799")
ENV["PATH"] = str(Path.home() / ".local/bin") + ":" + ENV["PATH"]
ENV.pop("CAPSTAN_DB_MAX_CONNS", None)
ENV.pop("CAPSTAN_LIVE", None)
ENV.pop("CAPSTAN_GATE_E2E", None)
ORDER = [(50,1000,4),(200,1000,4),(800,1600,4),(50,1000,1),
         (50,1000,8),(25,1000,4),(10,500,4),(100,1000,4),
         (50,1000,4),(200,1000,4),(800,1600,4)]
KEEP = {"metadata.json", "runs.jsonl", "system.jsonl", "result.json",
        "audit.json", "load-errors.log", "sampling.sql", "metrics-before.txt",
        "metrics-after.txt", "history-before.json", "history-after.json"}


def replace(text, old, new):
    assert old in text, f"source changed: {old!r}"
    return text.replace(old, new)


def prepare():
    os.chdir(ROOT)
    OUT.mkdir(parents=True, exist_ok=True)
    SCRATCH.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "diff", "--exit-code", SOURCE, "--", "cmd", "internal",
                    "sdk", "gen", "proto", "examples", "scripts", "Makefile",
                    "go.mod", "go.sum", "compose.yaml", "conformance"], check=True)
    lib = (ROOT / "scripts/evidence-lib.mjs").read_text()
    lib = replace(lib, 'dirname(dirname(fileURLToPath(import.meta.url)))',
                  'dirname(dirname(dirname(fileURLToPath(import.meta.url))))')
    lib = replace(lib, '(port >= 7600 && port <= 7699)', '(port >= 7700 && port <= 7799)')
    lib = replace(lib, 'databasePrefix = "capstan_evidence"', 'databasePrefix = "capstan_final"')
    lib = replace(lib, '["capstan_evidence", "capstan_perf", "capstan_audit"]', '["capstan_final"]')
    (SCRATCH / "evidence-lib.mjs").write_text(lib)
    for name, port in [("run-load", None), ("demo-crash", 7302),
                       ("demo-blocked", 7303), ("demo-idle-wait", 7304)]:
        text = (ROOT / f"scripts/{name}.mjs").read_text()
        if port:
            text = replace(text, str(port), str(port + 400))
        if name == "demo-idle-wait":
            text = replace(text, "human() is still a stub at this lane's base/main check. Using the authorized nextSignal('decision') fallback.",
                           "This retained demo exercises nextSignal('decision'); human() is implemented and tested separately.")
        if name == "run-load":
            text = replace(text, 'fetch(`${env.address}/metrics`)',
                           'fetch(`${env.address}/metrics`, { headers: { Authorization: `Bearer ${env.key}` } })')
        (SCRATCH / f"{name}.mjs").write_text(text)
    # Observe each result before the unchanged campaign accumulator consumes it.
    main = (ROOT / "cmd/capstan-lab/main.go").read_text()
    main = replace(main, '\t"context"', '\t"context"\n\t"encoding/json"')
    main = replace(main, 'for outcome := range results {', '''for outcome := range results {
        faultKinds := make([]string, 0, len(outcome.result.Faults))
        for _, fault := range outcome.result.Faults { faultKinds = append(faultKinds, string(fault.Kind)) }
        failure := ""
        if outcome.err != nil { failure = outcome.err.Error() }
        if err := json.NewEncoder(os.Stdout).Encode(map[string]any{
            "seed": outcome.seed, "scenario": outcome.result.Scenario,
            "steps": outcome.result.Steps, "root_runs": outcome.result.RootRuns,
            "transaction_steps": outcome.result.TransactionSteps,
            "faults": faultKinds, "gate_responses": outcome.result.GateResponses,
            "error": failure,
        }); err != nil { panic(err) }''')
    (SCRATCH / "lab-main.go.txt").write_text(main)
    (SCRATCH / "lab-overlay.json").write_text(json.dumps({"Replace": {
        str(ROOT / "cmd/capstan-lab/main.go"): str(SCRATCH / "lab-main.go.txt")}}))
    pg = (ROOT / "internal/lab/pg_test.go").read_text()
    pg = replace(pg, '\t\tsteps += result.steps', '''        t.Logf(`{"seed":%d,"steps":%d,"duplicate_acks":%d,"database_errors":%d,"server_crashes":%d}`,
            seed, result.steps, result.duplicateAcks, result.databaseErrors, result.serverCrashes)
        steps += result.steps''')
    (SCRATCH / "pg-test.go.txt").write_text(pg)
    (SCRATCH / "pg-overlay.json").write_text(json.dumps({"Replace": {
        str(ROOT / "internal/lab/pg_test.go"): str(SCRATCH / "pg-test.go.txt")}}))
    for f in SCRATCH.iterdir():
        if f.is_file():
            target = OUT / "harness" / f.name
            target.parent.mkdir(exist_ok=True)
            shutil.copyfile(f, target)


def record(name, args, seed_output=False):
    path = OUT / f"{name}.log"
    assert not path.exists(), f"refusing to overwrite {path}"
    checkout = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    header = f"Measured source SHA: {SOURCE}\nCheckout SHA: {checkout}\nCommand: {json.dumps(args)}\n"
    print(header, flush=True)
    seeds = gzip.open(OUT / f"{name}-seeds.jsonl.gz", "wt") if seed_output else None
    count = 0
    try:
        with path.open("w") as log:
            log.write(header); log.flush()
            with subprocess.Popen(args, cwd=ROOT, env=ENV, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, text=True) as child:
                for line in child.stdout:
                    if seeds and line.startswith('{"') and '"seed":' in line:
                        seeds.write(line)
                        count += 1
                        if count % 10000 == 0:
                            print(f"{name}: recorded {count} seeds", flush=True)
                    else:
                        log.write(line); log.flush()
                        print(line, end="", flush=True)
                code = child.wait()
            log.write(f"Exit: {code}\n")
        if code:
            raise RuntimeError(f"{name}: exit {code}; raw failure retained")
    finally:
        if seeds:
            seeds.close()
    return path


def collect(folder, target):
    target.mkdir(parents=True, exist_ok=True)
    for file in folder.iterdir():
        if file.name not in KEEP:
            continue
        if file.suffix == ".jsonl":
            with (target / (file.name + ".gz")).open("wb") as sink:
                sink.write(gzip.compress(file.read_bytes(), mtime=0))
        else:
            shutil.copyfile(file, target / file.name)


def measure(mode):
    prepare()
    if mode in ("all", "load"):
        record("versions", ["bash", "scripts/evidence-versions.sh"])
        for i, point in enumerate(ORDER, 1):
            log = record(f"load-{i:02}", ["node", str(SCRATCH / "run-load.mjs"), *map(str, point)])
            folder = re.search(r"^Raw logs: (.+)$", log.read_text(), re.M)[1]
            collect(ROOT / folder, OUT / "load" / Path(folder).name)
    if mode in ("all", "lab"):
        record("lab", ["go", "run", "-overlay", str(SCRATCH / "lab-overlay.json"),
               "./cmd/capstan-lab", "-seeds", "200000", "-parallel", "8", "-workers", "3",
               "-max-steps", "1000", "-faults", "all", "-out", str(OUT / "lab-report.md")], True)
    if mode in ("all", "mutate"):
        record("mutation", ["go", "run", "./cmd/capstan-lab", "mutate", "-catalogue",
               "internal/lab/mutants/*.patch", "-seeds", "2000", "-timeout", "3m",
               "-logs", str(OUT / "mutations"), "-out", str(OUT / "mutation-report.md")])
    if mode in ("all", "pg"):
        record("pg", ["go", "test", "-overlay", str(SCRATCH / "pg-overlay.json"), "-count=1",
               "./internal/lab", "-run", "^TestPostgresCampaign$", "-lab.pg-seeds", "500", "-timeout", "11m", "-v"])
    if mode in ("all", "demos"):
        for name in ("crash", "blocked", "idle-wait"):
            before = set((ROOT / ".lane").iterdir())
            record(f"demo-{name}", ["node", str(SCRATCH / f"demo-{name}.mjs")])
            for folder in set((ROOT / ".lane").iterdir()) - before:
                if folder.is_dir():
                    collect(folder, OUT / f"demo-{name}")
    if mode == "gate":
        record("verify", ["make", "verify"])
    if mode == "controls":
        record("human-seven-days", ["go", "test", "-race", "-count=1", "./internal/server",
                                    "-run", "^TestAuditPostgresHumanWaitSevenDays$", "-v"])
        record("controls", ["bash", "-c", "set -euo pipefail\n"
               "go test -race -count=1 -tags pgengine ./internal/engine -run '^TestPostgresTerminateAgainstTaskOperations$' -v\n"
               "go test -race -count=1 ./internal/store/pgstore -run '^TestQueueNotificationClaimTransactions$' -v"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("all","load","lab","mutate","pg","demos","gate","controls"))
    parser.add_argument("--out", type=Path, help="fresh output directory for a repeat; existing logs are never overwritten")
    args = parser.parse_args()
    if args.out:
        OUT = args.out.resolve()
    measure(args.mode)
