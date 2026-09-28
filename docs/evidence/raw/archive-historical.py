#!/usr/bin/env python3
"""Copy the original local artifacts, with an explicit allowlist and provenance."""
import gzip
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[3]
DEST = ROOT / "docs/evidence/raw/historical"
LANES = {name: ROOT.parent / f"capstan--{name}" for name in ("evidence", "perf", "lab")}
manifest = []


def retain(data, source, relative):
    target = DEST / relative
    original_hash = hashlib.sha256(data).hexdigest()
    original_size = len(data)
    if target.suffix == ".jsonl":
        target = target.with_suffix(".jsonl.gz")
        data = gzip.compress(data, mtime=0)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    manifest.append({"source": source, "path": str(target.relative_to(ROOT)),
                     "source_bytes": original_size, "source_sha256": original_hash,
                     "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})


def copy(path, relative):
    retain(path.read_bytes(), str(path), relative)


results_file = ROOT / "docs/evidence/load-results.json"
rows = json.loads(results_file.read_text())
for row in rows:
    original = row.get("original_raw_directory", row["raw_directory"])
    name = Path(original).name
    matches = [lane / ".lane" / name for lane in (LANES["evidence"], LANES["perf"])
               if (lane / ".lane" / name).is_dir()]
    assert len(matches) == 1, (name, matches)
    source = matches[0]
    for file in source.iterdir():
        if file.name in ("runs.jsonl", "system.jsonl", "metadata.json", "result.json",
                         "audit.json", "sampling.sql", "load-errors.log"):
            copy(file, Path("load") / name / file.name)
    # The wrapper stdout identifies the same raw directory and SQL audit.
    logs = [p for p in source.parent.glob("*.log") if p.stat().st_size < 2000
            and name in p.read_text(errors="replace")]
    assert len(logs) == 1, (name, logs)
    copy(logs[0], Path("load") / name / "driver.log")
    row["original_raw_directory"] = original
    row["raw_directory"] = f"docs/evidence/raw/historical/load/{name}"
results_file.write_text(json.dumps(rows, indent=2) + "\n")

selected = {
    "evidence": ["verify.log", "versions.log", "cleanup-tests.log", "interrupt-demo.log", "interrupt-check.log", "ui-browser.log"],
    "perf": ["targeted-before.log", "targeted-after-reviewed.log", "lock-red-race.log",
             "lock-green-engine.log", "lock-green-store.log", "d30-verify.log", "pool-verify.log",
             "targeted-verify.log", "lock-verify.log", "merged-verify-reviewed.log",
             "lab-default-reviewed.log", "after-versions.log"],
    "lab": ["l2-campaign-200000.log", "delta-campaign.log", "l2-2000-final.log",
            "gate-make-verify-l2-final.log", "delta-make-verify-final.log",
            "delta-pg-unscaled-probe.log", "delta-pg-timer-red.log", "delta-pg-deadline-red.log",
            "delta-pg-green.log", "delta-pg-500.log", "delta-pg-500-final.log",
            "delta-pg-500-uncontended.log", "l2-fault-red-behavior.log", "delta-mutation-final.log"],
}
for lane, names in selected.items():
    for name in names:
        copy(LANES[lane] / ".lane" / name, Path(lane) / name)
for file in (LANES["lab"] / ".lane/delta-mutation-final").iterdir():
    if file.suffix in (".jsonl", ".json"):
        copy(file, Path("lab/mutations") / file.name)
for file in (LANES["lab"] / ".lane/delta-mutants").rglob("*"):
    if file.is_file() and file.suffix in (".jsonl", ".json", ".log"):
        copy(file, Path("lab/development-mutants") / file.relative_to(LANES["lab"] / ".lane/delta-mutants"))
# These original committed report snapshots retain the counters the CLI wrote
# only to Markdown. They are aggregate evidence, not a per-seed raw trace.
for revision, name in [("c37ce35", "lab-2026-09-28.md"), ("faa95ea", "lab-delta1-2026-09-28.md")]:
    source = f"{revision}:docs/evidence/{name}"
    data = subprocess.check_output(["git", "show", source], cwd=ROOT)
    retain(data, "git:" + source, Path("lab") / name)
(DEST / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(f"Archived {len(rows)} load directories; {len(manifest)} files; "
      f"{sum(x['source_bytes'] for x in manifest)} source bytes -> {sum(x['bytes'] for x in manifest)} stored bytes")
