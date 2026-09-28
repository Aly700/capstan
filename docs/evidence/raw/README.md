# Raw evidence

The measured production revision is `904cb6c41da4f57ac0399d1524989288e3ededb1`.
The final lane changes documentation and evidence tooling only. Every command log
prints that revision and the checkout revision. Measurements ran on 2026-09-28.

`historical/manifest.json` records each original local path or Git blob, its byte
count and SHA-256, and the stored file's byte count and SHA-256. The original
`load-results.json` paths remain in `original_raw_directory`; `raw_directory` now
names the archive. JSONL files are gzip compressed without changing their contents.
Server and worker logs were omitted because these claims use driver output, SQL,
latencies, system snapshots and test results. The selected files passed the
[credential scan](historical-scan.log) without redaction.
The [final scan](final-scan.log) also covers the new measurements before commit;
it decompresses gzip files and found no credential-shaped values.

The old lab CLI emitted summary counts to stdout and detailed counters to a
Markdown report. Both surviving artifacts are retained. The original report blobs
are historical aggregate evidence; no per-seed historical output exists. They do
not acquire the stronger provenance of the new per-seed campaign.

See [numeric-coverage.md](numeric-coverage.md) for each checked claim group and its limits.

## Reproduce and verify

Run from the repository root, with the existing PostgreSQL available:

```sh
python3 docs/evidence/raw/reproduce.py all --out .lane/final-repeat
python3 docs/evidence/raw/verify-numbers.py
python3 docs/evidence/raw/scan-credentials.py docs/evidence/raw
```

The reproducer refuses to overwrite an existing command log. Use a fresh `--out`
directory for a repeat; never delete committed evidence to make a repeat look like
the original. The recorded final commands used the revision directory shown here. The eleven loads run first, then the memory lab, mutations,
PostgreSQL lab and crash/blocked/idle demos. It does not call a provider or AWS.

The original JS harness accepts only earlier lanes' resources. Local copies admit
`capstan_final` databases and the final lane's ports; all final runs use ports
7701–7704. Copies of all harness
files are retained under the measured revision. The load sampler authenticates its
metrics requests because `/metrics` now requires a key. The idle demo's stale stub
announcement is corrected. All workload assertions are unchanged.

The Go campaign overlay only emits each completed seed's result before the existing
aggregation. The PostgreSQL test overlay only prints each seed's existing counters.
Both preserve the original faults, scheduler, assertions and timeouts. Emitting
observations adds some overhead to the reported campaign time. Mutation tests run
the ordinary command without an overlay, including its own temporary worktree
creation and cleanup. No source contract or engine/SDK implementation is changed.

Historical versions and timings describe their original revisions. The final
measurements supersede them for current performance claims. A finite successful
campaign is evidence for those executions, not a proof that every workflow or
schedule is correct. The local compose database disables fsync, synchronous commit
and full-page writes; these tests establish application/process recovery, not
recovery from PostgreSQL or host power loss.
