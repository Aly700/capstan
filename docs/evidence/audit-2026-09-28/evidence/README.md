# Independent evidence rerun

These files record commands run by the audit lane on 2026-09-28. They are separate
from the historical evidence measurements and do not replace their raw data.
All application runs used PostgreSQL on the existing shared server, fresh
`capstan_audit_*` databases, ports 7650–7654, and Go 1.26.4. Owned processes and
databases were removed after each run. No external provider call was made.

From the repository root:

```sh
export PATH="$HOME/.local/bin:$PATH" GOTOOLCHAIN=go1.26.4
python3 docs/evidence/audit-2026-09-28/evidence/verify.py
python3 docs/evidence/audit-2026-09-28/evidence/reproduce.py crash
python3 docs/evidence/audit-2026-09-28/evidence/reproduce.py idle
python3 docs/evidence/audit-2026-09-28/evidence/reproduce.py load50
python3 docs/evidence/audit-2026-09-28/evidence/reproduce.py load200
python3 docs/evidence/audit-2026-09-28/evidence/reproduce.py ui
go test -count=1 ./internal/lab -run '^TestPostgresCampaign$' -lab.pg-seeds 500 -timeout 11m -v
go run ./cmd/capstan-lab -seeds 2000 -parallel 4 -workers 3 -max-steps 1000 -faults all -out .lane/audit-lab-campaign.md
go run ./cmd/capstan-lab mutate -catalogue 'internal/lab/mutants/*.patch' -seeds 2000 -timeout 3m -logs .lane/audit-mutations-rerun -out .lane/audit-mutations-rerun.md
go test -race -count=1 ./cmd/capstan-lab ./internal/lab/labworker ./internal/lab/scenarios
node --test scripts/evidence-lib.test.mjs
```

`reproduce.py` copies the original demonstration programs into `.lane/audit-scripts`
and changes only the harness location, database prefix, ports, and screenshot output
paths. It executes the original assertions. The idle demonstration's statement that
`human()` was a stub refers to the historical lane base; this run tests the signal
fallback. Every new invocation creates fresh logs; its timing can differ on the
shared machine. The viewer uses the original pinned ephemeral Playwright tooling.

`verify.py` checks the historical Markdown tables against `load-results.json`, the
committed recordings, fixture counts, historical mutation snapshot hash, and the new
raw measurements here. It prints `FAIL` for the missing historical raw directories;
its successful exit establishes the asserted arithmetic and artifact consistency,
not the absent historical measurement provenance. It reports credential scan
locations/counts without printing candidate values.

| Artifact | What it records |
| --- | --- |
| `evidence-numbers.log` | Table/cast/fixture/credential and new raw-data checks |
| `mutants-red.log`, `mutants-green.log`, `mutants-race.log` | Catalogue regression fails before patch refresh, passes after |
| `mutations/`, `mutations.log` | First full attempt: 22 caught, four invalid stale patches |
| `mutations-fixed/`, `mutations-fixed.log`, `mutations-fixed.md` | Refreshed full catalogue: 25 caught, M003 equivalent; 2,000 seeds in baseline and M003 |
| `pg-campaign.log` | 500 real-clock PostgreSQL seeds; 53,835 steps and 7,589 rejected duplicates |
| `lab-campaign.md`, `lab-campaign.log` | New 2,000-seed memstore campaign and fault counts |
| `labworker.log`, `conformance.log` | Reference-worker race tests and 53 shared fixtures plus one TS-only fixture |
| `crash.log`, `idle.log` | Real server SIGKILL/restart and worker-free persisted wait |
| `ui.log`, `ui-*.png` | Browser assertions on 13 real runs; fresh desktop/mobile screenshots |
| `evidence-harness-tests.log` | All three harness cleanup tests |
| `load50.log`, `load200.log`, `load-results.json` | New load output and derived summaries |
| `load-c50/`, `load-c200/` | Per-run latencies, sampler observations, SQL, metadata, emitted result, persisted audit |

The new load runs completed 1,000 workflows and 5,000 activities each, with no
errors. Concurrency 50 measured 100.63206327728602 runs/s and p99 567.800125 ms;
concurrency 200 measured 93.27193977640002 runs/s and p99 2361.649125 ms. The nearest-rank
percentiles are recomputed from the included `runs.jsonl`. CPU figures can be
recomputed with `scripts/summarize-load.py` on the two included load directories.

M003 removes only `tok.StartedEventId != task.StartedEventID`. Polling stores the
new `TaskStarted` ID in the task and leaves it as the run's last event. While the
task is in flight, external events enter the inbox; completion, failure, lease
expiry, or closing clears or replaces the task. The retained checks require that
same live task and `run.LastEventID == tok.StartedEventId`. Thus the removed
comparison adds no rejection on engine-reachable states. This argument assumes
one mutation and no manually corrupted database; the passing seed run alone is
not the proof.
