# Evidence index

The [independent audit](audit-2026-09-28.md) is the current verification ledger.
Rows below describe the original evidence lane's measurements, which the audit checks
separately. Historical load and large lab campaigns lack their referenced raw logs;
those traceability claims are FAILED in the audit. Fresh commands and raw measurements
are preserved under [audit-2026-09-28/](audit-2026-09-28/).

Measurements and demonstrations run on **2026-09-28** in `lane/evidence`, starting
from integration commit `73804f2`. Every VERIFIED row below has been executed against
the real binaries. Pending rows are explicit gaps, not claims established by these demos.
The root README is owned by integration and was not edited.

| Spec §13 claim | Evidence file | Command to reproduce | Status |
| --- | --- | --- | --- |
| A run survives a crash | [crash.md](crash.md), [session](demo-crash.cast), [GIF](demo-crash.gif) | `scripts/demo-crash.sh` | **VERIFIED — 2026-09-28.** Real server SIGKILL/restart; result `[1,2,3,4]`; original 10 events remain an exact prefix. |
| Effects apply once when the destination honors the idempotency key (D3) | [lab-2026-09-28.md](lab-2026-09-28.md), [concurrent campaign](lab-delta1-2026-09-28.md), [mutation score](lab-mutation.md), [PostgreSQL campaign](lab-pg.md), [defects.md](defects.md), [fault model](lab-l2.md) | `go run ./cmd/capstan-lab -seeds 200000 -parallel 8 -workers 3 -max-steps 1000 -faults all`; `go run ./cmd/capstan-lab mutate -catalogue 'internal/lab/mutants/*.patch' -seeds 2000` | **VERIFIED within the lab's fault model — 2026-09-28.** 200,000 + 148,554 seeds (concurrent runs, interleaved transactions, Gate faults) and 500 real-clock seeds on PostgreSQL, 0 failures; the lab catches 25 of 26 planted engine bugs (1 argued equivalent). |
| A workflow can wait days | [idle-wait.md](idle-wait.md), [session](demo-idle-wait.cast); clock-jump evidence pending | `scripts/demo-idle-wait.sh` reproduces the verified idle portion | **PENDING — full multi-day approval claim.** Zero demo workers/leases and later signal resume **verified 2026-09-28**; agent/lab approval plus clock-jump proof remains. |
| Code changes do not corrupt live runs | [blocked.md](blocked.md), [session](demo-blocked.cast) | `scripts/demo-blocked.sh` | **VERIFIED — 2026-09-28.** Incompatible V2 blocks; explicit resume with `patched()` preserves the old result; new runs take the new branch. |
| Throughput, with its ceiling named | [load.md](load.md), [full numbers](load-results.json) | `scripts/run-load.sh 50 1000 4`; `scripts/run-load.sh 200 1000 4`; `scripts/run-load.sh 800 1600 4` | **VERIFIED — 2026-09-28.** Eleven measurements including repeats and worker controls; 11,700 runs, 58,500 activity completions, zero failed runs. |
| Cost of a real agent run | [cost.md](cost.md), [session](demo-cost.cast) | `CAPSTAN_ANTHROPIC_ENV_FILE=<file> scripts/demo-cost.sh` | **VERIFIED — 2026-09-28.** One real claude-haiku-4-5 call: 14 in / 4 out tokens, $0.000034, one ledger row after a second worker replayed the run. |

The [run viewer evidence](ui.md) includes inspected [list](ui-list.png),
[completed](ui-completed.png), [blocked](ui-blocked.png), and [mobile](ui-mobile.png)
screenshots. Reproduce with `scripts/check-ui.sh`; Playwright comes from npx and
does not modify SDK dependencies. The UI is public static content; data is fetched
from the authenticated ClientService over same-origin JSON Connect.

All `.cast` recordings preserve wall-clock timing. The crash GIF is a VHS rendering
of the same cast at playback speed 1. Scripts build the real server, create their own
databases on the shared PostgreSQL, and clean up owned processes/databases. Each SDK
worker is one Node process in its own process group. Logs remain under `.lane/`.
No public deployment or push was performed.

The observed throughput range and limits are in [load.md](load.md). CPU, task counts,
`pg_stat_activity`, `pg_locks`, and worker-count controls point to the server's
transaction/polling path to PostgreSQL. They do not establish a row-lock-bound
`SKIP LOCKED` ceiling. All repeats and the sample that observed another lane's
activity are retained.

## Integrated implementation and remaining evidence gaps

The lab and `human()` implementations are now integrated. The original idle recording
still demonstrates a five-second signal wait; it does not become a human-approval or
multi-day recording retroactively. The audit adds an authenticated PostgreSQL RPC test
that advances the engine clock seven days with no worker and no task lease, then approves
and completes the run. A live multi-day Gate deployment remains outside that proof.

## Verification

```sh
CAPSTAN_E2E=1 CAPSTAN_E2E_PORT=7399 make verify
GOTOOLCHAIN=go1.26.4 go test ./internal/server/ui ./internal/server
scripts/check-ui.sh
node --test scripts/evidence-lib.test.mjs
```

The merge gate passed, including generated-code drift checks, buf lint, gofmt,
Go vet, all Go tests with the race detector and real PostgreSQL tests, SDK typecheck,
and **295 SDK tests in 13 files**, including both real-server e2e cases. Those counts describe the evidence lane's original revision. The integrated Makefile
sets `CAPSTAN_E2E=1` itself and includes the lab. Current gate output and audit ports
are recorded in the independent audit.

Three harness regression tests also passed. A real SIGTERM interruption during the
crash demo exited 130 and left no owned process groups or evidence databases.
Cleanup closes resource registration before waiting for processes to stop, so an
interrupted script cannot restart a server or worker after cleanup begins.
