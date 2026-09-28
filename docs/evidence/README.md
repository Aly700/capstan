# Evidence index

Measurements and demonstrations run on **2026-09-28** in `lane/evidence`, starting
from integration commit `73804f2`. Every VERIFIED row below has been executed against
the real binaries. Pending rows are explicit gaps, not claims established by these demos.
The root README is owned by integration and was not edited.

| Spec §13 claim | Evidence file | Command to reproduce | Status |
| --- | --- | --- | --- |
| A run survives a crash | [crash.md](crash.md), [session](demo-crash.cast), [GIF](demo-crash.gif) | `scripts/demo-crash.sh` | **VERIFIED — 2026-09-28.** Real server SIGKILL/restart; result `[1,2,3,4]`; original 10 events remain an exact prefix. |
| Effects apply once when the destination honors the idempotency key (D3) | Lab-owned `lab-*.md` and `defects.md`; see [pending evidence](#pending-evidence) | Planned lab command: `GOTOOLCHAIN=go1.26.4 go test ./internal/lab -run TestLab -seeds 2000` | **PENDING — lab lane P1**, campaign and duplicate-delivery effect-sink evidence. |
| A workflow can wait days | [idle-wait.md](idle-wait.md), [session](demo-idle-wait.cast); clock-jump evidence pending | `scripts/demo-idle-wait.sh` reproduces the verified idle portion | **PENDING — full multi-day approval claim.** Zero demo workers/leases and later signal resume **verified 2026-09-28**; agent/lab approval plus clock-jump proof remains. |
| Code changes do not corrupt live runs | [blocked.md](blocked.md), [session](demo-blocked.cast) | `scripts/demo-blocked.sh` | **VERIFIED — 2026-09-28.** Incompatible V2 blocks; explicit resume with `patched()` preserves the old result; new runs take the new branch. |
| Throughput, with its ceiling named | [load.md](load.md), [full numbers](load-results.json) | `scripts/run-load.sh 50 1000 4`; `scripts/run-load.sh 200 1000 4`; `scripts/run-load.sh 800 1600 4` | **VERIFIED — 2026-09-28.** Eleven measurements including repeats and worker controls; 11,700 runs, 58,500 activity completions, zero failed runs. |
| Cost of a real agent run | [Pending ledger artifact](#pending-evidence) | After a real agent run: `psql "$CAPSTAN_DATABASE_URL" -c 'select run_id,activity_seq,model,status,input_tokens,output_tokens,cost_usd from ai_call order by id'` | **PENDING — agent lane / real cost ledger.** No paid model call was made in this lane. |

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

## Pending evidence

The lab lane owns `lab-*.md` and `defects.md`; those files and its CLI are not in this
worktree's starting revision. The lab command above comes from the plan and is
marked pending until its owner supplies the campaign. No lab result is inferred
from the crash demo's completion count.

`human()` remained a stub in main when checked on 2026-09-28. The explicitly permitted
signal fallback proves an idle wait with no worker process or leased task in this
demo, followed by persisted signal delivery and successful resume. Five real seconds
are not multi-day clock-jump evidence. The complete approval/days claim stays pending.

A real agent `ai_call` ledger export has not been delivered. Its evidence filename
and invocation will be linked when supplied by the agent/workload lane. A zero-cost
non-agent workflow is not evidence of model pricing or replay billing behavior.

## Verification

```sh
CAPSTAN_E2E=1 CAPSTAN_E2E_PORT=7399 make verify
GOTOOLCHAIN=go1.26.4 go test ./internal/server/ui ./internal/server
scripts/check-ui.sh
node --test scripts/evidence-lib.test.mjs
```

The merge gate passed, including generated-code drift checks, buf lint, gofmt,
Go vet, all Go tests with the race detector and real PostgreSQL tests, SDK typecheck,
and **295 SDK tests in 13 files**, including both real-server e2e cases. This revision's
Makefile does not itself set `CAPSTAN_E2E`, so the command above enables it explicitly
and reserves port 7399 for this lane. No lab package exists in this base revision;
its campaign is pending integration.

Three harness regression tests also passed. A real SIGTERM interruption during the
crash demo exited 130 and left no owned process groups or evidence databases.
Cleanup closes resource registration before waiting for processes to stop, so an
interrupted script cannot restart a server or worker after cleanup begins.
