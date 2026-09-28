# Capstan

Capstan runs TypeScript workflows through a Go server backed by PostgreSQL. The server records events, tasks, timers and approvals. Workers replay that history to resume work after a crash. Activities perform I/O outside the workflow. The [crash demo](docs/evidence/crash.md) shows a server restart during a run; the [decisions](docs/decisions.md) define the guarantees.

## Why

Agent work can outlast the process that started it. A model call may finish just before a worker dies. A tool may need permission from [AgentOps Gate](sdk/src/worker/builtins/gate.ts), or a workflow may wait for a [human decision](sdk/src/workflow/agent.ts). Capstan persists the work and the wait. A replacement worker continues from recorded results. The [idle-wait evidence](docs/evidence/idle-wait.md) separates a real signal wait from a human approval tested across an engine-clock jump.

## Five-minute quickstart

From the repository root, use Bash with Go 1.26.4, Node 26, npm, OpenSSL, Docker Compose and PostgreSQL client tools on PATH. On the development Mac, Docker runs through Colima. These are the [measured tools](docs/evidence/raw/904cb6c/versions.log). The example uses only local arithmetic; it needs no provider or Gate account.

Run the following in one shell. The Compose project name reuses the shared development PostgreSQL on port 55432. A fresh database keeps this run separate from other work.

<!-- quickstart:start -->
```bash
export PATH="$HOME/.local/bin:$PATH"
export GOTOOLCHAIN=go1.26.4 COMPOSE_PROJECT_NAME=capstan
make pg-up
npm --prefix sdk ci
mkdir -p .lane
umask 077
go build -o .lane/quickstart-server ./cmd/capstan-server

export CAPSTAN_DEMO_DB="capstan_final_quickstart_$(date +%s)_$$"
PGPASSWORD=capstan createdb -h 127.0.0.1 -p 55432 -U capstan "$CAPSTAN_DEMO_DB"
export CAPSTAN_DATABASE_URL="postgres://capstan:capstan@127.0.0.1:55432/$CAPSTAN_DEMO_DB?sslmode=disable"
export CAPSTAN_API_KEY="$(openssl rand -hex 32)"
printf '%s' "$CAPSTAN_API_KEY" > .lane/quickstart-key
export CAPSTAN_API_KEY_HASHES="local:$(.lane/quickstart-server hash-key < .lane/quickstart-key)"
export CAPSTAN_ADDR=127.0.0.1:7773 CAPSTAN_ADDRESS=http://127.0.0.1:7773
export CAPSTAN_MIGRATE=true

.lane/quickstart-server serve > .lane/quickstart-server.log 2>&1 &
CAPSTAN_DEMO_SERVER_PID=$!
export EVIDENCE_QUEUE=quickstart EVIDENCE_IDENTITY=quickstart-worker
export EVIDENCE_WORKFLOWS="$PWD/examples/evidence/load.ts"
(cd sdk && exec node --import tsx ../examples/evidence/worker.ts) > .lane/quickstart-worker.log 2>&1 &
CAPSTAN_DEMO_WORKER_PID=$!

curl --retry 30 --retry-connrefused --retry-delay 1 --fail --silent --show-error "$CAPSTAN_ADDRESS/readyz"
node sdk/bin/capstan.mjs start loadFive quickstart-1 --queue quickstart
node sdk/bin/capstan.mjs describe quickstart-1
```
<!-- quickstart:end -->

Open [http://127.0.0.1:7773/ui/](http://127.0.0.1:7773/ui/). Paste the key from `.lane/quickstart-key`, connect, and open `quickstart-1`. The workflow calls `step` five times and returns `5` ([source](examples/evidence/load.ts)). If the CLI initially says `running`, run `describe` again. The [tested transcript](docs/evidence/raw/904cb6c/quickstart.log) and [viewer check](docs/evidence/raw/904cb6c/quickstart-ui.log) record this path.

When finished, run this in the same shell. It removes only the example's processes, key and database. Leave the shared PostgreSQL running.

<!-- quickstart:cleanup -->
```bash
kill "$CAPSTAN_DEMO_WORKER_PID" "$CAPSTAN_DEMO_SERVER_PID"
wait "$CAPSTAN_DEMO_WORKER_PID" "$CAPSTAN_DEMO_SERVER_PID" || true
PGPASSWORD=capstan dropdb -h 127.0.0.1 -p 55432 -U capstan "$CAPSTAN_DEMO_DB"
rm .lane/quickstart-key
```
<!-- quickstart:cleanup:end -->

## Architecture

- The [Go server](internal/server/handlers.go) exposes authenticated worker and client RPCs through Connect.
- The [engine](internal/engine/workflow_task.go) turns worker commands into events and updates run state in the same transaction.
- The [PostgreSQL store](internal/store/pgstore/) holds history, queues, leases, timers, approvals and the cost ledger. History is append-only.
- The [TypeScript worker](sdk/src/worker/) runs activities and replays workflows in a fresh `node:vm` context.
- The [background loops](internal/server/loops.go) process timers, expired leases, run deadlines and Gate approval checks. Gate calls happen outside transactions.
- The [CLI](sdk/src/cli/main.ts) starts and inspects runs. The [viewer](internal/server/ui/) reads the same authenticated run data at `/ui/`.

## Guarantees

Activities run **at least once**. Each attempt receives the same activity key. One external effect requires a destination that honours that key; arbitrary activity code may execute again. [D3](docs/decisions.md#d3--2026-09-28--no-server-side-idempotency-table), [fault-lab evidence](docs/evidence/lab-delta1-2026-09-28.md).

Workflows must be **replay-safe**: they ask for the same steps in the same order and use recorded results. The SDK checks the fields specified by the [replay contract](conformance/README.md). An incompatible change makes a run **blocked, not failed**. After fixing the code or adding a patch branch, an operator explicitly resumes it. [D5](docs/decisions.md#d5--2026-09-28--blocked-runs-resume-explicitly), [blocked demo](docs/evidence/blocked.md).

The daily AI cap uses the Toronto calendar day. It is absolute for **bounded reservations** from the SDK's text-only `model()` path at configured server prices. The server reserves the larger of the worker estimate and its own price for conservative input and provider-enforced output bounds. This path enables no cache writes. Other callers must include higher-cost usage classes in their estimate and honour their declared bounds. Unbounded reservations and unknown models rely on the caller's estimate. Finish records true usage even when it exceeds a reservation. A lost Reserve acknowledgement keeps that reservation counted until the day's midnight. [D8, D22, D27, D28 and D32](docs/decisions.md), [accounting evidence](docs/evidence/audit-2026-09-28.md#post-audit-resolution-2026-09-28), [ledger tests](internal/engine/ledger_test.go).

## Evidence

<!-- final-evidence:start -->
Final code: `904cb6c`, measured 2026-09-28. The paid-call row is historical.

| Claim | How it is shown | Evidence | Status |
| --- | --- | --- | --- |
| Crash recovery | Kill and restart the server; unchanged history prefix | [Crash](docs/evidence/crash.md) | VERIFIED |
| Key-honouring effects | 200,000 memory seeds; 500 PostgreSQL seeds; 25/26 mutants caught, 1 equivalent | [Lab](docs/evidence/lab-delta1-2026-09-28.md) | VERIFIED within the fault model |
| Durable waits | Worker-free signal wait; human approval after an engine-clock jump | [Waits](docs/evidence/idle-wait.md) | VERIFIED locally |
| Incompatible code blocks | Changed worker, explicit resume and patched old/new branches | [Replay](docs/evidence/blocked.md) | VERIFIED |
| Observed throughput | 11,700 workflows; 58,500 activity completions; 0 errors | [Load](docs/evidence/load.md) | VERIFIED on the measured host |
| Recorded model cost | Retained paid-call recording and ledger arithmetic | [Cost](docs/evidence/cost.md) | VERIFIED historical arithmetic; no paid rerun |
<!-- final-evidence:end -->

The [evidence index](docs/evidence/README.md) names the source revision, commands, raw data and scope. Historical results remain labelled historical. Run `make verify` and `python3 docs/evidence/raw/verify-numbers.py` to check the code and the published numbers.

## Security model

Workflow code runs in `node:vm` to support replay-safety. Workflows must use the SDK for I/O, clocks and randomness. **`node:vm` is not an isolation boundary for untrusted code.** Workers run trusted code only. Workflow Date operations use UTC; ambiguous date strings and locale-dependent formatting are rejected. Use ISO dates with an explicit offset when importing dates. Existing workflows that used the worker's local timezone need replay review before resuming. [Sandbox code](sdk/src/sandbox/), [security audit](docs/evidence/audit-2026-09-28/security/README.md).

Payloads are opaque. Capstan stores activity inputs and results, signals and workflow results as supplied, and exposes them to API key holders through history, the CLI and the viewer. It redacts configured credentials from diagnostics, but it does not inspect arbitrary payloads. An activity that returns a secret puts it in history. Return secret references; keep credentials out of inputs, results, markers and failure details. [D33](docs/decisions.md#d33--2026-09-28--payloads-are-opaque-and-the-docs-say-so), [privacy probe](docs/evidence/audit-2026-09-28/security/README.md).

RPCs and `/metrics` require an API key. `/ui/` serves public static files and obtains run data through authenticated RPCs. The quickstart binds to loopback. [Authentication evidence](docs/evidence/audit-2026-09-28/security/README.md).

## Limitations

- The deployment runbook specifies one server. A bounded two-process timer test does not establish general high-availability support. [Audit scope](docs/evidence/audit-2026-09-28.md).
- The load measurements are short runs on a shared Mac and PostgreSQL instance. Their observed throughput is not a universal capacity limit. [Load evidence](docs/evidence/load.md).
- The fault lab covers a finite scenario and fault catalogue. It does not explore every workflow, instruction-level interleaving or network failure. [Fault model](docs/evidence/lab-l2.md).
- Local Compose disables PostgreSQL durability settings for tests. Process restart demos do not prove recovery from database or host power loss. [Compose configuration](compose.yaml).
- Histories have a configured size limit; use continuation for longer workflows. An orphaned model reservation can consume budget without a provider call. [History limit](internal/engine/workflow_task.go), [D28](docs/decisions.md#d28--2026-09-28--a-lost-reserve-acknowledgement-stays-counted-until-midnight).
- A live Gate deployment, cloud operation and provider invoices are outside this local verification. The recorded paid model call was not repeated. [Audit](docs/evidence/audit-2026-09-28.md), [cost recording](docs/evidence/cost.md).

## License

None yet. This repository is private.
