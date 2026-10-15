# Capstan

Capstan is a durable execution engine for agent work. Workflows are ordinary TypeScript. A Go server backed by PostgreSQL records what each workflow decided, and a worker replays that record to continue the workflow after a crash. This page is a tour. It explains the ideas in the order you need them, points at the code and tests behind the contracts, and ends with the commands to run it yourself. A reader who has never used a durable execution engine should be able to follow it top to bottom.

A capstan is the winch that hauls an anchor chain. It ratchets: what it has gained it holds, and it never slips back. That is the one promise this engine makes. The rest of this page is about what the promise means and how it is checked.

## The problem

Picture an agent run that takes an hour. It calls a model a few dozen times. It runs tools that change things outside the process: it opens a pull request, writes to a database, starts a remote job. Twice it stops and waits for a person to approve a step. At minute forty the process dies. The laptop closes, the container is replaced, someone presses Ctrl-C.

What should happen next is clear. A replacement process should pick up at minute forty. The first thirty-nine minutes of model calls should not be paid for again. A tool call that already happened should not happen a second time. The approval that was granted should still count. The workflow code should not have to know that any of this took place.

A plain script cannot do this. Its state is a loop counter and a chain of promises in memory, and memory is what the crash took. The usual answers are to start over, which repeats every side effect and every bill, or to hand-write a checkpoint for each step, which is bespoke, easy to get wrong, and never covers the step you forgot.

## The idea in one picture

Capstan keeps the workflow's state outside the process, as an append-only history of events, and rebuilds the in-memory state by replaying that history.

Three nouns. A **run** is one execution of a workflow, identified by a run id you choose. **Commands** are what workflow code asks for: schedule this activity, start this timer, request this approval. **Events** are what the server durably recorded. Workers never write history. They return commands, and the server decides which events those become ([design](docs/design.md), section 4).

Here is the event sequence checked by the crash demo for [`crashSurvivor`](examples/evidence/crash.ts). Event numbers skip because each workflow task also records `TaskScheduled`, `TaskStarted` and `TaskCompleted` around the decisions:

```
 1  RunStarted         workflowType=crashSurvivor
 5  ActivityScheduled  seq=1  step(0)
 6  ActivityCompleted  seq=1  result=1
10  TimerStarted       seq=2  fireAfter=3s
```

Suppose the worker holding this run dies and a new worker is handed these ten events. It does not know where the old worker was. It does not need to. It runs `crashSurvivor` again from its first line, inside a fresh sandbox:

1. The code calls `activity("step", 0)`. That is the first command, `seq` 1. The SDK looks at history position 5, finds an `ActivityScheduled` with the same type and input, and sees `ActivityCompleted` for `seq` 1 at position 6. It resolves the promise with the recorded result, `1`. No activity runs.
2. The code calls `sleep("3s")`. That is `seq` 2. History has `TimerStarted` at position 10 but no `TimerFired`, so the promise stays pending. The code has nowhere to go, the worker returns no new commands, and the run sits idle in the database until the server's timer sweeper fires the timer and schedules another workflow task.
3. On that task the replay runs the same two steps the same way, the timer promise now resolves, and the code reaches `activity("step", 1)`, which is new. The worker returns one `ScheduleActivity` command and the server records it as `ActivityScheduled` for `seq` 3.

Replay touches nothing outside the process, so it is cheap to do on every task. The workflow's memory was never the process. It was the history.

This only works if the code asks for the same things in the same order every time it is re-run. The SDK does not trust that; it checks it. During replay every command the code emits is compared with the event recorded at that position, by type and by the fields that identify the step ([replay contract](conformance/README.md)). A mismatch means the code changed underneath a live run. The run is marked **blocked**, not failed, and an operator resumes it after fixing the code or adding a patch branch ([D5](docs/decisions.md#d5--2026-09-28--blocked-runs-resume-explicitly), [blocked demo](scripts/demo-blocked.mjs)).

That rule is why workflow code must be **replay-safe**. If it read the wall clock, called `Math.random()`, or made an HTTP request, a replay would make different decisions from the first run. So workflow code runs in a `node:vm` context whose globals have no `fetch`, `fs`, `net`, `process.env` or timers, and where `Date` and `Math.random` are replaced ([sandbox](sdk/src/sandbox/), [workflow API](sdk/src/workflow/index.ts)). In their place:

| In workflow code | First run | Replay |
| --- | --- | --- |
| `now()` | the time the current task started, from history | the same recorded time |
| `random()` | seeded from the run id and the command index | the same value |
| `uuid()`, `sideEffect(fn)` | runs once, records the value in a marker | returns the recorded value |
| `sleep(d)` | emits `StartTimer` | returns when `TimerFired` is in history |
| `activity(name, input)` | emits `ScheduleActivity` | returns the recorded result |
| `nextSignal(name)`, `condition(fn)` | waits | resolves from recorded events |
| `patched(id)` | records a marker, returns true | returns whether the marker exists |

Everything with a side effect happens in an **activity**: a plain function registered on the worker and run in normal Node, outside the sandbox. Activities do; workflows decide. The sandbox exists for replay safety, not for security. `node:vm` is not an isolation boundary for untrusted code, and workers run trusted code only.

The smallest workflow in the repository is the one the quickstart runs, [`loadFive`](examples/evidence/load.ts):

```ts
import { activity } from "@capstan/sdk/workflow";   // the only way out of the sandbox is this API

export async function loadFive(): Promise<number> {  // an exported async function is a workflow type
  let value = 0;                                      // ordinary local state; rebuilt by replay, never saved
  for (let index = 0; index < 5; index++) {
    value = await activity<number>("step", value,     // seq 1..5: ScheduleActivity "step" with the current value
      { startToCloseTimeout: "60s" });                // an attempt that runs longer than this times out and is retried
  }
  return value;                                       // CompleteRun with 5
}
```

`step` is `(n) => n + 1` in the [example worker](examples/evidence/worker.ts). Five activities, five `ActivityScheduled` and `ActivityCompleted` pairs, a result of `5`. The [load driver](scripts/run-load.mjs) runs this workflow.

## Architecture

![Capstan architecture: caller and worker outside AWS, API Gateway in front of one Fargate task that keeps all state in PostgreSQL](docs/architecture.svg)

The numbered arrows are walked one by one in [docs/architecture.md](docs/architecture.md), with the component table, the trust and data boundaries, and where each guarantee is proven.

## Walk through a crash

[`scripts/demo-crash.sh`](scripts/demo-crash.sh) does what the first section described, with a real server, a real worker and a real database.

What was running: `crashSurvivor`, four `step` activities with a three-second durable sleep between them. The script builds the Go server, starts one SDK worker in its own process group, starts the run, and waits for the first `TimerStarted` to appear in history. At that moment the history is the ten events shown above: one activity done, one timer pending.

Where it was killed: the script sends `SIGKILL` to the **server** process. The worker stays alive and keeps polling an address that no longer answers. After four real seconds the script starts a new server process against the same database. Nothing is reconstructed by hand and no event row is edited.

What the history looked like after: the run completed with `[1,2,3,4]`. The final history has 40 events. Its first ten are the ten that existed when the server died, byte for byte. There are four `ActivityCompleted` events, one per step, and three `TimerFired` events, one per sleep, each pointing at a different `TimerStarted`. The demo checks the result, history prefix and timer identities on each run.

"The prefix is unchanged" is the whole guarantee, and it is worth being precise about why. The workflow's state is its history. If the first ten events are intact, the replayed code sees exactly the decisions it had already made, so the activity that completed before the crash is not run again and the timer started before the crash fires once, not twice and not never. Everything the new server did was append. A system that lost, reordered or rewrote one of those ten events could still print `[1,2,3,4]` by accident. It could not pass the prefix check.


## What at-least-once really means

Replay settles what happens to decisions: they come from history, so they happen once. Activities are different, and this is the part that is easiest to get wrong.

An activity runs outside the sandbox and does something real. A worker can finish that work and die before telling the server. The server sees the lease expire, hands the task to another worker, and the activity runs again. Capstan does not pretend otherwise: **activities run at least once**. What it guarantees is narrower and more useful. Every attempt of an activity receives the same idempotency key, `<run_id>/<seq>`, and that key never changes across retries or crashes ([D3](docs/decisions.md#d3--2026-09-28--no-server-side-idempotency-table)). An external effect happens exactly once when its destination honours that key. AgentOps Gate does: `POST /decisions` takes an `Idempotency-Key` header ([gate activity](sdk/src/worker/builtins/gate.ts)). A table whose primary key is the idempotency key does. A plain POST to a service that ignores the header does not, and no engine can repair that from the outside.

The test that proves the contract is `applies a keyed effect once when the worker is killed after the effect and before the acknowledgement` in [sdk/test/e2e.test.ts](sdk/test/e2e.test.ts), with its activity in [sdk/test/e2e/worker.ts](sdk/test/e2e/worker.ts). It runs on the real server. The `deposit` activity writes two rows: one into an attempt log with no key, and one into an effect table whose primary key is the idempotency key, with `on conflict do nothing`. Then it writes a marker file and parks forever. The test waits for the marker, so the effect is committed, and sends `SIGKILL` to the worker before it can report. A second worker starts. The activity's three-second start-to-close timeout expires, the server retries, and the retry lands on the survivor with the same key.

Afterwards the attempt log has two rows: attempt 1 from the dead worker's pid and attempt 2 from the survivor's. The effect table has one row, written by the dead worker. History has one `ActivityCompleted` and no `ActivityTimedOut`, because a retried attempt is not a history event ([D4](docs/decisions.md#d4--2026-09-28--activitystarted-is-not-a-history-event)). The workflow's result is the key, `<run_id>/1`. Read that as a sentence: Capstan ran the activity twice and the destination applied it once. The engine's job was to keep the key stable. The table's job was to honour it.

The fault lab checks the same property across its seeds with a `kill-activity` fault that commits the destination effect, discards the worker before the acknowledgement, and retries with the engine-supplied key. Its P1 property reads the destination's idempotency ledger and requires an applied count of exactly one for every effect of a completed run ([fault model](internal/lab/properties.go)).

## Agents are just workflows with three extra step types

Nothing above mentioned models. That is the point. An agent run is a workflow, and the agent layer is three functions built on activities and approvals ([sdk/src/workflow/agent.ts](sdk/src/workflow/agent.ts)).

**`model(request)`** is a durable model call. It runs as the built-in activity `capstan.model`. Before calling the provider, the worker reserves an estimate with the server against the daily cap, `CAPSTAN_DAILY_CAP_USD`, and fails closed if the cap would be exceeded. After the call it reports token usage and the server prices it from its own table ([D8](docs/decisions.md#d8--2026-09-28--model-cost-is-computed-by-the-server)). The result, tokens and cost are recorded; on replay nothing is called and nothing is billed. The `ai_call` table is the ledger: one row per call with token counts and cost, no prompt or response text, and `DescribeRun` reports a run's cost as the sum of its rows.

The cap deserves a careful sentence, because the review found it weaker than it looked (more below). It is measured on the Toronto calendar day. It is absolute for **bounded reservations** from the SDK's text-only `model()` path at configured server prices: the SDK sends an input upper bound counted in UTF-8 bytes and the provider-enforced `max_tokens`, the server prices that worst case and reserves the larger of it and the worker's estimate, and a provider cannot return more than `max_tokens` ([D32](docs/decisions.md#d32--2026-09-28--the-server-bounds-every-reservation-itself)). This path enables no cache writes. Reservations without bounds and unknown models rely on the caller's estimate. `Finish` records true usage even when it exceeds a reservation, so spend is never hidden. A lost `Reserve` acknowledgement stays counted until midnight ([D28](docs/decisions.md#d28--2026-09-28--a-lost-reserve-acknowledgement-stays-counted-until-midnight)).

Two tests pin this on the real server with a fake provider that counts requests ([sdk/test/cost-cap.test.ts](sdk/test/cost-cap.test.ts)). With a cap of $0.00001 the run fails with a non-retryable `ModelBudgetExceeded`, the provider receives zero requests and `ai_call` has zero rows. With a cap of $2 the worker makes one call, one ledger row appears with the server's price, the worker is killed, and the survivor replays: still one provider request, still one row.

**`tool(name, args)`** is a tool call that goes through AgentOps Gate before it runs. The built-in activity `capstan.gate.decide` posts the proposed call to the Gate with the activity's idempotency key. `ALLOW` schedules the real tool activity. `DENY` resolves `{ allowed: false }` without running anything. `REQUIRE_APPROVAL` records `ApprovalRequested` with the Gate's approval id, and the workflow waits, holding no worker, no task and no lease, while the server polls the Gate for the decision with backoff from 15 seconds to 5 minutes, outside any transaction ([D6](docs/decisions.md#d6--2026-09-28--gate-approvals-are-watched-not-pushed)). When the Gate reports approval the server appends `ApprovalResolved`, schedules a workflow task, and the next line of the workflow runs, five minutes or five days later.

Why the Gate sits in front of every tool call: workflow code cannot reach the network, so tool calls are exactly where effects enter the world. The Gate's decision is recorded in history before the effect, and the tool runs with the arguments the Gate decided on, taken from the recorded decision and never recomputed by the current code. If the current code proposes different arguments for a step the Gate already decided, the run blocks rather than running something nobody saw ([D29](docs/decisions.md#d29--2026-09-28--a-tool-runs-with-the-arguments-the-gate-decided-on)).

**`human(prompt, options)`** is the same wait without the Gate, for decisions that are not tool calls. It records `ApprovalRequested` with a `uuid()` approval id and resolves when someone runs `capstan approve` or `capstan deny`, or the timeout elapses.

The test that the wait survives a kill is `a human approval wait survives SIGKILL of the worker and resumes under the same approval id` in [sdk/test/kill-points.test.ts](sdk/test/kill-points.test.ts). A `shipGate` workflow calls `human("Ship?")`. The test waits until `ApprovalRequested` is in history and the run holds no task row, then kills the worker. With no worker alive it resolves the approval through the client and watches `ApprovalResolved` land in history, and it checks that the follow-up activity has not run. Then it starts a new worker. The run completes with `shipped: true`, exactly one `ApprovalRequested`, exactly one `ApprovalResolved` carrying the same approval id, and a second attempt to resolve that approval is rejected. The optional [Gate integration suite](sdk/test/agent-e2e.test.ts) covers allow, deny, approval and recovery during the approval wait.

## How we know it works

The tests exercise replay, storage and cost accounting. This section explains the checks and their limits.

**The fault lab** ([lab-l2.md](internal/lab/properties.go), [lab-delta1-2026-09-28.md](internal/lab/harness.go)). The lab runs the real engine against an in-memory store with a virtual clock and a scheduler it controls. A **seed** is one integer. From it the lab draws the scenario (twelve, from a sequential pipeline through Gate approvals, continuation, cancellation and retry), the order in which three workers, the timer sweeper, the task reaper, the approval loop and an external client take their steps, how pairs of background transactions overlap, and which faults to inject: kill a worker between commands, kill it after the effect but before the acknowledgement, crash the server mid-transaction, fail a database call, deliver a task twice, acknowledge late, fire a timer early or twice, deliver a signal while a task is in flight. At most six faults per seed, at most one of each kind. Each seed first runs its scenario with no faults, then with them.

The **oracle** is four properties, checked between scheduled steps and at the end. P1: every effect of a completed run was applied exactly once, read from the destination's idempotency ledger. P2: the faulted run reaches the same terminal state as the fault-free run, and every capture of the history extends the previous one without rewriting it. P3: no timer fires twice, early, or never. P4: history is gap-free and append-only, with `last_event_id` equal to the maximum. A failing seed is a complete reproduction: `go test ./internal/lab -run '^TestLab$' -seed N` replays it. The [PostgreSQL driver](internal/lab/pg.go) exercises the engine against a real database. The lab is finite. It does not explore instruction-level preemption, real process death, HTTP transport or PostgreSQL crash recovery, and a zero-failure seed range is evidence for those executions, not a proof over all workflows.

**Mutation testing** ([lab-mutation.md](cmd/capstan-lab/mutate.go)) asks whether the lab would notice if the engine were wrong. A **mutant** is a deliberate bug applied to production code in a scratch worktree: keep the timer row after delivery, drop the run id from the effect key, treat a Gate `pending` as approval, accept an expired workflow completion. There are 26 in [internal/lab/mutants](internal/lab/mutants/). Each runs the lab at 2,000 seeds and is **caught** if some seed fails. M003 removes a guard comparing the run's last event id with the token's started event id. Its guard is redundant because the guard is redundant on any state the engine can reach: `PollWorkflowTask` writes `TaskStarted` last and stores that same id in the task, and while a task is in flight external events only enter the inbox. That is an argument about reachable states, not a measurement. It does not cover a hand-corrupted database or two mutants applied together.

**Regression coverage.** The following cases explain the corrected behavior and link to permanent tests.

*Poll starvation under lock contention (F02).* Lock the oldest run's row in one transaction and leave a second run's task ready. Both polls returned empty for the whole two-second timeout. `ClaimTask` selected tasks in `visible_at` order, so it kept choosing the locked run's task first; the `NOWAIT` on that run's row failed, the claim rolled back, and the task went straight back to the head of the queue. One busy run stalled everyone behind it. The fix probes the run under a savepoint and skips that run for the rest of the poll, keeping the `NOWAIT` fence. The first version of the fix re-probed 2,048 children of the busy run and hit a five-second deadline; skipping the whole run brought that case to 5.387 ms. [pg_poll_contention_test.go](internal/engine/pg_poll_contention_test.go) now holds the line: a 40-activity fan-out of a locked run sits at the head of the queue with twelve other runs behind it, and six concurrent pollers must drain all twelve within the poll bound, never claim the locked run, and claim each run exactly once; when the lock releases, all forty must be claimed.

*Time-zone-dependent replay (F05).* The sandbox replaced `Date.now()` with the recorded clock but delegated parsing and the local-time methods to the host. The same recorded instant gave `new Date(0).getHours()` as 0 under UTC and 19 under Toronto, and offset-free date strings parsed five hours apart. A worker in a different zone would replay a different decision. The fix makes workflow `Date` operations UTC, parses only ISO strings with an explicit offset, and rejects locale-dependent formatting. [replay-timezone.test.ts](sdk/test/replay-timezone.test.ts) replays one history under UTC, Asia/Kolkata (+05:30), Pacific/Kiritimati (+14:00, already the next calendar day) and America/St_Johns (-03:30, on daylight time that morning) and requires byte-identical commands that carry the UTC reading of the clock. Workflows that depended on the host zone need replay review before resuming.

*Cost accounting (F04, F07).* Three bugs in one area. A provider timeout or malformed usage was sanitised to zero tokens, which could release a billed reservation; now required counts are validated, unknown usage keeps the estimate, and malformed success fails with `ModelUsageInvalid`. A `CAPSTAN_MODEL_PRICES` override replaced the defaults in the SDK but merged them on the server, so the two sides estimated $0.000002 and $0.002466 for the same request; now both merge. And the cap trusted the worker's estimate: a $1 cap accepted a $0 reservation and then recorded $2 of real usage. That one became D32, described above, and the bounded reservation tests in [ledger_bounds_test.go](internal/engine/ledger_bounds_test.go). The remaining fixes were an unauthenticated `/metrics` endpoint (F01) and configured credentials reaching logs, RPC errors and failure diagnostics (F03, F06).


**Process-level properties.** Replay safety over **random server kill points** ([kill-points.test.ts](sdk/test/kill-points.test.ts)): for each seed, a workflow with four to six steps that records a `uuid()` and a `random()` in every activity input is killed at a seed-chosen event count, on half the seeds together with its worker, and must complete once with a byte-identical prefix, contiguous event ids and replayed decisions equal to the recorded ones; a failing seed reproduces with `CAPSTAN_KILL_SEED`. The **exactly-once effect** test described above. **Poll contention**, **time-zone replay** and the **cost cap**, described above. **Approval recovery**, including the Gate case asserting the resumed approval id.

## Run it yourself

From the repository root, use Bash with Go 1.26.4, Node 26, npm, OpenSSL, Docker Compose and PostgreSQL client tools on PATH. The example uses only local arithmetic; it needs no provider or Gate account.

Run the following in one shell. The Compose project name reuses the shared development PostgreSQL on port 55432. A fresh database keeps this run separate from other work.

<!-- quickstart:start -->
```bash
export PATH="$HOME/.local/bin:$PATH"
export GOTOOLCHAIN=go1.26.4 COMPOSE_PROJECT_NAME=capstan
make pg-up
npm --prefix sdk ci
mkdir -p tmp
umask 077
go build -o tmp/quickstart-server ./cmd/capstan-server

export CAPSTAN_DEMO_DB="capstan_final_quickstart_$(date +%s)_$$"
PGPASSWORD=capstan createdb -h 127.0.0.1 -p 55432 -U capstan "$CAPSTAN_DEMO_DB"
export CAPSTAN_DATABASE_URL="postgres://capstan:capstan@127.0.0.1:55432/$CAPSTAN_DEMO_DB?sslmode=disable"
export CAPSTAN_API_KEY="$(openssl rand -hex 32)"
printf '%s' "$CAPSTAN_API_KEY" > tmp/quickstart-key
export CAPSTAN_API_KEY_HASHES="local:$(tmp/quickstart-server hash-key < tmp/quickstart-key)"
export CAPSTAN_ADDR=127.0.0.1:7773 CAPSTAN_ADDRESS=http://127.0.0.1:7773
export CAPSTAN_MIGRATE=true

tmp/quickstart-server serve > tmp/quickstart-server.log 2>&1 &
CAPSTAN_DEMO_SERVER_PID=$!
export EVIDENCE_QUEUE=quickstart EVIDENCE_IDENTITY=quickstart-worker
export EVIDENCE_WORKFLOWS="$PWD/examples/evidence/load.ts"
(cd sdk && exec node --import tsx ../examples/evidence/worker.ts) > tmp/quickstart-worker.log 2>&1 &
CAPSTAN_DEMO_WORKER_PID=$!

curl --retry 30 --retry-connrefused --retry-delay 1 --fail --silent --show-error "$CAPSTAN_ADDRESS/readyz"
node sdk/bin/capstan.mjs start loadFive quickstart-1 --queue quickstart
node sdk/bin/capstan.mjs describe quickstart-1
```
<!-- quickstart:end -->

Open [http://127.0.0.1:7773/ui/](http://127.0.0.1:7773/ui/). Paste the key from `tmp/quickstart-key`, connect, and open `quickstart-1`. The workflow calls `step` five times and returns `5` ([source](examples/evidence/load.ts)). If the CLI initially says `running`, run `describe` again.

What to look at once it has run. The viewer shows the run's history as a timeline, with the attributes and payloads of each event under it: `RunStarted`, then five `ActivityScheduled` and `ActivityCompleted` pairs with their `seq` numbers and recorded results, each wrapped in its workflow task events, then `RunCompleted` with the result `5`. `node sdk/bin/capstan.mjs history quickstart-1` prints the same events, and `PGPASSWORD=capstan psql -h 127.0.0.1 -p 55432 -U capstan "$CAPSTAN_DEMO_DB" -c 'select event_id, type from event order by event_id'` shows the rows they came from. The ledger is the `ai_call` table in the same database; for this run it is empty, because `loadFive` makes no `model()` call. To watch a crash and recovery, run `scripts/demo-crash.sh` from the same shell. It runs `crashSurvivor` on its own database and port 7302, waits for the first timer, kills the server, restarts it, prints the final history from the CLI, and ends with a PASS line for the result, the unchanged prefix and the timer firings. It can run beside the quickstart without touching it.

When finished, run this in the same shell. It removes only the example's processes, key and database. Leave the shared PostgreSQL running.

<!-- quickstart:cleanup -->
```bash
kill "$CAPSTAN_DEMO_WORKER_PID" "$CAPSTAN_DEMO_SERVER_PID"
wait "$CAPSTAN_DEMO_WORKER_PID" "$CAPSTAN_DEMO_SERVER_PID" || true
PGPASSWORD=capstan dropdb -h 127.0.0.1 -p 55432 -U capstan "$CAPSTAN_DEMO_DB"
rm tmp/quickstart-key
```
<!-- quickstart:cleanup:end -->

## Reading order for the code

One `.proto` is the contract between Go and TypeScript, and the decisions log records every place the implementation refined the design. Start there, then read one file per layer.

| Layer | Read first | Why |
| --- | --- | --- |
| Contract | [proto/capstan/v1/capstan.proto](proto/capstan/v1/capstan.proto), [docs/decisions.md](docs/decisions.md) | Every event, command and RPC in one place; D1 to D33 explain the choices that are not obvious from the code. |
| Store | [internal/store/store.go](internal/store/store.go) | The rules every store honours: everything inside `InTx`, append-only gap-free history, notifications only on commit, time always passed in by the caller. Then [pgstore/history.go](internal/store/pgstore/history.go) for how `AppendEvents` enforces the gap-free rule, and the [migrations](internal/store/pgstore/migrations/) for the tables. |
| Engine | [internal/engine/workflow_task.go](internal/engine/workflow_task.go) | `PollWorkflowTask` claims a task, appends `TaskStarted` and builds the token; the completion path turns a worker's commands into events and updates the run in the same transaction. Then [commands.go](internal/engine/commands.go), [timers.go](internal/engine/timers.go) and [ledger.go](internal/engine/ledger.go). |
| Server | [internal/server/handlers.go](internal/server/handlers.go) | The authenticated Connect services. Then [loops.go](internal/server/loops.go) for the background jobs (timers, expired leases, run deadlines, Gate approval checks) and [longpoll.go](internal/server/longpoll.go). |
| SDK | [sdk/src/workflow/index.ts](sdk/src/workflow/index.ts) | The whole API workflow code can see, with the sandbox rules in its header. Then [replay/runtime.ts](sdk/src/replay/runtime.ts) for the replay loop and command matching, [sandbox/globals.ts](sdk/src/sandbox/globals.ts) for what replaces `Date` and `Math.random`, [worker/index.ts](sdk/src/worker/index.ts) for the host-side pollers, and [worker/builtins/](sdk/src/worker/builtins/) for the Gate and model activities. |
| Lab | [internal/lab/harness.go](internal/lab/harness.go) | The fault kinds and campaign options. Then [properties.go](internal/lab/properties.go) for P1 to P4, [scenario_catalog.go](internal/lab/scenario_catalog.go), and [mutants/](internal/lab/mutants/) for the 26 planted bugs. [cmd/capstan-lab](cmd/capstan-lab/) is the campaign and mutation runner. |
| Examples | [examples/evidence/](examples/evidence/) | Runnable crash, replay, wait and cost demonstrations. |
| Operations | [docs/runbook.md](docs/runbook.md), [infra/](infra/) | The single-server deployment, the CDK stacks and the OIDC deploy. [sdk/src/cli/main.ts](sdk/src/cli/main.ts) is the `capstan` CLI; [internal/server/ui/](internal/server/ui/) is the read-only viewer. |

## Limits and non-goals

Non-goals, from the [design](docs/design.md): multi-tenant SaaS, a hosted control plane for other people, a visual workflow builder, cross-region replication, sub-millisecond scheduling, Kubernetes, and a UI beyond the read-only run viewer. Capstan is single-owner infrastructure. It is not an operated service, and nothing on this page claims production use.

- Activities run at least once. One external effect requires a destination that honours the activity key; arbitrary activity code may execute again. [D3](docs/decisions.md#d3--2026-09-28--no-server-side-idempotency-table), [fault-lab evidence](internal/lab/harness.go).
- The deployment runbook specifies one server. A bounded two-process timer test does not establish general high-availability support. [Audit scope](internal/engine/pg_poll_contention_test.go).
- Load results depend on the host, database and workload. Use the [load driver](scripts/run-load.mjs) to measure the target environment.
- The fault lab covers a finite scenario and fault catalogue. It does not explore every workflow, instruction-level interleaving or network failure. [Fault model](internal/lab/properties.go).
- Local Compose disables PostgreSQL durability settings for tests. Process restart demos do not prove recovery from database or host power loss. [Compose configuration](compose.yaml).
- Histories have a configured size limit; use continuation for longer workflows. An orphaned model reservation can consume budget without a provider call. [History limit](internal/engine/workflow_task.go), [D28](docs/decisions.md#d28--2026-09-28--a-lost-reserve-acknowledgement-stays-counted-until-midnight).
- Workflow code runs in `node:vm` to support replay safety. Workflows must use the SDK for I/O, clocks and randomness. **`node:vm` is not an isolation boundary for untrusted code.** Workers run trusted code only. Workflow `Date` operations use UTC; ambiguous date strings and locale-dependent formatting are rejected. Use ISO dates with an explicit offset when importing dates. Existing workflows that used the worker's local timezone need replay review before resuming. [Sandbox code](sdk/src/sandbox/), [security audit](internal/server/audit_live_security_test.go).
- Payloads are opaque. Capstan stores activity inputs and results, signals and workflow results as supplied, and exposes them to API key holders through history, the CLI and the viewer. It redacts configured credentials from diagnostics, but it does not inspect arbitrary payloads. An activity that returns a secret puts it in history. Return secret references; keep credentials out of inputs, results, markers and failure details. [D33](docs/decisions.md#d33--2026-09-28--payloads-are-opaque-and-the-docs-say-so), [privacy probe](internal/server/audit_live_security_test.go).
- RPCs and `/metrics` require an API key. `/ui/` serves public static files and obtains run data through authenticated RPCs. The quickstart binds to loopback. [Authentication evidence](internal/server/audit_live_security_test.go).

## Verification commands

Run `make verify` with the prerequisites above. Run the [crash demo](scripts/demo-crash.sh), [blocked-code demo](scripts/demo-blocked.sh) and [idle-wait demo](scripts/demo-idle-wait.sh) to inspect real workflow histories. Generated output stays under the ignored `tmp/` directory.

## License

None yet.
