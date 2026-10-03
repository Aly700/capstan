# Capstan — design spec

Date: 2026-09-28. Status: approved.

Implementation refinements are recorded in [D1–D30](decisions.md); the frozen
contracts govern event names, schema, and public signatures where this initial design
differs. The [independent audit](evidence/audit-2026-09-28.md) records executed
checks and unresolved claims. AWS proof remains outside that local audit.

A capstan is the winch that hauls an anchor chain. It ratchets, so it holds what it has
gained even under load and never slips back. That is the guarantee this engine makes.

## 1. Purpose

Capstan is a durable execution engine for agent work. A workflow is ordinary TypeScript
that may call tools, call models, sleep for days, and wait on a human decision. If the
worker crashes, the server is redeployed, or the laptop closes mid-run, the workflow
resumes from recorded history and sees the same recorded values. Activities execute at
least once; external effects apply once only when their destination honors the stable
activity idempotency key (D3).

It exists for three reasons, in order:

1. **The correctness problem is the point.** Re-running a workflow must produce the same
   steps in the same order, and the engine must catch it when that stops being true.
   This is a harder guarantee than anything in AgentOps Gate and is the reason to build it.
2. **It completes the Gate.** The Gate answers ALLOW / DENY / REQUIRE_APPROVAL and creates
   a durable approval. Nothing on the owner's machine survives the wait. Capstan is what
   waits.
3. **It runs real work.** Codex lanes started from a terminal die with the
   session. A lane becomes a workflow: fan out to worktrees, gate the merge on an approval,
   survive a crash.

### Non-goals

Multi-tenant SaaS, a hosted control plane for other people, a visual workflow builder,
cross-region replication, sub-millisecond scheduling, Kubernetes, and a UI beyond the
read-only run viewer. Capstan is single-owner infrastructure that happens to be built to
production standards.

## 2. Decisions already made (owner, 2026-09-28)

| Decision | Choice |
| --- | --- |
| Shape | Server owns history, timers and task queues; workers run user code; RPC between |
| Server language | Go |
| Worker SDK language | TypeScript, so existing agents are clients |
| Agent layer | Model calls, tool calls through the Gate, human steps — all durable |
| Test method | Fault lab: virtual clock, seeded fault injection, properties asserted per seed |
| Proof | Deployed to AWS, a real Codex lane runs on it, survives a kill mid-run |
| Repo | Private: `Aly700/capstan` |

Rejected: a library-only design with no server (deletes the scheduling and matching
problems, which are the interesting ones); building the agent layer on real Temporal
(deletes the hard part entirely).

## 3. Stack (verified 2026-09-28)

| Layer | Choice | Note |
| --- | --- | --- |
| Server | Go 1.26.4 (the installed toolchain; do not upgrade mid-build) | stdlib-first |
| RPC | Connect (connectrpc.com/connect, `@connectrpc/connect-node` 2.2.0) over HTTP/2 | gRPC-compatible, first-class TypeScript |
| Schema | Protobuf via `buf`, `@bufbuild/protobuf` 2.15.0 | one `.proto` is the contract |
| Storage | PostgreSQL 16, `pgx` v5, numbered SQL files in `migrations/` embedded with `embed` and applied in order in one transaction against a `schema_migration` table | single store, no Redis |
| Worker SDK | TypeScript 7.0.2 on Node 26, `zod` 4.6.5 for payload schemas | workflow sandbox uses `node:vm` |
| Tests | Go stdlib `testing` + `testing/synctest` for the fault lab; `vitest` 5.0.2 for the SDK | |
| Infra | AWS CDK, ECS Fargate, RDS Postgres, GitHub OIDC | same pattern proven in AgentOps Gate |

No third-party workflow, queue, or actor library. The point is to build the mechanism.

## 4. Core model

Three nouns. A **run** is one execution of a workflow, identified by a caller-supplied
`run_id` so starting twice with the same id is idempotent. **Commands** are what worker
code asks for. **Events** are what the server durably recorded. Workers never write
history; they return commands, and the server decides what events those become.

### Event history

Every run owns an append-only, gap-free sequence of events numbered from 1. The history
records the run's transitions. A mutable run row projects its status, result and last
event ID; engine transactions update that projection atomically with history.

Event types:

```
RunStarted            RunCompleted        RunFailed        RunCancelRequested
RunCancelled          RunTimedOut         RunContinuedAsNew
TaskScheduled         TaskStarted         TaskCompleted    TaskFailed
ActivityScheduled     ActivityStarted     ActivityCompleted
ActivityFailed        ActivityTimedOut    ActivityCancelled
TimerStarted          TimerFired          TimerCancelled
SignalReceived        MarkerRecorded
ApprovalRequested     ApprovalResolved
```

Commands a worker may return when it completes a workflow task:

```
ScheduleActivity   StartTimer   CancelTimer   RequestApproval
RecordMarker       CompleteRun  FailRun       ContinueAsNew
```

### The same-steps rule

Workflow code must ask for the same things in the same order when re-run. The engine does
not trust that; it checks it. During replay the SDK compares every command the code emits
against the event recorded at that position. A mismatch in command type or in the fields
that identify the step means the code changed underneath a live run. The SDK fails the
workflow task with a `HistoryMismatch` error naming the position, the expected event and
the emitted command, and the server marks the run **blocked** rather than failed.

Blocked is deliberate. A failed run is over; a blocked run is paused on a code problem,
and once the code is fixed or a patch marker is added it resumes from the same history.
That distinction is the difference between an engine that punishes a deploy and one that
survives it.

To keep the rule satisfiable, workflow code cannot reach the outside world. It runs in a
`node:vm` context whose globals exclude `fetch`, `fs`, `net`, `process.env`, and timers,
and which provides instead:

| API | Behaviour on first run | Behaviour on replay |
| --- | --- | --- |
| `now()` | the time the current task started, from history | the same recorded time |
| `random()` | seeded from `run_id` plus the command index | the same value |
| `sleep(d)` | emits `StartTimer` | returns when `TimerFired` is in history |
| `activity(name, input, opts)` | emits `ScheduleActivity` | returns the recorded result |
| `signal(name)` | waits | resolves from `SignalReceived` |
| `sideEffect(fn)` | runs `fn`, records the value in a marker | returns the recorded value |
| `patched(id)` | records a marker, returns true | returns whether the marker exists |
| `uuid()` | `sideEffect` over `crypto.randomUUID` | the recorded id |

`patched(id)` is how live runs survive a code change: new runs take the new branch and
record the marker, old runs find no marker and take the old branch. `deprecatePatch(id)`
inverts it once no old runs remain.

### Delivery guarantees, stated plainly

- A workflow's *decisions* happen once, because they come from history.
- An *activity* can run more than once: a worker may complete the work and die before
  reporting it, and the server will hand the task to another worker. Any activity with an
  external effect therefore carries an idempotency key derived from
  `run_id + activity_id + attempt-independent seed`. The Gate already implements exactly
  this contract on `POST /decisions`, which is why the two systems fit.
- A timer fires exactly once, because firing it deletes the timer row, appends
  `TimerFired`, and enqueues the workflow task in a single transaction.

## 5. Storage

One PostgreSQL database. Tables:

- `run` — `run_id` (pk), `workflow_type`, `task_queue`, `status`, `started_at`,
  `closed_at`, `last_event_id`, `input`, `result`, `search_attrs` jsonb.
  `status` ∈ `running | completed | failed | cancelled | timed_out | blocked | continued`.
- `event` — `(run_id, event_id)` pk, `type`, `payload` jsonb, `at`. Append-only; a trigger
  rejects update and delete.
- `task` — pending workflow and activity tasks: `id`, `run_id`, `kind`, `task_queue`,
  `scheduled_event_id`, `visible_at`, `attempt`, `locked_until`, `worker_id`.
- `timer` — `run_id`, `timer_id`, `due_at`, indexed on `due_at`.
- `signal_inbox` — signals delivered to a run that is not currently running a task.
- `idempotency` — `key` (pk), `run_id`, `result_hash`, `at`; guards external effects.
- `ai_call` — `run_id`, `activity_id`, `model`, tokens, `cost_usd`, `at`. The cost ledger.
- `approval` — `run_id`, `approval_id`, `gate_decision_id`, `status`, `requested_at`,
  `resolved_at`, `resolver`.

Two rules the schema enforces rather than documents: history is append-only, and
`last_event_id` on `run` is updated in the same transaction that appends the event, so
optimistic concurrency has a single number to compare.

### Appending history

Every workflow task completion is one transaction:

1. `SELECT last_event_id FROM run WHERE run_id = $1 FOR UPDATE`
2. reject with `TaskTokenStale` if it differs from the token the worker holds
3. append the events the commands imply
4. update `run.last_event_id`, and `status` when the run closes
5. insert activity tasks, timers, or approval rows the commands created
6. insert the next workflow task if the run still has work

If any step fails the whole thing rolls back and the worker retries with fresh history.
There is no window where an event exists without its task, or a task without its event.

## 6. Server

One Go binary, several goroutine groups, all speaking to the same database.

- **Worker API** (Connect service, see §7) — poll, complete, fail, heartbeat.
- **Client API** — start a run, signal it, cancel it, query status, read history.
- **Timer sweeper** — wakes at the earliest `due_at`, fires everything due in one
  transaction each, and sleeps again. A jittered fallback tick covers clock changes.
- **Task reaper** — returns tasks whose `locked_until` has passed to the pool and applies
  the retry policy; this is what makes a dead worker harmless.
- **Run timeout sweeper** — closes runs past their execution timeout.

Matching is `SELECT ... FOR UPDATE SKIP LOCKED` on `task` with a long poll: the poll holds
for up to 30 seconds, returning as soon as a row is claimable. This is simple, correct,
and honest about its ceiling; the README will state the throughput it buys rather than
implying a distributed matching service.

## 7. Protocol

One `capstan.v1` proto package, the single contract between Go and TypeScript.

```
service WorkerService {
  rpc PollWorkflowTask(PollWorkflowTaskRequest) returns (PollWorkflowTaskResponse);
  rpc CompleteWorkflowTask(CompleteWorkflowTaskRequest) returns (CompleteWorkflowTaskResponse);
  rpc FailWorkflowTask(FailWorkflowTaskRequest) returns (FailWorkflowTaskResponse);
  rpc PollActivityTask(PollActivityTaskRequest) returns (PollActivityTaskResponse);
  rpc CompleteActivityTask(CompleteActivityTaskRequest) returns (CompleteActivityTaskResponse);
  rpc FailActivityTask(FailActivityTaskRequest) returns (FailActivityTaskResponse);
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
}

service ClientService {
  rpc StartRun(StartRunRequest) returns (StartRunResponse);
  rpc SignalRun(SignalRunRequest) returns (SignalRunResponse);
  rpc CancelRun(CancelRunRequest) returns (CancelRunResponse);
  rpc DescribeRun(DescribeRunRequest) returns (DescribeRunResponse);
  rpc GetHistory(GetHistoryRequest) returns (GetHistoryResponse);
  rpc ResolveApproval(ResolveApprovalRequest) returns (ResolveApprovalResponse);
}
```

`PollWorkflowTaskResponse` carries the full history up to the task plus a **task token**
containing `run_id` and `last_event_id`. `CompleteWorkflowTask` sends the token and the
commands; a stale token is rejected. Payloads are `bytes` with a `content_type`, so the
SDK owns serialization and the server never parses user data.

## 8. TypeScript worker SDK

Published as `@capstan/sdk` inside the repo, not to npm.

**Authoring.** A workflow is an exported async function; activities are plain async
functions registered by name.

```ts
export async function reviewLane(input: { branch: string }) {
  const plan = await activity(analyze, { branch: input.branch });
  const results = await Promise.all(plan.tasks.map((t) => activity(runLane, t)));
  const decision = await tool("git.merge", { branch: input.branch });  // may pause for a human
  if (!decision.allowed) return { merged: false, reason: decision.reason };
  await activity(merge, { branch: input.branch });
  return { merged: true, lanes: results.length };
}
```

**Runtime.** The worker process runs two loops, one polling workflow tasks and one polling
activity tasks, each with its own concurrency limit. A workflow task is executed by
building a fresh `node:vm` context, loading the workflow bundle into it, replaying the
history through the SDK's scheduler, then draining microtasks until the code either
finishes or is blocked on a command that has no recorded result yet. The commands
accumulated during that pass are returned to the server.

`Promise.all` over activities works naturally: each call emits its command during the same
pass, so the batch is scheduled together. The scheduler drains microtasks deterministically
between steps rather than yielding to Node's event loop, which is what makes concurrent
workflow code reproducible at all.

**Replay checker.** Available as `capstan replay <run-id>` in the CLI: fetch a history,
run the current code against it locally, and report the first position where the code and
the history disagree. This is the debugging tool that makes a blocked run actionable.

## 9. Agent layer

Three durable primitives on top of the activity mechanism.

**`model(request)`** — an activity that calls Anthropic, records request, response, tokens
and cost in `ai_call`, and returns the parsed result. On replay nothing is called and
nothing is billed. Before calling the provider, the worker reserves an estimate with the server against
`CAPSTAN_DAILY_CAP_USD`. Concurrent reservations cannot exceed that day's cap. Actual
spend can exceed reservations if the estimate is too low; the current protocol relies
on callers supplying upper bounds. Cost for a run is `SUM(cost_usd)` over its
`ai_call` rows and appears in `DescribeRun`.

**`tool(name, args)`** — the join with AgentOps Gate:

1. an activity posts the proposed call to the Gate's `POST /decisions` with an idempotency
   key derived from the run and activity ids;
2. `ALLOW` schedules the real tool activity;
3. `DENY` fails the step with the rule that denied it;
4. `REQUIRE_APPROVAL` emits `ApprovalRequested` with the Gate's approval id, and the
   workflow waits on a signal. The run is idle in the database, holding no worker, no
   connection, and no memory. When the approval is resolved in the Gate, its notification
   reaches `ResolveApproval`, which appends `ApprovalResolved` and schedules a workflow
   task. Five minutes or five days later, the workflow continues on the next line.

**`human(prompt, options)`** — the same wait without the Gate, for decisions that are not
tool calls: pick an art direction, approve a deploy, choose between two plans.

## 10. Fault lab

A Go test harness that runs the whole engine in memory with three things under its control:
the clock, the store, and the interleaving of goroutines. The stdlib `testing/synctest`
provides the virtual clock; the store is an in-memory implementation of the same interface
the Postgres store satisfies; the scheduler orders concurrent steps from a seed.

Each seed executes a scenario and injects faults drawn from that seed:

- kill the worker between any two commands, or after doing an activity's work but before
  reporting it;
- kill the server mid-transaction;
- deliver a task twice, or deliver an acknowledgement late;
- fail a database call;
- fire a timer early or twice;
- deliver a signal while a workflow task is in flight.

Four properties are asserted on every seed:

- **P1** a completed run applied each external effect exactly once (checked through the
  idempotency table, not by trusting the activity);
- **P2** a run killed at any point and resumed reaches the same end state, and its history
  is a prefix-extension of the history before the kill;
- **P3** no timer fires twice and no timer is lost;
- **P4** history is gap-free, append-only, and `last_event_id` always matches the maximum.

A failing seed is a complete reproduction: `go test -run Lab -seed 918273` replays it
exactly. The README's headline number is the count of real defects this found in Capstan,
with each one named, linked to its seed, and covered by a permanent regression test.

## 11. Deployment

One `capstan-server` image on ECS Fargate with RDS Postgres, deployed by CDK with a GitHub
OIDC role, under the ten-dollar ceiling with a Budgets alarm, destroyable to zero. This is
the pattern AgentOps Gate already proved, reused deliberately rather than reinvented.

Workers run wherever the work is: on the M2 for Codex lanes, on the owner's Mac, or as a
second Fargate service. A worker needs the server address and an API key, nothing else.

## 12. Security

- Every RPC requires an API key; keys are namespaced and stored hashed.
- Workflow code cannot reach the network, the filesystem, or the environment. Activities
  can, which is exactly why tool calls route through the Gate.
- Payloads are opaque to the server; it never deserializes user data.
- No static AWS credentials anywhere; the task role grants only what it needs.
- The deployment design supplies the Gate key from Secrets Manager to the worker that
  proposes calls and to the server approval watcher (D6). Local configuration uses
  environment variables; the audit does not certify deployed secret wiring.
- Configuration credentials are redacted from diagnostic paths. Opaque workflow
  payloads remain caller-controlled and must not contain secrets. The `ai_call` ledger
  stores token counts and cost; model prompts and responses live in workflow history.

## 13. Testing and evidence

Every claim in the README must run, the same rule as the Gate.

| Claim | How it is shown |
| --- | --- |
| A run survives a crash | Recorded session: kill the server mid-run, restart, the workflow finishes correctly |
| Effects apply once | The fault lab's P1 across thousands of seeds, plus the idempotency table after a duplicate-delivery scenario |
| A workflow can wait days | An approval-gated run resumed after a clock jump, with the run holding no resources while idle |
| Code changes do not corrupt live runs | A live run blocked by a changed workflow, then resumed after a patch marker |
| Throughput | A load run at two concurrency levels with the numbers and the ceiling named |
| Cost | The `ai_call` ledger for a real agent run |

Gate: `make verify` runs `go vet`, `go test ./...` including the fault lab at a fixed seed
count, the SDK's vitest suite, a Postgres integration suite in Testcontainers, `buf lint`,
and a replay test that runs recorded histories against current code.

## 14. Build order

1. **Store and history** — schema, migrations, append transaction with optimistic
   concurrency, run lifecycle, the append-only trigger, Testcontainers suite.
2. **Timers and matching** — task table, `SKIP LOCKED` long poll, timer sweeper, reaper,
   retry policy.
3. **Protocol and server** — the proto, Connect handlers for both services, API keys.
4. **Worker SDK core** — sandbox, scheduler, replay checker, activity worker, `capstan`
   CLI (`start`, `describe`, `history`, `replay`, `signal`, `approve`).
5. **Agent layer** — model activity with the ledger and cap, Gate client, tool and human
   waits.
6. **Fault lab** — virtual clock, in-memory store, injector, the four properties, seed CLI.
7. **Infrastructure** — CDK stacks, OIDC deploy workflow, Budgets alarm, runbook.
8. **Real workload** — a Codex lane as a workflow, with the merge behind an approval.
9. **Adversarial review** — an independent review whose job is to make a run lose an effect,
   resume wrong, or fire a timer twice, and to check every README claim against evidence.

Order: 1 → (2, 3) → 4 → (5, 6) → 7 → 8 → 9.

## 15. Risks

- **The sandbox is the hardest single piece.** If `node:vm` plus a hand-drained microtask
  queue proves unworkable, the fallback is a convention-based runtime in normal Node where
  the replay checker still catches violations; the guarantee weakens from prevention to
  detection, and the README says so.
- **Postgres-as-queue has a ceiling.** Acceptable and stated; the load evidence names it.
- **Scope.** Nine components is a large build. Components 1 through 6 are the project;
  7 and 8 are what make it real; if time runs short, 8 is the one to defer, not 6.
- **The Gate must be running** for the tool path. Local Compose is enough for development;
  the deployed proof needs the Gate deployed at the same time, briefly, and destroyed after.
