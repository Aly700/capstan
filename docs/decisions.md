# Decisions

Numbered, dated, never rewritten. A later decision may supersede an earlier one by number.

## D1 — 2026-09-28 — Contracts first, written by the lead

The proto, the store interface, the engine API, the schema, the conformance rules and the
SDK's public signatures are written by the lead before any lane starts and frozen for the
build. Lanes implement them in parallel. This is the only way nine lanes avoid colliding.

## D2 — 2026-09-28 — The inbox generalises the spec's signal inbox

Every external event (activity results, timer fires, signals, approval resolutions, cancel
requests) waits in the run's inbox while a workflow task is in flight, and is flushed into
history after that task's command events, in arrival order. Consequences: a task's command
events always directly follow its `TaskCompleted`; the task token's `started_event_id` can
be checked strictly against `last_event_id`; replay never sees an external event in the
middle of an activation.

## D3 — 2026-09-28 — No server-side idempotency table

The spec listed an `idempotency` table. It is replaced by a stable per-activity key,
`<run_id>/<seq>`, delivered to every attempt. Capstan guarantees at-least-once activity
execution; an effect happens exactly once when its destination honours the key, as AgentOps
Gate does on `POST /decisions`. The fault lab checks this with a key-honouring effect sink.
Claiming exactly-once execution of arbitrary code would be false, so the README will not.

## D4 — 2026-09-28 — ActivityStarted is not a history event

Attempts, leases and heartbeats live on the task row. `ActivityCompleted`, `ActivityFailed`
and `ActivityTimedOut` carry the attempt number. This keeps history small and makes retries
invisible to replay.

## D5 — 2026-09-28 — Blocked runs resume explicitly

A history mismatch moves a run to `blocked` and appends `RunBlocked`. `ResumeRun` (CLI:
`capstan resume`) moves it back to running and schedules a workflow task. There is no
automatic resume on deploy: resuming is a human decision after reading the mismatch.

## D6 — 2026-09-28 — Gate approvals are watched, not pushed

For `source = GATE`, the server polls `GET /approvals/{id}` with backoff (15 s doubling to
5 min) and appends `ApprovalResolved` when the Gate reports APPROVED, DENIED or EXPIRED.
No change to AgentOps Gate is needed. The call happens outside any transaction.

## D7 — 2026-09-28 — Shared PostgreSQL instead of Testcontainers

Tests use one compose PostgreSQL on port 55432 and create a database per test. Testcontainers
needs extra configuration under colima and would start one container per test package per
lane. CI uses a PostgreSQL service container with the same helper.

## D8 — 2026-09-28 — Model cost is computed by the server

Workers report token counts; the server prices them from its table (overridable with
`CAPSTAN_MODEL_PRICES`). An unknown model is charged its reservation estimate. Reservations
are serialised with an advisory lock so concurrent workers cannot overspend the cap.

## D9 — 2026-09-28 — Transport and port

Connect over HTTP/2 cleartext using the Go standard library's unencrypted HTTP/2 support
(no golang.org/x/net). The server listens on 7233.

## D10 — 2026-09-28 — Long polls default to 20 seconds; HTTPS through API Gateway

The deployed server sits behind an API Gateway HTTP API with a VPC link to Fargate through
Cloud Map: HTTPS on the default execute-api domain, no load balancer, no NAT. API Gateway's
integration timeout is 30 s, so long polls default to 20 s (`CAPSTAN_POLL_TIMEOUT`). Every
RPC is unary, so the Connect protocol works over HTTP/1.1 through the gateway; the SDK uses
HTTP/2 cleartext for `http://` addresses and HTTP/1.1 for `https://`. Proto comments that say
"up to 30 seconds" describe the ceiling, not the default.

## D11 — 2026-09-28 — Compile-time stubs for every cross-lane symbol

`engine.New`, `memstore.New`, `pgstore.Open`, `pgstore.Migrate` and `cmd/capstan-server`
exist as stubs from wave 0 so each lane compiles against real symbols. The owning lane
replaces the stub; no other lane edits it.

## D12 — 2026-09-28 — Cancel commands reference; they do not allocate

`ScheduleActivity`, `StartTimer`, `RecordMarker` and `RequestApproval` allocate the next seq;
their seq must be greater than every seq already allocated in the run (history plus earlier
commands in the same task). `RequestActivityCancel` and `CancelTimer` carry the seq of the
activity or timer they refer to: it must exist in history (or earlier in the same task) and
not be settled yet. The plan's "strictly increasing" rule applies to allocating commands only.
(SDK lane issue 1.)

## D13 — 2026-09-28 — No per-activity cancellation API in v1

The workflow API has no call to cancel one activity. `RequestActivityCancel` stays in the
protocol and the engine supports it, for the lab and a later API; v1 workflows cancel through
run cancellation. (SDK issue 2.)

## D14 — 2026-09-28 — Mismatch position for an extra command

When code emits more commands than an already-completed activation recorded, the mismatch
names that activation's `TaskCompleted` event id. (SDK issue 3; conformance README updated.)

## D15 — 2026-09-28 — now() is set before events resolve

An activation sets `now()` to its `TaskStarted` time first, then resolves external events,
then drains. Signal handlers, condition predicates and continuations all observe the current
activation's time. (SDK issue 4; conformance README steps reordered.)

## D16 — 2026-09-28 — SDK failure metadata travels in details

`ActivityFailure.activityType/seq` and `TimeoutFailure.timeoutType` are carried inside
`Failure.details` as `{"$capstan": {kind, …}, "details": <original>}`. The server never reads
payloads, so this is an SDK-internal encoding. (SDK issue 5.)

## D17 — 2026-09-28 — An unrecordable side effect fails the task

A `sideEffect` callback that throws, returns a non-JSON value, or emits commands fails the
workflow task (`SDK_ERROR`) even if user code catches the error, because continuing would
record a history that cannot replay. (SDK issue 6.)

## D18 — 2026-09-28 — TerminateRun

A run whose workflow task fails forever, or which is blocked with no fix coming, must be
closable by an operator. `ClientService.TerminateRun(run_id, reason)` flushes the inbox,
appends `RunFailed{failure.type = "Terminated", message = reason}` with
`task_completed_event_id = 0`, and closes the run as FAILED. Additive to the protocol; CLI
`capstan terminate`. Implemented at integration.

## D19 — 2026-09-28 — "~" is reserved for continuation run ids

The schema's run_id check allows `~`; StartRun's user-facing validation does not. Continued
runs are named `<base>~<k>`, so they can never collide with a caller-chosen id. (SDK issue 7.)

## D20 — 2026-09-28 — Language-specific fixtures

A fixture may carry `"only": ["ts"]`. Fixture 054 depends on JavaScript promise-continuation
depth when two results arrive in the same activation; the Go reference worker does not model
V8 microtasks and skips it. Workflow authors should not rely on which of two results that
arrive together wins a `Promise.race`: the outcome is stable across replays of the same code,
but it is an engine detail, not a guarantee. (SDK issue 8.)

## D21 — 2026-09-28 — Caller run ids are at most 180 characters

A continuation appends `~<k>` to its base id, and the schema caps run_id at 200 characters.
StartRun therefore accepts caller ids of 1–180 characters, leaving room for up to 19
characters of suffix. (Server lane issue 1.) The engine's contract comment on `Deps` now
states that GATE approvals resolve only through the Gate or by timeout (server issue 2).

## D22 — 2026-09-28 — Money is rounded to the micro-dollar by the engine

The schema stores USD as numeric(12,6), but the price formula can produce smaller amounts (one
claude-sonnet-5 cache-read token costs $0.0000002). The engine rounds every USD amount it
computes (reservation estimates, finished costs) to 6 decimal places, half away from zero,
before it writes the amount or returns it. Both stores then hold the same value, and a repeated
FinishAICall returns exactly what the first one returned. (Engine lane issue 4.)

## D23 — 2026-09-28 — Store precision and updates

PostgreSQL keeps timestamps to the microsecond and run timeouts in milliseconds. The engine
truncates every time it computes (each clock reading and every deadline derived from one) to
the whole microsecond before it reaches a store or a response. StartRun rejects a
task_timeout or run_timeout that is not a whole number of milliseconds. Activity options travel
inside serialized protobufs and are kept exactly.

`Tx.Update*` replaces every field of the record except its key, as pgstore does. The engine
never changes a record's creation fields, so no caller depends on which fields are
"mutable". (Engine lane issue 5.)

## D24 — 2026-09-28 — Due-query limits must be positive

`DueTasks`, `DueTimers`, `DueApprovals` and `RunsPastDeadline` return at most `limit` rows, and
a limit of zero or less returns none (pgstore's behaviour). `ReadHistory` and `ListRuns` keep
their documented rule that a limit of zero or less means no limit. The engine's background
methods reject `limit <= 0` with ErrInvalidArgument.

## D25 — 2026-09-28 — Model prices match by family prefix

The price table is keyed by model family (`claude-opus-5`, `claude-sonnet-5`, `claude-haiku-4-5`,
`claude-fable-5-1`), while workers send the real model id (`claude-opus-5-5`,
`claude-haiku-4-5-20251001`, …). The engine prices a call by the exact key if present, otherwise
by the longest key that is a prefix of the id and ends at a `-` boundary of it; otherwise the
model is unknown and is charged its estimate (D8). Implemented by the lead at integration.

## D26 — 2026-09-28 — Gates run on the pinned toolchain

The machine's default Go is 1.27.1; go.mod and CI say 1.26.4. The Makefile exports
`GOTOOLCHAIN ?= go1.26.4` so local gates run what CI runs. `GOTOOLCHAIN=local make verify`
opts out.
