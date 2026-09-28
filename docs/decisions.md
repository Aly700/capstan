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
