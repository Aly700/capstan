# Capstan

A durable execution engine for agent work, with a Go server on PostgreSQL and a
TypeScript worker SDK. Workflows resume from recorded history after a worker or
server restart. Activities execute at least once: an external effect applies once
only when its destination honors the stable activity idempotency key (D3).

Design: [spec](docs/superpowers/specs/2026-09-28-capstan-design.md), refined by
[decisions D1–D30](docs/decisions.md). The [independent local audit](docs/evidence/audit-2026-09-28.md)
records verified behavior, defects and remaining gaps, with commands and raw results.

## Security model

Workflow code runs in `node:vm` to support replay-safety. Workflows must use the SDK for
I/O, clocks, and randomness so replay follows the same steps in the same order.
`node:vm` is **not an isolation boundary for untrusted code**. Workers run trusted code only.
Workflow Date operations use UTC; ambiguous date strings and locale-dependent date
formatting are rejected. Use ISO dates with an explicit offset when importing dates.
Existing workflows that depended on a worker's local timezone need replay review before
resuming with this runtime.

RPCs and `/metrics` require an API key. `/ui/` serves public static files; its run data
comes from authenticated RPCs. Payloads are opaque and persisted as supplied: callers
must keep credentials out of workflow inputs, results, markers and failure details.

The daily AI cap limits reservations in the Toronto calendar day. Actual provider spend
can exceed it if the estimate is too low; it is not an unconditional billing limit.
A lost Reserve acknowledgement leaves its reservation counted until that day's midnight.

The deployment runbook specifies one server process. The audit exercised timers with
two processes sharing PostgreSQL; that bounded test is not a general high-availability
or multiple-server support claim.
