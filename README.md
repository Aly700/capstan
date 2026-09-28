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

Payloads are opaque. Capstan stores activity inputs and results, signal inputs and workflow
results exactly as the worker sends them, and shows them to anyone holding an API key
(history, `capstan describe`, the `/ui/` viewer). Capstan redacts the credentials it knows
about from errors and logs, but it does not inspect payloads: an activity that returns a
secret puts that secret in history. Return references to secrets, never the secrets.

RPCs and `/metrics` require an API key. `/ui/` serves public static files; its run data
comes from authenticated RPCs. Payloads are opaque and persisted as supplied: callers
must keep credentials out of workflow inputs, results, markers and failure details.

**Cost cap.** The daily AI cap applies to the Toronto calendar day and is absolute for
bounded reservations from the SDK's text-only `model()` path at configured server prices
(D32). Every SDK call sends conservative input and provider-enforced output token bounds
and enables no cache writes. For priced models, the server reserves the larger of its own
priced bound and the worker's estimate. Other callers must cover any higher-cost usage
classes, such as cache writes, in their estimate and keep usage within declared bounds.
A reservation without both bounds is unbounded and trusted as given; unknown models also
rely on the worker's estimate because the server has no price for them (D8). Finish always
records true usage, even above an unbounded or incorrectly declared reservation.
A lost Reserve acknowledgement leaves its reservation counted until that day's midnight.

The deployment runbook specifies one server process. The audit exercised timers with
two processes sharing PostgreSQL; that bounded test is not a general high-availability
or multiple-server support claim.
