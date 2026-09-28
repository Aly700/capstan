# Fault lab implementation

Measured on Apple M5 Max, Go 1.26.4 (`darwin/arm64`); SDK gates used Node 26.8.1.

The lab runs the merged engine on memstore with three worker actors, the timer,
task-reaper, approval, and run-timeout loops, and an external-client actor. A seeded
scheduler grants one actor a step at a time. `TestLab` runs inside `testing/synctest`;
the standalone campaign uses an explicit logical clock because the testing bubble
is only available to tests. Both modes avoid real-time sleeps for durable waits.

Each seed first runs its scenario without injected faults, then with faults. The
catalogue covers sequential and parallel effects, signals, human and Gate approvals,
continuation, cancellation, run timeout, approval expiry, and activity retry. Explicit
expected results/statuses additionally check the baseline. Workflow functions rerun
from their first line for every task; no saved coroutine stack survives a replay.

## Fault model

| Selection | Injection and check |
| --- | --- |
| `kill-workflow` | Abort replay at a new-command boundary; discard both pollers' volatile tasks/results; expire outstanding leases and reconstruct from history. |
| `kill-activity` | Commit the destination effect, discard the worker before acknowledging it, and retry using the engine-supplied key. |
| `crash-server` | Panic after a selected store operation, unwind the transaction, and create a new engine over the surviving store. |
| `database-failure` | Return an injected error after a selected operation; `Notify`, which has no error return, unwinds with a sentinel panic. |
| `duplicate-task` | Execute/acknowledge a task twice; workflow commands must agree, repeated acknowledgements must be stale, and repeated effects must retain one applied value. |
| `late-ack` | Advance beyond the lease and acknowledge before relying on a reaper; the response must be stale. |
| `early-timer` | Invoke the sweeper before pending deadlines and assert that it fires nothing. |
| `duplicate-timer` | Repeat a successful sweep at the same clock reading and assert that it fires nothing again. |
| `signal-in-flight` | Deliver a signal during the first successful activation; assert its request ID, name, and input occur exactly once in final history. The signal scenario consumes its injected input. |

Fault kinds are selectable and accept integer weights through `lab.Options`. A seed
injects at most six faults, at most one of each kind. Store injection selects the
first or second transaction of an engine call and a boundary after 1–12 successful
operations, falling back to the selected transaction's commit boundary. All 34 Tx
methods are intercepted. This includes the transaction after the Gate call. An idle
Gate scenario jumps to its next persisted poll deadline after a crash leaves a lease.

## Assertions

- P1 reads the destination's idempotency ledger: every effect scheduled by a completed
  run must be present with an applied count of one; attempts are counted separately.
- P2 compares terminal status, continuation links, result and failure with the fault-free
  execution. Each store capture must extend the preceding history without rewriting it.
- P3 tracks timer starts, deadlines, fires, cancellations, and pending rows. A live timer
  must remain pending or have a recorded/buffered terminal event; duplicates are rejected.
- P4 checks contiguous IDs, append-only history, and `last_event_id` after every step.

Synthetic corruption tests demonstrate that the property checks reject lost effects,
changed outcomes, missing/duplicate timers, rewritten history, and dropped signals.
A review mutation that silently discarded injected signals passed the earlier harness;
the strengthened harness rejects it at seed 0. That is a harness correction, not a
Capstan engine defect.

## Reproduction

```sh
GOTOOLCHAIN=go1.26.4 go test ./internal/lab -run '^TestLab$' -seed 14 -lab.known=false -count=1 -v
GOTOOLCHAIN=go1.26.4 go test ./internal/lab -run '^TestLab$' -seeds 2000 -count=1 -v
GOTOOLCHAIN=go1.26.4 go run ./cmd/capstan-lab -seeds 200000 -parallel 8
```

Custom campaign failures include the matching CLI command with scenario, worker,
step-limit and fault options, alongside the default test reproduction. The CLI retains
failures and their traces, bounds parallelism, validates options before scheduling,
and prints failure diagnostics even if writing its Markdown report fails.

The final 2,000-seed test took 16.87 seconds without the race detector. The full
200,000-seed campaign passed in 12m34.513863084s, with all nine fault types exercised
and zero known-seed exclusions. Its 46,179,964 reported steps count faulted executions;
each seed additionally completed a fault-free baseline. Both the lab race gate and
`make verify` passed, including mandatory PostgreSQL tests and 293 SDK tests. See
[the campaign report](lab-2026-09-28.md) for the larger run and
[the defect register](defects.md) for confirmed findings and known-seed handling.

## Limits

The scheduler explores interleavings at actor/API boundaries, with explicit replay
command and transaction-operation interruption points. It does not explore arbitrary
instruction-level preemption, real process termination, network transport failures,
or PostgreSQL crash recovery. Its Gate is an injected approved response.

The frozen Store API exposes only inbox size, not inbox contents. Timer checks bound
buffered-fire allowances by that size and recheck after flush; the early-sweep check
also asserts zero immediate fires. Closing runs may legally discard pending timers
and buffered events. Injected signals are limited to activations that remain open so
the delivery assertion does not reject that documented close behavior.

Passing this finite catalogue and seed range is evidence for those executions, not a
proof over all workflows. No frozen contracts or production engine/store code were
changed by the lab implementation.
