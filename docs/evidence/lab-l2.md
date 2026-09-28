# Fault lab implementation

## Historical implementation and measurements

Historical seed timings and test counts now trace to [original logs](raw/historical/lab/), including [the original test](raw/historical/lab/l2-2000-final.log) and [the Delta 1 gate](raw/historical/lab/delta-make-verify-final.log). Current measurements appear below.

Measured on Apple M5 Max, Go 1.26.4 (`darwin/arm64`); SDK gates used Node 26.8.1.

The lab runs the merged engine on memstore with three worker actors, the timer,
task-reaper, approval, and run-timeout loops, and an external-client actor. Two
simultaneous root runs share a task queue and the worker pool. A seeded scheduler
selects actor steps and also overlaps pairs of background-loop calls, choosing the
order of their individual store transactions. `TestLab` runs inside `testing/synctest`;
the standalone campaign uses an explicit logical clock because the testing bubble
is only available to tests. Both modes avoid real-time sleeps for durable waits.

Each seed first runs its scenario without injected faults, then with faults. The
catalogue covers twelve primary scenarios: sequential and parallel effects, signals,
human and Gate approvals, Gate errors and late decisions, continuation, cancellation,
run timeout, approval expiry, and activity retry. Every primary runs beside a peer
with distinct effect inputs. Explicit
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
operations, falling back to the selected transaction's commit boundary. All 35 Tx
methods are intercepted. This includes the transaction after the Gate call. An idle
Gate scenario jumps to its next persisted poll deadline after a crash leaves a lease.

The Gate client additionally returns pending, a 503 error, a timeout, then approval
in sequence. A separate case advances time during the external Gate call so that an
approved decision arrives after the approval deadline; the result must be expired.
These are engine-facing client responses, not HTTP transport tests.

The overlapping background actors rendezvous at `InTx`. The seeded scheduler runs
one whole transaction, checks history and timers, then chooses which actor may run
its next transaction. This explores orders between lease and resolution transactions
and between sweeper items. Memstore serializes each transaction under one mutex;
the lab does not interleave operations inside that transaction. Worker steps with
fault injection remain serial, and overlap waves do not arm the store fault injector.

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

Ten bounded protocol probes supplement workflow outcomes: heartbeat renewal, retry
deadline, cancellation before start, failed-workflow lease release, approval
idempotency, signal ordering, retry attempt/key/token behavior, workflow token fields,
closed inbox cleanup, and exclusive claims. Each seed selects one probe against a
fresh real engine and memstore. The [mutation score](lab-mutation.md) measures whether
these checks and the scenario checks detect independently applied production bugs.

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

The original, pre-Delta 1 2,000-seed test took 16.87 seconds without the race detector. The full
200,000-seed campaign passed in 12m34.513863084s, with all nine fault types exercised
and zero known-seed exclusions. Its 46,179,964 reported steps count faulted executions;
each seed additionally completed a fault-free baseline. Both the lab race gate and
`make verify` passed, including mandatory PostgreSQL tests and 293 SDK tests. See
[the campaign report](lab-2026-09-28.md) for the larger run and
[the defect register](defects.md) for confirmed findings and known-seed handling.

Delta 1's larger fault model and bounded campaign are recorded separately in
[the concurrent campaign](lab-delta1-2026-09-28.md): 148,554 seeds passed in
19m50.041s at parallelism eight, with zero failing seeds and 9,870,634 interleaved
transaction steps. The final 2,000-seed gate passed in 52.109 seconds. These results supersede the
original timings for the current implementation; the original report remains a
record of the earlier, narrower campaign.

## PostgreSQL feasibility

A small real-clock campaign is practical as a separate opt-in driver. The original
20–25 ms memstore leases were too short for the same sequence of network round trips;
the PG driver uses 500 ms leases and completes each claimed task in one actor step.
It runs outside synctest, on two fresh databases in the existing shared PostgreSQL,
with two runs per seed, durable timers, effects, duplicate acknowledgements, rollback
faults, and engine reconstruction. It does not emulate network I/O with fake time.
See [the PostgreSQL evidence](lab-pg.md) for the exact command, measurements, timer
deadline oracle, and the narrower scenario and scheduling bounds.

## Limits

The memstore scheduler explores interleavings between background-loop transactions,
with actor/API steps elsewhere and explicit replay-command and transaction-operation
interruption points. It does not explore arbitrary
instruction-level preemption, real process termination, network transport failures,
or PostgreSQL crash recovery. The optional PG driver uses serial actor steps and
real network timing; its seed alone cannot control timer eligibility to the same
extent as the logical-clock campaign.

The frozen Store API exposes only inbox size, not inbox contents. Timer checks bound
buffered-fire allowances by that size and recheck after flush; the early-sweep check
also asserts zero immediate fires. Closing runs may legally discard pending timers
and buffered events. Injected signals are limited to activations that remain open so
the delivery assertion does not reject that documented close behavior.

Passing this finite catalogue and seed range is evidence for those executions, not a
proof over all workflows. No frozen contracts or production engine/store code were
changed by the lab implementation.

<!-- final-numbers:start -->
## Final code (904cb6c)

Measured on 2026-09-28. These numbers are recomputed by
[verify-numbers.py](raw/verify-numbers.py) from the committed observations.

The current fault model completed **200,000 seeds**, with
**200,000 passing, 0 failing and
0 known-failure exclusions**. Elapsed:
**23m31.169260583s**. The seed range is 0–199999; parallelism is 8,
with 3 workers and a 1,000-step bound per seed.

The faulted executions contain **65,602,948 actor steps**,
**400,000 initial root runs** and
**13,285,961 scheduled store transaction steps**.
Including fault-free baselines, that is 400,000 scenario executions and
800,000 initial root runs, plus 200,000 protocol probes.
Continuation descendants are additional.

Raw [per-seed records](raw/904cb6c/lab-seeds.jsonl.gz), [command output](raw/904cb6c/lab.log)
and the [emitted report](raw/904cb6c/lab-report.md) are committed. The verifier sums
the individual records and checks the report. The logging-only overlay and its
timing overhead are described in [raw/README.md](raw/README.md).

All 9 fault kinds and 5 Gate response kinds occurred. The verifier also checks all 35 transaction wrappers against the Store interface. The final mutation catalogue catches 25 of 26 valid mutants; M003 is equivalent under the documented single-mutant, engine-reachable-state premise. These results do not expand the fault model's limits.
<!-- final-numbers:end -->
