# Fault-lab defect register

## Historical defect register

The historical campaign and gate logs are archived in [raw/historical/lab/](raw/historical/lab/). New current-code results appear below.

Confirmed seed-reproduced engine/store defects: **0**.

All 200,000 seeds (0–199999) passed with every fault type exercised.

That is the original campaign. Delta 1 adds shared-queue runs, overlapping background
transactions, Gate pending/error/deadline responses, and API protocol probes. Its
bounded results are in [the concurrent campaign](lab-delta1-2026-09-28.md); the
separate real-clock PostgreSQL results are in [the PG campaign](lab-pg.md).
All 148,554 seeds in the expanded memstore campaign passed in 19m50.041s, with no
known exclusions or new seed-reproduced engine/store defect.
The [mutation campaign](lab-mutation.md) introduces production bugs only in isolated
scratch worktrees. Killed mutants establish fault-detection evidence; they are not
defects found in the unmodified engine.

The lab has no known-defect exclusions: `lab.KnownFailure` returns false for every
seed. `-lab.known=false` therefore runs the same assertions as the default. If a real
defect is added later, record its exact reproducing seed and symptom here, retain a
minimal regression in the owning package, and recognize only that documented failure.
A seed alone must never suppress unrelated errors.

No engine or store fix was made in this campaign. See [the campaign report](lab-2026-09-28.md)
for measured coverage and [the fault model](lab-l2.md) for scope and limits.

## Harness findings corrected during development

These are not counted as Capstan defects:

- Replay cleanup could run workflow defers concurrently, emit commands while stopping,
  or let a caught JSON-marshaler panic evade a fatal marker error. L1 regression tests
  now enforce sequential cleanup, stopped-context guards, and fatal marker failures.
- A worker kill originally retained the other poller's volatile state. It now drops
  both workflow and activity state.
- Unconsumed injected signals could disappear without a failed assertion. Delivery is
  now checked by request ID, name and input. An engine mutation dropping signals is
  rejected at seed 0.
- Fault-free and faulted executions could agree on a wrong terminal result or retained
  work. Explicit scenario outcomes and closed-run cleanup checks now reject that;
  a task-deletion mutation is rejected at seed 437.
- A crash after the Gate call can leave a valid minute-long poll lease. The scheduler
  now jumps to its persisted deadline when idle; seed 14 no longer exhausts the
  harness's step budget.
- CLI invalid-scenario classification, lost diagnostics after report-write failure,
  and TestLab's maximum-seed overflow/zero-count false success were corrected.
- Mutation probes now check lifecycle obligations that terminal results alone missed:
  heartbeat renewal, retry deadlines and attempts, pre-start cancellation, failed
  workflow lease release, closed inbox cleanup, and signal order. These changes catch
  actual patched production behavior, not just synthetic snapshots.
- The real-clock PG timer oracle originally assumed that timer creation and event
  recording read the same instant. The corrected oracle bounds the persisted deadline
  by the workflow acknowledgement's observed start/end times plus the requested
  duration, and tracks that deadline through delivery and cleanup.
- PG result comparison now independently rejects retained terminal work, pending
  approvals, live workflow leases, and a non-nil failure on a successful result.

## Earlier unseeded follow-up, resolved upstream

While checking the unarmed wrapper against the shared store conformance suite,
`Time/ZeroTimesRoundTrip` and `Time/UTCPreserved` reported that memstore preserved a
caller's non-UTC location, whereas the shared suite expects UTC. The lab's clocks use
UTC, so this observation is **not** a seed-reproduced P1–P4 defect and is excluded from
the headline count and known list.

Evidence is preserved in [archived l2-fault-red-behavior.log](raw/historical/lab/l2-fault-red-behavior.log). Main now normalizes cloned
run/task/timer/approval/audit times to UTC and runs the full shared memstore conformance
suite (`internal/store/memstore/conformance_test.go`). Delta 1 received those changes
through its initial merge of main. This campaign did not alter the store or hide tests
behind a skip.

<!-- final-numbers:start -->
## Final code (904cb6c)

Measured on 2026-09-28. These numbers are recomputed by
[verify-numbers.py](raw/verify-numbers.py) from the committed observations.

No new seed-reproduced engine/store defect appeared in the 200,000-seed
current memory campaign or the 500-seed PostgreSQL campaign. Known-failure
exclusions remain zero. This does not erase defects independently found by the
[audit](audit-2026-09-28.md), and planted mutation kills are not production defects.
See [per-seed memory data](raw/904cb6c/lab-seeds.jsonl.gz) and
[PostgreSQL data](raw/904cb6c/pg.log).
<!-- final-numbers:end -->
