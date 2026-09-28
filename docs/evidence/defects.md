# Fault-lab defect register

Confirmed seed-reproduced engine/store defects: **0**.

All 200,000 seeds (0–199999) passed with every fault type exercised.

The lab has no known-defect exclusions: `lab.KnownFailure` returns false for every
seed. `-lab.known=false` therefore runs the same assertions as the default. If a real
defect is added later, record its exact reproducing seed and symptom here, retain a
minimal regression in the owning package, and recognize only that documented failure.
A seed alone must never suppress unrelated errors.

No engine or store fix was made by this lane. See [the campaign report](lab-2026-09-28.md)
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

## Unseeded follow-up for the owning lane

While checking the unarmed wrapper against the shared store conformance suite,
`Time/ZeroTimesRoundTrip` and `Time/UTCPreserved` reported that memstore preserved a
caller's non-UTC location, whereas the shared suite expects UTC. The lab's clocks use
UTC, so this observation is **not** a seed-reproduced P1–P4 defect and is excluded from
the headline count and known list.

Evidence is preserved in `.lane/l2-fault-red-behavior.log`. Inspect
`internal/store/memstore/memstore.go:198` (run cloning) against
`internal/store/storetest/time.go:15` (non-UTC input and UTC round-trip expectation).
The lead should route full shared memstore-conformance coverage and time normalization
to the owning lane. No store changes or tests were hidden behind a skip in this lane.
