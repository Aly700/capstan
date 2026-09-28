# PostgreSQL fault-lab campaign

## Historical PostgreSQL campaigns

The [original successful run](raw/historical/lab/delta-pg-500-uncontended.log), [earlier run](raw/historical/lab/delta-pg-500.log), [time-limited failure](raw/historical/lab/delta-pg-500-final.log) and [development probes](raw/historical/lab/) are archived. Their old elapsed times describe those runs, not final-code performance.


A small real-clock campaign is practical on the shared PostgreSQL: **500 seeds
passed in 5 minutes 22.945 seconds** on the final sources, with zero reproduced
engine or store defects. It is a separate, deliberately narrower driver than
the memstore campaign. It uses the real engine, `pgstore`, `engine.SystemClock`
and actual `time.Sleep`; no network operation runs inside a `testing/synctest`
bubble.

Reproduce the opt-in campaign:

```sh
GOTOOLCHAIN=go1.26.4 PATH="$HOME/.local/bin:$PATH" go test -count=1 ./internal/lab -run '^TestPostgresCampaign$' -lab.pg-seeds 500 -timeout 11m -v
```

`-lab.pg-seeds` defaults to zero, so the optional campaign does not run during the
normal lab gate. Selecting it requires a working PostgreSQL and fails if the
database is unavailable. The ordinary mandatory pgstore tests are unchanged.
The driver has a ten-minute context; the eleven-minute test timeout allows
database cleanup after that bound is reached.
The test uses `internal/testpg` to create two isolated databases on the existing
shared server and removes only those databases at cleanup. Each seed keeps its
history in its campaign database until cleanup; the lab never resets the shared
server or modifies a pre-existing database.

## Scope

Every seed starts two simultaneous runs on the same task queue. Three worker
identities share that queue. Each run performs an effect, waits on a durable
1–3 ms timer, then joins two more effects. Worker and background actions are
selected from a seeded schedule. This PostgreSQL driver executes one actor step
at a time, so its SQL transactions do not overlap. A claimed task is replayed and
acknowledged in one actor step; workflow and activity leases are 500 ms. This
prevents ordinary database latency from turning an otherwise healthy worker into
a permanently stale worker.

Each seed runs a fault-free PostgreSQL baseline and a faulted PostgreSQL variant
with identical run IDs. The faulted variant:

- submits each successful workflow and activity acknowledgement again and
  requires a stale-token error;
- interrupts one activity acknowledgement after a seeded store operation,
  alternating a returned database error and a server-crash panic by seed;
- verifies that the failed transaction changed no persisted state, reconstructs
  the engine after a crash, and retries the acknowledgement;
- reapplies that activity's effect through the same idempotency key.

P1 checks the sink's applied effects. P2 compares terminal results with the
fault-free PostgreSQL execution and verifies prefix-extension after each step.
P4 checks contiguous history and `last_event_id`. Explicit result checks also
require both runs to return their three expected effect values, with no failure,
in-flight task, workflow task ID, retained task, timer, inbox entry, or pending
approval after closure.

P3 checks every timer's identity, stable deadline, delivery time, duplicate
delivery, loss, and cleanup. Its real-clock deadline oracle differs from the
frozen-clock oracle: before acknowledging a workflow task, the driver records
the call's start time; after return, it records the end time. A new timer's
persisted deadline must lie between those two times plus its requested duration.
That validated deadline is retained for later checks, even after its row is
deleted. `CheckTimersWithDeadlines` requires an observed deadline for every
`TimerStarted`; it does not alter recorded event timestamps.

This campaign does **not** cover worker-process kills between commands, long
Gate leases, Gate responses, signals, continuation, cancellation, or arbitrary
overlap between PostgreSQL transactions. Those remain covered by other suites or
the broader memstore campaign. The seed selects actor actions and faults; actual
network timing can change when a timer becomes eligible. A real-clock failure
therefore needs an independently repeatable regression before it counts as a
seed-reproduced defect.

## Feasibility observations

The original memstore driver was first tested against `pgstore` through a
temporary Go source overlay, retaining its real 20–25 ms leases and per-step
snapshots. Seed 0 failed to finish after 1,000 steps in 3.772 seconds even with no
injected faults. Repeated network round trips consumed the short leases. That
probe is preserved in [archived delta-pg-unscaled-probe.log](raw/historical/lab/delta-pg-unscaled-probe.log).

An initial real-clock P3 assertion also exposed a lab assumption: it required
`DueAt == TimerStarted.Time + FireAfter`. The normative plan permits separate
`Clock.Now` reads when appending an event and computing a deadline. Their values
can differ under a real clock, so this was an oracle correction, not an engine
defect. The clock-window checks above retain a bounded deadline check and reject
missing deadlines, deadline changes, and early fires. Red/green evidence is in
[archived delta-pg-timer-red.log](raw/historical/lab/delta-pg-timer-red.log), [archived delta-pg-deadline-red.log](raw/historical/lab/delta-pg-deadline-red.log), and
[archived delta-pg-green.log](raw/historical/lab/delta-pg-green.log).

The first ten seeds passed in 6.313 seconds, executing 975 actor steps, rejecting
149 duplicate acknowledgements, and recovering five database errors and five
server crashes. The first 500-seed campaign passed in 9 minutes 12.647 seconds,
executing 53,835 actor steps, rejecting 7,589 duplicate acknowledgements, and
recovering 250 database errors and 250 server crashes. That run preceded the
additional terminal-cleanup and nil-failure checks requested in review; it is
preserved in [archived delta-pg-500.log](raw/historical/lab/delta-pg-500.log).

The first final-source attempt ran concurrently with the eight-worker memstore
campaign and other verification work. Seeds 0–376 passed every check; seed 377's
baseline reached the driver's ten-minute context before finishing. The command
failed in 600.208 seconds with `context deadline exceeded`, then cleaned up its
temporary databases. This is a measured campaign time bound, not an engine or
store defect. Its log is [archived delta-pg-500-final.log](raw/historical/lab/delta-pg-500-final.log).

After the root memstore campaign and verification jobs finished, the unchanged
final sources passed all 500 seeds (0–499):

```text
validated 500 PostgreSQL seeds; two shared-queue runs/seed; steps=53835 duplicate_acks=7589 database_errors=250 server_crashes=250 elapsed=5m22.944687959s
--- PASS: TestPostgresCampaign (323.05s)
PASS
ok   github.com/Aly700/capstan/internal/lab   323.388s
```

That is 2,000 completed run executions across baseline and faulted variants,
with every final cleanup check enabled. The log is
[archived delta-pg-500-uncontended.log](raw/historical/lab/delta-pg-500-uncontended.log); despite its filename, the host was still
shared with other lanes. Only this lane's CPU-heavy jobs had ended. The databases
were removed by normal test cleanup. No lease duration, scheduler behavior,
source assertion, or property was changed between the bounded final-source
attempt and this successful rerun.

<!-- final-numbers:start -->
## Final code (904cb6c)

Measured on 2026-09-28. These numbers are recomputed by
[verify-numbers.py](raw/verify-numbers.py) from the committed observations.

The real-clock PostgreSQL campaign passed **500 seeds** in
**5m34.384440667s**, with **53,835 actor steps**,
**7,589 rejected duplicate acknowledgements**,
**250 injected database errors** and
**250 injected server crashes**.
The two variants and two roots per seed give **2,000 completed run executions**.

The [raw command output](raw/904cb6c/pg.log) includes every seed's counters.
The verifier sums them and checks the emitted aggregate. The overlay only prints
those counters; the real clock, 500 ms leases, fault injection, transaction
assertions and cleanup are unchanged. Both campaign databases were created and
dropped by the test. The shared PostgreSQL was left running.

Reproduce: `python3 docs/evidence/raw/reproduce.py pg`. The narrower scope and
real-clock limitations described above still apply.
<!-- final-numbers:end -->
