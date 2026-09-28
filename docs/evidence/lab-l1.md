# Go reference worker

## Historical reference-worker report

The fixture inventory can be read from the committed corpus; the final gate below checks the current code. Historical gate output is archived in [raw/historical/lab/](raw/historical/lab/).

The L1 worker reconstructs a run from history on every task. `Replay` starts the
scenario at its first line, feeds successful activations in order, compares their
commands, and returns only the current activation's new commands. Failed and
timed-out activations execute no scenario code and do not advance workflow time.

Each replay owns a short-lived set of coroutines. A permit/yield scheduler runs
one at a time, in explicit queue order. `All` returns values in branch order;
`Race` returns the first branch settled by that scheduler. Every coroutine is
stopped and joined before `Replay` returns, including pending losing branches.
Cleanup stops and joins coroutines one at a time, so ordinary Go deferred code
also runs in a stable order. The worker stores configuration and the scenario
registry only. It retains no run state or coroutine stack between tasks.

Scenario code must keep state local and use the workflow context for activities,
timers, signals, approvals, concurrency, time, randomness, and markers. Signal
handlers are synchronous. The Go twin of the async-handler fixture represents
its final drained value; the worker does not emulate JavaScript microtasks.
Workflow time preserves fractional milliseconds from recorded timestamps.

## Driving one task

The driver consumes the existing engine methods through a narrow interface:

```go
worker := labworker.NewWorker(engine, "lab", "worker-1", "lab-v1", scenarios.Registry())
task, found, err := worker.Poll(ctx)
if err != nil { return err }
if !found { return nil }
result := worker.Execute(task)
return worker.Respond(ctx, task, result)
```

The lab can retain or discard the task and result between these calls, recreate
the worker, replay the same history, or submit an acknowledgement twice. No phase
performs hidden retries. Scenario errors become `FailRun`; replay mismatches
become `HISTORY_MISMATCH`, and other replay errors become `SDK_ERROR`.

`CancelActivity(seq)` is a lab-only API for D12: it emits a reference to an
unsettled activity without allocating another sequence or settling its result.
An activity result that wins the cancellation race is still delivered.

## Conformance and verification

| Corpus | Count | Go result |
| --- | ---: | --- |
| Shared command-output fixtures | 39 | Pass |
| Shared mismatch-position fixtures | 14 | Pass |
| TS-only fixture 054 | 1 | Skipped under D20 |
| TypeScript workflow exports with Go twins | 29 | All registered |

The fixture loader resolves `../../../conformance/fixtures` from its source path,
with a repository lookup for `-trimpath`. It parses history and commands as
protobuf JSON. All command fields are compared, with only JSON payload encoding
normalized so whitespace and object key order do not affect equality.

Additional tests cover worker restarts from history, ordered fan-out, unknown and
duplicate results, D12 cancellation, uncatchable side-effect failures, recorded
microseconds, coroutine disposal, and separately controlled poll/replay/submit
phases. A command-matcher table covers every command type and identifying field,
including fields deliberately excluded from matching.

```sh
GOTOOLCHAIN=go1.26.4 go test -race ./internal/lab/...
GOTOOLCHAIN=go1.26.4 go test -trimpath ./internal/lab/...
GOTOOLCHAIN=go1.26.4 make verify
```

This evidence covers the reference worker. Seed campaigns and engine defect
counts belong to L2, which requires the engine lane to be merged first.

<!-- final-numbers:start -->
## Final code (904cb6c)

Measured on 2026-09-28. These numbers are recomputed by
[verify-numbers.py](raw/verify-numbers.py) from the committed observations.

The checked corpus contains 39 shared command fixtures and 14 shared mismatch
fixtures. The Go worker covers those 53 fixtures; fixture 054 is TypeScript-only
under D20. All 29 TypeScript workflow exports have Go twins. The verifier derives
these counts from the committed corpus and registration code.

The final [merge gate](raw/904cb6c/verify.log) exercises the reference worker and
the SDK. The [per-seed campaign](raw/904cb6c/lab-seeds.jsonl.gz) exercises the same
reference worker across 200,000 seeds. Fixture coverage does not imply that the
Go worker models V8 microtask scheduling.
<!-- final-numbers:end -->
