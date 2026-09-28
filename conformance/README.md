# Replay conformance fixtures

These fixtures pin what "re-running a workflow produces the same steps in the same order"
means, in a form both implementations must satisfy:

- the TypeScript SDK (`sdk/`), which runs real workflows, and
- the Go reference worker in the fault lab (`internal/lab/labworker`), which drives the
  engine in thousands of seeded scenarios.

If the two ever disagree about a fixture, one of them is wrong, and the fixture decides.

## Format

Each file in `fixtures/` is JSON:

```json
{
  "name": "one activity, then complete",
  "workflow": "singleActivity",
  "description": "why this case exists",
  "history": [ /* capstan.v1.HistoryEvent in protojson, ending with the current TaskStarted */ ],
  "expect": { "commands": [ /* capstan.v1.Command in protojson */ ] }
}
```

`expect` is exactly one of:

- `{ "commands": [...] }` — replay of every earlier activation must match history, and the
  final activation (the one whose `TaskStarted` ends the history) must return exactly these
  commands, in this order. An empty list means the workflow is waiting on something.
- `{ "mismatch": { "eventId": N } }` — replay must fail with a history mismatch that names
  event `N`: the first recorded command event the code did not reproduce.

`workflow` names an export of `workflows.ts` (TypeScript) and a scenario of the same name in
`internal/lab/scenarios` (Go). The two implementations must behave identically.

## What "matches" means

Replay walks the history in activations. An activation starts at a `TaskStarted` event:

1. Resolve, in history order, every external event since the previous activation's command
   events: `ActivityCompleted/Failed/TimedOut/Cancelled`, `TimerFired`, `SignalReceived`,
   `ApprovalResolved`, `RunCancelRequested`. (`TaskFailed`, `TaskTimedOut`, `RunBlocked`,
   `RunResumed`, `TaskScheduled` carry no workflow-visible result and are skipped.)
2. Set `now()` to this `TaskStarted` event's time.
3. Run workflow code until nothing more can happen without a new event, draining every
   pending promise continuation, and collect the commands it emits.
4. If this activation has a `TaskCompleted` in history, compare the collected commands, in
   order, with the command events that immediately follow it. Otherwise these are the new
   commands returned to the server.

Two commands match when all of these are equal:

| Command | Compared fields |
| --- | --- |
| `ScheduleActivity` ↔ `ActivityScheduled` | `seq`, `activity_type` |
| `RequestActivityCancel` ↔ `ActivityCancelRequested` | `seq` |
| `StartTimer` ↔ `TimerStarted` | `seq` |
| `CancelTimer` ↔ `TimerCancelled` | `seq` |
| `RecordMarker` ↔ `MarkerRecorded` | `seq`, `name`, `marker_id` |
| `RequestApproval` ↔ `ApprovalRequested` | `seq`, `approval_id`, `source` |
| `CompleteRun` ↔ `RunCompleted` | type only |
| `FailRun` ↔ `RunFailed` | type only |
| `CancelRun` ↔ `RunCancelled` | type only |
| `ContinueAsNew` ↔ `RunContinuedAsNew` | type only |

Inputs, results, timeouts, retry policies and timer durations are **not** compared: changing
them in code is allowed and takes effect for steps not yet recorded. A different command type
at a position, a different identifying field, a missing command, or an extra command in an
activation that already completed is a mismatch.

Payload values in `expect.commands` are compared by decoded JSON value, not by bytes, so
key order and whitespace differences between languages do not matter.

## Adding a fixture

Generate fixtures with the helper the SDK lane provides (`sdk/test/fixtures/build.ts`)
rather than writing base64 by hand, give the file the next number, add the workflow to
`workflows.ts` and to the Go scenarios, and make both test suites pass.
