# Fault-lab mutation score

Caught **25/26 valid mutants (96.15%)**; equivalent survivors: **1**.

Excluding explicitly justified equivalent survivors: **25/25 (100.00%)**.

Production revision: `59065273ba73f66109eea66779383eb58286f9a6`. Lab snapshot SHA-256: `795120ef8cd87a27269b4f5565310f4359ef629da7987a38b3883ddd40804671`.
Scratch snapshot commit: `58d93317a028c3434f4e797c28873c9dd94f4a63`. Started: 2026-09-29T02:02:24Z.

Every mutant runs TestLab with 2000 seeds, stopping at its first failure. The unmutated
baseline must pass all seeds first. Builds, infrastructure failures and timeouts are
reported separately and do not count as kills. Production sources are changed only
in an isolated temporary worktree beneath `.lane/`, removed after the run.

```sh
PATH=$HOME/.local/bin:$PATH GOTOOLCHAIN=go1.26.4 go run ./cmd/capstan-lab mutate -catalogue 'internal/lab/mutants/*.patch' -seeds 2000 -timeout 2m0s -logs .lane/polish-review-mutations
```

| ID | Mutation | Result | First seed | Property or check |
| --- | --- | --- | ---: | --- |
| M001 | Timer delivery retains timer row | caught | 0 | baseline pipeline: P3: run "lab": timer row 2 is duplicated or retained after settling |
| M002 | Inbox flush precedes command events | caught | 2 | protocol cancel-before-start: cancel-before-start emitted 0 cancellation events |
| M003 | Workflow token omits started event comparison | equivalent | — | The remaining run.LastEventID != tok.StartedEventId guard rejects every differing started ID on engine-reachable states: PollWorkflowTask writes TaskStarted last and stores that same ID in task.StartedEventID; while InFlight, external events only enter the inbox; complete, fail, and timeout clear or replace the task. This mutant removes a redundant guard only, assuming state was produced through the engine. |
| M004 | Activity retry does not increment attempt | caught | 6 | protocol retry-attempt: activity retry attempt = 1 after 1 |
| M005 | Close retains buffered events | caught | 8 | protocol closed-inbox: closed run retains 1 buffered events |
| M006 | In-flight signal is acknowledged and dropped | caught | 0 | scenario pipeline: signal peer/inflight-40: got 0 deliveries, want one |
| M007 | Workflow task failure retains InFlight | caught | 3 | protocol failed-workflow-release: failed workflow task still owns InFlight |
| M008 | Last event projection increments one too far | caught | 0 | baseline pipeline: store: conflict |
| M009 | Heartbeat does not extend deadline | caught | 0 | protocol heartbeat-renewal: renewed heartbeat task expired early: processed 1 |
| M010 | Retry is queued past schedule-to-close | caught | 1 | protocol retry-deadline: activity retry remains queued beyond schedule-to-close: attempt 2 |
| M011 | Cancel-before-start retains deliverable activity | caught | 2 | protocol cancel-before-start: cancelled unstarted activity was delivered |
| M012 | Continuation uses predecessor input | caught | 7 | baseline continuation: did not finish in 1000 steps |
| M013 | Human approval may resolve twice | caught | 4 | protocol approval-once: duplicate approval resolution must be rejected: <nil> |
| M014 | Timer deletion commits before delivery | caught | 0 | baseline pipeline: P3: run "lab": 1 timer(s) lost: no row, terminal event, or sufficient inbox entries |
| M015 | ClaimTask ignores an active lease | caught | 0 | baseline pipeline: worker replay failed: invalid history after TaskStarted event 3 |
| M016 | Expired workflow completion is accepted | caught | 0 | scenario pipeline: late workflow completion accepted: <nil> |
| M017 | Expired activity completion is accepted | caught | 2 | scenario signal: late activity completion accepted: <nil> |
| M018 | Transaction snapshot aliases mutable tasks | caught | 7 | scenario continuation: did not finish in 1000 steps |
| M019 | Inbox push prepends instead of appending | caught | 5 | protocol signal-order: accepted signal order[0] = "third" |
| M020 | Timer selection includes a future millisecond | caught | 0 | scenario pipeline: P3: run "lab": timer 2 fired before its deadline 2000-01-01T00:00:00.081Z |
| M021 | Timer row references the completing task event | caught | 0 | baseline pipeline: P3: run "lab": timer row 2 references an unknown start |
| M022 | Effect idempotency key omits run identity | caught | 0 | baseline pipeline: root peer: scenario pipeline-peer: unexpected result content_type:"application/json" data:"[1,\"done\"]" or failure <nil> |
| M023 | Gate decision arriving after deadline wins | caught | 6 | baseline gate-late: root lab: scenario gate-late: unexpected result content_type:"application/json"  data:"{\"choice\":\"\",\"note\":\"\",\"outcome\":\"approved\",\"resolver\":\"lab-gate\"}" or failure <nil> |
| M024 | Gate pending status is treated as approval | caught | 5 | baseline gate-errors: root lab: scenario gate-errors: unexpected result content_type:"application/json"  data:"{\"choice\":\"\",\"note\":\"\",\"outcome\":\"approved\",\"resolver\":\"\"}" or failure <nil> |
| M025 | Gate error becomes a final denial | caught | 5 | baseline gate-errors: root lab: scenario gate-errors: unexpected result content_type:"application/json" data:"{\"choice\":\"\",\"note\":\"\",\"outcome\":\"denied\",\"resolver\":\"gate-error\"}" or failure <nil> |
| M026 | RunTasks leaks other runs task rows | caught | 0 | baseline pipeline: did not finish in 1000 steps |

## Survivors and invalid runs

- **M003 (equivalent):** The remaining run.LastEventID != tok.StartedEventId guard rejects every differing started ID on engine-reachable states: PollWorkflowTask writes TaskStarted last and stores that same ID in task.StartedEventID; while InFlight, external events only enter the inbox; complete, fail, and timeout clear or replace the task. This mutant removes a redundant guard only, assuming state was produced through the engine.

## Evidence

Full per-mutant output, the baseline, patch SHA-256 values and machine-readable
results are stored in `.lane/polish-review-mutations`. The patch catalogue describes the intended bug;
the kill table records the observed check. A survived mutant without a proved
equivalence remains a lab coverage gap. The score concerns this catalogue and
these seeds; it is not a proof that production contains no defects.
