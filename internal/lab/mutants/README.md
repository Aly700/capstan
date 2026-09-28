# Lab mutation catalogue

Each patch introduces one plausible engine or memstore bug. Apply exactly one to
an isolated scratch checkout, run the unmodified lab, then discard that checkout's
production changes. The catalogue intentionally includes changes to transaction
boundaries, ordering, lifecycle fences, deadlines, retries, and cross-run isolation;
it is not a set of arbitrary crashes or always-failing return values.

`catalogue.json` is an array with `id`, `name`, `patch`, `description`, and
`expected_check`. Patch paths are relative to this directory. `expected_check`
identifies the behavior the lab should observe; the mutation report records the
actual first failing seed and assertion. An optional `equivalent_reason` must be
supported by an invariant argument, not by a passing campaign.

The patches were authored against `eb2659e`. They change only production files in
`internal/engine/` and `internal/store/memstore/`, and every isolated patch compiled
with Go 1.26.4 before the campaign. No patch changes tests or lab assertions.

M003 is a deliberate equivalent control. The direct comparison between a workflow
token's started-event ID and its task's started-event ID is redundant for states
reachable through this engine. Polling appends TaskStarted, records its ID in the
task, and leaves that event as the run's last history event. While the task is in
flight, external events go into the inbox. Successful completion, failure, timeout,
or close removes or replaces that task. The remaining comparison between the run's
last-event ID and the token's started-event ID therefore enforces the same fence.
This argument does not cover manually corrupted database rows or combining M003
with another mutant.

Continue-as-new losing buffered events is not included as a bug: the current
closing-command contract deliberately discards the predecessor's inbox. M012
instead loses the command's new input, which violates the continuation contract.
