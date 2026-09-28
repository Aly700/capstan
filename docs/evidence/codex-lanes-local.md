# Codex lanes survive a worker crash

Verified on 2026-09-28 with two **real** Codex lanes at `--effort xhigh`, a real Capstan
server/SDK worker, and PostgreSQL. [Recording](codex-lanes-local.cast) ·
[complete before/after histories, reports and timings](codex-lanes-local-history.json) ·
[example](../../examples/codex-lanes/README.md) · [proof script](../../scripts/demo-codex-lanes.mjs).

Run `workload-local-1790615773509` completed with only the `greet` lane merged. The
worker was killed while both lanes ran; both lane runner PIDs survived, and a new worker
reattached on activity attempt 2. No AWS operations were part of this recording.

| Milestone (UTC) | Observed value |
| --- | --- |
| Proof started | 17:16:13.508 |
| Server / first worker | PID 77193 / PID 77696; port 7501 |
| Both lanes running; worker SIGKILL | 17:16:14.879 |
| Surviving greet / count runners | PID 78075 / PID 78072 |
| Replacement worker | PID 79846 |
| Both activities attached on attempt 2 | 17:16:45.857; **30.978 s** after kill |
| Greeting activity completed | 17:17:00.142 |
| Count activity completed | 17:17:08.269 |
| Approval resolved through `capstan approve --choice greet` | 17:17:09.548 |
| Run closed | 17:17:09.663 |
| Cleanup verified | 17:17:10.574; **57.066 s** total proof/cleanup |

The server PID stayed unchanged. This is the local **worker** crash proof; an ECS
server replacement is a separate Phase B proof and is not implied by these results.

## Work and approval

The isolated `.lane/demo-repo` started from commit
`e68c1a9eeb089f3e3da1fc938a3c80510de86fda`. Each task ran in a separate worktree and
committed locally:

| Lane | Work and verification | Commit |
| --- | --- | --- |
| greet | Executable `greet.sh` plus default/name assertions in `test-greet.sh`; test passed | `f426e49e3575a2202957c11701421c0e0e2d7c68` |
| count | Executable `count.sh` plus empty/three-line assertions in `test-count.sh`; test passed | `878da7f0bf5ed430e0078785938c28111131ec02` |

Each CLI runner recorded exactly one fresh launch. The example collected each
`~/.codex/lanes/cw-…/last-message-1.md`; the JSON artifact retains both report contents
after removing those temporary directories. Both tests were executed again by the
proof script, and the merged greeting printed `Hello, Ada!`.

`human()` created approval `bb09600a-2c39-4f00-8ddd-5a5216c1ca1e` with options `greet`,
`count`, `none`. While waiting, the database contained **zero tasks**, hence zero task
leases. Under the owner's instruction to approve one demo lane, the script invoked the
real CLI with resolver `workload-demo` and choice `greet`. It fast-forwarded the target
to the reported greeting commit and asserted that `count.sh` was absent from the target.
This was a real approval API call made by the authorized proof script, not a separate
interactive decision by another person.

## History and duplicate-launch checks

The **13** events captured before SIGKILL exactly match the first 13 of the final
**33** events. The script asserts equality, five activity completions, and attempt 2
on both lane completions. `capstan replay` of the final history returned `OK`.

The SHA-256 of the prefix, after sorting object keys and encoding compact JSON, is
`40816e29c136b8f9af886a9aaa27d76e7c23c19628ac5fd6175326da69f597b2`.
The corresponding full-history hash is
`ef76779cd9b30bfa96b44da8ce317fb1994a9b55f93db4b03f4eda6d2f19cec2`.

The duplicate-launch guarantee comes from a persistent exclusive claim, not a
check-then-start assumption about `codex-lane`. The CLI itself uses a non-atomic
directory check. The example launches it only from a detached helper that has won an
exclusive `mkdir`, with a name derived from the attempt-independent activity key.
The claim is never removed by a retry. A new worker can attach to running or finished
work; a missing lane after an ambiguous claim fails closed instead of starting again.
This requires preserving the same Mac's Git metadata and CLI lane state. It does not
claim recovery from deleted state or a host disk failure.

The fake-CLI suite also exercises process SIGKILL in the unacknowledged launch window,
two overlapping activity deliveries, overlapping worktree setup, partial startup,
an abandoned claim, a foreign lane identity, a branch moved after its report, a merge retry, and selection of
one lane or none. Sandbox replay tests exercise both approval choices.

## Reproduction and cleanup

Source at recording: `678b9bd6be1117a023c7be787e9330b1c40e5502` plus the workload
files committed with this evidence. Node 26.8.1, Go 1.26.4, PostgreSQL 16.15, asciinema
3.2.1. The cast retains wall-clock timing, with no idle limit or speed adjustment.

```sh
npm --prefix examples/codex-lanes test
npm --prefix examples/codex-lanes run typecheck
PATH="$HOME/.local/bin:$PATH" GOTOOLCHAIN=go1.26.4 make verify
asciinema rec --output-format asciicast-v2 --window-size 150x40 --capture-env TERM \
  --return --command scripts/demo-codex-lanes.sh docs/evidence/codex-lanes-local.cast
```

Choose a different recording filename when reproducing; asciinema will not overwrite
the committed cast. The script refuses an existing `.lane/demo-repo`. Its finally
block stops only its recorded child processes and exact owned lane names, verifies
their cwd, removes its worktrees and throwaway repo, and drops only its fresh database.
The final recorded verification was:

```text
3 child processes exited
2 owned demo lane directories removed
demo worktrees/repo removed
capstan_workload_76634_1790615773509 absent
shared PostgreSQL and other lanes untouched
```

The two lane directories removed were
`cw-greet-286f7971692904c38c1b6adc0dd158c6` and
`cw-count-cc46e70b92dcfa671f8a2b75d76c0957`. No `capstan-*` lane was stopped or edited.
Capstan's model ledger reports $0 because Codex runs outside `model()`; it does not
measure or establish the external Codex cost. AWS runtime and AWS cost for Phase A
were both zero.
