# Codex lanes on Capstan

`codexLanes({ brief, repo, lanes: [{ name, branch, brief, effort }] })` creates one Git
worktree per lane, runs the installed `codex-lane`, collects its final report path and
commit, then waits for a real `human()` decision. The options are each lane's name and
`none`. One approved lane is merged; `none`, denial, or expiry leaves the target alone.

This is **write-capable live work** on a trusted local repository. Codex uses the local
CLI's existing login/profile and can execute shell commands. Use a throwaway repository
for the proof. The example does not put Codex usage in Capstan's model ledger: these
are external CLI activities, not calls through `model()`.

The target checkout must stay exclusively owned by this run until its merge finishes:
do not check out another branch, edit it, or run another merger concurrently. The
checkout must start clean on a named branch, and all lane branches must be new. Merging
uses the exact commit collected with the report, checks that the target is still the
original branch/base, and uses `git merge --ff-only`. A moved target requires a fresh
review/run. A retry after a successful merge sees the approved commit already reachable
and returns without repeating the merge.

## Run

Prerequisites: Node 26, `cd sdk && npm ci`, Git, and `codex-lane` on PATH. Start a Capstan
server separately. Put its address and API key in the worker environment; credentials
are never workflow input. `CAPSTAN_*`, `AWS_*`, and provider/GitHub token variables are
removed from the Codex subprocess environment.

```sh
export PATH="$HOME/.local/bin:$PATH"
export CAPSTAN_ADDRESS=http://127.0.0.1:7501
# Load CAPSTAN_API_KEY from a private local file/secret store.
npm --prefix examples/codex-lanes run worker
```

From another terminal using the same server/key:

```sh
node sdk/bin/capstan.mjs start codexLanes my-lanes --queue codex-lanes --input \
  '{"brief":"Implement and test these independent changes; commit locally.","repo":"/absolute/path/to/repo","lanes":[{"name":"greet","branch":"demo/greet","brief":"Add greet.sh and test-greet.sh","effort":"xhigh"},{"name":"count","branch":"demo/count","brief":"Add count.sh and test-count.sh","effort":"xhigh"}]}'
node sdk/bin/capstan.mjs history my-lanes
node sdk/bin/capstan.mjs approve my-lanes APPROVAL_ID --choice greet
# Or: approve my-lanes APPROVAL_ID --choice none
```

Inspect both reports before approving. A nonempty final message plus a clean committed
change is required; the CLI's `finished` status alone does not establish success, and
the example cannot judge a report's quality. The recorded commit is what approval
authorizes, even if someone later moves that lane branch.

`CODEX_LANES_QUEUE` changes the queue and `CODEX_LANES_IDENTITY` names the worker. There
are eight activity slots. Worktree creation and merge allow two minutes per attempt;
each lane allows 60 minutes per attempt and two hours across retries. Lane activities
poll status every two seconds and heartbeat with a 30-second timeout. Retry backoff
starts at one second and caps at 30 seconds. For large jobs, set the run timeout through
the SDK client to cover queued lanes and the human wait.

## Crash and retry boundary

All filesystem/process work lives in activities. Workflow code only schedules work and
waits on `human()`, so sandbox replay performs no shell or network operations.

The worker stores manifests/briefs under the repository's Git common directory at
`capstan-codex-lanes/<hash(run ID, lane name)>/`. Worktrees live there too. The real CLI
stores its own state and `last-message-N.md` reports under `~/.codex/lanes/`.

1. Each lane gets a stable `cw-<name>-<hash(activity idempotency key)>` name. Attempts do
   not enter the hash. Manifest input must match on every retry.
2. A detached helper uses exclusive `mkdir` to claim the launch. The claim is permanent;
   only its winning process may invoke `codex-lane run`. Overlapping deliveries can
   start helpers, but losing helpers exit before calling the CLI.
3. A worker can die before spawning the helper: a retry can still spawn one. Once the
   helper claims launch, it and the CLI live in another process group and survive a
   worker SIGKILL. Retries read `codex-lane status` and verify its worktree before attaching.
4. A helper/host failure after claiming but before the CLI publishes its lane is an
   ambiguous launch. After 30 seconds without usable state, the activity fails closed
   with `LaneLaunchUncertain`. It never clears the claim or guesses that a second launch
   is safe. Inspect local logs; establish that no lane is alive before using a new run.

This guarantee requires the same Mac and persistent local Git/CLI state. Do not delete
the claims or CLI state while a run can retry, and do not start/resume those reserved
lane names outside the workflow. The guarantee covers one invocation of `codex-lane
run`; the installed CLI can make its own sequential capacity retries.

Shutting down/cancelling an activity stops its attachment, **not the independently
running lane**. That distinction lets another worker attach after a lost lease. To
abandon a job, cancel the Capstan run and stop its exact lane names with `codex-lane
stop NAME`, checking that its descendants have exited. After exporting evidence,
remove only its worktrees with `git worktree remove`, its CLI directories, and its
manifests. Never use a broad `codex-lane stop`/process-kill pattern. The demo script
performs this scoped cleanup automatically, including its server/worker and database.

## Verify and record

```sh
npm --prefix examples/codex-lanes test
npm --prefix examples/codex-lanes run typecheck
PATH="$HOME/.local/bin:$PATH" GOTOOLCHAIN=go1.26.4 make verify
asciinema rec --output-format asciicast-v2 --window-size 150x40 --capture-env TERM \
  --return --command scripts/demo-codex-lanes.sh docs/evidence/codex-lanes-local.cast
```

The unit suite puts a fake `codex-lane` on PATH but uses real child processes and Git.
It covers a completed lane, process SIGKILL and reattachment, an unacknowledged launch,
overlapping deliveries/preparation, partial startup, ambiguous claims, report identity,
merge retries, and choices of one lane or none. Sandbox tests replay both choices.

The real proof uses `.lane/demo-repo`, port 7501, and a fresh `capstan_workload_*`
database on the existing PostgreSQL at 127.0.0.1:55432. It kills only the worker while
both real lanes run, starts another worker, verifies attempt 2 attaches to the same
lanes, approves `greet` through the real CLI, compares the exact history prefix, and
cleans up. It refuses to overwrite an existing demo repo or recording. See
[local evidence](../../docs/evidence/codex-lanes-local.md).
