# Waiting without a worker

## Historical recording

The original signal recording is retained. Final-code signal and human-approval checks are linked below.

Idle signal portion verified 2026-09-28: [recording](demo-idle-wait.cast).

```sh
scripts/demo-idle-wait.sh
asciinema rec --headless --output-format asciicast-v2 --window-size 120x36 --capture-env TERM --return --command scripts/demo-idle-wait.sh docs/evidence/demo-idle-wait.cast
asciinema play docs/evidence/demo-idle-wait.cast
```

At the 2026-09-28 check, `git show main:sdk/src/workflow/agent.ts` still contained
`human()`'s unimplemented stub. As authorized in the evidence brief, this demo uses
`nextSignal(defineSignal("decision"))` and the CLI's `signal` command. It does **not**
claim to have exercised `human()`, `capstan approve`, or a multi-day clock jump.

The script starts a real server on port 7304, a fresh PostgreSQL database, and a real
SDK worker. Once the workflow is waiting with an empty task table, it stops every
worker belonging to this demo. It checks the exited worker PIDs with `ps` and runs:

```sql
select count(*) as tasks,
       count(*) filter (where leased_until is not null) as leased_tasks
from task;
```

Both counts must be zero before and after five real seconds. The run remains
RUNNING, persisted in the database. It then submits `{"approved":true}` via the real
CLI while no worker exists, shows the resulting unleased workflow task, starts a
replacement worker, and requires completion with that same result.

“Zero workers” is scoped to this isolated demo; other lanes are not stopped. There
is no run-owned task lease while idle. The shared server and its database pool still
exist, and history occupies storage. The recording preserves wall-clock timing and
contains the actual SQL/CLI output. Cleanup removes all demo processes and its
database. Logs remain in `.lane/idle-*`.

<!-- final-numbers:start -->
## Final code (904cb6c)

Measured on 2026-09-28. These numbers are recomputed by
[verify-numbers.py](raw/verify-numbers.py) from the committed observations.

The signal demo again observed zero demo workers and zero task leases across
5 real seconds. It persisted the signal before starting a replacement worker,
then completed with `{approved:true}`. [Raw SQL, process and CLI output](raw/904cb6c/demo-idle-wait.log).

The separate [seven-day human-approval test](raw/904cb6c/human-seven-days.log)
uses the engine Clock over real PostgreSQL RPCs. It verifies a 7-day clock
advance with zero tasks or leases, then approval and completion. Neither test
is a live multi-day Gate deployment.
<!-- final-numbers:end -->
