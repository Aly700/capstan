# Load measurements

Verified on 2026-09-28. This is the complete set of **11 measured runs**: 11,700
workflows and 58,500 persisted activity completions, with zero failed runs. The best
observed run was 78.7942 runs/s at concurrency 50. Throughput stopped improving in
the 50–100 range and did not scale at 200; 800 made both throughput and latency worse.
Repeated runs vary substantially, so all repeats are retained below.

## Machine and exact versions

Apple M5 Max, 18 logical CPUs, 51,539,607,552 bytes (48 GiB; marketed as 48 GB),
macOS 26.6.2 build 25G83. The server and Node workers ran natively on the Mac.
PostgreSQL ran in the existing Colima VM: 4 vCPUs, 8 GiB configured memory, Docker
reported 8,307,109,888 usable bytes. Other lanes shared this machine and PostgreSQL.

- Go 1.26.4 darwin/arm64; Connect Go 1.21.0; pgx 5.11.0; Go protobuf 1.36.12.
- PostgreSQL 16.15, aarch64 Alpine, GCC 15.2.0; psql client 16.13.
- Node 26.8.1; npm 11.19.0; TypeScript 7.0.2; tsx 4.23.15; esbuild 0.28.2.
- Connect JS/connect-node 2.2.0; protobuf JS 2.15.0; zod 4.6.5.
- Colima 0.10.3; Docker client 29.4.1, server 29.5.2.
- Base engine/store/SDK: `73804f2`; viewer: `48ad5b6`. Load sources are in this commit.

Versions were read with `scripts/evidence-versions.sh` (raw output `.lane/versions.log`).
No engine, store, SDK, schema, PostgreSQL settings or connection-pool settings were changed.

## Reproduce

Run from the worktree with the shared PostgreSQL already reachable at port 55432:

```sh
scripts/evidence-versions.sh
scripts/run-load.sh 50 1000 4
scripts/run-load.sh 200 1000 4
scripts/run-load.sh 800 1600 4
scripts/run-load.sh 50 1000 1
scripts/run-load.sh 50 1000 8
scripts/run-load.sh 25 1000 4
scripts/run-load.sh 10 500 4
scripts/run-load.sh 100 1000 4
scripts/run-load.sh 50 1000 4   # repeat
scripts/run-load.sh 200 1000 4  # repeat
scripts/run-load.sh 800 1600 4  # repeat after a sample observed another lane
python3 scripts/summarize-load.py .lane/load_c10_w4-* .lane/load_c25_w4-* .lane/load_c50_w4-* .lane/load_c100_w4-* .lane/load_c200_w4-* .lane/load_c800_w4-* .lane/load_c50_w1-* .lane/load_c50_w8-*
```

The arguments are concurrency, total runs, and real worker processes. Each invocation
builds `.lane/bin/capstan-server` and `.lane/bin/capstan-load` with Go 1.26.4, creates
its own `capstan_evidence_*` database, starts the server on 7301, and starts each worker
as `(cd sdk && node --import tsx ../examples/evidence/worker.ts)` in its own process
group. Every worker has 10 workflow slots and 10 activity slots. Workers use the
SDK's HTTP/2 cleartext transport; the Go load client uses Connect over HTTP/1.1.
A fresh ephemeral API key is passed in the environment, never printed.

After an untimed one-run warmup, the actual driver command is:

```sh
CAPSTAN_API_KEY='<ephemeral key>' .lane/bin/capstan-load \
  -address http://127.0.0.1:7301 -queue evidence-load_c50_w4 \
  -concurrency 50 -n 1000 -samples '.lane/<this-run>/runs.jsonl'
```

The wrapper substitutes the concurrency-specific queue and paths. Its printed invocation redacts the key and log directory; per-run metadata
identifies the actual log path. The [workflow](../../examples/evidence/load.ts) calls the real
`step` activity five times sequentially and must return 5. There is no work delay,
external API or mocked worker. The driver verifies the result; a post-run SQL audit
independently counts completed runs and `ACTIVITY_COMPLETED` events.

This is a closed-loop test: each completed run releases a slot for the next start.
The timed window includes starts, ramp-up and drain, and excludes building, warmup,
SQL audit and cleanup. End-to-end latency is client time from just before StartRun
until AwaitRun observes completion. It includes the server's 250 ms observation
polling. p50/p99 use nearest rank among successful runs. Activities/s counts five
per verified successful result, checked against SQL; on an error it is a lower
bound and the separate SQL audit includes partial runs. Errors print individually
and cause a nonzero exit. No run in this campaign errored.

## Results

Values below are **truncated downward** to four decimal places, never rounded up.
[load-results.json](load-results.json) preserves the full emitted values, timestamps,
raw `.lane/` directory names, SQL audits and derived sampling statistics. Repeated
rows appear in measurement order within each concurrency.

| Concurrency | Runs | Workers | Runs/s | Activities/s | p50 ms | p99 ms | Errors |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | 500 | 4 | 26.5831 | 132.9156 | 274.6876 | 533.4565 | 0 |
| 25 | 1000 | 4 | 46.8293 | 234.1465 | 532.5927 | 546.1956 | 0 |
| 50 | 1000 | 4 | 65.0399 | 325.1995 | 803.9686 | 845.9902 | 0 |
| 50 | 1000 | 4 | 78.7942 | 393.9710 | 564.7231 | 818.5701 | 0 |
| 100 | 1000 | 4 | 76.6850 | 383.4253 | 1325.2689 | 1604.0133 | 0 |
| 200 | 1000 | 4 | 55.3210 | 276.6053 | 3611.5061 | 4155.4549 | 0 |
| 200 | 1000 | 4 | 75.8139 | 379.0695 | 2594.2520 | 2846.8105 | 0 |
| 800 | 1600 | 4 | 30.9716 | 154.8584 | 25541.6017 | 27482.4260 | 0 |
| 800 | 1600 | 4 | 39.0705 | 195.3526 | 19888.4398 | 22581.8527 | 0 |
| 50 | 1000 | 1 | 61.8262 | 309.1310 | 768.9546 | 1048.8492 | 0 |
| 50 | 1000 | 8 | 58.8740 | 294.3703 | 829.1462 | 1057.5688 | 0 |

## Where it stops scaling

The useful ceiling for this implementation and setup was reached at **50–100
concurrent workflows**: 65.0399–78.7942 runs/s at 50 and 76.6850 at 100. Increasing
to 200 produced 55.3210–75.8139 runs/s with longer end-to-end waits. At 800,
throughput dropped to 30.9716–39.0705 and p99 reached 22,581.8527–27,482.4260 ms.
These are observations of the whole stack, including AwaitRun, not a universal
PostgreSQL capacity number.

The evidence points to the **server's transaction/polling path to PostgreSQL** as
the limiting component (an inference from the controls and samples):

1. Increasing workers from one to four to eight at concurrency 50 did not yield
   proportional throughput: 61.8262, 65.0399 (first four-worker run), and 58.8740
   runs/s. The four-worker repeat was 78.7942. Worker CPU had ample headroom with
   four/eight processes; the eight-worker control used more polling transactions.
2. The store fixes the pool at 20 connections in
   [pgstore.go](../../internal/store/pgstore/pgstore.go). Samples at higher
   concurrency reached 20 active/in-transaction backends, with most waiting for
   the next client statement. The sampler also sees the listener/readiness
   connections; they are not additional pool slots.
3. [AwaitRun](../../internal/server/longpoll.go) reads status every 250 ms per open
   run. [DescribeRun](../../internal/engine/runs.go) performs multiple SQL reads
   inside a transaction. [Queue notifications](../../internal/store/pgstore/notify.go)
   wake every subscriber on that queue. More outstanding runs and idle pollers
   therefore add database transactions and statement round trips to this fixed pool.
4. No ungranted heavyweight locks were observed in any sample, and the deadlock
   counter delta was zero in every sampling window. Short internal LWLock waits
   appeared in four backend observations (LockManager once, SubtransSLRU once,
   BufferContent twice). There is no evidence here
   of a row-lock-bound `SKIP LOCKED` ceiling. The SQL CPU samples are below the
   VM's four-core capacity; server CPU is also far below the host's 18-core capacity.

`ClientRead` means the backend is waiting on its client socket; it is not a SQL row
lock. See the [PostgreSQL 16 wait-event definitions](https://www.postgresql.org/docs/16/monitoring-stats.html#WAIT-EVENT-CLIENT-TABLE).
Samples cannot separately quantify pool-acquisition wait, Go scheduling, and the
Mac-to-Colima statement round trips. Identifying that split would need additional
server instrumentation; the evidence supports the transaction path as the bottleneck,
not an assertion that PostgreSQL CPU or workers are exhausted.

The first 800 run sampled another lane's test database and an active `postgres`
connection. Its result is retained and marked in the JSON. The repeat sampled no
other active databases and still dropped to 39.0705 runs/s. The full machine was
shared; absence from a one-second sample does not rule out brief competing work.

## CPU and lock samples

`run-load.mjs` samples roughly once a second, concurrently using:

```sh
ps -p <server-pid>,<worker-pids> -o pid=,pcpu=,time=,rss=,comm=
docker exec capstan-postgres-1 cat /sys/fs/cgroup/cpu.stat
psql <this-run-dsn> -XAtq -v ON_ERROR_STOP=1 -c '<SQL below>'
```

CPU percentages in the table use **deltas of accumulated CPU time**, not `ps`'s
moving-average `%CPU`. PostgreSQL uses cgroup `usage_usec`; server/workers use `ps`
TIME (centisecond resolution). 100% is one logical core. The observation window
slightly brackets the timed load and is recorded in the JSON. PostgreSQL CPU
covers its whole shared container; database wait/lock/task counts are scoped to
the test database. The first 800 observation is consequently less isolated.

| Concurrency / workers | Sample count | PostgreSQL CPU % | Server CPU % | Sum worker CPU % | Max ready tasks | Max ungranted locks |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 / 4 | 19 | 99.5640 | 118.3497 | 53.1004 | 7 | 0 |
| 25 / 4 | 21 | 124.4040 | 142.3236 | 84.0278 | 4 | 0 |
| 50 / 4 | 16 | 127.7799 | 128.2346 | 109.7509 | 15 | 0 |
| 50 / 4 | 13 | 144.3923 | 149.8901 | 132.7728 | 14 | 0 |
| 100 / 4 | 14 | 141.4065 | 145.0956 | 127.0152 | 58 | 0 |
| 200 / 4 | 18 | 130.5455 | 121.8630 | 104.6363 | 163 | 0 |
| 200 / 4 | 14 | 138.9160 | 143.6140 | 118.1678 | 166 | 0 |
| 800 / 4 | 48 | 147.7014 | 141.3589 | 66.2343 | 770 | 0 |
| 800 / 4 | 39 | 141.0522 | 146.1705 | 73.1450 | 779 | 0 |
| 50 / 1 | 16 | 123.1763 | 119.4671 | 71.0263 | 41 | 0 |
| 50 / 8 | 17 | 136.4067 | 140.4303 | 128.8788 | 19 | 0 |

The complete executed sampler SQL is in `scripts/run-load.mjs` and copied into
each `.lane/<run>/sampling.sql`. Its lock checks are:

```sql
select count(*)
from pg_locks l join pg_stat_activity a using(pid)
where a.datname=current_database() and not l.granted;

select wait_event, pg_blocking_pids(pid), left(query,120)
from pg_stat_activity
where datname=current_database() and wait_event_type='Lock';
```

It also groups `pg_stat_activity` by database/state/wait event, records pending and
leased tasks, and takes `pg_stat_database` transaction/deadlock counters. Observer
queries exclude their own PID. Zero sampled locks does not mean no brief locks ever
occurred; successful transactions do acquire locks.

## Raw output excerpts

The first 50 run, first 200 run, and second 800 run emitted these unrounded lines:

```json
{"runs":1000,"concurrency":50,"completed":1000,"errors":0,"activities":5000,"elapsed_seconds":15.375173958,"runs_per_second":65.0399145227024,"activities_per_second":325.19957261351203,"p50_ms":803.968667,"p99_ms":845.990209}
{"runs":1000,"concurrency":200,"completed":1000,"errors":0,"activities":5000,"elapsed_seconds":18.076296125,"runs_per_second":55.321067606154855,"activities_per_second":276.6053380307743,"p50_ms":3611.506125,"p99_ms":4155.454958}
{"runs":1600,"concurrency":800,"completed":1600,"errors":0,"activities":8000,"elapsed_seconds":40.951582917,"runs_per_second":39.07052880575713,"activities_per_second":195.35264402878562,"p50_ms":19888.439875,"p99_ms":22581.85275}
```

The second 800 SQL audit (warmup excluded):

```json
{"runs" : 1600, "completed" : 1600, "other" : 0, "activity_completions" : 8000, "task_failures" : 0, "remaining_tasks" : 0}
```

One actual mid-run sample from that same run (executable paths in the process output
are shortened to their basenames):

```json
{
  "at": "2026-09-28T15:35:08.871Z",
  "sample_ms": 97.26145800000086,
  "processes": "21521 155.5   0:31.00 239088 capstan-server\n21532  13.0   0:04.01 164176 node\n21533  12.3   0:03.96 159072 node\n21534  15.6   0:04.09 167936 node\n21535  14.2   0:03.83 166352 node",
  "postgres_cpu": "usage_usec 457923854\nuser_usec 223464989\nsystem_usec 234458864\ncore_sched.force_idle_usec 0\nnr_periods 0\nnr_throttled 0\nthrottled_usec 0\nnr_bursts 0\nburst_usec 0",
  "postgres": {
    "at": "2026-09-28T15:35:08.837569+00:00",
    "activity": [
      {
        "datname": "capstan_evidence_load_c800_w4_21505_1790609687010",
        "state": "idle",
        "wait_event_type": "Client",
        "wait_event": "ClientRead",
        "n": 4
      },
      {
        "datname": "capstan_evidence_load_c800_w4_21505_1790609687010",
        "state": "idle in transaction",
        "wait_event_type": "Client",
        "wait_event": "ClientRead",
        "n": 18
      }
    ],
    "waiting_locks": 0,
    "lock_waits": [],
    "tasks": {
      "total": 759,
      "leased": 23,
      "ready": 736
    },
    "database": {
      "xact_commit": 88439,
      "xact_rollback": 0,
      "blks_read": 19,
      "blks_hit": 2048793,
      "deadlocks": 0
    }
  }
}
```
