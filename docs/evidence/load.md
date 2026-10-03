# Load measurements

## Historical load measurements

The original load samples are now archived under [raw/historical/load/](raw/historical/load/). The [manifest](raw/historical/manifest.json) preserves their source paths and hashes. Percentiles, CPU and lock statistics are recomputed from those samples; the final-code section below supersedes historical performance claims.


Verified on 2026-09-28. This is the complete set of **11 measured runs**: 11,700
workflows and 58,500 persisted activity completions, with zero failed runs. The best
observed run was 78.7942 runs/s at concurrency 50. Throughput stopped improving in
the 50–100 range and did not scale at 200; 800 made both throughput and latency worse.
Repeated runs vary substantially, so all repeats are retained below.

## Machine and exact versions

Apple M5 Max, 18 logical CPUs, 51,539,607,552 bytes (48 GiB; marketed as 48 GB),
macOS 26.6.2 build 25G83. The server and Node workers ran natively on the Mac.
PostgreSQL ran in the existing Colima VM: 4 vCPUs, 8 GiB configured memory, Docker
reported 8,307,109,888 usable bytes. Other work shared this machine and PostgreSQL.

- Go 1.26.4 darwin/arm64; Connect Go 1.21.0; pgx 5.11.0; Go protobuf 1.36.12.
- PostgreSQL 16.15, aarch64 Alpine, GCC 15.2.0; psql client 16.13.
- Node 26.8.1; npm 11.19.0; TypeScript 7.0.2; tsx 4.23.15; esbuild 0.28.2.
- Connect JS/connect-node 2.2.0; protobuf JS 2.15.0; zod 4.6.5.
- Colima 0.10.3; Docker client 29.4.1, server 29.5.2.
- Base engine/store/SDK: `73804f2`; viewer: `48ad5b6`. Load sources are in this commit.

Versions were read with `scripts/evidence-versions.sh` (raw output [archived versions.log](raw/historical/evidence/versions.log)).
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
scripts/run-load.sh 800 1600 4  # repeat after a sample observed another process
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

The first 800 run sampled another test database and an active `postgres`
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


## Historical performance measurements

Measured on 2026-09-28, with the same machine, versions, workload, driver, worker
slots, transports, sampling SQL and timing boundaries described above. The before
section is preserved verbatim. Four local performance commits are measured separately:
`449874b` (D30), `00bac7a` (pool), `de91ce3` (queue wakes), and `df11bda` (lock order).
The final campaigns also include main through `eb2659e`, including the fault lab.
The required D30 fault-wrapper adapter affects lab tests only.

The prescribed 11 runs were repeated in the exact original invocation order, then
the entire 11-run campaign was repeated because other processes were sampled. Together
they completed **23,400 workflows and 117,000 persisted activity completions, with zero
errors**, zero task failures and no remaining tasks in any post-run audit. All
50 new baseline, tuning and final measurements are appended to
[load-results.json](load-results.json); its original 11 entries are unchanged.
Numbers in the tables are truncated downward to four decimals.

### Reproduce the after campaigns

Use the same 11 `scripts/run-load.sh` commands in the Reproduce section, with:

```sh
export PATH="$HOME/.local/bin:$PATH"
export CAPSTAN_LOAD_PORT=7401 CAPSTAN_LOAD_DB_PREFIX=capstan_perf
export CAPSTAN_LOAD_PHASE=after
unset CAPSTAN_DB_MAX_CONNS  # measured default: 40
# Run the original eleven commands in their listed order.
# Repeat them with CAPSTAN_LOAD_PHASE=after-repeat.
```

The harness changes only add a safe database-prefix option, performance ports,
and phase/pool metadata. Its defaults remain `capstan_evidence_*` and 7301.
The performance campaign used only port 7401 and its own `capstan_perf_*` load databases.
Archived controls and gates are in [raw/historical/perf/](raw/historical/perf/).
Raw logs were `.lane/after-1.log` through `after-11.log`, and
`.lane/after-repeat-1.log` through `after-repeat-11.log`. Each points to an unchanged
per-run directory containing samples, driver output, worker/server logs and SQL audit.
Versions are in [archived after-versions.log](raw/historical/perf/after-versions.log). `dbMaxConns: "default"` means 20 for the
fresh `baseline`/`d30`/`d30-repeat` phases and 40 for `pool-default`/`after`/`after-repeat`.

### Changes measured separately

These are 1,000-run, four-worker points; each row adds only its named change to the
previous row. No other active database was sampled in these selected comparisons.
Early D30 runs and reverse-order pool confirmations were contaminated; they and
their repeats remain in the JSON. Short points can vary even without sampled
competition, so the table is evidence of the observed effect, not an isolated
causal estimate of every throughput difference.

| Implementation | c50 runs/s | c200 runs/s | c50 p99 ms | c200 p99 ms | Sampled commits c50 / c200 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original, pool 20 | 73.1594 | 70.4963 | 827.8188 | 2964.6783 | 47,076 / 43,727 |
| D30, pool 20 | 73.8684 | 73.8752 | 820.1086 | 2941.0007 | 37,547 / 34,594 |
| D30, pool 40 | 82.7599 | 87.1296 | 742.3722 | 2591.4980 | 47,208 / 37,066 |
| Targeted queue wakes, pool 40 | 97.0447 | 90.2726 | 633.8322 | 2381.7598 | 38,493 / 33,988 |
| Run/child lock ordering, pool 40 | 102.9712 | 102.6073 | 591.4913 | 2300.6898 | 38,641 / 35,478 |

D30 replaces the 250 ms status tick with commit-only run-close notifications and a
five-second fallback. AwaitRun registers before reading state, and re-reads committed
state when woken. Every waiter for that run receives a hint. PostgreSQL multiplexes
these topics over its existing single LISTEN connection. The harness still sets a
two-second server poll timeout, so renewed requests can re-read sooner than the
five-second lost-notification fallback. The server close test failed at 250 ms before
the change and now satisfies its 10 ms bound under Go's test clock.

`CAPSTAN_DB_MAX_CONNS` accepts a positive 32-bit integer. The default is **40**,
chosen from the following sweep after D30, before targeted wakeups:

| Pool limit | c50 runs/s | c200 runs/s |
| ---: | ---: | ---: |
| 10 | 48.9822 * | 51.9673 |
| 20 | 73.8684 | 73.8752 |
| 40 | 82.7599 | 87.1296 |
| 60 | 70.7011 | 81.4314 |

`*` sampled another active database. Forty was the best of these measured limits
on this machine; sixty added transactions and reduced throughput. Reverse-order
confirmations also favored 40 over 20, but were all contaminated. This supports a
configurable default for this setup, not a claim that 40 is optimal everywhere.
The single LISTEN connection is additional to the configured transaction pool.
No PostgreSQL setting or dependency was changed.

Queue hints now rotate through waiting local subscribers and wake at most one per
notification. Distinct same-transaction payloads preserve multiple new-task hints.
Buffered subscribers are skipped; run-close hints still broadcast. The worker's
existing one-second fallback remains. This targets one subscriber **per store/server
instance**; multiple server listeners can each wake one poller for a database hint.
Reconnect wakes all subscribers once to recover potentially lost hints.

For a controlled 40-waiter/one-task test, `pg_stat_database.xact_commit` changed
from 9 to 50 before (delta 41) and 9 to 11 after (delta 2). The matched no-claim
control changed from 8 to 9 (delta 1), leaving **40 versus 1 claim transactions**.
Direct claim-attempt counters independently measured 40 versus 1, with one task
claimed in both cases. See [archived targeted-before.log](raw/historical/perf/targeted-before.log) and
[archived targeted-after-reviewed.log](raw/historical/perf/targeted-after-reviewed.log). The insert and LISTEN delivery precede this
window; both final isolated control and workload residuals are zero. Database-wide
counters can include background work and delayed flushes, so the regression asserts
actual attempts and claims and logs counter residuals. Whole-load commit deltas in
the earlier table include all transactions and are not labeled claim counts.

Lock-order changes take the run before a token's task, acquire due run/child rows
with `SKIP LOCKED`, and use `NOWAIT` when a claim already holds a task. A busy parent
rolls back that claim before the engine returns an empty poll result. The bounded
`40P01` transaction retry remains. At concurrency 200 and 800, deadlock-counter
deltas were **0 / 0 before and 0 / 0 after**. A forced terminate/activity-completion
race did reproduce one deadlock retry before; the 16 ordered termination races now
require zero retries. Load did not reproduce that race, so no load-throughput gain
is attributed to avoiding measured deadlocks.

### Before and after

Ranges include every original repeat and both final campaigns, without discarding
contaminated or slower runs:

| Concurrency / workers | Before runs/s | After runs/s, both campaigns | Before p99 ms | After p99 ms, both campaigns |
| --- | ---: | ---: | ---: | ---: |
| 10 / 4 | 26.5831 | 35.1522–53.0748 | 533.4565 | 235.7692–371.1258 |
| 25 / 4 | 46.8293 | 55.3789–85.1757 | 546.1956 | 357.1042–885.0265 |
| 50 / 4 | 65.0399–78.7942 | 64.0164–100.4584 | 818.5701–845.9902 | 609.5528–911.7544 |
| 100 / 4 | 76.6850 | 77.8635–104.0593 | 1604.0133 | 1076.9780–1452.7068 |
| 200 / 4 | 55.3210–75.8139 | 65.2419–99.0307 | 2846.8105–4155.4549 | 2188.5067–3449.4383 |
| 800 / 4 | 30.9716–39.0705 | 61.7943–99.9086 | 22581.8527–27482.4260 | 8584.0774–13778.9257 |
| 50 / 1 | 61.8262 | 40.3525–69.4714 | 1048.8492 | 833.1988–1358.0146 |
| 50 / 8 | 58.8740 | 60.1368–95.8973 | 1057.5688 | 602.4330–1023.2328 |

### Final campaigns, in measurement order

| Phase / run | Concurrency | Runs | Workers | Runs/s | Activities/s | p50 ms | p99 ms | Errors | Other active DBs sampled |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| after / 1 | 50 | 1000 | 4 | 100.4584 | 502.2922 | 479.8036 | 723.4297 | 0 | yes |
| after / 2 | 200 | 1000 | 4 | 99.0307 | 495.1538 | 2008.3281 | 2188.5067 | 0 | yes |
| after / 3 | 800 | 1600 | 4 | 99.9086 | 499.5431 | 7667.6937 | 8584.0774 | 0 | yes |
| after / 4 | 50 | 1000 | 1 | 69.4714 | 347.3572 | 713.6930 | 833.1988 | 0 | yes |
| after / 5 | 50 | 1000 | 8 | 95.8973 | 479.4865 | 515.9572 | 602.4330 | 0 | yes |
| after / 6 | 25 | 1000 | 4 | 85.1757 | 425.8789 | 289.7465 | 357.1042 | 0 | yes |
| after / 7 | 10 | 500 | 4 | 53.0748 | 265.3740 | 185.6120 | 235.7692 | 0 | yes |
| after / 8 | 100 | 1000 | 4 | 104.0593 | 520.2965 | 953.8575 | 1076.9780 | 0 | yes |
| after / 9 | 50 | 1000 | 4 | 98.6294 | 493.1474 | 504.0983 | 609.5528 | 0 | yes |
| after / 10 | 200 | 1000 | 4 | 96.5353 | 482.6765 | 1970.1265 | 2521.5074 | 0 | yes |
| after / 11 | 800 | 1600 | 4 | 84.4352 | 422.1761 | 8688.1524 | 10325.3389 | 0 | yes |
| after-repeat / 1 | 50 | 1000 | 4 | 64.0164 | 320.0824 | 773.8389 | 911.7544 | 0 | yes |
| after-repeat / 2 | 200 | 1000 | 4 | 65.2419 | 326.2097 | 3040.2248 | 3449.4383 | 0 | yes |
| after-repeat / 3 | 800 | 1600 | 4 | 61.7943 | 308.9715 | 12525.3935 | 13778.9257 | 0 | yes |
| after-repeat / 4 | 50 | 1000 | 1 | 40.3525 | 201.7626 | 1233.0484 | 1358.0146 | 0 | yes |
| after-repeat / 5 | 50 | 1000 | 8 | 60.1368 | 300.6842 | 844.7996 | 1023.2328 | 0 | yes |
| after-repeat / 6 | 25 | 1000 | 4 | 55.3789 | 276.8947 | 432.6226 | 885.0265 | 0 | yes |
| after-repeat / 7 | 10 | 500 | 4 | 35.1522 | 175.7611 | 280.2942 | 371.1258 | 0 | yes |
| after-repeat / 8 | 100 | 1000 | 4 | 77.8635 | 389.3175 | 1257.1277 | 1452.7068 | 0 | yes |
| after-repeat / 9 | 50 | 1000 | 4 | 73.9042 | 369.5211 | 671.3789 | 827.0068 | 0 | yes |
| after-repeat / 10 | 200 | 1000 | 4 | 74.4456 | 372.2284 | 2694.7702 | 2874.3902 | 0 | yes |
| after-repeat / 11 | 800 | 1600 | 4 | 70.9953 | 354.9767 | 10969.5038 | 11851.9331 | 0 | yes |

### Where it now saturates

The observed ceiling is now around **100 runs/s**, reached around **50–100
concurrent workflows** on this setup. The best final-campaign observation is
104.0593 runs/s at concurrency 100 with 4 workers.
At concurrency 100 the campaigns produced 77.8635–104.0593 runs/s;
200 produced 65.2419–99.0307; 800 produced 61.7943–99.9086.
More outstanding runs still primarily increase queueing: p99 at 800 is
8584.0774–13778.9257 ms. The former 800-concurrency collapse to
30.9716–39.0705 runs/s and p99 22,581.8527–27,482.4260 ms is substantially reduced.
These short, shared-machine runs establish an observed operating range, not a
statistical confidence interval or a universal capacity limit.

The transaction path remains the most supported limiting-component inference.
The following samples show pool occupancy and queued tasks as concurrency increases.
At 800, hundreds of tasks wait while throughput stays near the 50–100 range.
One/four/eight-worker controls do not show proportional scaling. PostgreSQL's shared
container and server/worker CPU observations do not establish CPU exhaustion;
`ClientRead` continues to dominate backend waits. The pool, application scheduling,
and Mac-to-Colima statement round trips remain combined in this measurement.
Separating them requires additional instrumentation; no fifth optimization was
made without that measurement.

| Phase / run | Concurrency / workers | Samples | PostgreSQL CPU % | Server CPU % | Sum worker CPU % | Max active/in-transaction backends | Max ready tasks | Max ungranted locks | Deadlock delta |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| after / 1 | 50 / 4 | 11 | 171.8966 | 166.9992 | 148.4034 | 39 | 22 | 2 | 0 |
| after / 2 | 200 / 4 | 11 | 179.8197 | 166.4858 | 149.6654 | 40 | 164 | 1 | 0 |
| after / 3 | 800 / 4 | 16 | 187.7708 | 186.3324 | 156.1407 | 39 | 767 | 1 | 0 |
| after / 4 | 50 / 1 | 15 | 112.5673 | 111.4655 | 66.0199 | 18 | 41 | 0 | 0 |
| after / 5 | 50 / 8 | 11 | 183.1759 | 172.1944 | 188.0963 | 40 | 28 | 1 | 0 |
| after / 6 | 25 / 4 | 12 | 168.3621 | 150.8809 | 137.0667 | 28 | 13 | 1 | 0 |
| after / 7 | 10 / 4 | 10 | 100.4130 | 97.1149 | 88.6834 | 12 | 7 | 0 | 0 |
| after / 8 | 100 / 4 | 10 | 190.5615 | 180.2583 | 165.7246 | 40 | 61 | 2 | 0 |
| after / 9 | 50 / 4 | 11 | 180.2159 | 167.5090 | 157.6714 | 40 | 21 | 1 | 0 |
| after / 10 | 200 / 4 | 11 | 186.4148 | 168.5291 | 155.4465 | 39 | 163 | 1 | 0 |
| after / 11 | 800 / 4 | 19 | 183.0284 | 163.5333 | 138.0157 | 39 | 764 | 0 | 0 |
| after-repeat / 1 | 50 / 4 | 16 | 168.4401 | 141.3076 | 123.6138 | 38 | 33 | 0 | 0 |
| after-repeat / 2 | 200 / 4 | 16 | 188.7756 | 144.0955 | 128.6842 | 39 | 166 | 0 | 0 |
| after-repeat / 3 | 800 / 4 | 25 | 195.1394 | 149.6033 | 121.8558 | 40 | 763 | 2 | 0 |
| after-repeat / 4 | 50 / 1 | 24 | 143.5111 | 89.5857 | 61.2975 | 19 | 42 | 0 | 0 |
| after-repeat / 5 | 50 / 8 | 17 | 188.5050 | 134.0102 | 141.7290 | 39 | 36 | 1 | 0 |
| after-repeat / 6 | 25 / 4 | 18 | 168.1386 | 119.8141 | 109.6694 | 32 | 13 | 1 | 0 |
| after-repeat / 7 | 10 / 4 | 15 | 106.1406 | 84.8547 | 78.7690 | 15 | 5 | 2 | 0 |
| after-repeat / 8 | 100 / 4 | 13 | 186.4564 | 158.9538 | 146.7440 | 40 | 64 | 0 | 0 |
| after-repeat / 9 | 50 / 4 | 14 | 180.7315 | 152.8359 | 130.7627 | 40 | 26 | 2 | 0 |
| after-repeat / 10 | 200 / 4 | 14 | 181.4597 | 152.8311 | 134.3294 | 40 | 163 | 1 | 0 |
| after-repeat / 11 | 800 / 4 | 22 | 186.1713 | 163.5366 | 128.5671 | 40 | 764 | 0 | 0 |

Other work shared the machine and PostgreSQL: **22 of these 22 final
runs sampled other active databases**. An independent process audit observed the
lab's PostgreSQL campaign and later CPU-heavy lab campaigns overlapping the
first campaign, including its slower final 800 point. No performance tests ran
alongside its loads, and no own orphan process was found. The second campaign is
retained regardless of overlap. Container CPU includes other databases; absence
from a one-second sample still cannot establish an otherwise idle host. The JSON
lists every observed competing database. All final deadlock deltas are zero. Unlike
the before campaign, 17 of 331 samples observed ungranted locks, with a maximum of
two. The separate lock-wait detail queries recorded 17 `transactionid` and six
`object` backend observations. Wait durations were not measured, and the concurrent
sampler queries do not describe exactly the same instant. These waits may contribute
to latency; the samples do not establish row locking as the throughput ceiling.

### Correctness

Every performance commit passed `make verify`: generation drift checks, buf lint,
gofmt, go vet, Go race tests, both store suites, pgengine engine tests, SDK typecheck
and tests, and the real-server e2e. Logs are [archived d30-verify.log](raw/historical/perf/d30-verify.log),
[archived pool-verify.log](raw/historical/perf/pool-verify.log), [archived targeted-verify.log](raw/historical/perf/targeted-verify.log), and [archived lock-verify.log](raw/historical/perf/lock-verify.log).
After merging main and adapting its fault wrapper to D30, the final merged gate
passed ([archived merged-verify-reviewed.log](raw/historical/perf/merged-verify-reviewed.log)). It includes the lab at 50 seeds under
race and 2,000 without; an additional `go test -count=1 -v ./internal/lab/...`
passed its default 2,000 seeds ([archived lab-default-reviewed.log](raw/historical/perf/lab-default-reviewed.log)). Database tests
were never skipped. SDK output: 18 files passed, 2 opt-in files skipped;
369 tests passed, 5 skipped. The performance campaign made no SDK source changes.

<!-- final-numbers:start -->
## Final code (904cb6c)

Measured on 2026-09-28. These numbers are recomputed by
[verify-numbers.py](raw/verify-numbers.py) from the committed observations.

The prescribed 11 runs completed **11,700 workflows and 58,500
activity completions, with zero failed runs**. Every SQL audit reports zero task
failures and zero remaining tasks. The order, worker counts and untimed warmup are
unchanged. The server used its default connection pool, port 7701,
`CAPSTAN_LOAD_PHASE=final` and a fresh `capstan_final_*` database for every run.

| Run | Concurrency | Runs | Workers | Runs/s | Activities/s | p50 ms | p99 ms | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [1](raw/904cb6c/load-01.log) | 50 | 1000 | 4 | 96.7596 | 483.7980 | 511.7766 | 609.4396 | 0 |
| [2](raw/904cb6c/load-02.log) | 200 | 1000 | 4 | 95.5274 | 477.6374 | 2060.8009 | 2363.9277 | 0 |
| [3](raw/904cb6c/load-03.log) | 800 | 1600 | 4 | 84.5360 | 422.6800 | 9181.9240 | 9909.9627 | 0 |
| [4](raw/904cb6c/load-04.log) | 50 | 1000 | 1 | 60.2563 | 301.2818 | 825.4301 | 1026.4371 | 0 |
| [5](raw/904cb6c/load-05.log) | 50 | 1000 | 8 | 88.9244 | 444.6220 | 553.7171 | 671.7268 | 0 |
| [6](raw/904cb6c/load-06.log) | 25 | 1000 | 4 | 73.7525 | 368.7625 | 331.3333 | 426.4686 | 0 |
| [7](raw/904cb6c/load-07.log) | 10 | 500 | 4 | 47.4597 | 237.2989 | 209.4845 | 255.4377 | 0 |
| [8](raw/904cb6c/load-08.log) | 100 | 1000 | 4 | 95.3990 | 476.9950 | 1027.2412 | 1222.5159 | 0 |
| [9](raw/904cb6c/load-09.log) | 50 | 1000 | 4 | 91.5157 | 457.5787 | 543.8870 | 633.9914 | 0 |
| [10](raw/904cb6c/load-10.log) | 200 | 1000 | 4 | 93.4880 | 467.4403 | 2098.0417 | 2388.8757 | 0 |
| [11](raw/904cb6c/load-11.log) | 800 | 1600 | 4 | 87.7664 | 438.8323 | 8947.5163 | 9451.8642 | 0 |

Rates and percentiles are truncated downward to four decimal places. The
[derived JSON](raw/904cb6c/numbers.json) retains full precision. Each linked command
log identifies its [raw directory](raw/904cb6c/load/), containing every latency,
system sample, metadata, driver result, SQL audit and sampler SQL. JSONL is gzip
compressed. The elapsed interval is emitted by the load driver; rates are computed
from it and verified successful samples. It cannot be recovered by summing
overlapping run latencies.

The highest observation was **96.7596 runs/s** at
concurrency 50 with 4 workers.
This is a measured point on a shared host, not a service capacity guarantee.
Increasing concurrency does not produce proportional throughput. These results
include the audit's busy-parent polling fix and supersede the earlier throughput
claims for this source revision.

The observed ceiling in the four-worker runs was
91.5157–96.7596 runs/s at concurrency 50–200.
At concurrency 800, rates fell to
84.5360–87.7664 runs/s,
with p99 latency 9451.8642–9909.9627 ms.
These points establish the observed operating range, not a confidence interval or
a causal estimate of the audit fix's performance cost.

| Run | Samples | PG CPU % | Server CPU % | Worker CPU % | Max active/transaction | Max ready | Max ungranted locks | Deadlocks | Other active DB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 11 | 178.1955 | 180.3323 | 150.1861 | 39 | 35 | 1 | 0 | no |
| 2 | 11 | 184.5295 | 178.2636 | 151.8642 | 39 | 165 | 0 | 0 | no |
| 3 | 19 | 186.8298 | 175.7663 | 144.2095 | 40 | 774 | 2 | 0 | no |
| 4 | 17 | 105.7920 | 106.2314 | 61.3465 | 20 | 50 | 0 | 0 | no |
| 5 | 12 | 176.8406 | 171.7337 | 176.4995 | 40 | 33 | 0 | 0 | no |
| 6 | 14 | 161.7579 | 142.4293 | 128.0819 | 28 | 13 | 1 | 0 | no |
| 7 | 11 | 96.2781 | 94.3533 | 86.9763 | 10 | 7 | 0 | 0 | no |
| 8 | 11 | 181.3124 | 178.9053 | 156.7577 | 39 | 60 | 1 | 0 | no |
| 9 | 12 | 185.8290 | 181.8100 | 152.3929 | 40 | 33 | 3 | 0 | no |
| 10 | 11 | 190.2656 | 179.6195 | 153.3514 | 40 | 163 | 2 | 0 | no |
| 11 | 18 | 188.5199 | 183.0521 | 147.0165 | 39 | 767 | 1 | 0 | no |

Other active databases were sampled in **0 of 11** runs. CPU
percentages use accumulated-time deltas; PostgreSQL CPU covers the shared container.
One core is 100%. These observations combine pool waiting, statement round trips
and application scheduling. They do not identify a universal row-lock or CPU ceiling.

Reproduce the exact sequence with `python3 docs/evidence/raw/reproduce.py load`.
The [harness notes](raw/README.md) identify resource and metrics-authentication
changes; engine, SDK and workload assertions are unchanged.
<!-- final-numbers:end -->
