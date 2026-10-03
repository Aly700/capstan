"""Generate checked final numeric sections from verifier-derived observations."""
from collections import Counter
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[3]
E = ROOT / "docs/evidence"
SHA = "904cb6c"
START = "<!-- final-numbers:start -->"
END = "<!-- final-numbers:end -->"


def trunc(value):
    return format(Decimal(str(value)).quantize(Decimal("0.0001"),rounding=ROUND_DOWN),".4f")


def table(headers, rows):
    return "\n".join(["| "+" | ".join(headers)+" |","| "+" | ".join(["---"]*len(headers))+" |",
                     *["| "+" | ".join(map(str,row))+" |" for row in rows]])+"\n"


def section(content):
    return f"## Final code ({SHA})\n\nMeasured on 2026-09-28. These numbers are recomputed by\n[verify-numbers.py](raw/verify-numbers.py) from the committed observations.\n\n{content.strip()}\n"


def replace_block(path, text, write, start=START, end=END):
    current = path.read_text()
    desired = start+"\n"+text.strip()+"\n"+end
    pattern = re.escape(start)+r".*?"+re.escape(end)
    if start in current:
        updated = re.sub(pattern,lambda _:desired,current,flags=re.S)
    else:
        updated = current.rstrip()+"\n\n"+desired+"\n"
    if write:
        path.write_text(updated)
    else:
        assert updated==current, f"stale numeric section: {path}; run verify-numbers.py --write"


def check_documents(numbers, mutations, write=False):
    loads,lab,pg = numbers["load"],numbers["lab"],numbers["pg"]
    total = sum(d["result"]["runs"] for d in loads)
    activities = sum(d["result"]["activities"] for d in loads)
    best = max(loads,key=lambda d:d["result"]["runs_per_second"])
    result_rows, cpu_rows = [],[]
    for i,d in enumerate(loads,1):
        r,s = d["result"],d["sampler"]
        workers = d["command"].split()[-1]
        result_rows.append([f"[{i}](raw/{SHA}/load-{i:02}.log)",r["concurrency"],r["runs"],workers,
                            *[trunc(r[k]) for k in ("runs_per_second","activities_per_second","p50_ms","p99_ms")],r["errors"]])
        cpu_rows.append([i,s["samples"],trunc(s["postgres_cpu_percent"]),trunc(s["server_cpu_percent"]),
                         trunc(sum(s["worker_cpu_percent"])),s["max_active_or_in_transaction"],
                         s["max_ready_tasks"],s["max_waiting_locks"],s["deadlocks_delta"],
                         "yes" if s["other_active_databases"] else "no"])
    other = sum(bool(d["sampler"]["other_active_databases"]) for d in loads)
    plateau = [d["result"]["runs_per_second"] for d in loads
               if d["command"].split()[-1]=="4" and d["result"]["concurrency"] in (50,100,200)]
    crowded = [d["result"] for d in loads if d["result"]["concurrency"]==800]
    load_text = f"""The prescribed {len(loads)} runs completed **{total:,} workflows and {activities:,}
activity completions, with zero failed runs**. Every SQL audit reports zero task
failures and zero remaining tasks. The order, worker counts and untimed warmup are
unchanged. The server used its default connection pool, port 7701,
`CAPSTAN_LOAD_PHASE=final` and a fresh `capstan_final_*` database for every run.

{table(["Run","Concurrency","Runs","Workers","Runs/s","Activities/s","p50 ms","p99 ms","Errors"],result_rows)}
Rates and percentiles are truncated downward to four decimal places. The
[derived JSON](raw/{SHA}/numbers.json) retains full precision. Each linked command
log identifies its [raw directory](raw/{SHA}/load/), containing every latency,
system sample, metadata, driver result, SQL audit and sampler SQL. JSONL is gzip
compressed. The elapsed interval is emitted by the load driver; rates are computed
from it and verified successful samples. It cannot be recovered by summing
overlapping run latencies.

The highest observation was **{trunc(best["result"]["runs_per_second"])} runs/s** at
concurrency {best["result"]["concurrency"]} with {best["command"].split()[-1]} workers.
This is a measured point on a shared host, not a service capacity guarantee.
Increasing concurrency does not produce proportional throughput. These results
include the audit's busy-parent polling fix and supersede the earlier throughput
claims for this source revision.

The observed ceiling in the four-worker runs was
{trunc(min(plateau))}–{trunc(max(plateau))} runs/s at concurrency 50–200.
At concurrency 800, rates fell to
{trunc(min(r["runs_per_second"] for r in crowded))}–{trunc(max(r["runs_per_second"] for r in crowded))} runs/s,
with p99 latency {trunc(min(r["p99_ms"] for r in crowded))}–{trunc(max(r["p99_ms"] for r in crowded))} ms.
These points establish the observed operating range, not a confidence interval or
a causal estimate of the audit fix's performance cost.

{table(["Run","Samples","PG CPU %","Server CPU %","Worker CPU %","Max active/transaction","Max ready","Max ungranted locks","Deadlocks","Other active DB"],cpu_rows)}
Other active databases were sampled in **{other} of {len(loads)}** runs. CPU
percentages use accumulated-time deltas; PostgreSQL CPU covers the shared container.
One core is 100%. These observations combine pool waiting, statement round trips
and application scheduling. They do not identify a universal row-lock or CPU ceiling.

Reproduce the exact sequence with `python3 docs/evidence/raw/reproduce.py load`.
The [harness notes](raw/README.md) identify resource and metrics-authentication
changes; engine, SDK and workload assertions are unchanged.
"""
    replace_block(E/"load.md",section(load_text),write)
    lab_summary = f"""The current fault model completed **{lab["Runs"]:,} seeds**, with
**{lab["Passed"]:,} passing, {lab["Failing seeds"]} failing and
{lab["Recognized known failures"]} known-failure exclusions**. Elapsed:
**{lab["elapsed"]}**. The seed range is 0–{lab["Runs"]-1}; parallelism is 8,
with 3 workers and a 1,000-step bound per seed.

The faulted executions contain **{lab["Total steps"]:,} actor steps**,
**{lab["Root runs"]:,} initial root runs** and
**{lab["Store transaction steps"]:,} scheduled store transaction steps**.
Including fault-free baselines, that is {lab["Runs"]*2:,} scenario executions and
{lab["Root runs"]*2:,} initial root runs, plus {lab["Runs"]:,} protocol probes.
Continuation descendants are additional.

Raw [per-seed records](raw/{SHA}/lab-seeds.jsonl.gz), [command output](raw/{SHA}/lab.log)
and the [emitted report](raw/{SHA}/lab-report.md) are committed. The verifier sums
the individual records and checks the report. The logging-only overlay and its
timing overhead are described in [raw/README.md](raw/README.md).
"""
    lab_tables = table(["Fault","Count"],sorted(lab["faults"].items()))+"\n"+table(["Gate response","Count"],sorted(lab["gate_responses"].items()))
    for name in ("lab-2026-09-28.md","lab-delta1-2026-09-28.md"):
        replace_block(E/name,section(lab_summary+"\n"+lab_tables+"\nReproduce: `python3 docs/evidence/raw/reproduce.py lab`.\n\nThese are finite executions within the documented fault model. No new seed-reproduced engine or store defect was found."),write)
    replace_block(E/"lab-l2.md",section(lab_summary+"\nAll 9 fault kinds and 5 Gate response kinds occurred. The verifier also checks all 35 transaction wrappers against the Store interface. The final mutation catalogue catches 25 of 26 valid mutants; M003 is equivalent under the documented single-mutant, engine-reachable-state premise. These results do not expand the fault model's limits."),write)
    replace_block(E/"lab-l1.md",section("""The checked corpus contains 39 shared command fixtures and 14 shared mismatch
fixtures. The Go worker covers those 53 fixtures; fixture 054 is TypeScript-only
under D20. All 29 TypeScript workflow exports have Go twins. The verifier derives
these counts from the committed corpus and registration code.

The final [merge gate](raw/904cb6c/verify.log) exercises the reference worker and
the SDK. The [per-seed campaign](raw/904cb6c/lab-seeds.jsonl.gz) exercises the same
reference worker across 200,000 seeds. Fixture coverage does not imply that the
Go worker models V8 microtask scheduling."""),write)
    mutation_rows = [[r["mutant"]["id"],r["status"],"—" if r["seed"]<0 else r["seed"],
                      r["check"].replace("|","\\|").replace("\n"," ")] for r in mutations["results"]]
    counts = Counter(r["status"] for r in mutations["results"])
    caught,equivalent = counts["caught"],counts["equivalent"]
    replace_block(E/"lab-mutation.md",section(f"""The ordinary mutation command ran a {mutations["seeds"]:,}-seed baseline and all
{len(mutation_rows)} catalogue entries: **{caught} caught, {equivalent} equivalent,
0 invalid and 0 unexplained survivors**. Score: {caught/len(mutation_rows)*100:.2f}%;
excluding the justified equivalent survivor: {caught/(len(mutation_rows)-equivalent)*100:.2f}%.
The baseline and M003 each passed all {mutations["seeds"]:,} seeds. Each caught
mutant stopped at the first failing seed below.

{table(["ID","Result","First seed","Observed check"],mutation_rows)}
The verifier reads every [raw mutant log](raw/{SHA}/mutations/), derives each first
failing seed and outcome, and checks the [machine report](raw/{SHA}/mutations/results.json).
Patch hashes are checked against the committed catalogue. The M003 invariant
argument above still applies; surviving an execution alone is not that argument.

Reproduce: `python3 docs/evidence/raw/reproduce.py mutate`.
"""),write)
    replace_block(E/"lab-pg.md",section(f"""The real-clock PostgreSQL campaign passed **{pg["seeds"]} seeds** in
**{pg["elapsed"]}**, with **{pg["steps"]:,} actor steps**,
**{pg["duplicate_acks"]:,} rejected duplicate acknowledgements**,
**{pg["database_errors"]} injected database errors** and
**{pg["server_crashes"]} injected server crashes**.
The two variants and two roots per seed give **{pg["executions"]:,} completed run executions**.

The [raw command output](raw/{SHA}/pg.log) includes every seed's counters.
The verifier sums them and checks the emitted aggregate. The overlay only prints
those counters; the real clock, 500 ms leases, fault injection, transaction
assertions and cleanup are unchanged. Both campaign databases were created and
dropped by the test. The shared PostgreSQL was left running.

Reproduce: `python3 docs/evidence/raw/reproduce.py pg`. The narrower scope and
real-clock limitations described above still apply.
"""),write)
    replace_block(E/"defects.md",section(f"""No new seed-reproduced engine/store defect appeared in the {lab["Runs"]:,}-seed
current memory campaign or the {pg["seeds"]}-seed PostgreSQL campaign. Known-failure
exclusions remain zero. This does not erase defects independently found by the
[audit](audit-2026-09-28.md), and planted mutation kills are not production defects.
See [per-seed memory data](raw/{SHA}/lab-seeds.jsonl.gz) and
[PostgreSQL data](raw/{SHA}/pg.log)."""),write)
    replace_block(E/"crash.md",section(f"""The rerun on this revision returned `[1,2,3,4]`, preserving
**{numbers["crash_prefix"]} pre-crash events** as an exact prefix. It recorded
**4 activity completions and 3 distinct timer firings**. The original recording
was retained. [Command log](raw/{SHA}/demo-crash.log),
[before/after histories](raw/{SHA}/demo-crash/).
"""),write)
    replace_block(E/"blocked.md",section(f"""The real V2 worker blocked the old run at history event 5. An explicit
resume with the patched worker completed it with value 1 and one activity
completion; a new run recorded the patch marker and returned value 10.
[Full CLI histories and assertions](raw/{SHA}/demo-blocked.log). The original
recording was retained.
"""),write)
    replace_block(E/"idle-wait.md",section(f"""The signal demo again observed zero demo workers and zero task leases across
5 real seconds. It persisted the signal before starting a replacement worker,
then completed with `{{approved:true}}`. [Raw SQL, process and CLI output](raw/{SHA}/demo-idle-wait.log).

The separate [seven-day human-approval test](raw/{SHA}/human-seven-days.log)
uses the engine Clock over real PostgreSQL RPCs. It verifies a 7-day clock
advance with zero tasks or leases, then approval and completion. Neither test
is a live multi-day Gate deployment.
"""),write)
    evidence_rows = [
        ["Crash recovery","Kill and restart the server; unchanged history prefix","[Crash](docs/evidence/crash.md)","VERIFIED"],
        ["Key-honouring effects",f'{lab["Runs"]:,} memory seeds; {pg["seeds"]} PostgreSQL seeds; {caught}/{len(mutation_rows)} mutants caught, {equivalent} equivalent',"[Lab](docs/evidence/lab-delta1-2026-09-28.md)","VERIFIED within the fault model"],
        ["Durable waits","Worker-free signal wait; human approval after an engine-clock jump","[Waits](docs/evidence/idle-wait.md)","VERIFIED locally"],
        ["Incompatible code blocks","Changed worker, explicit resume and patched old/new branches","[Replay](docs/evidence/blocked.md)","VERIFIED"],
        ["Observed throughput",f'{total:,} workflows; {activities:,} activity completions; 0 errors',"[Load](docs/evidence/load.md)","VERIFIED on the measured host"],
        ["Recorded model cost","Retained paid-call recording and ledger arithmetic","[Cost](docs/evidence/cost.md)","VERIFIED historical arithmetic; no paid rerun"],
    ]
    root_table = f"Final code: `{SHA}`, measured 2026-09-28. The paid-call row is historical.\n\n"+table(["Claim","How it is shown","Evidence","Status"],evidence_rows)
    replace_block(ROOT/"README.md",root_table,write,"<!-- final-evidence:start -->","<!-- final-evidence:end -->")
    index_rows = [[claim,how,link.replace("docs/evidence/",""),
                   status+f" — 2026-09-28, {SHA}" if "historical" not in status else status+f" — arithmetic checked 2026-09-28, {SHA}"]
                  for claim,how,link,status in evidence_rows]
    index_rows.extend([
        ["Bounded model cap","SDK/server bounds, concurrent reservations, pricing and unknown usage",
         "[Accounting and D32](audit-2026-09-28.md#post-audit-resolution-2026-09-28)",f"VERIFIED by final gate — 2026-09-28, {SHA}"],
        ["Quickstart and viewer","Clean shell, example worker, completed run and authenticated viewer",
         f"[Transcript](raw/{SHA}/quickstart.log), [browser](raw/{SHA}/quickstart-ui.log)",f"VERIFIED — 2026-09-28, {SHA}"],
    ])
    replace_block(E/"README.md",table(["Claim","How it is shown","Evidence","Status"],index_rows),
                  write,"<!-- final-evidence:start -->","<!-- final-evidence:end -->")
    replace_block(E/"ui.md",section(f"""The clean-shell quickstart completed `quickstart-1` and the real browser displayed
its completed history and result 5. [Browser transcript](raw/{SHA}/quickstart-ui.log),
[inspected screenshot](raw/{SHA}/quickstart-ui.png). This checks the README path
on the final code; the wider historical filter, paging, mobile and privacy
campaign above was not repeated on the final code."""),write)
    replace_block(E/"cost.md",section("""No provider request was made. The verifier reads the retained recording:
14 input tokens and 4 output tokens, priced at $1/M and $5/M, give $0.000034.
The recorded reservation was $0.010000, and the second worker left one ledger row.
These are historical recording and arithmetic checks, not a new provider invoice
or final-code paid-call measurement. Current reservation behavior follows D32
and is checked by the final accounting tests."""),write)
