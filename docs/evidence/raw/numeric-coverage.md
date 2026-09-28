# What the numeric gate checks

`verify-numbers.py` uses only Python's standard library. It reads committed files;
it does not need PostgreSQL, a provider, a browser or the old worktrees. `--write`
regenerates the final numeric sections; normal invocation fails on a mismatch.

| Published group | Independent input and computation |
| --- | --- |
| Historical load tables and ranges | Every `runs.jsonl.gz`: unique run IDs, counts, activity sums and nearest-rank latencies. Driver elapsed time gives rates. SQL audits independently agree with completed runs and activity events. |
| Historical and final CPU, queue and lock claims | Every `system.jsonl.gz` plus metadata: accumulated CPU-time deltas, sampling windows, maxima, backend observations, other active databases and database-counter deltas. Lock sample and wait-event counts are summed from the individual records. |
| Historical large lab reports | Original stdout verifies reported seed counts and elapsed times. Original Git report snapshots preserve all aggregate counters. Historical per-seed counts cannot be recovered; those portions remain explicitly FAILED in the audit resolution. |
| Final large lab | Every seed, scenario, outcome, fault list, Gate response list, actor-step count, root count and scheduled transaction count is retained. The verifier checks the complete contiguous seed range and sums the records before comparing the CLI report and prose. |
| Mutation scores and first failures | The baseline and every mutant's Go JSON event stream establish passes, failures and seed order. First failing seed and diagnostic match the result report and the published table. Current patch SHA-256 values must match the catalogue. Equivalent status still requires the invariant argument in the report. |
| PostgreSQL campaign | Every final seed's counters sum to the command's aggregate. Historical logs retain all quoted early, successful and time-limited attempts. Rounded prose durations are checked against exact raw durations. |
| Historical gate inventories and controls | Recovered test output establishes the reported passed/skipped counts and durations. Queue attempts and sampled transaction controls come from the original red/green logs. The final verbose race test enumerates the operation/order matrix. |
| Crash, blocked and idle demos | Raw histories prove prefix equality and activity/timer counts. CLI objects prove old/new results and the blocked mismatch. Repeated SQL/process snapshots establish the idle checks. No recording is regenerated. |
| Cost | The retained cast contains the ledger row. The token counts reproduce the published micro-dollar arithmetic. This does not verify a provider invoice or a new paid request. |
| Source inventories and audit ledger | Fixture JSON, exported workflows, Go registrations and transaction wrappers establish the source counts. Original audit ledger statuses are counted; the original report must remain a byte-for-byte prefix of the appended report. Every originally FAILED row must appear in the resolution. |
| README quickstart | The published shell commands match the executed command snapshot. Clean-shell output shows completion and the viewer endpoint. The browser transcript shows the final payload. |
| Final prose and tables | Every generated numeric block in the README and evidence reports is regenerated from the checked observations and compared byte-for-byte. Editing a quoted number fails the gate. |
| Archive integrity | Each historical file's stored hash, decompressed hash and byte counts must match its manifest. This establishes copy integrity; it does not turn an aggregate into an individual observation. |

Dates, source SHAs, command options, test names, protocol IDs and configured limits
are provenance or source definitions, not measured results. Timings are observations
of their own run. The gate does not claim that rerunning code must reproduce an old
wall-clock duration. The historical audit's broader security and runtime claims
retain their own deciding commands and raw artifacts; this gate preserves that
ledger rather than reclassifying those claims from arithmetic alone.
