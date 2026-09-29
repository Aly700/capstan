# Polish verification — 2026-09-28

Reviewed source: `0a80a08f7a0a3bb88afd4221e1fe9b085c48afb1` on `lane/polish`.
These observations check the ten readability refactors and the viewer fix. They
do not remeasure the historical throughput, 200,000-seed campaign or AWS session.
The earlier measurements and generated tables still describe `904cb6c`.

- [Merge gate](verify.txt): `make verify`, exit 0; 438 SDK tests passed, five
  existing opt-in live Gate/provider tests skipped. Mandatory PostgreSQL tests ran.
- [Mutation baseline](mutations-before/report.md) at `c85928e` and
  [refactored mutations](mutations-after/report.md) at `e9e1fb3`: 2,000 seeds per
  baseline/equivalent run, 25 caught, M003 equivalent, zero invalid patches.
  Every first failing seed and check is unchanged (protobuf diagnostic whitespace
  can differ). Only M003 and M016 needed patch refreshes, removing precisely the
  same started-event and lease-expiry checks as before.
- Each mutation directory retains the original `results.json` and compressed
  `baseline.jsonl` / `M001.jsonl` … `M026.jsonl` output. Reports and JSON retain
  the original local log paths; their contents are archived alongside them here.
- [Browser regression failure](ui-red.txt) shows the old event-14 heading failing
  the event-5 assertion. [Passing browser checks](ui-check.json) cover the heading,
  timeline link, loading another history page, opening the event in a new tab,
  old-history fallback, resume command, mobile layout, authentication and existing
  viewer checks. The [link regression failure](ui-link-red.txt) records the initial
  event-only fragment; the final URL carries both the run and the event.
- The refreshed [blocked screenshot](../../ui-blocked.png) was inspected at
  1280px desktop; a 390px mobile capture was also inspected and retained locally
  in `.lane/polish-ui-mobile.png`. No overflow, overlap or browser errors were seen.

Reproduce without replacing these observations:

```sh
PATH="$HOME/.local/bin:$PATH" GOTOOLCHAIN=go1.26.4 CAPSTAN_E2E_PORT=7799 make verify
PATH="$HOME/.local/bin:$PATH" GOTOOLCHAIN=go1.26.4 go run ./cmd/capstan-lab mutate \
  -catalogue 'internal/lab/mutants/*.patch' -seeds 2000 \
  -logs .lane/polish-repeat -out .lane/polish-repeat.md
python3 docs/evidence/raw/verify-numbers.py
```

The verifier pins the reviewed source separately from historical source. Its
explicit change inventory also includes the pre-existing `71601cf` PostgreSQL
test-cleanup fix, which already made the old source-equality check fail before
polish. Historical patch hashes and screenshot dimensions are checked against
their original Git blobs; current patch hashes and the inspected screenshot are
checked separately. No old receipt or original auditor row was rewritten.

Commands ran locally with Go 1.26.4. UTC timestamps on the receipts fall on
2026-09-29; the session date in America/Toronto was 2026-09-28. No AWS, Gate or
paid-provider call was made. Owned test databases and children were cleaned up;
the shared PostgreSQL was never stopped, reset or restarted. No dependency changed.
