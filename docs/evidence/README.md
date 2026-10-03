# Evidence index

Final production source: **`904cb6c41da4f57ac0399d1524989288e3ededb1`**, measured
**2026-09-28**. Later commits add evidence and documentation; the engine and SDK
remain at that revision. [Raw data and reproduction](raw/README.md),
[historical archive manifest](raw/historical/manifest.json),
[numeric verifier](raw/verify-numbers.py).

The [independent audit](audit-2026-09-28.md) is preserved. Its appended resolution
records what the recovered data and final reruns establish. A passing finite test
supports its stated scope; it does not establish every possible execution.

<!-- final-evidence:start -->
| Claim | How it is shown | Evidence | Status |
| --- | --- | --- | --- |
| Crash recovery | Kill and restart the server; unchanged history prefix | [Crash](crash.md) | VERIFIED — 2026-09-28, 904cb6c |
| Key-honouring effects | 200,000 memory seeds; 500 PostgreSQL seeds; 25/26 mutants caught, 1 equivalent | [Lab](lab-delta1-2026-09-28.md) | VERIFIED within the fault model — 2026-09-28, 904cb6c |
| Durable waits | Worker-free signal wait; human approval after an engine-clock jump | [Waits](idle-wait.md) | VERIFIED locally — 2026-09-28, 904cb6c |
| Incompatible code blocks | Changed worker, explicit resume and patched old/new branches | [Replay](blocked.md) | VERIFIED — 2026-09-28, 904cb6c |
| Observed throughput | 11,700 workflows; 58,500 activity completions; 0 errors | [Load](load.md) | VERIFIED on the measured host — 2026-09-28, 904cb6c |
| Recorded model cost | Retained paid-call recording and ledger arithmetic | [Cost](cost.md) | VERIFIED historical arithmetic; no paid rerun — arithmetic checked 2026-09-28, 904cb6c |
| Bounded model cap | SDK/server bounds, concurrent reservations, pricing and unknown usage | [Accounting and D32](audit-2026-09-28.md#post-audit-resolution-2026-09-28) | VERIFIED by final gate — 2026-09-28, 904cb6c |
| Quickstart and viewer | Clean shell, example worker, completed run and authenticated viewer | [Transcript](raw/904cb6c/quickstart.log), [browser](raw/904cb6c/quickstart-ui.log) | VERIFIED — 2026-09-28, 904cb6c |
<!-- final-evidence:end -->

Recorded sessions (single runs, not recomputed by `raw/verify-numbers.py`):

| Claim | How it is shown | Evidence | Status |
| --- | --- | --- | --- |
| Tool calls through AgentOps Gate | Real Gate from its own Compose: allow, deny, approval, worker SIGKILLed during the approval wait | [Gate](gate-e2e.md), [log](raw/historical/agent/agent-delta1-gate-e2e.log) | VERIFIED — 2026-09-28 (historical recorded run) |
| Real agent work survives a worker crash | Two real Codex lanes at xhigh; worker SIGKILLed; activities reattach with one launch per lane | [Codex lanes](codex-lanes-local.md) | VERIFIED — 2026-09-28 (recorded) |
| Cloud deployment survives a server kill | AWS agentops via GitHub OIDC; `ecs stop-task` mid-run; replacement task; 16-event prefix intact; teardown inventory | [AWS](aws-2026-09-28.md) | VERIFIED once — 2026-09-28, image b49c413 (recorded; standing resources deleted, AWS-retained INACTIVE records noted) |

The final [quickstart transcript](raw/904cb6c/quickstart.log) runs from a clean shell.
Its [browser transcript](raw/904cb6c/quickstart-ui.log) shows the completed run in the
viewer. The original full viewer checks and screenshots remain [historical](ui.md).
The seven-day human test advances the engine Clock over PostgreSQL RPCs. It does
not substitute for a live multi-day Gate deployment.

## Historical provenance

All historical load directories now contain the individual latencies, system
snapshots, metadata, driver results, SQL audits and sampling SQL used by the tables.
The original paths remain in [load-results.json](load-results.json). JSONL is
compressed with gzip. Historical campaign summaries, mutation traces, gate logs,
queue controls and PostgreSQL attempts are archived with source and stored hashes.
The [credential scan](raw/historical-scan.log) found no credential-shaped values in
that selection.

The old large memory campaigns never emitted per-seed records. Their stdout proves
the reported seed totals and elapsed times; their original reports retain aggregate
fault and step counters. Those historical counters cannot now be independently
summed from individual observations. This limitation is retained in the audit
resolution. The final memory campaign includes every seed and supersedes those
aggregate claims for the current code.

The cost recording is retained and its arithmetic checked. No paid model request,
provider invoice check, AWS operation, push or deployment was part of this pass.
The static viewer is public; its data comes from authenticated same-origin RPCs.
Workflow payloads are opaque and can contain caller-supplied secrets (D33).

## Verification

```sh
PATH="$HOME/.local/bin:$PATH" GOTOOLCHAIN=go1.26.4 CAPSTAN_E2E_PORT=7799 make verify
python3 docs/evidence/raw/verify-numbers.py
```

The [merge-gate output](raw/904cb6c/verify.log) records generation checks, buf lint,
gofmt, Go vet, Go race tests, mandatory PostgreSQL tests, SDK typecheck and SDK tests.
The [number-gate output](raw/904cb6c/verify-numbers.log) records recomputation from
raw observations and comparison with all final numeric sections. The verifier also
checks historical load tables, mutation seeds, recorded arithmetic, source inventories,
archive integrity and historical test/timing logs. It does not relabel absent
historical per-seed observations as verified.

The shared PostgreSQL was never stopped, reset or restarted. Measurement commands
clean up their owned processes and databases. The ordinary test suite retains its
existing isolated test database prefixes and audit ports; the load and demo campaigns
use only their own databases and ports. [Final cleanup](raw/904cb6c/cleanup.log).
