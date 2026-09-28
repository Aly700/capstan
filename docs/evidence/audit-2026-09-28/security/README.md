# Security and SDK audit evidence — 2026-09-28

These are locally executed command outputs, including failing regressions captured before fixes. All credential values in failure output are deliberately fake audit canaries. Six synthetic database URL tokens in the two failing diagnostic logs are replaced with `[SYNTHETIC_DATABASE_URL_CANARY]` for publication; failure assertions and all other output are retained. Unmasked originals remain in the ignored local `.lane/` directory. No provider request or AWS operation was made. The real-process privacy probe uses the shared PostgreSQL only through a fresh `capstan_audit_*` database, ports 7630/7631, and cleans up its own resources.

## Commands and results

All Go commands use `PATH="$HOME/.local/bin:$PATH" GOTOOLCHAIN=go1.26.4`. SDK commands use `PATH="$HOME/.local/bin:$PATH"`.

| ID | Command | Decisive output / file |
| --- | --- | --- |
| C1 | `go test ./internal/server -run TestAuditMetricsRequiresOneValidKey -count=1 -v` before fix | Seven missing/invalid/duplicate header variants returned `status=200`; **FAIL**, [red](audit-metrics-red.log). |
| C2 | `go test -race ./internal/server -run 'TestAuditMetrics\|TestRPCMetrics\|TestLoopAndWaitingMetrics\|TestEveryRPC' -count=1 -v` | **PASS**, including all 19 RPCs, [green](audit-metrics-green.log). |
| C3 | `go test ./internal/server -run TestAuditConfiguredCredentials -count=1 -v` before fix | All five configured credential canaries leaked through logs or mapped RPC errors; **FAIL**, [red](audit-rpc-privacy-red.log). |
| C4 | `go test -race ./internal/server -run 'TestAuditConfiguredCredentials\|TestErrorMappingAndInternalPrivacy\|TestLoops\|TestLoop' -count=1 -v` | **PASS**, [green](audit-rpc-privacy-green.log). |
| C5 | `npm --prefix sdk test -- test/audit-sandbox.test.ts` before fix | `Date()` differed by timezone; epoch hour `0` versus `19`; parsed/constructed dates differed by five hours. **4 failed / 9 passed**, [red](audit-sandbox-red.log). |
| C6 | `npm --prefix sdk test -- test/audit-sandbox.test.ts test/sandbox.test.ts test/agent-replay.test.ts test/conformance.test.ts` | **138 passed**, [green](audit-sandbox-green.log). A boxed-string date found during repair also failed before its correction: [boxed-string red](audit-sandbox-boxed-red.log). |
| C7 | `npm --prefix sdk test -- test/audit-privacy.test.ts` before fix | Activity failure message, stack, nested details, cause, and poll log contained fake API/Gate/provider/database credentials. **FAIL**, [red](audit-worker-privacy-red.log). |
| C8 | `npm --prefix sdk test -- test/audit-privacy.test.ts test/codecs.test.ts test/worker.test.ts` | **49 passed**, [green](audit-worker-privacy-green.log). |
| C9 | `go test -race ./internal/server -run TestAuditLiveAuthentication -count=1 -v` | `19 RPCs × 6 invalid-header cases rejected; metrics authenticated; viewer data RPCs guarded; history, logs, UI, metrics and three CLI commands contained no canaries`; **PASS**, [output](audit-live-security.log). Uses a real server and pgstore on port 7630. |
| C10 | `npm --prefix sdk test -- test/audit-gate.test.ts test/agent-replay.test.ts test/agent-worker.test.ts` | **49 passed**, including new eight-case D29 matrix, [output](audit-d29.log). Gate HTTP responses are local test doubles; replay and worker implementations are real. |
| C11 | `go test -race ./cmd/capstan-server ./internal/auth ./internal/config ./internal/server ./internal/gate -count=1` | Five packages **PASS**, [output](audit-security-go.log). |
| C12 | `npm --prefix sdk test` | **426 passed / 11 skipped**, [output](audit-sdk-all.log). Skipped suites require opt-in process/Gate/provider flags; this command is not a replacement for root's `make verify`. |
| C13 | `npm --prefix sdk run typecheck` | `tsc --noEmit`, exit **0**, [output](audit-sdk-typecheck.log). |
| C14 | `go test ./internal/auth -run 'TestParse\|TestNewKey' -count=1 -v`; `sed -n '37,55p' internal/auth/auth.go` | Tests **PASS**; source hashes input and invokes `subtle.ConstantTimeCompare` for every stored hash without early return, [output](auth-comparison.log). This verifies the comparison primitive and control flow, not a statistical end-to-end timing guarantee. |
| C15 | `rg -n 'node:vm\|not a security boundary' README.md` | Explicit `node:vm` warning, [output](vm-readme.log). |
| C16 | `go test ./cmd/capstan-server -run TestAuditStartupDatabaseErrors -count=1 -v` | Both actual pgx invalid-sslmode startup paths **PASS**, [output](audit-db-privacy-red.log). The filename was allocated before running; this was a negative hypothesis and never failed. No startup defect was asserted. |
| C17 | `node .lane/audit-opaque-privacy.mjs` before and after worker diagnostic fix | Real custom activity throws fake provider key. History, DescribeRun, CLI, UI data-source checks changed **true → false**: [before](audit-opaque-privacy.log), [after](audit-opaque-privacy-green.log). |
| C18 | `node .lane/audit-opaque-privacy.mjs` final probe | Error paths have all checks **false**. A second custom activity deliberately returns the fake credential as a successful opaque result: history and DescribeRun checks **true**, [output](audit-opaque-privacy-final.log). This remains a limit of the absolute secret claim. |

## Findings

| Severity | Finding, reproduction and root cause | Resolution |
| --- | --- | --- |
| High | Worker activity exceptions could persist configured credentials into history and expose them to authenticated DescribeRun, CLI and viewer clients. C7/C17. `sdk/src/internal/failure.ts:19` previously forwarded message/stack/details/cause strings verbatim; `sdk/src/worker/activities.ts:107` reported them; `sdk/src/worker/pollers.ts:23` and `:34` logged raw RPC errors. | `28684fb` adds diagnostic-only redaction for configured worker/environment credentials, including database password, causes/details/stacks and logs. C8/C17 green. |
| High | A workflow's Date operations consulted the worker timezone, so honest date operations changed after migration to another timezone. C5. `sdk/src/sandbox/globals.ts:69` replaces the prior native parse delegation; Date.prototype's native local getters/formatters were retained before the fix. | `511d905` pins Date operations to UTC, normalizes offset-free ISO dates, rejects ambiguous date strings and locale date formatting. C6 green. Existing workflows that depended on local-time values need a deliberate patch or the old code; this is a behavior correction. |
| Medium | `/metrics` exposed operational data without a valid key. C1. `internal/server/handlers.go:71` mounted metrics directly. | `2ce601e` adds an authenticated HTTP wrapper and malformed-header regression. C2/C9 green. |
| Medium | Dependency error messages could leak configured credentials into server logs and mapped RPC/CLI errors. C3. `internal/server/errors.go:32` and `:35`, `internal/server/loops.go:58` previously forwarded raw error strings. | `414b206` redacts configured secret strings and the authenticated request key before emitting diagnostics. C4 green. |
| Medium | The absolute promise that secrets never enter history is false for trusted application payloads: returning the fake provider credential as an activity result writes it into history and DescribeRun. C18. `sdk/src/worker/activities.ts:97` serializes the result; `:108` reports the opaque payload. | **Unfixed**. Server-side inspection would violate the opaque payload contract and cannot reliably identify every secret. Proposed approach: keep credential references outside workflow payloads; add an optional SDK payload policy with an explicit rejection contract if the owner wants stronger prevention. The evidence must retain this limitation. |

The server CLI's `keygen` deliberately prints a newly generated key once, as the plan requires; `TestKeyCommandsAndUsage` verifies that behavior. Privacy checks for normal operational CLI commands exclude that explicit provisioning output.

## Claim ledger

Status is the final observed state. The findings table retains failures discovered and repaired; a final VERIFIED status does not erase the original failure.

| Claim | Status | Deciding command/output |
| --- | --- | --- |
| Spec §12: named, hashed keys authenticate every RPC | VERIFIED | C9: 19/19 proto RPCs, 114 invalid-header requests rejected; C14 key parsing tests. |
| Invalid, missing, duplicate and unknown-name-prefixed bearer values are rejected | VERIFIED | C1/C2/C9/C14. |
| Secret hash comparison uses a constant-time primitive | VERIFIED | C14; source line 51 is `subtle.ConstantTimeCompare`, loop does not stop at a match. |
| `/metrics` requires a key | VERIFIED | C1 red, C2/C9 green after `2ce601e`. |
| All run data used by `/ui/` requires a key | VERIFIED | C9 exercises the same ListRuns/DescribeRun/GetHistory RPCs; only credential-free static assets are public. |
| Config validation rejects invalid settings without echoing private values | VERIFIED | C11 config tests; C16 real pgx startup checks. |
| Server logs and RPC errors do not echo configured credentials from dependency errors | VERIFIED | C3 red, C4 green after `414b206`. |
| Worker diagnostics do not record configured credentials in failures, nested details/causes/stacks or logs | VERIFIED | C7 red, C8/C17 green after `28684fb`. |
| Normal run history, logs, metrics, UI assets and operational CLI output contain no worker/server config canaries | VERIFIED | C9 real pgstore plus CLI; C17 actual worker exception path. |
| Secrets can never enter history or returned run data through any application code | FAILED | C18: deliberately successful opaque activity result still contains the fake provider credential; unfixed. |
| Gate built-in drops echoed request/error data and does not leak its configured API key | VERIFIED | C10 real worker/local HTTP tests, including response echo, error bodies, malformed responses and redirects. |
| Spec §4/§8: SDK-backed current time and random are stable in the VM | VERIFIED | C6 constructor/prototype probes, sandbox tests and conformance recorded time/random fixtures. |
| Honest Date code cannot observe worker-local timezone | VERIFIED | C5 red, C6 green after `511d905`. |
| Workflows cannot access ordinary network/filesystem/environment/timer globals or Node builtin imports | VERIFIED | C6 forbidden-global/import tests; computed dynamic imports rejected. |
| SDK inputs, objects, promises, errors and prototype chains do not expose a host constructor in tested paths | VERIFIED | C6 eight constructor/realm probes plus existing nested failure/codec tests. |
| README plainly states node:vm is not a boundary for untrusted code | VERIFIED | C15: explicit warning and trusted-code requirement. |
| D29: executed tool name and argument bytes match the recorded Gate proposal | VERIFIED | C10: ALLOW and approved REQUIRE_APPROVAL paths dispatch exact name and bytes. |
| D29: changed name/arguments block before dispatch, and catches cannot swallow the mismatch | VERIFIED | C10 new eight-case audit matrix and existing worker HISTORY_MISMATCH tests. |
| D29: legacy results recover only the historical proposal; canonical argument reorder is safe | VERIFIED | C10 existing pre-D29/reordered-argument tests. |
| D14: added-command mismatch identifies TaskCompleted position | VERIFIED | C6 conformance 030 plus C12 replay/match tests. |
| D15: activation time is set before external event continuations resolve | VERIFIED | C6 conformance 022 and C12 replay tests. |
| D16: failure metadata survives SDK encoding via details | VERIFIED | C8 codec round trips and C6 conformance 041–043. |
| D17: unrecordable side effects fail the task even when caught | VERIFIED | C12 replay tests, including throws/non-JSON/command emission. |
| D20: JavaScript continuation-depth fixture is replay-safe for the same code | VERIFIED | C6 fixture 054. This does not promise Promise.race winner portability across code changes. |
| Worker concurrency, stale results and shutdown follow the SDK plan | VERIFIED | C8/C12 worker suite; real-process shutdown in C17/C18. |
| Bundle cache changes when imported source changes | VERIFIED | C6 dependency-edit cache test. |
| Protection against all malicious node:vm escape attempts | NOT CHECKED | No such product claim is accepted; representative constructor probes passed, but node:vm is explicitly not an isolation boundary. |
| Real AgentOps Gate deployment integration in this subtask | NOT CHECKED | C10 uses local HTTP doubles and recorded Gate histories. No Gate deployment was started or edited. |
| AWS Secrets Manager/task-role behavior | NOT CHECKED | Outside audit authorization; no AWS or deployment state accessed. |

Ledger counts: **25 VERIFIED / 1 FAILED / 3 NOT CHECKED**.

## Re-running the process privacy probe

Copy the three stored source snapshots into the existing worktree's `.lane/`, then run from the repository root:

```sh
cp docs/evidence/audit-2026-09-28/security/audit-opaque-privacy.mjs.txt .lane/audit-opaque-privacy.mjs
cp docs/evidence/audit-2026-09-28/security/audit-opaque-worker.mts.txt .lane/audit-opaque-worker.mts
cp docs/evidence/audit-2026-09-28/security/audit-opaque-workflows.ts.txt .lane/audit-opaque-workflows.ts
PATH="$HOME/.local/bin:$PATH" GOTOOLCHAIN=go1.26.4 node .lane/audit-opaque-privacy.mjs
```

The `.txt` suffix prevents the evidence snapshots from joining the build. [cleanup.log](cleanup.log) records zero owned privacy databases and no remaining listeners on 7630/7631 after the probe. Local helper tests/real-runtime checks are committed in `1b6b5bb`. No dependencies were added.
