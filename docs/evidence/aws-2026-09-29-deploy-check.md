# AWS deploy check — Node 24 action pins

**Passed.** [Deploy 36662235038](https://github.com/Aly700/capstan/actions/runs/36662235038)
deployed **dc92bff775e8c99036c6dd3369c14084bc795b9a** using all four new action versions.
The public workflow completed with result **5** and five activity completions.
All standing Capstan infrastructure was then destroyed. AWS retains deleted ECS metadata
and unconfirmed SNS subscriptions, listed below. No local worker or watchdog remains.

This is the September 29 Toronto session; all timestamps below are **September 30, 2026 UTC**.
No deployment fix, source change, dependency, Codex lane, or manual ECS stop-task was needed.
Nothing was pushed. The evidence commit stays local.

## Identity, baseline and foundation

Every local AWS operation used profile **agentops**, account **280517746513**, IAM user
**agentops-deployer**, region **us-east-1**. A local launcher removed every ambient
`AWS_*` variable before explicitly selecting that profile and region. It also set
`PATH=$HOME/.local/bin:$PATH` and `GOTOOLCHAIN=go1.26.4`.

The [baseline inventory](aws-2026-09-29-deploy-check-baseline.json) captured no standing
Capstan infrastructure and no active, inactive or deleting task definitions. It already
contained an old tagged cluster record, independently confirmed
[INACTIVE with zero counts](aws-2026-09-29-deploy-check-retained-before.json), and
[two PendingConfirmation SNS entries](aws-2026-09-29-deploy-check-subscriptions-before.json).

Before provisioning, captured the complete metadata, templates and resource lists of
TellerGithubOidcStack, AgentOpsGithubOidcStack, TellerBudgetStack, AgentOpsBudgetStack
and CDKToolkit, plus the complete imported OIDC-provider response, including thumbprints
and tags. The [before](aws-2026-09-29-deploy-check-shared-baseline.json) and
[after](aws-2026-09-29-deploy-check-shared-after.json) JSON files are byte-identical:
SHA-256 `17541767c40bfa94e80651bc7f0f18f2a422640575e35f5576a484d3cf1a4a78`.

Reviewed the CDK IAM diff and confirmed GitHub's immutable main-only OIDC subject.
From `infra/`, provisioned exactly the attempt-2 foundation and imported-provider context:

```sh
AWS_PROFILE=agentops AWS_REGION=us-east-1 npx cdk deploy \
  CapstanGithubOidc CapstanBudget CapstanData --profile agentops \
  --require-approval never --no-lookups -c imageTag=prepare \
  -c budgetEmail=affanyasir9@gmail.com -c existingGithubProvider=true \
  --outputs-file ../.lane/deploycheck-foundation-outputs.json
```

CapstanData included CapstanNetwork. The [foundation log](aws-2026-09-29-deploy-check-foundation.log)
and [inventory](aws-2026-09-29-deploy-check-foundation-inventory.json) record success.
Generated a new key using `capstan-server keygen deploy-check` into
`.lane/aws-worker-2026-09-29-deploy-check.key`, mode **0600**. Only its `name:sha256hex`
line was uploaded to `capstan/api-key-hashes` using `--secret-string file://...`.
The [provisioning receipt](aws-2026-09-29-deploy-check-secret-provisioning.json) contains
only response metadata. The Gate URL remained empty.

Set each variable with `gh variable set --repo Aly700/capstan --body ...`:

| Variable | Value |
| --- | --- |
| CAPSTAN_AWS_ACCOUNT_ID | 280517746513 |
| CAPSTAN_DEPLOY_ROLE_ARN | arn:aws:iam::280517746513:role/capstan-github-deploy |
| CAPSTAN_BUDGET_EMAIL | affanyasir9@gmail.com |

The [variable receipt](aws-2026-09-29-deploy-check-github-variables-before-deploy.json)
confirms all three. No secret was placed in GitHub variables.

## Verified SHA and complete Deploy log

[Verify 36661244021](https://github.com/Aly700/capstan/actions/runs/36661244021) passed for
the exact SHA before dispatch; its [receipt](aws-2026-09-29-deploy-check-verify.json)
includes both jobs. Re-read remote main immediately before:

```sh
gh workflow run deploy.yml --repo Aly700/capstan --ref main
gh run view 36662235038 --repo Aly700/capstan --log
```

Remote main equalled the recorded SHA at dispatch and at final verification. The
[Deploy receipt](aws-2026-09-29-deploy-check-deploy-run.json) binds the successful job
to that SHA. Read the complete **959-line [job log](aws-2026-09-29-deploy-check-deploy.log)**
before starting the proof. Its downloaded versions and resolved commits were:

| Observed action | Resolved commit |
| --- | --- |
| actions/checkout@v7 | 3d3c42e5aac5ba805825da76410c181273ba90b1 |
| actions/setup-node@v7 | 820762786026740c76f36085b0efc47a31fe5020 |
| docker/setup-buildx-action@v4 | f87e5991a6d7451dcb8d9637bfbc97413f497069 |
| aws-actions/configure-aws-credentials@v6 | e1253824e5c10ff9df46874f81ed3ec929e19cfd |

All four steps succeeded. Their exact-commit `action.yml` files declare `node24`;
the [log-check receipt](aws-2026-09-29-deploy-check-log-check.json) links each source.
The project tooling installed by setup-node was separately **Node 26.10.0**.

The credentials step shows `allowed-account-ids: 280517746513`, then
`Assuming role with OIDC` and successful authentication as
`AROAUCUBJD5I6PIINMBSK:capstan-36662235038`. The supplied allowlist plus successful
credentials-step conclusion establish acceptance; the action emits no separate
"allowlist accepted" line. There was no unsupported-input warning.

The log records the ARM64 build and manifest push, successful CDK deployment of
Data/Budget/Service, two `ok` responses, and:

```text
Smoke passed: https://dqofm24886.execute-api.us-east-1.amazonaws.com
```

The [ECR receipt](aws-2026-09-29-deploy-check-image.json) records image tag equal to
the full SHA, 5,448,204 bytes, and manifest-list digest
`sha256:69679db1999c46fff9c6b0bf6c387367ce35e2c62dfaa68f21844d9b5aa0267b`.

## Real workflow and timings

Started `examples/evidence/worker.ts` locally with `examples/evidence/load.ts`, the
new key and the execute-api URL above, using the existing Evidence process/RPC helpers.
The one-off launcher was `node .lane/deploycheck-proof.mjs <execute-api-url>` under
the clean environment. It did not call the helpers' local server/database setup.
Worker workflow/activity concurrency was one. Authenticated StartRun, DescribeRun
and GetHistory requests went through the public HTTPS address.

Run **deploy-check-1790737519531**, workflow **loadFive**, completed with result **5**.
Its 30-event [history and proof receipt](aws-2026-09-29-deploy-check-proof.json) contains
activity completions at event IDs **6, 11, 16, 21, 26**, returning **1, 2, 3, 4, 5**,
all on attempt 1. Event 30 completes the run. Worker PID **43691** then exited and
was independently checked absent. Its [log](aws-2026-09-29-deploy-check-worker.log)
contains the readiness receipt. No model or Gate calls were made.

| Event | UTC |
| --- | --- |
| Baseline inventory captured | 02:47:30.697 |
| Conservative clock / watchdog armed | 02:49:30.665 / 02:49:30.705 |
| First stack created: CapstanNetwork | 02:49:56.627 |
| Verify completed successfully | 02:50:43 |
| Foundation complete: CapstanData | 02:56:56.291 |
| Deploy dispatched | 02:57:47.476 |
| OIDC authenticated | 02:58:32.509 |
| ECR image pushed | 02:59:32.001 |
| CapstanService CREATE_COMPLETE | 03:04:02.745 |
| Both smoke checks passed | 03:04:08.875 |
| Deploy job completed successfully | 03:04:18 |
| Run started / closed on server | 03:05:20.476 / 03:05:21.637 |
| Proof worker cleanup complete | 03:05:21.970 |
| Runbook destruction began | 03:06:22.788 |
| Last stack DELETE_COMPLETE | 03:14:01.174 |
| Final standing-resource verification | 03:15:23.327 |
| Watchdog stopped after verification | 03:15:23.337 |

The session-local watchdog reused `scripts/demo-codex-lanes-aws-watchdog.mjs`, changing
only its local import/session path and timing: warning at 60 minutes, early teardown
at **04:19:30.665** (90 minutes), hard deadline **04:49:30.665** (two hours).
It started before the first resource and kept that original, slightly earlier clock.
It remained armed through verification and never needed to trigger teardown.
See the [watchdog log](aws-2026-09-29-deploy-check-watchdog.log) and
[final session](aws-2026-09-29-deploy-check-session.json).

## Ordered teardown and final inventory

Reused `scripts/demo-codex-lanes-aws-cleanup.mjs` with this session and its pre-deploy
baseline. The [cleanup log](aws-2026-09-29-deploy-check-cleanup.log) records these CDK
destroy groups, all with `--force --profile agentops --no-lookups`, `imageTag=cleanup`,
the same budget email and `existingGithubProvider=true`:

1. CapstanService
2. CapstanData
3. CapstanNetwork CapstanBudget
4. CapstanGithubOidc

CDK rechecked already-absent dependent stacks without recreating them. The
[stack lifecycle receipts](aws-2026-09-29-deploy-check-stack-lifecycle.json) confirm:

| Stack | DELETE_COMPLETE (UTC) |
| --- | --- |
| CapstanService | 03:09:52.785 |
| CapstanData | 03:13:02.447 |
| CapstanBudget | 03:13:14.074 |
| CapstanNetwork | 03:13:40.373 |
| CapstanGithubOidc | 03:14:01.174 |

CloudFormation deregistered **capstan-server:3**, independently observed
[INACTIVE](aws-2026-09-29-deploy-check-revision-before-delete.json). The taskdefs helper
checked the empty definition baseline, account/region/family, registration time and
Project tag, then submitted [deletion](aws-2026-09-29-deploy-check-task-definition-deletions.json).

The cleanup helper successfully deleted all three GitHub variables but its immediate
listing failed `deployment variables remain`. This failure is preserved in the log.
Subsequent [CLI](aws-2026-09-29-deploy-check-github-variables-after.json) and
[REST](aws-2026-09-29-deploy-check-github-variables-api-after.json) listings returned
empty without another deletion, consistent with delayed visibility. No source fix or
assertion relaxation was applied. Ran the inventory helper separately with the saved
pre-teardown VPC inventory, then performed the direct final checks.

The [full inventory](aws-2026-09-29-deploy-check-teardown.json) has **zero API errors**.
It includes commands and unabridged results, including untagged interfaces/endpoints
in former VPC **vpc-050fe83e31c16ed24**. The
[final verification](aws-2026-09-29-deploy-check-final-verification.json) confirms:

| Category | Remaining |
| --- | --- |
| Capstan stacks; discoverable ECS clusters; active/inactive task definitions | 0 |
| RDS instances, subnet groups, snapshots; ECR repositories; secrets | 0 |
| HTTP APIs, VPC links; Cloud Map namespaces/services; hosted zones | 0 |
| VPCs, security groups; tagged/named/known-VPC ENIs and endpoints | 0 |
| Log groups, alarms, SNS topics, Lambda functions, scheduled rules | 0 |
| Capstan roles, policies and budgets; GitHub variables | 0 |
| Unrelated stacks and imported OIDC provider | Unchanged, byte-identical snapshots |

**Retained AWS records are not an empty raw inventory:**

- `arn:aws:ecs:us-east-1:280517746513:cluster/capstan`: **INACTIVE**;
  zero running/pending tasks, container instances and active services.
- `arn:aws:ecs:us-east-1:280517746513:service/capstan/capstan-server`: **INACTIVE**;
  desired/running/pending counts zero, also zero in its retained deployment.
- `arn:aws:ecs:us-east-1:280517746513:task-definition/capstan-server:3`:
  **DELETE_IN_PROGRESS**, deletion request accepted. No active or inactive revision remains.
- **Three PendingConfirmation SNS entries** for `affanyasir9@gmail.com`, topic
  `arn:aws:sns:us-east-1:280517746513:capstan-alerts`: two were in the baseline and
  one was added by this deployment. The topic itself is deleted.

AWS documents retention of [inactive clusters](https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_DeleteCluster.html),
[task-definition deletion delays](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-definition-state.html)
and [unconfirmed subscriptions](https://docs.aws.amazon.com/sns/latest/dg/sns-delete-subscription-topic.html).
No tags were removed to conceal records. As in attempt 2, the session explicitly records
`standingResourcesEmpty: true`, `rawInventoryEmpty: false`, and the legacy helper's
strict `teardownVerified: false`. The separate final verification establishes the
runbook's zero-recurring-workload-cost boundary. Only then was watchdog PID **25722**
stopped; it and worker PID **43691** are absent. AWS metadata expiry needs no local process.

## Hours, cost and validation

First stack creation to last stack deletion used **0.401263 hours** (24m 4.547s).
The conservative clock through deletion used **0.408475 hours**; including final
inventory verification gives **0.431295 hours** (25m 52.662s), below both deadlines.

The [cost calculation](aws-2026-09-29-deploy-check-cost.json) applies the full
[runbook standing rate](../runbook.md#cost-table) to the larger interval:
`0.431295 × $0.03782 = $0.016312`. Add $0.02 for two
[Cost Explorer requests](https://aws.amazon.com/aws-cost-management/aws-cost-explorer/pricing/)
and a conservative $0.025 allowance for 10,000 HTTP calls, 10,000 discovery calls and
0.01 GB logs: **about $0.07**, plus small secret API, CPU-credit and transfer charges.
Reserve **$0.60**, including the $0.50 hosted-zone step and miscellaneous usage.
The single private zone was deleted within 12 hours, qualifying for the documented
[Route 53 testing waiver](https://aws.amazon.com/route53/pricing/).

This is an estimate, not a settled bill. Account month-to-date estimated UnblendedCost
was **$0.1472758848** both [before](aws-2026-09-29-deploy-check-billing-before.json) and
[after](aws-2026-09-29-deploy-check-billing-after.json); billing visibility lags usage.

Validation: green Verify on the deployed SHA; local infra typecheck and **26 tests**;
**18 cleanup/watchdog/taskdefs tests**; successful Deploy; real public workflow/history;
complete teardown inventory and byte comparison. Local shared PostgreSQL was not used.
All evidence was scanned against the actual key, hash line and raw hash: none occur.
Log exports retain every line with trailing whitespace normalized; their source/export
hashes are in the [log manifest](aws-2026-09-29-deploy-check-log-exports.json).
No frozen contract or source file changed. No push or rerun is required for this check.

## Addendum — 2026-10-01 inventory

Read-only re-inventory at **2026-10-01 13:07 UTC**, about 34 hours after this session's last
stack deletion (03:14 UTC, 2026-09-30), with the same profile, account and categories:
[inventory](aws-2026-10-01-final-inventory.json) (`errors={}`) and
[SNS subscriptions](aws-2026-10-01-sns-subscriptions.json) (endpoint redacted).

| Category | Result | Cost |
| --- | --- | --- |
| CloudFormation | No Capstan stacks; the five unrelated stacks only | none |
| ECS | `cluster/capstan` record INACTIVE, 0 running tasks, 0 services; no task definitions in any status | none (retained metadata) |
| RDS, ECR, Secrets Manager, Cloud Map, Route 53, API Gateway, VPC/ENIs, log groups, budgets | Empty | none |
| SNS | `capstan-alerts` topic gone. One PendingConfirmation subscription remains (this session's). The two from 2026-09-28 have expired. | none |

Nothing billable remains, so nothing was deleted. The remaining subscription was created at
about 03:00 UTC on 2026-09-30, so it should expire on the same schedule as the earlier two,
around 03:00 UTC on 2026-10-02. It cannot be deleted manually. The INACTIVE cluster record is
retained by AWS and does not bill.
