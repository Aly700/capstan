# Capstan deployment runbook

This runbook describes the deployment design and operator procedures. The integrated
server runs locally against PostgreSQL; the [independent audit](evidence/audit-2026-09-28.md)
exercises that implementation. AWS deployment, identity, prices, alarms and destruction
were outside the local audit's permitted scope. Obtain current deployment evidence from
the deployment lane before treating those procedures as validated.

The documented deployment uses one server task. The local audit's successful two-process
timer test does not establish general multiple-server support.

## Shape and cost boundary

The target is `us-east-1`: two public subnets, no NAT gateway, no load balancer or VPC
endpoints. A single ARM64 Fargate task uses 0.25 vCPU / 0.5 GB and a public IP for HTTPS
egress to ECR, Secrets Manager, CloudWatch and the Gate. Its security group accepts TCP
7233 **only** from the HTTP API VPC link's security group. PostgreSQL accepts 5432 only
from the task group; it is encrypted, single-AZ `db.t4g.micro`, 20 GB gp3 and not public.
The task and database occupy the first AZ; the VPC link uses both subnets.

Ingress is HTTPS on the execute-api domain → HTTP API → VPC link → Cloud Map SRV
discovery → the task's private IP:7233. The integration removes any stage prefix and
uses HTTP/1.1. Workers use Connect over HTTP/1.1 for HTTPS addresses. Server long polls
are **20 s**, leaving room below the gateway's **30 s** integration timeout. Health,
readiness and the two RPC service paths are exposed; `/metrics` is private. Every RPC
still requires a server API key. See [AWS private integration requirements](https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-develop-integrations-private.html).

The $10 budget is an account-wide monthly alarm, **not a hard spending cap**. Continuous
operation is approximately **$28/month before traffic**, so the ceiling requires short
deploy/prove/destroy sessions. Reserve four hours for a proof, set a teardown reminder
before starting, and destroy even after a failed deployment. At the 50% alert, stop
starting proofs and reconcile actual spend; leave the remaining $5 for billing delay
and cleanup. The 90% and 100% alerts require immediate cleanup and investigation.
The server's separate $2/day model cap does not cover AWS or the Gate's infrastructure.

## Local validation (no AWS identity needed)

Use Go 1.26.4, Node 26, buf 1.73.0, and the protoc plugins pinned by `go.mod`.
The existing Colima PostgreSQL remains at 127.0.0.1:55432. Do not restart it or run
Compose `down`; all lanes share it. These checks do not start another database:

```sh
make verify
docker build -t capstan-server:lane .
docker run --rm capstan-server:lane
# Without required configuration: exit 1; this is not a configured-server smoke test.
cd infra
npm ci
npm run typecheck
npm test
AWS_EC2_METADATA_DISABLED=true AWS_SHARED_CREDENTIALS_FILE=/dev/null AWS_CONFIG_FILE=/dev/null \
  npx cdk synth --no-lookups -c imageTag=test -c budgetEmail=test@example.com
```

Run offline checks in a shell without exported AWS credentials. Synthesis uses no AWS
lookups. Templates and the task-count Lambda code are inline, with no CDK asset bucket
or bootstrap roles. `LegacyStackSynthesizer` lets deployment pass a specific
CloudFormation execution role. Tests also enforce the 51,200-byte inline template limit.
See [CDK synthesis options](https://docs.aws.amazon.com/cdk/v2/guide/customize-synth.html).

For local server use, the optional Compose service is under profile
`full`. The defaults point at `postgres:5432` inside Compose; a host-side database URL
from `.env` must be adjusted before using that service. On this shared machine, continue
using the existing database and run the server directly; do not bring up this worktree's
Compose project against port 55432.

## First deployment: owner provisions the foundation

Use an owner-controlled SSO session in the intended account for this section. There are
no static AWS keys or secret values in GitHub. Record the account, region, budget email
and whether this account already has the GitHub OIDC provider.

From `infra/`, after setting `BUDGET_EMAIL` to the real notification address:

```sh
npx cdk deploy CapstanGithubOidc CapstanBudget CapstanData \
  -c imageTag=prepare -c "budgetEmail=$BUDGET_EMAIL"
```

`CapstanData` includes its `CapstanNetwork` dependency. These stacks create the ECR
repository, GitHub identity, execution role and workload permissions boundary, budget,
database and four secrets. They do not start ECS. If the GitHub provider already exists,
add `-c existingGithubProvider=true`; that imports it and leaves it owner-managed. Keep
this choice for later foundation operations. Review the IAM diff before approving.

The GitHub role accepts only audience `sts.amazonaws.com` and subject
`repo:Aly700/capstan:ref:refs/heads/main`. It can push/read image metadata in
`capstan-server`, deploy the three workload stacks and pass only `capstan-cloudformation`
to CloudFormation. It cannot change its own foundation stack, read secrets directly,
destroy stacks or assume other roles. The execution role names its resource families
and required actions; create/list APIs that cannot name a resource use scoped exceptions.
All three workload roles require the foundation's fixed permissions boundary, so an
inline role policy cannot broaden their runtime access. The task role itself has no
AWS permissions; the ECS execution role fetches the three injected secrets.

`CapstanNetwork` is also owner-managed. Workload synthesis imports its named subnet,
AZ and security-group exports; the GitHub execution role has no EC2 mutation actions.
Network changes require an owner-reviewed deployment. This avoids granting broad
network access or relying on tag-on-create conditions that CloudFormation does not
support for security groups; see [AWS VPC policy limitations](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-policy-examples.html).

Prepare secrets **in Secrets Manager**, never in context, GitHub variables, command-line
arguments, CloudFormation outputs or committed files:

1. `capstan/database-credentials` is generated with a URL-safe password. RDS consumes it.
   `capstan/database-url` is assembled by CloudFormation with a secret reference and
   `sslmode=require`; ECS injects the complete URL as `CAPSTAN_DATABASE_URL`.
2. Generate a client key with the integrated `capstan-server keygen <name>` command on
   a trusted machine. Give its cleartext key to the worker through that worker's secret
   store. Put only its `name:sha256hex` line in `capstan/api-key-hashes`. Multiple keys
   form a comma-separated list. The initially generated random value is intentionally
   invalid, so missing provisioning fails closed.
3. Put the Gate key in `capstan/gate-api-key` when using the Gate. Its HTTPS URL goes in
   the nonsensitive GitHub variable `CAPSTAN_GATE_URL`. Leave the URL empty when disabled.
   The randomly generated initial key is not a working Gate credential.

Database password rotation is a separate operation: the credentials and complete URL
must stay consistent. This demo does not enable automatic database rotation.

Set these **GitHub repository variables** in `Aly700/capstan`:

- `CAPSTAN_AWS_ACCOUNT_ID`: the 12-digit account ID.
- `CAPSTAN_DEPLOY_ROLE_ARN`: `DeployRoleArn` from `CapstanGithubOidc` outputs.
- `CAPSTAN_BUDGET_EMAIL`: the same real address used above.
- `CAPSTAN_GATE_URL`: optional HTTPS Gate URL without credentials or query parameters.

## Manual deployment and smoke

After Verify is green on the desired `main` commit, choose Actions → Deploy → Run
workflow, selecting `main`. Other refs are rejected both by the job condition and OIDC
trust. Do not add a GitHub Environment without also reviewing its changed OIDC subject.

The workflow builds and pushes `linux/arm64` tagged with the full commit SHA. ECR tags
are immutable; rerunning the same SHA reuses the existing image. Authentication failures
stop the workflow. It then runs `cdk deploy --all --require-approval never` with
`workloadOnly=true`, the SHA, budget email and the scoped execution role. That context
excludes `CapstanGithubOidc` and `CapstanNetwork`: changes to identity, the repository
or networking require the owner.
The deployment includes the budget before starting the service. No scaling policy can
increase desired count. Replacements stop the old task before starting the new one,
which keeps one task and causes a brief outage; worker retries must tolerate it.

Confirm the SNS email subscription sent to the budget address; the two CloudWatch alarms
cannot email until it is confirmed. Budget emails are separate from this subscription.

The workflow reads the execute-api URL from the nonsensitive deployment outputs and
retries both `/healthz` and `/readyz`. An owner can repeat:

```sh
CAPSTAN_ADDRESS=$(aws cloudformation describe-stacks --stack-name CapstanService \
  --region us-east-1 --query 'Stacks[0].Outputs[?OutputKey==`ApiUrl`].OutputValue' --output text)
curl --fail --show-error "$CAPSTAN_ADDRESS/healthz"
curl --fail --show-error "$CAPSTAN_ADDRESS/readyz"
```

Then connect the integrated SDK with `CAPSTAN_ADDRESS` and its API key and run the
approved workflow proof. Health is an ingress/process check; readiness checks the DB;
neither alone proves workflow correctness. Record the deployed SHA and start/end time.
Long-poll 504s usually mean the poll timeout was increased, the task is unavailable,
or the VPC link/discovery path is broken. Keep the server at 20 s. A VPC link unused
for 60 days can become inactive and need time to recreate ENIs when used again; see
[VPC link lifecycle](https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-vpc-links.html).

## Rotate an API key

Generate a second named key with `capstan-server keygen`. Add its hash alongside the
old hash in `capstan/api-key-hashes` using Secrets Manager's plaintext-value editor.
Secrets are read at task launch, so replace the task from an owner session:

```sh
aws ecs update-service --region us-east-1 --cluster capstan \
  --service capstan-server --force-new-deployment
aws ecs wait services-stable --region us-east-1 --cluster capstan --services capstan-server
```

Move workers to the new cleartext key, test an authenticated RPC, remove the old hash,
and force a second deployment. Verify the old key now gets an authentication failure.
Never paste keys into issue bodies or log output. Rotate the Gate key similarly,
coordinating its value with the Gate before replacing the task.

## Logs and alarms

Logs retain seven days and are deleted with the service stack. Access logs include
request ID, route, status and latency, without headers, bodies or secret values:

```sh
aws logs tail /capstan/server --region us-east-1 --since 10m --follow
aws logs tail /capstan/http-api --region us-east-1 --since 10m --follow
aws logs tail /aws/lambda/capstan-task-count --region us-east-1 --since 10m
aws ecs describe-services --region us-east-1 --cluster capstan --services capstan-server
```

- `capstan-http-5xx-rate`: at least 5% 5xx over five minutes. Check access logs, ECS
  service events and server logs. Check Cloud Map SRV registration and the two security
  group hops. Verify the DB and secret provisioning. Roll back to a known SHA with an
  owner-reviewed CDK deployment if a release caused it; do not open port 7233 publicly.
- `capstan-no-running-task`: running count below one in two of three one-minute samples.
  Check ECS stopped-task reasons, image architecture/tag, secret access and migrations.
  The ARM Lambda probes `DescribeServices` once a minute and publishes exactly one
  custom metric; missing samples also alarm, so inspect its logs and schedule if ECS is
  healthy. Container Insights is disabled to avoid its larger metric set.
- Budget 50% / 90% / 100%: reconcile the entire account's spend and execute the cleanup
  below. Alerts can lag accrued charges; they are not an automatic shutdown mechanism.

## Destroy to zero recurring workload cost

Export any evidence required by the proof first. This is a disposable database: no final
snapshot, no automated backup retention, no retained secrets or logs. Destroy from the
owner's SSO session, since GitHub has no destroy permission. Stop workers, then from
`infra/` use the same email and context choices as deployment:

```sh
npx cdk destroy CapstanService -c imageTag=cleanup -c "budgetEmail=$BUDGET_EMAIL"
npx cdk destroy CapstanData -c imageTag=cleanup -c "budgetEmail=$BUDGET_EMAIL"
npx cdk destroy CapstanNetwork CapstanBudget -c imageTag=cleanup -c "budgetEmail=$BUDGET_EMAIL"
npx cdk destroy CapstanGithubOidc -c imageTag=cleanup -c "budgetEmail=$BUDGET_EMAIL"
```

The order releases exports and ENIs before deleting the VPC, and leaves identity until
last. ECR uses `EmptyOnDelete` so images do not block removal. Secrets Manager resources
use `DESTROY`; CloudFormation deletes them without recovery. An imported GitHub OIDC
provider and AWS service-linked roles remain because they may be shared and have no
standing charge. There is no bootstrap bucket to empty. Check for DELETE_FAILED stacks
and retry after resolving the named resource; a failed deploy does not imply cleanup.

Verify in the CloudFormation, ECS, RDS, ECR, Secrets Manager, CloudWatch and Cloud Map
consoles that the Capstan stacks/resources are gone, including the private hosted zone,
VPC link ENIs and task public IP. Check for manually created RDS snapshots and log
exports; those are outside stack deletion. Remove the GitHub deployment variables and
reconcile Billing again after usage arrives. Previously incurred charges remain due;
"zero" means no resources left accruing recurring workload charges.

## Cost table

USD on-demand estimates for `us-east-1`, checked 2026-09-28; no free-tier credits,
Savings Plans or taxes assumed. Use 730 hours/month for hourly conversions; actual
proration follows the service's billing month. Each row links the AWS pricing page.
The RDS values are also confirmed by the public [AWS regional price list](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonRDS/current/us-east-1/index.json),
published 2026-09-24. Traffic quantities below are billable units, not application runs.

| Cost | Rate and monthly example | Hourly amount or formula | AWS pricing |
| --- | --- | --- | --- |
| Fargate ARM CPU, 0.25 vCPU | $0.03237984/vCPU-hour; $5.908/month | $0.00809496/hour | [Fargate](https://aws.amazon.com/fargate/pricing/) |
| Fargate ARM memory, 0.5 GB | $0.00356004/GB-hour; $1.300/month; included 20 GB ephemeral storage | $0.00178002/hour | [Fargate](https://aws.amazon.com/fargate/pricing/) |
| RDS PostgreSQL db.t4g.micro, single-AZ | $0.016/instance-hour; $11.68/month | $0.016/hour | [RDS PostgreSQL](https://aws.amazon.com/rds/postgresql/pricing/) |
| RDS gp3, 20 GB | $0.115/GB-month; $2.30/month, baseline IOPS included | $0.00315068/hour | [RDS PostgreSQL](https://aws.amazon.com/rds/postgresql/pricing/) |
| One public IPv4 for the task | $0.005/IP-hour; $3.65/month | $0.005/hour | [VPC](https://aws.amazon.com/vpc/pricing/) |
| HTTP API and VPC link | $1.00/million HTTP requests at the first tier; no VPC link hourly fee | $0 fixed; $0.000001 × billable requests/hour (512 KB units) | [API Gateway](https://aws.amazon.com/api-gateway/pricing/) |
| Cloud Map registry and discovery | $0.10/resource-month + $1/million discovery calls | $0.00013699/hour + $0.000001 × lookups/hour | [Cloud Map](https://aws.amazon.com/cloud-map/pricing/) |
| Route 53 private hosted zone | $0.50/zone-month; private DNS queries free | $0.00068493/hour equivalent only; **not prorated**; reserve $0.50/session (waived if deleted within 12 hours) | [Route 53](https://aws.amazon.com/route53/pricing/) |
| Secrets Manager, four secrets | $0.40/secret-month, $1.60/month; $0.05/10,000 API calls | $0.00219178/hour + $0.000005 × calls/hour | [Secrets Manager](https://aws.amazon.com/secrets-manager/pricing/) |
| ECR image storage, assume 0.1 GB total | $0.10/GB-month; $0.01/month; same-region pulls free | $0.00001370/hour at 0.1 GB, scale with retained image size | [ECR](https://aws.amazon.com/ecr/pricing/) |
| One custom metric and two alarms | $0.30/metric-month + $0.10/alarm-metric-month; three alarm metrics (two for the rate, one task count), $0.60/month total | $0.00082192/hour | [CloudWatch](https://aws.amazon.com/cloudwatch/pricing/) |
| CloudWatch PutMetricData | $0.01/1,000 calls; 60 calls/hour | $0.0006/hour before API free tier | [CloudWatch](https://aws.amazon.com/cloudwatch/pricing/) |
| Logs ingestion and retention | $0.50/GB ingested + $0.03/GB-month stored | $0.50 × GB ingested/hour + $0.00004110 × GB stored | [CloudWatch](https://aws.amazon.com/cloudwatch/pricing/) |
| Task-count Lambda, ARM 128 MB | $0.20/million invocations + $0.0000133334/GB-second | About $0.000032/hour at 60 invocations/hour and 0.2 s each (measure actual duration) | [Lambda](https://aws.amazon.com/lambda/pricing/) |
| EventBridge scheduled rule | AWS service events to the same account have no event-bus charge | $0/hour; target Lambda priced above | [EventBridge](https://aws.amazon.com/eventbridge/pricing/) |
| SNS alarm delivery | $0.50/million standard requests; $2/100,000 email deliveries | $0.0000005 × requests/hour + $0.00002 × emails/hour | [SNS](https://aws.amazon.com/sns/pricing/) |
| Budget monitoring and email alerts | No monitoring/notification charge; no paid reports or actions | $0/hour | [Budgets](https://aws.amazon.com/aws-cost-management/aws-budgets/pricing/) |
| RDS surplus CPU credits | $0.075/vCPU-hour above the T4g baseline in Unlimited mode | $0.075 × surplus vCPU-hours per hour | [RDS PostgreSQL](https://aws.amazon.com/rds/postgresql/pricing/) |
| Internet/cross-AZ data transfer | Internet egress tier starts at $0.09/GB after the shared free allowance; regional transfer can add $0.01/GB per charged side | Volume-dependent; reserve up to $0.02/GB for two charged cross-AZ sides | [EC2 transfer](https://aws.amazon.com/ec2/pricing/on-demand/) |
| ECS control plane and IAM | No added Fargate orchestration or IAM charge | $0/hour | [ECS](https://aws.amazon.com/ecs/pricing/), [IAM pricing FAQ](https://aws.amazon.com/iam/faqs/) |

The standing estimate is **$0.03782/hour plus the hosted-zone billing step**, or about
**$28.11 for 730 hours including the zone**, before traffic/logs/CPU credits. A four-hour
proof with 10,000 HTTP calls, conservatively 10,000 discovery calls, 0.01 GB of log
ingestion and a $0.50 zone reserve is approximately **$0.68** plus transfer, secret calls
and surplus CPU credits. This is an estimate, not a billing guarantee. Count all time
from resource creation until deletion completes, including provisioning and failed
deployments. RDS has a ten-minute minimum following billable state changes; Fargate
Linux has a one-minute minimum. Retained databases, images and logs keep billing even
when workers stop. Price any separately deployed Gate and model provider independently.
