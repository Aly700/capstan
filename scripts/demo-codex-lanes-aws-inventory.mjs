// Read-only inventory. All AWS calls are confined to the authorized profile/account.
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { readFileSync, writeFileSync } from 'node:fs';
const exec = promisify(execFile);
const env = { ...process.env, AWS_PROFILE: 'agentops', AWS_REGION: 'us-east-1', AWS_PAGER: '' };
for (const key of ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN']) delete env[key];
const calls = [];
async function aws(...args) {
  calls.push(`AWS_PROFILE=agentops aws --profile agentops --region us-east-1 ${args.join(' ')} --output json`);
  const { stdout } = await exec('aws', ['--profile', 'agentops', '--region', 'us-east-1', ...args, '--output', 'json'], { env, timeout: 60_000, maxBuffer: 16 * 1024 * 1024 });
  return JSON.parse(stdout);
}
const identity = await aws('sts', 'get-caller-identity');
assert.equal(identity.Account, '280517746513');
assert.equal(identity.Arn, 'arn:aws:iam::280517746513:user/agentops-deployer');
const baseline = process.argv[3] ? JSON.parse(readFileSync(process.argv[3], 'utf8')) : undefined;
const out = { capturedAt: new Date().toISOString(), identity, resources: {}, errors: {}, commands: calls };
const queries = {
  taggedResources: ['resourcegroupstaggingapi', 'get-resources', '--tag-filters', 'Key=Project,Values=capstan', '--query', 'ResourceTagMappingList'],
  stacks: ['cloudformation', 'describe-stacks', '--query', 'Stacks[].{Name:StackName,Status:StackStatus,Id:StackId,Created:CreationTime,Updated:LastUpdatedTime}'],
  ecsClusters: ['ecs', 'list-clusters', '--query', "clusterArns[?contains(@, '/capstan')]"],
  ecsActiveTaskDefinitions: ['ecs', 'list-task-definitions', '--status', 'ACTIVE', '--query', "taskDefinitionArns[?contains(@, ':task-definition/capstan')]"],
  ecsInactiveTaskDefinitions: ['ecs', 'list-task-definitions', '--status', 'INACTIVE', '--query', "taskDefinitionArns[?contains(@, ':task-definition/capstan')]"],
  ecsDeletingTaskDefinitions: ['ecs', 'list-task-definitions', '--status', 'DELETE_IN_PROGRESS', '--query', "taskDefinitionArns[?contains(@, ':task-definition/capstan')]"],
  databases: ['rds', 'describe-db-instances', '--query', "DBInstances[?contains(DBInstanceIdentifier, 'capstan')].{Id:DBInstanceIdentifier,Arn:DBInstanceArn,Status:DBInstanceStatus}"],
  databaseSubnetGroups: ['rds', 'describe-db-subnet-groups', '--query', "DBSubnetGroups[?contains(DBSubnetGroupName, 'capstan')].{Name:DBSubnetGroupName,Vpc:VpcId}"],
  databaseSnapshots: ['rds', 'describe-db-snapshots', '--query', "DBSnapshots[?contains(DBInstanceIdentifier, 'capstan')].{Id:DBSnapshotIdentifier,Status:Status,Type:SnapshotType}"],
  ecr: ['ecr', 'describe-repositories', '--query', "repositories[?contains(repositoryName, 'capstan')].{Name:repositoryName,Arn:repositoryArn}"],
  secrets: ['secretsmanager', 'list-secrets', '--include-planned-deletion', '--filters', 'Key=name,Values=capstan/', '--query', 'SecretList[].{Name:Name,Arn:ARN,Deleted:DeletedDate}'],
  cloudMapNamespaces: ['servicediscovery', 'list-namespaces', '--query', "Namespaces[?contains(Name, 'capstan')].{Id:Id,Name:Name,Arn:Arn}"],
  cloudMapServices: ['servicediscovery', 'list-services', '--query', "Services[?Name=='server' || contains(Name, 'capstan')].{Id:Id,Name:Name,Arn:Arn,Instances:InstanceCount}"],
  hostedZones: ['route53', 'list-hosted-zones', '--query', "HostedZones[?contains(Name, 'capstan')].{Id:Id,Name:Name,Private:Config.PrivateZone}"],
  apis: ['apigatewayv2', 'get-apis', '--query', "Items[?contains(Name, 'capstan')].{Id:ApiId,Name:Name}"],
  vpcLinks: ['apigatewayv2', 'get-vpc-links', '--query', "Items[?contains(Name, 'capstan')].{Id:VpcLinkId,Name:Name,Status:VpcLinkStatus}"],
  vpcs: ['ec2', 'describe-vpcs', '--filters', 'Name=tag:Project,Values=capstan', '--query', 'Vpcs[].{Id:VpcId,State:State,Tags:Tags}'],
  taggedEndpoints: ['ec2', 'describe-vpc-endpoints', '--filters', 'Name=tag:Project,Values=capstan', '--query', 'VpcEndpoints[].{Id:VpcEndpointId,Vpc:VpcId,State:State}'],
  taggedEnis: ['ec2', 'describe-network-interfaces', '--filters', 'Name=tag:Project,Values=capstan', '--query', 'NetworkInterfaces[].{Id:NetworkInterfaceId,Vpc:VpcId,Status:Status}'],
  namedEnis: ['ec2', 'describe-network-interfaces', '--query', "NetworkInterfaces[?contains(Description, 'capstan') || contains(Description, 'Capstan')].{Id:NetworkInterfaceId,Vpc:VpcId,Status:Status,Description:Description}"],
  securityGroups: ['ec2', 'describe-security-groups', '--filters', 'Name=tag:Project,Values=capstan', '--query', 'SecurityGroups[].{Id:GroupId,Name:GroupName,Vpc:VpcId}'],
  logGroups: ['logs', 'describe-log-groups', '--query', "logGroups[?contains(logGroupName, 'capstan')].{Name:logGroupName,StoredBytes:storedBytes}"],
  alarms: ['cloudwatch', 'describe-alarms', '--alarm-name-prefix', 'capstan-', '--query', 'MetricAlarms[].{Name:AlarmName,State:StateValue}'],
  topics: ['sns', 'list-topics', '--query', "Topics[?contains(TopicArn, ':capstan-')]"],
  functions: ['lambda', 'list-functions', '--query', "Functions[?starts_with(FunctionName, 'capstan-')].{Name:FunctionName,Arn:FunctionArn}"],
  rules: ['events', 'list-rules', '--name-prefix', 'capstan-', '--query', 'Rules[].{Name:Name,Arn:Arn}'],
  budgets: ['budgets', 'describe-budgets', '--account-id', '280517746513', '--query', "Budgets[?starts_with(BudgetName, 'capstan-')].{Name:BudgetName,Limit:BudgetLimit}"],
  roles: ['iam', 'list-roles', '--query', "Roles[?starts_with(RoleName, 'capstan-')].{Name:RoleName,Arn:Arn}"],
  policies: ['iam', 'list-policies', '--scope', 'Local', '--query', "Policies[?starts_with(PolicyName, 'capstan-')].{Name:PolicyName,Arn:Arn}"],
};
async function capture(name, args) {
  try { out.resources[name] = await aws(...args); }
  catch (error) { out.errors[name] = String(error.stderr ?? error.message); }
}
// Small batches keep account API requests bounded while collecting every category.
const entries = Object.entries(queries);
for (let i = 0; i < entries.length; i += 4) await Promise.all(entries.slice(i, i + 4).map(([name, args]) => capture(name, args)));
const vpcIds = [...new Set([...(out.resources.vpcs ?? []), ...(baseline?.resources?.vpcs ?? [])].map(v => v.Id))];
if (vpcIds.length) {
  await capture('knownVpcEnis', ['ec2', 'describe-network-interfaces', '--filters', `Name=vpc-id,Values=${vpcIds.join(',')}`, '--query', 'NetworkInterfaces[].{Id:NetworkInterfaceId,Vpc:VpcId,Status:Status,Description:Description,PublicIp:Association.PublicIp}']);
  await capture('knownVpcEndpoints', ['ec2', 'describe-vpc-endpoints', '--filters', `Name=vpc-id,Values=${vpcIds.join(',')}`, '--query', 'VpcEndpoints[].{Id:VpcEndpointId,Vpc:VpcId,State:State}']);
}
for (const cluster of out.resources.ecsClusters ?? []) {
  await capture('ecsServices', ['ecs', 'list-services', '--cluster', cluster, '--query', 'serviceArns']);
  await capture('ecsRunningTasks', ['ecs', 'list-tasks', '--cluster', cluster, '--desired-status', 'RUNNING', '--query', 'taskArns']);
}
await capture('sharedOidcProvider', ['iam', 'get-open-id-connect-provider', '--open-id-connect-provider-arn', 'arn:aws:iam::280517746513:oidc-provider/token.actions.githubusercontent.com', '--query', '{Url:Url,Clients:ClientIDList}']);
writeFileSync(process.argv[2], JSON.stringify(out, null, 2) + '\n');
console.log(JSON.stringify(out, null, 2));
if (Object.keys(out.errors).length) process.exitCode = 1;
