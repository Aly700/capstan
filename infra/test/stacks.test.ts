import assert from 'node:assert/strict';
import { existsSync, mkdirSync, mkdtempSync, readFileSync } from 'node:fs';
import test from 'node:test';
import { App } from 'aws-cdk-lib';
import { Template, Match } from 'aws-cdk-lib/assertions';

async function synth(extra: Record<string, unknown> = {}) {
  assert.ok(existsSync(new URL('../lib/app.ts', import.meta.url)), 'CDK stacks must be implemented');
  const { createStacks } = await import('../lib/app.ts');
  mkdirSync(new URL('../../.lane/', import.meta.url), { recursive: true });
  const defaults = JSON.parse(readFileSync(new URL('../cdk.json', import.meta.url), 'utf8')).context;
  const app = new App({ outdir: mkdtempSync(new URL('../../.lane/cdk-test-', import.meta.url).pathname), context: { ...defaults, imageTag: 'test', budgetEmail: 'test@example.com', ...extra } });
  const stacks = createStacks(app);
  return Object.fromEntries(Object.entries(stacks).map(([name, stack]) => [name, Template.fromStack(stack)]));
}

test('network uses two public AZs and never creates NAT, endpoints, or a load balancer', async () => {
  const templates = await synth();
  templates.network.resourceCountIs('AWS::EC2::Subnet', 2);
  for (const template of Object.values(templates)) {
    for (const type of ['AWS::EC2::NatGateway', 'AWS::EC2::VPCEndpoint', 'AWS::ElasticLoadBalancingV2::LoadBalancer', 'AWS::ElasticLoadBalancing::LoadBalancer']) template.resourceCountIs(type, 0);
  }
  templates.network.resourceCountIs('AWS::EC2::InternetGateway', 1);
  templates.network.hasResourceProperties('AWS::EC2::SecurityGroupIngress', { IpProtocol: 'tcp', FromPort: 7233, ToPort: 7233, SourceSecurityGroupId: Match.anyValue() });
  const ingress = Object.values(templates.network.findResources('AWS::EC2::SecurityGroupIngress'));
  assert.equal(ingress.length, 2);
  assert.deepEqual(ingress.map((r: any) => r.Properties.FromPort).sort(), [5432, 7233]);
  assert.ok(ingress.every((r: any) => r.Properties.SourceSecurityGroupId && !r.Properties.CidrIp));
});

test('database is private, encrypted, single-AZ micro with a generated secret and disposable storage', async () => {
  const { data } = await synth();
  data.hasResourceProperties('AWS::RDS::DBInstance', {
    Engine: 'postgres', EngineVersion: '16.13', DBInstanceClass: 'db.t4g.micro',
    PubliclyAccessible: false, MultiAZ: false, AllocatedStorage: '20', StorageType: 'gp3', StorageEncrypted: true,
    BackupRetentionPeriod: 0, DeletionProtection: false, DeleteAutomatedBackups: true,
    MasterUserPassword: Match.anyValue(),
  });
  data.hasResource('AWS::RDS::DBInstance', { DeletionPolicy: 'Delete', UpdateReplacePolicy: 'Delete' });
  data.resourceCountIs('AWS::SecretsManager::Secret', 4);
  data.hasResourceProperties('AWS::SecretsManager::Secret', { Name: 'capstan/database-url', SecretString: Match.anyValue() });
  data.hasResourceProperties('AWS::SecretsManager::Secret', { GenerateSecretString: Match.objectLike({ GenerateStringKey: 'password', ExcludePunctuation: true, PasswordLength: 40 }) });
  assert.match(JSON.stringify(data.toJSON()), /resolve:secretsmanager:/);
  assert.doesNotMatch(JSON.stringify(data.toJSON().Outputs ?? {}), /password|SecretString|database-url/i);
});

test('one ARM task uses secret injection and only HTTPS gateway ingress with SRV discovery', async () => {
  const { service } = await synth();
  service.hasResourceProperties('AWS::ECS::TaskDefinition', {
    Cpu: '256', Memory: '512', RuntimePlatform: { CpuArchitecture: 'ARM64', OperatingSystemFamily: 'LINUX' },
    ContainerDefinitions: Match.arrayWith([Match.objectLike({
      Environment: Match.arrayWith([{ Name: 'CAPSTAN_POLL_TIMEOUT', Value: '20s' }]),
      Secrets: Match.arrayWith(['CAPSTAN_DATABASE_URL', 'CAPSTAN_API_KEY_HASHES'].map(Name => ({ Name, ValueFrom: Match.anyValue() }))),
      PortMappings: Match.arrayWith([Match.objectLike({ ContainerPort: 7233 })]), ReadonlyRootFilesystem: true,
    })]),
  });
  service.hasResourceProperties('AWS::ECS::Service', { DesiredCount: 1, DeploymentConfiguration: Match.objectLike({ MinimumHealthyPercent: 0, MaximumPercent: 100 }), NetworkConfiguration: { AwsvpcConfiguration: Match.objectLike({ AssignPublicIp: 'ENABLED' }) } });
  service.hasResourceProperties('AWS::ServiceDiscovery::Service', { DnsConfig: Match.objectLike({ DnsRecords: [{ TTL: 10, Type: 'SRV' }] }) });
  service.resourceCountIs('AWS::ApiGatewayV2::VpcLink', 1);
  service.hasResourceProperties('AWS::ApiGatewayV2::Api', { ProtocolType: 'HTTP', DisableExecuteApiEndpoint: false });
  service.hasResourceProperties('AWS::ApiGatewayV2::Integration', { ConnectionType: 'VPC_LINK', IntegrationType: 'HTTP_PROXY', IntegrationMethod: 'ANY', PayloadFormatVersion: '1.0', TimeoutInMillis: 30000, RequestParameters: { 'overwrite:path': '$request.path' }, IntegrationUri: Match.anyValue() });
  const integration = Object.values(service.findResources('AWS::ApiGatewayV2::Integration'))[0] as any;
  const discoveryId = Object.keys(service.findResources('AWS::ServiceDiscovery::Service'))[0];
  assert.deepEqual(integration.Properties.IntegrationUri, { 'Fn::GetAtt': [discoveryId, 'Arn'] });
  service.hasResourceProperties('AWS::ApiGatewayV2::Stage', { AccessLogSettings: Match.objectLike({ DestinationArn: Match.anyValue(), Format: Match.anyValue() }) });
  const outputs = JSON.stringify(service.toJSON().Outputs);
  assert.match(outputs, /execute-api/);
  assert.doesNotMatch(outputs, /SecretString|password|api-key-hashes/i);
  assert.deepEqual((Object.values(service.findResources('AWS::ApiGatewayV2::Route')) as any[]).map(r => r.Properties.RouteKey).sort(), ['GET /healthz', 'GET /readyz', 'POST /capstan.v1.ClientService/{method}', 'POST /capstan.v1.WorkerService/{method}']);
  for (const log of Object.values(service.findResources('AWS::Logs::LogGroup')) as any[]) {
    assert.equal(log.Properties.RetentionInDays, 7);
    assert.equal(log.DeletionPolicy, 'Delete');
  }
  const taskRole = Object.entries(service.findResources('AWS::IAM::Role')).find(([, r]) => r.Properties.RoleName === 'capstan-task')![0];
  for (const policy of Object.values(service.findResources('AWS::IAM::Policy')) as any[]) assert.ok(!JSON.stringify(policy.Properties.Roles).includes(taskRole), 'application task role stays empty');
});

test('alarms measure the 5xx rate and actual service task count, treating missing task data as failure', async () => {
  const { service } = await synth();
  service.resourceCountIs('AWS::CloudWatch::Alarm', 2);
  service.hasResourceProperties('AWS::CloudWatch::Alarm', { Threshold: 1, ComparisonOperator: 'LessThanThreshold', TreatMissingData: 'breaching', Namespace: 'Capstan/Service', MetricName: 'RunningTaskCount', AlarmActions: Match.anyValue() });
  service.hasResourceProperties('AWS::CloudWatch::Alarm', { Threshold: 5, ComparisonOperator: 'GreaterThanOrEqualToThreshold', Metrics: Match.arrayWith([Match.objectLike({ Expression: 'IF(requests > 0, 100 * errors / requests, 0)' })]) });
  service.hasResourceProperties('AWS::Lambda::Function', { Runtime: 'python3.13', Architectures: ['arm64'], Timeout: 10 });
  service.hasResourceProperties('AWS::Events::Rule', { ScheduleExpression: 'rate(1 minute)' });
});

test('budget is account-wide $10 with 50/90/100 percent actual email alerts', async () => {
  const { budget } = await synth();
  budget.hasResourceProperties('AWS::Budgets::Budget', {
    Budget: { BudgetName: 'capstan-monthly', BudgetType: 'COST', TimeUnit: 'MONTHLY', BudgetLimit: { Amount: 10, Unit: 'USD' } },
    NotificationsWithSubscribers: [50, 90, 100].map(Threshold => ({ Notification: { ComparisonOperator: 'GREATER_THAN', NotificationType: 'ACTUAL', Threshold, ThresholdType: 'PERCENTAGE' }, Subscribers: [{ Address: 'test@example.com', SubscriptionType: 'EMAIL' }] })),
  });
});

test('GitHub trust binds the immutable repository identity to main and cannot mutate its foundation', async () => {
  const { oidc } = await synth();
  // Captured from this repository's OIDC customization API during the live deployment.
  oidc.hasResourceProperties('AWS::IAM::Role', { RoleName: 'capstan-github-deploy', AssumeRolePolicyDocument: { Version: '2012-10-17', Statement: [Match.objectLike({ Action: 'sts:AssumeRoleWithWebIdentity', Condition: { StringEquals: { 'token.actions.githubusercontent.com:aud': 'sts.amazonaws.com', 'token.actions.githubusercontent.com:sub': 'repo:Aly700@112176329/capstan@1392801248:ref:refs/heads/main' } } })] } });
  const roles = oidc.findResources('AWS::IAM::Role');
  const githubID = Object.keys(roles).find(id => roles[id].Properties.RoleName === 'capstan-github-deploy')!;
  const policies = Object.values(oidc.findResources('AWS::IAM::Policy')).filter((p: any) => JSON.stringify(p.Properties.Roles).includes(githubID));
  assert.equal(policies.length, 1);
  const policy = JSON.stringify(policies[0]);
  assert.doesNotMatch(policy, /AdministratorAccess|sts:AssumeRole"|iam:Create|iam:Put|secretsmanager:|CapstanGithubOidc|CapstanNetwork/);
  assert.match(policy, /ecr:PutImage/);
  assert.match(policy, /cloudformation:CreateChangeSet/);
  assert.match(policy, /iam:PassedToService/);
  for (const statement of (policies[0] as any).Properties.PolicyDocument.Statement) {
    const actions = [statement.Action].flat();
    assert.ok(actions.every((a: string) => !a.includes('*')), 'no wildcard actions');
    if (statement.Resource === '*') assert.deepEqual(actions, ['ecr:GetAuthorizationToken']);
  }
  oidc.hasResourceProperties('AWS::ECR::Repository', { RepositoryName: 'capstan-server', EmptyOnDelete: true, ImageTagMutability: 'IMMUTABLE' });
});

test('workload deployment excludes OIDC, has no lookups/assets, and rejects unsafe context', async () => {
  const templates = await synth({ workloadOnly: 'true' });
  assert.equal(templates.oidc, undefined);
  for (const template of Object.values(templates)) {
    assert.doesNotMatch(JSON.stringify(template.toJSON()), /BootstrapVersion|cdk-hnb659fds|AWS::CDK::Metadata/);
  }
  await assert.rejects(synth({ imageTag: 'bad/tag' }), /imageTag/);
  await assert.rejects(synth({ budgetEmail: '' }), /budgetEmail/);
  await assert.rejects(synth({ gateUrl: 'http://gate.example.com' }), /gateUrl/);
  await assert.rejects(synth({ apiKey: 'must-not-be-context' }), /context/);
});

test('deployment can create Cloud Map namespaces and runtime roles cannot escape their boundary', async () => {
  const { oidc, service } = await synth();
  for (const role of Object.values(service.findResources('AWS::IAM::Role')) as any[]) {
    assert.match(JSON.stringify(role.Properties.PermissionsBoundary), /capstan-workload-boundary/);
  }
  const policies = Object.values(oidc.findResources('AWS::IAM::Policy')) as any[];
  const statements = policies.flatMap(policy => policy.Properties.PolicyDocument.Statement);
  const namespace = statements.find(s => [s.Action].flat().includes('servicediscovery:CreatePrivateDnsNamespace'));
  assert.equal(namespace.Resource, '*', 'Cloud Map namespace creation does not support a resource ARN');
  const createRole = statements.find(s => [s.Action].flat().includes('iam:CreateRole'));
  assert.match(JSON.stringify(createRole.Condition), /iam:PermissionsBoundary.*capstan-workload-boundary/);
  assert.doesNotMatch(JSON.stringify(statements), /iam:DeleteRolePermissionsBoundary|iam:AttachRolePolicy|AdministratorAccess/);
  for (const policy of policies) assert.ok(JSON.stringify(policy.Properties.PolicyDocument).length < 10240, 'inline role policy quota');
  for (const template of [oidc, service]) assert.ok(JSON.stringify(template.toJSON()).length < 51200, 'inline CloudFormation template limit');
});

test('first deployment can create the API Gateway service-linked role under its actual service name', async () => {
  const { oidc } = await synth();
  const statements = (Object.values(oidc.findResources('AWS::IAM::Policy')) as any[]).flatMap(p => p.Properties.PolicyDocument.Statement);
  const create = statements.find(s => [s.Action].flat().includes('iam:CreateServiceLinkedRole'));
  assert.deepEqual(create.Condition.StringEquals['iam:AWSServiceName'], ['ecs.amazonaws.com', 'rds.amazonaws.com', 'ops.apigateway.amazonaws.com']);
});

test('Cloud Map tagging APIs use the required regional wildcard resource', async () => {
  const { oidc } = await synth();
  const statements = (Object.values(oidc.findResources('AWS::IAM::Policy')) as any[]).flatMap(p => p.Properties.PolicyDocument.Statement);
  // Live namespace creation authorizes TagResource before a namespace ARN exists;
  // AWS also lists no resource-level support for UntagResource/ListTagsForResource.
  for (const action of ['servicediscovery:TagResource', 'servicediscovery:UntagResource', 'servicediscovery:ListTagsForResource']) {
    const grants = statements.filter(s => [s.Action].flat().includes(action));
    assert.equal(grants.length, 1, action);
    assert.equal(grants[0].Resource, '*', `${action} cannot be scoped to a namespace/service ARN`);
    assert.deepEqual(grants[0].Condition, { StringEquals: { 'aws:RequestedRegion': 'us-east-1' } });
  }
});

test('API Gateway handlers can tag APIs and VPC links without a wildcard resource', async () => {
  const { oidc } = await synth();
  const executionId = Object.entries(oidc.findResources('AWS::IAM::Role')).find(([, role]) => role.Properties.RoleName === 'capstan-cloudformation')![0];
  const statements = (Object.values(oidc.findResources('AWS::IAM::Policy')) as any[])
    .filter(policy => JSON.stringify(policy.Properties.Roles).includes(executionId))
    .flatMap(policy => policy.Properties.PolicyDocument.Statement);
  // The live VpcLink create denial and registry handler schema require these
  // named actions in addition to the existing HTTP method permissions.
  for (const action of ['apigateway:TagResource', 'apigateway:UntagResource']) {
    const grants = statements.filter(s => [s.Action].flat().includes(action));
    assert.equal(grants.length, 1, `${action} must be granted explicitly`);
    assert.equal(grants[0].Resource.length, 2);
    const resources = JSON.stringify(grants[0].Resource);
    assert.match(resources, /:apigateway:us-east-1::\/apis\*/);
    assert.match(resources, /:apigateway:us-east-1::\/vpclinks\*/);
    assert.doesNotMatch(resources, /restapis|"Resource":"\*"/);
  }
});

test('GitHub can apply and remove the workload stack tags required by CloudFormation changesets', async () => {
  const { oidc } = await synth();
  const statements = (Object.values(oidc.findResources('AWS::IAM::Policy')) as any[]).flatMap(p => p.Properties.PolicyDocument.Statement);
  const changesets = statements.find(s => [s.Action].flat().includes('cloudformation:CreateChangeSet'));
  assert.ok(changesets.Action.includes('cloudformation:TagResource'));
  assert.ok(changesets.Action.includes('cloudformation:UntagResource'));
  assert.equal(changesets.Resource.length, 3);
  assert.doesNotMatch(JSON.stringify(changesets.Resource), /CapstanGithubOidc|CapstanNetwork/);
});

test('workload deployment imports the owner-managed network and has no EC2 mutation permissions', async () => {
  const { oidc, network, data, service } = await synth();
  const workload = await synth({ workloadOnly: 'true' });
  assert.deepEqual(Object.keys(workload).sort(), ['budget', 'data', 'service']);
  for (const template of Object.values(workload)) assert.ok(Object.values(template.toJSON().Resources).every((r: any) => !r.Type.startsWith('AWS::EC2::')), 'workload cannot create networking');
  assert.deepEqual(workload.data.toJSON(), data.toJSON(), 'owner and GitHub deploy the same data template');
  assert.deepEqual(workload.service.toJSON(), service.toJSON(), 'owner and GitHub deploy the same service template');
  const exports = Object.values(network.toJSON().Outputs).map((o: any) => o.Export.Name);
  for (const name of ['VpcId', 'PublicSubnet1Id', 'PublicSubnet2Id', 'PublicSubnet1Az', 'PublicSubnet2Az', 'LinkGroupId', 'TaskGroupId', 'DatabaseGroupId']) {
    assert.ok(exports.includes(`CapstanNetwork:${name}`), `${name} must have a stable export`);
  }
  service.hasResourceProperties('AWS::ApiGatewayV2::VpcLink', { SecurityGroupIds: [{ 'Fn::ImportValue': 'CapstanNetwork:LinkGroupId' }], SubnetIds: [{ 'Fn::ImportValue': 'CapstanNetwork:PublicSubnet1Id' }, { 'Fn::ImportValue': 'CapstanNetwork:PublicSubnet2Id' }] });
  data.hasResourceProperties('AWS::RDS::DBInstance', { VPCSecurityGroups: [{ 'Fn::ImportValue': 'CapstanNetwork:DatabaseGroupId' }] });
  const statements = (Object.values(oidc.findResources('AWS::IAM::Policy')) as any[]).flatMap(p => p.Properties.PolicyDocument.Statement);
  const ec2Actions = statements.flatMap(s => [s.Action].flat()).filter((a: string) => a.startsWith('ec2:'));
  assert.ok(ec2Actions.length > 0);
  assert.ok(ec2Actions.every((a: string) => a.startsWith('ec2:Describe')), 'network provisioning and mutation belong to the owner');
});

// The server refuses to start with only one of CAPSTAN_GATE_URL / CAPSTAN_GATE_API_KEY
// (AWS attempt 2: the ECS circuit breaker tripped on exactly that).
test('Gate URL and key are injected together or not at all', async () => {
  const container = (templates: Record<string, Template>) =>
    (Object.values(templates.service.findResources('AWS::ECS::TaskDefinition'))[0] as any).Properties.ContainerDefinitions[0];
  const names = (list: { Name: string }[] = []) => list.map(e => e.Name);
  const without = container(await synth());
  assert.ok(!names(without.Environment).includes('CAPSTAN_GATE_URL'));
  assert.ok(!names(without.Secrets).includes('CAPSTAN_GATE_API_KEY'));
  const withGate = container(await synth({ gateUrl: 'https://gate.example.com' }));
  assert.ok(names(withGate.Environment).includes('CAPSTAN_GATE_URL'));
  assert.ok(names(withGate.Secrets).includes('CAPSTAN_GATE_API_KEY'));
});

// AWS attempt 2: ecs:DeregisterTaskDefinition is evaluated against "*", so a
// task-definition-ARN grant left the failed service stack in ROLLBACK_FAILED.
test('the CloudFormation role can register, describe and deregister task definitions', async () => {
  const { oidc } = await synth();
  const statements = Object.values(oidc.findResources('AWS::IAM::Policy')).flatMap((p: any) => p.Properties.PolicyDocument.Statement);
  for (const action of ['ecs:RegisterTaskDefinition', 'ecs:DescribeTaskDefinition', 'ecs:DeregisterTaskDefinition']) {
    assert.ok(statements.some((s: any) => [s.Action].flat().includes(action) && [s.Resource].flat().includes('*')), `${action} on *`);
  }
});
