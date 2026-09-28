import assert from 'node:assert/strict';
import { existsSync, mkdirSync, mkdtempSync } from 'node:fs';
import test from 'node:test';
import { App } from 'aws-cdk-lib';
import { Template, Match } from 'aws-cdk-lib/assertions';

async function synth(extra: Record<string, unknown> = {}) {
  assert.ok(existsSync(new URL('../lib/app.ts', import.meta.url)), 'CDK stacks must be implemented');
  const { createStacks } = await import('../lib/app.ts');
  mkdirSync(new URL('../../.lane/', import.meta.url), { recursive: true });
  const app = new App({ outdir: mkdtempSync(new URL('../../.lane/cdk-test-', import.meta.url).pathname), context: { imageTag: 'test', budgetEmail: 'test@example.com', ...extra } });
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
  assert.match(JSON.stringify(data.toJSON()), /resolve:secretsmanager:/);
  assert.doesNotMatch(JSON.stringify(data.toJSON().Outputs ?? {}), /password|SecretString|database-url/i);
});

test('one ARM task uses secret injection and only HTTPS gateway ingress with SRV discovery', async () => {
  const { service } = await synth();
  service.hasResourceProperties('AWS::ECS::TaskDefinition', {
    Cpu: '256', Memory: '512', RuntimePlatform: { CpuArchitecture: 'ARM64', OperatingSystemFamily: 'LINUX' },
    ContainerDefinitions: Match.arrayWith([Match.objectLike({
      Environment: Match.arrayWith([{ Name: 'CAPSTAN_POLL_TIMEOUT', Value: '20s' }]),
      Secrets: Match.arrayWith(['CAPSTAN_DATABASE_URL', 'CAPSTAN_API_KEY_HASHES', 'CAPSTAN_GATE_API_KEY'].map(Name => ({ Name, ValueFrom: Match.anyValue() }))),
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

test('GitHub trust permits only main and permissions cannot mutate its own foundation', async () => {
  const { oidc } = await synth();
  oidc.hasResourceProperties('AWS::IAM::Role', { RoleName: 'capstan-github-deploy', AssumeRolePolicyDocument: { Version: '2012-10-17', Statement: [Match.objectLike({ Action: 'sts:AssumeRoleWithWebIdentity', Condition: { StringEquals: { 'token.actions.githubusercontent.com:aud': 'sts.amazonaws.com', 'token.actions.githubusercontent.com:sub': 'repo:Aly700/capstan:ref:refs/heads/main' } } })] } });
  const roles = oidc.findResources('AWS::IAM::Role');
  const githubID = Object.keys(roles).find(id => roles[id].Properties.RoleName === 'capstan-github-deploy')!;
  const policies = Object.values(oidc.findResources('AWS::IAM::Policy')).filter((p: any) => JSON.stringify(p.Properties.Roles).includes(githubID));
  assert.equal(policies.length, 1);
  const policy = JSON.stringify(policies[0]);
  assert.doesNotMatch(policy, /AdministratorAccess|sts:AssumeRole"|iam:Create|iam:Put|secretsmanager:|CapstanGithubOidc/);
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
