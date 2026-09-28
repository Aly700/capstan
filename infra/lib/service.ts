import { readFileSync } from 'node:fs';
import { CfnOutput, Duration, RemovalPolicy, Stack } from 'aws-cdk-lib';
import type { StackProps } from 'aws-cdk-lib';
import * as apigateway from 'aws-cdk-lib/aws-apigatewayv2';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import { SnsAction } from 'aws-cdk-lib/aws-cloudwatch-actions';
import { SubnetType } from 'aws-cdk-lib/aws-ec2';
import { Repository } from 'aws-cdk-lib/aws-ecr';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import { Rule, Schedule } from 'aws-cdk-lib/aws-events';
import { LambdaFunction } from 'aws-cdk-lib/aws-events-targets';
import { ManagedPolicy, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { DnsRecordType, PrivateDnsNamespace } from 'aws-cdk-lib/aws-servicediscovery';
import { Topic } from 'aws-cdk-lib/aws-sns';
import { EmailSubscription } from 'aws-cdk-lib/aws-sns-subscriptions';
import type { Construct } from 'constructs';
import type { CapstanData } from './data.ts';
import type { CapstanNetwork } from './network.ts';

export class CapstanService extends Stack {
  constructor(scope: Construct, id: string, props: StackProps & { network: CapstanNetwork; data: CapstanData; imageTag: string; email: string; gateUrl: string }) {
    super(scope, id, props);
    const { network, data } = props;
    const cluster = new ecs.Cluster(this, 'Cluster', { clusterName: 'capstan', vpc: network.vpc, containerInsightsV2: ecs.ContainerInsights.DISABLED });
    const permissionsBoundary = ManagedPolicy.fromManagedPolicyName(this, 'Boundary', 'capstan-workload-boundary');
    const taskRole = new Role(this, 'TaskRole', { roleName: 'capstan-task', assumedBy: new ServicePrincipal('ecs-tasks.amazonaws.com'), permissionsBoundary });
    const executionRole = new Role(this, 'ExecutionRole', { roleName: 'capstan-execution', assumedBy: new ServicePrincipal('ecs-tasks.amazonaws.com'), permissionsBoundary });
    const task = new ecs.FargateTaskDefinition(this, 'Task', {
      family: 'capstan-server', cpu: 256, memoryLimitMiB: 512, taskRole, executionRole,
      runtimePlatform: { cpuArchitecture: ecs.CpuArchitecture.ARM64, operatingSystemFamily: ecs.OperatingSystemFamily.LINUX },
    });
    const logs = new LogGroup(this, 'ServerLogs', { logGroupName: '/capstan/server', retention: RetentionDays.ONE_WEEK, removalPolicy: RemovalPolicy.DESTROY });
    const repository = Repository.fromRepositoryName(this, 'Repository', 'capstan-server');
    const container = task.addContainer('server', {
      image: ecs.ContainerImage.fromEcrRepository(repository, props.imageTag),
      readonlyRootFilesystem: true,
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: 'server', logGroup: logs }),
      environment: {
        CAPSTAN_ADDR: ':7233', CAPSTAN_POLL_TIMEOUT: '20s', CAPSTAN_MIGRATE: 'true',
        CAPSTAN_DAILY_CAP_USD: '2.00', CAPSTAN_LOG_LEVEL: 'info', CAPSTAN_GATE_URL: props.gateUrl,
      },
      secrets: {
        CAPSTAN_DATABASE_URL: ecs.Secret.fromSecretsManager(data.databaseUrl),
        CAPSTAN_API_KEY_HASHES: ecs.Secret.fromSecretsManager(data.apiKeyHashes),
        CAPSTAN_GATE_API_KEY: ecs.Secret.fromSecretsManager(data.gateKey),
      },
      stopTimeout: Duration.seconds(30),
    });
    container.addPortMappings({ containerPort: 7233, name: 'http' });
    const namespace = new PrivateDnsNamespace(this, 'Namespace', { name: 'capstan.internal', vpc: network.vpc });
    const service = new ecs.FargateService(this, 'Service', {
      serviceName: 'capstan-server', cluster, taskDefinition: task, desiredCount: 1,
      assignPublicIp: true,
      // Keep the task beside the single-AZ database; both public subnets serve the VPC link.
      vpcSubnets: { subnets: [network.vpc.publicSubnets[0]] },
      securityGroups: [network.taskGroup],
      minHealthyPercent: 0, maxHealthyPercent: 100,
      circuitBreaker: { rollback: true },
      cloudMapOptions: { name: 'server', cloudMapNamespace: namespace, dnsRecordType: DnsRecordType.SRV, dnsTtl: Duration.seconds(10), container, containerPort: 7233 },
    });
    const link = new apigateway.CfnVpcLink(this, 'VpcLink', { name: 'capstan', subnetIds: network.vpc.selectSubnets({ subnetType: SubnetType.PUBLIC }).subnetIds, securityGroupIds: [network.linkGroup.securityGroupId] });
    const api = new apigateway.CfnApi(this, 'HttpApi', { name: 'capstan', protocolType: 'HTTP', disableExecuteApiEndpoint: false });
    const integration = new apigateway.CfnIntegration(this, 'CloudMapIntegration', {
      apiId: api.ref, connectionId: link.ref, connectionType: 'VPC_LINK',
      integrationType: 'HTTP_PROXY', integrationMethod: 'ANY', integrationUri: service.cloudMapService!.serviceArn,
      payloadFormatVersion: '1.0', timeoutInMillis: 30000,
      requestParameters: { 'overwrite:path': '$request.path' },
    });
    // Expose only health and RPC paths. /metrics remains inside the VPC.
    for (const [name, routeKey] of Object.entries({ Health: 'GET /healthz', Ready: 'GET /readyz', Client: 'POST /capstan.v1.ClientService/{method}', Worker: 'POST /capstan.v1.WorkerService/{method}' })) {
      new apigateway.CfnRoute(this, name, { apiId: api.ref, routeKey, authorizationType: 'NONE', target: `integrations/${integration.ref}` });
    }
    const accessLogs = new LogGroup(this, 'AccessLogs', { logGroupName: '/capstan/http-api', retention: RetentionDays.ONE_WEEK, removalPolicy: RemovalPolicy.DESTROY });
    new apigateway.CfnStage(this, 'Stage', {
      apiId: api.ref, stageName: '$default', autoDeploy: true,
      defaultRouteSettings: { throttlingBurstLimit: 20, throttlingRateLimit: 10 },
      accessLogSettings: { destinationArn: accessLogs.logGroupArn, format: JSON.stringify({ requestId: '$context.requestId', route: '$context.routeKey', status: '$context.status', latency: '$context.responseLatency', integrationStatus: '$context.integrationStatus' }) },
    });
    const alerts = new Topic(this, 'Alerts', { topicName: 'capstan-alerts' });
    alerts.addSubscription(new EmailSubscription(props.email));
    const action = new SnsAction(alerts);
    const errors = new cloudwatch.Metric({ namespace: 'AWS/ApiGateway', metricName: '5xx', dimensionsMap: { ApiId: api.ref }, statistic: 'Sum', period: Duration.minutes(5) });
    const requests = new cloudwatch.Metric({ namespace: 'AWS/ApiGateway', metricName: 'Count', dimensionsMap: { ApiId: api.ref }, statistic: 'Sum', period: Duration.minutes(5) });
    const gatewayAlarm = new cloudwatch.Alarm(this, 'GatewayErrors', {
      alarmName: 'capstan-http-5xx-rate',
      metric: new cloudwatch.MathExpression({ expression: 'IF(requests > 0, 100 * errors / requests, 0)', usingMetrics: { errors, requests }, period: Duration.minutes(5) }),
      threshold: 5, evaluationPeriods: 1, comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });
    gatewayAlarm.addAlarmAction(action);
    const probeLogs = new LogGroup(this, 'ProbeLogs', { logGroupName: '/aws/lambda/capstan-task-count', retention: RetentionDays.ONE_WEEK, removalPolicy: RemovalPolicy.DESTROY });
    const probeRole = new Role(this, 'ProbeRole', { roleName: 'capstan-task-count', assumedBy: new ServicePrincipal('lambda.amazonaws.com'), permissionsBoundary });
    probeLogs.grantWrite(probeRole);
    probeRole.addToPolicy(new PolicyStatement({ actions: ['ecs:DescribeServices'], resources: [service.serviceArn] }));
    probeRole.addToPolicy(new PolicyStatement({ actions: ['cloudwatch:PutMetricData'], resources: ['*'], conditions: { StringEquals: { 'cloudwatch:namespace': 'Capstan/Service' } } }));
    const probe = new lambda.Function(this, 'TaskCountProbe', {
      functionName: 'capstan-task-count', runtime: lambda.Runtime.PYTHON_3_13, architecture: lambda.Architecture.ARM_64,
      handler: 'index.handler', role: probeRole, memorySize: 128, timeout: Duration.seconds(10),
      logGroup: probeLogs,
      code: lambda.Code.fromInline(readFileSync(new URL('../runtime/task-count.py', import.meta.url), 'utf8')),
      environment: { CLUSTER: cluster.clusterName, SERVICE: service.serviceName },
    });
    new Rule(this, 'ProbeSchedule', { ruleName: 'capstan-task-count', schedule: Schedule.rate(Duration.minutes(1)), targets: [new LambdaFunction(probe)] });
    const taskAlarm = new cloudwatch.Alarm(this, 'MissingTask', {
      alarmName: 'capstan-no-running-task',
      metric: new cloudwatch.Metric({ namespace: 'Capstan/Service', metricName: 'RunningTaskCount', dimensionsMap: { ClusterName: cluster.clusterName, ServiceName: service.serviceName }, statistic: 'Minimum', period: Duration.minutes(1) }),
      threshold: 1, evaluationPeriods: 3, datapointsToAlarm: 2,
      comparisonOperator: cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD, treatMissingData: cloudwatch.TreatMissingData.BREACHING,
    });
    taskAlarm.addAlarmAction(action);
    new CfnOutput(this, 'ApiUrl', { value: `https://${api.ref}.execute-api.${this.region}.${this.urlSuffix}` });
  }
}
