import { CfnOutput, Duration, RemovalPolicy, Stack } from 'aws-cdk-lib';
import type { StackProps } from 'aws-cdk-lib';
import { Repository, TagMutability } from 'aws-cdk-lib/aws-ecr';
import { CfnOIDCProvider, FederatedPrincipal, ManagedPolicy, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import type { Construct } from 'constructs';

export const workloadNames = ['CapstanData', 'CapstanService', 'CapstanBudget'];

export class CapstanGithubOidc extends Stack {
  constructor(scope: Construct, id: string, props: StackProps & { existingProvider: boolean }) {
    super(scope, id, props);
    const arn = (service: string, resource: string, region = this.region, account = this.account) => `arn:${this.partition}:${service}:${region}:${account}:${resource}`;
    const roleArn = (name: string) => arn('iam', `role/${name}`, '');
    const boundaryArn = arn('iam', 'policy/capstan-workload-boundary', '');
    const repository = new Repository(this, 'Repository', {
      repositoryName: 'capstan-server', imageTagMutability: TagMutability.IMMUTABLE,
      emptyOnDelete: true, removalPolicy: RemovalPolicy.DESTROY,
      lifecycleRules: [{ maxImageCount: 10 }],
    });
    const runtimeRoles = ['capstan-task', 'capstan-execution', 'capstan-task-count'].map(roleArn);
    const runtimeStatements = [
      new PolicyStatement({ actions: ['ecr:GetAuthorizationToken'], resources: ['*'] }),
      new PolicyStatement({ actions: ['ecr:BatchCheckLayerAvailability', 'ecr:GetDownloadUrlForLayer', 'ecr:BatchGetImage'], resources: [repository.repositoryArn] }),
      new PolicyStatement({ actions: ['secretsmanager:GetSecretValue'], resources: [arn('secretsmanager', 'secret:capstan/database-url-??????'), arn('secretsmanager', 'secret:capstan/api-key-hashes-??????'), arn('secretsmanager', 'secret:capstan/gate-api-key-??????')] }),
      new PolicyStatement({ actions: ['logs:CreateLogStream', 'logs:PutLogEvents'], resources: [arn('logs', 'log-group:/capstan/*:*'), arn('logs', 'log-group:/aws/lambda/capstan-task-count:*')] }),
      new PolicyStatement({ actions: ['ecs:DescribeServices'], resources: [arn('ecs', 'service/capstan/capstan-server')] }),
      new PolicyStatement({ actions: ['cloudwatch:PutMetricData'], resources: ['*'], conditions: { StringEquals: { 'cloudwatch:namespace': 'Capstan/Service' } } }),
    ];
    new ManagedPolicy(this, 'WorkloadBoundary', { managedPolicyName: 'capstan-workload-boundary', statements: runtimeStatements });
    const execution = new Role(this, 'CloudFormationExecution', { roleName: 'capstan-cloudformation', assumedBy: new ServicePrincipal('cloudformation.amazonaws.com') });
    const allow = (actions: string[], resources: string[], conditions?: Record<string, Record<string, unknown>>) => execution.addToPolicy(new PolicyStatement({ actions, resources, conditions }));
    // Networking belongs to the owner-managed foundation. Workload providers may
    // describe it; their service-linked roles manage service ENIs, not this role.
    allow(['ec2:DescribeVpcs', 'ec2:DescribeVpcAttribute', 'ec2:DescribeSubnets', 'ec2:DescribeRouteTables', 'ec2:DescribeInternetGateways', 'ec2:DescribeSecurityGroups', 'ec2:DescribeSecurityGroupRules', 'ec2:DescribeAvailabilityZones', 'ec2:DescribeNetworkInterfaces', 'ec2:DescribeRegions'], ['*'], { StringEquals: { 'aws:RequestedRegion': this.region } });
    allow(['rds:CreateDBInstance', 'rds:ModifyDBInstance', 'rds:DeleteDBInstance', 'rds:DescribeDBInstances', 'rds:AddTagsToResource', 'rds:RemoveTagsFromResource', 'rds:ListTagsForResource', 'rds:CreateDBSubnetGroup', 'rds:ModifyDBSubnetGroup', 'rds:DeleteDBSubnetGroup', 'rds:DescribeDBSubnetGroups'], [arn('rds', 'db:capstan'), arn('rds', 'subgrp:capstandata-*')]);
    allow(['secretsmanager:CreateSecret', 'secretsmanager:DeleteSecret', 'secretsmanager:DescribeSecret', 'secretsmanager:UpdateSecret', 'secretsmanager:PutSecretValue', 'secretsmanager:GetSecretValue', 'secretsmanager:TagResource', 'secretsmanager:UntagResource', 'secretsmanager:GetResourcePolicy'], [arn('secretsmanager', 'secret:capstan/*')]);
    allow(['secretsmanager:GetRandomPassword'], ['*']);
    allow(['ecs:CreateCluster', 'ecs:DeleteCluster', 'ecs:DescribeClusters', 'ecs:UpdateClusterSettings', 'ecs:CreateService', 'ecs:UpdateService', 'ecs:DeleteService', 'ecs:DescribeServices', 'ecs:DescribeTaskDefinition', 'ecs:DeregisterTaskDefinition', 'ecs:TagResource', 'ecs:UntagResource'], [arn('ecs', 'cluster/capstan'), arn('ecs', 'service/capstan/*'), arn('ecs', 'task-definition/capstan-server:*')]);
    allow(['ecs:RegisterTaskDefinition'], ['*'], { StringEquals: { 'aws:RequestedRegion': this.region } });
    allow(['servicediscovery:CreateService', 'servicediscovery:GetNamespace', 'servicediscovery:GetService', 'servicediscovery:UpdateService', 'servicediscovery:DeleteNamespace', 'servicediscovery:DeleteService', 'servicediscovery:TagResource', 'servicediscovery:UntagResource', 'servicediscovery:ListTagsForResource'], [arn('servicediscovery', 'namespace/*'), arn('servicediscovery', 'service/*')]);
    allow(['servicediscovery:CreatePrivateDnsNamespace', 'servicediscovery:GetOperation', 'servicediscovery:ListNamespaces', 'servicediscovery:ListServices'], ['*'], { StringEquals: { 'aws:RequestedRegion': this.region } });
    allow(['route53:CreateHostedZone', 'route53:GetHostedZone', 'route53:DeleteHostedZone', 'route53:ChangeResourceRecordSets', 'route53:ListResourceRecordSets', 'route53:GetChange', 'route53:ListHostedZonesByName'], ['*']);
    allow(['apigateway:GET', 'apigateway:POST', 'apigateway:PUT', 'apigateway:PATCH', 'apigateway:DELETE'], [arn('apigateway', '/apis*', this.region, ''), arn('apigateway', '/vpclinks*', this.region, ''), arn('apigateway', '/tags/*', this.region, '')]);
    allow(['logs:CreateLogGroup', 'logs:DeleteLogGroup', 'logs:PutRetentionPolicy', 'logs:DeleteRetentionPolicy', 'logs:TagResource', 'logs:UntagResource', 'logs:ListTagsForResource'], [arn('logs', 'log-group:/capstan/*'), arn('logs', 'log-group:/aws/lambda/capstan-task-count*')]);
    allow(['logs:CreateLogDelivery', 'logs:GetLogDelivery', 'logs:UpdateLogDelivery', 'logs:DeleteLogDelivery', 'logs:ListLogDeliveries', 'logs:PutResourcePolicy', 'logs:DescribeResourcePolicies', 'logs:DescribeLogGroups'], ['*']);
    allow(['cloudwatch:PutMetricAlarm', 'cloudwatch:DeleteAlarms', 'cloudwatch:DescribeAlarms', 'cloudwatch:TagResource', 'cloudwatch:UntagResource'], [arn('cloudwatch', 'alarm:capstan-*')]);
    allow(['lambda:CreateFunction', 'lambda:DeleteFunction', 'lambda:GetFunction', 'lambda:GetFunctionConfiguration', 'lambda:UpdateFunctionCode', 'lambda:UpdateFunctionConfiguration', 'lambda:AddPermission', 'lambda:RemovePermission', 'lambda:GetPolicy', 'lambda:TagResource', 'lambda:UntagResource'], [arn('lambda', 'function:capstan-task-count')]);
    allow(['events:PutRule', 'events:DeleteRule', 'events:DescribeRule', 'events:PutTargets', 'events:RemoveTargets', 'events:ListTargetsByRule', 'events:TagResource', 'events:UntagResource'], [arn('events', 'rule/capstan-task-count')]);
    allow(['sns:CreateTopic', 'sns:DeleteTopic', 'sns:GetTopicAttributes', 'sns:SetTopicAttributes', 'sns:Subscribe', 'sns:Unsubscribe', 'sns:GetSubscriptionAttributes', 'sns:SetSubscriptionAttributes', 'sns:TagResource', 'sns:UntagResource'], [arn('sns', 'capstan-alerts'), arn('sns', 'capstan-alerts:*')]);
    allow(['budgets:ViewBudget', 'budgets:ModifyBudget'], [arn('budgets', 'budget/capstan-monthly', '')]);
    allow(['iam:CreateRole'], runtimeRoles, { StringEquals: { 'iam:PermissionsBoundary': boundaryArn } });
    allow(['iam:GetRole', 'iam:DeleteRole', 'iam:UpdateAssumeRolePolicy', 'iam:PutRolePolicy', 'iam:GetRolePolicy', 'iam:DeleteRolePolicy', 'iam:ListRolePolicies', 'iam:TagRole', 'iam:UntagRole'], runtimeRoles);
    allow(['iam:PassRole'], runtimeRoles, { StringEquals: { 'iam:PassedToService': ['ecs-tasks.amazonaws.com', 'lambda.amazonaws.com'] } });
    allow(['iam:CreateServiceLinkedRole'], [arn('iam', 'role/aws-service-role/*', '')], { StringEquals: { 'iam:AWSServiceName': ['ecs.amazonaws.com', 'rds.amazonaws.com', 'ops.apigateway.amazonaws.com'] } });

    const providerArn = props.existingProvider
      ? arn('iam', 'oidc-provider/token.actions.githubusercontent.com', '')
      : new CfnOIDCProvider(this, 'Provider', { url: 'https://token.actions.githubusercontent.com', clientIdList: ['sts.amazonaws.com'] }).attrArn;
    const github = new Role(this, 'Github', {
      roleName: 'capstan-github-deploy', maxSessionDuration: Duration.hours(1),
      assumedBy: new FederatedPrincipal(providerArn, { StringEquals: {
        'token.actions.githubusercontent.com:aud': 'sts.amazonaws.com',
        'token.actions.githubusercontent.com:sub': 'repo:Aly700/capstan:ref:refs/heads/main',
      } }, 'sts:AssumeRoleWithWebIdentity'),
    });
    github.addToPolicy(new PolicyStatement({ actions: ['ecr:GetAuthorizationToken'], resources: ['*'] }));
    github.addToPolicy(new PolicyStatement({ actions: ['ecr:BatchCheckLayerAvailability', 'ecr:InitiateLayerUpload', 'ecr:UploadLayerPart', 'ecr:CompleteLayerUpload', 'ecr:PutImage', 'ecr:BatchGetImage', 'ecr:GetDownloadUrlForLayer', 'ecr:DescribeImages'], resources: [repository.repositoryArn] }));
    github.addToPolicy(new PolicyStatement({ actions: ['cloudformation:CreateStack', 'cloudformation:UpdateStack', 'cloudformation:DescribeStacks', 'cloudformation:DescribeStackEvents', 'cloudformation:GetTemplate', 'cloudformation:GetTemplateSummary', 'cloudformation:CreateChangeSet', 'cloudformation:DescribeChangeSet', 'cloudformation:ExecuteChangeSet', 'cloudformation:DeleteChangeSet', 'cloudformation:ListStackResources', 'cloudformation:TagResource', 'cloudformation:UntagResource'], resources: workloadNames.map(name => arn('cloudformation', `stack/${name}/*`)) }));
    github.addToPolicy(new PolicyStatement({ actions: ['iam:PassRole'], resources: [execution.roleArn], conditions: { StringEquals: { 'iam:PassedToService': 'cloudformation.amazonaws.com' } } }));
    new CfnOutput(this, 'DeployRoleArn', { value: github.roleArn });
    new CfnOutput(this, 'RepositoryUri', { value: repository.repositoryUri });
  }
}
