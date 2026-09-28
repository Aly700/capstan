import { Fn, Stack } from 'aws-cdk-lib';
import type { StackProps } from 'aws-cdk-lib';
import { Peer, Port, SecurityGroup, SubnetType, Vpc } from 'aws-cdk-lib/aws-ec2';
import type { Construct } from 'constructs';

export class CapstanNetwork extends Stack {
  readonly vpc: Vpc;
  readonly linkGroup: SecurityGroup;
  readonly taskGroup: SecurityGroup;
  readonly databaseGroup: SecurityGroup;

  constructor(scope: Construct, id: string, props: StackProps) {
    super(scope, id, props);
    this.vpc = new Vpc(this, 'Vpc', {
      availabilityZones: [Fn.select(0, Fn.getAzs('us-east-1')), Fn.select(1, Fn.getAzs('us-east-1'))],
      natGateways: 0,
      subnetConfiguration: [{ name: 'Public', subnetType: SubnetType.PUBLIC, cidrMask: 24 }],
      restrictDefaultSecurityGroup: false,
    });
    this.linkGroup = new SecurityGroup(this, 'LinkGroup', { vpc: this.vpc, allowAllOutbound: false, disableInlineRules: true });
    this.taskGroup = new SecurityGroup(this, 'TaskGroup', { vpc: this.vpc, allowAllOutbound: false, disableInlineRules: true });
    this.databaseGroup = new SecurityGroup(this, 'DatabaseGroup', { vpc: this.vpc, allowAllOutbound: false, disableInlineRules: true });
    this.linkGroup.addEgressRule(this.taskGroup, Port.tcp(7233), 'HTTP API to server');
    this.taskGroup.addIngressRule(this.linkGroup, Port.tcp(7233), 'Only the HTTP API VPC link');
    this.taskGroup.addEgressRule(this.databaseGroup, Port.tcp(5432), 'PostgreSQL');
    this.databaseGroup.addIngressRule(this.taskGroup, Port.tcp(5432), 'Only the server');
    this.taskGroup.addEgressRule(Peer.anyIpv4(), Port.tcp(443), 'ECR, Secrets Manager, logs and Gate HTTPS');
  }
}
