import { CfnOutput, Fn, Stack } from 'aws-cdk-lib';
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
    const outputs: Record<string, string> = {
      VpcId: this.vpc.vpcId, LinkGroupId: this.linkGroup.securityGroupId,
      TaskGroupId: this.taskGroup.securityGroupId, DatabaseGroupId: this.databaseGroup.securityGroupId,
    };
    this.vpc.publicSubnets.forEach((subnet, i) => {
      outputs[`PublicSubnet${i + 1}Id`] = subnet.subnetId;
      outputs[`PublicSubnet${i + 1}Az`] = subnet.availabilityZone;
      outputs[`PublicSubnet${i + 1}RouteTableId`] = subnet.routeTable.routeTableId;
    });
    for (const [name, value] of Object.entries(outputs)) new CfnOutput(this, name, { value, exportName: `CapstanNetwork:${name}` });
  }
}

// The owner provisions networking before GitHub deploys the workload. Both entry
// points consume the same exports, so switching credentials does not change templates.
export function importNetwork(scope: Construct) {
  const value = (name: string) => Fn.importValue(`CapstanNetwork:${name}`);
  const vpc = Vpc.fromVpcAttributes(scope, 'Network', {
    vpcId: value('VpcId'),
    availabilityZones: [value('PublicSubnet1Az'), value('PublicSubnet2Az')],
    publicSubnetIds: [value('PublicSubnet1Id'), value('PublicSubnet2Id')],
    publicSubnetRouteTableIds: [value('PublicSubnet1RouteTableId'), value('PublicSubnet2RouteTableId')],
  });
  const group = (name: string) => SecurityGroup.fromSecurityGroupId(scope, name, value(`${name}Id`), { mutable: false });
  return { vpc, linkGroup: group('LinkGroup'), taskGroup: group('TaskGroup'), databaseGroup: group('DatabaseGroup') };
}
