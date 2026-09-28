import { Duration, Fn, RemovalPolicy, SecretValue, Stack } from 'aws-cdk-lib';
import type { StackProps } from 'aws-cdk-lib';
import { InstanceClass, InstanceSize, InstanceType, SubnetType } from 'aws-cdk-lib/aws-ec2';
import { Credentials, DatabaseInstance, DatabaseInstanceEngine, PostgresEngineVersion, StorageType } from 'aws-cdk-lib/aws-rds';
import { Secret } from 'aws-cdk-lib/aws-secretsmanager';
import type { Construct } from 'constructs';
import type { CapstanNetwork } from './network.ts';

export class CapstanData extends Stack {
  readonly databaseUrl: Secret;
  readonly apiKeyHashes: Secret;
  readonly gateKey: Secret;

  constructor(scope: Construct, id: string, props: StackProps & { network: CapstanNetwork }) {
    super(scope, id, props);
    const credentials = new Secret(this, 'Credentials', {
      secretName: 'capstan/database-credentials',
      generateSecretString: { secretStringTemplate: '{"username":"capstan"}', generateStringKey: 'password', passwordLength: 40, excludePunctuation: true },
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const database = new DatabaseInstance(this, 'Database', {
      instanceIdentifier: 'capstan',
      engine: DatabaseInstanceEngine.postgres({ version: PostgresEngineVersion.VER_16_13 }),
      instanceType: InstanceType.of(InstanceClass.T4G, InstanceSize.MICRO),
      credentials: Credentials.fromSecret(credentials),
      databaseName: 'capstan',
      vpc: props.network.vpc,
      vpcSubnets: { subnetType: SubnetType.PUBLIC },
      securityGroups: [props.network.databaseGroup],
      availabilityZone: props.network.vpc.publicSubnets[0].availabilityZone,
      multiAz: false,
      publiclyAccessible: false,
      allocatedStorage: 20,
      storageType: StorageType.GP3,
      storageEncrypted: true,
      backupRetention: Duration.days(0),
      deleteAutomatedBackups: true,
      deletionProtection: false,
      removalPolicy: RemovalPolicy.DESTROY,
      autoMinorVersionUpgrade: true,
      enablePerformanceInsights: false,
    });
    // CloudFormation resolves the reference into Secrets Manager; synthesis never reads it.
    this.databaseUrl = new Secret(this, 'DatabaseUrl', {
      secretName: 'capstan/database-url',
      secretStringValue: SecretValue.unsafePlainText(Fn.join('', [
        'postgres://capstan:', credentials.secretValueFromJson('password').unsafeUnwrap(),
        '@', database.dbInstanceEndpointAddress, ':5432/capstan?sslmode=require',
      ])),
      removalPolicy: RemovalPolicy.DESTROY,
    });
    // Random initial values deliberately cannot authenticate a client. The owner supplies
    // the real hash list and Gate key through Secrets Manager before starting the service.
    this.apiKeyHashes = new Secret(this, 'ApiKeyHashes', { secretName: 'capstan/api-key-hashes', removalPolicy: RemovalPolicy.DESTROY });
    this.gateKey = new Secret(this, 'GateKey', { secretName: 'capstan/gate-api-key', removalPolicy: RemovalPolicy.DESTROY });
  }
}
