import { Stack } from 'aws-cdk-lib';
import type { StackProps } from 'aws-cdk-lib';
import { CfnBudget } from 'aws-cdk-lib/aws-budgets';
import type { Construct } from 'constructs';

export class CapstanBudget extends Stack {
  constructor(scope: Construct, id: string, props: StackProps & { email: string }) {
    super(scope, id, props);
    new CfnBudget(this, 'Monthly', {
      budget: { budgetName: 'capstan-monthly', budgetType: 'COST', timeUnit: 'MONTHLY', budgetLimit: { amount: 10, unit: 'USD' } },
      notificationsWithSubscribers: [50, 90, 100].map(threshold => ({
        notification: { comparisonOperator: 'GREATER_THAN', notificationType: 'ACTUAL', threshold, thresholdType: 'PERCENTAGE' },
        subscribers: [{ address: props.email, subscriptionType: 'EMAIL' }],
      })),
    });
  }
}
