import { LegacyStackSynthesizer, Stack, Tags } from 'aws-cdk-lib';
import type { App, StackProps } from 'aws-cdk-lib';
import { CapstanBudget } from './budget.ts';
import { CapstanData } from './data.ts';
import { CapstanNetwork } from './network.ts';
import { CapstanGithubOidc } from './oidc.ts';
import { CapstanService } from './service.ts';

export function createStacks(app: App): Record<string, Stack> {
  const context = (name: string) => app.node.tryGetContext(name);
  for (const key of Object.keys(app.node.getAllContext())) {
    if (!key.startsWith('@') && /secret|password|apikey|databaseurl|credential/i.test(key)) throw new Error(`sensitive context is forbidden: ${key}`);
  }
  const imageTag = context('imageTag');
  const email = context('budgetEmail');
  const gateUrl = context('gateUrl') ?? '';
  if (typeof imageTag !== 'string' || !/^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$/.test(imageTag)) throw new Error('imageTag must be a valid ECR tag');
  if (typeof email !== 'string' || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) throw new Error('budgetEmail is required');
  if (gateUrl && (typeof gateUrl !== 'string' || !/^https:\/\/[^\s/]+/.test(gateUrl) || new URL(gateUrl).username || new URL(gateUrl).password || new URL(gateUrl).search)) throw new Error('gateUrl must be HTTPS without credentials or query parameters');
  const workloadOnly = context('workloadOnly') === 'true';
  const props = (): StackProps => ({
    env: { region: 'us-east-1' },
    analyticsReporting: false,
    // All templates are inline and the image is pushed separately. No bootstrap
    // assets or assumed bootstrap roles; deploy.yml passes the execution role.
    synthesizer: new LegacyStackSynthesizer(),
  });
  const network = workloadOnly ? undefined : new CapstanNetwork(app, 'CapstanNetwork', props());
  const data = new CapstanData(app, 'CapstanData', props());
  const budget = new CapstanBudget(app, 'CapstanBudget', { ...props(), email });
  const service = new CapstanService(app, 'CapstanService', { ...props(), data, imageTag, email, gateUrl });
  service.addStackDependency(budget);
  const stacks: Record<string, Stack> = { data, service, budget };
  if (network) {
    data.addStackDependency(network);
    service.addStackDependency(network);
    stacks.network = network;
  }
  if (!workloadOnly) {
    stacks.oidc = new CapstanGithubOidc(app, 'CapstanGithubOidc', { ...props(), existingProvider: context('existingGithubProvider') === 'true' });
    service.addStackDependency(stacks.oidc);
  }
  for (const stack of Object.values(stacks)) Tags.of(stack).add('Project', 'capstan');
  return stacks;
}
