// Runbook destroy order, guarded by this lane's pre-deploy inventory/session receipt.
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { root } from './evidence-lib.mjs';
import { localProofDirectory, requestLocalShutdown } from './demo-codex-lanes-aws-local.mjs';
const sessionPath = join(root, '.lane/aws-session-2026-09-28.json');
const session = JSON.parse(readFileSync(sessionPath, 'utf8'));
const baseline = JSON.parse(readFileSync(join(root, '.lane/aws-baseline-2026-09-28.json'), 'utf8'));
const stackNames = ['CapstanService', 'CapstanData', 'CapstanNetwork', 'CapstanBudget', 'CapstanGithubOidc'];
assert.equal(session.account, '280517746513'); assert(session.startedAt);
assert(!baseline.resources.stacks.some(s => stackNames.includes(s.Name)), 'refuse to delete a preexisting stack');
const env = { ...process.env, AWS_PROFILE: 'agentops', AWS_REGION: 'us-east-1', AWS_PAGER: '' };
for (const key of ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN']) delete env[key];
const aws = args => spawnSync('aws', ['--profile', 'agentops', '--region', 'us-east-1', ...args, '--output', 'json'], { env, encoding: 'utf8', timeout: 60_000 });
const identity = aws(['sts', 'get-caller-identity']); assert.equal(identity.status, 0, identity.stderr);
assert.equal(JSON.parse(identity.stdout).Account, session.account);
await requestLocalShutdown(localProofDirectory(root, session.startedAt), session.startedAt);
console.log('Local proof is absent or has verified worker/lane cleanup. AWS teardown may begin.');
console.log(`${new Date().toISOString()} Destroy starting; AWS_PROFILE=agentops, account ${session.account}, region us-east-1.`);
for (const names of [['CapstanService'], ['CapstanData'], ['CapstanNetwork', 'CapstanBudget'], ['CapstanGithubOidc']]) {
  const existing = [];
  for (const name of names) {
    const result = aws(['cloudformation', 'describe-stacks', '--stack-name', name, '--query', 'Stacks[0].{Name:StackName,Created:CreationTime,Status:StackStatus}']);
    if (result.status !== 0 && result.stderr.includes('does not exist')) { console.log(`${name}: absent`); continue; }
    assert.equal(result.status, 0, result.stderr);
    const stack = JSON.parse(result.stdout);
    assert(Date.parse(stack.Created) >= Date.parse(session.startedAt) - 5000, `refuse to delete older stack ${name}`);
    existing.push(name);
  }
  if (!existing.length) continue;
  const args = ['cdk', 'destroy', ...existing, '--force', '--profile', 'agentops', '--no-lookups', '-c', 'imageTag=cleanup', '-c', 'budgetEmail=affanyasir9@gmail.com', '-c', 'existingGithubProvider=true'];
  console.log(`$ AWS_PROFILE=agentops npx ${args.join(' ')}`);
  const result = spawnSync('npx', args, { cwd: join(root, 'infra'), env, stdio: 'inherit', timeout: 60 * 60_000 });
  assert.equal(result.status, 0, `destroy failed for ${existing.join(', ')}; inspect the named resource before retrying`);
}
session.destroyCommandsFinishedAt = new Date().toISOString();
writeFileSync(sessionPath, JSON.stringify(session, null, 2) + '\n');
console.log(`${session.destroyCommandsFinishedAt} CDK destroy commands finished. Full inventory verification is still required.`);
