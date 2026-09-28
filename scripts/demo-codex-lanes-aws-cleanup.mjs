// Runbook destroy order, guarded by this lane's pre-deploy inventory/session receipt.
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { root } from './evidence-lib.mjs';
import { localProofDirectory, requestLocalShutdown } from './demo-codex-lanes-aws-local.mjs';
const sessionPath = process.argv[2] ?? join(root, '.lane/aws-session-2026-09-28.json');
const session = JSON.parse(readFileSync(sessionPath, 'utf8'));
const baseline = JSON.parse(readFileSync(join(root, session.baselineFile ?? '.lane/aws-baseline-2026-09-28.json'), 'utf8'));
const evidencePrefix = session.evidencePrefix ?? 'aws-2026-09-28';
assert(/^aws-[a-z0-9-]+$/.test(evidencePrefix));
const stackNames = ['CapstanService', 'CapstanData', 'CapstanNetwork', 'CapstanBudget', 'CapstanGithubOidc'];
assert.equal(session.account, '280517746513'); assert(session.startedAt);
const hardDeadline = Math.min(Date.parse(session.hardDeadline), Date.parse(session.startedAt) + 4 * 60 * 60_000);
assert(Number.isFinite(hardDeadline), 'cleanup requires the original hard deadline');
assert(!baseline.resources.stacks.some(s => stackNames.includes(s.Name)), 'refuse to delete a preexisting stack');
const env = { ...process.env, AWS_PROFILE: 'agentops', AWS_REGION: 'us-east-1', AWS_PAGER: '' };
for (const key of ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN']) delete env[key];
const aws = args => spawnSync('aws', ['--profile', 'agentops', '--region', 'us-east-1', ...args, '--output', 'json'], { env, encoding: 'utf8', timeout: 60_000 });
const identity = aws(['sts', 'get-caller-identity']); assert.equal(identity.status, 0, identity.stderr);
assert.equal(JSON.parse(identity.stdout).Account, session.account);
await requestLocalShutdown(localProofDirectory(root, session.startedAt), session.startedAt);
console.log('Local proof is absent or has verified worker/lane cleanup. AWS teardown may begin.');
const inventory = (suffix, previous) => {
  const path = join(root, `docs/evidence/${evidencePrefix}-${suffix}.json`);
  const args = [join(root, 'scripts/demo-codex-lanes-aws-inventory.mjs'), path, ...(previous ? [previous] : [])];
  const result = spawnSync(process.execPath, args, { cwd: root, env, encoding: 'utf8', timeout: 180_000 });
  assert.equal(result.status, 0, result.stderr || `inventory failed: ${suffix}`);
  console.log(`Saved ${suffix} inventory: ${path}`);
  return path;
};
const beforePath = join(root, `docs/evidence/${evidencePrefix}-cleanup-before.json`);
if (!existsSync(beforePath)) inventory('cleanup-before');
console.log(`${new Date().toISOString()} Destroy starting; AWS_PROFILE=agentops, account ${session.account}, region us-east-1.`);
const groups = [['CapstanService'], ['CapstanData'], ['CapstanNetwork', 'CapstanBudget'], ['CapstanGithubOidc']];
for (const [index, names] of groups.entries()) {
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
  // Reserve ten minutes for the remaining API calls and verification, and share
  // the available wait among groups. After the deadline, keep short recovery attempts.
  const available = Math.max(0, hardDeadline - Date.now() - 10 * 60_000);
  const timeout = Math.max(30_000, Math.min(15 * 60_000, Math.floor(available / (groups.length - index))));
  const args = [join(root, 'infra/node_modules/aws-cdk/bin/cdk'), 'destroy', ...existing, '--force', '--profile', 'agentops', '--no-lookups', '-c', 'imageTag=cleanup', '-c', 'budgetEmail=affanyasir9@gmail.com', '-c', 'existingGithubProvider=true'];
  console.log(`$ AWS_PROFILE=agentops ${process.execPath} ${args.join(' ')} (wait limit ${timeout / 1000}s)`);
  const result = spawnSync(process.execPath, args, { cwd: join(root, 'infra'), env, stdio: 'inherit', timeout, killSignal: 'SIGKILL' });
  assert.equal(result.status, 0, `destroy failed for ${existing.join(', ')}; inspect the named resource before retrying`);
}
session.destroyCommandsFinishedAt ??= new Date().toISOString();
writeFileSync(sessionPath, JSON.stringify(session, null, 2) + '\n');
const variables = spawnSync('gh', ['variable', 'list', '--repo', 'Aly700/capstan', '--json', 'name,value'], { env, encoding: 'utf8', timeout: 60_000 });
assert.equal(variables.status, 0, variables.stderr);
const ownedVariables = ['CAPSTAN_AWS_ACCOUNT_ID', 'CAPSTAN_DEPLOY_ROLE_ARN', 'CAPSTAN_BUDGET_EMAIL'];
for (const variable of JSON.parse(variables.stdout).filter(v => ownedVariables.includes(v.name))) {
  const deleted = spawnSync('gh', ['variable', 'delete', variable.name, '--repo', 'Aly700/capstan'], { env, encoding: 'utf8', timeout: 60_000 });
  assert.equal(deleted.status, 0, deleted.stderr);
  console.log(`Removed GitHub variable ${variable.name}`);
}
const remainingVariables = spawnSync('gh', ['variable', 'list', '--repo', 'Aly700/capstan', '--json', 'name,value'], { env, encoding: 'utf8', timeout: 60_000 });
assert.equal(remainingVariables.status, 0, remainingVariables.stderr);
session.githubVariablesAfter = JSON.parse(remainingVariables.stdout);
assert(!session.githubVariablesAfter.some(v => ownedVariables.includes(v.name)), 'deployment variables remain');
writeFileSync(join(root, `docs/evidence/${evidencePrefix}-github-variables-after.json`), JSON.stringify(session.githubVariablesAfter, null, 2) + '\n');
const afterPath = inventory('teardown', beforePath);
const after = JSON.parse(readFileSync(afterPath, 'utf8'));
assert.deepEqual(after.errors, {});
assert(!after.resources.stacks.some(s => stackNames.includes(s.Name)), 'Capstan stacks remain');
for (const [category, resources] of Object.entries(after.resources)) {
  if (!['stacks', 'sharedOidcProvider'].includes(category)) assert.deepEqual(resources, [], `resources remain: ${category}`);
}
session.cleanedAt = new Date().toISOString();
session.teardownVerified = true;
writeFileSync(sessionPath, JSON.stringify(session, null, 2) + '\n');
console.log(JSON.stringify(after.resources, null, 2));
console.log(`${session.cleanedAt} Runbook teardown and inventory verification finished.`);
