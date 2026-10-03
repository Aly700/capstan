import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const root = fileURLToPath(new URL('../../', import.meta.url));

test('deployment requires a manual main-branch dispatch and rejects the server stub before OIDC', () => {
  assert.ok(existsSync(`${root}.github/workflows/deploy.yml`), 'manual deployment workflow is required');
  const workflow = readFileSync(`${root}.github/workflows/deploy.yml`, 'utf8');
  assert.match(workflow, /on:\s+workflow_dispatch:/);
  assert.match(workflow, /if: github.ref == 'refs\/heads\/main'/);
  assert.match(workflow, /id-token: write/);
  assert.match(workflow, /cancel-in-progress: false/);
  assert.match(workflow, /role-to-assume: \$\{\{ vars.CAPSTAN_DEPLOY_ROLE_ARN \}\}/);
  assert.ok(workflow.indexOf('not implemented yet (server stub)') < workflow.indexOf('aws-actions/configure-aws-credentials'));
  assert.doesNotMatch(workflow, /pull_request|push:\s*$|AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|secrets\./m);
  assert.match(workflow, /aws-actions\/configure-aws-credentials@v6/);
  assert.match(workflow, /allowed-account-ids: \$\{\{ vars.CAPSTAN_AWS_ACCOUNT_ID \}\}/, 'the credentials step refuses any other account');
});

function deploy(mode = 'missing', identity = 'expected') {
  assert.ok(existsSync(`${root}.github/scripts/deploy.sh`), 'deployment script is required');
  mkdirSync(`${root}.lane`, { recursive: true });
  const dir = mkdtempSync(`${root}.lane/deploy-test-`);
  const trace = `${dir}/trace.jsonl`;
  for (const name of ['aws', 'docker', 'npx', 'curl']) {
    writeFileSync(`${dir}/${name}`, `#!/usr/bin/env node
import {appendFileSync,writeFileSync,readFileSync} from 'node:fs';
const tool=${JSON.stringify(name)}, args=process.argv.slice(2);
appendFileSync(process.env.TRACE,JSON.stringify({tool,args})+'\\n');
if(tool==='aws' && args.includes('get-caller-identity')) {
  if(process.env.IDENTITY_MODE==='denied') { console.error('AccessDenied'); process.exit(254); }
  console.log(process.env.IDENTITY_MODE==='wrong'?'111111111111':'000000000000'); process.exit(0);
}
if(tool==='aws' && args.includes('describe-images')) {
  if(process.env.IMAGE_MODE==='existing') process.exit(0);
  console.error(process.env.IMAGE_MODE==='denied'?'AccessDeniedException':'ImageNotFoundException'); process.exit(254);
}
if(tool==='aws' && args.includes('get-login-password')) console.log('fake-test-password');
if(tool==='docker' && args.includes('login')) readFileSync(0);
if(tool==='npx') { const file=args[args.indexOf('--outputs-file')+1]; writeFileSync(file,JSON.stringify({CapstanService:{ApiUrl:'https://test.execute-api.us-east-1.amazonaws.com'}})); }
`, { mode: 0o755 });
  }
  const env = Object.fromEntries(Object.entries(process.env).filter(([name]) => !name.startsWith('AWS_')));
  const result = spawnSync('bash', [`${root}.github/scripts/deploy.sh`], {
    cwd: root, encoding: 'utf8', env: { ...env, PATH: `${dir}:${process.env.PATH}`, TRACE: trace, IMAGE_MODE: mode, IDENTITY_MODE: identity,
      AWS_ACCOUNT_ID: '000000000000', AWS_REGION: 'us-east-1', COMMIT_SHA: 'a'.repeat(40), BUDGET_EMAIL: 'test@example.com', GATE_URL: '', TMPDIR: dir },
  });
  const calls = existsSync(trace) ? readFileSync(trace, 'utf8').trim().split('\n').map(JSON.parse) : [];
  return { result, calls };
}

test('wrong or unreadable AWS identity stops before any image or deployment operation', () => {
  for (const identity of ['wrong', 'denied']) {
    const { result, calls } = deploy('missing', identity);
    assert.notEqual(result.status, 0, `identity ${identity} must fail closed`);
    assert.equal(calls.length, 1, 'only the STS identity check may run');
    assert.equal(calls[0].tool, 'aws');
    assert.ok(calls[0].args.includes('get-caller-identity'));
  }
});

test('deployment pushes ARM64 SHA image before deploying three workload stacks and smokes HTTPS', () => {
  const { result, calls } = deploy();
  assert.equal(result.status, 0, result.stdout + result.stderr);
  const build = calls.find(c => c.tool === 'docker' && c.args.includes('build'));
  assert.ok(build);
  assert.ok(build.args.includes('linux/arm64'));
  assert.ok(build.args.includes('--push'));
  assert.ok(build.args.some(a => a.endsWith(`capstan-server:${'a'.repeat(40)}`)));
  const cdk = calls.find(c => c.tool === 'npx');
  assert.ok(calls.indexOf(build) < calls.indexOf(cdk));
  assert.ok(cdk.args.includes('--all'));
  assert.ok(cdk.args.includes('workloadOnly=true'));
  assert.ok(cdk.args.includes('--role-arn'));
  assert.ok(cdk.args.includes('arn:aws:iam::000000000000:role/capstan-cloudformation'));
  assert.ok(cdk.args.includes('never'));
  assert.deepEqual(calls.filter(c => c.tool === 'curl').map(c => c.args.at(-1)), ['https://test.execute-api.us-east-1.amazonaws.com/healthz', 'https://test.execute-api.us-east-1.amazonaws.com/readyz']);
  assert.ok(calls.filter(c => c.tool === 'curl').every(c => c.args.includes('--fail')));
});

test('rerunning a SHA reuses its immutable image but an ECR authorization failure stops deployment', () => {
  const existing = deploy('existing');
  assert.equal(existing.result.status, 0, existing.result.stderr);
  assert.equal(existing.calls.filter(c => c.tool === 'docker').length, 0);
  const denied = deploy('denied');
  assert.notEqual(denied.result.status, 0);
  assert.equal(denied.calls.filter(c => ['npx', 'docker', 'curl'].includes(c.tool)).length, 0);
});

test('runbook prices every cost row, explains the ceiling and covers first deploy, rotation, alarms and cleanup', () => {
  assert.ok(existsSync(`${root}docs/runbook.md`), 'runbook is required');
  const doc = readFileSync(`${root}docs/runbook.md`, 'utf8');
  for (const term of ['CapstanGithubOidc', 'workloadOnly=true', 'capstan/api-key-hashes', 'force-new-deployment', 'capstan-http-5xx-rate', 'capstan-no-running-task', 'cdk destroy', '20 s', '30 s', 'not a hard spending cap', '730', 'hour', '50%', '90%', '100%', 'Route 53', 'CPU credits']) assert.ok(doc.includes(term), `runbook must explain ${term}`);
  const rows = doc.split('\n').filter(line => line.startsWith('| ') && !line.startsWith('| Cost ') && !line.startsWith('| ---'));
  assert.ok(rows.length >= 12);
  for (const row of rows) assert.match(row, /https:\/\/aws\.amazon\.com\/[^ )]+/);
  assert.doesNotMatch(doc, /\bdeterministic\b|AdministratorAccess|cdk bootstrap/);
});
