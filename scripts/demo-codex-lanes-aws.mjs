import assert from 'node:assert/strict';
import { spawn, execFileSync, spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, openSync, closeSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { root, until, delay, decode, payload } from './evidence-lib.mjs';
import { claimLocalProof, localProofDirectory, recordLocalProof } from './demo-codex-lanes-aws-local.mjs';

const sessionPath = join(root, '.lane/aws-session-2026-09-28.json');
const session = JSON.parse(readFileSync(sessionPath, 'utf8'));
assert.equal(session.account, '280517746513');
assert(/^https:\/\/[a-z0-9]+\.execute-api\.us-east-1\.amazonaws\.com$/.test(session.address));
assert(/^[a-f0-9]{40}$/.test(session.deployedSha));
const startedAt = new Date();
const runId = `workload-aws-${Date.now()}`;
const queue = runId;
const repo = join(root, '.lane/demo-repo-aws');
const logdir = join(root, '.lane', runId);
const localDirectory = localProofDirectory(root, session.startedAt);
const localState = { startedAt: session.startedAt, runId, pid: process.pid };
let proofClaimed = false; let cleanupPromise; let shutdownTimer;
const address = session.address;
const keyPath = join(root, '.lane/aws-worker-2026-09-28.key');
assert.equal(statSync(keyPath).mode & 0o777, 0o600);
const key = readFileSync(keyPath, 'utf8').trim();
const children = []; let repoCreated = false; let closing = false;
const ownedLanes = new Map();
mkdirSync(logdir, { recursive: true });
const events = [];
function note(message) {
  const line = `${new Date().toISOString()} ${message}`; events.push(line); console.log(line);
  writeFileSync(join(logdir, 'events.json'), JSON.stringify(events, null, 2));
}
function git(...args) { return execFileSync('git', ['-C', repo, ...args], { encoding: 'utf8' }).trim(); }
const awsEnv = { ...process.env, AWS_PROFILE: 'agentops', AWS_REGION: 'us-east-1', AWS_PAGER: '' };
for (const name of ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN']) delete awsEnv[name];
function aws(...args) {
  return JSON.parse(execFileSync('aws', ['--profile', 'agentops', '--region', 'us-east-1', ...args, '--output', 'json'], { env: awsEnv, encoding: 'utf8', timeout: 60_000 }));
}
function child(binary, args, name, extraEnv) {
  assert(!closing, 'cleanup has started');
  const fd = openSync(join(logdir, `${name}.log`), 'a', 0o600);
  const processChild = spawn(binary, args, { cwd: root, env: { ...process.env, ...extraEnv }, detached: true, stdio: ['ignore', fd, fd] });
  closeSync(fd); children.push(processChild);
  processChild.on('error', error => { processChild.spawnError = error; });
  return processChild;
}
async function stop(processChild, signal = 'SIGTERM') {
  if (!processChild?.pid || processChild.exitCode !== null || processChild.signalCode !== null) return;
  try { process.kill(-processChild.pid, signal); } catch (error) { if (error.code !== 'ESRCH') throw error; }
  try { await until('child exit', () => processChild.exitCode !== null || processChild.signalCode !== null, 5000); }
  catch { process.kill(-processChild.pid, 'SIGKILL'); await until('child killed', () => processChild.signalCode !== null, 5000); }
}
async function rpc(method, body) {
  const response = await fetch(`${address}/capstan.v1.ClientService/${method}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', 'Connect-Protocol-Version': '1', Authorization: `Bearer ${key}` },
    body: JSON.stringify(body), signal: AbortSignal.timeout(35_000),
  });
  if (!response.ok) throw new Error(`${method}: HTTP ${response.status} ${await response.text()}`);
  return response.json();
}
async function history() {
  const result = [];
  for (;;) {
    const page = await rpc('GetHistory', { runId, afterEventId: result.at(-1)?.eventId ?? '0', pageSize: 500 });
    result.push(...(page.events ?? [])); if (!page.more) return result;
    assert(page.events?.length, 'history must advance');
  }
}
function cli(...args) {
  console.log(`$ capstan ${args.join(' ')}`);
  const output = execFileSync(process.execPath, [join(root, 'sdk/bin/capstan.mjs'), ...args], {
    cwd: root, env: { ...process.env, CAPSTAN_ADDRESS: address, CAPSTAN_API_KEY: key }, encoding: 'utf8',
  });
  process.stdout.write(output); return output;
}
function laneStatus(name) {
  const result = spawnSync('codex-lane', ['status', name], { encoding: 'utf8', timeout: 30_000 });
  if (result.error) throw result.error;
  const lines = result.stdout.trim().split('\n');
  const match = lines[0]?.match(/^lane ([a-z0-9-]+): running \(pid (\d+)\)$/);
  return { pid: match ? Number(match[2]) : null, summary: lines.slice(0, 2).join('\n'), finished: lines[0] === `lane ${name}: finished` };
}
function discoverOwnedLanes() {
  const base = join(repo, '.git/capstan-codex-lanes');
  if (!repoCreated || !existsSync(base)) return;
  for (const state of readdirSync(base)) {
    for (const launch of readdirSync(join(base, state)).filter(n => n.startsWith('launch-'))) {
      const path = join(base, state, launch, 'request.json'); if (!existsSync(path)) continue;
      const request = JSON.parse(readFileSync(path, 'utf8'));
      assert(request.key.startsWith(`${runId}/`)); assert(request.laneName.startsWith('cw-'));
      assert(request.worktree.startsWith(`${base}/`)); ownedLanes.set(request.laneName, { ...request, launchDir: join(base, state, launch) });
    }
  }
}
async function cleanup() {
  if (!proofClaimed) return;
  return cleanupPromise ??= cleanupOnce();
}
async function cleanupOnce() {
  closing = true;
  await Promise.all(children.map(c => stop(c)));
  discoverOwnedLanes();
  for (const [name, request] of ownedLanes) {
    const dir = join(homedir(), '.codex/lanes', name);
    if (existsSync(join(dir, 'cwd'))) {
      const cwd = readFileSync(join(dir, 'cwd'), 'utf8').trim();
      assert(!cwd || cwd === request.worktree);
    }
    if (existsSync(dir) && laneStatus(name).pid) {
      execFileSync('codex-lane', ['stop', name], { stdio: 'inherit' });
    }
    // The detached launcher's process group also contains the lane's descendants.
    // Check its command identity before signalling; never match arbitrary codex PIDs.
    const owner = join(request.launchDir, 'claimed/owner.json');
    if (existsSync(owner)) {
      const { pid } = JSON.parse(readFileSync(owner, 'utf8'));
      const members = () => execFileSync('ps', ['-axo', 'pid=,pgid=,command='], { encoding: 'utf8' }).split('\n')
        .map(l => l.trim().match(/^(\d+)\s+(\d+)\s+(.+)$/)).filter(m => m && Number(m[2]) === pid);
      if (members().some(m => m[3].includes(name) || m[3].includes(request.worktree) || m[3].includes(request.launchDir))) {
        try { process.kill(-pid, 'SIGTERM'); } catch (error) { if (error.code !== 'ESRCH') throw error; }
        try { await until('lane process group exit', () => members().length === 0, 5000); }
        catch { process.kill(-pid, 'SIGKILL'); await until('lane process group killed', () => members().length === 0, 5000); }
      }
    }
    // Only remove the exact per-run lane directories whose cwd we verified above.
    rmSync(dir, { recursive: true, force: true });
  }
  if (repoCreated) {
    assert.equal(readFileSync(join(repo, '.git/workload-demo-owner'), 'utf8'), runId);
    const worktrees = git('worktree', 'list', '--porcelain').split('\n').filter(l => l.startsWith('worktree ')).map(l => l.slice(9));
    for (const worktree of worktrees.filter(p => p !== repo)) {
      assert(worktree.startsWith(`${repo}/.git/capstan-codex-lanes/`));
      git('worktree', 'remove', '--force', worktree);
    }
    assert.equal(git('worktree', 'list', '--porcelain').split('\n').filter(l => l.startsWith('worktree ')).length, 1);
    rmSync(repo, { recursive: true }); repoCreated = false;
  }
  assert(children.every(c => c.exitCode !== null || c.signalCode !== null));
  assert(!existsSync(repo));
  for (const name of ownedLanes.keys()) assert(!existsSync(join(homedir(), '.codex/lanes', name)));
  note(`Local cleanup verified: ${children.length} child processes exited; ${ownedLanes.size} demo lane directories removed; demo worktrees/repo removed. AWS resources must now be destroyed in runbook order.`);
  recordLocalProof(localDirectory, { ...localState, cleanedAt: new Date().toISOString() });
  clearInterval(shutdownTimer);
}
let interrupted = false;
const interrupt = () => {
  if (interrupted) return;
  interrupted = true;
  void cleanup().then(() => process.exit(130), error => { console.error(error); process.exit(1); });
};
process.once('SIGINT', interrupt); process.once('SIGTERM', interrupt);

let proof;
try {
  claimLocalProof(localDirectory, localState); proofClaimed = true;
  shutdownTimer = setInterval(() => {
    if (existsSync(join(localDirectory, 'stop-requested'))) interrupt();
  }, 100);
  shutdownTimer.unref();
  note('Real AWS ECS/RDS Capstan + one local SDK worker + two real Codex lanes at xhigh. Stop the ECS server task while the worker stays alive.');
  note(`Deployed SHA ${session.deployedSha}; endpoint ${address}; local source ${execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim()}.`);
  assert.equal(aws('sts', 'get-caller-identity').Account, '280517746513');
  assert(!existsSync(repo), `${repo} already exists; inspect it before rerunning`);
  assert((await fetch(`${address}/readyz`)).ok);
  mkdirSync(repo); repoCreated = true;
  git('init', '-q', '-b', 'main'); git('config', 'user.name', 'Capstan demo'); git('config', 'user.email', 'demo@example.invalid');
  writeFileSync(join(repo, '.git/workload-demo-owner'), runId);
  writeFileSync(join(repo, '.gitignore'), '.lane/\n');
  writeFileSync(join(repo, 'AGENTS.md'), '# Tiny throwaway demo\nThis separate repository contains only two approved shell-script tasks. Implement exactly the supplied brief, test, and commit locally. No planning documents, questions, additional agents, network access, dependency installs, pushes, or merges are needed. Parent Capstan project instructions concern another repository.\n');
  git('add', 'AGENTS.md', '.gitignore'); git('commit', '-qm', 'initialize two-task workload demo');
  const input = {
    repo, brief: 'Implement the following already-approved tiny shell task. Keep it minimal, run the test, and commit your files. Other tasks run in independent worktrees. Do not pause for design approval.',
    lanes: [
      { name: 'greet', branch: 'demo/greet', effort: 'xhigh', brief: 'Create executable greet.sh using POSIX sh. Print Hello, NAME! followed by one newline; NAME is the first argument, default world. Create executable test-greet.sh using sh assertions for default and Ada. Run ./test-greet.sh, git add only these two scripts, commit with message Add greeting script and tests. Final report: commit, files, test output.' },
      { name: 'count', branch: 'demo/count', effort: 'xhigh', brief: 'Create executable count.sh using POSIX sh. Take one filename and print its number of newline characters as a bare integer plus newline, using wc -l with whitespace removed. Create executable test-count.sh using mktemp and trap cleanup, test an empty file and a three-line file. Run ./test-count.sh, git add only these two scripts, commit with message Add line counter and tests. Final report: commit, files, test output.' },
    ],
  };
  function worker(index) {
    const identity = `workload-${index}`;
    return child(process.execPath, ['--import', join(root, 'sdk/node_modules/tsx/dist/loader.mjs'), join(root, 'examples/codex-lanes/worker.ts')], `worker-${index}`, {
      CAPSTAN_ADDRESS: address, CAPSTAN_API_KEY: key, CODEX_LANES_QUEUE: queue, CODEX_LANES_IDENTITY: identity,
    });
  }
  const first = worker(1);
  await rpc('StartRun', { runId, workflowType: 'codexLanes', taskQueue: queue, input: payload(input), runTimeout: '3600s' });
  note(`Started ${runId} through execute-api; local worker PID ${first.pid}.`);
  const running = await until('both real Codex lanes running', () => {
    discoverOwnedLanes();
    if (ownedLanes.size !== 2) return false;
    const statuses = [...ownedLanes.keys()].map(name => ({ name, ...laneStatus(name) }));
    return statuses.every(s => s.pid) ? statuses : false;
  }, 120_000);
  for (const s of running) { console.log(`$ codex-lane status ${s.name}`); console.log(s.summary); }
  const before = await history();
  writeFileSync(join(logdir, 'history-before.json'), JSON.stringify(before, null, 2));
  const tasks = aws('ecs', 'list-tasks', '--cluster', 'capstan', '--service-name', 'capstan-server', '--desired-status', 'RUNNING').taskArns;
  assert.equal(tasks.length, 1); const taskBefore = tasks[0];
  const deployed = aws('ecs', 'describe-tasks', '--cluster', 'capstan', '--tasks', taskBefore).tasks[0];
  assert(deployed.containers.every(c => c.image.endsWith(`:${session.deployedSha}`)));
  const killedAt = new Date();
  note(`$ AWS_PROFILE=agentops aws --profile agentops --region us-east-1 ecs stop-task --cluster capstan --task ${taskBefore} --reason workload-proof`);
  const stopped = aws('ecs', 'stop-task', '--cluster', 'capstan', '--task', taskBefore, '--reason', 'workload-proof');
  assert.equal(stopped.task.taskArn, taskBefore);
  assert.equal(first.exitCode, null); process.kill(first.pid, 0);
  let taskAfter;
  const replacement = await until('ECS replacement task running', async () => {
    const arns = aws('ecs', 'list-tasks', '--cluster', 'capstan', '--service-name', 'capstan-server', '--desired-status', 'RUNNING').taskArns;
    taskAfter = arns.find(arn => arn !== taskBefore);
    if (taskAfter) {
      const task = aws('ecs', 'describe-tasks', '--cluster', 'capstan', '--tasks', taskAfter).tasks[0];
      if (task.lastStatus === 'RUNNING') return task;
    }
    await delay(5000); return false;
  }, 10 * 60_000);
  const replacementAt = new Date();
  assert(replacement.containers.every(c => c.image.endsWith(`:${session.deployedSha}`)));
  await until('execute-api ready after replacement', async () => {
    try { if ((await fetch(`${address}/readyz`, { signal: AbortSignal.timeout(5000) })).ok) return true; } catch {}
    await delay(2000); return false;
  }, 5 * 60_000);
  const reachableAt = new Date();
  note(`Replacement ${taskAfter} is RUNNING; execute-api ready after ${((reachableAt - killedAt) / 1000).toFixed(3)} s; worker PID ${first.pid} is unchanged.`);
  assert.equal(first.exitCode, null); process.kill(first.pid, 0);
  let lastProgress = 0;
  const approvalHistory = await until('both reports and real human approval', async () => {
    const { run } = await rpc('DescribeRun', { runId });
    if (run.status !== 'RUN_STATUS_RUNNING') throw new Error(`Run stopped before approval: ${run.status} ${run.failure?.message ?? ''}`);
    const h = await history();
    if (h.some(e => e.approvalRequested)) return h;
    if (Date.now() - lastProgress > 15_000) {
      for (const s of running) note(laneStatus(s.name).summary.split('\n')[0]);
      lastProgress = Date.now();
    }
    await delay(1000); return false;
  }, 25 * 60_000);
  const approval = approvalHistory.find(e => e.approvalRequested).approvalRequested;
  assert.deepEqual(approval.options, ['greet', 'count', 'none']);
  note('Both reports collected; real human approval is waiting. Selecting greet under the authorized AWS proof brief.');
  cli('approve', runId, approval.approvalId, '--choice', 'greet', '--resolver', 'workload-demo', '--note', 'Authorized AWS proof: merge greeting lane only');
  const completed = await until('completed run', async () => {
    const { run } = await rpc('DescribeRun', { runId });
    if (run.status === 'RUN_STATUS_COMPLETED') return run;
    if (run.status !== 'RUN_STATUS_RUNNING') throw new Error(`Run ${run.status}: ${run.failure?.message ?? ''}`);
    return false;
  });
  const result = decode(completed.result); assert.equal(result.merged.name, 'greet');
  assert(existsSync(join(repo, 'greet.sh'))); assert(!existsSync(join(repo, 'count.sh')));
  const greeting = execFileSync(join(repo, 'greet.sh'), ['Ada'], { cwd: repo, encoding: 'utf8' }); assert.equal(greeting, 'Hello, Ada!\n');
  execFileSync(join(repo, 'test-greet.sh'), [], { cwd: repo, stdio: 'inherit' });
  const reports = result.lanes.map(l => ({ name: l.name, laneName: l.laneName, reportPath: l.reportPath, report: readFileSync(l.reportPath, 'utf8'), head: l.head }));
  for (const l of result.lanes) {
    assert.equal(laneStatus(l.laneName).finished, true);
    const runlog = readFileSync(join(homedir(), '.codex/lanes', l.laneName, 'run.log'), 'utf8');
    assert.equal((runlog.match(/attempt \d+ fresh/g) ?? []).length, 1);
    note(`${l.name}: commit ${l.head}, report ${l.reportPath}, one fresh lane launch.`);
    execFileSync(join(l.worktree, l.name === 'greet' ? 'test-greet.sh' : 'test-count.sh'), [], { cwd: l.worktree, stdio: 'inherit' });
  }
  const after = await history();
  assert.deepEqual(after.slice(0, before.length), before);
  const activityResults = after.filter(e => e.activityCompleted).map(e => e.activityCompleted);
  assert.equal(activityResults.length, 5);
  const retried = activityResults.filter(e => [3, 4].includes(Number(e.seq)));
  assert.equal(retried.length, 2);
  cli('describe', runId); cli('history', runId); cli('replay', runId, '--workflows', 'examples/codex-lanes/workflow.ts');
  const reconnectLog = readFileSync(join(logdir, 'worker-1.log'), 'utf8').split('\n').filter(l => /poll failed|heartbeat failed|codex-lane-attached|report failed/.test(l));
  for (const line of reconnectLog) console.log(line);
  note(`PASS: ${before.length} history events preserved byte-for-byte as a prefix of ${after.length}; server task replaced; worker survived and resumed; only greet merged; both lane tests passed.`);
  proof = { runId, deployedSha: session.deployedSha, address, source: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim(), startedAt: startedAt.toISOString(), killedAt: killedAt.toISOString(), replacementAt: replacementAt.toISOString(), reachableAt: reachableAt.toISOString(), completedAt: new Date().toISOString(), workerPid: first.pid, taskBefore, taskAfter, running, before, after, result, reports, reconnectLog };
  writeFileSync(join(root, 'docs/evidence/aws-2026-09-28-history.json'), JSON.stringify(proof, null, 2) + '\n');
} finally {
  await cleanup(); process.removeListener('SIGINT', interrupt); process.removeListener('SIGTERM', interrupt);
  if (proof) {
    proof.cleanedAt = new Date().toISOString(); proof.cleanup = events.at(-1);
    writeFileSync(join(root, 'docs/evidence/aws-2026-09-28-history.json'), JSON.stringify(proof, null, 2) + '\n');
  }
}
