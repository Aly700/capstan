import assert from 'node:assert/strict';
import { spawn, execFileSync, spawnSync } from 'node:child_process';
import { createHash, randomBytes } from 'node:crypto';
import { existsSync, mkdirSync, openSync, closeSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:net';
import { root, until, delay, decode, payload } from './evidence-lib.mjs';

// Own port/database namespace: evidence-lib's Evidence class is reserved for 7300–7399.
const port = 7501;
const startedAt = new Date();
const runId = `workload-local-${Date.now()}`;
const queue = runId;
const repo = join(root, '.lane/demo-repo');
const logdir = join(root, '.lane', runId);
const database = `capstan_workload_${process.pid}_${Date.now()}`;
const admin = 'postgres://capstan:capstan@127.0.0.1:55432/postgres?sslmode=disable';
const url = new URL(admin); url.pathname = `/${database}`; const dsn = url.toString();
const address = `http://127.0.0.1:${port}`;
const key = `cap_workload_${randomBytes(24).toString('hex')}`;
const keyHash = createHash('sha256').update(key).digest('hex');
const children = []; let databaseCreated = false; let repoCreated = false; let closing = false;
const ownedLanes = new Map();
mkdirSync(logdir, { recursive: true });
const events = [];
function note(message) {
  const line = `${new Date().toISOString()} ${message}`; events.push(line); console.log(line);
  writeFileSync(join(logdir, 'events.json'), JSON.stringify(events, null, 2));
}
function git(...args) { return execFileSync('git', ['-C', repo, ...args], { encoding: 'utf8' }).trim(); }
function sql(query, target = dsn) { return execFileSync('psql', [target, '-X', '-Atq', '-v', 'ON_ERROR_STOP=1', '-c', query], { encoding: 'utf8' }).trim(); }
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
  if (closing) return;
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
  if (databaseCreated) { sql(`drop database ${database} with (force)`, admin); databaseCreated = false; }
  assert(children.every(c => c.exitCode !== null || c.signalCode !== null));
  assert.equal(sql(`select count(*) from pg_database where datname = '${database}'`, admin), '0');
  assert(!existsSync(repo));
  for (const name of ownedLanes.keys()) assert(!existsSync(join(homedir(), '.codex/lanes', name)));
  note(`Cleanup verified: ${children.length} child processes exited; ${ownedLanes.size} demo lane directories removed; demo worktrees/repo removed; database ${database} absent. Shared Postgres and other lanes untouched.`);
}
const interrupt = () => { void cleanup().finally(() => { process.exitCode = 130; }); };
process.once('SIGINT', interrupt); process.once('SIGTERM', interrupt);

let proof;
try {
  note('Real Capstan server + PostgreSQL + SDK worker + two real Codex lanes at xhigh. Local worker SIGKILL proof; no AWS.');
  note(`Source ${execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim()} plus working-tree workload files.`);
  assert(!existsSync(repo), `${repo} already exists; inspect it before rerunning`);
  const probe = createServer();
  await new Promise((resolve, reject) => { probe.once('error', reject); probe.listen(port, '127.0.0.1', () => probe.close(resolve)); });
  mkdirSync(join(root, '.lane/bin'), { recursive: true });
  execFileSync('go', ['build', '-o', '.lane/bin/workload-server', './cmd/capstan-server'], { cwd: root, env: { ...process.env, GOTOOLCHAIN: 'go1.26.4' }, stdio: 'inherit' });
  sql(`create database ${database}`, admin); databaseCreated = true;
  const server = child(join(root, '.lane/bin/workload-server'), ['serve'], 'server', {
    CAPSTAN_DATABASE_URL: dsn, CAPSTAN_API_KEY_HASHES: `workload:${keyHash}`, CAPSTAN_ADDR: `127.0.0.1:${port}`,
    CAPSTAN_POLL_TIMEOUT: '2s', CAPSTAN_MIGRATE: 'true', CAPSTAN_LOG_LEVEL: 'warn',
  });
  await until('server readiness', async () => {
    if (server.spawnError || server.exitCode !== null) throw new Error('server exited; see local logs');
    try { return (await fetch(`${address}/readyz`)).ok; } catch { return false; }
  });
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
  note(`Started ${runId} on port ${port}; server PID ${server.pid}, worker PID ${first.pid}; database ${database}.`);
  const running = await until('both real Codex lanes running', () => {
    discoverOwnedLanes();
    if (ownedLanes.size !== 2) return false;
    const statuses = [...ownedLanes.keys()].map(name => ({ name, ...laneStatus(name) }));
    return statuses.every(s => s.pid) ? statuses : false;
  }, 120_000);
  for (const s of running) { console.log(`$ codex-lane status ${s.name}`); console.log(s.summary); }
  const before = await history();
  writeFileSync(join(logdir, 'history-before.json'), JSON.stringify(before, null, 2));
  const killedAt = new Date();
  note(`$ kill -9 ${first.pid}  # the WORKER, while both independent lanes are running`);
  await stop(first, 'SIGKILL');
  await delay(1500);
  for (const s of running) { assert.equal(laneStatus(s.name).pid, s.pid); process.kill(s.pid, 0); }
  note(`Both lane runner PIDs survived: ${running.map(s => s.pid).join(', ')}. Server PID ${server.pid} is unchanged.`);
  const second = worker(2); note(`Replacement worker PID ${second.pid}; waiting for 30 s heartbeat leases to expire.`);
  await until('replacement worker attaches to both lanes', () => {
    const log = readFileSync(join(logdir, 'worker-2.log'), 'utf8');
    return running.every(s => log.includes(`"laneName":"${s.name}","attempt":2`));
  }, 120_000);
  const attachedAt = new Date();
  const attachments = readFileSync(join(logdir, 'worker-2.log'), 'utf8').split('\n').filter(l => l.includes('codex-lane-attached')).map(JSON.parse);
  for (const entry of attachments) console.log(JSON.stringify(entry));
  note(`Both activities reattached after ${((attachedAt - killedAt) / 1000).toFixed(3)} s. Launch claims remain one per lane.`);
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
  const tasksDuringApproval = sql('select count(*) from task'); assert.equal(tasksDuringApproval, '0');
  note('Both reports collected; human approval is waiting with zero tasks/leases. Selecting greet under the authorized demo brief.');
  cli('approve', runId, approval.approvalId, '--choice', 'greet', '--resolver', 'workload-demo', '--note', 'Authorized local proof: merge greeting lane only');
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
  assert.equal(retried.length, 2); assert(retried.every(e => e.attempt === 2));
  cli('describe', runId); cli('history', runId); cli('replay', runId, '--workflows', 'examples/codex-lanes/workflow.ts');
  note(`PASS: ${before.length} history events preserved byte-for-byte as a prefix of ${after.length}; both lane completions on attempt 2; only greet merged; greeting and both lane tests passed.`);
  proof = { runId, source: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim(), startedAt: startedAt.toISOString(), killedAt: killedAt.toISOString(), attachedAt: attachedAt.toISOString(), completedAt: new Date().toISOString(), serverPid: server.pid, workerPids: [first.pid, second.pid], running, database, before, after, result, reports, attachments };
  writeFileSync(join(root, 'docs/evidence/codex-lanes-local-history.json'), JSON.stringify(proof, null, 2) + '\n');
} finally {
  await cleanup(); process.removeListener('SIGINT', interrupt); process.removeListener('SIGTERM', interrupt);
  if (proof) {
    proof.cleanedAt = new Date().toISOString(); proof.cleanup = events.at(-1);
    writeFileSync(join(root, 'docs/evidence/codex-lanes-local-history.json'), JSON.stringify(proof, null, 2) + '\n');
  }
}
