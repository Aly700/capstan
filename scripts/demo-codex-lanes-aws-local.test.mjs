import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { root } from './evidence-lib.mjs';
import { claimLocalProof, recordLocalProof, requestLocalShutdown } from './demo-codex-lanes-aws-local.mjs';

function directory() {
  mkdirSync(join(root, '.lane'), { recursive: true });
  return mkdtempSync(join(root, '.lane/aws-shutdown-test-'));
}
const startedAt = '2026-09-28T17:38:59.491635+00:00';

test('teardown blocks a proof from starting after local shutdown was requested', async () => {
  const dir = directory();
  try {
    await requestLocalShutdown(dir, startedAt);
    assert.throws(() => claimLocalProof(dir, { startedAt, runId: 'late' }), /teardown/);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('teardown waits for the proof process to verify local cleanup', async () => {
  const dir = directory();
  const module = new URL('./demo-codex-lanes-aws-local.mjs', import.meta.url).href;
  const source = `
    import { existsSync, writeFileSync } from 'node:fs';
    import { join } from 'node:path';
    import { claimLocalProof, recordLocalProof } from ${JSON.stringify(module)};
    const dir = ${JSON.stringify(dir)}, state = { startedAt: ${JSON.stringify(startedAt)}, runId: 'active', pid: process.pid };
    claimLocalProof(dir, state);
    const timer = setInterval(() => {
      if (!existsSync(join(dir, 'stop-requested'))) return;
      clearInterval(timer);
      setTimeout(() => {
        writeFileSync(join(dir, 'worker-stopped'), 'yes');
        recordLocalProof(dir, { ...state, cleanedAt: new Date().toISOString() });
        process.exit(0);
      }, 100);
    }, 10);
  `;
  const child = spawn(process.execPath, ['--input-type=module', '-e', source], { stdio: 'ignore' });
  const exit = new Promise(resolve => child.once('exit', resolve));
  try {
    const deadline = Date.now() + 5000;
    while (!existsSync(join(dir, 'state.json'))) {
      assert(Date.now() < deadline && child.exitCode === null, 'proof child must start');
      await delay(10);
    }
    await requestLocalShutdown(dir, startedAt, 5000);
    assert.equal(readFileSync(join(dir, 'worker-stopped'), 'utf8'), 'yes');
    assert.equal(await exit, 0);
  } finally {
    if (child.exitCode === null) { child.kill('SIGKILL'); await exit; }
    rmSync(dir, { recursive: true, force: true });
  }
});

test('an unresponsive or foreign proof prevents AWS teardown', async () => {
  const dir = directory();
  try {
    claimLocalProof(dir, { startedAt, runId: 'unresponsive' });
    await assert.rejects(requestLocalShutdown(dir, startedAt, 20), /not verified/);
    writeFileSync(join(dir, 'state.json'), JSON.stringify({ startedAt: 'foreign' }));
    await assert.rejects(requestLocalShutdown(dir, startedAt, 20), /different AWS session/);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

function orphanFixture() {
  const projectRoot = mkdtempSync(join(tmpdir(), 'capstan-orphan-test-'));
  const dir = join(projectRoot, '.lane/proof');
  const repo = join(projectRoot, '.lane/demo-repo-aws');
  const lanesDirectory = join(projectRoot, 'lanes');
  const runId = 'workload-aws-123';
  const marker = `--capstan-proof-worker=${runId}-worker-1`;
  const state = { ownershipVersion: 1, startedAt, runId, pid: 100, repo, workers: [{ pid: 200, marker }] };
  mkdirSync(join(repo, '.git'), { recursive: true });
  writeFileSync(join(repo, '.git/workload-demo-owner'), runId);
  claimLocalProof(dir, state);
  let processes = [
    { pid: 200, pgid: 200, command: `node ${join(projectRoot, 'examples/codex-lanes/worker.ts')} ${marker}` },
    { pid: 999, pgid: 999, command: 'unrelated-worker' },
  ];
  const signals = [];
  const runtime = {
    projectRoot, lanesDirectory,
    processes: () => processes,
    signal: (pid, signal) => { signals.push([pid, signal]); processes = processes.filter(p => p.pgid !== -pid); },
    exec: (binary, args) => {
      assert.equal(binary, 'git');
      assert.deepEqual(args, ['-C', repo, 'worktree', 'list', '--porcelain']);
      return `worktree ${repo}\n`;
    },
  };
  return { projectRoot, dir, repo, state, runtime, signals, setProcesses: value => { processes = value; } };
}

test('an orphaned proof stops its recorded worker and verifies cleanup before teardown', async () => {
  const fixture = orphanFixture();
  try {
    await requestLocalShutdown(fixture.dir, startedAt, 20, fixture.runtime);
    assert.deepEqual(fixture.signals, [[-200, 'SIGTERM']]);
    assert.deepEqual(fixture.runtime.processes().map(p => p.pid), [999]);
    assert.equal(existsSync(fixture.repo), false);
    assert.ok(JSON.parse(readFileSync(join(fixture.dir, 'state.json'), 'utf8')).cleanedAt);
  } finally { rmSync(fixture.projectRoot, { recursive: true, force: true }); }
});

test('orphan recovery refuses a reused worker PID without touching it', async () => {
  const fixture = orphanFixture();
  try {
    fixture.setProcesses([{ pid: 200, pgid: 200, command: 'foreign-process' }]);
    await assert.rejects(requestLocalShutdown(fixture.dir, startedAt, 20, fixture.runtime), /worker ownership/);
    assert.deepEqual(fixture.signals, []);
    assert.equal(existsSync(fixture.repo), true);
    assert.equal(JSON.parse(readFileSync(join(fixture.dir, 'state.json'), 'utf8')).cleanedAt, undefined);
  } finally { rmSync(fixture.projectRoot, { recursive: true, force: true }); }
});

test('orphan recovery refuses a foreign repository ownership marker', async () => {
  const fixture = orphanFixture();
  try {
    writeFileSync(join(fixture.repo, '.git/workload-demo-owner'), 'another-run');
    await assert.rejects(requestLocalShutdown(fixture.dir, startedAt, 20, fixture.runtime), /repository ownership/);
    assert.deepEqual(fixture.signals, []);
    assert.equal(existsSync(fixture.repo), true);
  } finally { rmSync(fixture.projectRoot, { recursive: true, force: true }); }
});

test('orphan recovery finds a worker whose marker was recorded before its PID', async () => {
  const fixture = orphanFixture();
  try {
    delete fixture.state.workers[0].pid;
    recordLocalProof(fixture.dir, fixture.state);
    await requestLocalShutdown(fixture.dir, startedAt, 20, fixture.runtime);
    assert.deepEqual(fixture.signals, [[-200, 'SIGTERM']]);
    assert.deepEqual(fixture.runtime.processes().map(p => p.pid), [999]);
  } finally { rmSync(fixture.projectRoot, { recursive: true, force: true }); }
});

function addLane(fixture) {
  const laneName = `cw-greet-${'a'.repeat(32)}`;
  const worktree = join(fixture.repo, '.git/capstan-codex-lanes/lane/worktree');
  const launchDir = join(fixture.repo, '.git/capstan-codex-lanes/lane/launch-123');
  const laneDir = join(fixture.runtime.lanesDirectory, laneName);
  const key = `${fixture.state.runId}/3`;
  mkdirSync(join(launchDir, 'claimed'), { recursive: true });
  mkdirSync(laneDir, { recursive: true });
  writeFileSync(join(laneDir, 'cwd'), worktree);
  writeFileSync(join(launchDir, 'request.json'), JSON.stringify({ key, laneName, worktree }));
  writeFileSync(join(launchDir, 'claimed/owner.json'), JSON.stringify({ key, pid: 300 }));
  fixture.setProcesses([...fixture.runtime.processes(),
    { pid: 300, pgid: 300, command: `node ${join(fixture.projectRoot, 'examples/codex-lanes/launch.mjs')} ${join(launchDir, 'request.json')}` },
    { pid: 301, pgid: 300, command: `codex-lane run ${laneName} --cwd ${worktree}` },
  ]);
  const git = fixture.runtime.exec;
  fixture.runtime.exec = (binary, args) => {
    if (binary === 'git') return git(binary, args);
    assert.equal(binary, 'codex-lane'); assert.equal(args[1], laneName);
    if (args[0] === 'status') return `lane ${laneName}: running (pid 301)\n`;
    assert.equal(args[0], 'stop');
    fixture.setProcesses(fixture.runtime.processes().filter(p => p.pid !== 301));
    return '';
  };
  return { laneDir, launchDir };
}

test('orphan recovery uses the existing lane cleanup with exact receipts and leaves foreign processes alone', async () => {
  const fixture = orphanFixture();
  try {
    const lane = addLane(fixture);
    await requestLocalShutdown(fixture.dir, startedAt, 20, fixture.runtime);
    assert.deepEqual(fixture.signals, [[-200, 'SIGTERM'], [-300, 'SIGTERM']]);
    assert.deepEqual(fixture.runtime.processes().map(p => p.pid), [999]);
    assert.equal(existsSync(lane.laneDir), false);
    assert.equal(existsSync(fixture.repo), false);
  } finally { rmSync(fixture.projectRoot, { recursive: true, force: true }); }
});

test('orphan recovery refuses a foreign lane cwd before stopping the lane', async () => {
  const fixture = orphanFixture();
  try {
    const lane = addLane(fixture);
    writeFileSync(join(lane.laneDir, 'cwd'), '/foreign/worktree');
    await assert.rejects(requestLocalShutdown(fixture.dir, startedAt, 20, fixture.runtime), /foreign lane cwd/);
    assert.deepEqual(fixture.signals, [[-200, 'SIGTERM']]);
    assert(fixture.runtime.processes().some(p => p.pid === 301));
    assert.equal(existsSync(lane.laneDir), true);
    assert.equal(JSON.parse(readFileSync(join(fixture.dir, 'state.json'), 'utf8')).cleanedAt, undefined);
  } finally { rmSync(fixture.projectRoot, { recursive: true, force: true }); }
});
