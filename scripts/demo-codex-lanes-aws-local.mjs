// Coordinate the proof's own cleanup before a separate process destroys AWS.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, readdirSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { root } from './evidence-lib.mjs';

const exec = (binary, args) => execFileSync(binary, args, { encoding: 'utf8', timeout: 30_000 });
const runtimeDefaults = {
  projectRoot: root, lanesDirectory: join(homedir(), '.codex/lanes'), exec,
  processes: () => exec('ps', ['-axo', 'pid=,pgid=,command=']).split('\n')
    .map(line => line.trim().match(/^(\d+)\s+(\d+)\s+(.+)$/)).filter(Boolean)
    .map(match => ({ pid: Number(match[1]), pgid: Number(match[2]), command: match[3] })),
  signal: (pid, signal) => process.kill(pid, signal),
};
const hasArgument = (command, argument) => command.split(/\s+/).includes(argument);

// Shared by normal proof exit and orphan recovery. Every signal is fenced by a
// receipt plus a matching live command; a reused PID or foreign cwd fails closed.
export async function cleanupOwnedProof(state, options = {}) {
  const runtime = { ...runtimeDefaults, ...options };
  assert.equal(state.ownershipVersion, 1, 'local proof lacks recoverable ownership');
  assert.match(state.runId, /^workload-aws-\d+$/);
  const repo = join(runtime.projectRoot, '.lane/demo-repo-aws');
  assert.equal(state.repo, repo, 'foreign repository path');
  if (existsSync(repo)) assert.equal(readFileSync(join(repo, '.git/workload-demo-owner'), 'utf8'), state.runId, 'repository ownership mismatch');
  assert(Array.isArray(state.workers), 'local proof lacks worker ownership');

  const stopGroup = async (pid, owns) => {
    assert(Number.isSafeInteger(pid) && pid > 1, 'invalid owned process group');
    const members = () => runtime.processes().filter(p => p.pgid === pid);
    const send = signal => {
      const live = members();
      if (!live.length) return;
      assert(live.some(owns), `process group ownership is not verified: ${pid}`);
      try { runtime.signal(-pid, signal); } catch (error) { if (error.code !== 'ESRCH') throw error; }
    };
    const wait = async () => {
      const deadline = Date.now() + 5000;
      while (members().length && Date.now() < deadline) await delay(20);
      return members().length === 0;
    };
    send('SIGTERM');
    if (!await wait()) { send('SIGKILL'); assert(await wait(), 'owned process group did not stop'); }
  };

  for (const worker of state.workers) {
    assert.match(worker.marker, new RegExp(`^--capstan-proof-worker=${state.runId}-worker-[0-9]+$`));
    const owns = p => hasArgument(p.command, worker.marker)
      && hasArgument(p.command, join(runtime.projectRoot, 'examples/codex-lanes/worker.ts'));
    const processes = runtime.processes();
    const recorded = processes.find(p => p.pid === worker.pid);
    assert(!recorded || owns(recorded), 'worker ownership mismatch; PID may have been reused');
    // The marker is published before spawn, covering death before PID publication.
    const groups = new Set(processes.filter(owns).map(p => {
      assert.equal(p.pid, p.pgid, 'worker must own its detached process group'); return p.pgid;
    }));
    if (worker.pid && processes.some(p => p.pgid === worker.pid)) groups.add(worker.pid);
    for (const pid of groups) await stopGroup(pid, owns);
  }

  const base = join(repo, '.git/capstan-codex-lanes');
  const lanes = [];
  if (existsSync(base)) for (const directory of readdirSync(base)) {
    for (const launch of readdirSync(join(base, directory)).filter(name => name.startsWith('launch-'))) {
      const launchDir = join(base, directory, launch), path = join(launchDir, 'request.json');
      if (!existsSync(path)) continue;
      const request = JSON.parse(readFileSync(path, 'utf8'));
      assert(request.key.startsWith(`${state.runId}/`), 'foreign lane activity key');
      assert.match(request.laneName, /^cw-[a-z0-9-]+-[a-f0-9]{32}$/);
      assert(resolve(request.worktree).startsWith(`${base}/`), 'foreign lane worktree');
      const laneDir = join(runtime.lanesDirectory, request.laneName);
      if (existsSync(laneDir)) assert.equal(readFileSync(join(laneDir, 'cwd'), 'utf8').trim(), request.worktree, 'foreign lane cwd');
      const owns = p => hasArgument(p.command, request.laneName) || hasArgument(p.command, request.worktree)
        || hasArgument(p.command, path) || p.command.includes(`${laneDir}/`);
      if (existsSync(laneDir)) {
        const status = runtime.exec('codex-lane', ['status', request.laneName]);
        const match = status.split('\n')[0].match(/^lane [a-z0-9-]+: running \(pid (\d+)\)$/);
        if (match) {
          const active = runtime.processes().find(p => p.pid === Number(match[1]));
          assert(!active || owns(active), 'lane process ownership mismatch');
          if (active) runtime.exec('codex-lane', ['stop', request.laneName]);
        }
      }
      const ownerPath = join(launchDir, 'claimed/owner.json');
      if (existsSync(ownerPath)) {
        const owner = JSON.parse(readFileSync(ownerPath, 'utf8'));
        assert.equal(owner.key, request.key, 'foreign lane launcher ownership');
        await stopGroup(owner.pid, owns);
      }
      assert(!runtime.processes().some(owns), 'owned lane processes remain');
      rmSync(laneDir, { recursive: true, force: true }); lanes.push(request.laneName);
    }
  }
  if (existsSync(repo)) {
    const git = (...args) => runtime.exec('git', ['-C', repo, ...args]);
    const worktrees = () => git('worktree', 'list', '--porcelain').split('\n').filter(line => line.startsWith('worktree ')).map(line => line.slice(9));
    for (const worktree of worktrees().filter(path => path !== repo)) {
      assert(resolve(worktree).startsWith(`${base}/`), 'foreign proof worktree');
      git('worktree', 'remove', '--force', worktree);
    }
    assert.deepEqual(worktrees(), [repo]);
    rmSync(repo, { recursive: true });
  }
  return lanes.length;
}

export function localProofDirectory(root, startedAt) {
  const timestamp = Date.parse(startedAt);
  assert(Number.isFinite(timestamp), 'AWS session needs a valid start time');
  return join(root, '.lane', `aws-local-proof-${timestamp}`);
}

export function recordLocalProof(directory, state) {
  const temporary = join(directory, `state-${process.pid}.tmp`);
  writeFileSync(temporary, JSON.stringify(state) + '\n', { mode: 0o600 });
  renameSync(temporary, join(directory, 'state.json'));
}

export function claimLocalProof(directory, state) {
  mkdirSync(directory, { recursive: true });
  assert(!existsSync(join(directory, 'stop-requested')), 'AWS teardown has already started');
  mkdirSync(join(directory, 'claimed')); // One proof process per AWS session.
  recordLocalProof(directory, state); // Publish ownership before starting any worker.
  if (existsSync(join(directory, 'stop-requested'))) {
    recordLocalProof(directory, { ...state, cleanedAt: new Date().toISOString() });
    throw new Error('AWS teardown has already started');
  }
}

export async function requestLocalShutdown(directory, startedAt, timeoutMs = 60_000, options = {}) {
  const runtime = { ...runtimeDefaults, ...options };
  mkdirSync(directory, { recursive: true });
  const readState = () => {
    const path = join(directory, 'state.json');
    if (!existsSync(path)) return;
    const state = JSON.parse(readFileSync(path, 'utf8'));
    assert.equal(state.startedAt, startedAt, 'local proof belongs to a different AWS session');
    return state;
  };
  readState(); // Never send a shutdown request to a foreign receipt.
  writeFileSync(join(directory, 'stop-requested'), new Date().toISOString() + '\n', { mode: 0o600 });
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const state = readState();
    if (!state || state.cleanedAt) return;
    if (state.ownershipVersion === 1 && Number.isSafeInteger(state.pid) && state.pid > 1
      && !runtime.processes().some(p => p.pid === state.pid)) {
      await cleanupOwnedProof(state, runtime);
      recordLocalProof(directory, { ...state, cleanedAt: new Date().toISOString(), recoveredOrphan: true });
      return;
    }
    if (Date.now() >= deadline) throw new Error('Local worker/lane cleanup is not verified; refusing AWS teardown. Inspect the owned proof before retrying.');
    await delay(20);
  }
}
