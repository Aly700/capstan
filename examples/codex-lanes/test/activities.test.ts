import assert from 'node:assert/strict';
import { spawn, execFileSync, type ChildProcess } from 'node:child_process';
import { AsyncLocalStorage } from 'node:async_hooks';
import { createHash } from 'node:crypto';
import { chmodSync, copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { test, type TestContext } from 'node:test';
import { createLaneActivities } from '../activities.ts';
import { codexLanes } from '../workflow.ts';
import { __installRuntime, type WorkflowRuntime } from '../../../sdk/src/workflow/index.ts';
import type { ActivityContext } from '../../../sdk/src/worker/index.ts';

const project = resolve(fileURLToPath(new URL('../../../', import.meta.url)));
const fixture = fileURLToPath(new URL('./fixtures/codex-lane.mjs', import.meta.url));
async function until(probe: () => unknown, timeout = 10_000) {
  const deadline = Date.now() + timeout;
  while (!probe()) { assert(Date.now() < deadline, 'timed out waiting for process evidence'); await delay(25); }
}
function setup(t: TestContext, extraEnv: NodeJS.ProcessEnv = {}) {
  const root = mkdtempSync(join(project, '.lane/workload-test-'));
  const repo = join(root, 'repo'); const lanesDirectory = join(root, 'lanes'); const bin = join(root, 'bin');
  for (const dir of [repo, lanesDirectory, bin]) mkdirSync(dir);
  copyFileSync(fixture, join(bin, 'codex-lane')); chmodSync(join(bin, 'codex-lane'), 0o755);
  const env = { ...process.env, ...extraEnv, PATH: `${bin}:${process.env.PATH}`, FAKE_LANES_DIR: lanesDirectory };
  const git = (...args: string[]) => execFileSync('git', ['-C', repo, ...args], { encoding: 'utf8' }).trim();
  git('init', '-q', '-b', 'main'); git('config', 'user.name', 'Workload test'); git('config', 'user.email', 'test@example.invalid');
  git('commit', '-q', '--allow-empty', '-m', 'base');
  const base = git('rev-parse', 'HEAD');
  const storage = new AsyncLocalStorage<ActivityContext>();
  const controller = new AbortController();
  const heartbeats: unknown[] = [];
  const context = (key = 'test-run/3', attempt = 1): ActivityContext => ({
    runId: 'test-run', workflowType: 'codexLanes', activityType: 'test', seq: Number(key.split('/').at(-1)), attempt,
    idempotencyKey: key, heartbeatDetails: undefined, signal: controller.signal, heartbeat: (d) => { heartbeats.push(d); },
  });
  const acts = createLaneActivities({ env, lanesDirectory, pollIntervalMs: 30, launchTimeoutMs: 1000, context: () => storage.getStore() ?? context() });
  const input = { brief: 'Two independent tasks. Commit each task.', repo, lanes: [
    { name: 'alpha', branch: 'lane/alpha', brief: 'Add alpha.txt', effort: 'xhigh' as const },
    { name: 'beta', branch: 'lane/beta', brief: 'Add beta.txt', effort: 'xhigh' as const },
  ] };
  const children: ChildProcess[] = [];
  t.after(async () => {
    // Release every detached fake runner before deleting its files.
    writeFileSync(join(lanesDirectory, 'launch-release'), ''); writeFileSync(join(lanesDirectory, 'release'), '');
    for (const child of children) if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    controller.abort();
    for (let i = 0; i < 100; i++) {
      if (readdirSync(lanesDirectory).filter(n => n.startsWith('cw-')).every(n => !existsSync(join(lanesDirectory, n, 'run.pid')))) break;
      await delay(30);
    }
    for (const n of readdirSync(lanesDirectory).filter(n => n.startsWith('cw-'))) {
      const pid = join(lanesDirectory, n, 'run.pid');
      if (existsSync(pid)) { try { process.kill(Number(readFileSync(pid, 'utf8')), 'SIGKILL'); } catch {} }
    }
    rmSync(root, { recursive: true, force: true });
  });
  const prepare = (i = 0) => acts.createLaneWorktree({ brief: input.brief, repo, lane: input.lanes[i]!, runId: 'test-run' });
  const names = () => readdirSync(lanesDirectory).filter(n => n.startsWith('cw-'));
  const invocations = () => existsSync(join(lanesDirectory, 'invocations')) ? readFileSync(join(lanesDirectory, 'invocations'), 'utf8').trim().split('\n') : [];
  const release = () => writeFileSync(join(lanesDirectory, 'release'), '');
  return { root, repo, bin, env, git, base, storage, context, heartbeats, acts, input, children, prepare, names, invocations, release, lanesDirectory };
}

test('happy path: worktree retry, real shell lane, report, heartbeat, and merge retry', async t => {
  const h = setup(t); const prepared = await h.prepare();
  assert.deepEqual(await h.prepare(), prepared);
  const running = h.acts.runCodexLane(prepared);
  await until(() => h.names().some(n => existsSync(join(h.lanesDirectory, n, 'run.pid'))));
  assert(h.heartbeats.length > 0);
  h.release(); const report = await running;
  assert.equal(h.invocations().length, 1);
  assert.match(readFileSync(report.reportPath, 'utf8'), /Committed/);
  assert.notEqual(report.head, h.base);
  assert.equal(h.git('rev-parse', 'HEAD'), h.base);
  const merged = await h.acts.mergeLane(report);
  assert.deepEqual(await h.acts.mergeLane(report), merged);
  assert.equal(h.git('rev-parse', 'HEAD'), report.head);
  assert(existsSync(join(h.repo, 'alpha.txt')));
});

for (const early of [false, true]) test(`SIGKILL activity process and reattach${early ? ' during the unacknowledged launch' : ' mid-lane'}`, async t => {
  const h = setup(t); const prepared = await h.prepare();
  const start = (attempt: number) => {
    const config = { prepared, attempt, lanesDirectory: h.lanesDirectory, heartbeats: join(h.root, `hb-${attempt}`), result: join(h.root, `result-${attempt}`) };
    const path = join(h.root, `config-${attempt}.json`); writeFileSync(path, JSON.stringify(config));
    const worker = spawn(process.execPath, ['--import', join(project, 'sdk/node_modules/tsx/dist/loader.mjs'), fileURLToPath(new URL('./fixtures/activity-process.ts', import.meta.url)), path], {
      env: { ...h.env, FAKE_LAUNCH_WAIT: early ? '1' : '0' }, stdio: ['ignore', 'pipe', 'pipe'],
    });
    let output = ''; worker.stderr!.on('data', b => { output += b; });
    h.children.push(worker);
    return { worker, config, output: () => output };
  };
  const first = start(1);
  await until(() => early ? h.invocations().length === 1 : h.names().some(n => existsSync(join(h.lanesDirectory, n, 'run.pid'))));
  const pid = early ? undefined : readFileSync(join(h.lanesDirectory, h.names()[0]!, 'run.pid'), 'utf8');
  first.worker.kill('SIGKILL'); await until(() => first.worker.signalCode === 'SIGKILL');
  const second = start(2);
  await until(() => existsSync(second.config.heartbeats));
  writeFileSync(join(h.lanesDirectory, 'launch-release'), '');
  await until(() => h.names().some(n => existsSync(join(h.lanesDirectory, n, 'run.pid'))));
  if (pid) assert.equal(readFileSync(join(h.lanesDirectory, h.names()[0]!, 'run.pid'), 'utf8'), pid);
  h.release();
  await until(() => second.worker.exitCode !== null, 15_000);
  assert.equal(second.worker.exitCode, 0, second.output());
  assert(existsSync(JSON.parse(readFileSync(second.config.result, 'utf8')).reportPath));
  assert.equal(h.invocations().length, 1, 'retry never invokes codex-lane run twice');
});

test('overlapping deliveries share one exclusive launch claim', async t => {
  const h = setup(t); const prepared = await h.prepare();
  const results = Promise.all([h.acts.runCodexLane(prepared), h.acts.runCodexLane(prepared)]);
  await until(() => h.names().some(n => existsSync(join(h.lanesDirectory, n, 'run.pid'))));
  h.release();
  const [a, b] = await results; assert.deepEqual(a, b); assert.equal(h.invocations().length, 1);
});

test('retry never rewrites a brief that a launcher may be reading', async t => {
  const h = setup(t); const prepared = await h.prepare();
  const before = statSync(prepared.briefPath); await delay(30);
  await h.prepare();
  assert.equal(statSync(prepared.briefPath).mtimeMs, before.mtimeMs);
});

test('an incomplete checkout is never returned as a prepared worktree', async t => {
  const h = setup(t); writeFileSync(join(h.repo, 'seed.txt'), 'baseline\n');
  h.git('add', 'seed.txt'); h.git('commit', '-qm', 'seed');
  const prepared = await h.prepare(); rmSync(join(prepared.worktree, 'seed.txt'));
  await assert.rejects(h.prepare(), /uncommitted changes/);
  assert.deepEqual(h.invocations(), []);
});

test('overlapping preparation recognizes the manifest published during its branch probe', async t => {
  const h = setup(t);
  const realGit = execFileSync('which', ['git'], { encoding: 'utf8' }).trim();
  const wrapper = `#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import { existsSync, openSync, closeSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
const base = ${JSON.stringify(h.root)};
if (process.argv.includes('show-ref')) {
  let first = false;
  try { closeSync(openSync(base + '/branch-probe', 'wx')); first = true; } catch {}
  if (first) while (!existsSync(base + '/branch-release')) await delay(10);
}
const result = spawnSync(${JSON.stringify(realGit)}, process.argv.slice(2), { stdio: 'inherit' });
process.exit(result.status ?? 1);
`;
  writeFileSync(join(h.bin, 'git'), wrapper, { mode: 0o755 });
  const previousPath = process.env.PATH; process.env.PATH = h.env.PATH;
  t.after(() => { process.env.PATH = previousPath; });
  const first = h.prepare().catch(error => error);
  await until(() => existsSync(join(h.root, 'branch-probe')));
  const second = await h.prepare();
  writeFileSync(join(h.root, 'branch-release'), '');
  const result = await first;
  assert(!(result instanceof Error), String(result));
  assert.deepEqual(result, second);
});

test('lane directory can appear before the CLI finishes writing cwd', async t => {
  const h = setup(t, { FAKE_CWD_WAIT: '1' }); const prepared = await h.prepare(); h.release();
  const report = await h.acts.runCodexLane(prepared);
  assert.equal(report.name, 'alpha'); assert.equal(h.invocations().length, 1);
});

test('a lost launcher with a permanent claim fails closed instead of starting again', async t => {
  const h = setup(t); const prepared = await h.prepare();
  const hash = createHash('sha256').update('test-run/3').digest('hex');
  mkdirSync(join(prepared.stateDir, `launch-${hash}`, 'claimed'), { recursive: true });
  await assert.rejects(h.acts.runCodexLane(prepared), { type: 'LaneLaunchUncertain', nonRetryable: true });
  assert.deepEqual(h.invocations(), []);
});

test('an existing lane with another cwd is never attached or replaced', async t => {
  const h = setup(t); const prepared = await h.prepare();
  const hash = createHash('sha256').update('test-run/3').digest('hex').slice(0, 32);
  const lane = join(h.lanesDirectory, `cw-alpha-${hash}`); mkdirSync(lane);
  writeFileSync(join(lane, 'cwd'), h.repo);
  await assert.rejects(h.acts.runCodexLane(prepared), /another worktree/);
  assert.deepEqual(h.invocations(), []);
});

test('approval pins the reported commit even if the lane branch later moves', async t => {
  const h = setup(t); const prepared = await h.prepare(); h.release();
  const report = await h.acts.runCodexLane(prepared);
  execFileSync('git', ['-C', prepared.worktree, 'commit', '-qm', 'later change', '--allow-empty']);
  await h.acts.mergeLane(report);
  assert.equal(h.git('rev-parse', 'HEAD'), report.head);
});

for (const choice of ['beta', 'none']) test(`human choosing ${choice} controls the only merge`, async t => {
  const h = setup(t); h.release(); let seq = 0; const merged: string[] = [];
  __installRuntime({
    workflowInfo: () => ({ runId: 'test-run' }), uuid: () => 'human-choice',
    activity: async (name: keyof typeof h.acts, input: any) => {
      if (name === 'mergeLane') merged.push(input.name);
      return h.storage.run(h.context(`test-run/${++seq}`), () => h.acts[name](input));
    },
    requestApproval: async (request: any) => {
      assert.deepEqual(request.options, ['alpha', 'beta', 'none']);
      assert.match(request.prompt, /merge lanes alpha, beta/);
      assert.equal(h.invocations().length, 2);
      return { outcome: 'approved', choice, resolver: 'test-owner', note: '' };
    },
  } as unknown as WorkflowRuntime);
  const result = await codexLanes(h.input);
  assert.equal(result.lanes.length, 2); assert.equal(h.invocations().length, 2);
  assert.deepEqual(merged, choice === 'none' ? [] : ['beta']);
  assert.equal(existsSync(join(h.repo, 'alpha.txt')), false);
  assert.equal(existsSync(join(h.repo, 'beta.txt')), choice === 'beta');
  if (choice === 'none') assert.equal(h.git('rev-parse', 'HEAD'), h.base);
});
