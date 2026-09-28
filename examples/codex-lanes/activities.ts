import { spawn } from 'node:child_process';
import { mkdir, open, readFile, readdir, realpath, stat } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { activityContext, type ActivityContext } from '../../sdk/src/worker/index.ts';
import { command, digest, git, publish, publishText, readJson, stop } from './io.ts';
import type { LaneReport, MergeResult, PreparedLane, PrepareLaneInput } from './types.ts';

interface Options {
  context?: () => ActivityContext;
  lanesDirectory?: string;
  pollIntervalMs?: number;
  launchTimeoutMs?: number;
  env?: NodeJS.ProcessEnv;
}
interface Manifest { request: PrepareLaneInput; prepared: PreparedLane }

async function assertWorktree(lane: PreparedLane) {
  if (await git(lane.worktree, 'rev-parse', '--show-toplevel') !== lane.worktree
    || await git(lane.worktree, 'branch', '--show-current') !== lane.branch) stop('Worktree ownership changed');
}
async function assertClean(repo: string) {
  if (await git(repo, 'status', '--porcelain')) stop('Refusing to merge or run a lane with uncommitted changes');
}

export function createLaneActivities(options: Options = {}) {
  const context = options.context ?? activityContext;
  const lanesDirectory = resolve(options.lanesDirectory ?? join(homedir(), '.codex/lanes'));
  const pollIntervalMs = options.pollIntervalMs ?? 2000;
  const launchTimeoutMs = options.launchTimeoutMs ?? 30_000;
  const env = { ...(options.env ?? process.env) };
  // The Codex subprocess needs its own login, not the server/database/cloud credentials.
  for (const key of Object.keys(env)) if (/^(CAPSTAN_|AWS_|ANTHROPIC_API_KEY$|GITHUB_TOKEN$|GH_TOKEN$)/.test(key)) delete env[key];

  async function createLaneWorktree(input: PrepareLaneInput): Promise<PreparedLane> {
    context().signal.throwIfAborted();
    if (!/^[a-z][a-z0-9-]{0,39}$/.test(input.lane.name) || input.lane.name === 'none'
      || !['low', 'medium', 'high', 'xhigh'].includes(input.lane.effort)) stop('Invalid lane name or effort');
    const repo = await realpath(input.repo);
    if (await git(repo, 'rev-parse', '--show-toplevel') !== repo) stop('repo must be the root of a local Git checkout');
    if ((await command('git', ['check-ref-format', '--branch', input.lane.branch])).code !== 0
      || input.lane.branch.startsWith('-')) stop('Invalid branch');
    const common = await git(repo, 'rev-parse', '--path-format=absolute', '--git-common-dir');
    const stateDir = join(common, 'capstan-codex-lanes', digest(`${input.runId}/${input.lane.name}`));
    await mkdir(stateDir, { recursive: true });
    const request = { ...input, repo };
    let manifest = await readJson<Manifest>(join(stateDir, 'worktree.json'));
    if (!manifest) {
      await assertClean(repo);
      const targetBranch = await git(repo, 'branch', '--show-current');
      if (!targetBranch || input.lane.branch === targetBranch) stop('A lane needs a new branch and a named target branch');
      if ((await command('git', ['-C', repo, 'show-ref', '--verify', '--quiet', `refs/heads/${input.lane.branch}`])).code === 0) {
        // Another attempt may have published ownership and created the branch
        // while this attempt was probing it. Validate that manifest below.
        manifest = await readJson<Manifest>(join(stateDir, 'worktree.json'));
        if (!manifest) stop('Lane branch already exists; choose a new branch');
      }
      manifest ??= await publish(join(stateDir, 'worktree.json'), { request, prepared: {
        name: input.lane.name, branch: input.lane.branch, effort: input.lane.effort, repo,
        stateDir, worktree: join(stateDir, 'worktree'), briefPath: join(stateDir, 'brief.md'),
        baseCommit: await git(repo, 'rev-parse', 'HEAD'), targetBranch,
      } });
    }
    if (JSON.stringify(manifest.request) !== JSON.stringify(request)) stop('Worktree key was reused for different input');
    const prepared = manifest.prepared;
    const brief = `${input.brief}\n\nLane ${input.lane.name}\n${input.lane.brief}\n\nWork only in this worktree on ${input.lane.branch}. Commit locally, never push or merge.\nFinish with a concise report of files, tests and commit. Do not start other agents or lanes.\n`;
    // The brief is stable, and only complete published content reaches the launcher.
    if (await publishText(prepared.briefPath, brief) !== brief) stop('Published lane brief changed');
    try {
      await stat(join(prepared.worktree, '.git'));
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      const ref = await command('git', ['-C', repo, 'rev-parse', '--verify', `refs/heads/${prepared.branch}`]);
      if (ref.code === 0 && ref.stdout.trim() !== prepared.baseCommit) stop('Incomplete worktree has a changed branch');
      // Git serializes its own metadata. A concurrent losing attempt retries and attaches.
      if (ref.code === 0) await git(repo, 'worktree', 'add', prepared.worktree, prepared.branch);
      else await git(repo, 'worktree', 'add', '-b', prepared.branch, prepared.worktree, prepared.baseCommit);
    }
    await assertWorktree(prepared);
    const worktreeGit = await git(prepared.worktree, 'rev-parse', '--absolute-git-dir');
    let checkoutLocked = false;
    try { await stat(join(worktreeGit, 'locked')); checkoutLocked = true; }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
    if (checkoutLocked) throw new Error('Worktree checkout is still locked; retry after Git finishes or inspect interrupted creation');
    await assertClean(prepared.worktree);
    return prepared;
  }

  async function runCodexLane(prepared: PreparedLane): Promise<LaneReport> {
    const ctx = context(); ctx.signal.throwIfAborted(); await assertWorktree(prepared);
    const laneName = `cw-${prepared.name}-${digest(ctx.idempotencyKey).slice(0, 32)}`;
    const laneDir = join(lanesDirectory, laneName);
    const launchDir = join(prepared.stateDir, `launch-${digest(ctx.idempotencyKey)}`);
    await mkdir(launchDir, { recursive: true });
    const request = { key: ctx.idempotencyKey, laneName, worktree: prepared.worktree, briefPath: prepared.briefPath, effort: prepared.effort };
    const requestPath = join(launchDir, 'request.json');
    if (JSON.stringify(await publish(requestPath, request)) !== JSON.stringify(request)) stop('Activity key was reused for another lane');
    const started = Date.now();
    let helperStarted = false;
    let attached = false;
    let finishedSince: number | undefined;
    for (;;) {
      ctx.signal.throwIfAborted();
      const status = await command('codex-lane', ['status', laneName], { env, signal: ctx.signal });
      const line = status.stdout.split('\n')[0];
      const running = line?.startsWith(`lane ${laneName}: running (pid `);
      const finished = line === `lane ${laneName}: finished`;
      ctx.heartbeat({ laneName, state: running ? 'running' : finished ? 'finished' : 'launching', attempt: ctx.attempt });
      if (running || finished) {
        let cwd = '';
        try { cwd = (await readFile(join(laneDir, 'cwd'), 'utf8')).trim(); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
        if (cwd !== prepared.worktree) {
          // The CLI publishes its directory, then cwd, then starts its runner.
          if (running || Date.now() - started > launchTimeoutMs) stop('Existing lane belongs to another worktree or its cwd was never published');
          await delay(pollIntervalMs, undefined, { signal: ctx.signal });
          continue;
        }
        if (!attached) {
          console.info(JSON.stringify({ event: 'codex-lane-attached', laneName, attempt: ctx.attempt, workerPid: process.pid, state: running ? 'running' : 'finished' }));
          attached = true;
        }
        if (running) finishedSince = undefined;
        if (finished) {
          const reports = (await readdir(laneDir)).filter(n => /^last-message-\d+\.md$/.test(n))
            .sort((a, b) => Number(b.match(/\d+/)![0]) - Number(a.match(/\d+/)![0]));
          const reportPath = reports[0] && join(laneDir, reports[0]);
          if (reportPath && (await stat(reportPath)).size > 0) {
            await assertWorktree(prepared); await assertClean(prepared.worktree);
            const head = await git(prepared.worktree, 'rev-parse', 'HEAD');
            if (head === prepared.baseCommit) stop('Lane finished without a committed change', 'LaneDidNotCommit');
            if ((await command('git', ['-C', prepared.repo, 'merge-base', '--is-ancestor', prepared.baseCommit, head])).code !== 0) stop('Lane replaced its base history');
            return { ...prepared, laneName, reportPath, head };
          }
          finishedSince ??= Date.now();
          // status calls a lane finished even during startup, before run.pid exists.
          if (Date.now() - finishedSince > launchTimeoutMs) stop('Lane stopped without a report; inspect its local logs', 'LaneNoReport');
        }
      } else if (status.stderr.trim() === `codex-lane: no lane '${laneName}'`) {
        if (!helperStarted) {
          await assertClean(prepared.worktree);
          const log = await open(join(launchDir, 'launcher.log'), 'a', 0o600);
          try {
            // A detached helper owns the claim and short launch call. Worker SIGKILL
            // cannot strand a caller between a durable claim and spawning the helper.
            const helper = spawn(process.execPath, [fileURLToPath(new URL('./launch.mjs', import.meta.url)), requestPath], {
              env, detached: true, stdio: ['ignore', log.fd, log.fd],
            });
            await new Promise<void>((resolve, reject) => { helper.once('spawn', resolve); helper.once('error', reject); });
            helper.unref(); helperStarted = true;
          } finally { await log.close(); }
        }
        if (Date.now() - started > launchTimeoutMs) stop('Launch claim exists but no lane appeared; refusing to launch a second copy', 'LaneLaunchUncertain');
      } else {
        throw new Error('codex-lane status returned an unrecognized response; refusing to launch');
      }
      // Abort ends attachment only. A worker shutdown, stale lease or network outage
      // must not stop independent work that the next attempt will attach to.
      await delay(pollIntervalMs, undefined, { signal: ctx.signal });
    }
  }

  async function mergeLane(lane: LaneReport): Promise<MergeResult> {
    context().signal.throwIfAborted();
    if (!/^[a-f0-9]{40,64}$/.test(lane.head)) stop('Invalid approved commit');
    if (await git(lane.repo, 'branch', '--show-current') !== lane.targetBranch) stop('Merge target branch changed');
    await assertClean(lane.repo);
    const result = { name: lane.name, branch: lane.branch, head: lane.head };
    // The commit in the report is the approved effect. A lost completion acknowledgement
    // cannot repeat it: it is already reachable from the target on the next attempt.
    if ((await command('git', ['-C', lane.repo, 'merge-base', '--is-ancestor', lane.head, 'HEAD'])).code === 0) return result;
    if (await git(lane.repo, 'rev-parse', 'HEAD') !== lane.baseCommit) stop('Merge target moved; review it before starting another run');
    await git(lane.repo, 'merge', '--ff-only', '--', lane.head);
    return result;
  }
  return { createLaneWorktree, runCodexLane, mergeLane };
}
