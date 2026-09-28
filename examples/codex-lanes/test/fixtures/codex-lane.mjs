#!/usr/bin/env node
// A deliberately non-atomic run command, matching the installed CLI's interface.
import { spawn, execFileSync } from 'node:child_process';
import { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync, unlinkSync } from 'node:fs';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

const [command, name, ...args] = process.argv.slice(2);
const root = process.env.FAKE_LANES_DIR;
const dir = join(root, name);
const waitFor = async (file) => { while (!existsSync(file)) await delay(20); };
const fail = (message) => { console.error(`codex-lane: ${message}`); process.exit(1); };
if (command === 'run') {
  appendFileSync(join(root, 'invocations'), `${name}\n`);
  if (process.env.FAKE_LAUNCH_WAIT === '1') await waitFor(join(root, 'launch-release'));
  if (existsSync(dir)) fail(`lane '${name}' exists`);
  const cwd = args[args.indexOf('--cwd') + 1];
  const brief = args[args.indexOf('--brief') + 1];
  const effort = args[args.indexOf('--effort') + 1];
  if (effort !== 'xhigh' || !existsSync(brief)) fail('invalid invocation');
  mkdirSync(dir, { recursive: true });
  if (process.env.FAKE_CWD_WAIT === '1') {
    writeFileSync(join(dir, 'cwd'), '');
    await delay(350);
  }
  writeFileSync(join(dir, 'cwd'), cwd + '\n');
  writeFileSync(join(dir, 'brief.md'), readFileSync(brief));
  const child = spawn(process.execPath, [process.argv[1], '_runner', name], { detached: true, stdio: 'ignore' });
  child.unref();
  console.log(`lane '${name}' started in ${cwd}`);
} else if (command === '_runner') {
  writeFileSync(join(dir, 'run.pid'), String(process.pid));
  await waitFor(join(root, 'release'));
  const cwd = readFileSync(join(dir, 'cwd'), 'utf8').trim();
  const git = (...a) => execFileSync('git', ['-C', cwd, ...a], { encoding: 'utf8' }).trim();
  const branch = git('branch', '--show-current');
  const file = `${branch.split('/').at(-1)}.txt`;
  writeFileSync(join(cwd, file), `completed ${branch}\n`);
  git('add', file);
  git('commit', '-qm', `add ${file}`);
  writeFileSync(join(dir, 'last-message-1.md'), `Committed ${git('rev-parse', 'HEAD')}; test passed.\n`);
  writeFileSync(join(dir, 'run.log'), '[00:00:00] lane finished after attempt 1\n');
  unlinkSync(join(dir, 'run.pid'));
} else if (command === 'status') {
  if (!existsSync(dir)) fail(`no lane '${name}'`);
  let pid;
  try { pid = Number(readFileSync(join(dir, 'run.pid'), 'utf8')); process.kill(pid, 0); } catch { pid = undefined; }
  console.log(`lane ${name}: ${pid ? `running (pid ${pid})` : 'finished'}`);
  console.log(`cwd: ${readFileSync(join(dir, 'cwd'), 'utf8').trim()}   session: fake-session`);
} else if (command === 'stop') {
  try { process.kill(Number(readFileSync(join(dir, 'run.pid'), 'utf8')), 'SIGTERM'); unlinkSync(join(dir, 'run.pid')); } catch {}
} else fail(`unknown command ${command}`);
