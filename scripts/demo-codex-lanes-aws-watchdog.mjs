// A local reminder and early teardown trigger; it creates no AWS resources.
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { root } from './evidence-lib.mjs';
const sessionPath = join(root, '.lane/aws-session-2026-09-28.json');
// Restarting with the original fallback path keeps the same authorized window.
const initial = JSON.parse(readFileSync(process.argv[2] ?? sessionPath, 'utf8'));
const startedAt = Date.parse(initial.startedAt);
if (!Number.isFinite(startedAt)) throw new Error('Invalid watchdog start time');
// A frozen receipt lets the timer finish cleanup even if an interrupted writer
// leaves the main progress receipt unreadable. The original window never resets.
const fallbackPath = join(root, '.lane', `aws-watchdog-${startedAt}-session.json`);
if (!existsSync(fallbackPath)) writeFileSync(fallbackPath, JSON.stringify(initial, null, 2) + '\n', { mode: 0o600, flag: 'wx' });
const frozen = JSON.parse(readFileSync(fallbackPath, 'utf8'));
if (frozen.startedAt !== initial.startedAt || frozen.hardDeadline !== initial.hardDeadline || frozen.account !== initial.account) {
  throw new Error('Frozen watchdog receipt does not match the original authorization');
}
writeFileSync(join(root, initial.watchdogPidFile ?? '.lane/aws-watchdog.pid'), String(process.pid));
console.log(`${new Date().toISOString()} Watchdog ${process.pid} armed for session ${initial.startedAt}; early teardown at 150 minutes.`);
let warned = false;
let replacementWarned = false;
for (;;) {
  let session;
  try { session = JSON.parse(readFileSync(sessionPath, 'utf8')); }
  catch (error) { console.error('Session receipt temporarily unreadable:', error.message); session = initial; }
  if (session.startedAt !== initial.startedAt) {
    if (!replacementWarned) console.error('Main receipt belongs to another session; retaining the original watchdog window and cleanup receipt.');
    replacementWarned = true;
    session = frozen;
  }
  if (session.cleanedAt && session.teardownVerified) break;
  const elapsed = Date.now() - startedAt;
  if (!warned && elapsed >= 90 * 60_000) { console.log('90 minutes elapsed. Finish the proof and start teardown well before the four-hour limit.'); warned = true; }
  if (elapsed >= 150 * 60_000) {
    console.log('150 minutes elapsed. Starting runbook teardown, leaving 90 minutes for cleanup.');
    const child = spawn(process.execPath, [join(root, 'scripts/demo-codex-lanes-aws-cleanup.mjs'), fallbackPath], { cwd: root, stdio: 'inherit' });
    const code = await new Promise(resolve => { child.once('error', error => { console.error(error); resolve(1); }); child.once('exit', resolve); });
    if (code === 0) {
      const completed = JSON.parse(readFileSync(fallbackPath, 'utf8'));
      if (!completed.cleanedAt || !completed.teardownVerified) throw new Error('Cleanup exited without verified completion');
      // Never overwrite a later session or stale-copy progress written during cleanup.
      let current;
      try { current = JSON.parse(readFileSync(sessionPath, 'utf8')); } catch {}
      if (current?.startedAt === initial.startedAt) {
        writeFileSync(sessionPath, JSON.stringify({ ...completed, ...current,
          destroyCommandsFinishedAt: completed.destroyCommandsFinishedAt,
          githubVariablesAfter: completed.githubVariablesAfter,
          cleanedAt: completed.cleanedAt, teardownVerified: true,
        }, null, 2) + '\n');
      }
      break;
    }
    console.error(`${new Date().toISOString()} Cleanup exited ${code}; retrying in 30 seconds. Hard deadline: ${initial.hardDeadline}`);
    await delay(30_000);
    continue;
  }
  await delay(10_000);
}
console.log('AWS watchdog finished.');
