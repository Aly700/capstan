// A local reminder and early teardown trigger; it creates no AWS resources.
import { readFileSync, writeFileSync } from 'node:fs';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { root } from './evidence-lib.mjs';
const sessionPath = join(root, '.lane/aws-session-2026-09-28.json');
writeFileSync(join(root, '.lane/aws-watchdog.pid'), String(process.pid));
let warned = false;
for (;;) {
  const session = JSON.parse(readFileSync(sessionPath, 'utf8'));
  if (session.destroyCommandsFinishedAt || session.cleanedAt) break;
  const elapsed = Date.now() - Date.parse(session.startedAt);
  if (!warned && elapsed >= 90 * 60_000) { console.log('90 minutes elapsed. Finish the proof and start teardown well before the four-hour limit.'); warned = true; }
  if (elapsed >= 150 * 60_000) {
    console.log('150 minutes elapsed. Starting runbook teardown, leaving 90 minutes for cleanup.');
    const child = spawn(process.execPath, [join(root, 'scripts/demo-codex-lanes-aws-cleanup.mjs')], { stdio: 'inherit' });
    await new Promise((resolve, reject) => { child.once('error', reject); child.once('exit', code => code === 0 ? resolve() : reject(new Error(`cleanup exit ${code}`))); });
    break;
  }
  await delay(10_000);
}
console.log('AWS watchdog finished.');
