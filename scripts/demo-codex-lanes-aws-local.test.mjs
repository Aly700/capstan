import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { root } from './evidence-lib.mjs';
import { claimLocalProof, requestLocalShutdown } from './demo-codex-lanes-aws-local.mjs';

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
