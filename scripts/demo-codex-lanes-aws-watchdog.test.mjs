import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

const startedAt = '2026-09-28T18:23:13.721981Z';
const initial = { account: '280517746513', startedAt, hardDeadline: '2026-09-28T22:23:13.721981Z', evidencePrefix: 'aws-test', baselineFile: '.lane/baseline.json' };
function fixture(script) {
  const root = mkdtempSync(join(tmpdir(), 'capstan-watchdog-test-'));
  for (const path of ['scripts', '.lane', 'docs/evidence']) mkdirSync(join(root, path), { recursive: true });
  for (const name of [script, 'demo-codex-lanes-aws-local.mjs']) copyFileSync(new URL(name, import.meta.url), join(root, 'scripts', name));
  writeFileSync(join(root, 'scripts/evidence-lib.mjs'), `export const root = ${JSON.stringify(root)};\n`);
  const sessionPath = join(root, '.lane/aws-session-2026-09-28.json');
  writeFileSync(sessionPath, JSON.stringify(initial));
  writeFileSync(join(root, '.lane/baseline.json'), JSON.stringify({ resources: { stacks: [] } }));
  return { root, sessionPath, script: join(root, 'scripts', script), preload: join(root, 'preload.mjs') };
}
function run(f, args = []) {
  return spawnSync(process.execPath, ['--import', f.preload, f.script, ...args], { encoding: 'utf8', timeout: 3000 });
}

test('watchdog keeps its frozen window and does not overwrite a replacement receipt', () => {
  const f = fixture('demo-codex-lanes-aws-watchdog.mjs');
  const replacement = { startedAt: '2026-09-29T18:23:13Z', cleanedAt: 'another session' };
  try {
    writeFileSync(f.preload, `
      import fs from 'node:fs'; import { syncBuiltinESMExports } from 'node:module';
      const read = fs.readFileSync; let first = true;
      fs.readFileSync = function(path, ...args) {
        const value = read(path, ...args);
        if (String(path) === ${JSON.stringify(f.sessionPath)} && first) {
          first = false; fs.writeFileSync(path, ${JSON.stringify(JSON.stringify(replacement))});
        }
        return value;
      };
      syncBuiltinESMExports(); Date.now = () => ${Date.parse(startedAt) + 150 * 60_000};
    `);
    writeFileSync(join(f.root, 'scripts/demo-codex-lanes-aws-cleanup.mjs'), `
      import { readFileSync, writeFileSync } from 'node:fs';
      const path = process.argv[2], session = JSON.parse(readFileSync(path));
      writeFileSync(path, JSON.stringify({ ...session, cleanedAt: 'verified', teardownVerified: true }));
    `);
    const result = run(f);
    assert.equal(result.status, 0, result.stderr);
    assert.deepEqual(JSON.parse(readFileSync(f.sessionPath)), replacement);
    const completed = JSON.parse(readFileSync(join(f.root, '.lane', `aws-watchdog-${Date.parse(startedAt)}-session.json`)));
    assert.equal(completed.startedAt, startedAt);
    assert.equal(completed.hardDeadline, initial.hardDeadline);
    assert.equal(completed.teardownVerified, true);
  } finally { rmSync(f.root, { recursive: true, force: true }); }
});

test('cleanup divides the remaining window between CDK groups and reserves verification time', () => {
  const f = fixture('demo-codex-lanes-aws-cleanup.mjs');
  const callsPath = join(f.root, 'calls.json');
  try {
    writeFileSync(f.preload, `
      import cp from 'node:child_process'; import fs from 'node:fs'; import { syncBuiltinESMExports } from 'node:module';
      let now = ${Date.parse(initial.hardDeadline) - 20 * 60_000}; const calls = [];
      Date.now = () => now;
      cp.spawnSync = (binary, args, options) => {
        let value = [];
        if (binary === 'aws') value = args.includes('get-caller-identity')
          ? { Account: '280517746513' }
          : { Name: args[args.indexOf('--stack-name') + 1], Created: ${JSON.stringify(startedAt)}, Status: 'CREATE_COMPLETE' };
        else if (args[0]?.endsWith('demo-codex-lanes-aws-inventory.mjs')) {
          fs.writeFileSync(args[1], JSON.stringify({ errors: {}, resources: { stacks: [], sharedOidcProvider: {} } }));
        } else if (binary !== 'gh') {
          calls.push({ binary, args, timeout: options.timeout, killSignal: options.killSignal, at: now });
          now += options.timeout;
        }
        fs.writeFileSync(${JSON.stringify(callsPath)}, JSON.stringify(calls));
        return { status: 0, stdout: JSON.stringify(value), stderr: '' };
      };
      syncBuiltinESMExports();
    `);
    const result = run(f);
    assert.equal(result.status, 0, result.stderr);
    const calls = JSON.parse(readFileSync(callsPath));
    assert.equal(calls.length, 4);
    assert(calls.every(call => call.timeout <= 15 * 60_000), 'individual CDK wait must be capped');
    assert(calls.reduce((sum, call) => sum + call.timeout, 0) <= 15 * 60_000, 'leave five minutes for inventory and variables');
    assert(calls.every(call => call.killSignal === 'SIGKILL'), 'bounded waits must not depend on SIGTERM handling');
  } finally { rmSync(f.root, { recursive: true, force: true }); }
});

test('watchdog restarts from its original receipt even when the main receipt belongs to a later session', () => {
  const f = fixture('demo-codex-lanes-aws-watchdog.mjs');
  try {
    const originalPath = join(f.root, '.lane', `aws-watchdog-${Date.parse(startedAt)}-session.json`);
    writeFileSync(originalPath, JSON.stringify(initial));
    writeFileSync(f.sessionPath, JSON.stringify({ startedAt: '2026-09-29T18:23:13Z' }));
    writeFileSync(f.preload, `Date.now = () => ${Date.parse(startedAt) + 150 * 60_000};`);
    writeFileSync(join(f.root, 'scripts/demo-codex-lanes-aws-cleanup.mjs'), `
      import { readFileSync, writeFileSync } from 'node:fs';
      const path = process.argv[2], session = JSON.parse(readFileSync(path));
      writeFileSync(path, JSON.stringify({ ...session, cleanedAt: 'verified', teardownVerified: true }));
    `);
    const result = run(f, [originalPath]);
    assert.equal(result.status, 0, result.stderr);
    const completed = JSON.parse(readFileSync(originalPath));
    assert.equal(completed.startedAt, startedAt);
    assert.equal(completed.hardDeadline, initial.hardDeadline);
    assert.equal(completed.teardownVerified, true);
    assert.equal(JSON.parse(readFileSync(f.sessionPath)).startedAt, '2026-09-29T18:23:13Z');
  } finally { rmSync(f.root, { recursive: true, force: true }); }
});
