// Coordinate the proof's own cleanup before a separate process destroys AWS.
import assert from 'node:assert/strict';
import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

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

export async function requestLocalShutdown(directory, startedAt, timeoutMs = 60_000) {
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
    if (Date.now() >= deadline) throw new Error('Local worker/lane cleanup is not verified; refusing AWS teardown. Inspect the owned proof before retrying.');
    await delay(20);
  }
}
