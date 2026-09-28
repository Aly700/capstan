// Runs outside the worker's process group. The permanent claim fences all retries,
// including two workers that both observed a missing lane before either launched it.
import { spawnSync } from 'node:child_process';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';

const requestPath = process.argv[2];
const request = JSON.parse(readFileSync(requestPath, 'utf8'));
const dir = dirname(requestPath);
try { mkdirSync(join(dir, 'claimed')); }
catch (error) { if (error.code === 'EEXIST') process.exit(0); throw error; }
writeFileSync(join(dir, 'claimed', 'owner.json'), JSON.stringify({ pid: process.pid, key: request.key }), { mode: 0o600 });
const result = spawnSync('codex-lane', ['run', request.laneName, '--cwd', request.worktree,
  '--brief', request.briefPath, '--effort', request.effort], { stdio: 'inherit', timeout: 120_000 });
writeFileSync(join(dir, 'exit.json'), JSON.stringify({ code: result.status, signal: result.signal, error: result.error?.message }), { mode: 0o600 });
process.exitCode = result.status ?? 1;
