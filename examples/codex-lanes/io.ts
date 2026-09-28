import { execFile } from 'node:child_process';
import { randomUUID, createHash } from 'node:crypto';
import { link, open, readFile, unlink } from 'node:fs/promises';
import { ApplicationFailure } from '../../sdk/src/types.ts';

export const digest = (text: string) => createHash('sha256').update(text).digest('hex');
export function stop(message: string, type = 'LaneStateConflict'): never {
  throw new ApplicationFailure(message, { type, nonRetryable: true });
}
export async function readJson<T>(path: string): Promise<T | undefined> {
  try { return JSON.parse(await readFile(path, 'utf8')) as T; }
  catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined; throw error; }
}
// Publish a complete file once, even when two attempts reach this point together.
export async function publish<T>(path: string, value: T): Promise<T> {
  return JSON.parse(await publishText(path, JSON.stringify(value))) as T;
}
export async function publishText(path: string, text: string): Promise<string> {
  const temp = `${path}.${randomUUID()}.tmp`;
  const file = await open(temp, 'wx', 0o600);
  try {
    await file.writeFile(text); await file.sync(); await file.close();
    try { await link(temp, path); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error; }
  } finally { await file.close(); await unlink(temp); }
  return readFile(path, 'utf8');
}
export function command(binary: string, args: string[], options: { cwd?: string; env?: NodeJS.ProcessEnv; signal?: AbortSignal } = {}) {
  return new Promise<{ code: number; stdout: string; stderr: string }>((resolve, reject) => {
    execFile(binary, args, { ...options, encoding: 'utf8', timeout: 30_000, maxBuffer: 4 * 1024 * 1024 }, (error, stdout, stderr) => {
      if (error && typeof error.code !== 'number') { reject(error); return; }
      resolve({ code: typeof error?.code === 'number' ? error.code : 0, stdout, stderr });
    });
  });
}
export async function git(repo: string, ...args: string[]): Promise<string> {
  const result = await command('git', ['-C', repo, ...args]);
  if (result.code !== 0) throw new Error(`git ${args[0]} failed: ${result.stderr.trim()}`);
  return result.stdout.trim();
}
