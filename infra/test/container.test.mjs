import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const root = fileURLToPath(new URL('../../', import.meta.url));

test('build context excludes local secrets, evidence, and dependency trees', () => {
  assert.ok(existsSync(`${root}.dockerignore`), '.dockerignore is required');
  const lines = readFileSync(`${root}.dockerignore`, 'utf8').split('\n');
  for (const path of ['.git', '.env', '.env.*', '.lane', 'sdk/node_modules', 'infra/node_modules', 'infra/cdk.out']) {
    assert.ok(lines.includes(path), `exclude ${path}`);
  }
});

test('compose server is opt-in, loopback-only, and waits for the existing postgres', () => {
  const config = JSON.parse(execFileSync('docker', ['compose', '--profile', 'full', 'config', '--format', 'json'], { cwd: root }));
  const server = config.services.server;
  assert.ok(server, 'full profile must contain the server');
  assert.deepEqual(server.profiles, ['full']);
  assert.equal(server.depends_on.postgres.condition, 'service_healthy');
  assert.equal(server.environment.CAPSTAN_ADDR, ':7233');
  assert.equal(server.environment.CAPSTAN_POLL_TIMEOUT, '20s');
  assert.match(server.environment.CAPSTAN_DATABASE_URL, /@postgres:5432\/capstan/);
  assert.equal(server.ports[0].host_ip, '127.0.0.1');
  assert.equal(server.ports[0].target, 7233);
  assert.equal(config.services.postgres.ports[0].published, '55432');
  assert.equal(config.services.postgres.image, 'postgres:16-alpine');
});
