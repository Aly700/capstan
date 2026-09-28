import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const root = fileURLToPath(new URL('../../', import.meta.url));

test('image builds the real ARM64 entrypoint and runs without root', () => {
  assert.ok(existsSync(`${root}Dockerfile`), 'Dockerfile is required');
  const file = readFileSync(`${root}Dockerfile`, 'utf8');
  assert.match(file, /FROM .*golang:1\.26\.4.* AS build/);
  assert.match(file, /CGO_ENABLED=0 GOOS=linux GOARCH=arm64/);
  assert.match(file, /go build -trimpath -ldflags "-s -w".*\.\/cmd\/capstan-server/);
  assert.match(file, /FROM .*gcr\.io\/distroless\/static-debian12:nonroot/);
  assert.match(file, /EXPOSE 7233/);
  assert.match(file, /USER nonroot/);
  assert.match(file, /ENTRYPOINT \["\/capstan-server", "serve"\]/);
});

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
  const original = execFileSync('git', ['show', '6baf434:compose.yaml'], { cwd: root, encoding: 'utf8' });
  assert.ok(readFileSync(`${root}compose.yaml`, 'utf8').startsWith(original), 'existing postgres service stays byte-for-byte intact');
});
