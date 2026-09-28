import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import test from 'node:test';

const read = (path) => readFileSync(new URL(`../../${path}`, import.meta.url), 'utf8');

test('CI uses the local toolchain, shared-database contract, and complete merge gate', () => {
  assert.ok(existsSync(new URL('../../.github/workflows/verify.yml', import.meta.url)), 'verify workflow is required');
  const workflow = read('.github/workflows/verify.yml');
  for (const expected of ['push:', 'pull_request:', 'ubuntu-24.04', 'postgres:16-alpine', '55432:5432', 'POSTGRES_USER: capstan', 'POSTGRES_PASSWORD: capstan', 'POSTGRES_DB: capstan', 'pg_isready -U capstan -d capstan', 'go-version: "1.26.4"', 'node-version: "26"', 'version: "1.73.0"', 'cache: true', 'npm ci', 'make verify', 'cancel-in-progress: true', '${{ github.ref }}']) {
    assert.ok(workflow.includes(expected), `CI must contain ${expected}`);
  }
  assert.match(workflow, /go list -m -f '{{.Version}}' google.golang.org\/protobuf/);
  assert.match(workflow, /go list -m -f '{{.Version}}' connectrpc.com\/connect/);
  assert.match(workflow, /go install "google.golang.org\/protobuf\/cmd\/protoc-gen-go@\$protobuf_version"/);
  assert.match(workflow, /go install "connectrpc.com\/connect\/cmd\/protoc-gen-connect-go@\$connect_version"/);
  assert.match(workflow, /permissions:\s+contents: read/);
  assert.doesNotMatch(workflow, /id-token: write|aws-actions|pull_request_target|pg-up|pg-down/);
});

test('CI also checks the CDK templates without AWS credentials and builds the ARM image', () => {
  const workflow = read('.github/workflows/verify.yml');
  for (const expected of ['working-directory: infra', 'npm run typecheck', 'npm test', 'imageTag=test', 'budgetEmail=test@example.com', 'AWS_EC2_METADATA_DISABLED', 'AWS_SHARED_CREDENTIALS_FILE: /dev/null', 'linux/arm64', 'push: false']) {
    assert.ok(workflow.includes(expected), `infra CI must contain ${expected}`);
  }
});
