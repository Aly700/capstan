import assert from 'node:assert/strict';
import test from 'node:test';
import { deleteOwnedInactiveDefinitions } from './demo-codex-lanes-aws-taskdefs.mjs';

const startedAt = '2026-09-28T18:23:13.721Z';
const arn = 'arn:aws:ecs:us-east-1:280517746513:task-definition/capstan-server:1';
const definition = () => ({ taskDefinition: { taskDefinitionArn: arn, status: 'INACTIVE', registeredAt: '2026-09-28T19:01:06Z' }, tags: [{ key: 'Project', value: 'capstan' }] });
const baseline = () => ({ resources: { ecsActiveTaskDefinitions: [], ecsInactiveTaskDefinitions: [] } });
function fixture(details = definition(), before = baseline()) {
  const calls = [];
  const aws = args => {
    calls.push(args);
    if (args[1] === 'list-task-definitions') return { taskDefinitionArns: [arn] };
    if (args[1] === 'describe-task-definition') return details;
    if (args[1] === 'delete-task-definitions') return { taskDefinitions: [{ taskDefinitionArn: arn, status: 'DELETE_IN_PROGRESS' }], failures: [] };
    throw new Error(`Unexpected AWS call: ${args}`);
  };
  return { calls, run: () => deleteOwnedInactiveDefinitions({ aws, baseline: before, startedAt }) };
}

test('deletes only a tagged inactive revision created during this empty-baseline session', () => {
  const f = fixture();
  assert.equal(f.run()[0].taskDefinitions[0].taskDefinitionArn, arn);
  assert.deepEqual(f.calls.at(-1), ['ecs', 'delete-task-definitions', '--task-definitions', arn]);
});

for (const [name, change] of [
  ['preexisting revision', d => { d.taskDefinition.registeredAt = '2026-09-27T19:00:00Z'; }],
  ['active revision', d => { d.taskDefinition.status = 'ACTIVE'; }],
  ['foreign family', d => { d.taskDefinition.taskDefinitionArn = arn.replace('capstan-server:', 'other:'); }],
  ['missing ownership tag', d => { d.tags = []; }],
]) test(`refuses ${name} before deleting anything`, () => {
  const d = definition(); change(d); const f = fixture(d);
  assert.throws(f.run);
  assert(!f.calls.some(c => c[1] === 'delete-task-definitions'));
});

test('refuses a session whose baseline already contained task definitions', () => {
  const b = baseline(); b.resources.ecsInactiveTaskDefinitions.push(arn); const f = fixture(definition(), b);
  assert.throws(f.run);
  assert(!f.calls.some(c => c[1] === 'delete-task-definitions'));
});
