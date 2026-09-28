import assert from 'node:assert/strict';

// CloudFormation deregisters revisions but leaves INACTIVE metadata behind.
// Call only after the runbook's stack deletions; never deregister active work here.
export function deleteOwnedInactiveDefinitions({ aws, baseline, startedAt }) {
  for (const category of ['ecsActiveTaskDefinitions', 'ecsInactiveTaskDefinitions', 'ecsDeletingTaskDefinitions']) {
    assert.deepEqual(baseline.resources[category] ?? [], [], `preexisting definitions: ${category}`);
  }
  const start = Date.parse(startedAt);
  assert(Number.isFinite(start));
  const arns = aws(['ecs', 'list-task-definitions', '--family-prefix', 'capstan', '--status', 'INACTIVE']).taskDefinitionArns;
  // Validate every candidate before making the first mutation.
  for (const arn of arns) {
    assert.match(arn, /^arn:aws:ecs:us-east-1:280517746513:task-definition\/capstan-server:\d+$/);
    const { taskDefinition: task, tags } = aws(['ecs', 'describe-task-definition', '--task-definition', arn, '--include', 'TAGS']);
    assert.equal(task.taskDefinitionArn, arn);
    assert.equal(task.status, 'INACTIVE');
    assert(Date.parse(task.registeredAt) >= start - 5000 && Date.parse(task.registeredAt) <= Date.now(), `older or invalid revision: ${arn}`);
    assert(tags?.some(tag => tag.key === 'Project' && tag.value === 'capstan'), `missing ownership tag: ${arn}`);
  }
  const receipts = [];
  for (let i = 0; i < arns.length; i += 10) {
    const response = aws(['ecs', 'delete-task-definitions', '--task-definitions', ...arns.slice(i, i + 10)]);
    assert.deepEqual(response.failures ?? [], [], 'task-definition deletion failed');
    receipts.push(response);
  }
  return receipts;
}
