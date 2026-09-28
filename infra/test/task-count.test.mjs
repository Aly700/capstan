import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import test from 'node:test';

test('task count probe publishes the service count, handles absent services, and propagates API failure', () => {
  const source = new URL('../runtime/task-count.py', import.meta.url);
  assert.ok(existsSync(source), 'task count probe is required');
  const result = spawnSync('python3', ['-c', `
import os, runpy, sys, types
from unittest.mock import Mock
ecs, cw = Mock(), Mock()
sys.modules['boto3'] = types.SimpleNamespace(client=lambda service: ecs if service == 'ecs' else cw)
os.environ.update(CLUSTER='capstan', SERVICE='capstan-server')
handler = runpy.run_path(sys.argv[1])['handler']
for services, expected in [([{'runningCount': 1}], 1), ([{'runningCount': 0}], 0), ([], 0)]:
    ecs.describe_services.return_value = {'services': services}
    handler({}, None)
    ecs.describe_services.assert_called_with(cluster='capstan', services=['capstan-server'])
    data = cw.put_metric_data.call_args.kwargs
    assert data['Namespace'] == 'Capstan/Service'
    assert data['MetricData'][0]['Value'] == expected
    assert data['MetricData'][0]['Dimensions'] == [{'Name': 'ClusterName', 'Value': 'capstan'}, {'Name': 'ServiceName', 'Value': 'capstan-server'}]
cw.reset_mock()
ecs.describe_services.side_effect = RuntimeError('unavailable')
try:
    handler({}, None)
    raise AssertionError('API failure must surface; missing metric will alarm')
except RuntimeError:
    pass
cw.put_metric_data.assert_not_called()
print('PASS: real count, zero count, absent service, API failure')
`, source.pathname], { encoding: 'utf8' });
  assert.equal(result.status, 0, result.stdout + result.stderr);
});
