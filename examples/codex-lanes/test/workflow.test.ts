import assert from 'node:assert/strict';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { fromJson, toJson, type JsonObject } from '../../../sdk/node_modules/@bufbuild/protobuf/dist/esm/index.js';
import { CommandSchema, HistoryEventSchema } from '../../../sdk/src/gen/capstan/v1/capstan_pb.ts';
import { FixtureBuilder } from '../../../sdk/test/fixtures/build.ts';
import { bundleWorkflows } from '../../../sdk/src/sandbox/bundle.ts';
import { replay } from '../../../sdk/src/replay/runtime.ts';

const payload = (value: unknown) => ({ contentType: 'application/json', data: Buffer.from(JSON.stringify(value)).toString('base64') });
const decode = (value: any) => JSON.parse(Buffer.from(value.data, 'base64').toString());
for (const choice of ['beta', 'none']) test(`sandbox replay preserves fan-out, long timeouts, and human ${choice}`, async () => {
  const input = { repo: '/repo', brief: 'small changes', lanes: ['alpha', 'beta'].map(name => ({ name, branch: `lane/${name}`, brief: `add ${name}`, effort: 'xhigh' })) };
  const bundle = await bundleWorkflows(fileURLToPath(new URL('../workflow.ts', import.meta.url)));
  const h = new FixtureBuilder('codexLanes', input).task();
  const run = async () => (await replay({ bundle, runId: 'test-run', history: h.history.map(e => fromJson(HistoryEventSchema, e)) })).map(c => toJson(CommandSchema, c)) as any[];
  const recordActivities = (commands: any[]) => {
    for (const { scheduleActivity: a } of commands) {
      const { seq, activityType, input, ...extra } = a;
      h.scheduleActivity(Number(seq), activityType, decode(input), extra);
    }
  };
  const creates = await run(); assert.equal(creates.length, 2);
  assert(creates.every(c => c.scheduleActivity.activityType === 'createLaneWorktree'));
  recordActivities(creates);
  const prepared = input.lanes.map(l => ({ ...l, repo: '/repo', worktree: `/repo/${l.name}`, baseCommit: 'a'.repeat(40), targetBranch: 'main', stateDir: '/state', briefPath: '/brief' }));
  h.completeActivity(2, prepared[1]).completeActivity(1, prepared[0]).task();
  const lanes = await run(); assert.equal(lanes.length, 2);
  for (const c of lanes) {
    assert.equal(c.scheduleActivity.activityType, 'runCodexLane');
    assert.equal(c.scheduleActivity.startToCloseTimeout, '3600s');
    assert.equal(c.scheduleActivity.scheduleToCloseTimeout, '7200s');
    assert.equal(c.scheduleActivity.heartbeatTimeout, '30s');
  }
  recordActivities(lanes);
  const reports = prepared.map(l => ({ ...l, laneName: `cw-${l.name}`, head: 'b'.repeat(40), reportPath: `/reports/${l.name}` }));
  h.completeActivity(4, reports[1]).completeActivity(3, reports[0]).task();
  const approval = await run(); assert.equal(approval.length, 2);
  h.command('markerRecorded', approval[0].recordMarker as JsonObject);
  const request = approval[1].requestApproval;
  assert.deepEqual(request.options, ['alpha', 'beta', 'none']);
  assert.equal(request.source, 'APPROVAL_SOURCE_HUMAN');
  h.command('approvalRequested', request);
  h.external('approvalResolved', { seq: request.seq, approvalId: request.approvalId, outcome: 'APPROVAL_OUTCOME_APPROVED', choice, resolver: 'owner' }).task();
  const after = await run();
  if (choice === 'none') { assert.equal(decode(after[0].completeRun.result).merged, null); return; }
  assert.equal(after.length, 1); assert.equal(after[0].scheduleActivity.activityType, 'mergeLane');
  assert.equal(decode(after[0].scheduleActivity.input).name, 'beta');
  recordActivities(after);
  h.completeActivity(Number(after[0].scheduleActivity.seq), { name: 'beta', head: reports[1]!.head, branch: 'lane/beta' }).task();
  const complete = await run(); assert.equal(decode(complete[0].completeRun.result).merged.name, 'beta');
  assert.equal(decode(complete[0].completeRun.result).lanes.length, 2);
});
