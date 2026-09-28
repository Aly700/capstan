import { activity, human, workflowInfo, ApplicationFailure } from '../../sdk/src/workflow/index.ts';
import type { CodexLanesInput, LaneReport, MergeResult, PreparedLane } from './types.ts';

export async function codexLanes(input: CodexLanesInput) {
  if (!input || !input.repo || !input.brief || !input.lanes?.length
    || input.lanes.some(l => !/^[a-z][a-z0-9-]{0,39}$/.test(l.name) || l.name === 'none' || !l.brief || !l.branch)
    || new Set(input.lanes.map(l => l.name)).size !== input.lanes.length
    || new Set(input.lanes.map(l => l.branch)).size !== input.lanes.length) {
    throw new ApplicationFailure('Supply a repo, brief, and unique lane names/branches; none is reserved', { nonRetryable: true });
  }
  const runId = workflowInfo().runId;
  const prepared = await Promise.all(input.lanes.map(lane => activity<PreparedLane>('createLaneWorktree', {
    repo: input.repo, brief: input.brief, lane, runId,
  }, { startToCloseTimeout: '2m', scheduleToCloseTimeout: '5m' })));
  const lanes = await Promise.all(prepared.map(lane => activity<LaneReport>('runCodexLane', lane, {
    startToCloseTimeout: '60m', scheduleToCloseTimeout: '2h', heartbeatTimeout: '30s',
    retry: { initialInterval: '1s', maximumInterval: '30s' },
  })));
  const decision = await human(`merge lanes ${lanes.map(l => l.name).join(', ')}?`, {
    options: [...lanes.map(l => l.name), 'none'],
  });
  const chosen = decision.outcome === 'approved' && decision.choice !== 'none'
    ? lanes.find(l => l.name === decision.choice) : undefined;
  if (decision.outcome === 'approved' && decision.choice !== 'none' && !chosen) {
    throw new ApplicationFailure('Approval did not select a lane option', { nonRetryable: true });
  }
  const merged = chosen ? await activity<MergeResult>('mergeLane', chosen, {
    startToCloseTimeout: '2m', scheduleToCloseTimeout: '5m',
  }) : null;
  return { lanes, decision, merged };
}
