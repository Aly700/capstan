import { appendFileSync, readFileSync, writeFileSync } from 'node:fs';
import { createLaneActivities } from '../../activities.ts';

const config = JSON.parse(readFileSync(process.argv[2]!, 'utf8'));
const controller = new AbortController();
const activities = createLaneActivities({
  lanesDirectory: config.lanesDirectory, pollIntervalMs: 30, launchTimeoutMs: 10_000,
  context: () => ({
    runId: 'test-run', workflowType: 'codexLanes', activityType: 'runCodexLane', seq: 3,
    attempt: config.attempt, idempotencyKey: 'test-run/3', heartbeatDetails: undefined,
    signal: controller.signal,
    heartbeat: (details) => appendFileSync(config.heartbeats, JSON.stringify(details) + '\n'),
  }),
});
try {
  writeFileSync(config.result, JSON.stringify(await activities.runCodexLane(config.prepared)));
} catch (error) {
  console.error(error);
  process.exitCode = 1;
}
