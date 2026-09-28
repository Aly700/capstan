import { fileURLToPath } from 'node:url';
import { Worker } from '../../sdk/src/worker/index.ts';
import { createLaneActivities } from './activities.ts';

if (!process.env.CAPSTAN_ADDRESS || !process.env.CAPSTAN_API_KEY) throw new Error('CAPSTAN_ADDRESS and CAPSTAN_API_KEY are required');
const worker = await Worker.create({
  address: process.env.CAPSTAN_ADDRESS, apiKey: process.env.CAPSTAN_API_KEY,
  taskQueue: process.env.CODEX_LANES_QUEUE ?? 'codex-lanes',
  identity: process.env.CODEX_LANES_IDENTITY ?? `codex-lanes:${process.pid}`,
  workflowsPath: fileURLToPath(new URL('./workflow.ts', import.meta.url)),
  activities: createLaneActivities(), maxConcurrentWorkflowTasks: 2, maxConcurrentActivities: 8,
});
for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => void worker.shutdown(0));
console.log(JSON.stringify({ workerReady: true, pid: process.pid }));
await worker.run();
