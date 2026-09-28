import { Worker } from "../../sdk/src/worker/index.ts";

const worker = await Worker.create({
  address: process.env.CAPSTAN_ADDRESS!, apiKey: process.env.CAPSTAN_API_KEY!,
  taskQueue: process.env.EVIDENCE_QUEUE!, workflowsPath: process.env.EVIDENCE_WORKFLOWS!,
  identity: process.env.EVIDENCE_IDENTITY!,
  maxConcurrentWorkflowTasks: Number(process.env.EVIDENCE_WORKFLOW_CONCURRENCY ?? 10),
  maxConcurrentActivities: Number(process.env.EVIDENCE_ACTIVITY_CONCURRENCY ?? 10),
  activities: { step: (n: number) => n + 1, revisedStep: (n: number) => n + 10 },
});
process.on("SIGTERM", () => void worker.shutdown(0).then(() => process.exit(0)));
process.on("SIGINT", () => void worker.shutdown(0).then(() => process.exit(0)));
console.log(JSON.stringify({ workerReady: true, pid: process.pid, identity: process.env.EVIDENCE_IDENTITY }));
await worker.run();
