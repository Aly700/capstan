import { appendFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { activityContext, Worker } from "../../src/worker/index.ts";

function effect(input: unknown) {
  const context = activityContext();
  appendFileSync(process.env.AGENT_E2E_EFFECTS!, `${JSON.stringify({ runId: context.runId, activityType: context.activityType, idempotencyKey: context.idempotencyKey, pid: process.pid, input })}\n`);
  return { executed: context.activityType, input };
}
const worker = await Worker.create({
  address: process.env.AGENT_E2E_ADDRESS!, apiKey: process.env.AGENT_E2E_API_KEY!, taskQueue: process.env.AGENT_E2E_QUEUE!,
  workflowsPath: fileURLToPath(new URL("./workflows.ts", import.meta.url)), identity: process.env.AGENT_E2E_IDENTITY!,
  // A successful probe while an approval waits proves both single-task slots are free.
  maxConcurrentWorkflowTasks: 1, maxConcurrentActivities: 1,
  gateUrl: process.env.AGENT_E2E_GATE_URL!, gateApiKey: process.env.AGENT_E2E_GATE_KEY!,
  gatePolicyId: process.env.AGENT_E2E_POLICY!, gateAgentId: "capstan-e2e",
  activities: { "tool.allow": effect, "tool.deny": effect, "tool.approve": effect, probe: () => ({ pid: process.pid }) },
});
process.on("SIGTERM", () => void worker.shutdown(0).then(() => process.exit(0)));
process.send?.({ event: "ready", pid: process.pid });
await worker.run();
