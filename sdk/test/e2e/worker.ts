// A worker process for the end-to-end suites. E2E_WORKFLOWS selects the workflow file
// (default: the conformance corpus). With E2E_HANG set, the hanging activities write a
// marker file and then park forever, so a test can SIGKILL the process at a known point.
import { execFileSync } from "node:child_process";
import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { activityContext, Worker } from "../../src/worker/index.ts";

const hang = process.env.E2E_HANG;
const hangStep = process.env.E2E_HANG_STEP;
function park(marker: string): Promise<never> {
  writeFileSync(marker, String(process.pid));
  return new Promise(() => {});
}
function sql(query: string): void {
  execFileSync("psql", ["-X", "-qAt", "-v", "ON_ERROR_STOP=1", process.env.E2E_DATABASE_URL!, "-c", query]);
}

const worker = await Worker.create({
  address: process.env.E2E_ADDRESS!,
  apiKey: process.env.E2E_API_KEY!,
  taskQueue: process.env.E2E_QUEUE!,
  workflowsPath: process.env.E2E_WORKFLOWS ?? fileURLToPath(new URL("../../../conformance/workflows.ts", import.meta.url)),
  identity: process.env.E2E_IDENTITY ?? "e2e",
  logger: () => {},
  activities: {
    double: async (n: number) => {
      if (hang) await park(hang);
      return n * 2;
    },
    step: async (input: { index: number }) => {
      if (hang && String(input.index) === hangStep) await park(hang);
      return input.index * 2;
    },
    // The destination honours the activity key: one row per key, every attempt logged.
    deposit: async (input: { amount: number }) => {
      const context = activityContext();
      sql(`insert into e2e_attempt(run_id, key, attempt, pid) values ('${context.runId}', '${context.idempotencyKey}', ${context.attempt}, ${process.pid})`);
      sql(`insert into e2e_effect(key, run_id, amount, pid) values ('${context.idempotencyKey}', '${context.runId}', ${input.amount}, ${process.pid}) on conflict (key) do nothing`);
      if (hang) await park(hang);
      return { key: context.idempotencyKey };
    },
    hang: async () => {
      if (hang) await park(hang);
      return "continued";
    },
  },
});
process.on("SIGTERM", () => void worker.shutdown(0).then(() => process.exit(0)));
await worker.run();
