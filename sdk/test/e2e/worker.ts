// A worker process for the end-to-end test. With E2E_HANG set, the "double" activity writes
// a marker file and then hangs, so the test can kill the process mid-activity.
import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { Worker } from "../../src/worker/index.ts";

const hang = process.env.E2E_HANG;
const worker = await Worker.create({
  address: process.env.E2E_ADDRESS!,
  apiKey: process.env.E2E_API_KEY!,
  taskQueue: process.env.E2E_QUEUE!,
  workflowsPath: fileURLToPath(new URL("../../../conformance/workflows.ts", import.meta.url)),
  identity: process.env.E2E_IDENTITY ?? "e2e",
  logger: () => {},
  activities: {
    double: async (n: number) => {
      if (hang) {
        writeFileSync(hang, String(process.pid));
        await new Promise(() => {});
      }
      return n * 2;
    },
  },
});
process.on("SIGTERM", () => void worker.shutdown(0).then(() => process.exit(0)));
await worker.run();
