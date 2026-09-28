import { fileURLToPath } from "node:url";
import { Code, ConnectError } from "@connectrpc/connect";
import { afterEach, expect, it, vi } from "vitest";
import { Worker } from "../src/worker/index.ts";
import { ApplicationFailure } from "../src/types.ts";
import type { FailActivityTaskRequest } from "../src/gen/capstan/v1/capstan_pb.ts";
import { decode } from "../src/internal/payload.ts";
import { fakeServer } from "./fake-server.ts";

const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const close of cleanup.reverse()) await close(); cleanup.length = 0; vi.unstubAllEnvs(); });

it("redacts configured credentials from activity failures, causes, details, stacks and worker logs", async () => {
  const apiKey = "audit-worker-api-canary";
  const gateApiKey = "audit-worker-gate-canary";
  const providerKey = "audit-worker-provider-canary";
  const dsn = "postgres://audit:audit-worker-db-canary@127.0.0.1:55432/capstan_audit_unused";
  const secrets = [apiKey, gateApiKey, providerKey, dsn, "audit-worker-db-canary"];
  vi.stubEnv("ANTHROPIC_API_KEY", providerKey);
  vi.stubEnv("CAPSTAN_DATABASE_URL", dsn);
  vi.stubEnv("CAPSTAN_KEEP_STACKS", "1");
  let sent = false;
  const server = await fakeServer({ worker: {
    pollActivityTask: async (_request, context) => {
      if (!sent) { sent = true; return { taskToken: new Uint8Array([1]), runId: "audit", activityType: "fail", seq: 1n, startToCloseTimeout: { seconds: 5n } }; }
      if (!context.signal.aborted) await new Promise<void>((resolve) => context.signal.addEventListener("abort", () => resolve(), { once: true }));
      return {};
    },
    pollWorkflowTask: () => { throw new ConnectError("upstream echoed " + secrets.join(" "), Code.Unavailable); },
  }});
  cleanup.push(() => server.close());
  const logs: Record<string, unknown>[] = [];
  const worker = await Worker.create({
    address: server.address, apiKey, gateApiKey, taskQueue: "audit", maxConcurrentActivities: 1, maxConcurrentWorkflowTasks: 1,
    workflowsPath: fileURLToPath(new URL("./worker-fixtures/workflows.ts", import.meta.url)), logger: (entry) => logs.push(entry),
    activities: { fail() {
      const error = new ApplicationFailure("dependency echoed " + secrets.join(" "), { type: "DependencyFailure", nonRetryable: true, details: { nested: secrets }, cause: new Error(providerKey) });
      error.stack = "upstream stack " + secrets.join(" ");
      throw error;
    } },
  });
  cleanup.push(() => worker.shutdown(0));
  void worker.run();
  await expect.poll(() => server.requests("FailActivityTask").length).toBe(1);
  await expect.poll(() => logs.length).toBeGreaterThan(0);
  const failure = server.requests<FailActivityTaskRequest>("FailActivityTask")[0]!.failure!;
  const recorded = JSON.stringify({ message: failure.message, stack: failure.stack, details: decode(failure.details), cause: failure.cause?.message, logs });
  for (const secret of secrets) expect(recorded).not.toContain(secret);
  expect(failure).toMatchObject({ type: "DependencyFailure", nonRetryable: true, cause: { message: "[REDACTED]" } });
  expect(decode(failure.details)).toEqual({ nested: secrets.map(() => "[REDACTED]") });
});
