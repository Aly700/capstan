// The daily cap on the real server, with a fake Anthropic endpoint that counts requests.
// A reservation the cap cannot cover is refused before any provider call and leaves no
// ledger row; a worker killed after a completed model call is replaced by a worker that
// replays the recorded result, so the provider count and the ledger do not grow.
// Runs only when CAPSTAN_E2E=1 (make verify sets it).
import { existsSync } from "node:fs";
import { createServer, type Server } from "node:http";
import { once } from "node:events";
import { join } from "node:path";
import { createClient } from "@connectrpc/connect";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Evidence, root, until } from "./e2e/evidence.ts";
import { Client } from "../src/client/index.ts";
import { ClientService, EventType } from "../src/gen/capstan/v1/capstan_pb.ts";
import { createTransport } from "../src/worker/transport.ts";

const enabled = process.env.CAPSTAN_E2E === "1";
const workflows = join(root, "sdk/test/e2e/workflows.ts");
// claude-sonnet-5 at the default prices: 12 input tokens + 4 output tokens.
const expectedCost = (12 * 2 + 4 * 10) / 1_000_000;

async function fakeProvider(): Promise<{ server: Server; url: string; requests: () => number }> {
  let requests = 0;
  const server = createServer((request, response) => {
    requests++;
    request.resume();
    request.on("end", () => {
      response.setHeader("content-type", "application/json");
      response.end(JSON.stringify({
        id: "msg_fake", type: "message", role: "assistant", model: "claude-sonnet-5",
        content: [{ type: "text", text: "pong" }], stop_reason: "end_turn", stop_sequence: null,
        usage: { input_tokens: 12, output_tokens: 4 },
      }));
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("no provider port");
  return { server, url: `http://127.0.0.1:${address.port}`, requests: () => requests };
}

describe.skipIf(!enabled)("daily cap on the real server", () => {
  let provider: Awaited<ReturnType<typeof fakeProvider>>;
  beforeAll(async () => { provider = await fakeProvider(); });
  afterAll(async () => { provider.server.close(); });

  function stack(name: string, port: number, capUsd: string) {
    const env = new Evidence(name, port, { databasePrefix: "capstan_audit" });
    const count = (query: string) => Number(env.sql(query, ["-Atq"]).trim());
    const startWorker = (identity: string, extra: Record<string, string> = {}) =>
      env.child(process.execPath, ["--import", "tsx", join(root, "sdk/test/e2e/worker.ts")], identity, {
        E2E_ADDRESS: env.address, E2E_API_KEY: env.key, E2E_QUEUE: env.queue, E2E_IDENTITY: identity, E2E_WORKFLOWS: workflows,
        ANTHROPIC_API_KEY: "fake-provider-key", ANTHROPIC_BASE_URL: provider.url, ...extra,
      }, join(root, "sdk"));
    const setup = async () => {
      // Evidence passes the test process environment to the server it spawns.
      process.env.CAPSTAN_DAILY_CAP_USD = capUsd;
      await env.setup();
      return {
        client: new Client({ address: env.address, apiKey: env.key }),
        rpc: createClient(ClientService, createTransport({ address: env.address, apiKey: env.key })),
      };
    };
    return { env, count, startWorker, setup };
  }

  it("refuses a model call the cap cannot cover before the provider is called and records nothing", async () => {
    const { env, count, startWorker, setup } = stack("cap_refused", 7607, "0.00001");
    try {
      const { client, rpc } = await setup();
      const before = provider.requests();
      const worker = startWorker("capped-worker");
      const runId = `capped-${Date.now()}`;
      await client.start("ask", { prompt: "ping" }, { runId, taskQueue: env.queue });
      const done = await client.result(runId, { timeoutMs: 30_000 });
      expect(done.status).toBe("failed");
      const { events } = await rpc.getHistory({ runId });
      const failed = events.filter((event) => event.type === EventType.ACTIVITY_FAILED);
      expect(failed).toHaveLength(1);
      expect(failed[0]!.attributes).toMatchObject({ case: "activityFailed", value: { failure: { type: "ModelBudgetExceeded", nonRetryable: true } } });
      expect(provider.requests() - before).toBe(0);
      expect(count("select count(*) from ai_call")).toBe(0);
      await env.stop(worker);
    } finally {
      await env.cleanup();
    }
  }, 120_000);

  it("a worker killed after its model call is replaced without a second provider call or ledger row", async () => {
    const { env, count, startWorker, setup } = stack("cap_replay", 7608, "2");
    try {
      const { client } = await setup();
      const before = provider.requests();
      const marker = join(env.logdir, "hang-marker");
      const doomed = startWorker("model-doomed", { E2E_HANG: marker });
      const runId = `replayed-${Date.now()}`;
      await client.start("ask", { prompt: "ping" }, { runId, taskQueue: env.queue });
      await until("the model call to finish and the next activity to park", () => existsSync(marker));
      expect(provider.requests() - before).toBe(1);
      expect(env.sql("select status, cost_usd from ai_call order by id", ["-Atq"]).trim()).toBe(`2|${expectedCost.toFixed(6)}`);
      await env.stop(doomed, "SIGKILL");

      const survivor = startWorker("model-survivor");
      const done = await client.result(runId, { timeoutMs: 90_000 });
      expect(done.status).toBe("completed");
      expect(done.result).toEqual({ text: "pong", costUsd: expectedCost, after: "continued" });
      expect(provider.requests() - before).toBe(1);
      expect(count("select count(*) from ai_call")).toBe(1);
      expect(env.sql("select status, cost_usd from ai_call", ["-Atq"]).trim()).toBe(`2|${expectedCost.toFixed(6)}`);
      await env.stop(survivor);
    } finally {
      await env.cleanup();
    }
  }, 180_000);
});
