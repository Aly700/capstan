import { createServer, type IncomingMessage } from "node:http";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { create, type MessageInitShape } from "@bufbuild/protobuf";
import { DurationSchema } from "@bufbuild/protobuf/wkt";
import { Code, ConnectError } from "@connectrpc/connect";
import { afterEach, describe, expect, it, vi } from "vitest";
import { PollActivityTaskResponseSchema, type CompleteActivityTaskRequest, type FailActivityTaskRequest, type FinishAICallRequest, type ReserveAICallRequest } from "../src/gen/capstan/v1/capstan_pb.ts";
import { Worker, type WorkerOptions } from "../src/worker/index.ts";
import { decode, encode } from "../src/internal/payload.ts";
import { fakeServer } from "./fake-server.ts";

const workflowsPath = fileURLToPath(new URL("./agent-fixtures/workflows.ts", import.meta.url));
const decisionId = "00000000-0000-4000-8000-000000000001";
const approvalId = "00000000-0000-4000-8000-000000000002";
const policyId = "00000000-0000-4000-8000-000000000003";
const apiKey = "gate-secret-must-never-appear-in-history";
const input = { toolName: "fs.write", arguments: { path: "report.txt" }, riskTier: "HIGH" };
const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const close of cleanup.reverse()) await close(); cleanup.length = 0; vi.unstubAllEnvs(); vi.restoreAllMocks(); });
type Reply = { status?: number; body?: unknown; location?: string; truncate?: boolean };
async function gate(script: (attempt: number) => Reply | Promise<Reply>) {
  const requests: Array<{ method: string | undefined; url: string | undefined; headers: IncomingMessage["headers"]; body: unknown }> = [];
  const server = createServer(async (request, response) => {
    const buffers = []; for await (const buffer of request) buffers.push(buffer);
    requests.push({ method: request.method, url: request.url, headers: request.headers, body: JSON.parse(Buffer.concat(buffers).toString()) });
    const reply = await script(requests.length);
    response.writeHead(reply.status ?? 201, { "Content-Type": "application/json", ...(reply.location ? { Location: reply.location } : {}) });
    if (reply.truncate) { response.write('{"id":'); await delay(20); response.destroy(); return; }
    response.end(JSON.stringify(reply.body ?? { id: decisionId, effect: "ALLOW", approvalId: null, arguments: { echo: apiKey } }));
  });
  server.listen(0, "127.0.0.1"); await once(server, "listening");
  cleanup.push(() => new Promise<void>((resolve, reject) => { server.closeAllConnections(); server.close((e) => e ? reject(e) : resolve()); }));
  const address = server.address(); if (!address || typeof address === "string") throw new Error("no address");
  return { gateUrl: `http://127.0.0.1:${address.port}`, requests };
}
function task(overrides: MessageInitShape<typeof PollActivityTaskResponseSchema> = {}) {
  return create(PollActivityTaskResponseSchema, { taskToken: new Uint8Array([1]), runId: "run", workflowType: "toolStep", activityType: "capstan.gate.decide", input: encode(input)!, seq: 1n, attempt: 1, idempotencyKey: "run/1", startToCloseTimeout: create(DurationSchema, { seconds: 10n }), ...overrides });
}
async function setup(options: Partial<WorkerOptions>, tasks = [task()], retry = false, scripts: Parameters<typeof fakeServer>[0] = {}) {
  const logs: Record<string, unknown>[] = [];
  const server = await fakeServer({ worker: {
    pollActivityTask: async (_request, context) => {
      const next = tasks.shift(); if (next) return next;
      if (!context.signal.aborted) await new Promise<void>((resolve) => context.signal.addEventListener("abort", () => resolve(), { once: true }));
      return {};
    },
    failActivityTask: (request) => {
      if (retry && !request.failure?.nonRetryable) tasks.push(task({ taskToken: new Uint8Array([2]), attempt: 2 }));
      return {};
    },
    ...scripts.worker,
  } });
  cleanup.push(() => server.close());
  const worker = await Worker.create({ address: server.address, apiKey: server.apiKey, workflowsPath, taskQueue: "q", identity: "agent-test", maxConcurrentActivities: 1, maxConcurrentWorkflowTasks: 1, logger: (entry) => logs.push(entry), ...options });
  cleanup.push(() => worker.shutdown(0));
  void worker.run();
  return { server, worker, logs };
}
const gateOptions = (gateUrl: string) => ({ gateUrl, gateApiKey: apiKey, gatePolicyId: policyId, gateAgentId: "agent-test" });
function recorded(server: Awaited<ReturnType<typeof fakeServer>>, logs: unknown) {
  return JSON.stringify({ failures: server.requests("FailActivityTask"), completions: server.requests("CompleteActivityTask"), logs }, (_key, value) => typeof value === "bigint" ? String(value) : value);
}

describe("built-in activities in a real worker with local HTTP and Connect fakes", () => {
  it("posts the Gate API body and key, maps id, and drops echoed request data", async () => {
    const g = await gate(() => ({}));
    const s = await setup(gateOptions(g.gateUrl));
    await expect.poll(() => s.server.requests("CompleteActivityTask").length).toBe(1);
    expect(g.requests).toHaveLength(1);
    expect(g.requests[0]).toMatchObject({ method: "POST", url: "/decisions", headers: { "x-api-key": apiKey, "idempotency-key": "run/1" }, body: { ...input, policyId, agentId: "agent-test" } });
    const result = decode(s.server.requests<CompleteActivityTaskRequest>("CompleteActivityTask")[0]!.result) as { arguments: unknown };
    expect(result).toEqual({ effect: "ALLOW", decisionId, approvalId: null, arguments: input.arguments });
    expect(JSON.stringify(result.arguments)).toBe(JSON.stringify((g.requests[0]!.body as { arguments: unknown }).arguments));
    expect(recorded(s.server, s.logs)).not.toContain(apiKey);
  });
  it("a 5xx retry reuses the key and body across distinct activity attempts", async () => {
    const g = await gate((n) => n === 1 ? { status: 503, body: { detail: apiKey } } : {});
    const s = await setup(gateOptions(`${g.gateUrl}/`), [task()], true);
    await expect.poll(() => s.server.requests("CompleteActivityTask").length).toBe(1);
    expect(s.server.requests<FailActivityTaskRequest>("FailActivityTask")[0]!.failure).toMatchObject({ type: "GateUnavailable", nonRetryable: false });
    expect(g.requests).toHaveLength(2);
    expect(g.requests[0]!.headers["idempotency-key"]).toBe("run/1");
    expect(g.requests[1]!.headers["idempotency-key"]).toBe("run/1");
    expect(g.requests[1]!.body).toEqual(g.requests[0]!.body);
    expect(recorded(s.server, s.logs)).not.toContain(apiKey);
  });
  it("retries a disconnected success response using the same Gate idempotency key", async () => {
    const g = await gate((n) => n === 1 ? { truncate: true } : {});
    const s = await setup(gateOptions(g.gateUrl), [task()], true);
    await expect.poll(() => s.server.requests("CompleteActivityTask").length).toBe(1);
    expect(s.server.requests<FailActivityTaskRequest>("FailActivityTask")[0]!.failure).toMatchObject({ type: "GateUnavailable", nonRetryable: false });
    expect(g.requests.map((r) => r.headers["idempotency-key"])).toEqual(["run/1", "run/1"]);
  });
  it("409 is a non-retryable failure and never copies the response body", async () => {
    vi.stubEnv("CAPSTAN_KEEP_STACKS", "1");
    const g = await gate(() => ({ status: 409, body: { detail: apiKey } }));
    const s = await setup(gateOptions(g.gateUrl), [task()], true);
    await expect.poll(() => s.server.requests("FailActivityTask").length).toBe(1);
    expect(s.server.requests<FailActivityTaskRequest>("FailActivityTask")[0]!.failure).toMatchObject({ type: "GateIdempotencyConflict", nonRetryable: true });
    expect(g.requests).toHaveLength(1);
    expect(s.server.requests("CompleteActivityTask")).toHaveLength(0);
    expect(recorded(s.server, s.logs)).not.toContain(apiKey);
  });
  it("returns Gate approval identity", async () => {
    const g = await gate(() => ({ body: { id: decisionId, effect: "REQUIRE_APPROVAL", approvalId } }));
    const s = await setup(gateOptions(g.gateUrl));
    await expect.poll(() => s.server.requests("CompleteActivityTask").length).toBe(1);
    expect(decode(s.server.requests<CompleteActivityTaskRequest>("CompleteActivityTask")[0]!.result)).toEqual({ effect: "REQUIRE_APPROVAL", decisionId, approvalId, arguments: input.arguments });
  });
  it.each([
    { id: decisionId, effect: "REQUIRE_APPROVAL", approvalId: null },
    { id: apiKey, effect: "ALLOW" },
    { id: decisionId, effect: "UNKNOWN" },
  ])("rejects malformed Gate decisions without leaking response fields: %j", async (body) => {
    const g = await gate(() => ({ body })); const s = await setup(gateOptions(g.gateUrl));
    await expect.poll(() => s.server.requests("FailActivityTask").length).toBe(1);
    expect(s.server.requests<FailActivityTaskRequest>("FailActivityTask")[0]!.failure).toMatchObject({ type: "GateResponseInvalid", nonRetryable: true });
    expect(recorded(s.server, s.logs)).not.toContain(apiKey);
  });
  it("does not forward a Gate key through a redirect", async () => {
    const destination = await gate(() => ({}));
    const g = await gate(() => ({ status: 307, location: `${destination.gateUrl}/decisions` }));
    const s = await setup(gateOptions(g.gateUrl));
    await expect.poll(() => s.server.requests("FailActivityTask").length).toBe(1);
    expect(destination.requests).toHaveLength(0);
    expect(recorded(s.server, s.logs)).not.toContain(apiKey);
  });
  it("fails closed if Gate options are absent", async () => {
    const s = await setup({});
    await expect.poll(() => s.server.requests("FailActivityTask").length).toBe(1);
    expect(s.server.requests<FailActivityTaskRequest>("FailActivityTask")[0]!.failure).toMatchObject({ type: "GateConfigurationInvalid", nonRetryable: true });
  });
  it("registers capstan.model and fails closed at the real RPC boundary", async () => {
    const s = await setup({}, [task({ activityType: "capstan.model", input: encode({ prompt: "Hello" }) })], false, { worker: { reserveAICall: () => { throw new ConnectError("cap exceeded", Code.ResourceExhausted); } } });
    await expect.poll(() => s.server.requests("FailActivityTask").length).toBe(1);
    expect(s.server.requests("ReserveAICall")).toHaveLength(1);
    expect(s.server.requests<FailActivityTaskRequest>("FailActivityTask")[0]!.failure).toMatchObject({ type: "ModelBudgetExceeded", nonRetryable: true });
    expect(s.server.requests("FinishAICall")).toHaveLength(0);
  });
  it("never records a Gate key even when it is a UUID echoed as a decision id", async () => {
    const g = await gate(() => ({ body: { id: decisionId, effect: "ALLOW", approvalId: null } }));
    const s = await setup({ ...gateOptions(g.gateUrl), gateApiKey: decisionId });
    await expect.poll(() => s.server.requests("FailActivityTask").length).toBe(1);
    expect(s.server.requests<FailActivityTaskRequest>("FailActivityTask")[0]!.failure).toMatchObject({ type: "GateResponseInvalid", nonRetryable: true });
    expect(recorded(s.server, s.logs)).not.toContain(decisionId);
  });

  it("the real Anthropic SDK uses only the environment key and never logs bodies in debug mode", async () => {
    const prompt = "provider-secret-prompt";
    const response = "provider-secret-response";
    const key = "provider-secret-key";
    const output = [vi.spyOn(console, "debug").mockImplementation(() => {}), vi.spyOn(console, "info").mockImplementation(() => {}), vi.spyOn(console, "warn").mockImplementation(() => {}), vi.spyOn(console, "error").mockImplementation(() => {})];
    const g = await gate(() => ({ body: { id: "msg_test", type: "message", role: "assistant", model: "claude-sonnet-5", content: [{ type: "text", text: response }], stop_reason: "end_turn", stop_sequence: null, usage: { input_tokens: 10, output_tokens: 5 } } }));
    vi.stubEnv("ANTHROPIC_API_KEY", key);
    vi.stubEnv("ANTHROPIC_BASE_URL", g.gateUrl);
    vi.stubEnv("ANTHROPIC_LOG", "debug");
    vi.stubEnv("ANTHROPIC_AUTH_TOKEN", "unused-token");
    const s = await setup({}, [task({ activityType: "capstan.model", input: encode({ prompt, maxTokens: 32, apiKey: "payload-key-must-not-be-used" }) })], false, { worker: {
      reserveAICall: () => ({ reservationId: 12n }), finishAICall: () => ({ costUsd: 0.00007 }),
    } });
    await expect.poll(() => s.server.requests("CompleteActivityTask").length).toBe(1);
    expect(g.requests[0]).toMatchObject({ url: "/v1/messages", headers: { "x-api-key": key }, body: { messages: [{ role: "user", content: prompt }], max_tokens: 32, model: "claude-sonnet-5" } });
    expect(g.requests[0]!.headers.authorization).toBeUndefined();
    expect(g.requests[0]!.body).not.toHaveProperty("apiKey");
    expect(s.server.requests<ReserveAICallRequest>("ReserveAICall")[0]).toMatchObject({ maxOutputTokens: 32n, inputTokensUpperBound: 150n });
    expect(s.server.requests<FinishAICallRequest>("FinishAICall")[0]).toMatchObject({ reservationId: 12n, ok: true, inputTokens: 10n, outputTokens: 5n });
    expect(JSON.stringify([...output.flatMap((spy) => spy.mock.calls), ...s.logs])).not.toMatch(/provider-secret/);
  });
  it("worker shutdown waits for model ledger cleanup with its own RPC deadline", async () => {
    const g = await gate(async () => { await delay(200); return {}; });
    vi.stubEnv("ANTHROPIC_API_KEY", "local-test-key");
    vi.stubEnv("ANTHROPIC_BASE_URL", g.gateUrl);
    let finished = false;
    const s = await setup({}, [task({ activityType: "capstan.model", input: encode({ prompt: "hello" }) })], false, { worker: {
      reserveAICall: () => ({ reservationId: 13n }),
      finishAICall: async () => { await delay(60); finished = true; return { costUsd: 0 }; },
    } });
    await expect.poll(() => g.requests.length).toBe(1);
    await s.worker.shutdown(0);
    expect(finished).toBe(true);
    expect(s.server.requests<FinishAICallRequest>("FinishAICall")[0]).toMatchObject({ reservationId: 13n, ok: false, errorCode: "ModelCancelled" });
    expect(s.server.calls.find((call) => call.method === "FinishAICall")?.timeoutMs).toBeLessThanOrEqual(5_000);
  });
  it("a local model timeout finishes before the worker reports the activity failure", async () => {
    const g = await gate(async () => { await delay(200); return {}; });
    vi.stubEnv("ANTHROPIC_API_KEY", "local-test-key");
    vi.stubEnv("ANTHROPIC_BASE_URL", g.gateUrl);
    const s = await setup({}, [task({ activityType: "capstan.model", input: encode({ prompt: "hello" }), startToCloseTimeout: create(DurationSchema, { nanos: 100_000_000 }) })], false, { worker: {
      reserveAICall: () => ({ reservationId: 14n }), finishAICall: () => ({ costUsd: 0 }),
    } });
    await expect.poll(() => s.server.requests("FailActivityTask").length).toBe(1);
    expect(s.server.requests<FinishAICallRequest>("FinishAICall")[0]).toMatchObject({ reservationId: 14n, ok: false, errorCode: "ModelTimeoutUsageUnknown", usageUnknown: true });
    expect(s.server.requests<FailActivityTaskRequest>("FailActivityTask")[0]!.failure).toMatchObject({ type: "ModelTimeout" });
    const methods = s.server.calls.map((call) => call.method);
    expect(methods.indexOf("FinishAICall")).toBeLessThan(methods.indexOf("FailActivityTask"));
  });
});
