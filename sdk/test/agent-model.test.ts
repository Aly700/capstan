import { create } from "@bufbuild/protobuf";
import { Code, ConnectError } from "@connectrpc/connect";
import type { Message, MessageCreateParamsNonStreaming } from "@anthropic-ai/sdk/resources/messages";
import Anthropic from "@anthropic-ai/sdk";
import { afterEach, describe, expect, it, vi } from "vitest";
import { z } from "zod";
import { FinishAICallResponseSchema, ReserveAICallResponseSchema, type FinishAICallRequest, type ReserveAICallRequest } from "../src/gen/capstan/v1/capstan_pb.ts";
import { createModelActivity, type ModelClient } from "../src/worker/builtins/model.ts";
import { activityStorage } from "../src/worker/activities.ts";
import { failureToProto } from "../src/internal/failure.ts";
import type { ModelRequest } from "../src/workflow/agent.ts";
import { TimeoutFailure } from "../src/types.ts";

afterEach(() => { vi.unstubAllEnvs(); vi.restoreAllMocks(); });
function response(text = "hello"): Message {
  return {
    id: "msg_test", type: "message", role: "assistant", model: "claude-sonnet-5", content: [{ type: "text", text, citations: null }], stop_reason: "end_turn", stop_sequence: null, container: null, stop_details: null,
    usage: { input_tokens: 11, output_tokens: 7, cache_read_input_tokens: 3, cache_creation_input_tokens: 5, cache_creation: null, inference_geo: null, server_tool_use: null, service_tier: "standard", output_tokens_details: null },
  };
}
function setup(provider: ModelClient["messages"]["create"] = async () => response()) {
  const order: string[] = [];
  const reserve = vi.fn(async (_request: Parameters<Parameters<typeof createModelActivity>[0]["client"]["reserveAICall"]>[0]) => {
    order.push("reserve"); return create(ReserveAICallResponseSchema, { reservationId: 17n, capUsd: 2 });
  });
  const finish = vi.fn(async (_request: Parameters<Parameters<typeof createModelActivity>[0]["client"]["finishAICall"]>[0], _options?: unknown) => {
    order.push("finish"); return create(FinishAICallResponseSchema, { costUsd: 0.0002 });
  });
  const call = vi.fn(async (params: MessageCreateParamsNonStreaming, options?: Anthropic.RequestOptions) => { order.push("provider"); return provider(params, options); });
  const controller = new AbortController();
  const invoke = (request: ModelRequest = { prompt: "Hello" }, model?: string) => activityStorage.run({
    runId: "run", workflowType: "agent", activityType: "capstan.model", seq: 2, attempt: 1, idempotencyKey: "run/2", heartbeatDetails: undefined, signal: controller.signal, heartbeat: () => {},
  }, () => createModelActivity({ client: { reserveAICall: reserve, finishAICall: finish }, taskToken: new Uint8Array([1, 2]), anthropic: { messages: { create: call } }, ...(model === undefined ? {} : { model }) })(request));
  return { invoke, reserve, finish, call, controller, order };
}

describe("capstan.model accounting and provider boundary", () => {
  it("reserves before one provider call and returns server-priced usage", async () => {
    const s = setup();
    expect(await s.invoke({ prompt: "Hello", system: "Be brief", maxTokens: 64 })).toEqual({ text: "hello", model: "claude-sonnet-5", inputTokens: 11, outputTokens: 7, cacheReadTokens: 3, cacheWriteTokens: 5, costUsd: 0.0002, stopReason: "end_turn" });
    expect(s.order).toEqual(["reserve", "provider", "finish"]);
    expect(s.reserve.mock.calls[0]![0]).toMatchObject({ taskToken: new Uint8Array([1, 2]), model: "claude-sonnet-5" });
    expect(s.call.mock.calls[0]).toEqual([{ model: "claude-sonnet-5", system: "Be brief", messages: [{ role: "user", content: "Hello" }], max_tokens: 64 }, { signal: s.controller.signal, maxRetries: 0 }]);
    expect(s.finish.mock.calls[0]![0]).toEqual({ reservationId: 17n, ok: true, inputTokens: 11n, outputTokens: 7n, cacheReadTokens: 3n, cacheWriteTokens: 5n, errorCode: "" });
  });
  it("uses real model ids and counts UTF-8 input conservatively as well as all output", async () => {
    const s = setup();
    const messages = [{ role: "user" as const, content: "日本語🙂".repeat(100) }];
    const system = "Instructions";
    await s.invoke({ model: "claude-opus-5-5", messages, system, maxTokens: 200 });
    const reservation = s.reserve.mock.calls[0]![0];
    const inputBytes = Buffer.byteLength(JSON.stringify({ messages, system }), "utf8");
    // One token per UTF-8 byte, plus framing/schema safety allowance, rounded upward.
    const inputBound = inputBytes + 1024 + 64 * messages.length;
    expect(reservation).toMatchObject({ model: "claude-opus-5-5", estimateUsd: Math.ceil(inputBound * 5 + 200 * 25) / 1e6 });
    expect(s.call.mock.calls[0]![0].model).toBe("claude-opus-5-5");
  });
  it("uses configured default model and honours an explicit upper-bound estimate", async () => {
    const s = setup();
    await s.invoke({ prompt: "Hi", estimateUsd: 0.0123451 }, "claude-haiku-4-5-20251001");
    expect(s.reserve.mock.calls[0]![0]).toMatchObject({ model: "claude-haiku-4-5-20251001", estimateUsd: 0.012346 });
  });
  it("reads worker price overrides for the estimate and chooses the longest family prefix", async () => {
    vi.stubEnv("CAPSTAN_MODEL_PRICES", JSON.stringify({ "claude-opus-5": { input: 7, output: 31 }, "claude-opus-5-5": { input: 11, output: 53 } }));
    const s = setup();
    await s.invoke({ model: "claude-opus-5-5", prompt: "Hi", maxTokens: 20 });
    const bytes = Buffer.byteLength(JSON.stringify({ messages: [{ role: "user", content: "Hi" }] }));
    expect(s.reserve.mock.calls[0]![0].estimateUsd).toBe(Math.ceil((bytes + 1088) * 11 + 20 * 53) / 1e6);
  });
  it("fails closed for an unknown model without an explicit estimate", async () => {
    const s = setup();
    await expect(s.invoke({ model: "unknown", prompt: "Hi" })).rejects.toMatchObject({ type: "ModelEstimateRequired", nonRetryable: true });
    expect(s.reserve).not.toHaveBeenCalled();
    expect(s.call).not.toHaveBeenCalled();
  });
  it("accepts an explicit estimate for an unknown model", async () => {
    const s = setup();
    await s.invoke({ model: "custom-model", prompt: "Hi", estimateUsd: 0.1 });
    expect(s.reserve.mock.calls[0]![0]).toMatchObject({ model: "custom-model", estimateUsd: 0.1 });
  });
  it("uses the installed SDK structured-output field and validates nested JSON", async () => {
    const value = { result: { count: 2 }, tags: ["ok"] };
    const schema = z.toJSONSchema(z.strictObject({ result: z.strictObject({ count: z.number().int().positive() }), tags: z.array(z.string()) }));
    const s = setup(async () => response(JSON.stringify(value)));
    expect(await s.invoke({ prompt: "JSON", jsonSchema: schema })).toMatchObject({ json: value, text: JSON.stringify(value) });
    const format = s.call.mock.calls[0]![0].output_config?.format;
    expect(format?.type).toBe("json_schema");
    expect(format?.schema).toMatchObject({ type: "object", properties: { result: { properties: { count: { type: "integer" } } } } });
    expect(format?.schema).not.toHaveProperty("properties.result.properties.count.exclusiveMinimum");
    expect(format?.schema).toHaveProperty("properties.result.properties.count.description");
    const inputBytes = Buffer.byteLength(JSON.stringify({ messages: [{ role: "user", content: "JSON" }], output_config: { format } }));
    expect(s.reserve.mock.calls[0]![0].estimateUsd).toBe(Math.ceil((inputBytes + 1088) * 2 + 1024 * 10) / 1e6);
  });
  it.each(["not json: secret-output", '{"count":"secret-output"}', '{"count":2,"extra":true}'])("invalid output still records billed tokens: %s", async (text) => {
    const s = setup(async () => response(text));
    const error = await s.invoke({ prompt: "secret-prompt", jsonSchema: z.toJSONSchema(z.strictObject({ count: z.number() })) }).catch((e: unknown) => e);
    expect(error).toMatchObject({ type: "ModelOutputInvalid", nonRetryable: true });
    expect(s.finish.mock.calls[0]![0]).toMatchObject({ ok: false, inputTokens: 11n, outputTokens: 7n, errorCode: "ModelOutputInvalid" });
    expect(JSON.stringify(failureToProto(error))).not.toMatch(/secret-output|secret-prompt/);
  });
  it("enforces original numeric bounds after simplifying the provider schema", async () => {
    const s = setup(async () => response('{"count":0}'));
    await expect(s.invoke({ prompt: "JSON", jsonSchema: z.toJSONSchema(z.strictObject({ count: z.number().int().positive() })) })).rejects.toMatchObject({ type: "ModelOutputInvalid", nonRetryable: true });
    expect(s.finish.mock.calls[0]![0]).toMatchObject({ ok: false, inputTokens: 11n });
  });
  it("does not let JSON Schema defaults fabricate required output", async () => {
    const s = setup(async () => response('{}'));
    await expect(s.invoke({ prompt: "JSON", jsonSchema: z.toJSONSchema(z.strictObject({ count: z.number().default(7) })) })).rejects.toMatchObject({ type: "ModelOutputInvalid", nonRetryable: true });
    expect(s.finish.mock.calls[0]![0]).toMatchObject({ ok: false, errorCode: "ModelOutputInvalid" });
  });
  it("defaults are annotations, so absent optional fields remain absent", async () => {
    const s = setup(async () => response('{}'));
    expect(await s.invoke({ prompt: "JSON", jsonSchema: { type: "object", properties: { count: { type: "integer", default: 7 } }, additionalProperties: false } })).toMatchObject({ json: {} });
  });
  it("retains properties named default and validates defaults behind local refs", async () => {
    const s = setup(async () => response('{"default":{}}'));
    const schema = { type: "object", properties: { default: { $ref: "#/$defs/item" } }, required: ["default"], additionalProperties: false,
      $defs: { item: { type: "object", properties: { count: { type: "integer", default: 7 } }, required: ["count"], additionalProperties: false } } };
    await expect(s.invoke({ prompt: "JSON", jsonSchema: schema })).rejects.toMatchObject({ type: "ModelOutputInvalid", nonRetryable: true });
  });
  it("rejects unsupported draft-7 definitions before reserving instead of sending dangling refs", async () => {
    const shared = z.strictObject({ value: z.number() });
    const schema = z.toJSONSchema(z.strictObject({ left: shared, right: shared }), { target: "draft-7", reused: "ref" });
    const s = setup(async () => response('{"left":{"value":1},"right":{"value":2}}'));
    await expect(s.invoke({ prompt: "JSON", jsonSchema: schema })).rejects.toMatchObject({ type: "ModelSchemaInvalid", nonRetryable: true });
    expect(s.reserve).not.toHaveBeenCalled();
    expect(s.call).not.toHaveBeenCalled();
  });
  it("cap exceeded fails closed without calling or finishing a model", async () => {
    const s = setup();
    s.reserve.mockRejectedValueOnce(new ConnectError("private ledger text", Code.ResourceExhausted));
    await expect(s.invoke()).rejects.toMatchObject({ type: "ModelBudgetExceeded", nonRetryable: true });
    expect(s.call).not.toHaveBeenCalled();
    expect(s.finish).not.toHaveBeenCalled();
  });
  it("provider errors always finish and contain only fixed public error text", async () => {
    vi.stubEnv("CAPSTAN_KEEP_STACKS", "1");
    const s = setup(async () => { throw Object.assign(new Error("secret-key secret-prompt secret-response"), { status: 500 }); });
    const error = await s.invoke().catch((e: unknown) => e);
    expect(error).toMatchObject({ type: "ModelProviderError", nonRetryable: false });
    expect(s.finish.mock.calls[0]![0]).toEqual({ reservationId: 17n, ok: false, inputTokens: 0n, outputTokens: 0n, cacheReadTokens: 0n, cacheWriteTokens: 0n, errorCode: "ModelProviderError" });
    expect(JSON.stringify(failureToProto(error))).not.toContain("secret-");
  });
  it("a billed timeout records any reported usage even after the activity signal aborts", async () => {
    const s = setup(async (_params, options) => {
      s.controller.abort(new TimeoutFailure("expired", "START_TO_CLOSE"));
      expect(options?.signal?.aborted).toBe(true);
      throw Object.assign(new Error("private"), { name: "APIConnectionTimeoutError", usage: { input_tokens: 20, output_tokens: 9 } });
    });
    await expect(s.invoke()).rejects.toMatchObject({ type: "ModelTimeout" });
    expect(s.finish.mock.calls[0]![0]).toMatchObject({ ok: false, inputTokens: 20n, outputTokens: 9n, errorCode: "ModelTimeout" });
    expect((s.finish.mock.calls[0]![1] as { signal?: AbortSignal } | undefined)?.signal?.aborted).not.toBe(true);
  });
  it("a timeout with no usage still finishes with a distinguishable fixed code", async () => {
    const s = setup(async () => { throw Object.assign(new Error("private"), { name: "APIConnectionTimeoutError" }); });
    await expect(s.invoke()).rejects.toMatchObject({ type: "ModelTimeout" });
    expect(s.finish.mock.calls[0]![0]).toMatchObject({ ok: false, inputTokens: 0n, outputTokens: 0n, errorCode: "ModelTimeoutUsageUnknown" });
  });
  it("recognizes the installed SDK's real timeout error class", async () => {
    const s = setup(async () => { throw new Anthropic.APIConnectionTimeoutError(); });
    await expect(s.invoke()).rejects.toMatchObject({ type: "ModelTimeout" });
    expect(s.finish.mock.calls[0]![0]).toMatchObject({ errorCode: "ModelTimeoutUsageUnknown", ok: false });
  });
  it("retries only the idempotent finish RPC if its acknowledgement is lost", async () => {
    const s = setup();
    s.finish.mockRejectedValueOnce(new ConnectError("private", Code.Unavailable));
    expect(await s.invoke()).toMatchObject({ costUsd: 0.0002 });
    expect(s.call).toHaveBeenCalledTimes(1);
    expect(s.reserve).toHaveBeenCalledTimes(1);
    expect(s.finish).toHaveBeenCalledTimes(2);
    expect(s.finish.mock.calls[1]![0]).toEqual(s.finish.mock.calls[0]![0]);
  });
  it("unavailable accounting does not trigger another billed provider attempt", async () => {
    const s = setup();
    s.finish.mockRejectedValue(new ConnectError("private", Code.Unavailable));
    await expect(s.invoke()).rejects.toMatchObject({ type: "ModelAccountingFailed", nonRetryable: true });
    expect(s.call).toHaveBeenCalledTimes(1);
    expect(s.finish).toHaveBeenCalledTimes(3);
  });
  it.each([{ maxTokens: 0 }, { maxTokens: 1.5 }, { estimateUsd: -1 }, { estimateUsd: NaN }, { estimateUsd: Infinity }])("rejects invalid bounds before reserving: %j", async (options) => {
    const s = setup();
    await expect(s.invoke({ prompt: "Hi", ...options })).rejects.toMatchObject({ type: "ModelRequestInvalid", nonRetryable: true });
    expect(s.reserve).not.toHaveBeenCalled();
    expect(s.call).not.toHaveBeenCalled();
  });
});
