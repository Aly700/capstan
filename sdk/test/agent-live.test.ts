import { create } from "@bufbuild/protobuf";
import { expect, it } from "vitest";
import { FinishAICallResponseSchema, ReserveAICallResponseSchema, type FinishAICallRequest } from "../src/gen/capstan/v1/capstan_pb.ts";
import { activityStorage } from "../src/worker/activities.ts";
import { createModelActivity } from "../src/worker/builtins/model.ts";

// Explicit opt-in only. Phase A uses a recording ledger double, so the lead can
// smoke-test the installed provider SDK without requiring the Capstan server.
it.skipIf(process.env.CAPSTAN_LIVE !== "1")("live Anthropic smoke reserves under $0.05 and finishes with usage", async () => {
  expect(Boolean(process.env.ANTHROPIC_API_KEY)).toBe(true);
  let reserved = false;
  const finishes: unknown[] = [];
  const call = createModelActivity({
    taskToken: new Uint8Array([1]),
    client: {
      async reserveAICall(request) {
        expect(request.estimateUsd).toBeGreaterThan(0);
        expect(request.estimateUsd).toBeLessThan(0.05);
        expect(request.model).toBe("claude-sonnet-5");
        reserved = true;
        return create(ReserveAICallResponseSchema, { reservationId: 1n, capUsd: 0.05 });
      },
      async finishAICall(request) {
        expect(reserved).toBe(true);
        finishes.push(request);
        const costUsd = (Number(request.inputTokens ?? 0n) * 2 + Number(request.outputTokens ?? 0n) * 10 + Number(request.cacheReadTokens ?? 0n) * 0.2 + Number(request.cacheWriteTokens ?? 0n) * 2.5) / 1e6;
        return create(FinishAICallResponseSchema, { costUsd });
      },
    },
  });
  const result = await activityStorage.run({
    runId: "live-smoke", workflowType: "smoke", activityType: "capstan.model", seq: 1, attempt: 1,
    idempotencyKey: "live-smoke/1", heartbeatDetails: undefined, signal: AbortSignal.timeout(30_000), heartbeat() {},
  }, () => call({ prompt: "Reply with the single word OK.", maxTokens: 32 }));
  // Assertions expose no prompt or response text, even on failure.
  expect(result.inputTokens).toBeGreaterThan(0);
  expect(result.outputTokens).toBeGreaterThan(0);
  expect(result.costUsd).toBeLessThan(0.05);
  expect(finishes).toHaveLength(1);
  expect((finishes[0] as FinishAICallRequest).ok).toBe(true);
}, 45_000);
