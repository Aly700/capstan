import type { MessageCreateParamsNonStreaming } from "@anthropic-ai/sdk/resources/messages";
import { ApplicationFailure } from "../types.ts";

// Same USD / million token rates as internal/engine/api.go. With a server override,
// set CAPSTAN_MODEL_PRICES in the worker too, or pass an explicit upper bound.
const defaults: Record<string, { input: number; output: number }> = {
  "claude-opus-5": { input: 5, output: 25 },
  "claude-sonnet-5": { input: 2, output: 10 },
  "claude-haiku-4-5": { input: 1, output: 5 },
  "claude-fable-5-1": { input: 10, output: 50 },
};

export function estimateCost(request: MessageCreateParamsNonStreaming, explicit?: number): number {
  if (explicit !== undefined) {
    if (!Number.isFinite(explicit) || explicit < 0) throw new ApplicationFailure("Model estimate must be finite and non-negative", { type: "ModelRequestInvalid", nonRetryable: true });
    return Math.ceil(explicit * 1e6) / 1e6;
  }
  let prices = defaults;
  if (process.env.CAPSTAN_MODEL_PRICES !== undefined) {
    try {
      const value: unknown = JSON.parse(process.env.CAPSTAN_MODEL_PRICES);
      if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error();
      for (const [key, rate] of Object.entries(value)) {
        if (!key || !rate || typeof rate !== "object" || !Number.isFinite(rate.input) || rate.input < 0 || !Number.isFinite(rate.output) || rate.output < 0) throw new Error();
      }
      prices = value as typeof defaults;
    } catch { throw new ApplicationFailure("Worker model prices are invalid", { type: "ModelConfigurationInvalid", nonRetryable: true }); }
  }
  const family = Object.keys(prices).sort((a, b) => b.length - a.length).find((name) => request.model === name || request.model.startsWith(`${name}-`));
  if (!family) throw new ApplicationFailure("An explicit estimate is required for this model", { type: "ModelEstimateRequired", nonRetryable: true });
  const price = prices[family]!;
  const input = {
    messages: request.messages,
    ...(request.system === undefined ? {} : { system: request.system }),
    ...(request.output_config === undefined ? {} : { output_config: request.output_config }),
  };
  // For this text-only API, count one token per serialized UTF-8 byte (including
  // schema), plus 1,024 framing tokens and 64 per message. Reserve ALL max_tokens
  // at the output rate. No cache discount; this request never enables caching.
  const inputBound = Buffer.byteLength(JSON.stringify(input), "utf8") + 1024 + 64 * request.messages.length;
  const microdollars = inputBound * price.input + request.max_tokens * price.output;
  if (!Number.isFinite(microdollars)) throw new ApplicationFailure("Model estimate is out of range", { type: "ModelRequestInvalid", nonRetryable: true });
  return Math.ceil(microdollars) / 1e6;
}
