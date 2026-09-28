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

type TextModelRequest = Pick<MessageCreateParamsNonStreaming, "model" | "max_tokens" | "output_config"> & {
  system?: string;
  messages: { content: string }[];
};

export function inputTokensUpperBound(request: TextModelRequest): number {
  // The text-only workflow API sends no images, tools, or cache controls. Count
  // one token per UTF-8 content byte, including the prepared schema's JSON.
  // The installed provider types describe hidden request formatting but give no
  // fixed token count; allow a generous 64 base tokens plus 64 per message for it.
  let bound = 64 + Buffer.byteLength(request.system ?? "", "utf8");
  for (const message of request.messages) bound += 64 + Buffer.byteLength(message.content, "utf8");
  const schema = request.output_config?.format?.schema;
  if (schema !== undefined) bound += Buffer.byteLength(JSON.stringify(schema), "utf8");
  return bound;
}

export function estimateCost(request: TextModelRequest, explicit?: number): number {
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
      prices = { ...defaults, ...value as typeof defaults };
    } catch { throw new ApplicationFailure("Worker model prices are invalid", { type: "ModelConfigurationInvalid", nonRetryable: true }); }
  }
  const family = Object.keys(prices).sort((a, b) => b.length - a.length).find((name) => request.model === name || request.model.startsWith(`${name}-`));
  if (!family) throw new ApplicationFailure("An explicit estimate is required for this model", { type: "ModelEstimateRequired", nonRetryable: true });
  const price = prices[family]!;
  // Reserve ALL max_tokens at the output rate. No cache discount; this request
  // never enables caching. Use the same input bound that accompanies Reserve.
  const microdollars = inputTokensUpperBound(request) * price.input + request.max_tokens * price.output;
  if (!Number.isFinite(microdollars)) throw new ApplicationFailure("Model estimate is out of range", { type: "ModelRequestInvalid", nonRetryable: true });
  return Math.ceil(microdollars) / 1e6;
}
