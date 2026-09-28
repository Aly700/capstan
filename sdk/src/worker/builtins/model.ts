import { setTimeout as delay } from "node:timers/promises";
import Anthropic from "@anthropic-ai/sdk";
import { Code, ConnectError, type Client } from "@connectrpc/connect";
import type { Message, MessageCreateParamsNonStreaming } from "@anthropic-ai/sdk/resources/messages";
import type { WorkerService } from "../../gen/capstan/v1/capstan_pb.ts";
import type { ModelRequest, ModelResult } from "../../workflow/agent.ts";
import { ApplicationFailure } from "../../types.ts";
import { estimateCost } from "../../agent/estimate.ts";
import { prepareSchema } from "../../agent/schema.ts";
import { activityContext } from "../index.ts";

export interface ModelClient {
  messages: { create(request: MessageCreateParamsNonStreaming, options?: Anthropic.RequestOptions): PromiseLike<Message> };
}

function failure(type: string, nonRetryable = false): ApplicationFailure {
  // Never attach a provider/RPC error as cause, details or message: those may
  // contain request/response bodies or credentials, even with stack logging on.
  return new ApplicationFailure(type, { type, nonRetryable });
}
function providerClient(): ModelClient {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) throw failure("ModelConfigurationInvalid", true);
  return new Anthropic({ apiKey, authToken: null, maxRetries: 0, logLevel: "off", logger: { debug() {}, info() {}, warn() {}, error() {} } });
}
function usageIsKnown(value: unknown): boolean {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const usage = value as Record<string, unknown>;
  const validCount = (count: unknown) => typeof count === "number" && Number.isSafeInteger(count) && count >= 0;
  return validCount(usage.input_tokens) && validCount(usage.output_tokens) &&
    [usage.cache_read_input_tokens, usage.cache_creation_input_tokens].every((count) => count == null || validCount(count));
}
function usageOf(value: unknown) {
  const usage = value && typeof value === "object" ? value as Record<string, unknown> : {};
  const count = (key: string) => typeof usage[key] === "number" && Number.isSafeInteger(usage[key]) && usage[key] >= 0 ? usage[key] : 0;
  return { inputTokens: count("input_tokens"), outputTokens: count("output_tokens"), cacheReadTokens: count("cache_read_input_tokens"), cacheWriteTokens: count("cache_creation_input_tokens") };
}

export function createModelActivity(options: {
  client: Pick<Client<typeof WorkerService>, "reserveAICall" | "finishAICall">;
  taskToken: Uint8Array;
  model?: string;
  anthropic?: ModelClient;
}): (request: ModelRequest) => Promise<ModelResult> {
  return async (request) => {
    const context = activityContext();
    const model = request?.model ?? options.model ?? "claude-sonnet-5";
    const maxTokens = request?.maxTokens ?? 1024;
    const messages = request?.messages ?? (typeof request?.prompt === "string" ? [{ role: "user" as const, content: request.prompt }] : []);
    if (typeof model !== "string" || !model || !Number.isSafeInteger(maxTokens) || maxTokens < 1 || !Array.isArray(messages) || !messages.length ||
        messages.some((m) => !m || !["user", "assistant"].includes(m.role) || typeof m.content !== "string") ||
        (request.system !== undefined && typeof request.system !== "string")) throw failure("ModelRequestInvalid", true);
    let schema: ReturnType<typeof prepareSchema> | undefined;
    if (request.jsonSchema !== undefined) {
      try { schema = prepareSchema(request.jsonSchema); }
      catch { throw failure("ModelSchemaInvalid", true); }
    }
    const params: MessageCreateParamsNonStreaming = {
      model, messages, max_tokens: maxTokens,
      ...(request.system === undefined ? {} : { system: request.system }),
      ...(schema === undefined ? {} : { output_config: { format: schema.format } }),
    };
    const estimateUsd = estimateCost(params, request.estimateUsd);
    let reservationId: bigint;
    try {
      // Do not retry a reservation with an ambiguous acknowledgement: the frozen
      // RPC does not accept an idempotency key for reservations.
      const reservation = await options.client.reserveAICall({ taskToken: options.taskToken, model, estimateUsd }, { signal: context.signal, timeoutMs: 10_000 });
      reservationId = reservation.reservationId;
      if (reservationId <= 0n) throw failure("ModelReservationInvalid", true);
    } catch (error) {
      if (error instanceof ApplicationFailure) throw error;
      if (error instanceof ConnectError && error.code === Code.ResourceExhausted) throw failure("ModelBudgetExceeded", true);
      throw failure("ModelReservationFailed");
    }
    let usage = usageOf(undefined);
    let providerRequestSent = false;
    let usageReceived = false;
    let value: Omit<ModelResult, "costUsd"> | undefined;
    let failed: ApplicationFailure | undefined;
    let errorCode = "";
    try {
      context.signal.throwIfAborted();
      // create + output_config retains usage even if parsing/validation fails.
      // messages.parse throws before returning that usage on malformed JSON.
      const provider = options.anthropic ?? providerClient();
      providerRequestSent = true;
      const message = await provider.messages.create(params, { signal: context.signal, maxRetries: 0 });
      usageReceived = usageIsKnown(message.usage);
      usage = usageOf(message.usage);
      if (!usageReceived) throw failure("ModelUsageInvalid", true);
      const text = message.content.filter((block) => block.type === "text").map((block) => block.text).join("");
      let json: unknown;
      if (schema) {
        try {
          json = JSON.parse(text);
          if (!schema.valid(json)) throw new Error();
        } catch { throw failure("ModelOutputInvalid", true); }
      }
      value = { text, ...(schema ? { json } : {}), model: message.model, ...usage, stopReason: message.stop_reason ?? "" };
    } catch (error) {
      const fields = error && typeof error === "object" ? error as { usage?: unknown; name?: unknown; type?: unknown; status?: unknown } : {};
      if (fields.usage != null) { usage = usageOf(fields.usage); usageReceived = usageIsKnown(fields.usage); }
      if (error instanceof ApplicationFailure) failed = error;
      else if (error instanceof Anthropic.APIConnectionTimeoutError || fields.name === "APIConnectionTimeoutError" || (context.signal.aborted && context.signal.reason?.type === "TimeoutFailure")) failed = failure("ModelTimeout");
      else if (context.signal.aborted) failed = failure("ModelCancelled", true);
      else failed = failure("ModelProviderError", typeof fields.status === "number" && fields.status >= 400 && fields.status < 500 && ![408, 409, 429].includes(fields.status));
      errorCode = failed.type === "ModelTimeout" && providerRequestSent && !usageReceived ? "ModelTimeoutUsageUnknown" : failed.type;
    }
    const finish = { reservationId, ok: failed === undefined, inputTokens: BigInt(usage.inputTokens), outputTokens: BigInt(usage.outputTokens), cacheReadTokens: BigInt(usage.cacheReadTokens), cacheWriteTokens: BigInt(usage.cacheWriteTokens), errorCode, usageUnknown: providerRequestSent && !usageReceived };
    let costUsd: number | undefined;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        // A bounded, independent RPC must still run after timeout or shutdown.
        costUsd = (await options.client.finishAICall(finish, { timeoutMs: 5_000 })).costUsd;
        break;
      } catch (error) {
        if (!(error instanceof ConnectError) || ![Code.Unavailable, Code.DeadlineExceeded, Code.Unknown, Code.Internal].includes(error.code) || attempt === 2) throw failure("ModelAccountingFailed", true);
        await delay(100 * 2 ** attempt);
      }
    }
    if (failed) throw failed;
    return { ...value!, costUsd: costUsd! };
  };
}
