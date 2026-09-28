import { ApplicationFailure } from "../../types.ts";
import { activityContext, type WorkerOptions } from "../index.ts";

type GateOptions = Pick<WorkerOptions, "gateUrl" | "gateApiKey" | "gatePolicyId" | "gateAgentId">;
interface Proposal { toolName: string; arguments: unknown; riskTier: string }
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const failure = (type: string, nonRetryable = true) => new ApplicationFailure(type, { type, nonRetryable });

export function createGateActivity(options: GateOptions) {
  return async (input: Proposal): Promise<{ effect: "ALLOW" | "DENY" | "REQUIRE_APPROVAL"; decisionId: string; approvalId: string | null; arguments: unknown }> => {
    const { signal, idempotencyKey } = activityContext();
    let url: URL;
    try {
      if (!options.gateUrl || !options.gateApiKey || !options.gatePolicyId || !options.gateAgentId) throw new Error();
      url = new URL(`${options.gateUrl.replace(/\/+$/, "")}/decisions`);
      if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.search || url.hash) throw new Error();
    } catch { throw failure("GateConfigurationInvalid"); }
    let response: Response;
    let proposedArguments: unknown;
    try {
      const body = JSON.stringify({ policyId: options.gatePolicyId, agentId: options.gateAgentId, toolName: input.toolName, arguments: input.arguments, riskTier: input.riskTier });
      // Preserve the submitted JSON value, including its key order. Never trust
      // an arguments field echoed by the Gate or reread a mutable input later.
      proposedArguments = (JSON.parse(body) as { arguments?: unknown }).arguments;
      response = await fetch(url, {
        method: "POST", redirect: "error", signal,
        headers: { "Content-Type": "application/json", "X-API-Key": options.gateApiKey!, "Idempotency-Key": idempotencyKey },
        body,
      });
    } catch { throw failure("GateUnavailable", false); }
    if (!response.ok) {
      // Do not read error bodies or retain raw HTTP errors: they can echo secrets.
      await response.body?.cancel().catch(() => {});
      if (response.status === 409) throw failure("GateIdempotencyConflict");
      if (response.status >= 500 || response.status === 408 || response.status === 429) throw failure("GateUnavailable", false);
      throw failure("GateRequestRejected");
    }
    let body: string;
    try { body = await response.text(); }
    catch { throw failure("GateUnavailable", false); }
    let value: Record<string, unknown>;
    try { value = JSON.parse(body) as Record<string, unknown>; }
    catch { throw failure("GateResponseInvalid"); }
    if (!value || !["ALLOW", "DENY", "REQUIRE_APPROVAL"].includes(String(value.effect)) || typeof value.id !== "string" || !uuid.test(value.id) || value.id.includes(options.gateApiKey!) ||
        (value.approvalId != null && (typeof value.approvalId !== "string" || !uuid.test(value.approvalId))) ||
        (typeof value.approvalId === "string" && value.approvalId.includes(options.gateApiKey!)) ||
        (value.effect === "REQUIRE_APPROVAL" && !value.approvalId)) throw failure("GateResponseInvalid");
    return { effect: value.effect as "ALLOW" | "DENY" | "REQUIRE_APPROVAL", decisionId: value.id, approvalId: (value.approvalId as string | null | undefined) ?? null, arguments: proposedArguments };
  };
}
