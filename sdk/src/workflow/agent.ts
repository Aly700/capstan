// Agent primitives for workflow code: durable model calls, tool calls through AgentOps
// Gate, and human decisions. Frozen contract for the build, implemented
// on top of the activity and approval mechanisms.

import { ApplicationFailure, type Duration } from "../types.ts";
import { activity, uuid, type ApprovalRequest, type ApprovalResolution, type WorkflowRuntime } from "./index.ts";

// requestApproval is already part of the sandbox's runtime binding; agent helpers use
// that binding without adding a second scheduler or a separate sequence counter.
function approval(request: ApprovalRequest): Promise<ApprovalResolution> {
  const rt = (globalThis as Record<symbol, WorkflowRuntime | undefined>)[Symbol.for("capstan.workflow.runtime")];
  if (!rt) throw new Error("@capstan/sdk/workflow can only be used inside a workflow running on a Capstan worker");
  return rt.requestApproval(request);
}

export interface ModelRequest {
  /** Defaults to the worker's configured main model (claude-sonnet-5 unless overridden). */
  model?: string;
  system?: string;
  /** Either a single user message or a full message list. */
  prompt?: string;
  messages?: { role: "user" | "assistant"; content: string }[];
  maxTokens?: number;
  /**
   * When set, the response is parsed as JSON and validated against this JSON Schema
   * (produced by the worker from a zod schema via `schemaOf`); invalid output fails the
   * activity with a non-retryable ApplicationFailure of type "ModelOutputInvalid".
   */
  jsonSchema?: Record<string, unknown>;
  /** Upper-bound cost estimate for the cap check; the worker computes one when omitted. */
  estimateUsd?: number;
}

export interface ModelResult {
  text: string;
  /** The parsed value when jsonSchema was supplied. */
  json?: unknown;
  model: string;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheWriteTokens: number;
  /** Computed by the server from its price table. */
  costUsd: number;
  stopReason: string;
}

/**
 * A durable model call. Runs as the built-in activity "capstan.model": the worker reserves
 * budget with the server (failing closed when the daily cap would be exceeded), calls the
 * model, and reports token usage; the server prices it. On replay nothing is called and
 * nothing is billed.
 */
export function model(request: ModelRequest): Promise<ModelResult> {
  return activity<ModelResult>("capstan.model", request, { startToCloseTimeout: "5m" });
}

export interface ToolOptions {
  /** Risk tier sent to the Gate. Default "MEDIUM". */
  riskTier?: "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";
  /** How long to wait for a human when the Gate requires approval. Unset waits indefinitely. */
  approvalTimeout?: Duration;
  /** Options for the tool activity itself. startToCloseTimeout defaults to "5m". */
  startToCloseTimeout?: Duration;
}

export type ToolResult<T> =
  | { allowed: true; value: T; decisionId: string; approvedBy?: string }
  | { allowed: false; reason: "denied" | "rejected" | "expired"; decisionId: string; rule?: string; resolver?: string };

/**
 * A tool call governed by AgentOps Gate:
 *   1. built-in activity "capstan.gate.decide" posts the proposal to the Gate with the
 *      activity's idempotency key;
 *   2. ALLOW runs the registered tool activity named `name`;
 *   3. DENY resolves { allowed: false, reason: "denied" } without running anything;
 *   4. REQUIRE_APPROVAL records ApprovalRequested (source GATE) and waits, holding no worker,
 *      until the server observes the Gate's decision; approved runs the tool, rejected or
 *      expired resolves { allowed: false }.
 */
export function tool<T = unknown>(name: string, args: unknown, options?: ToolOptions): Promise<ToolResult<T>> {
  return (async () => {
    // Serialize once before yielding so caller mutation cannot change what the
    // Gate authorized. This uses the same JSON representation as activity payloads.
    const serialized = JSON.stringify(args);
    const proposedArguments: unknown = serialized === undefined ? undefined : JSON.parse(serialized);
    const decision = await activity<{ effect: string; decisionId: string; approvalId: string | null; arguments: unknown }>(
      "capstan.gate.decide", { toolName: name, arguments: proposedArguments, riskTier: options?.riskTier ?? "MEDIUM" }, { startToCloseTimeout: "30s" },
    );
    const invalid = () => new ApplicationFailure("Gate returned an invalid decision", { type: "GateResponseInvalid", nonRetryable: true });
    if (!decision || typeof decision.decisionId !== "string" || !decision.decisionId) throw invalid();
    if (decision.effect === "DENY") return { allowed: false, reason: "denied", decisionId: decision.decisionId };
    let approvedBy: string | undefined;
    if (decision.effect === "REQUIRE_APPROVAL") {
      if (typeof decision.approvalId !== "string" || !decision.approvalId) throw invalid();
      const resolution = await approval({
        source: "gate", approvalId: decision.approvalId, gateDecisionId: decision.decisionId, tool: name, arguments: decision.arguments,
        ...(options?.approvalTimeout === undefined ? {} : { timeout: options.approvalTimeout }),
      });
      if (resolution.outcome !== "approved") return {
        allowed: false, reason: resolution.outcome === "denied" ? "rejected" : "expired", decisionId: decision.decisionId, resolver: resolution.resolver,
      };
      approvedBy = resolution.resolver;
    } else if (decision.effect !== "ALLOW") throw invalid();
    const value = await activity<T>(name, decision.arguments, { startToCloseTimeout: options?.startToCloseTimeout ?? "5m" });
    return { allowed: true, value, decisionId: decision.decisionId, ...(approvedBy === undefined ? {} : { approvedBy }) };
  })();
}

export interface HumanOptions {
  /** Allowed choices; empty means approve / deny. */
  options?: string[];
  timeout?: Duration;
}

export interface HumanDecision {
  outcome: "approved" | "denied" | "expired";
  /** The chosen option when options were supplied. */
  choice: string;
  resolver: string;
  note: string;
}

/**
 * Waits for a person. Records ApprovalRequested (source HUMAN) and resolves when someone
 * runs `capstan approve` / `capstan deny` (ResolveApproval) or the timeout elapses.
 */
export function human(prompt: string, options?: HumanOptions): Promise<HumanDecision> {
  return approval({
    source: "human", approvalId: uuid(), prompt,
    ...(options?.options === undefined ? {} : { options: options.options }),
    ...(options?.timeout === undefined ? {} : { timeout: options.timeout }),
  });
}
