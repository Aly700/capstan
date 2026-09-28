// Agent primitives for workflow code: durable model calls, tool calls through AgentOps
// Gate, and human decisions. Frozen contract for the build; implemented by the agent lane
// on top of the activity and approval mechanisms. Until then each function throws.

import type { Duration } from "../types.ts";

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
  void request;
  throw new Error("capstan: model() is not implemented yet (agent lane)");
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
  void name;
  void args;
  void options;
  throw new Error("capstan: tool() is not implemented yet (agent lane)");
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
  void prompt;
  void options;
  throw new Error("capstan: human() is not implemented yet (agent lane)");
}
