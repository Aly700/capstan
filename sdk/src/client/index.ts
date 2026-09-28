// The client used by scripts, the CLI, and tests to start and inspect runs. Frozen
// contract for the build; implemented by the SDK core lane.

import type { Duration } from "../types.ts";
import { Code, ConnectError, createClient } from "@connectrpc/connect";
import { ApprovalOutcome, ClientService } from "../gen/capstan/v1/capstan_pb.ts";
import { encode } from "../internal/payload.ts";
import { toProtoDuration } from "../internal/duration.ts";
import { createTransport } from "../worker/transport.ts";
import { describe } from "./description.ts";

export interface ClientOptions {
  address: string;
  apiKey: string;
}

export type RunStatus =
  | "running"
  | "completed"
  | "failed"
  | "cancelled"
  | "timed_out"
  | "blocked"
  | "continued_as_new";

export interface RunDescription {
  runId: string;
  workflowType: string;
  taskQueue: string;
  status: RunStatus;
  startedAt: Date;
  closedAt?: Date;
  lastEventId: number;
  result?: unknown;
  failure?: { message: string; type: string };
  costUsd: number;
  pendingActivities: number;
  pendingApprovals: number;
  continuedAsNewRunId?: string;
}

export interface StartOptions {
  runId: string;
  taskQueue: string;
  runTimeout?: Duration;
  taskTimeout?: Duration;
}

export class Client {
  private readonly rpc;
  constructor(options: ClientOptions) {
    this.rpc = createClient(ClientService, createTransport(options));
  }

  /** Idempotent on runId. Returns whether this call created the run. */
  async start(workflowType: string, input: unknown, options: StartOptions): Promise<{ run: RunDescription; started: boolean }> {
    const payload = encode(input);
    const response = await this.rpc.startRun({
      workflowType, runId: options.runId, taskQueue: options.taskQueue,
      ...(payload === undefined ? {} : { input: payload }),
      ...(options.runTimeout === undefined ? {} : { runTimeout: toProtoDuration(options.runTimeout) }),
      ...(options.taskTimeout === undefined ? {} : { taskTimeout: toProtoDuration(options.taskTimeout) }),
    });
    return { run: describe(response.run), started: response.started };
  }

  async signal(runId: string, name: string, input?: unknown, requestId?: string): Promise<void> {
    const payload = encode(input);
    await this.rpc.signalRun({ runId, name, ...(payload === undefined ? {} : { input: payload }), ...(requestId === undefined ? {} : { requestId }) });
  }

  async cancel(runId: string, reason?: string): Promise<void> {
    await this.rpc.cancelRun({ runId, ...(reason === undefined ? {} : { reason }) });
  }

  async resume(runId: string, reason?: string): Promise<void> {
    await this.rpc.resumeRun({ runId, ...(reason === undefined ? {} : { reason }) });
  }

  async describe(runId: string): Promise<RunDescription> {
    return describe((await this.rpc.describeRun({ runId })).run);
  }

  /** Waits (repeating the server's 30 s long poll) until the run closes, then returns it. */
  async result(runId: string, options?: { timeoutMs?: number }): Promise<RunDescription> {
    if (options?.timeoutMs !== undefined && (!Number.isFinite(options.timeoutMs) || options.timeoutMs < 0)) throw new RangeError("result timeout must be a non-negative finite number");
    const deadline = options?.timeoutMs === undefined ? Infinity : performance.now() + options.timeoutMs;
    for (;;) {
      const remaining = deadline - performance.now();
      if (remaining <= 0) throw new ConnectError("waiting for run result timed out", Code.DeadlineExceeded);
      const response = await this.rpc.awaitRun({ runId }, { timeoutMs: Math.min(35_000, Math.ceil(remaining)) });
      if (response.closed) return describe(response.run);
    }
  }

  async resolveApproval(
    runId: string,
    approvalId: string,
    decision: { outcome: "approved" | "denied"; choice?: string; resolver: string; note?: string },
  ): Promise<void> {
    await this.rpc.resolveApproval({
      runId, approvalId, outcome: decision.outcome === "approved" ? ApprovalOutcome.APPROVED : ApprovalOutcome.DENIED,
      resolver: decision.resolver, ...(decision.choice === undefined ? {} : { choice: decision.choice }),
      ...(decision.note === undefined ? {} : { note: decision.note }),
    });
  }
}
