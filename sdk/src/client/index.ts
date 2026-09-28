// The client used by scripts, the CLI, and tests to start and inspect runs. Frozen
// contract for the build; implemented by the SDK core lane.

import type { Duration } from "../types.ts";

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
  constructor(options: ClientOptions) {
    void options;
  }

  /** Idempotent on runId. Returns whether this call created the run. */
  async start(workflowType: string, input: unknown, options: StartOptions): Promise<{ run: RunDescription; started: boolean }> {
    void workflowType;
    void input;
    void options;
    throw new Error("capstan: Client is not implemented yet (SDK core lane)");
  }

  async signal(runId: string, name: string, input?: unknown, requestId?: string): Promise<void> {
    void runId;
    void name;
    void input;
    void requestId;
    throw new Error("capstan: Client is not implemented yet (SDK core lane)");
  }

  async cancel(runId: string, reason?: string): Promise<void> {
    void runId;
    void reason;
    throw new Error("capstan: Client is not implemented yet (SDK core lane)");
  }

  async resume(runId: string, reason?: string): Promise<void> {
    void runId;
    void reason;
    throw new Error("capstan: Client is not implemented yet (SDK core lane)");
  }

  async describe(runId: string): Promise<RunDescription> {
    void runId;
    throw new Error("capstan: Client is not implemented yet (SDK core lane)");
  }

  /** Waits (repeating the server's 30 s long poll) until the run closes, then returns it. */
  async result(runId: string, options?: { timeoutMs?: number }): Promise<RunDescription> {
    void runId;
    void options;
    throw new Error("capstan: Client is not implemented yet (SDK core lane)");
  }

  async resolveApproval(
    runId: string,
    approvalId: string,
    decision: { outcome: "approved" | "denied"; choice?: string; resolver: string; note?: string },
  ): Promise<void> {
    void runId;
    void approvalId;
    void decision;
    throw new Error("capstan: Client is not implemented yet (SDK core lane)");
  }
}
