// The host-side worker: polls the server, runs workflow tasks in the sandbox and activities
// in normal Node. Frozen contract for the build; implemented by the SDK core lane.

import { hostname } from "node:os";
import { createClient, type Client } from "@connectrpc/connect";
import { WorkerService, TaskFailedCause, type PollWorkflowTaskResponse } from "../gen/capstan/v1/capstan_pb.ts";
import { bundleWorkflows } from "../sandbox/bundle.ts";
import { replay } from "../replay/runtime.ts";
import { failureToProto } from "../internal/failure.ts";
import { encode } from "../internal/payload.ts";
import { diagnosticRedactor, redactDiagnosticValue, type Redactor } from "../internal/privacy.ts";
import { CancelledFailure, HistoryMismatchError } from "../types.ts";
import { activityStorage, executeActivity } from "./activities.ts";
import { createTransport } from "./transport.ts";
import { pollLoop, report, untilAborted } from "./pollers.ts";
import { createGateActivity } from "./builtins/gate.ts";
import { createModelActivity } from "./builtins/model.ts";

export interface WorkerOptions {
  /** Server address, e.g. "http://127.0.0.1:7233". */
  address: string;
  apiKey: string;
  taskQueue: string;
  /** Path to the module exporting workflow functions; bundled with esbuild at startup. */
  workflowsPath: string;
  /** Activity implementations by activity type. Built-in agent activities are added by the agent lane. */
  activities?: Record<string, (input: any) => unknown>;
  /** Default model for capstan.model. Defaults to claude-sonnet-5. The API key is read only from ANTHROPIC_API_KEY. */
  model?: string;
  /** AgentOps Gate base URL. Gate configuration stays in the worker, outside workflow history. */
  gateUrl?: string;
  /** Gate's X-API-Key credential; distinct from the Capstan server's apiKey above. */
  gateApiKey?: string;
  /** Policy UUID and agent identity sent by capstan.gate.decide. Both are required to use tool(). */
  gatePolicyId?: string;
  gateAgentId?: string;
  /** Defaults to "<hostname>:<pid>". */
  identity?: string;
  /** Identifies the code version; recorded on TaskCompleted. Defaults to a hash of the bundle. */
  buildId?: string;
  /** Default 10. */
  maxConcurrentWorkflowTasks?: number;
  /** Default 50. */
  maxConcurrentActivities?: number;
  /** Structured log sink; defaults to JSON lines on stderr. */
  logger?: (entry: Record<string, unknown>) => void;
}

export interface ActivityContext {
  readonly runId: string;
  readonly workflowType: string;
  readonly activityType: string;
  readonly seq: number;
  readonly attempt: number;
  /** Stable across every attempt: pass it to external systems that support idempotency keys. */
  readonly idempotencyKey: string;
  /** Details recorded by the previous attempt's last heartbeat, if any. */
  readonly heartbeatDetails: unknown;
  /** Aborted when the server reports cancellation, the attempt times out, or the worker shuts down. */
  readonly signal: AbortSignal;
  /** Records progress and keeps a heartbeat-timeout activity alive. */
  heartbeat(details?: unknown): void;
}

/** Returns the context of the activity currently executing (AsyncLocalStorage-backed). */
export function activityContext(): ActivityContext {
  const context = activityStorage.getStore();
  if (!context) throw new Error("activityContext() can only be used inside a running activity");
  return context;
}

export class Worker {
  private options!: WorkerOptions;
  private client!: Client<typeof WorkerService>;
  private bundle!: Awaited<ReturnType<typeof bundleWorkflows>>;
  private identity = "";
  private buildId = "";
  private redact: Redactor = (text) => text;
  private readonly polls = new AbortController();
  private readonly tasks = new AbortController();
  private running: Promise<void> | undefined;
  private stopping: Promise<void> | undefined;

  static async create(options: WorkerOptions): Promise<Worker> {
    for (const [name, value] of Object.entries({ maxConcurrentWorkflowTasks: options.maxConcurrentWorkflowTasks ?? 10, maxConcurrentActivities: options.maxConcurrentActivities ?? 50 })) {
      if (!Number.isInteger(value) || value < 1) throw new RangeError(`${name} must be a positive integer`);
    }
    const worker = new Worker();
    worker.options = options;
    worker.redact = diagnosticRedactor([options.apiKey, options.gateApiKey]);
    worker.client = createClient(WorkerService, createTransport(options));
    worker.bundle = await bundleWorkflows(options.workflowsPath);
    worker.identity = options.identity ?? `${hostname()}:${process.pid}`;
    worker.buildId = options.buildId ?? worker.bundle.buildId;
    return worker;
  }

  private log = (entry: Record<string, unknown>) => {
    try {
      entry = redactDiagnosticValue(entry, this.redact);
      if (this.options.logger) this.options.logger(entry);
      else process.stderr.write(`${JSON.stringify(entry)}\n`);
    } catch { /* Logging must not change task outcomes. */ }
  };

  private async workflow(task: PollWorkflowTaskResponse): Promise<void> {
    let commands;
    try {
      commands = await untilAborted(replay({ bundle: this.bundle, runId: task.runId, workflowType: task.workflowType, history: task.history, attempt: task.attempt, logger: this.log }), this.tasks.signal);
    } catch (error) {
      if (this.tasks.signal.aborted) return;
      const mismatch = error instanceof HistoryMismatchError || (error instanceof Error && error.name === "HistoryMismatchError");
      const failure = failureToProto(error, this.redact);
      if (mismatch) failure.details = encode({ $capstan: { kind: "mismatch", eventId: (error as HistoryMismatchError).eventId } });
      await report(() => this.client.failWorkflowTask({ taskToken: task.taskToken, cause: mismatch ? TaskFailedCause.HISTORY_MISMATCH : TaskFailedCause.SDK_ERROR, failure, identity: this.identity }, { signal: this.tasks.signal }), this.log, "workflow");
      return;
    }
    if (!this.tasks.signal.aborted) await report(() => this.client.completeWorkflowTask({ taskToken: task.taskToken, commands, identity: this.identity, buildId: this.buildId }, { signal: this.tasks.signal }), this.log, "workflow");
  }

  /** Resolves when the worker has shut down. */
  async run(): Promise<void> {
    if (!this.client) throw new Error("create a worker with Worker.create() before calling run()");
    if (this.running) return this.running;
    if (this.polls.signal.aborted) return;
    const loops: Promise<void>[] = [];
    for (let i = 0; i < (this.options.maxConcurrentWorkflowTasks ?? 10); i++) loops.push(pollLoop({
      kind: "workflow", signal: this.polls.signal, logger: this.log,
      poll: () => this.client.pollWorkflowTask({ taskQueue: this.options.taskQueue, identity: this.identity, buildId: this.buildId }, { signal: this.polls.signal, timeoutMs: 35_000 }),
      execute: (task) => this.workflow(task),
    }));
    for (let i = 0; i < (this.options.maxConcurrentActivities ?? 50); i++) loops.push(pollLoop({
      kind: "activity", signal: this.polls.signal, logger: this.log,
      poll: () => this.client.pollActivityTask({ taskQueue: this.options.taskQueue, identity: this.identity }, { signal: this.polls.signal, timeoutMs: 35_000 }),
      execute: (task) => executeActivity({ task, client: this.client, activities: {
        ...this.options.activities,
        "capstan.gate.decide": createGateActivity(this.options),
        "capstan.model": createModelActivity({ client: this.client, taskToken: task.taskToken, ...(this.options.model === undefined ? {} : { model: this.options.model }) }),
      }, settleOnAbort: task.activityType === "capstan.model", identity: this.identity, shutdown: this.tasks.signal, logger: this.log, redact: this.redact }),
    }));
    this.running = Promise.all(loops).then(() => {});
    return this.running;
  }

  /** Stops polling, lets in-flight tasks finish (up to the grace period), then resolves. */
  async shutdown(graceMs = 25_000): Promise<void> {
    if (!Number.isFinite(graceMs) || graceMs < 0) throw new RangeError("shutdown grace must be a non-negative finite number");
    if (this.stopping) return this.stopping;
    this.polls.abort();
    this.stopping = (async () => {
      let graceTimer: ReturnType<typeof setTimeout> | undefined;
      await Promise.race([this.running, new Promise<void>((resolve) => { graceTimer = setTimeout(resolve, graceMs); })]);
      clearTimeout(graceTimer);
      this.tasks.abort(new CancelledFailure("worker is shutting down"));
      await this.running;
    })();
    return this.stopping;
  }
}
