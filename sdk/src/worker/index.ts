// The host-side worker: polls the server, runs workflow tasks in the sandbox and activities
// in normal Node. Frozen contract for the build; implemented by the SDK core lane.

export interface WorkerOptions {
  /** Server address, e.g. "http://127.0.0.1:7233". */
  address: string;
  apiKey: string;
  taskQueue: string;
  /** Path to the module exporting workflow functions; bundled with esbuild at startup. */
  workflowsPath: string;
  /** Activity implementations by activity type. Built-in agent activities are added by the agent lane. */
  activities?: Record<string, (input: any) => unknown>;
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
  throw new Error("capstan: activityContext() is not implemented yet (SDK core lane)");
}

export class Worker {
  static async create(options: WorkerOptions): Promise<Worker> {
    void options;
    throw new Error("capstan: Worker is not implemented yet (SDK core lane)");
  }

  /** Resolves when the worker has shut down. */
  async run(): Promise<void> {
    throw new Error("capstan: Worker is not implemented yet (SDK core lane)");
  }

  /** Stops polling, lets in-flight tasks finish (up to the grace period), then resolves. */
  async shutdown(graceMs = 25_000): Promise<void> {
    void graceMs;
    throw new Error("capstan: Worker is not implemented yet (SDK core lane)");
  }
}
