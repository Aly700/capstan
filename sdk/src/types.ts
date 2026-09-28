// Shared public types for the Capstan SDK. Frozen contract for the build.

/** Milliseconds, or a string such as "250ms", "30s", "5m", "2h", "3d". */
export type Duration = number | `${number}${"ms" | "s" | "m" | "h" | "d"}`;

export interface RetryPolicy {
  /** Default "1s". */
  initialInterval?: Duration;
  /** Default 2. */
  backoffCoefficient?: number;
  /** Default 100 x initialInterval. */
  maximumInterval?: Duration;
  /** 0 or undefined means unlimited. */
  maximumAttempts?: number;
  /** Failure types (ApplicationFailure.type) that are never retried. */
  nonRetryableErrorTypes?: string[];
}

export interface ActivityOptions {
  /** Required: how long one attempt may run once a worker has it. */
  startToCloseTimeout: Duration;
  scheduleToCloseTimeout?: Duration;
  scheduleToStartTimeout?: Duration;
  /** When set, the activity must heartbeat at least this often or the attempt times out. */
  heartbeatTimeout?: Duration;
  retry?: RetryPolicy;
  /** Defaults to the run's task queue. */
  taskQueue?: string;
}

/** Base of every failure a workflow can observe. Serializes to capstan.v1.Failure. */
export class CapstanFailure extends Error {
  readonly type: string;
  readonly nonRetryable: boolean;
  readonly details: unknown;
  constructor(message: string, opts: { type?: string; nonRetryable?: boolean; details?: unknown; cause?: unknown } = {}) {
    super(message, opts.cause === undefined ? undefined : { cause: opts.cause });
    this.name = new.target.name;
    this.type = opts.type ?? new.target.name;
    this.nonRetryable = opts.nonRetryable ?? false;
    this.details = opts.details;
  }
}

/** Thrown by user code to fail an activity or a workflow with a typed, optionally non-retryable error. */
export class ApplicationFailure extends CapstanFailure {}

/** An activity failed after its retry policy was exhausted. `cause` holds the last failure. */
export class ActivityFailure extends CapstanFailure {
  readonly activityType: string;
  readonly seq: number;
  constructor(message: string, activityType: string, seq: number, opts: { cause?: unknown } = {}) {
    super(message, { type: "ActivityFailure", ...opts });
    this.activityType = activityType;
    this.seq = seq;
  }
}

export class TimeoutFailure extends CapstanFailure {
  readonly timeoutType: "SCHEDULE_TO_START" | "START_TO_CLOSE" | "SCHEDULE_TO_CLOSE" | "HEARTBEAT";
  constructor(message: string, timeoutType: TimeoutFailure["timeoutType"], opts: { cause?: unknown } = {}) {
    super(message, { type: "TimeoutFailure", ...opts });
    this.timeoutType = timeoutType;
  }
}

/** The run, an activity, or a timer was cancelled. */
export class CancelledFailure extends CapstanFailure {}

/** Workflow code asked for something different from what its history recorded. */
export class HistoryMismatchError extends Error {
  readonly eventId: number;
  readonly expected: string;
  readonly got: string;
  constructor(eventId: number, expected: string, got: string) {
    super(`history mismatch at event ${eventId}: history has ${expected}, code emitted ${got}`);
    this.name = "HistoryMismatchError";
    this.eventId = eventId;
    this.expected = expected;
    this.got = got;
  }
}

/** Workflow code touched something the sandbox forbids (network, filesystem, real timers, env). */
export class SandboxViolationError extends Error {
  constructor(what: string) {
    super(`workflow code cannot use ${what}; use an activity instead`);
    this.name = "SandboxViolationError";
  }
}
