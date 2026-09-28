// The API available to workflow code. Frozen contract for the build.
//
// Workflow code is bundled and executed inside the SDK's sandbox, where these functions are
// bound to the replay runtime. Called anywhere else they throw. Workflow code must reach the
// outside world only through these functions: the sandbox provides no fetch, fs, net,
// process.env, setTimeout, setInterval, or setImmediate, and Date / Math.random are replaced
// so that re-running a workflow produces the same steps in the same order.
//
// Every function that emits a command consumes the next per-run `seq`, in call order.

import type { ActivityOptions, Duration } from "../types.ts";

export type { ActivityOptions, Duration, RetryPolicy } from "../types.ts";
export {
  ActivityFailure,
  ApplicationFailure,
  CancelledFailure,
  CapstanFailure,
  TimeoutFailure,
} from "../types.ts";

// ---- activities ----

type AnyFn = (...args: any[]) => any;
/** Activity functions take one argument (the input) and may be async. */
export type ActivityProxy<A> = {
  [K in keyof A]: A[K] extends AnyFn
    ? (input: Parameters<A[K]>[0]) => Promise<Awaited<ReturnType<A[K]>>>
    : never;
};

/**
 * Returns a typed proxy: `acts.analyze(input)` emits ScheduleActivity with
 * activity_type "analyze". Import activity types with `import type` only; activity
 * implementations never enter the workflow bundle.
 */
export function proxyActivities<A>(options: ActivityOptions): ActivityProxy<A> {
  return runtime().proxyActivities<A>(options);
}

/** Low-level form of proxyActivities. Resolves with the activity result or rejects with ActivityFailure / TimeoutFailure / CancelledFailure. */
export function activity<T = unknown>(activityType: string, input: unknown, options: ActivityOptions): Promise<T> {
  return runtime().activity<T>(activityType, input, options);
}

// ---- time and randomness ----

/** Durable sleep. Emits StartTimer; resolves when TimerFired is recorded. Rejects with CancelledFailure if the run is cancelled first. */
export function sleep(duration: Duration): Promise<void> {
  return runtime().sleep(duration);
}

/** Workflow time in epoch milliseconds: the time of the current workflow task's TaskStarted event. */
export function now(): number {
  return runtime().now();
}

/** A value in [0, 1) that is identical on every replay (seeded from run_id and seq). */
export function random(): number {
  return runtime().random();
}

/** A UUID v4 recorded in a marker the first time and returned from history on replay. */
export function uuid(): string {
  return runtime().uuid();
}

/**
 * Runs fn once, records its JSON-serializable result in a marker, and returns the recorded
 * value on every replay. fn runs inside the sandbox, so it cannot do I/O either; use it for
 * values that must not change between replays.
 */
export function sideEffect<T>(fn: () => T): T {
  return runtime().sideEffect(fn);
}

// ---- versioning ----

/**
 * Returns true for runs that reach this line on code that contains the patch (and records a
 * marker so replays agree), false for runs whose history predates it.
 */
export function patched(patchId: string): boolean {
  return runtime().patched(patchId);
}

/** Marks a patch as fully rolled out: removes the old branch once no pre-patch runs remain. */
export function deprecatePatch(patchId: string): void {
  runtime().deprecatePatch(patchId);
}

// ---- signals ----

export interface SignalDefinition<T> {
  readonly name: string;
  /** Phantom field carrying the payload type. */
  readonly __input?: T;
}

export function defineSignal<T = unknown>(name: string): SignalDefinition<T> {
  return { name };
}

/**
 * Registers a handler that runs synchronously, in history order, for every SignalReceived
 * with this name, including signals that arrived before the handler was set (they are
 * buffered and delivered on registration).
 */
export function setHandler<T>(signal: SignalDefinition<T>, handler: (input: T) => void): void {
  runtime().setHandler(signal, handler);
}

/** Resolves with the next signal of this name that no handler consumed. */
export function nextSignal<T>(signal: SignalDefinition<T>): Promise<T> {
  return runtime().nextSignal(signal);
}

/**
 * Resolves true as soon as predicate() is true (re-evaluated after every history event),
 * or false when the optional timeout elapses first (the timeout is a durable timer).
 */
export function condition(predicate: () => boolean, timeout?: Duration): Promise<boolean> {
  return runtime().condition(predicate, timeout);
}

// ---- cancellation and lifecycle ----

/** True once RunCancelRequested is in history. Pending sleeps and activities reject with CancelledFailure. */
export function isCancellationRequested(): boolean {
  return runtime().isCancellationRequested();
}

/** Ends this run and starts a fresh one with the given input and an empty history. Never returns. */
export function continueAsNew(input: unknown, options?: { workflowType?: string; taskQueue?: string }): never {
  return runtime().continueAsNew(input, options);
}

export interface WorkflowInfo {
  readonly runId: string;
  readonly workflowType: string;
  readonly taskQueue: string;
  readonly attempt: number;
  /** True while re-running recorded history; false once executing new code. */
  readonly isReplaying: boolean;
  readonly continuedFromRunId: string;
}

export function workflowInfo(): WorkflowInfo {
  return runtime().workflowInfo();
}

/** Logging that is suppressed during replay, so each line appears once per real step. */
export const log = {
  info: (message: string, fields?: Record<string, unknown>) => runtime().log("info", message, fields),
  warn: (message: string, fields?: Record<string, unknown>) => runtime().log("warn", message, fields),
  error: (message: string, fields?: Record<string, unknown>) => runtime().log("error", message, fields),
};

// ---- agent layer (implemented by the agent lane on top of activities and approvals) ----

export { human, model, tool } from "./agent.ts";
export type { HumanDecision, HumanOptions, ModelRequest, ModelResult, ToolOptions, ToolResult } from "./agent.ts";

// ---- runtime binding ----

/** The replay runtime the sandbox installs. Internal: implemented by the SDK core lane. */
export interface WorkflowRuntime {
  proxyActivities<A>(options: ActivityOptions): ActivityProxy<A>;
  activity<T>(activityType: string, input: unknown, options: ActivityOptions): Promise<T>;
  sleep(duration: Duration): Promise<void>;
  now(): number;
  random(): number;
  uuid(): string;
  sideEffect<T>(fn: () => T): T;
  patched(patchId: string): boolean;
  deprecatePatch(patchId: string): void;
  setHandler<T>(signal: SignalDefinition<T>, handler: (input: T) => void): void;
  nextSignal<T>(signal: SignalDefinition<T>): Promise<T>;
  condition(predicate: () => boolean, timeout?: Duration): Promise<boolean>;
  isCancellationRequested(): boolean;
  continueAsNew(input: unknown, options?: { workflowType?: string; taskQueue?: string }): never;
  workflowInfo(): WorkflowInfo;
  log(level: "info" | "warn" | "error", message: string, fields?: Record<string, unknown>): void;
  requestApproval(request: ApprovalRequest): Promise<ApprovalResolution>;
}

export interface ApprovalRequest {
  approvalId: string;
  source: "gate" | "human";
  gateDecisionId?: string;
  tool?: string;
  arguments?: unknown;
  prompt?: string;
  options?: string[];
  timeout?: Duration;
}

export interface ApprovalResolution {
  outcome: "approved" | "denied" | "expired";
  choice: string;
  resolver: string;
  note: string;
}

const RUNTIME_KEY = Symbol.for("capstan.workflow.runtime");

function runtime(): WorkflowRuntime {
  const rt = (globalThis as Record<symbol, WorkflowRuntime | undefined>)[RUNTIME_KEY];
  if (!rt) {
    throw new Error("@capstan/sdk/workflow can only be used inside a workflow running on a Capstan worker");
  }
  return rt;
}

/** Internal: the sandbox calls this to bind the runtime for the current workflow context. */
export function __installRuntime(rt: WorkflowRuntime): void {
  (globalThis as Record<symbol, WorkflowRuntime | undefined>)[RUNTIME_KEY] = rt;
}
