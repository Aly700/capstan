import { AsyncLocalStorage } from "node:async_hooks";
import type { Client } from "@connectrpc/connect";
import { Code, ConnectError } from "@connectrpc/connect";
import type { PollActivityTaskResponse, WorkerService } from "../gen/capstan/v1/capstan_pb.ts";
import { encode, decode } from "../internal/payload.ts";
import { failureToProto } from "../internal/failure.ts";
import type { Redactor } from "../internal/privacy.ts";
import { ApplicationFailure, CancelledFailure, TimeoutFailure } from "../types.ts";
import type { ActivityContext, WorkerOptions } from "./index.ts";
import { report, untilAborted, type Logger } from "./pollers.ts";

export const activityStorage = new AsyncLocalStorage<ActivityContext>();
function milliseconds(duration: { seconds: bigint; nanos: number } | undefined): number {
  return duration ? Number(duration.seconds) * 1_000 + duration.nanos / 1e6 : 0;
}

export async function executeActivity(options: {
  task: PollActivityTaskResponse;
  client: Client<typeof WorkerService>;
  activities: NonNullable<WorkerOptions["activities"]>;
  identity: string;
  shutdown: AbortSignal;
  logger: Logger;
  redact?: Redactor;
  /** Built-in model work honours abort itself, then finishes its reserved ledger row. */
  settleOnAbort?: boolean;
}): Promise<void> {
  const { task, client, shutdown, logger } = options;
  const abort = new AbortController();
  const onShutdown = () => abort.abort(shutdown.reason);
  if (shutdown.aborted) onShutdown();
  else shutdown.addEventListener("abort", onShutdown, { once: true });
  const timeoutMs = milliseconds(task.startToCloseTimeout);
  const elapsedMs = task.startedTime ? Math.max(0, Date.now() - milliseconds(task.startedTime)) : 0;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = performance.now() + Math.max(0, timeoutMs - elapsedMs);
  const expire = () => {
    const remaining = deadline - performance.now();
    if (remaining > 0) timer = setTimeout(expire, Math.min(remaining, 2_147_483_647));
    else abort.abort(new TimeoutFailure("activity start-to-close timeout expired", "START_TO_CLOSE"));
  };
  if (timeoutMs > 0) expire();
  const heartbeatAbort = new AbortController();
  const heartbeatSignal = AbortSignal.any([abort.signal, heartbeatAbort.signal]);
  const interval = Math.max(1_000, milliseconds(task.heartbeatTimeout) / 3);
  let heartbeatTimer: ReturnType<typeof setTimeout> | undefined;
  let heartbeatInFlight = false;
  let nextHeartbeatAt = 0;
  let latestDetails: unknown;
  let dirty = false;
  let finished = false;

  const sendHeartbeat = async () => {
    if (finished || abort.signal.aborted || heartbeatInFlight || !dirty) return;
    dirty = false;
    heartbeatInFlight = true;
    nextHeartbeatAt = performance.now() + interval;
    try {
      const details = encode(latestDetails);
      const response = await client.heartbeatActivityTask({ taskToken: task.taskToken, ...(details === undefined ? {} : { details }) }, { signal: heartbeatSignal });
      if (response.cancelRequested) abort.abort(new CancelledFailure("activity cancellation requested"));
    } catch (error) {
      if (error instanceof ConnectError && error.code === Code.FailedPrecondition) abort.abort(new CancelledFailure("activity task is stale"));
      else if (!abort.signal.aborted && !finished) logger({ level: "warn", message: "activity heartbeat failed", runId: task.runId, seq: Number(task.seq) });
    } finally {
      heartbeatInFlight = false;
      scheduleHeartbeat();
    }
  };
  const scheduleHeartbeat = () => {
    if (finished || abort.signal.aborted || heartbeatInFlight || heartbeatTimer || !dirty) return;
    const wait = nextHeartbeatAt - performance.now();
    if (wait <= 0) void sendHeartbeat();
    else heartbeatTimer = setTimeout(() => { heartbeatTimer = undefined; scheduleHeartbeat(); }, Math.min(wait, 2_147_483_647));
  };
  let result: unknown;
  let failure: unknown;
  let failed = false;
  try {
    abort.signal.throwIfAborted();
    const context: ActivityContext = {
      runId: task.runId,
      workflowType: task.workflowType,
      activityType: task.activityType,
      seq: Number(task.seq),
      attempt: task.attempt,
      idempotencyKey: task.idempotencyKey,
      heartbeatDetails: decode(task.heartbeatDetails),
      signal: abort.signal,
      heartbeat(details) { latestDetails = details; dirty = true; scheduleHeartbeat(); },
    };
    const fn = Object.hasOwn(options.activities, task.activityType) ? options.activities[task.activityType] : undefined;
    if (!fn) throw new ApplicationFailure(`activity type ${task.activityType} is not registered`, { type: "ActivityNotRegistered", nonRetryable: true });
    const work = Promise.resolve().then(() => { abort.signal.throwIfAborted(); return activityStorage.run(context, () => fn(decode(task.input))); });
    result = options.settleOnAbort ? await work : await untilAborted(work, abort.signal);
    // Serialization is part of the attempt, and a serialization error is reportable.
    result = encode(result);
  } catch (error) { failed = true; failure = error; }
  finally {
    finished = true;
    heartbeatAbort.abort();
    clearTimeout(timer);
    clearTimeout(heartbeatTimer);
    shutdown.removeEventListener("abort", onShutdown);
  }
  if (shutdown.aborted) return;
  if (failed) await report(() => client.failActivityTask({ taskToken: task.taskToken, failure: failureToProto(failure, options.redact), identity: options.identity }, { signal: shutdown }), logger, "activity");
  else await report(() => client.completeActivityTask({ taskToken: task.taskToken, ...(result === undefined ? {} : { result: result as NonNullable<ReturnType<typeof encode>> }), identity: options.identity }, { signal: shutdown }), logger, "activity");
}
