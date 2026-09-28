import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { create, fromJson, type MessageInitShape } from "@bufbuild/protobuf";
import { DurationSchema } from "@bufbuild/protobuf/wkt";
import { Code, ConnectError } from "@connectrpc/connect";
import { afterEach, describe, expect, it } from "vitest";
import { Worker, activityContext, type WorkerOptions, type ActivityContext } from "../src/worker/index.ts";
import { ApplicationFailure } from "../src/types.ts";
import { HistoryEventSchema, PollActivityTaskResponseSchema, PayloadSchema, TaskFailedCause, type CompleteActivityTaskRequest, type CompleteWorkflowTaskRequest, type FailActivityTaskRequest, type FailWorkflowTaskRequest, type HeartbeatActivityTaskRequest } from "../src/gen/capstan/v1/capstan_pb.ts";
import { fakeServer } from "./fake-server.ts";

const workflowsPath = fileURLToPath(new URL("./worker-fixtures/workflows.ts", import.meta.url));
const cleanup: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const close of cleanup.reverse()) await close(); cleanup.length = 0; });
async function setup(scripts: Parameters<typeof fakeServer>[0], options: Partial<WorkerOptions> = {}) {
  const server = await fakeServer(scripts);
  cleanup.push(() => server.close());
  const worker = await Worker.create({ address: server.address, apiKey: server.apiKey, taskQueue: "q", workflowsPath, maxConcurrentActivities: 1, maxConcurrentWorkflowTasks: 1, identity: "test-worker", buildId: "test-build", logger: () => {}, ...options });
  cleanup.push(() => worker.shutdown(0));
  const running = worker.run();
  return { server, worker, running };
}
function activityTask(overrides: MessageInitShape<typeof PollActivityTaskResponseSchema> = {}) {
  return create(PollActivityTaskResponseSchema, { taskToken: new Uint8Array([1]), runId: "run1", workflowType: "singleActivity", seq: 3n, activityType: "double", input: create(PayloadSchema, { contentType: "application/json", data: Buffer.from("21") }), attempt: 2, idempotencyKey: "run1/3", startToCloseTimeout: create(DurationSchema, { seconds: 10n }), ...overrides });
}
function onceTask<T>(task: T) { let sent = false; return async (_request: unknown, context: { signal: AbortSignal }): Promise<T | Record<string, never>> => { if (!sent) { sent = true; return task; } if (!context.signal.aborted) await new Promise<void>((resolve) => context.signal.addEventListener("abort", () => resolve(), { once: true })); return {}; }; }
async function history() {
  const fixture = JSON.parse(await readFile(new URL("../../conformance/fixtures/001-single-activity.json", import.meta.url), "utf8"));
  return fixture.history.map((event: unknown) => fromJson(HistoryEventSchema, event as Parameters<typeof fromJson<typeof HistoryEventSchema>>[1]));
}
function value(payload: { data: Uint8Array } | undefined) { return payload ? JSON.parse(Buffer.from(payload.data).toString()) : undefined; }

describe("worker over scripted h2c Connect", () => {
  it("runs an activity with isolated context, auth and a 35 second poll deadline", async () => {
    let context: ActivityContext | undefined;
    const { server } = await setup({ worker: { pollActivityTask: onceTask(activityTask({ heartbeatDetails: { contentType: "application/json", data: Buffer.from('{"offset":7}') } })) } }, { activities: { double: async (input) => { await Promise.resolve(); context = activityContext(); return input * 2; } } });
    await expect.poll(() => server.requests<CompleteActivityTaskRequest>("CompleteActivityTask").length).toBe(1);
    expect(value(server.requests<CompleteActivityTaskRequest>("CompleteActivityTask")[0]!.result)).toBe(42);
    expect(context).toMatchObject({ runId: "run1", workflowType: "singleActivity", activityType: "double", seq: 3, attempt: 2, idempotencyKey: "run1/3", heartbeatDetails: { offset: 7 } });
    expect(() => activityContext()).toThrow(/activity/i);
    expect(server.calls.every((call) => call.authorization === "Bearer test-api-key")).toBe(true);
    for (const poll of server.calls.filter((call) => call.method.startsWith("Poll"))) expect(poll.timeoutMs).toBeGreaterThan(34_000);
    expect(server.requests("PollActivityTask")[0]).toMatchObject({ identity: "test-worker", taskQueue: "q" });
  });

  it("preserves typed activity failure and rejects an unregistered activity without retry", async () => {
    const tasks = [activityTask(), activityTask({ activityType: "missing", taskToken: new Uint8Array([2]) })];
    const { server } = await setup({ worker: { pollActivityTask: async (_request, context) => tasks.shift() ?? onceTask({})(_request, context) } }, { activities: { double: () => { throw new ApplicationFailure("declined", { type: "Declined", nonRetryable: true, details: { code: 7 } }); } } });
    await expect.poll(() => server.requests<FailActivityTaskRequest>("FailActivityTask").length).toBe(2);
    const failures = server.requests<FailActivityTaskRequest>("FailActivityTask").map((request) => request.failure);
    expect(failures[0]).toMatchObject({ type: "Declined", nonRetryable: true, message: "declined" });
    expect(value(failures[0]!.details)).toEqual({ code: 7 });
    expect(failures[1]).toMatchObject({ type: "ActivityNotRegistered", nonRetryable: true });
  });

  it("throttles heartbeat RPCs to one second and sends the latest details", async () => {
    const { server } = await setup({ worker: { pollActivityTask: onceTask(activityTask({ heartbeatTimeout: { seconds: 1n } })) } }, { activities: { double: async () => { const context = activityContext(); context.heartbeat({ n: 1 }); context.heartbeat({ n: 2 }); await delay(100); context.heartbeat({ n: 3 }); await delay(1_200); return 42; } } });
    await expect.poll(() => server.requests<CompleteActivityTaskRequest>("CompleteActivityTask").length, { timeout: 3_000 }).toBe(1);
    const heartbeats = server.calls.filter((call) => call.method === "HeartbeatActivityTask");
    expect(heartbeats.length).toBe(2);
    expect(heartbeats[1]!.at - heartbeats[0]!.at).toBeGreaterThanOrEqual(970);
    expect(value((heartbeats[1]!.request as HeartbeatActivityTaskRequest).details)).toEqual({ n: 3 });
  });

  it("aborts the activity signal when the server requests cancellation", async () => {
    let reason: unknown;
    const { server } = await setup({ worker: { pollActivityTask: onceTask(activityTask({ heartbeatTimeout: { seconds: 3n } })), heartbeatActivityTask: () => ({ cancelRequested: true }) } }, { activities: { double: async () => { const { signal, heartbeat } = activityContext(); heartbeat("working"); await new Promise<void>((resolve) => signal.addEventListener("abort", () => { reason = signal.reason; resolve(); }, { once: true })); signal.throwIfAborted(); } } });
    await expect.poll(() => server.requests<FailActivityTaskRequest>("FailActivityTask").length).toBe(1);
    expect(reason).toMatchObject({ type: "CancelledFailure" });
    expect(server.requests<FailActivityTaskRequest>("FailActivityTask")[0]!.failure?.type).toBe("CancelledFailure");
  });

  it("aborts and reports local start-to-close expiry even when the function ignores abort", async () => {
    let signal: AbortSignal | undefined;
    const { server } = await setup({ worker: { pollActivityTask: onceTask(activityTask({ startToCloseTimeout: { nanos: 50_000_000 } })) } }, { activities: { double: async () => { signal = activityContext().signal; return new Promise(() => {}); } } });
    await expect.poll(() => server.requests<FailActivityTaskRequest>("FailActivityTask").length).toBe(1);
    expect(signal?.aborted).toBe(true);
    expect(server.requests<FailActivityTaskRequest>("FailActivityTask")[0]!.failure?.type).toBe("TimeoutFailure");
  });

  it("honours the original server start time for an already expired attempt", async () => {
    let signal: AbortSignal | undefined;
    const { server } = await setup({ worker: { pollActivityTask: onceTask(activityTask({ startedTime: { seconds: BigInt(Math.floor(Date.now() / 1_000) - 6) }, startToCloseTimeout: { seconds: 5n } })) } }, { activities: { double: async () => { signal = activityContext().signal; return new Promise(() => {}); } } });
    await expect.poll(() => server.requests<FailActivityTaskRequest>("FailActivityTask").length, { timeout: 500 }).toBe(1);
    expect(signal).toBeUndefined();
  });

  it("does not execute synchronous activity effects after its server deadline", async () => {
    let effects = 0;
    const { server } = await setup({ worker: { pollActivityTask: onceTask(activityTask({ startedTime: { seconds: BigInt(Math.floor(Date.now() / 1_000) - 6) }, startToCloseTimeout: { seconds: 5n } })) } }, { activities: { double: () => { ++effects; return 42; } } });
    await expect.poll(() => server.requests<FailActivityTaskRequest>("FailActivityTask").length, { timeout: 300 }).toBe(1);
    expect(effects).toBe(0);
    expect(server.requests("CompleteActivityTask")).toHaveLength(0);
    expect(server.requests<FailActivityTaskRequest>("FailActivityTask")[0]!.failure?.type).toBe("TimeoutFailure");
  });

  it("reports malformed heartbeat details as an attempt failure", async () => {
    let executed = false;
    const { server } = await setup({ worker: { pollActivityTask: onceTask(activityTask({ heartbeatDetails: { contentType: "application/json", data: Buffer.from("{") } })) } }, { activities: { double: () => { executed = true; return 42; } } });
    await expect.poll(() => server.requests<FailActivityTaskRequest>("FailActivityTask").length, { timeout: 300 }).toBe(1);
    expect(executed).toBe(false);
    expect(server.requests<FailActivityTaskRequest>("FailActivityTask")[0]!.failure?.type).toBe("SyntaxError");
  });

  it("cancels an in-flight heartbeat RPC once its activity completes", async () => {
    let heartbeatCancelled = false;
    const { server } = await setup({ worker: {
      pollActivityTask: onceTask(activityTask()),
      heartbeatActivityTask: async (_request, context) => {
        await new Promise<void>((resolve) => context.signal.addEventListener("abort", () => { heartbeatCancelled = true; resolve(); }, { once: true }));
        return {};
      },
    } }, { activities: { double: async () => { activityContext().heartbeat("progress"); await delay(50); return 42; } } });
    await expect.poll(() => server.requests("CompleteActivityTask").length).toBe(1);
    await expect.poll(() => heartbeatCancelled, { timeout: 300 }).toBe(true);
  });

  it("does not truncate activity timeouts longer than the native timer range", async () => {
    const { server } = await setup({ worker: { pollActivityTask: onceTask(activityTask({ startToCloseTimeout: { seconds: 30n * 86_400n } })) } }, { activities: { double: async () => { await delay(20); return 42; } } });
    await expect.poll(() => server.requests("CompleteActivityTask").length, { timeout: 300 }).toBe(1);
    expect(server.requests("FailActivityTask")).toHaveLength(0);
  });

  it("does not truncate long heartbeat throttle intervals to one millisecond", async () => {
    const { server } = await setup({ worker: { pollActivityTask: onceTask(activityTask({ heartbeatTimeout: { seconds: 90n * 86_400n } })) } }, { activities: { double: async () => { const context = activityContext(); context.heartbeat(1); context.heartbeat(2); await delay(50); return 42; } } });
    await expect.poll(() => server.requests("CompleteActivityTask").length).toBe(1);
    expect(server.requests("HeartbeatActivityTask")).toHaveLength(1);
  });

  it("honours activity concurrency and keeps AsyncLocalStorage contexts separate", async () => {
    let next = 0, active = 0, maxActive = 0;
    const seen: number[] = [];
    const { server } = await setup({ worker: { pollActivityTask: async (_request, context) => next < 6 ? activityTask({ seq: BigInt(++next), taskToken: new Uint8Array([next]) }) : onceTask({})(_request, context) } }, { maxConcurrentActivities: 2, activities: { double: async () => { ++active; maxActive = Math.max(maxActive, active); const seq = activityContext().seq; await delay(30); expect(activityContext().seq).toBe(seq); seen.push(seq); --active; return seq; } } });
    await expect.poll(() => server.requests("CompleteActivityTask").length).toBe(6);
    expect(maxActive).toBe(2);
    expect(seen.sort()).toEqual([1, 2, 3, 4, 5, 6]);
  });

  it("honours workflow concurrency through the completion RPC", async () => {
    const events = await history();
    let next = 0, active = 0, peak = 0;
    const { server } = await setup({ worker: {
      pollWorkflowTask: async (_request, context) => {
        if (next === 6) return onceTask({})(_request, context);
        ++active;
        peak = Math.max(peak, active);
        return { taskToken: new Uint8Array([++next]), runId: `r${next}`, workflowType: "singleActivity", history: events };
      },
      completeWorkflowTask: async () => { await delay(40); --active; return {}; },
    } }, { maxConcurrentWorkflowTasks: 2 });
    await expect.poll(() => server.requests("CompleteWorkflowTask").length).toBe(6);
    expect(peak).toBe(2);
  });

  it("drops stale completion without reporting an activity failure", async () => {
    const logs: Record<string, unknown>[] = [];
    const { server } = await setup({ worker: { pollActivityTask: onceTask(activityTask()), completeActivityTask: () => { throw new ConnectError("stale", Code.FailedPrecondition); } } }, { logger: (entry) => logs.push(entry), activities: { double: () => 42 } });
    await expect.poll(() => logs.length).toBeGreaterThan(0);
    expect(server.requests("FailActivityTask")).toHaveLength(0);
    expect(logs.some((entry) => JSON.stringify(entry).includes("stale"))).toBe(true);
  });

  it("backs off poll errors before retrying", async () => {
    let polls = 0;
    const { server } = await setup({ worker: { pollActivityTask: (_request, context) => { if (++polls <= 2) throw new ConnectError("unavailable", Code.Unavailable); return onceTask(activityTask())(_request, context); } } }, { activities: { double: () => 42 } });
    await expect.poll(() => server.requests("CompleteActivityTask").length).toBeGreaterThan(0);
    const calls = server.calls.filter((call) => call.method === "PollActivityTask");
    expect(calls[1]!.at - calls[0]!.at).toBeGreaterThanOrEqual(45);
    expect(calls[2]!.at - calls[1]!.at).toBeGreaterThanOrEqual(90);
  });

  it("lets active work finish during shutdown and cancels outstanding polls", async () => {
    let started = false;
    const { server, worker, running } = await setup({ worker: { pollActivityTask: onceTask(activityTask()) } }, { activities: { double: async () => { started = true; await delay(80); return 42; } } });
    await expect.poll(() => started).toBe(true);
    await worker.shutdown(1_000);
    await running;
    expect(server.requests("CompleteActivityTask")).toHaveLength(1);
    const count = server.calls.length;
    await delay(40);
    expect(server.calls).toHaveLength(count);
  });

  it("aborts remaining work after shutdown grace without waiting forever", async () => {
    let signal: AbortSignal | undefined;
    const { worker, running } = await setup({ worker: { pollActivityTask: onceTask(activityTask()) } }, { activities: { double: async () => { signal = activityContext().signal; return new Promise(() => {}); } } });
    await expect.poll(() => !!signal).toBe(true);
    const start = performance.now();
    await worker.shutdown(30);
    await running;
    expect(signal?.aborted).toBe(true);
    expect(performance.now() - start).toBeLessThan(500);
  });

  it("round trips a workflow task with commands, token, identity and build id", async () => {
    const { server } = await setup({ worker: { pollWorkflowTask: onceTask({ taskToken: new Uint8Array([7]), runId: "r", workflowType: "singleActivity", history: await history() }) } });
    await expect.poll(() => server.requests<CompleteWorkflowTaskRequest>("CompleteWorkflowTask").length).toBe(1);
    const request = server.requests<CompleteWorkflowTaskRequest>("CompleteWorkflowTask")[0]!;
    expect(request).toMatchObject({ taskToken: new Uint8Array([7]), identity: "test-worker", buildId: "test-build" });
    expect(request.commands[0]!.attributes.case).toBe("completeRun");
    if (request.commands[0]!.attributes.case === "completeRun") expect(value(request.commands[0]!.attributes.value.result)).toEqual({ doubled: 42 });
  });

  it("reports replay mismatch as HISTORY_MISMATCH", async () => {
    const events = await history();
    const scheduled = events[4]!;
    if (scheduled.attributes.case === "activityScheduled") scheduled.attributes.value.activityType = "changed";
    const { server } = await setup({ worker: { pollWorkflowTask: onceTask({ taskToken: new Uint8Array([7]), runId: "r", workflowType: "singleActivity", history: events }) } });
    await expect.poll(() => server.requests<FailWorkflowTaskRequest>("FailWorkflowTask").length).toBe(1);
    const failed = server.requests<FailWorkflowTaskRequest>("FailWorkflowTask")[0]!;
    expect(failed).toMatchObject({ cause: TaskFailedCause.HISTORY_MISMATCH, failure: { type: "HistoryMismatchError", message: "history mismatch at event 5: history has ActivityScheduled(seq=1, type=changed), code emitted ScheduleActivity(seq=1, type=double)" } });
    expect(value(failed.failure!.details)).toEqual({ $capstan: { kind: "mismatch", eventId: 5 } });
  });

  it.each(["throwsApplication", "forbidden"])("classifies workflow outcome for %s", async (workflowType) => {
    const events = (await history()).slice(0, 3);
    if (events[0]!.attributes.case === "runStarted") events[0]!.attributes.value.workflowType = workflowType;
    const { server } = await setup({ worker: { pollWorkflowTask: onceTask({ taskToken: new Uint8Array([7]), runId: "r", workflowType, history: events }) } });
    if (workflowType === "forbidden") {
      await expect.poll(() => server.requests<FailWorkflowTaskRequest>("FailWorkflowTask").length).toBe(1);
      expect(server.requests<FailWorkflowTaskRequest>("FailWorkflowTask")[0]!.cause).toBe(TaskFailedCause.SDK_ERROR);
    } else {
      await expect.poll(() => server.requests<CompleteWorkflowTaskRequest>("CompleteWorkflowTask").length).toBe(1);
      expect(server.requests<CompleteWorkflowTaskRequest>("CompleteWorkflowTask")[0]!.commands[0]!.attributes).toMatchObject({ case: "failRun", value: { failure: { type: "UserFailure" } } });
    }
  });
});
