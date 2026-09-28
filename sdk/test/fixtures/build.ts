import { fromJson, toJson } from "@bufbuild/protobuf";
import type { JsonObject, JsonValue } from "@bufbuild/protobuf";
import { CommandSchema, HistoryEventSchema, PayloadSchema } from "../../src/gen/capstan/v1/capstan_pb.ts";
import { encode } from "../../src/internal/payload.ts";

export interface Fixture {
  name: string;
  workflow: string;
  description: string;
  history: JsonObject[];
  expect: { commands: JsonObject[] } | { mismatch: { eventId: number } };
}

export function payload(value: unknown): JsonValue | undefined {
  const encoded = encode(value);
  return encoded ? toJson(PayloadSchema, encoded) : undefined;
}

function withPayload(key: string, value: unknown): JsonObject {
  const result = payload(value);
  return result === undefined ? {} : { [key]: result };
}

export const commands = {
  scheduleActivity(seq: number, activityType: string, input: unknown, extra: JsonObject = {}): JsonObject {
    return { scheduleActivity: { seq: String(seq), activityType, ...withPayload("input", input), startToCloseTimeout: "10s", ...extra } };
  },
  startTimer: (seq: number, fireAfter = "1s"): JsonObject => ({ startTimer: { seq: String(seq), fireAfter } }),
  cancelTimer: (seq: number): JsonObject => ({ cancelTimer: { seq: String(seq) } }),
  recordMarker: (seq: number, name: string, markerId: string, details: unknown): JsonObject => ({ recordMarker: { seq: String(seq), name, ...(markerId ? { markerId } : {}), ...withPayload("details", details) } }),
  requestApproval: (seq: number, approvalId: string, source = "APPROVAL_SOURCE_HUMAN"): JsonObject => ({ requestApproval: { seq: String(seq), approvalId, source, prompt: "Ship?" } }),
  completeRun: (result?: unknown): JsonObject => ({ completeRun: withPayload("result", result) }),
  cancelRun: (): JsonObject => ({ cancelRun: {} }),
  continueAsNew: (input: unknown, workflowType = "", taskQueue = ""): JsonObject => ({ continueAsNew: { ...withPayload("input", input), ...(workflowType ? { workflowType } : {}), ...(taskQueue ? { taskQueue } : {}) } }),
};

const baseTime = Date.parse("2026-09-28T12:00:00Z");

/** Builds histories independently of replay, including links the server would write. */
export class FixtureBuilder {
  readonly history: JsonObject[] = [];
  private milliseconds = baseTime;
  private scheduled = 0;
  private started = 0;
  private completed = 0;
  private open = false;
  private activityIds = new Map<number, number>();
  private timerIds = new Map<number, number>();
  private approvalIds = new Map<number, number>();

  constructor(readonly workflow: string, input: unknown) {
    this.append("runStarted", { workflowType: workflow, taskQueue: "q", taskTimeout: "10s", ...withPayload("input", input) });
  }

  private append(kind: string, attributes: JsonObject): number {
    const eventId = this.history.length + 1;
    this.history.push({ eventId: String(eventId), type: `EVENT_TYPE_${kind.replace(/[A-Z]/g, (letter) => `_${letter}`).toUpperCase()}`, time: new Date(this.milliseconds).toISOString(), [kind]: attributes });
    return eventId;
  }

  private finish(): void {
    if (!this.open || this.completed) return;
    this.completed = this.append("taskCompleted", { scheduledEventId: String(this.scheduled), startedEventId: String(this.started), identity: "w1", buildId: "b1" });
  }

  task(time?: string, attempt = 1): this {
    this.finish();
    if (time) this.milliseconds = Date.parse(time);
    else this.milliseconds += 1_000;
    this.scheduled = this.append("taskScheduled", { taskQueue: "q", startToCloseTimeout: "10s", attempt });
    this.started = this.append("taskStarted", { scheduledEventId: String(this.scheduled), identity: "w1" });
    this.completed = 0;
    this.open = true;
    return this;
  }

  command(kind: string, attributes: JsonObject): this {
    if (!this.open) throw new Error("a command needs an active task");
    this.finish();
    this.append(kind, { ...attributes, taskCompletedEventId: String(this.completed) });
    return this;
  }

  external(kind: string, attributes: JsonObject): this {
    this.finish();
    this.open = false;
    this.milliseconds += 1_000;
    this.append(kind, attributes);
    return this;
  }

  scheduleActivity(seq: number, activityType: string, input: unknown, extra: JsonObject = {}): this {
    this.command("activityScheduled", { seq: String(seq), activityType, taskQueue: "q", startToCloseTimeout: "10s", ...withPayload("input", input), ...extra });
    this.activityIds.set(seq, this.history.length);
    return this;
  }

  completeActivity(seq: number, result: unknown): this {
    const scheduled = this.activityIds.get(seq);
    if (!scheduled) throw new Error(`unknown activity seq ${seq}`);
    return this.external("activityCompleted", { seq: String(seq), scheduledEventId: String(scheduled), ...withPayload("result", result), attempt: 1, identity: "w1" });
  }

  failActivity(seq: number, message: string, type = "ApplicationFailure"): this {
    const scheduled = this.activityIds.get(seq);
    if (!scheduled) throw new Error(`unknown activity seq ${seq}`);
    return this.external("activityFailed", { seq: String(seq), scheduledEventId: String(scheduled), failure: { message, type }, attempt: 1, identity: "w1" });
  }

  startTimer(seq: number, fireAfter = "1s"): this {
    this.command("timerStarted", { seq: String(seq), fireAfter });
    this.timerIds.set(seq, this.history.length);
    return this;
  }

  fireTimer(seq: number): this {
    const started = this.timerIds.get(seq);
    if (!started) throw new Error(`unknown timer seq ${seq}`);
    return this.external("timerFired", { seq: String(seq), startedEventId: String(started) });
  }

  cancelTimer(seq: number): this {
    const started = this.timerIds.get(seq);
    if (!started) throw new Error(`unknown timer seq ${seq}`);
    return this.command("timerCancelled", { seq: String(seq), startedEventId: String(started) });
  }

  signal(name: string, input: unknown): this {
    return this.external("signalReceived", { name, ...withPayload("input", input), identity: "client" });
  }

  marker(seq: number, name: string, markerId: string, details: unknown): this {
    return this.command("markerRecorded", { seq: String(seq), name, markerId, ...withPayload("details", details) });
  }

  requestApproval(seq: number, approvalId: string, source = "APPROVAL_SOURCE_HUMAN"): this {
    this.command("approvalRequested", { seq: String(seq), approvalId, source, prompt: "Ship?" });
    this.approvalIds.set(seq, this.history.length);
    return this;
  }

  resolveApproval(seq: number, approvalId: string): this {
    const requested = this.approvalIds.get(seq);
    if (!requested) throw new Error(`unknown approval seq ${seq}`);
    return this.external("approvalResolved", { seq: String(seq), requestedEventId: String(requested), approvalId, outcome: "APPROVAL_OUTCOME_APPROVED", choice: "ship", resolver: "owner", note: "ok" });
  }

  discard(cause = "TASK_FAILED_CAUSE_SDK_ERROR"): this {
    if (!this.open || this.completed) throw new Error("discard requires an uncompleted task");
    this.append("taskFailed", { scheduledEventId: String(this.scheduled), startedEventId: String(this.started), cause, failure: { type: "Error", message: "discarded" }, identity: "w1" });
    this.open = false;
    return this;
  }

  timeout(): this {
    if (!this.open || this.completed) throw new Error("timeout requires an uncompleted task");
    this.append("taskTimedOut", { scheduledEventId: String(this.scheduled), startedEventId: String(this.started) });
    this.open = false;
    return this;
  }

  expectCommands(...expected: JsonObject[]): Fixture {
    return this.fixture({ commands: expected });
  }

  expectMismatch(eventId: number): Fixture {
    return this.fixture({ mismatch: { eventId } });
  }

  private fixture(expect: Fixture["expect"]): Fixture {
    if (this.history.at(-1)?.type !== "EVENT_TYPE_TASK_STARTED") throw new Error("fixture history must end with TaskStarted");
    for (const event of this.history) fromJson(HistoryEventSchema, event);
    if ("commands" in expect) for (const command of expect.commands) fromJson(CommandSchema, command);
    return { name: this.workflow, workflow: this.workflow, description: this.workflow, history: structuredClone(this.history), expect };
  }
}

export function run(workflow: string, input: unknown = undefined): FixtureBuilder {
  return new FixtureBuilder(workflow, input);
}
