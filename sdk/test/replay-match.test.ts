import { fromJson } from "@bufbuild/protobuf";
import type { JsonValue } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { CommandSchema, HistoryEventSchema } from "../src/gen/capstan/v1/capstan_pb.ts";
import { matchCommands } from "../src/replay/match.ts";

const pairs = [
  ["scheduleActivity", "activityScheduled", { seq: "1", activityType: "double" }, ["seq", "activityType"]],
  ["requestActivityCancel", "activityCancelRequested", { seq: "1" }, ["seq"]],
  ["startTimer", "timerStarted", { seq: "1" }, ["seq"]],
  ["cancelTimer", "timerCancelled", { seq: "1" }, ["seq"]],
  ["recordMarker", "markerRecorded", { seq: "1", name: "patch", markerId: "p" }, ["seq", "name", "markerId"]],
  ["requestApproval", "approvalRequested", { seq: "1", approvalId: "a", source: "APPROVAL_SOURCE_HUMAN" }, ["seq", "approvalId", "source"]],
  ["completeRun", "runCompleted", {}, []], ["failRun", "runFailed", {}, []],
  ["cancelRun", "runCancelled", {}, []], ["continueAsNew", "runContinuedAsNew", {}, []],
] as const;
function command(kind: string, attrs: unknown) {
  return fromJson(CommandSchema, { [kind]: attrs } as JsonValue);
}
function event(kind: string, attrs: unknown) {
  return fromJson(HistoryEventSchema, { eventId: "5", [kind]: attrs } as JsonValue);
}

describe("conformance command comparison", () => {
  it.each([
    ["scheduleActivity", { seq: "1", activityType: "revisedStep", input: { contentType: "application/json", data: "MA==" } }, "activityScheduled", { seq: "1", activityType: "step", input: { contentType: "application/json", data: "MA==" } }, "ActivityScheduled(seq=1, type=step)", "ScheduleActivity(seq=1, type=revisedStep)"],
    ["startTimer", { seq: "2", fireAfter: "99s" }, "timerStarted", { seq: "1", fireAfter: "1s" }, "TimerStarted(seq=1)", "StartTimer(seq=2)"],
    ["cancelTimer", { seq: "2" }, "timerCancelled", { seq: "1" }, "TimerCancelled(seq=1)", "CancelTimer(seq=2)"],
    ["recordMarker", { seq: "1", name: "uuid" }, "markerRecorded", { seq: "1", name: "side_effect" }, "MarkerRecorded(seq=1, name=side_effect)", "RecordMarker(seq=1, name=uuid)"],
    ["recordMarker", { seq: "1", name: "patch", markerId: "new" }, "markerRecorded", { seq: "1", name: "patch", markerId: "old" }, "MarkerRecorded(seq=1, name=patch, markerId=old)", "RecordMarker(seq=1, name=patch, markerId=new)"],
    ["requestApproval", { seq: "1", approvalId: "new", source: "APPROVAL_SOURCE_HUMAN" }, "approvalRequested", { seq: "1", approvalId: "old", source: "APPROVAL_SOURCE_HUMAN" }, "ApprovalRequested(seq=1, approvalId=old, source=HUMAN)", "RequestApproval(seq=1, approvalId=new, source=HUMAN)"],
    ["requestApproval", { seq: "1", approvalId: "same", source: "APPROVAL_SOURCE_GATE" }, "approvalRequested", { seq: "1", approvalId: "same", source: "APPROVAL_SOURCE_HUMAN" }, "ApprovalRequested(seq=1, approvalId=same, source=HUMAN)", "RequestApproval(seq=1, approvalId=same, source=GATE)"],
    ["requestActivityCancel", { seq: "2" }, "activityCancelRequested", { seq: "1" }, "ActivityCancelRequested(seq=1)", "RequestActivityCancel(seq=2)"],
    ["completeRun", { result: { contentType: "application/json", data: "MA==" } }, "runCancelled", {}, "RunCancelled", "CompleteRun"],
  ] as const)("describes %s using only compared identity fields", (cmd, current, ev, recorded, expected, got) => {
    let failure: unknown;
    try { matchCommands([command(cmd, current)], [event(ev, recorded)], 4); } catch (error) { failure = error; }
    expect(failure).toMatchObject({ name: "HistoryMismatchError", eventId: 5, message: `history mismatch at event 5: history has ${expected}, code emitted ${got}` });
  });
  for (const [cmd, ev, attrs, fields] of pairs) {
    for (const field of fields) {
      it(`${cmd} compares ${field} and names the first recorded event`, () => {
        const changed = { ...attrs, [field]: field === "seq" ? "2" : field === "source" ? "APPROVAL_SOURCE_GATE" : "changed" };
        expect(() => matchCommands([command(cmd, changed)], [event(ev, attrs)], 4)).toThrow(/event 5/);
      });
    }
  }
  it("ignores activity input, queue, retry and all timeouts", () => {
    const a = { seq: "1", activityType: "double", taskQueue: "old", startToCloseTimeout: "1s", input: {contentType:"application/json",data:"MQ=="} };
    const b = { ...a, taskQueue: "new", startToCloseTimeout: "8s", input: {contentType:"application/json",data:"Mg=="}, retryPolicy:{maximumAttempts:4} };
    expect(() => matchCommands([command("scheduleActivity", b)], [event("activityScheduled", a)], 4)).not.toThrow();
  });
  it("ignores closing results and timer durations", () => {
    expect(() => matchCommands([command("startTimer", {seq:"1",fireAfter:"2s"})], [event("timerStarted",{seq:"1",fireAfter:"1s"})], 4)).not.toThrow();
    expect(() => matchCommands([command("completeRun", {result:{contentType:"application/json",data:"Mg=="}})], [event("runCompleted", {})], 4)).not.toThrow();
  });
  it("rejects a removed command", () => {
    expect(() => matchCommands([], [event("timerStarted", {seq:"1"})], 4)).toThrow("history mismatch at event 5: history has TimerStarted(seq=1), code emitted <none>");
  });
  it("pins an extra command to the completed task when no recorded event exists", () => {
    expect(() => matchCommands([command("startTimer",{seq:"1"})], [], 4)).toThrow("history mismatch at event 4: history has <none>, code emitted StartTimer(seq=1)");
  });
  it("rejects a changed command type", () => {
    expect(() => matchCommands([command("startTimer", {seq:"1"})], [event("activityScheduled",{seq:"1",activityType:"double"})], 4)).toThrow(/event 5/);
  });
});
