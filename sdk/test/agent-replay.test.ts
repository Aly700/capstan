import { fileURLToPath } from "node:url";
import { fromJson, toJson, type JsonObject } from "@bufbuild/protobuf";
import { beforeAll, describe, expect, it } from "vitest";
import { CommandSchema, HistoryEventSchema } from "../src/gen/capstan/v1/capstan_pb.ts";
import { bundleWorkflows } from "../src/sandbox/bundle.ts";
import { replay } from "../src/replay/runtime.ts";
import { FixtureBuilder, payload } from "./fixtures/build.ts";

let bundle: Awaited<ReturnType<typeof bundleWorkflows>>;
beforeAll(async () => { bundle = await bundleWorkflows(fileURLToPath(new URL("./agent-fixtures/workflows.ts", import.meta.url))); });
const history = (workflow: string) => new FixtureBuilder(workflow, null).task();
async function run(h: FixtureBuilder, workflowType = h.workflow) {
  return (await replay({ bundle, runId: "agent-run", workflowType, history: h.history.map((e) => fromJson(HistoryEventSchema, e)) })).map((c) => toJson(CommandSchema, c));
}
const gateInput = { toolName: "fs.write", arguments: { path: "report.txt" }, riskTier: "HIGH" };
function decided(effect: string) {
  return history("toolStep").scheduleActivity(1, "capstan.gate.decide", gateInput, { startToCloseTimeout: "30s" })
    .completeActivity(1, { effect, decisionId: "decision-1", approvalId: effect === "REQUIRE_APPROVAL" ? "approval-1" : null, arguments: gateInput.arguments }).task();
}
function waiting() {
  return decided("REQUIRE_APPROVAL").command("approvalRequested", { seq: "2", approvalId: "approval-1", gateDecisionId: "decision-1", source: "APPROVAL_SOURCE_GATE", tool: "fs.write", arguments: payload(gateInput.arguments)!, timeout: "3600s" });
}
function resolved(outcome: string) {
  const h = waiting();
  return h.external("approvalResolved", { seq: "2", requestedEventId: String(h.history.length), approvalId: "approval-1", outcome: `APPROVAL_OUTCOME_${outcome}`, resolver: "reviewer", choice: "", note: "" }).task();
}
function result(value: unknown) { return [{ completeRun: { result: payload(value) } }]; }
async function matchesPlain(h: FixtureBuilder, plain: string) { expect(await run(h)).toEqual(await run(h, plain)); }

describe("agent replay uses the existing commands and sequence allocator", () => {
  it("model emits one activity and replays its recorded result", async () => {
    const h = history("modelStep");
    expect(await run(h)).toMatchObject([{ scheduleActivity: { seq: "1", activityType: "capstan.model", input: payload({ prompt: "Hello" }) } }]);
    await matchesPlain(h, "plainModel");
    const response = { text: "Recorded", model: "claude-sonnet-5", costUsd: 0.01 };
    h.scheduleActivity(1, "capstan.model", { prompt: "Hello" }).completeActivity(1, response).task();
    expect(await run(h)).toMatchObject([{ scheduleActivity: { seq: "2", activityType: "after" } }]);
    await matchesPlain(h, "plainModel");
    h.scheduleActivity(2, "after", null).completeActivity(2, null).task();
    expect(await run(h)).toEqual(result(response));
    h.command("runCompleted", { result: payload(response)! });
    expect(await run(h)).toEqual([]);
  });

  it("tool schedules Gate first, with no worker configuration in the payload", async () => {
    const h = history("toolStep");
    expect(await run(h)).toEqual([{ scheduleActivity: { seq: "1", activityType: "capstan.gate.decide", input: payload(gateInput), startToCloseTimeout: "30s" } }]);
    await matchesPlain(h, "plainTool");
  });
  it("ALLOW schedules exactly the registered activity at the next seq", async () => {
    const h = decided("ALLOW");
    expect(await run(h)).toEqual([{ scheduleActivity: { seq: "2", activityType: "fs.write", input: payload(gateInput.arguments), startToCloseTimeout: "120s" } }]);
    await matchesPlain(h, "plainTool");
    h.scheduleActivity(2, "fs.write", gateInput.arguments).completeActivity(2, "written").task();
    const value = { allowed: true, value: "written", decisionId: "decision-1" };
    expect(await run(h)).toEqual(result(value));
    h.command("runCompleted", { result: payload(value)! });
    expect(await run(h)).toEqual([]);
  });
  it("DENY completes without scheduling the tool", async () => {
    const h = decided("DENY");
    expect(await run(h)).toEqual(result({ allowed: false, reason: "denied", decisionId: "decision-1" }));
    await matchesPlain(h, "plainTool");
  });
  it("REQUIRE_APPROVAL allocates one GATE approval and yields no work while waiting", async () => {
    const h = decided("REQUIRE_APPROVAL");
    expect(await run(h)).toEqual([{ requestApproval: { seq: "2", approvalId: "approval-1", gateDecisionId: "decision-1", source: "APPROVAL_SOURCE_GATE", tool: "fs.write", arguments: payload(gateInput.arguments), timeout: "3600s" } }]);
    await matchesPlain(h, "plainTool");
    const idle = waiting().task();
    expect(await run(idle)).toEqual([]);
    await matchesPlain(idle, "plainTool");
    idle.task();
    expect(await run(idle)).toEqual([]);
  });
  it("approved Gate history resumes at the tool and preserves the resolver", async () => {
    const h = resolved("APPROVED");
    expect(await run(h)).toMatchObject([{ scheduleActivity: { seq: "3", activityType: "fs.write" } }]);
    await matchesPlain(h, "plainTool");
    h.scheduleActivity(3, "fs.write", gateInput.arguments).completeActivity(3, 42).task();
    const value = { allowed: true, value: 42, decisionId: "decision-1", approvedBy: "reviewer" };
    expect(await run(h)).toEqual(result(value));
    h.command("runCompleted", { result: payload(value)! });
    expect(await run(h)).toEqual([]);
  });
  it.each([["DENIED", "rejected"], ["EXPIRED", "expired"]])("maps %s without scheduling a tool", async (outcome, reason) => {
    const h = resolved(outcome!);
    expect(await run(h)).toEqual(result({ allowed: false, reason, decisionId: "decision-1", resolver: "reviewer" }));
    await matchesPlain(h, "plainTool");
  });

  it.each(["APPROVED", "DENIED", "EXPIRED"])("human records an id and maps %s, including choice and note", async (outcome) => {
    const h = history("humanStep");
    const fresh = await run(h);
    expect(fresh).toMatchObject([{ recordMarker: { seq: "1", name: "uuid" } }, { requestApproval: { seq: "2", source: "APPROVAL_SOURCE_HUMAN", prompt: "Ship?", options: ["ship", "hold"], timeout: "3600s" } }]);
    h.marker(1, "uuid", "", "human-1").command("approvalRequested", { seq: "2", source: "APPROVAL_SOURCE_HUMAN", approvalId: "human-1", prompt: "Ship?", options: ["ship", "hold"], timeout: "3600s" }).task();
    expect(await run(h)).toEqual([]);
    await matchesPlain(h, "plainHuman");
    h.external("approvalResolved", { seq: "2", approvalId: "human-1", outcome: `APPROVAL_OUTCOME_${outcome}`, choice: outcome === "APPROVED" ? "ship" : "", resolver: "owner", note: "reviewed" }).task();
    const value = { outcome: outcome.toLowerCase(), choice: outcome === "APPROVED" ? "ship" : "", resolver: "owner", note: "reviewed" };
    expect(await run(h)).toEqual(result(value));
    await matchesPlain(h, "plainHuman");
    h.command("runCompleted", { result: payload(value)! });
    expect(await run(h)).toEqual([]);
  });
  it("allocates mixed parallel calls synchronously in call order", async () => {
    expect(await run(history("mixedSteps"))).toMatchObject([
      { scheduleActivity: { seq: "1", activityType: "capstan.model" } },
      { scheduleActivity: { seq: "2", activityType: "capstan.gate.decide" } },
      { recordMarker: { seq: "3", name: "uuid" } },
      { requestApproval: { seq: "4", source: "APPROVAL_SOURCE_HUMAN" } },
    ]);
  });
  it("defaults risk to MEDIUM and tool timeout to five minutes", async () => {
    const h = history("defaults");
    expect(await run(h)).toMatchObject([{ scheduleActivity: { input: payload({ toolName: "read", arguments: {}, riskTier: "MEDIUM" }) } }]);
    h.scheduleActivity(1, "capstan.gate.decide", {}).completeActivity(1, { effect: "ALLOW", decisionId: "d", approvalId: null, arguments: {} }).task();
    expect(await run(h)).toMatchObject([{ scheduleActivity: { seq: "2", activityType: "read", startToCloseTimeout: "300s" } }]);
  });
  it("fails closed on an invalid recorded Gate effect", async () => {
    expect(await run(decided("UNKNOWN"))).toMatchObject([{ failRun: { failure: { type: "GateResponseInvalid", nonRetryable: true } } }]);
  });
  it.each(["ALLOW", "REQUIRE_APPROVAL"])("keeps the %s proposal unchanged if the caller mutates its arguments", async (effect) => {
    const h = history("mutableTool").scheduleActivity(1, "capstan.gate.decide", { toolName: "fs.write", arguments: { path: "approved.txt" }, riskTier: "MEDIUM" })
      .completeActivity(1, { effect, decisionId: "decision-1", approvalId: "approval-1", arguments: { path: "approved.txt" } }).task();
    if (effect === "ALLOW") expect(await run(h)).toMatchObject([{ scheduleActivity: { seq: "2", input: payload({ path: "approved.txt" }) } }]);
    else expect(await run(h)).toMatchObject([{ requestApproval: { seq: "2", arguments: payload({ path: "approved.txt" }) } }]);
  });
  it.each(["ALLOW", "REQUIRE_APPROVAL"])("blocks changed arguments after a recorded %s decision", async (effect) => {
    const h = effect === "ALLOW" ? decided(effect) : resolved("APPROVED");
    const eventId = Number(h.history.find((event) => event.activityCompleted)?.eventId);
    await expect(run(h, "changedToolArgs")).rejects.toMatchObject({
      name: "HistoryMismatchError", eventId,
      message: expect.stringMatching(/seq=1.*fs\.write.*arguments changed since the Gate decided/),
    });
    // A workflow catch cannot swallow the mismatch and emit a successful task.
    await expect(run(h, "caughtChangedToolArgs")).rejects.toMatchObject({ name: "HistoryMismatchError", eventId });
  });
  it.each(["ALLOW", "REQUIRE_APPROVAL"])("replays pre-D29 %s decisions using their recorded proposal", async (effect) => {
    const h = effect === "ALLOW" ? decided(effect) : resolved("APPROVED");
    const completed = h.history.find((event) => event.activityCompleted)!.activityCompleted as JsonObject;
    completed.result = payload({ effect, decisionId: "decision-1", approvalId: effect === "ALLOW" ? null : "approval-1" })!;
    expect(await run(h)).toMatchObject([{ scheduleActivity: { input: payload(gateInput.arguments), activityType: "fs.write" } }]);
    // Legacy recovery must never take the changed proposal from current code.
    await expect(run(h, "changedToolArgs")).rejects.toMatchObject({ name: "HistoryMismatchError" });
  });
  it.each(["ALLOW", "REQUIRE_APPROVAL"])("does not apply the %s decision to a renamed tool", async (effect) => {
    const h = effect === "ALLOW" ? decided(effect) : resolved("APPROVED");
    await expect(run(h, "changedToolName")).rejects.toMatchObject({ name: "HistoryMismatchError", message: expect.stringMatching(/seq=1.*fs\.write.*fs\.delete/) });
  });
  it.each(["ALLOW", "REQUIRE_APPROVAL"])("compares %s arguments canonically and emits the decided bytes", async (effect) => {
    const args = { config: { z: ["é", { y: null, x: true }], a: 1 }, path: "report.txt" };
    const h = history("reorderedToolArgs").scheduleActivity(1, "capstan.gate.decide", { toolName: "fs.write", arguments: args, riskTier: "MEDIUM" })
      .completeActivity(1, { effect, decisionId: "decision-1", approvalId: "approval-1", arguments: args }).task();
    if (effect === "REQUIRE_APPROVAL") {
      expect(await run(h)).toMatchObject([{ requestApproval: { arguments: payload(args) } }]);
      h.command("approvalRequested", { seq: "2", approvalId: "approval-1", source: "APPROVAL_SOURCE_GATE", gateDecisionId: "decision-1", tool: "fs.write", arguments: payload(args)! })
        .external("approvalResolved", { seq: "2", approvalId: "approval-1", outcome: "APPROVAL_OUTCOME_APPROVED", resolver: "owner" }).task();
    }
    const commands = await replay({ bundle, runId: "agent-run", history: h.history.map((event) => fromJson(HistoryEventSchema, event)) });
    const scheduled = commands[0]?.attributes;
    expect(scheduled?.case).toBe("scheduleActivity");
    if (scheduled?.case !== "scheduleActivity") throw new Error("tool was not scheduled");
    expect(Buffer.from(scheduled.value.input!.data).toString("utf8")).toBe(JSON.stringify(args));
    const seq = effect === "ALLOW" ? 2 : 3;
    h.scheduleActivity(seq, "fs.write", args).completeActivity(seq, "written").task();
    const completed = await run(h);
    h.command("runCompleted", (completed[0] as JsonObject).completeRun as JsonObject);
    expect(await run(h)).toEqual([]);
  });
});
