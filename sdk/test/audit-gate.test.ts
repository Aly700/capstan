import { fileURLToPath } from "node:url";
import { fromJson } from "@bufbuild/protobuf";
import { beforeAll, describe, expect, it } from "vitest";
import { HistoryEventSchema } from "../src/gen/capstan/v1/capstan_pb.ts";
import { replay } from "../src/replay/runtime.ts";
import { bundleWorkflows } from "../src/sandbox/bundle.ts";
import { decode } from "../src/internal/payload.ts";
import { FixtureBuilder, payload } from "./fixtures/build.ts";

let bundle: Awaited<ReturnType<typeof bundleWorkflows>>;
beforeAll(async () => { bundle = await bundleWorkflows(fileURLToPath(new URL("./agent-fixtures/workflows.ts", import.meta.url))); });
const proposal = { toolName: "fs.write", arguments: { path: "report.txt" }, riskTier: "HIGH" };
function decided(approved: boolean) {
  const h = new FixtureBuilder("toolStep", null).task()
    .scheduleActivity(1, "capstan.gate.decide", proposal, { startToCloseTimeout: "30s" })
    .completeActivity(1, { effect: approved ? "REQUIRE_APPROVAL" : "ALLOW", decisionId: "audit-decision", approvalId: approved ? "audit-approval" : null, arguments: proposal.arguments }).task();
  if (approved) h.command("approvalRequested", { seq: "2", approvalId: "audit-approval", gateDecisionId: "audit-decision", source: "APPROVAL_SOURCE_GATE", tool: proposal.toolName, arguments: payload(proposal.arguments)!, timeout: "3600s" })
    .external("approvalResolved", { seq: "2", approvalId: "audit-approval", outcome: "APPROVAL_OUTCOME_APPROVED", resolver: "audit" }).task();
  return h;
}
function execute(h: FixtureBuilder, workflowType: string) {
  return replay({ bundle, runId: "audit-d29", workflowType, history: h.history.map((event) => fromJson(HistoryEventSchema, event)) });
}

describe("audit D29 exact Gate proposal binding", () => {
  it.each([false, true])("dispatches the recorded name and exact argument bytes after approved=%s", async (approved) => {
    const commands = await execute(decided(approved), "toolStep");
    expect(commands).toHaveLength(1);
    const command = commands[0]!.attributes;
    expect(command.case).toBe("scheduleActivity");
    if (command.case !== "scheduleActivity") throw new Error("tool not dispatched");
    expect(command.value.activityType).toBe(proposal.toolName);
    expect(decode(command.value.input)).toEqual(proposal.arguments);
    expect(Buffer.from(command.value.input!.data).toString()).toBe(JSON.stringify(proposal.arguments));
  });
  for (const workflowType of ["changedToolName", "changedToolArgs", "caughtChangedToolArgs"]) {
    it.each([false, true])(`blocks ${workflowType} before any dispatch after approved=%s`, async (approved) => {
      await expect(execute(decided(approved), workflowType)).rejects.toMatchObject({ name: "HistoryMismatchError" });
    });
  }
});
