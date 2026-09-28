import { activity, human, model, tool, uuid, type ApprovalRequest, type WorkflowRuntime } from "../../src/workflow/index.ts";

function approval(request: ApprovalRequest) {
  return (globalThis as Record<symbol, WorkflowRuntime>)[Symbol.for("capstan.workflow.runtime")]!.requestApproval(request);
}

export async function modelStep() {
  const result = await model({ prompt: "Hello" });
  await activity("after", null, { startToCloseTimeout: "1s" });
  return result;
}
export async function plainModel() {
  const result = await activity("capstan.model", { prompt: "Hello" }, { startToCloseTimeout: "5m" });
  await activity("after", null, { startToCloseTimeout: "1s" });
  return result;
}
export function toolStep() {
  return tool("fs.write", { path: "report.txt" }, { riskTier: "HIGH", approvalTimeout: "1h", startToCloseTimeout: "2m" });
}
export async function plainTool() {
  const decision = await activity<{ effect: string; decisionId: string; approvalId: string | null }>(
    "capstan.gate.decide", { toolName: "fs.write", arguments: { path: "report.txt" }, riskTier: "HIGH" }, { startToCloseTimeout: "30s" },
  );
  if (decision.effect === "DENY") return { allowed: false, reason: "denied", decisionId: decision.decisionId };
  let approvedBy: string | undefined;
  if (decision.effect === "REQUIRE_APPROVAL") {
    const resolution = await approval({ source: "gate", approvalId: decision.approvalId!, gateDecisionId: decision.decisionId, tool: "fs.write", arguments: { path: "report.txt" }, timeout: "1h" });
    if (resolution.outcome !== "approved") return { allowed: false, reason: resolution.outcome === "denied" ? "rejected" : "expired", decisionId: decision.decisionId, resolver: resolution.resolver };
    approvedBy = resolution.resolver;
  }
  const value = await activity("fs.write", { path: "report.txt" }, { startToCloseTimeout: "2m" });
  return { allowed: true, value, decisionId: decision.decisionId, ...(approvedBy === undefined ? {} : { approvedBy }) };
}
export function humanStep() { return human("Ship?", { options: ["ship", "hold"], timeout: "1h" }); }
export function plainHuman() { return approval({ source: "human", approvalId: uuid(), prompt: "Ship?", options: ["ship", "hold"], timeout: "1h" }); }
export function mixedSteps() { return Promise.all([model({ prompt: "Hello" }), tool("read", {}), human("Ship?")]); }
export function defaults() { return tool("read", {}); }
export function mutableTool() {
  const args = { path: "approved.txt" };
  const result = tool("fs.write", args);
  args.path = "changed.txt";
  return result;
}
