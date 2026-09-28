import { activity, tool } from "../../src/workflow/index.ts";

export function gated(input: { name: string; arguments: unknown }) {
  return tool(input.name, input.arguments, { riskTier: "HIGH", approvalTimeout: "5m", startToCloseTimeout: "10s" });
}
export function capacityProbe() {
  return activity("probe", null, { startToCloseTimeout: "5s" });
}
