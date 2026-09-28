import { activity } from "@capstan/sdk/workflow";

// Used by the browser check; inputs and results are real persisted payloads.
export async function viewerCompleted(input: { n: number; note?: string }) {
  return { value: await activity<number>("step", input.n, { startToCloseTimeout: "30s" }), note: input.note };
}
