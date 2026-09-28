import { activity, defineSignal, nextSignal, patched } from "@capstan/sdk/workflow";
export async function changedWorkflow() {
  const value = await activity<number>(patched("revised-step-v2") ? "revisedStep" : "step", 0, { startToCloseTimeout: "30s" });
  await nextSignal(defineSignal("continue"));
  return { value };
}
