import { activity, defineSignal, nextSignal } from "@capstan/sdk/workflow";
export async function changedWorkflow() {
  const value = await activity<number>("revisedStep", 0, { startToCloseTimeout: "30s" });
  await nextSignal(defineSignal("continue"));
  return { value };
}
