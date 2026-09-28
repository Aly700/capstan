import { defineSignal, model, nextSignal } from "@capstan/sdk/workflow";

// One real model call, then a wait. The wait forces a later activation that replays the call
// from history on a different worker, so the ledger shows whether replay bills again.
export async function costAgent() {
  const answer = await model({ model: "claude-haiku-4-5-20251001", prompt: "Reply with the single word OK.", maxTokens: 16, estimateUsd: 0.01 });
  await nextSignal(defineSignal<null>("finish"));
  return { model: answer.model, inputTokens: answer.inputTokens, outputTokens: answer.outputTokens, costUsd: answer.costUsd };
}
