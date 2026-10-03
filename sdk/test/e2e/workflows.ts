// Workflows for the end-to-end suites. Not part of the conformance corpus.
import { activity, human, model, random, sleep, uuid } from "../../src/workflow/index.ts";

export interface Rung { index: number; value: number; tag: string; dice: number }

/** Every rung records two replay-sensitive decisions (uuid marker, seeded random) in its activity input. */
export async function ladder(input: { steps: number }): Promise<Rung[]> {
  const rungs: Rung[] = [];
  for (let index = 0; index < input.steps; index++) {
    const tag = uuid();
    const dice = random();
    const value = await activity<number>("step", { index, tag, dice }, { startToCloseTimeout: "30s" });
    rungs.push({ index, value, tag, dice });
    if (index % 2 === 1) await sleep("100ms");
  }
  return rungs;
}

export async function deposit(input: { amount: number }): Promise<unknown> {
  return activity("deposit", input, { startToCloseTimeout: "3s" });
}

export async function ask(input: { prompt: string }): Promise<unknown> {
  const reply = await model({ prompt: input.prompt, maxTokens: 16 });
  const after = await activity<string>("hang", null, { startToCloseTimeout: "5s" });
  return { text: reply.text, costUsd: reply.costUsd, after };
}

export async function shipGate(): Promise<unknown> {
  const decision = await human("Ship?", { timeout: "10m" });
  if (decision.outcome !== "approved") return { shipped: false, decision };
  const receipt = await activity<string>("hang", null, { startToCloseTimeout: "10s" });
  return { shipped: true, decision, receipt };
}
