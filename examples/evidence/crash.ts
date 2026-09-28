import { activity, sleep } from "@capstan/sdk/workflow";

export async function crashSurvivor(): Promise<number[]> {
  const steps: number[] = [];
  for (let index = 0; index < 4; index++) {
    steps.push(await activity<number>("step", index, { startToCloseTimeout: "30s" }));
    if (index < 3) await sleep("3s");
  }
  return steps;
}
