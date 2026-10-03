// Workflows referenced by the conformance fixtures. Each export here has a Go twin with the
// same name in internal/lab/scenarios. Keep both in step.

import {
  activity, condition, continueAsNew, defineSignal, deprecatePatch, nextSignal, now,
  patched, proxyActivities, random, setHandler, sideEffect, sleep, uuid,
} from "@capstan/sdk/workflow";
import type { WorkflowRuntime } from "@capstan/sdk/workflow";

type Activities = {
  double(n: number): number;
};

// fixtures/001-single-activity.json
export async function singleActivity(input: { n: number }): Promise<{ doubled: number }> {
  const acts = proxyActivities<Activities>({ startToCloseTimeout: "10s" });
  const doubled = await acts.double(input.n);
  return { doubled };
}

const options = { startToCloseTimeout: "10s" as const };

export async function parallelActivities(): Promise<number[]> {
  return Promise.all([activity<number>("double", 1, options), activity<number>("double", 2, options)]);
}

export async function retryInWorkflow(): Promise<number> {
  try { return await activity<number>("double", 1, options); }
  catch { return await activity<number>("double", 2, options); }
}

export async function sleeping(): Promise<string> {
  await sleep("1s");
  return "awake";
}

export async function raceActivityAndSleep(): Promise<unknown> {
  return Promise.race([activity("double", 21, options), sleep("1s").then(() => "timeout")]);
}

export async function immediateCondition(): Promise<boolean> {
  return condition(() => true);
}

export async function signalledCondition(input: { timeout?: "1s" }): Promise<boolean> {
  let ready = false;
  setHandler(defineSignal<boolean>("ready"), (value) => { ready = value; });
  return condition(() => ready, input.timeout);
}

export async function bufferedHandler(): Promise<number[]> {
  await activity("double", 1, options);
  const values: number[] = [];
  setHandler(defineSignal<number>("number"), (value) => { values.push(value); });
  return values;
}

export async function orderedHandler(): Promise<number[]> {
  const values: number[] = [];
  setHandler(defineSignal<number>("number"), (value) => { values.push(value); });
  await condition(() => values.length === 2);
  return values;
}

export async function twoSignals(): Promise<number[]> {
  const signal = defineSignal<number>("number");
  return [await nextSignal(signal), await nextSignal(signal)];
}

export async function recordedSideEffect(): Promise<unknown> {
  const value = sideEffect(() => { throw new Error("must use recorded side effect"); });
  await activity("double", 1, options);
  return value;
}

export async function recordedUuid(): Promise<string> {
  const value = uuid();
  await activity("double", 1, options);
  return value;
}

export async function patchBranch(): Promise<number> {
  return activity<number>(patched("v2") ? "newDouble" : "double", 21, options);
}

export async function deprecatedPatch(): Promise<number> {
  deprecatePatch("v2");
  return activity<number>("newDouble", 21, options);
}

export async function stableRandom(): Promise<number[]> {
  const first = random();
  await activity("double", 1, options);
  return [first, random()];
}

export async function activationTime(): Promise<number[]> {
  const first = now();
  await activity("double", 1, options);
  return [first, now()];
}

export async function continueCounting(input: { n: number }): Promise<never> {
  continueAsNew({ n: input.n + 1 });
}

export async function conditionThenSignal(): Promise<unknown> {
  let ready = false;
  setHandler(defineSignal<boolean>("ready"), (value) => { ready = value; });
  await condition(() => ready, "1s");
  return nextSignal(defineSignal("done"));
}

export async function renamedActivity(): Promise<unknown> {
  return activity("triple", 21, options);
}

export async function removedActivity(): Promise<string> { return "removed"; }

export async function markerChange(): Promise<unknown> {
  sideEffect(() => 1);
  return nextSignal(defineSignal("done"));
}

// These workflows bypass the agent API and exercise the frozen
// runtime approval contract directly, without implementing any agent primitive here.
function approvalRuntime(): WorkflowRuntime {
  return (globalThis as unknown as Record<symbol, WorkflowRuntime>)[Symbol.for("capstan.workflow.runtime")]!;
}

export async function approval(): Promise<unknown> {
  return approvalRuntime().requestApproval({ approvalId: "approval-1", source: "human", prompt: "Ship?" });
}

export async function observeActivityFailure(): Promise<unknown> {
  try { await activity("double", 1, options); return "unexpected"; }
  catch (error) {
    const failure = error as { type: string; timeoutType?: string; cause?: { type: string } };
    return { type: failure.type, ...(failure.timeoutType ? { timeoutType: failure.timeoutType } : {}), ...(failure.cause ? { cause: failure.cause.type } : {}) };
  }
}

export async function asyncSignalHandler(): Promise<number> {
  let value = 0;
  setHandler(defineSignal<number>("number"), async (input) => {
    await Promise.resolve();
    await Promise.resolve();
    value = input;
  });
  await condition(() => value !== 0);
  return value;
}

export async function nestedContinuations(): Promise<number> {
  async function nested(): Promise<number> {
    await Promise.resolve();
    return (await activity<number>("double", 21, options)) + 1;
  }
  const [value] = await Promise.all([nested()]);
  return value * 2;
}

export async function waitingOnly(): Promise<unknown> {
  return nextSignal(defineSignal("done"));
}

export async function newSideEffect(): Promise<unknown> {
  return sideEffect(() => ({ saved: 7 }));
}

export async function signalHandlerClock(): Promise<number[]> {
  let observed: number | undefined;
  setHandler(defineSignal("go"), () => { observed = now(); });
  await condition(() => observed !== undefined);
  return [observed!, now()];
}

export async function repeatedOldPatch(): Promise<boolean[]> {
  const first = patched("v2");
  await activity("double", 21, options);
  return [first, patched("v2")];
}
