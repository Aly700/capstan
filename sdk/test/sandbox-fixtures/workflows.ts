import { activity, ActivityFailure, ApplicationFailure, TimeoutFailure, workflowInfo, sideEffect, setHandler, defineSignal } from "@capstan/sdk/workflow";
import type * as activities from "./activities.ts";

export async function echo(input: unknown) { return input; }
export async function callActivity(input: unknown) {
  type Activities = typeof activities;
  const ignored: keyof Activities = "outside";
  void ignored;
  return activity("echo", input, { startToCloseTimeout: "1s" });
}
export async function catchFailure() {
  try { await activity("fail", null, { startToCloseTimeout: "1s" }); }
  catch (error) {
    if (!(error instanceof ActivityFailure)) throw new Error("wrong activity failure realm");
    if (!(error.cause instanceof TimeoutFailure)) throw new Error("wrong timeout failure realm");
    return { seq: error.seq, activityType: error.activityType, timeoutType: error.cause.timeoutType };
  }
}
export function customFailure() { throw new ApplicationFailure("no", { type: "Denied", details: { code: 7 } }); }
export function info() { return workflowInfo(); }
export function effect() { return sideEffect(() => ({ answer: 42 })); }
export function signalHandler() { setHandler(defineSignal("x"), async () => { await Promise.resolve(); globalThis.handlerRan = true; }); }
declare global { var handlerRan: boolean; }
