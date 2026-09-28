import { defineSignal, nextSignal } from "@capstan/sdk/workflow";

export async function idleWait() {
  return nextSignal(defineSignal<{ approved: boolean }>("decision"));
}
