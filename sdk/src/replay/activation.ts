import type { HistoryEvent } from "../gen/capstan/v1/capstan_pb.ts";
import { commandEventKinds } from "./match.ts";

export interface Activation {
  started: HistoryEvent;
  external: HistoryEvent[];
  recorded: HistoryEvent[];
  completed: HistoryEvent | undefined;
  attempt: number;
}
const externalKinds = new Set([
  "activityCompleted", "activityFailed", "activityTimedOut", "activityCancelled", "timerFired",
  "signalReceived", "approvalResolved", "runCancelRequested",
]);

/** One forward scan; discarded tasks neither execute code nor consume buffered events. */
export function activations(history: HistoryEvent[]): Activation[] {
  const result: Activation[] = [];
  const attempts = new Map<bigint, number>();
  let external: HistoryEvent[] = [];
  for (let i = 0; i < history.length; i++) {
    const event = history[i]!;
    const attrs = event.attributes;
    if (attrs.case === "taskScheduled") attempts.set(event.eventId, attrs.value.attempt);
    if (externalKinds.has(attrs.case ?? "")) external.push(event);
    if (attrs.case !== "taskStarted") continue;
    const next = history[i + 1];
    if (next?.attributes.case === "taskFailed" || next?.attributes.case === "taskTimedOut") continue;
    const completed = next?.attributes.case === "taskCompleted" ? next : undefined;
    const recorded: HistoryEvent[] = [];
    if (completed) {
      i++;
      for (;;) {
        const nextEvent = history[i + 1];
        if (!nextEvent) break;
        const isCommandEvent = commandEventKinds.has(nextEvent.attributes.case ?? "");
        if (!isCommandEvent) break;
        i++;
        recorded.push(nextEvent);
      }
    } else if (next) {
      throw new Error(`invalid history after TaskStarted event ${event.eventId}`);
    }
    result.push({ started: event, external, recorded, completed, attempt: attempts.get(attrs.value.scheduledEventId) ?? 1 });
    external = [];
  }
  return result;
}
