import type { Command, HistoryEvent } from "../gen/capstan/v1/capstan_pb.ts";
import { ApprovalSource } from "../gen/capstan/v1/capstan_pb.ts";
import { HistoryMismatchError } from "../types.ts";

/** Only these fields identify a step; payloads and execution options may change. */
const rules: Record<string, { event: string; fields: string[] }> = {
  scheduleActivity: { event: "activityScheduled", fields: ["seq", "activityType"] },
  requestActivityCancel: { event: "activityCancelRequested", fields: ["seq"] },
  startTimer: { event: "timerStarted", fields: ["seq"] },
  cancelTimer: { event: "timerCancelled", fields: ["seq"] },
  recordMarker: { event: "markerRecorded", fields: ["seq", "name", "markerId"] },
  requestApproval: { event: "approvalRequested", fields: ["seq", "approvalId", "source"] },
  completeRun: { event: "runCompleted", fields: [] },
  failRun: { event: "runFailed", fields: [] },
  cancelRun: { event: "runCancelled", fields: [] },
  continueAsNew: { event: "runContinuedAsNew", fields: [] },
};
export const commandEventKinds = new Set(Object.values(rules).map((rule) => rule.event));

function describe(kind: string | undefined, value: unknown): string {
  if (!kind) return "<none>";
  const rule = rules[kind] ?? Object.values(rules).find((candidate) => candidate.event === kind);
  const fields = value as Record<string, unknown> | undefined;
  const identity = (rule?.fields ?? []).filter((field) => field !== "markerId" || fields?.[field] !== "").map((field) => {
    const label = field === "activityType" ? "type" : field;
    const scalar = fields?.[field];
    return `${label}=${field === "source" ? ApprovalSource[Number(scalar)] ?? String(scalar) : String(scalar)}`;
  });
  return `${kind[0]!.toUpperCase()}${kind.slice(1)}${identity.length ? `(${identity.join(", ")})` : ""}`;
}

export function matchCommands(commands: Command[], events: HistoryEvent[], taskCompletedEventId: number): void {
  for (let i = 0; i < Math.max(commands.length, events.length); i++) {
    const cmd = commands[i]?.attributes;
    const ev = events[i]?.attributes;
    const rule = cmd?.case ? rules[cmd.case] : undefined;
    const a = cmd?.value as Record<string, unknown> | undefined;
    const b = ev?.value as Record<string, unknown> | undefined;
    if (!rule || !ev || rule.event !== ev.case || rule.fields.some((field) => a?.[field] !== b?.[field])) {
      // An added command has no event of its own: the completed task anchors the error.
      throw new HistoryMismatchError(Number(events[i]?.eventId ?? taskCompletedEventId), describe(ev?.case, ev?.value), describe(cmd?.case, cmd?.value));
    }
  }
}
