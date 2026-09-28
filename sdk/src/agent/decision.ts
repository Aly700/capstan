import { HistoryMismatchError } from "../types.ts";

function canonical(value: unknown): string | undefined {
  return JSON.stringify(value, (_key, item: unknown) => {
    if (item === null || typeof item !== "object" || Array.isArray(item)) return item;
    const object = item as Record<string, unknown>;
    return Object.fromEntries(Object.keys(object).sort().map((key) => [key, object[key]]));
  });
}

/** Bind the result before delivery so user code cannot catch the mismatch. */
export function replayGateDecision(proposal: unknown, recordedProposal: unknown, result: unknown, seq: number, eventId: number): unknown {
  const current = proposal as { toolName?: string; arguments?: unknown } | undefined;
  const original = recordedProposal as { toolName?: string; arguments?: unknown } | undefined;
  if (typeof original?.toolName === "string" && current?.toolName !== original.toolName) {
    throw new HistoryMismatchError(eventId, `GateDecision(seq=${seq}, tool=${original.toolName})`, `Tool(seq=${seq}, tool=${current?.toolName ?? "<unknown>"}): tool changed since the Gate decided`);
  }
  // Pre-D29 results omitted arguments. Recover only from the original scheduled
  // proposal that the worker sent, never from the current workflow's arguments.
  const decided = result !== null && typeof result === "object" && !Object.hasOwn(result, "arguments") && original
    ? { ...result, arguments: original.arguments } : result as { arguments?: unknown } | undefined;
  if (canonical(current?.arguments) !== canonical(decided?.arguments)) {
    const identity = `seq=${seq}, tool=${current?.toolName ?? "<unknown>"}`;
    throw new HistoryMismatchError(eventId, `GateDecision(${identity})`, `Tool(${identity}): arguments changed since the Gate decided`);
  }
  return decided;
}
