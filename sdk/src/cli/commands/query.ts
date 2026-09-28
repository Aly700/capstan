import { toJson } from "@bufbuild/protobuf";
import { timestampDate } from "@bufbuild/protobuf/wkt";
import { Code, ConnectError, type Client } from "@connectrpc/connect";
import { ClientService, HistoryEventSchema, EventType, type HistoryEvent } from "../../gen/capstan/v1/capstan_pb.ts";

export async function fetchHistory(client: Client<typeof ClientService>, runId: string): Promise<HistoryEvent[]> {
  const events: HistoryEvent[] = [];
  let afterEventId = 0n;
  for (;;) {
    const page = await client.getHistory({ runId, afterEventId, pageSize: 5_000 });
    events.push(...page.events);
    if (!page.more) return events;
    const last = page.events.at(-1)?.eventId;
    if (last === undefined || last <= afterEventId) throw new ConnectError("history pagination did not advance", Code.DataLoss);
    afterEventId = last;
  }
}

export function formatHistory(events: HistoryEvent[], json: boolean): string {
  if (json) return JSON.stringify(events.map((event) => toJson(HistoryEventSchema, event)), null, 2);
  return events.map((event) => {
    const attrs = event.attributes.value;
    const fields: string[] = [];
    if (attrs) for (const key of ["seq", "activityType", "name", "markerId", "approvalId", "workflowType", "newRunId", "reason"] as const) {
      if (key in attrs) fields.push(`${key}=${String((attrs as unknown as Record<string, unknown>)[key])}`);
    }
    return `${event.eventId}\t${event.time ? timestampDate(event.time).toISOString() : "-"}\t${EventType[event.type]}${fields.length ? `\t${fields.join(" ")}` : ""}`;
  }).join("\n");
}
