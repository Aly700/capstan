import { activity } from "../../src/workflow/index.ts";

/** Every Date observation a workflow could make from the activation clock. */
export async function calendar(): Promise<unknown> {
  const now = new Date();
  const midnight = new Date(now.getTime());
  midnight.setHours(0, 0, 0, 0);
  const observed = {
    iso: now.toISOString(),
    parts: [now.getFullYear(), now.getMonth(), now.getDate(), now.getDay(), now.getHours(), now.getMinutes()],
    offset: now.getTimezoneOffset(),
    text: now.toString(),
    date: now.toDateString(),
    time: now.toTimeString(),
    midnight: midnight.getTime(),
    offsetFree: Date.parse("2026-03-08T23:30"),
    components: new Date(2026, 2, 8, 23, 30).getTime(),
    dateOnly: Date.parse("2026-03-08"),
    tomorrow: new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1).getTime(),
  };
  return activity("record", observed, { startToCloseTimeout: "10s" });
}
