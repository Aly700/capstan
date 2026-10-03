// Replay must not depend on the worker's time zone: the same history replayed under
// UTC, Asia/Kolkata (+05:30), Pacific/Kiritimati (+14:00, a different calendar day) and
// America/St_Johns (-03:30) has to emit byte-identical commands, and those commands
// have to carry the UTC reading of the activation clock.
import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { describe, expect, it } from "vitest";

const exec = promisify(execFile);
const sdk = fileURLToPath(new URL("..", import.meta.url));
const script = fileURLToPath(new URL("./timezone/replay.ts", import.meta.url));
// 23:30 UTC on a Sunday: every zone east of +00:30 is already on Monday the 9th, and
// Newfoundland moved to daylight time (-02:30) that morning.
const at = "2026-03-08T23:30:00.000Z";
const zones: Record<string, number> = { UTC: 0, "Asia/Kolkata": -330, "Pacific/Kiritimati": -840, "America/St_Johns": 150 };

interface Output { tz: string; hostOffset: number; commands: Array<{ scheduleActivity?: { activityType: string; input: { data: string } } }> }

async function replayUnder(tz: string): Promise<Output> {
  const { stdout } = await exec(process.execPath, ["--import", "tsx", script, at], { cwd: sdk, env: { ...process.env, TZ: tz } });
  return JSON.parse(stdout) as Output;
}
function decision(output: Output): Record<string, unknown> {
  expect(output.commands).toHaveLength(1);
  const input = output.commands[0]!.scheduleActivity!.input.data;
  return JSON.parse(Buffer.from(input, "base64").toString("utf8")) as Record<string, unknown>;
}

describe("replay is independent of the worker's time zone", () => {
  it("emits identical commands under four zones and reads the clock in UTC", async () => {
    const outputs = await Promise.all(Object.keys(zones).map(replayUnder));
    for (const output of outputs) {
      // The zone really applied to the process; the sandbox is what kept it out of the workflow.
      expect(output.hostOffset, output.tz).toBe(zones[output.tz]!);
      expect(output.commands, output.tz).toEqual(outputs[0]!.commands);
    }
    expect(decision(outputs[0]!)).toEqual({
      iso: at,
      parts: [2026, 2, 8, 0, 23, 30],
      offset: 0,
      text: "Sun, 08 Mar 2026 23:30:00 GMT",
      date: "Sun, 08 Mar 2026",
      time: "23:30:00 GMT",
      midnight: Date.UTC(2026, 2, 8),
      offsetFree: Date.UTC(2026, 2, 8, 23, 30),
      components: Date.UTC(2026, 2, 8, 23, 30),
      dateOnly: Date.UTC(2026, 2, 8),
      tomorrow: Date.UTC(2026, 2, 9),
    });
  }, 60_000);
});
