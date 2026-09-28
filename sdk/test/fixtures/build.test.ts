import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { fromJson } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { CommandSchema, HistoryEventSchema } from "../../src/gen/capstan/v1/capstan_pb.ts";
import { commands, run } from "./build.ts";

const fixtureDir = fileURLToPath(new URL("../../../conformance/fixtures/", import.meta.url));

describe("fixture builder", () => {
  it("links task completions and activity results with gap-free event ids", () => {
    const fixture = run("singleActivity", { n: 21 }).task().scheduleActivity(1, "double", 21).completeActivity(1, 42).task().expectCommands(commands.completeRun({ doubled: 42 }));
    expect(fixture.history.map((event) => event.eventId)).toEqual(["1", "2", "3", "4", "5", "6", "7", "8"]);
    expect(fixture.history[4]).toMatchObject({ activityScheduled: { seq: "1", taskCompletedEventId: "4" } });
    expect(fixture.history[5]).toMatchObject({ activityCompleted: { seq: "1", scheduledEventId: "5" } });
    expect(fixture.expect).toEqual({ commands: [commands.completeRun({ doubled: 42 })] });
  });
  it("records empty completed and discarded activations without command events", () => {
    const fixture = run("waiting", null).task().task().discard().task().expectCommands();
    expect(fixture.history.map((event) => event.type)).toContain("EVENT_TYPE_TASK_COMPLETED");
    expect(fixture.history.map((event) => event.type)).toContain("EVENT_TYPE_TASK_FAILED");
  });
  it("discards timed-out activations without a TaskCompleted", () => {
    const fixture = run("sleeping").task().timeout().task().expectCommands(commands.startTimer(1));
    expect(fixture.history.map((event) => event.type)).not.toContain("EVENT_TYPE_TASK_COMPLETED");
    expect(fixture.history.map((event) => event.type)).toContain("EVENT_TYPE_TASK_TIMED_OUT");
  });
  it("requires a final TaskStarted and known operation references", () => {
    expect(() => run("invalid", null).expectCommands()).toThrow(/TaskStarted/);
    expect(() => run("invalid", null).task().completeActivity(12, true)).toThrow(/unknown activity/i);
  });
});

describe("conformance corpus structure", () => {
  const files = readdirSync(fixtureDir).filter((file) => file.endsWith(".json")).sort();
  it("contains at least 24 additional fixtures", () => expect(files.length).toBeGreaterThanOrEqual(25));
  for (const file of files) {
    it(`${file} is valid protojson with gap-free history and a current activation`, () => {
      const fixture = JSON.parse(readFileSync(`${fixtureDir}/${file}`, "utf8"));
      expect(fixture.name).toBeTypeOf("string");
      expect(fixture.workflow).toBeTypeOf("string");
      expect(fixture.description).toBeTypeOf("string");
      fixture.history.forEach((event: unknown, index: number) => {
        const parsed = fromJson(HistoryEventSchema, event as Parameters<typeof fromJson<typeof HistoryEventSchema>>[1]);
        expect(parsed.eventId).toBe(BigInt(index + 1));
      });
      expect(fixture.history.at(-1).type).toBe("EVENT_TYPE_TASK_STARTED");
      expect(Object.keys(fixture.expect)).toHaveLength(1);
      if (fixture.expect.commands) for (const command of fixture.expect.commands) fromJson(CommandSchema, command);
      else expect(fixture.expect.mismatch.eventId).toBeGreaterThan(0);
    });
  }
});
