import { fromJson } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { HistoryEventSchema } from "../src/gen/capstan/v1/capstan_pb.ts";
import { decode } from "../src/internal/payload.ts";
import { replay } from "../src/replay/runtime.ts";
import { run } from "./fixtures/build.ts";
import type { Fixture } from "./fixtures/build.ts";

function review(body: string, fixture: Fixture) {
  const code = `globalThis.__capstanWorkflows = { run: async () => { const rt = globalThis[Symbol.for('capstan.workflow.runtime')]; ${body} } };`;
  return replay({ bundle: { code, buildId: "review" }, runId: "review", workflowType: "run", history: fixture.history.map((event) => fromJson(HistoryEventSchema, event)) });
}

describe("independent replay contract regressions", () => {
  it("reports the first mismatch even when a later marker eagerly detects another", async () => {
    const history = run("run").task().scheduleActivity(1, "double", 21).marker(2, "uuid", "", "recorded").task().expectCommands();
    const body = "void rt.activity('triple',21,{startToCloseTimeout:'10s'}); rt.sideEffect(()=>1); return rt.nextSignal({name:'done'});";
    await expect(review(body, history)).rejects.toMatchObject({ name: "HistoryMismatchError", eventId: 5 });
  });

  it.each([
    ["callback throws undefined", "() => { throw undefined; }"],
    ["callback emits a command", "() => { void rt.sleep(1); return 1; }"],
    ["callback throws", "() => { throw new Error('side effect callback failed'); }"],
    ["BigInt result", "() => 1n"],
    ["circular result", "() => { const value = {}; value.self = value; return value; }"],
  ])("fails closed when a caught side effect has no recordable result: %s", async (_label, callback) => {
    const history = run("run").task().expectCommands();
    const body = `try { rt.sideEffect(${callback}); } catch {} return rt.activity('double',21,{startToCloseTimeout:'10s'});`;
    await expect(review(body, history)).rejects.toMatchObject({ message: expect.any(String) });
  });

  it("pins README delivery-before-clock order for synchronous signal handlers", async () => {
    const history = run("run").task("2026-09-28T12:00:01Z").signal("go", true).task("2026-09-28T13:00:00Z").expectCommands();
    const body = "let observed;rt.setHandler({name:'go'},()=>{observed=rt.now();});await rt.condition(()=>observed!==undefined);return [observed,rt.now()];";
    const result = await review(body, history);
    expect(result[0]?.attributes.case).toBe("completeRun");
    if (result[0]?.attributes.case === "completeRun") expect(decode(result[0].attributes.value.result)).toEqual([1790596801000, 1790600400000]);
  });

  it("memoizes an old-run false patch decision across later activations", async () => {
    const history = run("run").task().scheduleActivity(1, "double", 21).completeActivity(1, 42).task().expectCommands();
    const body = "const first=rt.patched('v2');await rt.activity('double',21,{startToCloseTimeout:'10s'});return [first,rt.patched('v2')];";
    const result = await review(body, history);
    expect(result).toHaveLength(1);
    expect(result[0]?.attributes.case).toBe("completeRun");
    if (result[0]?.attributes.case === "completeRun") expect(decode(result[0].attributes.value.result)).toEqual([false, false]);
  });
});
