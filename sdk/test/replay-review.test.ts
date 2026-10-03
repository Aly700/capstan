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
  it.each<[string, string, Record<string, unknown>]>([
    ["callback throws undefined", "() => { throw undefined; }", { name: "Error", message: "workflow callback threw an empty value" }],
    ["callback emits a command", "() => { void rt.sleep(1); return 1; }", { name: "Error", message: "sideEffect callbacks cannot emit workflow commands; call the workflow API outside the callback" }],
    ["callback throws", "() => { throw new Error('side effect callback failed'); }", { name: "Error", message: "side effect callback failed" }],
    ["BigInt result", "() => 1n", { name: "TypeError", message: expect.stringContaining("BigInt") }],
    ["circular result", "() => { const value = {}; value.self = value; return value; }", { name: "TypeError", message: expect.stringContaining("circular") }],
  ])("fails closed when a caught side effect has no recordable result: %s", async (_label, callback, failure) => {
    const history = run("run").task().expectCommands();
    const body = `try { rt.sideEffect(${callback}); } catch {} return rt.activity('double',21,{startToCloseTimeout:'10s'});`;
    const settled = await review(body, history).then((commands) => ({ commands, error: undefined }), (error: unknown) => ({ commands: undefined, error }));
    expect(settled.error).toMatchObject(failure);
    // The activity scheduled after the swallowed failure never leaves the runtime as a command.
    expect(settled.commands).toBeUndefined();
  });

  it("sets the current clock before evaluating a condition after a signal", async () => {
    const history = run("run").task("2026-09-28T12:00:01Z").signal("go", true).task("2026-09-28T13:00:00Z").expectCommands();
    const body = "let ready=false,observed;rt.setHandler({name:'go'},()=>{ready=true;});await rt.condition(()=>{if(!ready)return false;observed=rt.now();return true;});return [observed,rt.now()];";
    const result = await review(body, history);
    expect(result[0]?.attributes.case).toBe("completeRun");
    if (result[0]?.attributes.case === "completeRun") expect(decode(result[0].attributes.value.result)).toEqual([1790600400000, 1790600400000]);
  });
});
