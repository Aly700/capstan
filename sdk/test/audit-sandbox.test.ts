import vm from "node:vm";
import { describe, expect, it } from "vitest";
import { createContext } from "../src/sandbox/context.ts";
import type { WorkflowRuntime } from "../src/workflow/index.ts";

function realm() {
  const runtime: Partial<WorkflowRuntime> = {
    now: () => 1_800_000_000_000,
    random: () => 0.375,
    workflowInfo: () => ({ runId: "audit", workflowType: "probe", taskQueue: "audit", attempt: 1, isReplaying: false, continuedFromRunId: "" }),
  };
  return createContext(runtime as WorkflowRuntime);
}
function run(source: string) { return vm.runInContext(source, realm(), { timeout: 1000 }); }

describe("audit workflow realm", () => {
  it.each([
    "Object.constructor('return Date.now()')()",
    "Object.getPrototypeOf(globalThis[Symbol.for('capstan.workflow.runtime')].now).constructor('return process')()",
    "Object.getPrototypeOf((async () => {})()).constructor.constructor('return process')()",
    "(async () => {}).constructor('return process')()",
    "(function* () {}).constructor('yield process')().next()",
    "new TextEncoder().encode('x').constructor.constructor('return process')()",
  ])("blocks constructor compilation %s", (source) => {
    expect(() => run(source)).toThrow(/Code generation from strings disallowed/);
  });
  it("rejects dynamic Node imports even when the module name is computed", async () => {
    await expect(run("import(['node', 'fs'].join(':'))")).rejects.toThrow(/dynamic import callback/i);
  });
  it.each([
    "Date()", "new Date(0).getHours()", "new Date(2000, 0, 1).getTime()", "Date.parse('2000-01-01T00:00:00')",
    "new Date(new String('2000-01-01T00:00:00')).getTime()",
    "(() => { const d = new Date(0); d.setHours(2); return [d.getTime(), d.getTimezoneOffset(), d.toString(), d.toDateString(), d.toTimeString()]; })()",
  ])("does not read worker timezone through %s", (source) => {
    const prior = process.env.TZ;
    try {
      process.env.TZ = "UTC";
      const utc = run(source);
      process.env.TZ = "America/Toronto";
      expect(run(source)).toEqual(utc);
    } finally {
      if (prior === undefined) delete process.env.TZ;
      else process.env.TZ = prior;
    }
  });
  it.each(["new Date().toLocaleString()", "new Date().toLocaleDateString()", "new Date().toLocaleTimeString()", "Date.parse('9/28/2026')"])("rejects implicit locale or timezone input: %s", (source) => {
    expect(() => run(source)).toThrowError(expect.objectContaining({ name: "SandboxViolationError" }));
  });
  it("keeps explicit UTC dates and offsets", () => {
    expect(run("[new Date('2000-01-01T00:00:00-05:00').getTime(), new Date(new Date(123)).getTime(), Date.parse('2000-01-01')]")).toEqual([946702800000, 123, 946684800000]);
  });
});
