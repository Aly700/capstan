import { mkdtemp, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { bundleWorkflows } from "../src/sandbox/bundle.ts";
import { createContext, invokeWorkflow } from "../src/sandbox/context.ts";
import { ActivityFailure, TimeoutFailure } from "../src/types.ts";
import type { WorkflowRuntime } from "../src/workflow/index.ts";

const fixtures = fileURLToPath(new URL("./sandbox-fixtures/", import.meta.url));
function fakeRuntime(overrides: Partial<WorkflowRuntime> = {}): WorkflowRuntime {
  return {
    proxyActivities: () => { throw new Error("unexpected proxy access"); },
    activity: async <T>(_name: string, input: unknown) => input as T,
    sleep: async () => {}, now: () => 1_700_000_000_123, random: () => 0.25, uuid: () => "recorded-id",
    sideEffect: (fn) => fn(), patched: () => true, deprecatePatch: () => {}, setHandler: () => {},
    nextSignal: async <T>() => ({ value: 1 }) as T, condition: async (fn) => fn(),
    isCancellationRequested: () => false, continueAsNew: () => { throw new Error("ContinueAsNew"); },
    workflowInfo: () => ({ runId: "run", workflowType: "echo", taskQueue: "q", attempt: 1, isReplaying: false, continuedFromRunId: "" }),
    log: () => {}, requestApproval: async () => ({ outcome: "approved", choice: "yes", resolver: "test", note: "" }), ...overrides,
  };
}
const run = (context: vm.Context, code: string) => vm.runInContext(code, context, { timeout: 1000 });

describe("sandbox", () => {
  it.each(["fetch", "setTimeout", "setInterval", "setImmediate", "process", "require", "WebAssembly", "SharedArrayBuffer", "Atomics", "Buffer", "performance", "crypto", "Intl", "eval"])("rejects forbidden global %s with author guidance", (name) => {
    const context = createContext(fakeRuntime());
    expect(() => run(context, name)).toThrowError(expect.objectContaining({ name: "SandboxViolationError", message: expect.stringContaining("use an activity instead") }));
  });
  it("uses workflow time and randomness without native Date escape paths", () => {
    const context = createContext(fakeRuntime());
    expect(run(context, "[Date.now(), new Date().getTime(), new Date().constructor.now(), Math.random()]")).toEqual([1_700_000_000_123, 1_700_000_000_123, 1_700_000_000_123, 0.25]);
    expect(run(context, "new Date(0).getTime()")).toBe(0);
    expect(run(context, "Date.parse('2000-01-01T00:00:00Z')")).toBe(946684800000);
    expect(run(context, "Object.getPrototypeOf(Date) === Function.prototype")).toBe(true);
  });
  it("creates a fresh global and intrinsic graph for every workflow", () => {
    const one = createContext(fakeRuntime()); const two = createContext(fakeRuntime());
    run(one, "globalThis.leak = 5; Object.prototype.leak = 6; Math.extra = 7");
    expect(run(two, "[globalThis.leak, ({}).leak, Math.extra]")).toEqual([undefined, undefined, undefined]);
  });
  it.each(["Function('return 1')()", "({}).constructor.constructor('return process')()", "globalThis.constructor.constructor('return process')()", "Math.random.constructor('return process')()", "console.log.constructor('return process')()", "TextEncoder.constructor('return process')()", "queueMicrotask.constructor('return process')()", "Symbol.for('capstan.workflow.runtime') && globalThis[Symbol.for('capstan.workflow.runtime')].activity.constructor('return process')()"])("prevents string compilation and host constructor escape: %s", (source) => {
    expect(() => run(createContext(fakeRuntime()), source)).toThrow();
  });
  it("provides realm-native text codecs, cloning and microtasks", async () => {
    const context = createContext(fakeRuntime());
    expect(run(context, "new TextDecoder().decode(new TextEncoder().encode('hello 🌊'))")).toBe("hello 🌊");
    expect(run(context, "new TextEncoder().encode('hello').constructor === Uint8Array")).toBe(true);
    expect(run(context, "(() => { const x = { map: new Map([[1, 2]]) }; x.self = x; const c = structuredClone(x); return c !== x && c.self === c && c.map.get(1) === 2 })()")).toBe(true);
    run(context, "queueMicrotask(() => globalThis.drained = true)");
    await Promise.resolve();
    expect(run(context, "globalThis.drained")).toBe(true);
  });
  it("logs through the runtime only when not replaying", () => {
    const lines: unknown[] = [];
    const runtime = fakeRuntime({ log: (...line) => { lines.push(line); } });
    run(createContext(runtime), "console.log('hello', { x: 1 }); console.warn('warning')");
    expect(lines).toEqual([["info", 'hello {"x":1}', undefined], ["warn", "warning", undefined]]);
    runtime.workflowInfo = () => ({ runId: "r", workflowType: "w", taskQueue: "q", attempt: 1, isReplaying: true, continuedFromRunId: "" });
    run(createContext(runtime), "console.error('must be suppressed')");
    expect(lines).toHaveLength(2);
  });
  it("bundles the SDK API into the VM and erases activity type imports", async () => {
    const bundle = await bundleWorkflows(path.join(fixtures, "workflows.ts"));
    expect(bundle.buildId).toMatch(/^[0-9a-f]{64}$/);
    expect(bundle.code).not.toContain('node:fs');
    const context = createContext(fakeRuntime());
    run(context, bundle.code);
    expect(await invokeWorkflow(context, "callActivity", { answer: 42 })).toEqual({ answer: 42 });
    expect(await invokeWorkflow(context, "effect", null)).toEqual({ answer: 42 });
  });
  it("rejects Node builtin imports with the import named", async () => {
    await expect(bundleWorkflows(path.join(fixtures, "builtin.ts"))).rejects.toThrow(/node:fs/);
    await expect(bundleWorkflows(path.join(fixtures, "builtin-bare.ts"))).rejects.toThrow(/"fs"/);
    await expect(bundleWorkflows(path.join(fixtures, "builtin-dynamic.ts"))).rejects.toThrow(/node:net/);
  });
  it("invalidates cached output when a workflow dependency changes", async () => {
    const dir = await mkdtemp(path.join(fixtures, "cache-"));
    try {
      await writeFile(path.join(dir, "entry.ts"), 'export { value } from "./dependency.ts"');
      await writeFile(path.join(dir, "dependency.ts"), 'export const value = 1');
      const first = await bundleWorkflows(path.join(dir, "entry.ts"));
      const repeat = await bundleWorkflows(path.join(dir, "entry.ts"));
      expect(repeat).toBe(first);
      await writeFile(path.join(dir, "dependency.ts"), 'export const value = 2');
      expect((await bundleWorkflows(path.join(dir, "entry.ts"))).buildId).not.toBe(first.buildId);
    } finally { await rm(dir, { recursive: true, force: true }); }
  });
  it("keeps host input, runtime objects and promises outside the realm", async () => {
    const context = createContext(fakeRuntime());
    run(context, "globalThis.__capstanWorkflows = { inspect: async (input) => { const rt = globalThis[Symbol.for('capstan.workflow.runtime')]; const info = rt.workflowInfo(); const promise = rt.activity('x', input, { startToCloseTimeout: '1s' }); const result = await promise; return [input.constructor === Object, info.constructor === Object, promise.constructor === Promise, result.constructor === Object]; } }");
    expect(await invokeWorkflow(context, "inspect", { safe: true })).toEqual([true, true, true, true]);
  });
  it("rehydrates failure subclasses and their causes inside the bundle realm", async () => {
    const runtime = fakeRuntime({ activity: async () => { throw new ActivityFailure("failed", "charge", 3, { cause: new TimeoutFailure("slow", "HEARTBEAT") }); } });
    const context = createContext(runtime);
    run(context, (await bundleWorkflows(path.join(fixtures, "workflows.ts"))).code);
    expect(await invokeWorkflow(context, "catchFailure", null)).toEqual({ seq: 3, activityType: "charge", timeoutType: "HEARTBEAT" });
  });
  it("passes handler promises to the runtime without exposing host payloads", async () => {
    let handler: ((input: unknown) => unknown) | undefined;
    const context = createContext(fakeRuntime({ setHandler: (_signal, fn) => { handler = (input) => fn(input as never); } }));
    run(context, (await bundleWorkflows(path.join(fixtures, "workflows.ts"))).code);
    await invokeWorkflow(context, "signalHandler", null);
    const pending = handler?.({ hello: "world" });
    expect(pending).toBeTruthy();
    await pending;
    expect(run(context, "globalThis.handlerRan")).toBe(true);
  });
  it("does not expose host failures or their nested details", async () => {
    const context = createContext(fakeRuntime({ activity: async () => { throw new ActivityFailure("no", "work", 1, { cause: new TimeoutFailure("slow", "HEARTBEAT") }); } }));
    run(context, `globalThis.__capstanWorkflows = { inspect: async () => {
      try { await globalThis[Symbol.for('capstan.workflow.runtime')].activity('x', null, {}); }
      catch (error) { return [error instanceof Error, error.cause instanceof Error, error.constructor.constructor === Function]; }
    } }`);
    expect(await invokeWorkflow(context, "inspect", null)).toEqual([true, true, true]);
  });
  it("synchronously invokes predicates and later settles their realm promise", async () => {
    const context = createContext(fakeRuntime());
    expect(await run(context, "globalThis[Symbol.for('capstan.workflow.runtime')].condition(() => true)")).toBe(true);
  });
  it("calls proxies immediately in call order and does not behave like a thenable", async () => {
    const called: string[] = [];
    const context = createContext(fakeRuntime({ activity: async <T>(name: string) => { called.push(name); return name as T; } }));
    const promise = run(context, "(() => { const p = globalThis[Symbol.for('capstan.workflow.runtime')].proxyActivities({startToCloseTimeout: '1s'}); if(p.then !== undefined) throw new Error('thenable'); return Promise.all([p.first(null), p.second(null)]); })()");
    expect(called).toEqual(["first", "second"]);
    expect(await promise).toEqual(["first", "second"]);
  });
  it("preserves undefined inputs at the host boundary", async () => {
    let seen: unknown = "unset";
    const context = createContext(fakeRuntime({ activity: async <T>(_name: string, input: unknown) => { seen = input; return undefined as T; } }));
    await run(context, "globalThis[Symbol.for('capstan.workflow.runtime')].activity('x', undefined, {startToCloseTimeout:'1s'})");
    expect(seen).toBeUndefined();
  });
  it("recreates standard host errors using realm-native error constructors", () => {
    const context = createContext(fakeRuntime({ now: () => { throw new RangeError("clock unavailable"); } }));
    expect(run(context, "(() => { try { Date.now(); } catch(error) { return [error instanceof RangeError, error.message]; } })()")).toEqual([true, "clock unavailable"]);
  });
  it("rejects synchronous infinite loops during workflow invocation", () => {
    const context = createContext(fakeRuntime());
    run(context, "globalThis.__capstanWorkflows = { spin() { while(true) {} } }");
    expect(() => invokeWorkflow(context, "spin", null)).toThrow(/timed out/);
  });
  it.each([
    "queueMicrotask(() => { throw new Error('microtask failed'); })",
    "queueMicrotask(async () => { await Promise.resolve(); throw new Error('microtask failed'); })",
  ])("reports detached microtask failures to the replay runtime: %s", async (source) => {
    const failures: unknown[] = [];
    const context = createContext(fakeRuntime(), (error) => { failures.push(error); });
    run(context, source);
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatchObject({ name: "Error", message: "microtask failed" });
  });
});
