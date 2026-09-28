import vm from "node:vm";
import type { WorkflowRuntime } from "../workflow/index.ts";
import { bootstrapSource } from "./globals.ts";

function errorValue(error: unknown, depth = 0): Record<string, unknown> {
  if (depth > 12) return { name: "Error", message: "error cause nesting exceeded" };
  if (error === null || typeof error !== "object") return { name: "Error", message: String(error) };
  const source = error as Record<string, unknown>;
  const value: Record<string, unknown> = { name: String(source.name ?? "Error"), message: String(source.message ?? error) };
  for (const key of ["type", "nonRetryable", "details", "activityType", "seq", "timeoutType", "eventId", "expected", "got"]) {
    if (source[key] !== undefined) value[key] = source[key];
  }
  if (source.cause !== undefined) value.cause = errorValue(source.cause, depth + 1);
  return value;
}
const success = (value?: unknown) => JSON.stringify({ ok: true, value });
function failure(error: unknown): string {
  try { return JSON.stringify({ ok: false, error: errorValue(error) }); }
  catch { return '{"ok":false,"error":{"name":"Error","message":"runtime error could not be serialized"}}'; }
}

/** Every reachable function and object belongs to the VM; the hidden bridge passes strings. */
export function createContext(runtime: WorkflowRuntime, onError?: (error: unknown) => void): vm.Context {
  // The contextifying global proxy swallows throwing getters for absent globals.
  // A real VM global preserves their SandboxViolationError and has VM-native prototypes.
  const context = vm.createContext(vm.constants.DONT_CONTEXTIFY, {
    name: "capstan-workflow", codeGeneration: { strings: false, wasm: false },
  });
  const decoders = new Map<number, TextDecoder>();
  let nextDecoder = 0;
  const dispatch = (method: string, encoded: string, callback?: (encoded: string) => unknown): string => {
    try {
      const args = (JSON.parse(encoded) as { value?: any }[]).map((argument) => argument.value);
      const notify = callback;
      let value: unknown;
      switch (method) {
        case "activity": value = runtime.activity(args[0], args[1], args[2]); break;
        case "sleep": value = runtime.sleep(args[0]); break;
        case "nextSignal": value = runtime.nextSignal(args[0]); break;
        case "requestApproval": value = runtime.requestApproval(args[0]); break;
        case "condition": value = runtime.condition(() => Boolean(notify?.("")), args[0] ?? undefined); break;
        case "sideEffect": return success(runtime.sideEffect(() => JSON.parse(String(notify?.(""))).value));
        case "setHandler":
          runtime.setHandler(args[0], (input: unknown) => notify?.(JSON.stringify({ value: input })));
          return success();
        case "now": return success(runtime.now());
        case "random": return success(runtime.random());
        case "uuid": return success(runtime.uuid());
        case "patched": return success(runtime.patched(args[0]));
        case "deprecatePatch": runtime.deprecatePatch(args[0]); return success();
        case "isCancellationRequested": return success(runtime.isCancellationRequested());
        case "continueAsNew": return runtime.continueAsNew(args[0], args[1] ?? undefined);
        case "workflowInfo": return success(runtime.workflowInfo());
        case "log":
          if (!runtime.workflowInfo().isReplaying) runtime.log(args[0], args[1], args[2] ?? undefined);
          return success();
        case "$encode": return success(Array.from(new TextEncoder().encode(args[0])));
        case "$decoder": {
          const id = ++nextDecoder;
          const decoder = new TextDecoder(args[0], args[1] ?? undefined);
          decoders.set(id, decoder);
          return success({ id, encoding: decoder.encoding, fatal: decoder.fatal, ignoreBOM: decoder.ignoreBOM });
        }
        case "$decode": return success(decoders.get(args[0])!.decode(new Uint8Array(args[1]), args[2] ?? undefined));
        case "$error": {
          const data = args[0] as Record<string, unknown>;
          const error = Object.assign(new Error(String(data.message)), data);
          onError?.(error);
          return success();
        }
        default: throw new Error(`unknown workflow runtime method ${method}`);
      }
      // The VM creates its own Promise; its resolving function never receives a host value.
      Promise.resolve(value).then(
        (result) => { try { notify?.(success(result)); } catch (error) { notify?.(failure(error)); } },
        (error: unknown) => { notify?.(failure(error)); },
      );
      return success();
    } catch (error) { return failure(error); }
  };
  Object.setPrototypeOf(dispatch, null);
  Object.defineProperty(context, "__capstanBridge", { value: dispatch, configurable: true });
  vm.runInContext(bootstrapSource, context, { timeout: 1000, filename: "capstan-sandbox.js" });
  return context;
}

/** Decode input inside the context; a host object argument would leak its constructors. */
export function invokeWorkflow(context: vm.Context, name: string, input: unknown): unknown {
  const encoded = JSON.stringify({ value: input });
  return vm.runInContext(`(() => {
    const fn = globalThis.__capstanWorkflows[${JSON.stringify(name)}];
    if (typeof fn !== "function") throw new Error("workflow export is not a function: " + ${JSON.stringify(name)});
    return fn(JSON.parse(${JSON.stringify(encoded)}).value);
  })()`, context, { timeout: 1000, filename: "capstan-workflow-invoke.js" });
}
