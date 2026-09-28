// This bootstrap is evaluated inside the VM. Never add a host object to its globals.
export const bootstrapSource = String.raw`
(() => {
  "use strict";
  const bridge = globalThis.__capstanBridge;
  delete globalThis.__capstanBridge;
  const NativeDate = Date;
  const NativePromise = Promise;
  const NativeUint8Array = Uint8Array;
  const parse = JSON.parse;
  const stringify = JSON.stringify;
  const define = Object.defineProperty;
  const failureTypes = Object.create(null);
  const nativeErrorTypes = { Error, TypeError, RangeError, SyntaxError, ReferenceError, EvalError, URIError };
  class SandboxViolationError extends Error {
    constructor(what) { super("workflow code cannot use " + what + "; use an activity instead"); this.name = "SandboxViolationError"; }
  }
  function reviveFailure(data) {
    const cause = data.cause === undefined ? undefined : reviveFailure(data.cause);
    const Ctor = failureTypes[data.name];
    let error;
    if (Ctor && data.name === "ActivityFailure") error = new Ctor(data.message, data.activityType, data.seq, { cause });
    else if (Ctor && data.name === "TimeoutFailure") error = new Ctor(data.message, data.timeoutType, { cause });
    else if (Ctor && data.name === "HistoryMismatchError") error = new Ctor(data.eventId, data.expected, data.got);
    else if (Ctor && data.name !== "SandboxViolationError") error = new Ctor(data.message, { ...data, cause });
    else {
      const NativeError = Object.hasOwn(nativeErrorTypes, data.name) ? nativeErrorTypes[data.name] : Error;
      error = new NativeError(data.message, cause === undefined ? undefined : { cause }); error.name = data.name;
    }
    for (const key of ["type", "nonRetryable", "details", "activityType", "seq", "timeoutType", "eventId", "expected", "got"]) {
      if (data[key] !== undefined) error[key] = data[key];
    }
    return error;
  }
  define(globalThis, Symbol.for("capstan.sandbox.failures"), { value: (types) => {
    for (const key of ["CapstanFailure", "ApplicationFailure", "ActivityFailure", "TimeoutFailure", "CancelledFailure", "HistoryMismatchError", "SandboxViolationError"]) failureTypes[key] = types[key];
  }});
  function unpack(encoded) { const result = parse(encoded); if (!result.ok) throw reviveFailure(result.error); return result.value; }
  const sync = (method, args = [], callback) => unpack(bridge(method, stringify(args.map((value) => ({ value }))), callback));
  const asyncCall = (method, args) => new NativePromise((resolve, reject) => {
    try { sync(method, args, (encoded) => { try { resolve(unpack(encoded)); } catch (error) { reject(error); } }); }
    catch (error) { reject(error); }
  });
  const runtime = Object.freeze({
    proxyActivities: (options) => new Proxy(Object.create(null), { get: (_target, name) => {
      if (typeof name !== "string" || name === "then") return undefined;
      return (input) => asyncCall("activity", [name, input, options]);
    }}),
    activity: (name, input, options) => asyncCall("activity", [name, input, options]),
    sleep: (duration) => asyncCall("sleep", [duration]), now: () => sync("now"), random: () => sync("random"), uuid: () => sync("uuid"),
    sideEffect: (fn) => sync("sideEffect", [], () => stringify({ value: fn() })),
    patched: (id) => sync("patched", [id]), deprecatePatch: (id) => sync("deprecatePatch", [id]),
    setHandler: (signal, handler) => sync("setHandler", [signal], (encoded) => handler(parse(encoded).value)),
    nextSignal: (signal) => asyncCall("nextSignal", [signal]),
    condition: (predicate, timeout) => new NativePromise((resolve, reject) => {
      // A single callback carries predicate checks and the eventual resolution.
      try { sync("condition", [timeout], (encoded) => {
        if (encoded === "") return Boolean(predicate());
        try { resolve(unpack(encoded)); } catch (error) { reject(error); }
      }); } catch (error) { reject(error); }
    }),
    isCancellationRequested: () => sync("isCancellationRequested"),
    continueAsNew: (input, options) => sync("continueAsNew", [input, options]),
    workflowInfo: () => sync("workflowInfo"), log: (level, message, fields) => sync("log", [level, message, fields]),
    requestApproval: (request) => asyncCall("requestApproval", [request]),
  });
  define(globalThis, Symbol.for("capstan.workflow.runtime"), { value: runtime, configurable: false });
  const nativeDateParse = NativeDate.parse;
  function parseDate(value) {
    const text = String(value);
    // Offset-free ISO values have a fixed UTC meaning inside a workflow. Other
    // date spellings must include an explicit offset; native parsing otherwise
    // consults the worker's local timezone.
    if (/^[+-]?\d{4,6}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?$/.test(text)) return nativeDateParse(text + "Z");
    if (/^[+-]?\d{4,6}(?:-\d{2}(?:-\d{2})?)?$/.test(text) || /(?:Z|[+-]\d{2}:?\d{2}|(?:GMT|UTC)(?:[+-]\d{4})?)$/i.test(text)) return nativeDateParse(text);
    throw new SandboxViolationError("Date strings without an ISO format or explicit timezone");
  }
  for (const [local, utc] of Object.entries({
    getFullYear: "getUTCFullYear", getMonth: "getUTCMonth", getDate: "getUTCDate", getDay: "getUTCDay",
    getHours: "getUTCHours", getMinutes: "getUTCMinutes", getSeconds: "getUTCSeconds", getMilliseconds: "getUTCMilliseconds",
    setFullYear: "setUTCFullYear", setMonth: "setUTCMonth", setDate: "setUTCDate", setHours: "setUTCHours",
    setMinutes: "setUTCMinutes", setSeconds: "setUTCSeconds", setMilliseconds: "setUTCMilliseconds",
  })) define(NativeDate.prototype, local, { value: NativeDate.prototype[utc], writable: true, configurable: true });
  define(NativeDate.prototype, "getYear", { value() { return this.getUTCFullYear() - 1900; }, configurable: true });
  define(NativeDate.prototype, "setYear", { value(year) { const n = Number(year); return this.setUTCFullYear(n >= 0 && n <= 99 ? n + 1900 : n); }, configurable: true });
  define(NativeDate.prototype, "getTimezoneOffset", { value() { return Number.isNaN(this.getTime()) ? NaN : 0; }, configurable: true });
  const dateString = NativeDate.prototype.toUTCString;
  define(NativeDate.prototype, "toString", { value: dateString, configurable: true });
  define(NativeDate.prototype, "toDateString", { value() { const text = dateString.call(this); return text === "Invalid Date" ? text : text.slice(0, -13); }, configurable: true });
  define(NativeDate.prototype, "toTimeString", { value() { const text = dateString.call(this); return text === "Invalid Date" ? text : text.slice(-12); }, configurable: true });
  for (const name of ["toLocaleString", "toLocaleDateString", "toLocaleTimeString"]) define(NativeDate.prototype, name, { value() { throw new SandboxViolationError("Date." + name); }, configurable: true });
  function dateValue(value) {
    if (value instanceof NativeDate) return NativeDate.prototype.getTime.call(value);
    const primitive = (item) => item === null || (typeof item !== "object" && typeof item !== "function");
    if (!primitive(value)) {
      const convert = value[Symbol.toPrimitive];
      if (convert !== undefined) value = convert.call(value, "default");
      else {
        const original = value;
        for (const name of ["valueOf", "toString"]) {
          if (typeof original[name] === "function") value = original[name]();
          if (primitive(value)) break;
        }
      }
      if (!primitive(value)) throw new TypeError("Cannot convert object to primitive value");
    }
    return typeof value === "string" ? parseDate(value) : value;
  }
  function WorkflowDate(...args) {
    if (!new.target) return new NativeDate(runtime.now()).toString();
    const value = args.length === 0 ? runtime.now() : args.length > 1 ? NativeDate.UTC(...args) : dateValue(args[0]);
    return Reflect.construct(NativeDate, [value], new.target);
  }
  WorkflowDate.prototype = NativeDate.prototype;
  define(WorkflowDate.prototype, "constructor", { value: WorkflowDate, writable: true, configurable: true });
  define(WorkflowDate, "name", { value: "Date" });
  define(WorkflowDate, "now", { value: () => runtime.now() });
  define(WorkflowDate, "parse", { value: parseDate });
  define(WorkflowDate, "UTC", { value: NativeDate.UTC });
  globalThis.Date = WorkflowDate;
  Math.random = () => runtime.random();
  class TextEncoder {
    get encoding() { return "utf-8"; }
    encode(input = "") { return NativeUint8Array.from(sync("$encode", [String(input)])); }
    encodeInto(input, destination) {
      const text = String(input); let read = 0; let written = 0;
      for (const point of text) {
        const bytes = this.encode(point);
        if (written + bytes.length > destination.length) break;
        destination.set(bytes, written); written += bytes.length; read += point.length;
      }
      return { read, written };
    }
  }
  class TextDecoder {
    #id; #encoding; #fatal; #ignoreBOM;
    constructor(label = "utf-8", options) { const info = sync("$decoder", [String(label), options]); this.#id = info.id; this.#encoding = info.encoding; this.#fatal = info.fatal; this.#ignoreBOM = info.ignoreBOM; }
    get encoding() { return this.#encoding; } get fatal() { return this.#fatal; } get ignoreBOM() { return this.#ignoreBOM; }
    decode(input = new NativeUint8Array(), options) {
      const bytes = input instanceof ArrayBuffer ? new NativeUint8Array(input) : new NativeUint8Array(input.buffer, input.byteOffset, input.byteLength);
      return sync("$decode", [this.#id, Array.from(bytes), options]);
    }
  }
  globalThis.TextEncoder = TextEncoder; globalThis.TextDecoder = TextDecoder;
  globalThis.structuredClone = function clone(value, options) {
    if (options?.transfer?.length) throw new TypeError("transfer is unavailable in a workflow");
    const seen = new Map();
    function copy(value) {
      if (typeof value === "function" || typeof value === "symbol") throw new TypeError("value cannot be cloned");
      if (value === null || typeof value !== "object") return value;
      if (seen.has(value)) return seen.get(value);
      let result;
      if (value instanceof NativeDate) result = new WorkflowDate(value.getTime());
      else if (value instanceof RegExp) result = new RegExp(value.source, value.flags);
      else if (value instanceof Map) { result = new Map(); seen.set(value, result); for (const [k, v] of value) result.set(copy(k), copy(v)); return result; }
      else if (value instanceof Set) { result = new Set(); seen.set(value, result); for (const v of value) result.add(copy(v)); return result; }
      else if (value instanceof ArrayBuffer) result = value.slice(0);
      else if (ArrayBuffer.isView(value)) { const buffer = copy(value.buffer); result = value instanceof DataView ? new DataView(buffer, value.byteOffset, value.byteLength) : new value.constructor(buffer, value.byteOffset, value.length); }
      else { result = Array.isArray(value) ? [] : {}; seen.set(value, result); for (const key of Object.keys(value)) define(result, key, { value: copy(value[key]), enumerable: true, writable: true, configurable: true }); return result; }
      seen.set(value, result); return result;
    }
    return copy(value);
  };
  function reportDetachedError(error) {
    try {
      const serialize = (error, depth = 0) => {
        if (error === null || typeof error !== "object") return { name: "Error", message: String(error) };
        const data = { name: String(error.name ?? "Error"), message: String(error.message ?? error) };
        for (const key of ["type", "nonRetryable", "details", "activityType", "seq", "timeoutType", "eventId", "expected", "got"]) if (error[key] !== undefined) data[key] = error[key];
        if (error.cause !== undefined && depth < 10) data.cause = serialize(error.cause, depth + 1);
        return data;
      };
      bridge("$error", stringify([{ value: serialize(error) }]));
    } catch {
      bridge("$error", '[{"value":{"name":"Error","message":"workflow microtask failed with an unserializable error"}}]');
    }
  }
  globalThis.queueMicrotask = (callback) => {
    if (typeof callback !== "function") throw new TypeError("callback must be a function");
    NativePromise.resolve().then(callback).catch(reportDetachedError);
  };
  const format = (args) => args.map((value) => typeof value === "string" ? value : stringify(value) ?? String(value)).join(" ");
  globalThis.console = Object.freeze({ log: (...args) => runtime.log("info", format(args)), info: (...args) => runtime.log("info", format(args)), warn: (...args) => runtime.log("warn", format(args)), error: (...args) => runtime.log("error", format(args)), debug: (...args) => runtime.log("info", format(args)) });
  const allowed = new Set("globalThis Infinity NaN undefined Object Array Function Promise Map Set WeakMap WeakSet Symbol BigInt Number String Boolean RegExp JSON Math Date Error TypeError RangeError SyntaxError ReferenceError AggregateError Proxy Reflect ArrayBuffer DataView Uint8Array Int8Array Uint16Array Int16Array Uint32Array Int32Array Float32Array Float64Array BigInt64Array BigUint64Array TextEncoder TextDecoder structuredClone queueMicrotask console".split(" "));
  const blocked = new Set(["fetch", "setTimeout", "setInterval", "setImmediate", "process", "require", "WebAssembly", "SharedArrayBuffer", "Atomics", "Buffer", "performance", "crypto", "Intl", "eval", "__capstanBridge"]);
  for (const name of Object.getOwnPropertyNames(globalThis)) if (!allowed.has(name)) blocked.add(name);
  for (const name of blocked) define(globalThis, name, { configurable: false, get() { throw new SandboxViolationError(name); }, set() { throw new SandboxViolationError(name); } });
})();
`;
