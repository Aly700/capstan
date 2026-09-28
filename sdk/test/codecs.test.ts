import { create } from "@bufbuild/protobuf";
import { afterEach, describe, expect, it, vi } from "vitest";
import { PayloadSchema } from "../src/gen/capstan/v1/capstan_pb.ts";
import { decode, encode } from "../src/internal/payload.ts";
import { durationMs, toProtoDuration } from "../src/internal/duration.ts";
import { failureFromProto, failureToProto } from "../src/internal/failure.ts";
import { ActivityFailure, ApplicationFailure, CancelledFailure, TimeoutFailure } from "../src/types.ts";
import type { Duration } from "../src/types.ts";

afterEach(() => vi.unstubAllEnvs());

describe("payload codec", () => {
  it("round trips JSON values including UTF-8 and null", () => {
    for (const value of [null, false, 0, "雪 ⛵", [1, "two"], { nested: { yes: true } }]) {
      const payload = encode(value)!;
      expect(payload.contentType).toBe("application/json");
      expect(new TextDecoder().decode(payload.data)).toBe(JSON.stringify(value));
      expect(decode(payload)).toEqual(value);
    }
  });
  it("represents undefined by an absent payload", () => {
    expect(encode(undefined)).toBeUndefined();
    expect(decode(undefined)).toBeUndefined();
  });
  it("rejects unsupported encodings and malformed JSON", () => {
    expect(() => decode(create(PayloadSchema, { contentType: "text/plain", data: new TextEncoder().encode("1") }))).toThrow(/content.type|encoding/i);
    expect(() => decode(create(PayloadSchema, { contentType: "application/json", data: new Uint8Array([123]) }))).toThrow();
    expect(() => encode(1n)).toThrow();
    expect(() => encode(() => 1)).toThrow(/JSON/i);
  });
});

describe("duration codec", () => {
  it.each([[0, 0], [1.5, 1.5], ["250ms", 250], ["1.5s", 1500], ["2m", 120000], ["2h", 7200000], ["3d", 259200000]] as [Duration, number][]) ("parses %s", (input, expected) => expect(durationMs(input)).toBe(expected));
  it.each([-1, NaN, Infinity, "-1s", "3w", "1 s", "", "x", "Infinitys", "1e999s"]) ("rejects %s", (input) => expect(() => durationMs(input as Duration)).toThrow(/duration/i));
  it("encodes seconds and nanoseconds without losing sub-millisecond precision", () => {
    expect(toProtoDuration("1.250001s")).toMatchObject({ seconds: 1n, nanos: 250001000 });
    expect(toProtoDuration(0)).toMatchObject({ seconds: 0n, nanos: 0 });
  });
});

describe("failure codec", () => {
  it("round trips typed application failures and their JSON details", () => {
    const err = new ApplicationFailure("denied", { type: "Denied", nonRetryable: true, details: { rule: 7 } });
    const restored = failureFromProto(failureToProto(err));
    expect(restored).toBeInstanceOf(ApplicationFailure);
    expect(restored).toMatchObject({ message: "denied", type: "Denied", nonRetryable: true, details: { rule: 7 } });
  });
  it("round trips an ActivityFailure with an ApplicationFailure cause", () => {
    const err = new ActivityFailure("tool failed", "merge", 9, { cause: new ApplicationFailure("no", { type: "Denied", details: ["policy"] }) });
    const restored = failureFromProto(failureToProto(err));
    expect(restored).toBeInstanceOf(ActivityFailure);
    expect(restored).toMatchObject({ message: "tool failed", activityType: "merge", seq: 9 });
    expect(restored.cause).toBeInstanceOf(ApplicationFailure);
    expect(restored.cause).toMatchObject({ type: "Denied", details: ["policy"] });
  });
  it("round trips timeout and cancellation classes", () => {
    const timeout = failureFromProto(failureToProto(new TimeoutFailure("late", "HEARTBEAT")));
    expect(timeout).toBeInstanceOf(TimeoutFailure);
    expect(timeout).toMatchObject({ timeoutType: "HEARTBEAT" });
    expect(failureFromProto(failureToProto(new CancelledFailure("stop")))).toBeInstanceOf(CancelledFailure);
  });
  it("bounds nested and cyclic causes at ten failures", () => {
    const err = new Error("cycle");
    err.cause = err;
    let failure = failureToProto(err);
    let depth = 1;
    while (failure.cause) { depth++; failure = failure.cause; }
    expect(depth).toBe(10);
    let restored: Error | undefined = failureFromProto(failureToProto(err));
    depth = 0;
    while (restored) { depth++; restored = restored.cause as Error | undefined; }
    expect(depth).toBe(10);
  });
  it("omits stacks by default and keeps them only on explicit opt-in", () => {
    vi.stubEnv("CAPSTAN_KEEP_STACKS", "0");
    const err = new Error("oops");
    expect(failureToProto(err).stack).toBe("");
    expect(failureFromProto(failureToProto(err)).stack).toBeUndefined();
    vi.stubEnv("CAPSTAN_KEEP_STACKS", "1");
    expect(failureFromProto(failureToProto(err)).stack).toBe(err.stack);
  });
  it("normalizes thrown primitives and errors from another realm", () => {
    expect(failureToProto("no")).toMatchObject({ message: "no", type: "Error" });
    expect(failureToProto({ name: "RemoteError", message: "remote", type: "Conflict", nonRetryable: true, details: 1 })).toMatchObject({ message: "remote", type: "Conflict", nonRetryable: true });
  });
});
