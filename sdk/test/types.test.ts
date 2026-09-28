import { describe, expect, it } from "vitest";
import { ActivityFailure, ApplicationFailure, HistoryMismatchError, TimeoutFailure } from "../src/types.ts";

describe("failure types", () => {
  it("ApplicationFailure carries its type and retry flag", () => {
    const f = new ApplicationFailure("card declined", { type: "PaymentDeclined", nonRetryable: true, details: { code: 51 } });
    expect(f).toBeInstanceOf(Error);
    expect(f.name).toBe("ApplicationFailure");
    expect(f.type).toBe("PaymentDeclined");
    expect(f.nonRetryable).toBe(true);
    expect(f.details).toEqual({ code: 51 });
  });

  it("type defaults to the class name", () => {
    expect(new ApplicationFailure("x").type).toBe("ApplicationFailure");
    expect(new TimeoutFailure("slow", "HEARTBEAT").timeoutType).toBe("HEARTBEAT");
  });

  it("ActivityFailure keeps the activity identity and the cause", () => {
    const cause = new ApplicationFailure("boom");
    const f = new ActivityFailure("activity failed", "charge", 3, { cause });
    expect(f.activityType).toBe("charge");
    expect(f.seq).toBe(3);
    expect(f.cause).toBe(cause);
  });

  it("HistoryMismatchError names the event and both sides", () => {
    const e = new HistoryMismatchError(5, "ActivityScheduled(seq=1,double)", "StartTimer(seq=1)");
    expect(e.message).toContain("event 5");
    expect(e.eventId).toBe(5);
  });
});
