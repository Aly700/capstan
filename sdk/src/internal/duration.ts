import { create } from "@bufbuild/protobuf";
import { DurationSchema } from "@bufbuild/protobuf/wkt";
import type { Duration as ProtoDuration } from "@bufbuild/protobuf/wkt";
import type { Duration } from "../types.ts";

const units = { ms: 1, s: 1_000, m: 60_000, h: 3_600_000, d: 86_400_000 };

export function durationMs(duration: Duration): number {
  let milliseconds: number;
  if (typeof duration === "number") milliseconds = duration;
  else {
    const match = /^(\d+(?:\.\d+)?|\.\d+)(ms|s|m|h|d)$/.exec(duration);
    if (!match) throw new TypeError(`invalid duration: ${duration}`);
    milliseconds = Number(match[1]) * units[match[2] as keyof typeof units];
  }
  if (!Number.isFinite(milliseconds) || milliseconds < 0) throw new RangeError(`invalid duration: ${duration}`);
  return milliseconds;
}

export function toProtoDuration(duration: Duration): ProtoDuration {
  const milliseconds = durationMs(duration);
  let seconds = Math.floor(milliseconds / 1_000);
  let nanos = Math.round((milliseconds - seconds * 1_000) * 1_000_000);
  if (nanos === 1_000_000_000) { seconds++; nanos = 0; }
  if (seconds > 315_576_000_000) throw new RangeError("duration exceeds the protobuf maximum");
  return create(DurationSchema, { seconds: BigInt(seconds), nanos });
}
