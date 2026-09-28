import { create } from "@bufbuild/protobuf";
import { FailureSchema } from "../gen/capstan/v1/capstan_pb.ts";
import type { Failure } from "../gen/capstan/v1/capstan_pb.ts";
import { ActivityFailure, ApplicationFailure, CancelledFailure, CapstanFailure, TimeoutFailure } from "../types.ts";
import { decode, encode } from "./payload.ts";

type ErrorFields = { message?: unknown; name?: unknown; type?: unknown; nonRetryable?: unknown; details?: unknown; cause?: unknown; stack?: unknown; activityType?: unknown; seq?: unknown; timeoutType?: unknown };

/** Special SDK fields live in an envelope because Failure has only one opaque details field. */
export function failureToProto(error: unknown): Failure {
  function visit(value: unknown, depth: number): Failure {
    const fields: ErrorFields = value !== null && typeof value === "object" ? value : {};
    const type = typeof fields.type === "string" ? fields.type : typeof fields.name === "string" ? fields.name : "Error";
    let details = fields.details;
    if (type === "ActivityFailure") details = { $capstan: { kind: type, activityType: fields.activityType, seq: fields.seq }, details };
    if (type === "TimeoutFailure") details = { $capstan: { kind: type, timeoutType: fields.timeoutType }, details };
    return create(FailureSchema, {
      message: typeof fields.message === "string" ? fields.message : String(value),
      type,
      nonRetryable: fields.nonRetryable === true,
      stack: process.env.CAPSTAN_KEEP_STACKS === "1" && typeof fields.stack === "string" ? fields.stack : "",
      ...(details === undefined ? {} : { details: encode(details)! }),
      ...(fields.cause === undefined || depth >= 10 ? {} : { cause: visit(fields.cause, depth + 1) }),
    });
  }
  return visit(error, 1);
}

export function failureFromProto(failure: Failure): Error {
  function visit(value: Failure, depth: number): Error {
    const decoded = decode(value.details);
    const envelope = decoded !== null && typeof decoded === "object" ? decoded as { $capstan?: { kind?: unknown; activityType?: unknown; seq?: unknown; timeoutType?: unknown }; details?: unknown } : undefined;
    const metadata = envelope?.$capstan?.kind === value.type ? envelope.$capstan : undefined;
    const details = metadata ? envelope?.details : decoded;
    const cause = value.cause && depth < 10 ? visit(value.cause, depth + 1) : undefined;
    const options = { type: value.type, nonRetryable: value.nonRetryable, details, ...(cause === undefined ? {} : { cause }) };
    let error: CapstanFailure;
    if (value.type === "ActivityFailure") {
      error = new ActivityFailure(value.message, typeof metadata?.activityType === "string" ? metadata.activityType : "", typeof metadata?.seq === "number" ? metadata.seq : 0, options);
    } else if (value.type === "TimeoutFailure") {
      const timeout = metadata?.timeoutType;
      error = new TimeoutFailure(value.message, timeout === "SCHEDULE_TO_START" || timeout === "SCHEDULE_TO_CLOSE" || timeout === "HEARTBEAT" ? timeout : "START_TO_CLOSE", options);
    } else if (value.type === "CancelledFailure") error = new CancelledFailure(value.message, options);
    else if (value.type === "CapstanFailure") error = new CapstanFailure(value.message, options);
    else error = new ApplicationFailure(value.message, options);
    // Specialized constructors intentionally expose fewer options; retain the wire fields too.
    Object.defineProperties(error, { nonRetryable: { value: value.nonRetryable }, details: { value: details } });
    if (process.env.CAPSTAN_KEEP_STACKS === "1" && value.stack) error.stack = value.stack;
    else delete error.stack;
    return error;
  }
  return visit(failure, 1);
}
