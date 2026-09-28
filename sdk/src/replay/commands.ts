import { create } from "@bufbuild/protobuf";
import { CommandSchema, RetryPolicySchema } from "../gen/capstan/v1/capstan_pb.ts";
import type { Command } from "../gen/capstan/v1/capstan_pb.ts";
import type { ActivityOptions } from "../types.ts";
import { toProtoDuration } from "../internal/duration.ts";
import { encode } from "../internal/payload.ts";

export function command(attributes: Command["attributes"]): Command {
  return create(CommandSchema, { attributes });
}

export function scheduleActivity(seq: number, activityType: string, input: unknown, options: ActivityOptions): Command {
  return create(CommandSchema, { attributes: { case: "scheduleActivity", value: {
    seq: BigInt(seq), activityType, taskQueue: options.taskQueue ?? "", input: encode(input),
    startToCloseTimeout: toProtoDuration(options.startToCloseTimeout),
    ...(options.scheduleToCloseTimeout === undefined ? {} : { scheduleToCloseTimeout: toProtoDuration(options.scheduleToCloseTimeout) }),
    ...(options.scheduleToStartTimeout === undefined ? {} : { scheduleToStartTimeout: toProtoDuration(options.scheduleToStartTimeout) }),
    ...(options.heartbeatTimeout === undefined ? {} : { heartbeatTimeout: toProtoDuration(options.heartbeatTimeout) }),
    ...(options.retry === undefined ? {} : { retryPolicy: create(RetryPolicySchema, {
      ...(options.retry.initialInterval === undefined ? {} : { initialInterval: toProtoDuration(options.retry.initialInterval) }),
      ...(options.retry.maximumInterval === undefined ? {} : { maximumInterval: toProtoDuration(options.retry.maximumInterval) }),
      backoffCoefficient: options.retry.backoffCoefficient ?? 0,
      maximumAttempts: options.retry.maximumAttempts ?? 0,
      nonRetryableErrorTypes: options.retry.nonRetryableErrorTypes ?? [],
    }) }),
  } } });
}
