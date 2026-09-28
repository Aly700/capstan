import { timestampDate } from "@bufbuild/protobuf/wkt";
import { Code, ConnectError } from "@connectrpc/connect";
import { RunStatus as ProtoStatus, type RunInfo } from "../gen/capstan/v1/capstan_pb.ts";
import { decode } from "../internal/payload.ts";
import type { RunDescription, RunStatus } from "./index.ts";

export const statuses: Record<RunStatus, ProtoStatus> = {
  running: ProtoStatus.RUNNING, completed: ProtoStatus.COMPLETED, failed: ProtoStatus.FAILED,
  cancelled: ProtoStatus.CANCELLED, timed_out: ProtoStatus.TIMED_OUT, blocked: ProtoStatus.BLOCKED,
  continued_as_new: ProtoStatus.CONTINUED_AS_NEW,
};

export function describe(run: RunInfo | undefined): RunDescription {
  if (!run || !run.startedAt) throw new ConnectError("server response is missing run information", Code.DataLoss);
  const status = (Object.entries(statuses) as Array<[RunStatus, ProtoStatus]>).find(([, value]) => value === run.status)?.[0];
  if (!status) throw new ConnectError(`server returned unknown run status ${run.status}`, Code.DataLoss);
  return {
    runId: run.runId, workflowType: run.workflowType, taskQueue: run.taskQueue, status,
    startedAt: timestampDate(run.startedAt), ...(run.closedAt ? { closedAt: timestampDate(run.closedAt) } : {}),
    lastEventId: Number(run.lastEventId), ...(run.result ? { result: decode(run.result) } : {}),
    ...(run.failure ? { failure: { message: run.failure.message, type: run.failure.type } } : {}),
    costUsd: run.costUsd, pendingActivities: run.pendingActivities, pendingApprovals: run.pendingApprovals,
    ...(run.continuedAsNewRunId ? { continuedAsNewRunId: run.continuedAsNewRunId } : {}),
  };
}
