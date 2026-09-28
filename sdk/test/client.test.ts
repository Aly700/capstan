import { Code, ConnectError } from "@connectrpc/connect";
import { afterEach, describe, expect, it } from "vitest";
import { Client } from "../src/client/index.ts";
import { ApprovalOutcome, RunStatus, type StartRunRequest } from "../src/gen/capstan/v1/capstan_pb.ts";
import { fakeServer } from "./fake-server.ts";

const closers: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of closers.reverse()) await close(); closers.length = 0; });
async function setup(client: NonNullable<Parameters<typeof fakeServer>[0]>["client"]) {
  const server = await fakeServer({ ...(client ? { client } : {}) });
  closers.push(server.close);
  return { server, client: new Client(server) };
}
const run = { runId: "r", workflowType: "wf", taskQueue: "q", status: RunStatus.RUNNING, startedAt: { seconds: 1_000n, nanos: 500_000_000 }, lastEventId: 7n, costUsd: 0.02, pendingActivities: 2, pendingApprovals: 1 };

describe("client over scripted Connect", () => {
  it("starts with encoded input and timeouts and maps every description field", async () => {
    const { server, client } = await setup({ startRun: () => ({ run, started: false }) });
    const response = await client.start("wf", { n: 3 }, { runId: "r", taskQueue: "q", runTimeout: "1h", taskTimeout: "10s" });
    expect(response).toEqual({ started: false, run: { runId: "r", workflowType: "wf", taskQueue: "q", status: "running", startedAt: new Date(1_000_500), lastEventId: 7, costUsd: 0.02, pendingActivities: 2, pendingApprovals: 1 } });
    const request = server.requests<StartRunRequest>("StartRun")[0]!;
    expect(request).toMatchObject({ runId: "r", workflowType: "wf", taskQueue: "q", runTimeout: { seconds: 3_600n }, taskTimeout: { seconds: 10n } });
    expect(JSON.parse(Buffer.from(request.input!.data).toString())).toEqual({ n: 3 });
    expect(server.calls[0]!.authorization).toBe("Bearer test-api-key");
  });
  it("sends signal dedupe keys, lifecycle reasons and approval decisions", async () => {
    const { server, client } = await setup({});
    await client.signal("r", "go", { ok: true }, "dedupe");
    await client.cancel("r", "stop");
    await client.resume("r", "fixed");
    await client.resolveApproval("r", "a", { outcome: "approved", choice: "ship", resolver: "owner", note: "reviewed" });
    await client.resolveApproval("r", "b", { outcome: "denied", resolver: "owner" });
    expect(server.requests("SignalRun")[0]).toMatchObject({ runId: "r", name: "go", requestId: "dedupe" });
    expect(server.requests("CancelRun")[0]).toMatchObject({ runId: "r", reason: "stop" });
    expect(server.requests("ResumeRun")[0]).toMatchObject({ runId: "r", reason: "fixed" });
    expect(server.requests("ResolveApproval")).toMatchObject([{ runId: "r", approvalId: "a", outcome: ApprovalOutcome.APPROVED, choice: "ship", resolver: "owner", note: "reviewed" }, { approvalId: "b", outcome: ApprovalOutcome.DENIED }]);
  });
  it("maps closed descriptions, payloads, failures and continuation ids", async () => {
    const { client } = await setup({ describeRun: () => ({ run: { ...run, status: RunStatus.CONTINUED_AS_NEW, closedAt: { seconds: 2_000n }, result: { contentType: "application/json", data: Buffer.from('{"answer":42}') }, failure: { message: "failed", type: "UserFailure" }, continuedAsNewRunId: "r~2" } }) });
    expect(await client.describe("r")).toMatchObject({ status: "continued_as_new", closedAt: new Date(2_000_000), result: { answer: 42 }, failure: { message: "failed", type: "UserFailure" }, continuedAsNewRunId: "r~2" });
  });
  it("repeats AwaitRun until closed, including blocked results, and returns the failed description", async () => {
    let count = 0;
    const { server, client } = await setup({ awaitRun: () => ({ closed: ++count === 3, run: { ...run, status: count === 3 ? RunStatus.FAILED : RunStatus.BLOCKED, failure: { type: "Broken", message: "bad" } } }) });
    expect(await client.result("r")).toMatchObject({ status: "failed", failure: { type: "Broken", message: "bad" } });
    expect(server.requests("AwaitRun")).toHaveLength(3);
    expect(server.calls.every((call) => call.timeoutMs !== undefined && call.timeoutMs > 34_000)).toBe(true);
  });
  it("applies an overall result deadline across repeated long polls", async () => {
    const { client } = await setup({ awaitRun: async (_request, context) => { await new Promise<void>((resolve) => context.signal.addEventListener("abort", () => resolve(), { once: true })); return { run, closed: false }; } });
    await expect(client.result("r", { timeoutMs: 40 })).rejects.toMatchObject({ code: Code.DeadlineExceeded });
  });
  it("propagates server failures and rejects missing run information", async () => {
    const { client } = await setup({ startRun: () => { throw new ConnectError("exists", Code.AlreadyExists); }, describeRun: () => ({}) });
    await expect(client.start("wf", undefined, { runId: "r", taskQueue: "q" })).rejects.toMatchObject({ code: Code.AlreadyExists });
    await expect(client.describe("r")).rejects.toThrow(/run/i);
  });
});
