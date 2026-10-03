import { spawn } from "node:child_process";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { fromJson } from "@bufbuild/protobuf";
import { Code, ConnectError } from "@connectrpc/connect";
import { afterEach, describe, expect, it } from "vitest";
import { ApprovalOutcome, HistoryEventSchema, RunStatus, type GetHistoryRequest, type StartRunRequest } from "../src/gen/capstan/v1/capstan_pb.ts";
import { fakeServer } from "./fake-server.ts";

const closers: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of closers.reverse()) await close(); closers.length = 0; });
const launcher = fileURLToPath(new URL("../bin/capstan.mjs", import.meta.url));
const workflows = fileURLToPath(new URL("./worker-fixtures/workflows.ts", import.meta.url));
async function invoke(args: string[], client: NonNullable<Parameters<typeof fakeServer>[0]>["client"] = {}, env: NodeJS.ProcessEnv = {}) {
  const server = await fakeServer({ ...(client ? { client } : {}) });
  closers.push(server.close);
  const child = spawn(process.execPath, [launcher, ...args], { env: { ...process.env, CAPSTAN_ADDRESS: server.address, CAPSTAN_API_KEY: server.apiKey, ...env }, stdio: ["ignore", "pipe", "pipe"] });
  let stdout = "", stderr = "";
  child.stdout.on("data", (data: Buffer) => { stdout += data.toString(); });
  child.stderr.on("data", (data: Buffer) => { stderr += data.toString(); });
  const code = await new Promise<number | null>((resolve, reject) => { child.once("error", reject); child.once("close", resolve); });
  return { code, stdout, stderr, server };
}
const run = { runId: "r", workflowType: "wf", taskQueue: "q", status: RunStatus.RUNNING, startedAt: { seconds: 1_000n }, lastEventId: 3n };
async function history() { const fixture = JSON.parse(await readFile(new URL("../../conformance/fixtures/001-single-activity.json", import.meta.url), "utf8")); return fixture.history.map((e: unknown) => fromJson(HistoryEventSchema, e as Parameters<typeof fromJson<typeof HistoryEventSchema>>[1])); }

describe("capstan launcher commands", () => {
  it("start sends the workflow, run, queue and JSON input and prints its result", async () => {
    const result = await invoke(["start", "wf", "r", "--queue", "q", "--input", '{"n":21}'], { startRun: () => ({ run, started: true }) });
    expect(result.code).toBe(0);
    expect(JSON.parse(result.stdout)).toMatchObject({ started: true, run: { runId: "r" } });
    const request = result.server.requests<StartRunRequest>("StartRun")[0]!;
    expect(request).toMatchObject({ workflowType: "wf", runId: "r", taskQueue: "q" });
    expect(JSON.parse(Buffer.from(request.input!.data).toString())).toEqual({ n: 21 });
  });
  it("describe prints the run's status and cost", async () => {
    const result = await invoke(["describe", "r"], { describeRun: () => ({ run: { ...run, costUsd: 0.25 } }) });
    expect(result.code).toBe(0);
    expect(JSON.parse(result.stdout)).toMatchObject({ runId: "r", status: "running", costUsd: 0.25 });
    expect(result.server.requests("DescribeRun")[0]).toMatchObject({ runId: "r" });
  });
  it.each([false, true])("history paginates and renders all events (JSON=%s)", async (json) => {
    const events = await history();
    const result = await invoke(["history", "r", ...(json ? ["--json"] : [])], { getHistory: (request) => ({ events: request.afterEventId === 0n ? events.slice(0, 4) : events.slice(4), more: request.afterEventId === 0n }) });
    expect(result.code).toBe(0);
    expect(result.server.requests<GetHistoryRequest>("GetHistory").map((request) => request.afterEventId)).toEqual([0n, 4n]);
    if (json) { const output = JSON.parse(result.stdout); expect(output).toHaveLength(8); expect(output[4]).toMatchObject({ eventId: "5", type: "EVENT_TYPE_ACTIVITY_SCHEDULED" }); }
    else { expect(result.stdout).toContain("2026-09-28T12:00:01"); expect(result.stdout).toContain("ACTIVITY_SCHEDULED"); expect(result.stdout).toContain("double"); }
  });
  it("signal sends its name and input", async () => {
    const result = await invoke(["signal", "r", "go", "--input", "42"]);
    expect(result.code).toBe(0); expect(result.stdout).toContain("OK");
    expect(result.server.requests("SignalRun")[0]).toMatchObject({ runId: "r", name: "go", input: { data: new Uint8Array(Buffer.from("42")) } });
  });
  it.each(["cancel", "resume", "terminate"] as const)("%s sends its run and optional reason", async (command) => {
    const result = await invoke([command, "r", "--reason", "reviewed"]);
    expect(result.code).toBe(0); expect(result.stdout).toContain("OK");
    expect(result.server.requests({ cancel: "CancelRun", resume: "ResumeRun", terminate: "TerminateRun" }[command])[0]).toMatchObject({ runId: "r", reason: "reviewed" });
  });
  it("requires an API key before sending a request", async () => {
    const result = await invoke(["terminate", "run-1"], {}, { CAPSTAN_API_KEY: "" });
    expect(result.code).toBe(1);
    expect(result.stderr).toContain("CAPSTAN_API_KEY is required");
    expect(result.server.calls).toHaveLength(0);
  });
  it.each(["approve", "deny"])("%s sends the approval choice, note and resolver", async (command) => {
    const result = await invoke([command, "r", "approval1", "--choice", "ship", "--note", "checked", "--resolver", "owner"]);
    expect(result.code).toBe(0); expect(result.stdout).toContain("OK");
    expect(result.server.requests("ResolveApproval")[0]).toMatchObject({ runId: "r", approvalId: "approval1", outcome: command === "approve" ? ApprovalOutcome.APPROVED : ApprovalOutcome.DENIED, choice: "ship", note: "checked", resolver: "owner" });
  });
  it("list sends status filter and follows every page", async () => {
    const result = await invoke(["list", "--status", "blocked"], { listRuns: (request) => ({ runs: [{ ...run, runId: request.pageToken ? "r2" : "r1", status: RunStatus.BLOCKED }], nextPageToken: request.pageToken ? "" : "next" }) });
    expect(result.code).toBe(0);
    expect(JSON.parse(result.stdout).map((item: { runId: string }) => item.runId)).toEqual(["r1", "r2"]);
    expect(result.server.requests("ListRuns")).toMatchObject([{ status: RunStatus.BLOCKED, pageToken: "" }, { status: RunStatus.BLOCKED, pageToken: "next" }]);
  });
  it("replay prints OK for a matching history", async () => {
    const result = await invoke(["replay", "r", "--workflows", workflows], { getHistory: async () => ({ events: await history() }) });
    expect(result.code).toBe(0); expect(result.stdout).toContain("OK");
  });
  it("replay exits 3 and names the first mismatch", async () => {
    const events = await history(); if (events[4]!.attributes.case === "activityScheduled") events[4]!.attributes.value.activityType = "changed";
    const result = await invoke(["replay", "r", "--workflows", workflows], { getHistory: () => ({ events }) });
    expect(result.code).toBe(3);
    expect(result.stderr).toContain("history mismatch at event 5: history has ActivityScheduled(seq=1, type=changed), code emitted ScheduleActivity(seq=1, type=double)");
    expect(result.stderr).not.toMatch(/\$typeName|Buffer|\"data\"/);
  });
  it.each([[], ["unknown"], ["start", "wf", "r"], ["signal", "r", "go", "--input", "{"], ["list", "--status", "nonsense"], ["cancel", "r", "--wat"]])("invalid usage exits 1: %j", async (...args) => {
    const result = await invoke(args as string[]);
    expect(result.code).toBe(1); expect(result.stderr).toMatch(/usage|invalid|required|unknown/i); expect(result.server.calls).toHaveLength(0);
  });
  it("server errors exit 2 with an actionable error", async () => {
    const result = await invoke(["describe", "r"], { describeRun: () => { throw new ConnectError("run missing", Code.NotFound); } });
    expect(result.code).toBe(2); expect(result.stderr).toContain("run missing");
  });
});
