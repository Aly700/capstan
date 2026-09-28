import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { Code, ConnectError } from "@connectrpc/connect";
import { afterEach, describe, expect, it } from "vitest";
import type { TerminateRunRequest } from "../gen/capstan/v1/capstan_pb.ts";
import { fakeServer } from "../../test/fake-server.ts";

const closers: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of closers.reverse()) await close(); closers.length = 0; });
const launcher = fileURLToPath(new URL("../../bin/capstan.mjs", import.meta.url));
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

describe("capstan terminate launcher command", () => {
  it.each([undefined, "operator stopped the run"])("sends the run and optional reason (%s)", async (reason) => {
    const result = await invoke(["terminate", "run-1", ...(reason === undefined ? [] : ["--reason", reason])]);
    expect(result.code).toBe(0);
    expect(result.stdout).toBe("OK\n");
    expect(result.stderr).toBe("");
    expect(result.server.requests<TerminateRunRequest>("TerminateRun")).toMatchObject([{ runId: "run-1", reason: reason ?? "" }]);
    expect(result.server.calls).toHaveLength(1);
    expect(result.server.calls[0]!.authorization).toBe("Bearer test-api-key");
  });

  it("documents terminate in help", async () => {
    const result = await invoke(["--help"]);
    expect(result.code).toBe(0);
    expect(result.stdout).toContain("terminate <run-id> [--reason <text>]");
    expect(result.server.calls).toHaveLength(0);
  });

  it.each([
    ["terminate"],
    ["terminate", "run-1", "extra"],
    ["terminate", "run-1", "--reason"],
    ["terminate", "run-1", "--unknown"],
  ])("invalid terminate usage exits 1: %j", async (...args) => {
    const result = await invoke(args as string[]);
    expect(result.code).toBe(1);
    expect(result.stderr).toContain("Usage: capstan");
    expect(result.stdout).toBe("");
    expect(result.server.calls).toHaveLength(0);
  });

  it("requires an API key before sending a request", async () => {
    const result = await invoke(["terminate", "run-1"], {}, { CAPSTAN_API_KEY: "" });
    expect(result.code).toBe(1);
    expect(result.stderr).toContain("CAPSTAN_API_KEY is required");
    expect(result.server.calls).toHaveLength(0);
  });

  it.each([Code.NotFound, Code.FailedPrecondition])("server error code %s exits 2", async (code) => {
    const result = await invoke(["terminate", "run-1", "--reason", "stop"], { terminateRun: () => { throw new ConnectError("termination rejected", code); } });
    expect(result.code).toBe(2);
    expect(result.stderr).toContain("termination rejected");
    expect(result.stdout).toBe("");
    expect(result.server.requests<TerminateRunRequest>("TerminateRun")).toMatchObject([{ runId: "run-1", reason: "stop" }]);
  });
});
