// Opt-in proof against the real Go server, shared test PostgreSQL, and the real
// AgentOps Gate compose stack. All owned services and the fresh DB are cleaned up.
import { execFile, spawn, type ChildProcess } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { once } from "node:events";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";
import { toJson } from "@bufbuild/protobuf";
import { createClient } from "@connectrpc/connect";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Client } from "../src/client/index.ts";
import { ClientService, EventType, HistoryEventSchema, type HistoryEvent } from "../src/gen/capstan/v1/capstan_pb.ts";
import { createTransport } from "../src/worker/transport.ts";

const exec = promisify(execFile);
const root = fileURLToPath(new URL("../..", import.meta.url));
const project = "capstan-gate-e2e";
const gateRepo = process.env.CAPSTAN_GATE_REPO ?? fileURLToPath(new URL("../../../agentops-gate", import.meta.url));

async function until<T>(what: string, probe: () => Promise<T | undefined> | T | undefined, timeout = 45_000): Promise<T> {
  const deadline = Date.now() + timeout;
  for (;;) {
    const value = await probe();
    if (value !== undefined) return value;
    if (Date.now() >= deadline) throw new Error(`timed out waiting for ${what}`);
    await delay(100);
  }
}
async function freePort(): Promise<number> {
  const server = createServer();
  server.listen(0, "127.0.0.1"); await once(server, "listening");
  const address = server.address(); if (!address || typeof address === "string") throw new Error("no test port");
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  if (address.port === 7233) return freePort();
  return address.port;
}
function replaceOnce(value: string, old: string, replacement: string): string {
  if (value.split(old).length !== 2) throw new Error("Gate compose layout changed; inspect the source before adapting the E2E copy");
  return value.replace(old, replacement);
}

describe.skipIf(process.env.CAPSTAN_E2E_GATE !== "1")("agent end to end with the real AgentOps Gate", () => {
  const suffix = randomBytes(6).toString("hex");
  const database = `capstan_agent_e2e_${suffix}`;
  const queue = `agent-e2e-${suffix}`;
  const scratch = join(root, ".lane", `agent-e2e-${suffix}`);
  const composeFile = join(scratch, "compose.yml");
  const effectsFile = join(scratch, "effects.jsonl");
  const apiKey = `capstan-test-${randomBytes(24).toString("hex")}`;
  const gateKey = `gate-test-${randomBytes(24).toString("hex")}`;
  const admin = process.env.CAPSTAN_TEST_DATABASE_URL ?? "postgres://capstan:capstan@127.0.0.1:55432/postgres?sslmode=disable";
  const dbUrl = new URL(admin); dbUrl.pathname = `/${database}`;
  const composeEnv = { ...process.env, POSTGRES_USER: "capstan_gate_e2e", POSTGRES_PASSWORD: randomBytes(24).toString("hex"), AGENTOPS_API_KEY: gateKey };
  const children: ChildProcess[] = [];
  const logs = new Map<string, string[]>();
  let address: string;
  let gateUrl: string;
  let policyId: string;
  let client: Client;
  let rpc: ReturnType<typeof createClient<typeof ClientService>>;
  let ownsCompose = false;
  let ownsDatabase = false;
  let sharedPostgres: string | undefined;

  const compose = (args: string[]) => exec("docker", ["compose", "-p", project, "-f", composeFile, ...args], { cwd: scratch, env: composeEnv, maxBuffer: 16 * 1024 * 1024 });
  const sql = async (query: string, url = dbUrl.toString()) => (await exec("psql", ["-X", "-qAt", "-v", "ON_ERROR_STOP=1", url, "-c", query], { env: { ...process.env, PGAPPNAME: "capstan-agent-e2e" } })).stdout.trim();
  const postgresState = async () => (await exec("docker", ["inspect", "--format", "{{.Id}} {{.State.StartedAt}} {{.State.Running}}", "capstan-postgres-1"])).stdout.trim();

  function start(command: string, args: string[], env: NodeJS.ProcessEnv, name: string, cwd = root): ChildProcess {
    const child = spawn(command, args, { cwd, env, detached: true, stdio: ["ignore", "pipe", "pipe", "ipc"] });
    children.push(child);
    const output: string[] = []; logs.set(name, output);
    child.stdout?.on("data", (data: Buffer) => output.push(data.toString()));
    child.stderr?.on("data", (data: Buffer) => output.push(data.toString()));
    child.on("error", () => output.push("test child process failed to start"));
    return child;
  }
  async function stop(child: ChildProcess, signal: NodeJS.Signals = "SIGTERM") {
    if (child.pid && child.exitCode === null && child.signalCode === null) {
      try { process.kill(-child.pid, signal); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; }
    }
    await until("owned process exit", () => child.exitCode !== null || child.signalCode !== null ? true : undefined, 10_000);
  }
  async function worker(identity: string): Promise<ChildProcess> {
    const child = start(process.execPath, ["--import", "tsx", join(root, "sdk/test/agent-e2e/worker.ts")], {
      ...process.env, AGENT_E2E_ADDRESS: address, AGENT_E2E_API_KEY: apiKey, AGENT_E2E_QUEUE: queue,
      AGENT_E2E_IDENTITY: identity, AGENT_E2E_GATE_URL: gateUrl, AGENT_E2E_GATE_KEY: gateKey,
      AGENT_E2E_POLICY: policyId, AGENT_E2E_EFFECTS: effectsFile,
    }, identity, join(root, "sdk"));
    let ready = false;
    child.on("message", (message) => { if ((message as { event?: string }).event === "ready") ready = true; });
    await until("worker readiness", () => {
      if (child.exitCode !== null || child.signalCode !== null) throw new Error(`worker ${identity} exited before readiness`);
      return ready ? true : undefined;
    });
    return child;
  }
  async function gatePost(path: string, body: unknown): Promise<Record<string, any>> {
    const response = await fetch(`${gateUrl}${path}`, { method: "POST", headers: { "X-API-Key": gateKey, "Content-Type": "application/json" }, body: JSON.stringify(body), signal: AbortSignal.timeout(10_000) });
    if (!response.ok) throw new Error(`Gate setup HTTP ${response.status}`);
    return await response.json() as Record<string, any>;
  }

  beforeAll(async () => {
    await mkdir(scratch, { recursive: true });
    await writeFile(effectsFile, "");
    sharedPostgres = await postgresState();
    const existing = await exec("docker", ["ps", "-aq", "--filter", `label=com.docker.compose.project=${project}`]);
    if (existing.stdout.trim()) throw new Error("capstan-gate-e2e already has containers; refusing to change a pre-existing stack");
    const ports = new Set<number>();
    while (ports.size < 3) ports.add(await freePort());
    const [serverPort, gatePort, localstackPort] = [...ports] as [number, number, number];
    for (const port of ports) {
      try {
        await exec("lsof", ["-nP", `-iTCP:${port}`, "-sTCP:LISTEN"]);
        throw new Error(`E2E port ${port} already has a listener`);
      } catch (error) {
        if ((error as { code?: number }).code !== 1) throw error;
      }
    }
    await writeFile(join(scratch, "ports.json"), JSON.stringify({ serverPort, gatePort, localstackPort, lsof: "all three checked: exit 1, no listeners" }, null, 2));
    address = `http://127.0.0.1:${serverPort}`;
    gateUrl = `http://127.0.0.1:${gatePort}`;
    let copy = await readFile(join(gateRepo, "docker-compose.yml"), "utf8");
    copy = replaceOnce(copy, "context: .", `context: ${JSON.stringify(gateRepo)}`);
    copy = replaceOnce(copy, '"4566:4566"', JSON.stringify(`127.0.0.1:${localstackPort}:4566`));
    copy = replaceOnce(copy, '"8080:8080"', JSON.stringify(`127.0.0.1:${gatePort}:8080`));
    copy = replaceOnce(copy, "./localstack/init-aws.sh:/etc/localstack/init/ready.d/init-aws.sh:ro", JSON.stringify(`${join(gateRepo, "localstack/init-aws.sh")}:/etc/localstack/init/ready.d/init-aws.sh:ro`));
    await writeFile(composeFile, copy);
    ownsCompose = true;
    try {
      const up = await compose(["up", "--build", "-d", "--wait", "--wait-timeout", "240"]);
      await writeFile(join(scratch, "compose-up.log"), up.stdout + up.stderr);
    } catch (error) {
      const output = error as { stdout?: string; stderr?: string };
      await writeFile(join(scratch, "compose-up.log"), (output.stdout ?? "") + (output.stderr ?? ""));
      throw new Error("Gate compose setup failed; see .lane agent-e2e compose-up.log");
    }
    await until("Gate health", async () => {
      try { return (await fetch(`${gateUrl}/actuator/health`, { signal: AbortSignal.timeout(2_000) })).ok ? true : undefined; } catch { return undefined; }
    });
    policyId = String((await gatePost("/policies", { name: `capstan-${suffix}`, version: 1 })).id);
    for (const [toolNameGlob, effect, precedence] of [["tool.deny", "DENY", 10], ["tool.approve", "REQUIRE_APPROVAL", 20], ["tool.allow", "ALLOW", 30]] as const) {
      await gatePost(`/policies/${policyId}/rules`, { toolNameGlob, effect, precedence });
    }
    await sql(`create database ${database}`, admin); ownsDatabase = true;
    const server = start("go", ["run", "./cmd/capstan-server"], {
      ...process.env, GOTOOLCHAIN: "go1.26.4", CAPSTAN_DATABASE_URL: dbUrl.toString(),
      CAPSTAN_API_KEY_HASHES: `agent-e2e:${createHash("sha256").update(apiKey).digest("hex")}`,
      CAPSTAN_ADDR: `127.0.0.1:${serverPort}`, CAPSTAN_ADDRESS: address, CAPSTAN_POLL_TIMEOUT: "1s", CAPSTAN_LOG_LEVEL: "warn",
      CAPSTAN_GATE_URL: gateUrl, CAPSTAN_GATE_API_KEY: gateKey,
    }, "server");
    await until("Capstan readiness", async () => {
      if (server.exitCode !== null || server.signalCode !== null) throw new Error("Capstan server exited before readiness");
      try { return (await fetch(`${address}/readyz`, { signal: AbortSignal.timeout(2_000) })).ok ? true : undefined; } catch { return undefined; }
    }, 90_000);
    client = new Client({ address, apiKey });
    rpc = createClient(ClientService, createTransport({ address, apiKey }));
  }, 600_000);

  afterAll(async () => {
    const errors: unknown[] = [];
    for (const child of [...children].reverse()) {
      try { await stop(child, "SIGKILL"); } catch (error) { errors.push(error); }
    }
    if (ownsCompose) {
      try {
        const status = await compose(["ps", "--format", "json"]);
        await writeFile(join(scratch, "compose-before-down.json"), status.stdout);
      } catch (error) { errors.push(error); }
      // Diagnostic collection must never prevent cleanup of the owned stack.
      try {
        const down = await compose(["down", "-v"]);
        await writeFile(join(scratch, "compose-down.log"), down.stdout + down.stderr);
        expect((await compose(["ps", "-aq"])).stdout.trim()).toBe("");
        expect((await exec("docker", ["volume", "ls", "-q", "--filter", `label=com.docker.compose.project=${project}`])).stdout.trim()).toBe("");
      } catch (error) { errors.push(error); }
    }
    if (ownsDatabase) {
      try {
        await sql(`drop database ${database} with (force)`, admin);
        expect(await sql(`select count(*) from pg_database where datname='${database}'`, admin)).toBe("0");
      } catch (error) { errors.push(error); }
    }
    for (const [name, output] of logs) {
      const content = output.join("");
      // Verify secrets before saving even the diagnostic logs.
      try { expect(content.includes(gateKey) || content.includes(apiKey)).toBe(false); }
      catch (error) { errors.push(error); }
      await writeFile(join(scratch, `${name}.log`), content.replaceAll(gateKey, "[redacted]").replaceAll(apiKey, "[redacted]"));
    }
    if (sharedPostgres !== undefined) {
      try { expect(await postgresState()).toBe(sharedPostgres); } catch (error) { errors.push(error); }
    }
    if (errors.length) throw new AggregateError(errors, "E2E cleanup or credential check failed");
  }, 120_000);

  async function history(runId: string): Promise<HistoryEvent[]> {
    const result = await rpc.getHistory({ runId });
    expect(result.more).toBe(false);
    const json = result.events.map((event) => toJson(HistoryEventSchema, event));
    const decoded = JSON.stringify(result.events, (_key, value: unknown) => {
      if (value instanceof Uint8Array) return Buffer.from(value).toString("utf8");
      return typeof value === "bigint" ? String(value) : value;
    });
    expect(decoded.includes(gateKey) || decoded.includes(apiKey)).toBe(false);
    await writeFile(join(scratch, `${runId}-history.json`), JSON.stringify(json, null, 2));
    return result.events;
  }
  async function effects(runId: string) {
    const lines = (await readFile(effectsFile, "utf8")).trim();
    return (lines ? lines.split("\n").map((line) => JSON.parse(line) as { runId: string; pid: number; activityType: string; idempotencyKey: string }) : []).filter((effect) => effect.runId === runId);
  }
  async function launch(name: string, label: string): Promise<string> {
    const runId = `agent-${suffix}-${label}`;
    await client.start("gated", { name, arguments: { label } }, { runId, taskQueue: queue });
    return runId;
  }
  async function pending(runId: string) {
    await until("durable approval with no held task", async () => {
      const run = await client.describe(runId);
      if (run.status !== "running") throw new Error(`approval run ended as ${run.status}`);
      if (run.pendingApprovals !== 1 || run.pendingActivities !== 0) return undefined;
      return await sql(`select count(*) from task where run_id='${runId}'`) === "0" ? true : undefined;
    });
    expect(await sql(`select in_flight from run where run_id='${runId}'`)).toBe("f");
    const requested = (await history(runId)).find((e) => e.attributes.case === "approvalRequested");
    if (requested?.attributes.case !== "approvalRequested") throw new Error("approval event missing");
    expect(await effects(runId)).toHaveLength(0);
    return requested.attributes.value;
  }
  async function approve(approvalId: string) {
    // Required real curl approval. Supply the key via stdin, not process arguments.
    const child = spawn("curl", ["--fail", "--silent", "--show-error", "--max-time", "10", "-X", "POST", `${gateUrl}/approvals/${approvalId}/approve`, "--config", "-"], { stdio: ["pipe", "pipe", "pipe"] });
    let stdout = ""; let stderr = "";
    child.stdout.on("data", (data: Buffer) => { stdout += data.toString(); });
    child.stderr.on("data", (data: Buffer) => { stderr += data.toString(); });
    child.stdin.end(`header = "X-API-Key: ${gateKey}"\nheader = "Content-Type: application/json"\ndata = "{\\"decidedBy\\":\\"capstan-e2e-reviewer\\"}"\n`);
    const [code] = await once(child, "close");
    if (code !== 0) throw new Error(`curl approval failed (${String(code)}): ${stderr.replaceAll(gateKey, "[redacted]")}`);
    const approval = JSON.parse(stdout) as { status: string; decidedBy: string };
    expect(approval.status).toBe("APPROVED");
    expect(approval.decidedBy).toBe("capstan-e2e-reviewer");
  }
  async function assertSingleTool(runId: string, seq: number) {
    expect(await effects(runId)).toMatchObject([{ activityType: expect.stringMatching(/^tool\./), idempotencyKey: `${runId}/${seq}` }]);
    expect(await effects(runId)).toHaveLength(1);
    const events = await history(runId);
    const scheduled = events.flatMap((e) => e.attributes.case === "activityScheduled" ? [e.attributes.value] : []);
    expect(scheduled.map((activity) => activity.activityType)).toEqual(["capstan.gate.decide", expect.stringMatching(/^tool\./)]);
    expect(events.filter((e) => e.type === EventType.ACTIVITY_COMPLETED)).toHaveLength(2);
  }

  it("ALLOW executes the registered tool once", async () => {
    const w = await worker("allow-worker");
    const runId = await launch("tool.allow", "allow");
    const result = await client.result(runId, { timeoutMs: 30_000 });
    expect(result.status).toBe("completed");
    expect(result.result).toMatchObject({ allowed: true, value: { executed: "tool.allow", input: { label: "allow" } } });
    await assertSingleTool(runId, 2);
    await stop(w);
  }, 45_000);

  it("DENY completes without executing the registered tool", async () => {
    const w = await worker("deny-worker");
    const runId = await launch("tool.deny", "deny");
    const result = await client.result(runId, { timeoutMs: 30_000 });
    expect(result.status).toBe("completed");
    expect(result.result).toMatchObject({ allowed: false, reason: "denied" });
    expect(await effects(runId)).toHaveLength(0);
    expect((await history(runId)).filter((e) => e.type === EventType.ACTIVITY_SCHEDULED)).toHaveLength(1);
    await stop(w);
  }, 45_000);

  it("a real Gate approval waits durably while the single-slot worker runs other work", async () => {
    const w = await worker("approval-worker");
    const runId = await launch("tool.approve", "approved");
    const approval = await pending(runId);
    const probeId = `${runId}-probe`;
    await client.start("capacityProbe", null, { runId: probeId, taskQueue: queue });
    const probe = await client.result(probeId, { timeoutMs: 10_000 });
    expect(probe.status).toBe("completed");
    expect(probe.result).toEqual({ pid: w.pid });
    await approve(approval.approvalId);
    const result = await client.result(runId, { timeoutMs: 60_000 });
    expect(result.status).toBe("completed");
    expect(result.result).toMatchObject({ allowed: true, approvedBy: "capstan-e2e-reviewer" });
    await assertSingleTool(runId, 3);
    await stop(w);
  }, 90_000);

  it("survives SIGKILL during approval, observes approval with no worker, and resumes once", async () => {
    const doomed = await worker("doomed-worker");
    const runId = await launch("tool.approve", "killed");
    const approval = await pending(runId);
    await stop(doomed, "SIGKILL");
    expect(doomed.signalCode).toBe("SIGKILL");
    await approve(approval.approvalId);
    await until("server to observe Gate approval without a worker", async () => {
      return (await rpc.getHistory({ runId })).events.some((e) => e.type === EventType.APPROVAL_RESOLVED) ? true : undefined;
    }, 60_000);
    expect(await sql(`select count(*) from task where run_id='${runId}' and leased_until is not null`)).toBe("0");
    expect(await effects(runId)).toHaveLength(0);
    const survivor = await worker("survivor-worker");
    const result = await client.result(runId, { timeoutMs: 30_000 });
    expect(result.status).toBe("completed");
    expect(result.result).toMatchObject({ allowed: true, approvedBy: "capstan-e2e-reviewer" });
    await assertSingleTool(runId, 3);
    expect((await effects(runId))[0]!.pid).toBe(survivor.pid);
    const events = await history(runId);
    expect(events.filter((e) => e.type === EventType.APPROVAL_REQUESTED)).toHaveLength(1);
    const resolved = events.filter((e) => e.type === EventType.APPROVAL_RESOLVED);
    expect(resolved).toHaveLength(1);
    // The survivor resumed the wait the doomed worker started: same approval, resolved once.
    expect(resolved[0]!.attributes).toMatchObject({ case: "approvalResolved", value: { approvalId: approval.approvalId } });
    await stop(survivor);
  }, 100_000);
});
