// Shared real-process harness. Only databases created by this instance are removed.
import { spawn, execFileSync } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { mkdirSync, openSync, closeSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { createServer } from "node:net";
import { setTimeout as delay } from "node:timers/promises";

export const root = dirname(dirname(fileURLToPath(import.meta.url)));
export const payload = (value) => ({ contentType: "application/json", data: Buffer.from(JSON.stringify(value)).toString("base64") });
export const decode = (value) => value ? JSON.parse(Buffer.from(value.data || "", "base64").toString()) : undefined;
export { delay };
export async function until(what, probe, timeout = 60000) {
  const deadline = Date.now() + timeout;
  for (;;) {
    const result = await probe();
    if (result) return result;
    if (Date.now() >= deadline) throw new Error(`Timed out: ${what}`);
    await delay(100);
  }
}
export class Evidence {
  constructor(name, port, { databasePrefix = "capstan_evidence" } = {}) {
    if (!/^[a-z0-9_]+$/.test(name) || port < 7300 || port > 7499) throw new Error("Invalid evidence name or port");
    if (!["capstan_evidence", "capstan_perf"].includes(databasePrefix)) throw new Error("Invalid database prefix");
    this.name = name;
    this.port = port;
    this.address = `http://127.0.0.1:${port}`;
    this.database = `${databasePrefix}_${name}_${process.pid}_${Date.now()}`;
    this.admin = process.env.CAPSTAN_TEST_DATABASE_URL || "postgres://capstan:capstan@127.0.0.1:55432/postgres?sslmode=disable";
    const url = new URL(this.admin); url.pathname = `/${this.database}`;
    this.dsn = url.toString();
    this.key = `cap_evidence_${randomBytes(24).toString("hex")}`;
    this.queue = `evidence-${name}`;
    this.logdir = join(root, ".lane", `${name}-${Date.now()}`);
    mkdirSync(this.logdir, { recursive: true });
    this.children = [];
    this.workers = [];
    this.created = false;
  }
  ensureActive() {
    if (this.closing) throw new Error("Evidence cleanup has begun; refusing new resources");
  }
  sql(query, args = []) {
    return execFileSync("psql", [this.dsn, "-X", "-v", "ON_ERROR_STOP=1", ...args, "-c", query], { encoding: "utf8" });
  }
  async setup() {
    this.ensureActive();
    mkdirSync(join(root, ".lane/bin"), { recursive: true });
    execFileSync("go", ["build", "-o", ".lane/bin/capstan-server", "./cmd/capstan-server"], { cwd: root, env: { ...process.env, GOTOOLCHAIN: "go1.26.4" }, stdio: "inherit" });
    execFileSync("psql", [this.admin, "-X", "-v", "ON_ERROR_STOP=1", "-qc", `create database ${this.database}`]);
    this.created = true;
    await this.startServer();
    return this;
  }
  child(binary, args, name, extraEnv = {}, cwd = root) {
    this.ensureActive();
    const fd = openSync(join(this.logdir, `${name}.log`), "a", 0o600);
    // As in sdk/test/e2e.test.ts: the worker is one Node process, not a tsx wrapper.
    const child = spawn(binary, args, { cwd, env: { ...process.env, ...extraEnv }, detached: true, stdio: ["ignore", fd, fd] });
    closeSync(fd);
    child.spawnError = undefined;
    child.on("error", (error) => { child.spawnError = error; });
    this.children.push(child);
    return child;
  }
  async startServer() {
    this.ensureActive();
    const probe = createServer();
    await new Promise((resolve, reject) => {
      probe.once("error", reject);
      probe.listen(this.port, "127.0.0.1", () => probe.close(resolve));
    });
    this.server = this.child(join(root, ".lane/bin/capstan-server"), ["serve"], "server", {
      CAPSTAN_DATABASE_URL: this.dsn, CAPSTAN_API_KEY_HASHES: `evidence:${createHash("sha256").update(this.key).digest("hex")}`,
      CAPSTAN_ADDR: `127.0.0.1:${this.port}`, CAPSTAN_POLL_TIMEOUT: "2s", CAPSTAN_LOG_LEVEL: "warn",
    });
    await until("server readiness", async () => {
      if (this.server.spawnError || this.server.exitCode !== null) throw new Error("Server exited; inspect .lane logs");
      try {
        if (!(await fetch(`${this.address}/readyz`)).ok) return false;
        await this.rpc("ListRuns", { pageSize: 1 });
        return true;
      } catch { return false; }
    });
  }
  worker(workflows = "workflows.ts", index = this.workers.length, options = {}) {
    const child = this.child(process.execPath, ["--import", "tsx", join(root, "examples/evidence/worker.ts")], `worker-${index}`, {
      CAPSTAN_ADDRESS: this.address, CAPSTAN_API_KEY: this.key, EVIDENCE_QUEUE: this.queue,
      EVIDENCE_WORKFLOWS: join(root, "examples/evidence", workflows), EVIDENCE_IDENTITY: `evidence-${this.name}-${index}`,
      EVIDENCE_WORKFLOW_CONCURRENCY: String(options.workflowConcurrency ?? 10), EVIDENCE_ACTIVITY_CONCURRENCY: String(options.activityConcurrency ?? 10),
    }, join(root, "sdk"));
    this.workers.push(child);
    return child;
  }
  async stop(child, signal = "SIGTERM") {
    if (!child || child.pid === undefined || child.exitCode !== null || child.signalCode !== null) return;
    try { process.kill(-child.pid, signal); } catch (error) { if (error.code !== "ESRCH") throw error; }
    try { await until("process exit", () => child.exitCode !== null || child.signalCode !== null, 5000); }
    catch { try { process.kill(-child.pid, "SIGKILL"); } catch {} await until("killed process exit", () => child.exitCode !== null || child.signalCode !== null, 5000); }
  }
  async stopWorkers() { await Promise.all(this.workers.map((worker) => this.stop(worker))); }
  async rpc(method, body = {}) {
    const response = await fetch(`${this.address}/capstan.v1.ClientService/${method}`, {
      method: "POST", headers: { "Content-Type": "application/json", "Connect-Protocol-Version": "1", Authorization: `Bearer ${this.key}` },
      body: JSON.stringify(body), signal: AbortSignal.timeout(35000),
    });
    if (!response.ok) throw new Error(`${method}: HTTP ${response.status} ${await response.text()}`);
    return response.json();
  }
  async start(type, runId, input = {}) {
    return this.rpc("StartRun", { runId, workflowType: type, taskQueue: this.queue, input: payload(input), runTimeout: "180s" });
  }
  async status(runId, status) {
    return until(`${runId} ${status}`, async () => {
      const { run } = await this.rpc("DescribeRun", { runId });
      if (run.status === status) return run;
      if (["RUN_STATUS_FAILED", "RUN_STATUS_TIMED_OUT", "RUN_STATUS_CANCELLED"].includes(run.status)) throw new Error(`${runId}: ${run.status} ${run.failure?.message || ""}`);
    });
  }
  async history(runId) {
    const events = [];
    for (;;) {
      const page = await this.rpc("GetHistory", { runId, afterEventId: events.at(-1)?.eventId || "0", pageSize: 5000 });
      events.push(...(page.events || []));
      if (!page.more) return events;
      if (!page.events?.length) throw new Error("History pagination did not advance");
    }
  }
  cli(...args) {
    const quote = (value) => /^[A-Za-z0-9_./:-]+$/.test(value) ? value : "'" + value.replaceAll("'", "'\\''") + "'";
    console.log(`$ capstan ${args.map(quote).join(" ")}`);
    const result = execFileSync(process.execPath, [join(root, "sdk/bin/capstan.mjs"), ...args], { cwd: root, env: { ...process.env, CAPSTAN_ADDRESS: this.address, CAPSTAN_API_KEY: this.key }, encoding: "utf8" });
    process.stdout.write(result);
    return result;
  }
  async cleanup() {
    if (this.cleaning) return this.cleaning;
    // Close registration synchronously, before any stop operation yields.
    this.closing = true;
    this.cleaning = (async () => {
      await Promise.all([...this.children].reverse().map((child) => this.stop(child)));
      if (this.created) {
        execFileSync("psql", [this.admin, "-X", "-v", "ON_ERROR_STOP=1", "-qc", `drop database ${this.database} with (force)`]);
        this.created = false;
      }
    })();
    return this.cleaning;
  }
}
export async function managed(evidence, action) {
  let handlingSignal = false;
  const interrupted = async () => { if (handlingSignal) return; handlingSignal = true; await evidence.cleanup(); process.exit(130); };
  process.once("SIGINT", interrupted); process.once("SIGTERM", interrupted);
  try { await evidence.setup(); evidence.ensureActive(); return await action(evidence); }
  finally { await evidence.cleanup(); process.removeListener("SIGINT", interrupted); process.removeListener("SIGTERM", interrupted); }
}
