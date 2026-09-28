// End to end: the real capstan-server on PostgreSQL, a real worker process, the conformance
// workflows. Runs only when CAPSTAN_E2E=1 (make verify sets it).
import { spawn, execFileSync, type ChildProcess } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { createClient } from "@connectrpc/connect";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Client } from "../src/client/index.ts";
import { createTransport } from "../src/worker/transport.ts";
import { ClientService, EventType } from "../src/gen/capstan/v1/capstan_pb.ts";

const enabled = process.env.CAPSTAN_E2E === "1";
const root = fileURLToPath(new URL("../..", import.meta.url));
const port = Number(process.env.CAPSTAN_E2E_PORT ?? 7609);
const address = `http://127.0.0.1:${port}`;
const apiKey = `cap_e2e_${randomBytes(16).toString("hex")}`;
const adminUrl = process.env.CAPSTAN_TEST_DATABASE_URL ?? "postgres://capstan:capstan@127.0.0.1:55432/postgres?sslmode=disable";
const database = `capstan_audit_e2e_${process.pid}_${Date.now()}`;
const scratch = mkdtempSync(join(tmpdir(), "capstan-e2e-"));
const children: ChildProcess[] = [];

function databaseUrl(name: string): string {
  const url = new URL(adminUrl);
  url.pathname = `/${name}`;
  return url.toString();
}

function startWorker(identity: string, env: Record<string, string> = {}): ChildProcess {
  // One node process in its own process group (tsx as a loader, not as a wrapper that forks),
  // so kill() reaches the worker itself.
  const child = spawn(process.execPath, ["--import", "tsx", join(root, "sdk/test/e2e/worker.ts")], {
    cwd: join(root, "sdk"),
    env: { ...process.env, E2E_ADDRESS: address, E2E_API_KEY: apiKey, E2E_QUEUE: "e2e", E2E_IDENTITY: identity, ...env },
    stdio: ["ignore", "ignore", "inherit"],
    detached: true,
  });
  children.push(child);
  return child;
}

function kill(child: ChildProcess): void {
  if (child.pid !== undefined && child.exitCode === null && child.signalCode === null) {
    try { process.kill(-child.pid, "SIGKILL"); } catch { /* already gone */ }
  }
}

async function exited(child: ChildProcess): Promise<void> {
  await until("a process to exit", () => (child.exitCode !== null || child.signalCode !== null ? true : undefined));
}

async function until<T>(what: string, probe: () => Promise<T | undefined> | T | undefined, timeoutMs = 30_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await probe();
    if (value !== undefined) return value;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await delay(100);
  }
}

describe.skipIf(!enabled)("end to end on the real server", () => {
  const client = new Client({ address, apiKey });
  const rpc = createClient(ClientService, createTransport({ address, apiKey }));

  beforeAll(async () => {
    execFileSync("psql", [adminUrl, "-qc", `create database ${database}`]);
    const binary = join(scratch, "capstan-server");
    execFileSync("go", ["build", "-o", binary, "./cmd/capstan-server"], { cwd: root, stdio: "inherit" });
    const server = spawn(binary, [], {
      env: {
        ...process.env,
        CAPSTAN_DATABASE_URL: databaseUrl(database),
        CAPSTAN_API_KEY_HASHES: `e2e:${createHash("sha256").update(apiKey).digest("hex")}`,
        CAPSTAN_ADDR: `127.0.0.1:${port}`,
        CAPSTAN_ADDRESS: address,
        CAPSTAN_POLL_TIMEOUT: "2s",
        CAPSTAN_LOG_LEVEL: "warn",
      },
      stdio: ["ignore", "ignore", "inherit"],
      detached: true,
    });
    children.push(server);
    await until("server readiness", async () => {
      try { return (await fetch(`${address}/readyz`)).ok ? true : undefined; } catch { return undefined; }
    });
  }, 120_000);

  afterAll(async () => {
    for (const child of children.reverse()) kill(child);
    await Promise.all(children.map(exited));
    try { execFileSync("psql", [adminUrl, "-qc", `drop database if exists ${database} with (force)`]); } catch { /* best effort */ }
    rmSync(scratch, { recursive: true, force: true });
  });

  async function completions(runId: string): Promise<number> {
    const { events } = await rpc.getHistory({ runId });
    return events.filter((e) => e.type === EventType.ACTIVITY_COMPLETED).length;
  }

  it("runs singleActivity to completion", async () => {
    const worker = startWorker("e2e-a");
    const runId = `single-${Date.now()}`;
    await client.start("singleActivity", { n: 21 }, { runId, taskQueue: "e2e" });
    const done = await client.result(runId, { timeoutMs: 30_000 });
    expect(done.status).toBe("completed");
    expect(done.result).toEqual({ doubled: 42 });
    expect(await completions(runId)).toBe(1);
    kill(worker);
    await exited(worker);
  }, 60_000);

  it("finishes a run whose worker is killed mid-activity, with one completion", async () => {
    const marker = join(scratch, "hanging");
    const doomed = startWorker("e2e-doomed", { E2E_HANG: marker });
    const runId = `killed-${Date.now()}`;
    await client.start("singleActivity", { n: 5 }, { runId, taskQueue: "e2e" });
    await until("the activity to start", () => (existsSync(marker) ? true : undefined));
    kill(doomed);
    await exited(doomed);
    startWorker("e2e-survivor");
    // singleActivity's start-to-close timeout is 10s; the retry lands on the survivor.
    const done = await client.result(runId, { timeoutMs: 45_000 });
    expect(done.status).toBe("completed");
    expect(done.result).toEqual({ doubled: 10 });
    expect(await completions(runId)).toBe(1);
    const { events } = await rpc.getHistory({ runId });
    const timedOut = events.filter((e) => e.type === EventType.ACTIVITY_TIMED_OUT).length;
    expect(timedOut).toBe(0); // a retried attempt is not a history event (D4)
  }, 90_000);

  it("terminates a sleeping run and keeps its history closed past the timer deadline", async () => {
    const taskQueue = "e2e-terminate";
    const worker = startWorker("e2e-terminate", { E2E_QUEUE: taskQueue });
    try {
      const runId = `terminated-${Date.now()}`;
      await client.start("sleeping", undefined, { runId, taskQueue });
      const waiting = await until("the one-second timer to start", async () => {
        const { events } = await rpc.getHistory({ runId });
        return events.some((event) => event.type === EventType.TIMER_STARTED) ? events : undefined;
      });
      expect(waiting.some((event) => event.type === EventType.TIMER_FIRED)).toBe(false);
      await client.terminate(runId, "operator stopped the sleeping run");
      const done = await client.describe(runId);
      expect(done.status).toBe("failed");
      expect(done.failure).toEqual({ type: "Terminated", message: "operator stopped the sleeping run" });

      const closed = await rpc.getHistory({ runId });
      const terminal = closed.events.at(-1)!;
      expect(terminal.type).toBe(EventType.RUN_FAILED);
      expect(terminal.attributes).toMatchObject({ case: "runFailed", value: { taskCompletedEventId: 0n, failure: { type: "Terminated" } } });
      expect(closed.events.some((event) => event.type === EventType.TIMER_FIRED)).toBe(false);
      // sleeping uses a 1s timer, which started before termination. Cross its due time.
      await delay(1_200);
      const afterDeadline = await rpc.getHistory({ runId });
      expect(afterDeadline.events.filter((event) => event.eventId > terminal.eventId && event.type === EventType.TIMER_FIRED)).toEqual([]);
      expect(afterDeadline.events).toEqual(closed.events);
      expect((await client.describe(runId)).status).toBe("failed");
    } finally {
      kill(worker);
      await exited(worker);
    }
  }, 60_000);
});
