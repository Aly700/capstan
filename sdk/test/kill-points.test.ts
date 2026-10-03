// Replay safety as a property, on the real server and a real SDK worker. For every seed the
// server is SIGKILLed at a seed-chosen committed event count (and on half the seeds the
// worker dies with it); after a restart the run must complete exactly once with the pre-kill
// history prefix byte-identical and every replayed decision equal to the recorded one.
// Runs only when CAPSTAN_E2E=1 (make verify sets it). Reproduce a seed with CAPSTAN_KILL_SEED.
import { existsSync } from "node:fs";
import { join } from "node:path";
import { createClient } from "@connectrpc/connect";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Evidence, root, until } from "./e2e/evidence.ts";
import { Client } from "../src/client/index.ts";
import { createTransport } from "../src/worker/transport.ts";
import { ClientService, EventType, type HistoryEvent } from "../src/gen/capstan/v1/capstan_pb.ts";
import type { Rung } from "./e2e/workflows.ts";

const enabled = process.env.CAPSTAN_E2E === "1";
const seeds = Number(process.env.CAPSTAN_KILL_SEEDS ?? 4);
const firstSeed = Number(process.env.CAPSTAN_KILL_SEED ?? Date.now() % 1_000_000);
const workflows = join(root, "sdk/test/e2e/workflows.ts");

/** Deterministic per-seed choices (mulberry32), so a failing seed reproduces its kill point. */
function rng(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
function decodeInput(event: HistoryEvent): Rung {
  if (event.attributes.case !== "activityScheduled") throw new Error("not an ActivityScheduled event");
  return JSON.parse(Buffer.from(event.attributes.value.input!.data).toString("utf8")) as Rung;
}

describe.skipIf(!enabled)("recovery on the real server", () => {
  const env = new Evidence("kill_points", 7606, { databasePrefix: "capstan_audit" });
  let client: Client;
  let rpc: ReturnType<typeof createClient<typeof ClientService>>;
  const events = (runId: string) => env.sql(`select event_id, type, encode(data, 'hex') from event where run_id = '${runId}' order by event_id`, ["-Atq"]).trim();
  const count = (query: string) => Number(env.sql(query, ["-Atq"]).trim());
  const startWorker = (identity: string, extra: Record<string, string> = {}) =>
    env.child(process.execPath, ["--import", "tsx", join(root, "sdk/test/e2e/worker.ts")], identity, {
      E2E_ADDRESS: env.address, E2E_API_KEY: env.key, E2E_QUEUE: env.queue, E2E_IDENTITY: identity, E2E_WORKFLOWS: workflows, ...extra,
    }, join(root, "sdk"));

  beforeAll(async () => {
    await env.setup();
    client = new Client({ address: env.address, apiKey: env.key });
    rpc = createClient(ClientService, createTransport({ address: env.address, apiKey: env.key }));
  }, 120_000);
  afterAll(() => env.cleanup(), 60_000);

  it(`completes exactly once with its pre-kill history intact over ${seeds} random server kill points (seeds from ${firstSeed})`, async () => {
    let worker = startWorker("ladder-worker");
    for (let seed = firstSeed; seed < firstSeed + seeds; seed++) {
      const draw = rng(seed);
      const steps = 4 + Math.floor(draw() * 3);
      const killAfter = 2 + Math.floor(draw() * steps * 6);
      const killWorkerToo = draw() < 0.5;
      const runId = `ladder-${seed}`;
      const label = `seed ${seed}: ${steps} rungs, kill after ${killAfter} events${killWorkerToo ? " with the worker" : ""}`;

      await client.start("ladder", { steps }, { runId, taskQueue: env.queue });
      await until(`${label}: ${killAfter} committed events`, () => count(`select count(*) from event where run_id = '${runId}'`) >= killAfter);
      await env.stop(env.server, "SIGKILL");
      if (killWorkerToo) await env.stop(worker, "SIGKILL");
      const prefix = events(runId);
      const prefixLength = prefix.split("\n").length;
      await env.startServer();
      if (killWorkerToo) worker = startWorker(`ladder-worker-${seed}`);

      const done = await client.result(runId, { timeoutMs: 60_000 });
      expect(done.status, label).toBe("completed");
      const after = events(runId).split("\n");
      expect(after.slice(0, prefixLength).join("\n"), `${label}: history prefix`).toBe(prefix);
      expect(after.length, label).toBeGreaterThanOrEqual(prefixLength);

      const { events: history } = await rpc.getHistory({ runId });
      expect(history.map((event) => Number(event.eventId)), label).toEqual(history.map((_, index) => index + 1));
      expect(history.filter((event) => event.type === EventType.RUN_COMPLETED), label).toHaveLength(1);
      expect(history.filter((event) => event.type === EventType.ACTIVITY_COMPLETED), label).toHaveLength(steps);
      expect(count(`select last_event_id from run where run_id = '${runId}'`), label).toBe(history.length);
      // The decisions recorded before the kill are the decisions the replayed code reports.
      const recorded = history.filter((event) => event.type === EventType.ACTIVITY_SCHEDULED).map(decodeInput);
      expect(done.result, `${label}: replayed decisions`).toEqual(recorded.map((input, index) => ({ index, value: index * 2, tag: input.tag, dice: input.dice })));
    }
    await env.stop(worker);
  }, 300_000);

  it("a human approval wait survives SIGKILL of the worker and resumes under the same approval id", async () => {
    const marker = join(env.logdir, "ship-marker");
    const doomed = startWorker("ship-doomed");
    const runId = `ship-${Date.now()}`;
    await client.start("shipGate", null, { runId, taskQueue: env.queue });
    const requested = await until("the approval request to be recorded", async () =>
      (await rpc.getHistory({ runId })).events.find((event) => event.type === EventType.APPROVAL_REQUESTED));
    if (requested.attributes.case !== "approvalRequested") throw new Error("approval event missing");
    const approvalId = requested.attributes.value.approvalId;
    // The wait is idle in the database: no task row, no lease, no worker needed.
    await until("the run to hold no task", () => count(`select count(*) from task where run_id = '${runId}'`) === 0);
    await env.stop(doomed, "SIGKILL");
    expect(doomed.signalCode).toBe("SIGKILL");

    await client.resolveApproval(runId, approvalId, { outcome: "approved", resolver: "e2e-reviewer", note: "go" });
    await until("the resolution to be recorded with no worker alive", async () =>
      (await rpc.getHistory({ runId })).events.some((event) => event.type === EventType.APPROVAL_RESOLVED));
    expect(existsSync(marker)).toBe(false);

    const survivor = startWorker("ship-survivor");
    const done = await client.result(runId, { timeoutMs: 30_000 });
    expect(done.status).toBe("completed");
    expect(done.result).toMatchObject({ shipped: true, receipt: "continued", decision: { outcome: "approved", resolver: "e2e-reviewer", note: "go" } });
    const { events: history } = await rpc.getHistory({ runId });
    const requests = history.filter((event) => event.type === EventType.APPROVAL_REQUESTED);
    const resolutions = history.filter((event) => event.type === EventType.APPROVAL_RESOLVED);
    expect(requests).toHaveLength(1);
    expect(resolutions).toHaveLength(1);
    expect(resolutions[0]!.attributes).toMatchObject({ case: "approvalResolved", value: { approvalId } });
    await expect(client.resolveApproval(runId, approvalId, { outcome: "denied", resolver: "late" })).rejects.toThrow();
    await env.stop(survivor);
  }, 90_000);
});
