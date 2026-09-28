// Independent audit: real server processes, PostgreSQL, HTTP RPCs and SDK workers.
import { describe, it, expect } from "vitest";
import { Evidence, managed, until, payload, decode, delay, root } from "../../scripts/evidence-lib.mjs";
import { join } from "node:path";

const enabled = process.env.CAPSTAN_E2E === "1";
async function worker(env, method, body = {}, address = env.address) {
  const response = await fetch(`${address}/capstan.v1.WorkerService/${method}`, {
    method: "POST", headers: { "Content-Type": "application/json", "Connect-Protocol-Version": "1", Authorization: `Bearer ${env.key}` },
    body: JSON.stringify(body), signal: AbortSignal.timeout(15000),
  });
  const value = await response.json();
  if (!response.ok) { const error = new Error(value.message); error.code = value.code; throw error; }
  return value;
}
const poll = (env, queue = env.queue) => until("workflow task becomes claimable", async () => {
  const task = await worker(env, "PollWorkflowTask", { taskQueue: queue, identity: "audit" });
  return task.taskToken ? task : undefined;
});
const pollActivity = (env) => until("activity task becomes claimable", async () => {
  const task = await worker(env, "PollActivityTask", { taskQueue: env.queue });
  return task.taskToken ? task : undefined;
});
const complete = (env, task, commands = []) => worker(env, "CompleteWorkflowTask", { taskToken: task.taskToken, commands });
const timer = (seq, fireAfter = "0.15s") => ({ startTimer: { seq, fireAfter } });
const finish = { completeRun: { result: payload("done") } };

// Every process belongs to Evidence's cleanup list, including the second server.
describe.skipIf(!enabled)("independent audit on real server processes", () => {
  it("rejects expired/duplicate completions and retains a keyed effect across a server kill", async () => {
    await managed(new Evidence("audit_recovery", 7601, { databasePrefix: "capstan_audit" }), async (env) => {
      await env.rpc("StartRun", { runId: "effect", workflowType: "manual", taskQueue: env.queue, taskTimeout: "1s" });
      const expiredWorkflow = await poll(env);
      await delay(1100);
      await expect(complete(env, expiredWorkflow, [finish])).rejects.toMatchObject({ code: "failed_precondition" });
      const retried = await poll(env);
      expect(Number(retried.attempt)).toBeGreaterThan(1);
      await complete(env, retried, [{ scheduleActivity: { seq: "1", activityType: "effect", startToCloseTimeout: "1s", retryPolicy: { initialInterval: "0.1s", maximumAttempts: 5 } } }]);
      const first = await pollActivity(env);
      env.sql("create table audit_effect (key text primary key)");
      const apply = (key) => { expect(key).toBe("effect/1"); env.sql("insert into audit_effect values ('effect/1') on conflict do nothing"); };
      apply(first.idempotencyKey);
      // The destination committed; the process dies before CompleteActivityTask.
      await env.stop(env.server, "SIGKILL");
      await delay(1100);
      await env.startServer();
      await expect(worker(env, "CompleteActivityTask", { taskToken: first.taskToken })).rejects.toMatchObject({ code: "failed_precondition" });
      const second = await pollActivity(env);
      expect(second.idempotencyKey).toBe(first.idempotencyKey);
      apply(second.idempotencyKey);
      const outcomes = await Promise.allSettled([1, 2].map(() => worker(env, "CompleteActivityTask", { taskToken: second.taskToken, result: payload("applied") })));
      expect(outcomes.filter((outcome) => outcome.status === "fulfilled")).toHaveLength(1);
      expect(outcomes.filter((outcome) => outcome.status === "rejected")[0].reason.code).toBe("failed_precondition");
      await complete(env, await poll(env), [finish]);
      const history = await env.history("effect");
      expect(history.filter((event) => event.activityCompleted)).toHaveLength(1);
      expect(env.sql("select count(*) from audit_effect", ["-Atq"]).trim()).toBe("1");
      expect(decode((await env.rpc("DescribeRun", { runId: "effect" })).run.result)).toBe("done");
      console.log("AUDIT recovery: expired workflow/activity rejected; duplicate completion accepted once; one keyed effect after SIGKILL/restart");
    });
  }, 60000);

  it("fires every timer once with two server processes and keeps terminal races atomic", async () => {
    await managed(new Evidence("audit_timers", 7602, { databasePrefix: "capstan_audit" }), async (env) => {
      const primary = env.address;
      env.port = 7603; env.address = "http://127.0.0.1:7603";
      await env.startServer();
      env.address = primary;
      const runs = 20, timers = 3;
      for (let n = 0; n < runs; n++) {
        await env.start("manual", `timer-${n}`);
        await complete(env, await poll(env), Array.from({ length: timers }, (_, seq) => timer(String(seq + 1))));
        await until("all timers fired", async () => (await env.history(`timer-${n}`)).filter((e) => e.timerFired).length === timers);
        const task = await poll(env);
        await complete(env, task, [finish]);
      }
      for (let n = 0; n < runs; n++) {
        const history = await env.history(`timer-${n}`);
        const fired = history.filter((event) => event.timerFired);
        expect(fired).toHaveLength(timers);
        expect(new Set(fired.map((event) => event.timerFired.seq)).size).toBe(timers);
        expect(history.map((event) => Number(event.eventId))).toEqual(history.map((_, i) => i + 1));
      }
      for (let n = 0; n < 20; n++) {
        const id = `race-${n}`;
        await env.start("manual", id);
        await complete(env, await poll(env), [timer("1", "0.03s")]);
        await env.rpc("SignalRun", { runId: id, name: "wake" });
        const task = await poll(env);
        // A run lock holds the due timer and all three RPCs at the same boundary.
        const lock = env.child("psql", [env.dsn, "-X", "-qc", `begin;select 1 from run where run_id='${id}' for update;select pg_sleep(0.15);commit`], `lock-${n}`);
        await until("run lock held", () => Number(env.sql("select count(*) from pg_stat_activity where datname=current_database() and wait_event='PgSleep'", ["-Atq"]).trim()) === 1);
        const outcomes = await Promise.allSettled([
          env.rpc("CancelRun", { runId: id }),
          env.rpc("TerminateRun", { runId: id, reason: "audit race" }),
          complete(env, task, [finish]),
        ]);
        expect(outcomes.some((outcome) => outcome.status === "fulfilled")).toBe(true);
        await env.stop(lock);
        const history = await env.history(id);
        const terminal = history.filter((event) => event.runFailed || event.runCompleted);
        expect(terminal).toHaveLength(1);
        expect(history.at(-1)).toEqual(terminal[0]);
        expect(history.filter((event) => event.timerFired).length).toBeLessThanOrEqual(1);
        expect(env.sql(`select (select count(*) from task where run_id='${id}')+(select count(*) from timer where run_id='${id}')+(select count(*) from inbox where run_id='${id}')`, ["-Atq"]).trim()).toBe("0");
      }
      await delay(300);
      console.log(`AUDIT timers: ${runs * timers}/${runs * timers} fired once with two processes; 20 terminal races retain one terminal event and no tasks/timers/inbox`);
    });
  }, 90000);

  it("resumes BLOCKED via CLI and replays both old and new patched results", async () => {
    await managed(new Evidence("audit_patch", 7604, { databasePrefix: "capstan_audit" }), async (env) => {
      let process = env.worker("blocked-v1.ts");
      env.cli("start", "changedWorkflow", "audit-old", "--queue", env.queue);
      await until("v1 idle", async () => (await env.history("audit-old")).length >= 9 && env.sql("select count(*) from task", ["-Atq"]).trim() === "0");
      const before = await env.history("audit-old");
      await env.stop(process);
      process = env.worker("blocked-v2.ts");
      env.cli("signal", "audit-old", "continue", "--input", "true");
      await env.status("audit-old", "RUN_STATUS_BLOCKED");
      await env.stop(process);
      env.worker("blocked-patched.ts");
      env.cli("resume", "audit-old", "--reason", "audit patch installed");
      expect(decode((await env.status("audit-old", "RUN_STATUS_COMPLETED")).result)).toEqual({ value: 1 });
      expect((await env.history("audit-old")).slice(0, before.length)).toEqual(before);
      await env.start("changedWorkflow", "audit-new");
      await env.rpc("SignalRun", { runId: "audit-new", name: "continue", input: payload(true) });
      expect(decode((await env.status("audit-new", "RUN_STATUS_COMPLETED")).result)).toEqual({ value: 10 });
      expect((await env.history("audit-old")).some((e) => e.markerRecorded?.markerId === "revised-step-v2")).toBe(false);
      expect((await env.history("audit-new")).filter((e) => e.markerRecorded?.markerId === "revised-step-v2")).toHaveLength(1);
      for (const id of ["audit-old", "audit-new"]) expect(env.cli("replay", id, "--workflows", join(root, "examples/evidence/blocked-patched.ts"))).toContain("OK");
      console.log("AUDIT patch: BLOCKED -> CLI resume -> old result1/new result10; both CLI replays OK; old prefix unchanged");
    });
  }, 60000);
});
