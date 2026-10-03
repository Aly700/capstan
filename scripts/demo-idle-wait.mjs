import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { Evidence, managed, until, delay, decode } from "./evidence-lib.mjs";

console.log(`Capstan idle-wait demo · ${new Date().toISOString()}`);
console.log("human() is still a stub at this revision. Using the nextSignal('decision') fallback.");
console.log("This is a real wall-clock wait with no workers. It does not demonstrate a multi-day clock jump or an approval API.");
await managed(new Evidence("idle", 7304), async (env) => {
  const worker = env.worker("idle-signal.ts");
  console.log("$ (cd sdk && node --import tsx ../examples/evidence/worker.ts)  # idle-signal.ts");
  env.cli("start", "idleWait", "evidence-idle", "--queue", env.queue);
  await until("signal wait with no queued task", async () => (await env.history("evidence-idle")).length >= 4 && env.sql("select count(*) from task", ["-Atq"]).trim() === "0");
  console.log(`$ kill -TERM ${worker.pid}  # stop every worker belonging to this isolated demo`);
  await env.stopWorkers();
  for (const child of env.workers) assert(child.exitCode !== null || child.signalCode !== null);
  const query = "select count(*) as tasks, count(*) filter (where leased_until is not null) as leased_tasks from task";
  function showIdle() {
    console.log(`$ ps -p ${env.workers.map((w) => w.pid).join(",")} -o pid,pgid,ucomm`);
    const processes = spawnSync("ps", ["-p", env.workers.map((w) => w.pid).join(","), "-o", "pid,pgid,ucomm"], { encoding: "utf8" });
    assert.equal(processes.status, 1);
    console.log(processes.stdout.trim());
    console.log("Worker processes: 0 (all demo worker PIDs exited; other processes are untouched).");
    console.log(`$ psql <this demo database> -c '${query}'`);
    process.stdout.write(env.sql(query));
    assert.equal(env.sql("select count(*) from task", ["-Atq"]).trim(), "0");
  }
  showIdle();
  env.cli("describe", "evidence-idle");
  console.log("Waiting 5 real seconds with zero workers and zero task leases…");
  await delay(5000);
  showIdle();
  console.log("Resolve the signal gate through the real CLI while no worker is running:");
  env.cli("signal", "evidence-idle", "decision", "--input", '{"approved":true}', "--request-id", "evidence-decision");
  console.log("$ psql <this demo database> -c 'select kind, worker_id, leased_until from task'");
  process.stdout.write(env.sql("select kind, worker_id, leased_until from task"));
  assert.equal(env.sql("select count(*) from task where leased_until is not null", ["-Atq"]).trim(), "0");
  console.log("$ (cd sdk && node --import tsx ../examples/evidence/worker.ts)  # start a replacement worker");
  env.worker("idle-signal.ts");
  const result = await env.status("evidence-idle", "RUN_STATUS_COMPLETED");
  assert.deepEqual(decode(result.result), { approved: true });
  env.cli("describe", "evidence-idle");
  env.cli("history", "evidence-idle");
  console.log("PASS: zero demo workers and zero leased tasks during the idle interval; signal persisted before replacement worker; result {approved:true}.");
  console.log("Cleanup: stopping this demo's processes and dropping its database.");
});
