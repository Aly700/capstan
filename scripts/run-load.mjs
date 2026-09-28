import assert from "node:assert/strict";
import { execFile, execFileSync, spawn } from "node:child_process";
import { readFileSync, writeFileSync, appendFileSync } from "node:fs";
import { join } from "node:path";
import { promisify } from "node:util";
import { Evidence, managed, root, delay, until } from "./evidence-lib.mjs";

const exec = promisify(execFile);
const [concurrency = 50, n = 1000, workerCount = 4] = process.argv.slice(2).map(Number);
for (const value of [concurrency, n, workerCount]) assert(Number.isInteger(value) && value > 0);
const phase = process.env.CAPSTAN_LOAD_PHASE || "evidence";
const env = new Evidence(`load_c${concurrency}_w${workerCount}`, Number(process.env.CAPSTAN_LOAD_PORT || 7301), { databasePrefix: process.env.CAPSTAN_LOAD_DB_PREFIX || "capstan_evidence" });
const container = process.env.EVIDENCE_PG_CONTAINER || "capstan-postgres-1";
const sql = `select json_build_object(
 'at',clock_timestamp(),
 'activity',(select coalesce(json_agg(a),'[]') from (select datname,state,wait_event_type,wait_event,count(*) as n from pg_stat_activity where backend_type='client backend' and pid<>pg_backend_pid() group by 1,2,3,4) a),
 'waiting_locks',(select count(*) from pg_locks l join pg_stat_activity a using(pid) where a.datname=current_database() and not l.granted),
 'lock_waits',(select coalesce(json_agg(a),'[]') from (select wait_event,pg_blocking_pids(pid) as blockers,left(query,120) as query from pg_stat_activity where datname=current_database() and wait_event_type='Lock') a),
 'tasks',(select json_build_object('total',count(*),'leased',count(*) filter(where leased_until is not null),'ready',count(*) filter(where leased_until is null)) from task),
 'database',(select row_to_json(d) from (select xact_commit,xact_rollback,blks_read,blks_hit,deadlocks from pg_stat_database where datname=current_database()) d)
);`;
await managed(env, async () => {
  execFileSync("go", ["build", "-o", ".lane/bin/capstan-load", "./cmd/capstan-load"], { cwd: root, env: { ...process.env, GOTOOLCHAIN: "go1.26.4" } });
  for (let i = 0; i < workerCount; i++) env.worker("load.ts", i);
  await until("all workers ready", () => env.workers.every((worker, i) => {
    if (worker.exitCode !== null) throw new Error("Worker exited; inspect .lane logs");
    try { return readFileSync(join(env.logdir, `worker-${i}.log`), "utf8").includes('"workerReady":true'); } catch { return false; }
  }));
  // Untimed warmup checks all five activities before the closed-loop measurement.
  await env.start("loadFive", "warmup");
  await env.status("warmup", "RUN_STATUS_COMPLETED");
  writeFileSync(join(env.logdir, "sampling.sql"), sql);
  writeFileSync(join(env.logdir, "metadata.json"), JSON.stringify({ date: new Date().toISOString(), database: env.database, command: `scripts/run-load.sh ${concurrency} ${n} ${workerCount}`, serverPID: env.server.pid, workerPIDs: env.workers.map((w) => w.pid), workflowConcurrencyPerWorker: 10, activityConcurrencyPerWorker: 10, container, phase, dbMaxConns: process.env.CAPSTAN_DB_MAX_CONNS || "default", port: env.port }, null, 2));
  const sampleFile = join(env.logdir, "system.jsonl");
  const pids = [env.server.pid, ...env.workers.map((w) => w.pid)];
  async function sample() {
    const started = performance.now();
    const results = await Promise.allSettled([
      exec("ps", ["-p", pids.join(","), "-o", "pid=,pcpu=,time=,rss=,comm="]),
      exec("docker", ["exec", container, "cat", "/sys/fs/cgroup/cpu.stat"]),
      exec("psql", [env.dsn, "-XAtq", "-v", "ON_ERROR_STOP=1", "-c", sql]),
    ]);
    const row = { at: new Date().toISOString(), sample_ms: performance.now() - started };
    for (let i = 0; i < results.length; i++) {
      const result = results[i];
      row[["processes", "postgres_cpu", "postgres"][i]] = result.status === "fulfilled" ? (i === 2 ? JSON.parse(result.value.stdout) : result.value.stdout.trim()) : { error: result.reason.message };
    }
    appendFileSync(sampleFile, JSON.stringify(row)+"\n");
  }
  await sample();
  writeFileSync(join(env.logdir, "metrics-before.txt"), await (await fetch(`${env.address}/metrics`)).text());
  const args = ["-address", env.address, "-queue", env.queue, "-concurrency", String(concurrency), "-n", String(n), "-samples", join(env.logdir, "runs.jsonl")];
  console.log(`$ CAPSTAN_API_KEY=<ephemeral key> .lane/bin/capstan-load -address ${env.address} -queue ${env.queue} -concurrency ${concurrency} -n ${n} -samples .lane/<this-run>/runs.jsonl`);
  env.ensureActive();
  const child = spawn(join(root, ".lane/bin/capstan-load"), args, { cwd: root, env: { ...process.env, CAPSTAN_API_KEY: env.key }, detached: true, stdio: ["ignore", "pipe", "pipe"] });
  env.children.push(child);
  let output = "", errors = "", done = false;
  child.stdout.on("data", (chunk) => { output += chunk; process.stdout.write(chunk); });
  child.stderr.on("data", (chunk) => { errors += chunk; process.stderr.write(chunk); });
  const completion = new Promise((resolve, reject) => { child.once("error", reject); child.once("exit", (code) => { done = true; resolve(code); }); });
  while (!done) { await delay(1000); if (!done) await sample(); }
  const code = await completion;
  await sample();
  writeFileSync(join(env.logdir, "result.json"), output);
  writeFileSync(join(env.logdir, "load-errors.log"), errors);
  writeFileSync(join(env.logdir, "metrics-after.txt"), await (await fetch(`${env.address}/metrics`)).text());
  // Excludes the warmup; verifies persisted completions and partial work on errors.
  const audit = env.sql("select json_build_object('runs',count(*),'completed',count(*) filter(where status=2),'other',count(*) filter(where status<>2),'activity_completions',(select count(*) from event where run_id like 'load-%' and type=21),'task_failures',(select count(*) from event where run_id like 'load-%' and type in (13,14)),'remaining_tasks',(select count(*) from task)) from run where run_id like 'load-%'", ["-Atq"]);
  writeFileSync(join(env.logdir, "audit.json"), audit);
  console.log(`SQL audit: ${audit.trim()}`);
  console.log(`Raw logs: ${env.logdir.slice(root.length+1)}`);
  assert.equal(code, 0, "load driver reported errors (preserved in raw logs)");
  const parsed = JSON.parse(audit);
  assert.equal(parsed.completed, n); assert.equal(parsed.other, 0); assert.equal(parsed.activity_completions, n*5);
});
