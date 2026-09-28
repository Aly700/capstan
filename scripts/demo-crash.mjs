import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { Evidence, managed, root, until, delay, decode } from "./evidence-lib.mjs";

console.log(`Capstan crash demo · ${new Date().toISOString()}`);
console.log("Real Go binary + SDK worker + PostgreSQL. Wall-clock playback; no simulated crash or clock jump.");
console.log(`Source: ${execFileSync("git", ["rev-parse", "--short", "HEAD"], { cwd: root, encoding: "utf8" }).trim()} + this demo's working-tree files`);
await managed(new Evidence("crash", 7302), async (env) => {
  console.log("$ GOTOOLCHAIN=go1.26.4 go build -o .lane/bin/capstan-server ./cmd/capstan-server");
  console.log(`$ CAPSTAN_ADDR=127.0.0.1:7302 .lane/bin/capstan-server serve  # database ${env.database}`);
  const worker = env.worker("crash.ts");
  console.log("$ (cd sdk && node --import tsx ../examples/evidence/worker.ts)  # workflows: crash.ts");
  console.log(execFileSync("ps", ["-p", `${env.server.pid},${worker.pid}`, "-o", "pid,pgid,ucomm"], { encoding: "utf8" }).trim());
  env.cli("start", "crashSurvivor", "evidence-crash", "--queue", env.queue);
  const before = await until("first durable timer", async () => {
    const history = await env.history("evidence-crash");
    return history.some((event) => event.timerStarted) ? history : false;
  });
  writeFileSync(join(env.logdir, "history-before.json"), JSON.stringify(before, null, 2));
  env.cli("describe", "evidence-crash");
  const killedPID = env.server.pid;
  console.log(`$ kill -9 ${killedPID}  # the SERVER, while the run is waiting on its first timer`);
  await env.stop(env.server, "SIGKILL");
  let unavailable = false;
  try { await fetch(`${env.address}/healthz`, { signal: AbortSignal.timeout(1000) }); } catch { unavailable = true; }
  assert.equal(unavailable, true);
  console.log("Server is unreachable. Worker stays alive. Waiting 4 real seconds, past the timer's deadline.");
  await delay(4000);
  console.log("$ CAPSTAN_ADDR=127.0.0.1:7302 .lane/bin/capstan-server serve  # restart on the SAME database");
  await env.startServer();
  assert.notEqual(env.server.pid, killedPID);
  console.log(`Restarted server PID ${env.server.pid}; worker PID ${worker.pid} is unchanged.`);
  const run = await env.status("evidence-crash", "RUN_STATUS_COMPLETED");
  assert.deepEqual(decode(run.result), [1, 2, 3, 4]);
  const after = await env.history("evidence-crash");
  assert.deepEqual(after.slice(0, before.length), before);
  assert.equal(after.filter((event) => event.activityCompleted).length, 4);
  const timers = after.filter((event) => event.timerFired);
  assert.equal(timers.length, 3);
  assert.equal(new Set(timers.map((event) => event.timerFired.startedEventId)).size, 3);
  writeFileSync(join(env.logdir, "history-after.json"), JSON.stringify(after, null, 2));
  env.cli("describe", "evidence-crash");
  env.cli("history", "evidence-crash");
  console.log(`PASS: result [1,2,3,4]; ${before.length} pre-crash events preserved as an exact prefix; 4 activity completions; 3 distinct timer firings.`);
  console.log("Cleanup: stopping this demo's processes and dropping its database.");
});
