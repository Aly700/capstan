import assert from "node:assert/strict";
import { test } from "node:test";
import { Evidence } from "./evidence-lib.mjs";

test("cleanup rejects late resource creation", async (t) => {
  const env = new Evidence("cleanup_test", 7398);
  // If the guard regresses, the attempted child is harmless and still reaped.
  t.after(async () => { await Promise.all(env.children.map((child) => env.stop(child))); });
  await env.cleanup();
  assert.throws(() => env.child(process.execPath, ["-e", ""], "unexpected"), /cleanup has begun/);
  assert.throws(() => env.worker(), /cleanup has begun/);
  await assert.rejects(env.startServer(), /cleanup has begun/);
  await assert.rejects(env.setup(), /cleanup has begun/);
  assert.equal(env.children.length, 0);
});

test("cleanup closes registration before awaiting any child", async () => {
  const env = new Evidence("cleanup_pending_test", 7398);
  const existing = { name: "existing" };
  env.children.push(existing);
  let completeStop;
  const stopped = new Promise((resolve) => { completeStop = resolve; });
  env.stop = async (child) => { assert.equal(child, existing); await stopped; };
  const cleanup = env.cleanup();
  try { assert.throws(() => env.child(process.execPath, ["-e", ""], "unexpected"), /cleanup has begun/); }
  finally { completeStop(); await cleanup; }
});

test("a failed spawn with no PID can be cleaned up", async () => {
  const env = new Evidence("cleanup_failed_spawn_test", 7398);
  await env.stop({ pid: undefined, exitCode: null, signalCode: null });
});
