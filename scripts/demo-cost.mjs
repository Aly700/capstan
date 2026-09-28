import assert from "node:assert/strict";
import { Evidence, managed, until, decode } from "./evidence-lib.mjs";

// Needs ANTHROPIC_API_KEY in the environment (scripts/demo-cost.sh loads it). The key reaches
// only the worker processes; nothing here prints it, and the ledger holds counts, not text.
assert(process.env.ANTHROPIC_API_KEY, "ANTHROPIC_API_KEY is required");
console.log(`Capstan cost-ledger demo · ${new Date().toISOString()}`);
console.log("One real claude-haiku-4-5 call through the real server; a second worker replays it from history.");
await managed(new Evidence("cost", 7305), async (env) => {
  const first = env.worker("cost.ts");
  env.cli("start", "costAgent", "evidence-cost", "--queue", env.queue);
  await until("the model call to finish", async () => env.sql("select count(*) from ai_call where status = 2", ["-Atq"]).trim() === "1", 60000);
  await until("the run to wait on its signal", async () => (await env.history("evidence-cost")).some((e) => e.type === "EVENT_TYPE_ACTIVITY_COMPLETED")
    && env.sql("select count(*) from task", ["-Atq"]).trim() === "0", 30000);
  console.log(`$ kill -TERM ${first.pid}  # the worker that made the call is gone`);
  await env.stop(first);
  console.log("$ (cd sdk && node --import tsx ../examples/evidence/worker.ts)  # a fresh worker; it must replay, not re-call");
  env.worker("cost.ts");
  env.cli("signal", "evidence-cost", "finish", "--request-id", "evidence-finish");
  const run = await env.status("evidence-cost", "RUN_STATUS_COMPLETED");
  const result = decode(run.result);
  const query = "select id, run_id, activity_seq, model, status, estimate_usd, cost_usd, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens from ai_call order by id";
  console.log(`$ psql <this demo database> -c '${query}'`);
  process.stdout.write(env.sql(query));
  const rows = env.sql("select count(*), sum(cost_usd), min(input_tokens), min(output_tokens) from ai_call", ["-Atq", "-F", ","]).trim().split(",");
  assert.equal(rows[0], "1", "exactly one ledger row after replay on a second worker");
  const cost = Number(rows[1]);
  assert(cost > 0 && cost < 0.05, `cost ${cost} within (0, 0.05)`);
  assert(Number(rows[2]) > 0 && Number(rows[3]) > 0, "token counts recorded");
  assert.equal(Number(run.costUsd), cost, "RunInfo.cost_usd equals the ledger");
  assert.equal(result.costUsd, cost, "the workflow saw the server-priced cost");
  env.cli("describe", "evidence-cost");
  console.log(`PASS: one real model call, one ledger row (${rows[2]} input / ${rows[3]} output tokens, $${cost}), no second charge when a new worker replayed the run.`);
});
