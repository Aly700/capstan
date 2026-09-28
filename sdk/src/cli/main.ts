import { parseArgs } from "node:util";
import { Code, ConnectError, createClient } from "@connectrpc/connect";
import { Client } from "../client/index.ts";
import { describe, statuses } from "../client/description.ts";
import type { RunStatus } from "../client/index.ts";
import { ClientService, RunStatus as ProtoStatus } from "../gen/capstan/v1/capstan_pb.ts";
import { createTransport } from "../worker/transport.ts";
import { bundleWorkflows } from "../sandbox/bundle.ts";
import { replay } from "../replay/runtime.ts";
import { fetchHistory, formatHistory } from "./commands/query.ts";

const usage = `Usage: capstan <command> [arguments]
  start <type> <run-id> --queue <q> [--input <json>]
  describe <run-id>
  history <run-id> [--json]
  replay <run-id> --workflows <path>
  signal <run-id> <name> [--input <json>] [--request-id <id>]
  cancel|resume <run-id> [--reason <text>]
  approve|deny <run-id> <approval-id> [--choice <x>] [--note <text>] [--resolver <name>]
  list [--status <status>]
Environment: CAPSTAN_ADDRESS (default http://127.0.0.1:7233), CAPSTAN_API_KEY`;
class UsageError extends Error {}
const definitions: Record<string, { count: number; strings?: string[]; booleans?: string[]; required?: string[] }> = {
  start: { count: 2, strings: ["queue", "input"], required: ["queue"] },
  describe: { count: 1 }, history: { count: 1, booleans: ["json"] },
  replay: { count: 1, strings: ["workflows"], required: ["workflows"] },
  signal: { count: 2, strings: ["input", "request-id"] },
  cancel: { count: 1, strings: ["reason"] }, resume: { count: 1, strings: ["reason"] },
  approve: { count: 2, strings: ["choice", "note", "resolver"] }, deny: { count: 2, strings: ["choice", "note", "resolver"] },
  list: { count: 0, strings: ["status"] },
};

/** The launcher supplies process I/O; return codes are stable for shell scripts. */
export async function main(args = process.argv.slice(2), env = process.env): Promise<number> {
  try {
    const command = args[0];
    if (command === "--help" || command === "help") { process.stdout.write(`${usage}\n`); return 0; }
    const definition = command === undefined ? undefined : definitions[command];
    if (!definition) throw new UsageError(command ? `unknown command: ${command}` : "a command is required");
    let parsed;
    try {
      parsed = parseArgs({ args: args.slice(1), allowPositionals: true, strict: true, options: Object.fromEntries([
        ...(definition.strings ?? []).map((name) => [name, { type: "string" as const }]),
        ...(definition.booleans ?? []).map((name) => [name, { type: "boolean" as const }]),
      ]) });
    } catch (error) { throw new UsageError(error instanceof Error ? error.message : String(error)); }
    if (parsed.positionals.length !== definition.count) throw new UsageError(`${command} requires ${definition.count} positional argument(s)`);
    const values: Record<string, string | boolean | Array<string | boolean> | undefined> = parsed.values;
    const text = (name: string): string | undefined => typeof values[name] === "string" ? values[name] : undefined;
    for (const name of definition.required ?? []) if (!text(name)) throw new UsageError(`--${name} is required`);
    let input: unknown;
    if (text("input") !== undefined) {
      try { input = JSON.parse(text("input")!); } catch { throw new UsageError("invalid JSON for --input"); }
    }
    const status = text("status");
    if (status !== undefined && !Object.hasOwn(statuses, status)) throw new UsageError(`invalid status: ${status}`);
    if (!env.CAPSTAN_API_KEY) throw new UsageError("CAPSTAN_API_KEY is required");
    const options = { address: env.CAPSTAN_ADDRESS ?? "http://127.0.0.1:7233", apiKey: env.CAPSTAN_API_KEY };
    const client = new Client(options);
    const rpc = createClient(ClientService, createTransport(options));
    const [runId = "", second = ""] = parsed.positionals;
    let output: unknown;
    switch (command) {
      case "start": output = await client.start(runId, input, { runId: second, taskQueue: text("queue")! }); break;
      case "describe": output = await client.describe(runId); break;
      case "signal": await client.signal(runId, second, input, text("request-id")); break;
      case "cancel": await client.cancel(runId, text("reason")); break;
      case "resume": await client.resume(runId, text("reason")); break;
      case "approve": case "deny":
        await client.resolveApproval(runId, second, {
          outcome: command === "approve" ? "approved" : "denied", resolver: text("resolver") ?? env.USER ?? "capstan-cli",
          ...(text("choice") === undefined ? {} : { choice: text("choice")! }), ...(text("note") === undefined ? {} : { note: text("note")! }),
        });
        break;
      case "history": process.stdout.write(`${formatHistory(await fetchHistory(rpc, runId), values.json === true)}\n`); return 0;
      case "replay": {
        const history = await fetchHistory(rpc, runId);
        const bundle = await bundleWorkflows(text("workflows")!);
        await replay({ bundle, runId, history });
        break;
      }
      case "list": {
        const runs = [];
        let pageToken = "";
        const seen = new Set<string>();
        do {
          const page = await rpc.listRuns({ status: status === undefined ? ProtoStatus.UNSPECIFIED : statuses[status as RunStatus], pageSize: 500, pageToken });
          runs.push(...page.runs.map(describe));
          pageToken = page.nextPageToken;
          if (pageToken && seen.has(pageToken)) throw new ConnectError("run pagination did not advance", Code.DataLoss);
          seen.add(pageToken);
        } while (pageToken);
        output = runs;
        break;
      }
    }
    process.stdout.write(output === undefined ? "OK\n" : `${JSON.stringify(output, null, 2)}\n`);
    return 0;
  } catch (error) {
    if (error instanceof UsageError) { process.stderr.write(`${error.message}\n${usage}\n`); return 1; }
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    return error instanceof Error && error.name === "HistoryMismatchError" ? 3 : 2;
  }
}
