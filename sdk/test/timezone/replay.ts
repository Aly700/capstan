// Child process for replay-timezone.test.ts: replay `calendar` against a three-event
// history whose clock reads argv[2], under whatever TZ the parent set, and print the
// commands. The parent compares the output across zones.
import { fromJson, toJson } from "@bufbuild/protobuf";
import { fileURLToPath } from "node:url";
import { CommandSchema, HistoryEventSchema } from "../../src/gen/capstan/v1/capstan_pb.ts";
import { replay } from "../../src/replay/runtime.ts";
import { bundleWorkflows } from "../../src/sandbox/bundle.ts";

const at = process.argv[2]!;
const bundle = await bundleWorkflows(fileURLToPath(new URL("./workflows.ts", import.meta.url)));
const history = [
  { eventId: "1", type: "EVENT_TYPE_RUN_STARTED", time: at, runStarted: { workflowType: "calendar", taskQueue: "q", taskTimeout: "10s" } },
  { eventId: "2", type: "EVENT_TYPE_TASK_SCHEDULED", time: at, taskScheduled: { taskQueue: "q", startToCloseTimeout: "10s", attempt: 1 } },
  { eventId: "3", type: "EVENT_TYPE_TASK_STARTED", time: at, taskStarted: { scheduledEventId: "2", identity: "tz" } },
].map((event) => fromJson(HistoryEventSchema, event));
const commands = await replay({ bundle, runId: "tz", workflowType: "calendar", history });
process.stdout.write(JSON.stringify({
  tz: process.env.TZ,
  // The host's own Date proves the zone took effect outside the sandbox.
  hostOffset: new Date(at).getTimezoneOffset(),
  commands: commands.map((command) => toJson(CommandSchema, command)),
}));
