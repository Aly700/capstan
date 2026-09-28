import { fileURLToPath } from "node:url";
import { fromJson, toJson } from "@bufbuild/protobuf";
import { beforeAll, describe, expect, it } from "vitest";
import { CommandSchema, HistoryEventSchema } from "../src/gen/capstan/v1/capstan_pb.ts";
import { bundleWorkflows } from "../src/sandbox/bundle.ts";
import type { WorkflowBundle } from "../src/sandbox/bundle.ts";
import { replay } from "../src/replay/runtime.ts";
import { loadFixtures } from "./fixtures/load.ts";

const directory=new URL("../../conformance/fixtures/",import.meta.url);
const fixtures=await loadFixtures(directory, "ts");

/** Payload bytes are language-specific; the JSON values they encode are the contract. */
function normalized(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(normalized);
  if (value !== null && typeof value === "object") {
    const object=value as Record<string,unknown>;
    if (object.contentType === "application/json" && typeof object.data === "string") return {contentType:object.contentType,value:JSON.parse(Buffer.from(object.data,"base64").toString("utf8"))};
    return Object.fromEntries(Object.entries(object).map(([key,v])=>[key,normalized(v)]));
  }
  return value;
}
let bundle: WorkflowBundle;
beforeAll(async()=>{bundle=await bundleWorkflows(fileURLToPath(new URL("../../conformance/workflows.ts",import.meta.url)));});
describe("shared conformance corpus",()=>{
  for(const {file,fixture} of fixtures) {
    it(`${file}: ${fixture.name}`,async()=>{
      const task=replay({bundle,runId:"conformance",workflowType:fixture.workflow,history:fixture.history.map((event)=>fromJson(HistoryEventSchema,event))});
      if ("mismatch" in fixture.expect) await expect(task).rejects.toMatchObject({name:"HistoryMismatchError",eventId:fixture.expect.mismatch.eventId});
      else {
        const actual=(await task).map((command)=>normalized(toJson(CommandSchema,command)));
        const expected=fixture.expect.commands.map((command)=>normalized(toJson(CommandSchema,fromJson(CommandSchema,command))));
        expect(actual).toEqual(expected);
      }
    });
  }
});
