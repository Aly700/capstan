import { fromJson, toJson } from "@bufbuild/protobuf";
import type { JsonValue } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";
import { CommandSchema, HistoryEventSchema } from "../src/gen/capstan/v1/capstan_pb.ts";
import type { HistoryEvent } from "../src/gen/capstan/v1/capstan_pb.ts";
import { replay } from "../src/replay/runtime.ts";

const payload = (v: unknown) => ({contentType:"application/json",data:Buffer.from(JSON.stringify(v)).toString("base64")});
const source = (body: string) => ({buildId:"test",code:`globalThis.__capstanWorkflows = {run: async (input) => {const rt = globalThis[Symbol.for('capstan.workflow.runtime')]; ${body}}};`});
class History {
  events: HistoryEvent[] = [];
  private started = 0;
  private scheduled = 0;
  add(kind: string, attrs: Record<string, unknown> = {}, time = "2026-09-28T12:00:00Z") {
    const type = `EVENT_TYPE_${kind.replace(/[A-Z]/g, (x) => `_${x}`).toUpperCase()}`;
    this.events.push(fromJson(HistoryEventSchema,{eventId:String(this.events.length+1),type,time,[kind]:attrs} as JsonValue));
    return this;
  }
  constructor(input: unknown = null) { this.add("runStarted",{workflowType:"run",taskQueue:"q",input:payload(input)}); }
  task(time = "2026-09-28T12:00:01Z") {
    this.add("taskScheduled",{taskQueue:"q",attempt:1}); this.scheduled=this.events.length;
    this.add("taskStarted",{scheduledEventId:String(this.scheduled)},time); this.started=this.events.length;
    return this;
  }
  complete() { return this.add("taskCompleted",{scheduledEventId:String(this.scheduled),startedEventId:String(this.started)}); }
  activity(seq: number, name="double") { return this.add("activityScheduled",{seq:String(seq),activityType:name}); }
  result(seq: number, value: unknown) { return this.add("activityCompleted",{seq:String(seq),result:payload(value)}); }
  timer(seq: number) { return this.add("timerStarted",{seq:String(seq),fireAfter:"1s"}); }
  fire(seq: number) { return this.add("timerFired",{seq:String(seq)}); }
}
async function run(body: string, history: History, runId="test-run") {
  return (await replay({bundle:source(body),runId,history:history.events})).map((c)=>toJson(CommandSchema,c));
}

describe("replay activation runtime", () => {
  it("completes plain workflow returns", async () => {
    expect(await run("return input;",new History({answer:42}).task())).toEqual([{completeRun:{result:payload({answer:42})}}]);
  });
  it("allocates activity seq at call time inside Promise.all", async () => {
    const cmds=await run("return await Promise.all([rt.activity('a',1,{startToCloseTimeout:'1s'}),rt.activity('b',2,{startToCloseTimeout:'1s'})]);",new History().task());
    expect(cmds).toMatchObject([{scheduleActivity:{seq:"1",activityType:"a"}},{scheduleActivity:{seq:"2",activityType:"b"}}]);
  });
  it("drains nested async functions through Promise.all", async () => {
    const h=new History().task().complete().activity(1).activity(2).result(2,4).result(1,2).task();
    const body="async function nested(n){await Promise.resolve(); const v=await rt.activity('double',n,{startToCloseTimeout:'1s'}); await Promise.resolve(); return await (async()=>{await Promise.resolve();return v;})();} return await Promise.all([nested(1),nested(2)]);";
    expect(await run(body,h)).toEqual([{completeRun:{result:payload([2,4])}}]);
  });
  it("Promise.race resolves in external history order", async () => {
    const h=new History().task().complete().activity(1,"a").timer(2).fire(2).result(1,42).task();
    expect(await run("return await Promise.race([rt.activity('a',0,{startToCloseTimeout:'1s'}),rt.sleep('1s')]);",h)).toEqual([{completeRun:{}}]);
  });
  it("batched events retain native Promise.race continuation ordering", async () => {
    const h=new History().task().complete().activity(1,"a").timer(2).fire(2).result(1,42).task();
    expect(await run("return await Promise.race([rt.activity('a',0,{startToCloseTimeout:'1s'}),rt.sleep('1s').then(()=> 'timer')]);",h)).toEqual([{completeRun:{result:payload(42)}}]);
  });
  it("handler can await an activity and emit a later command", async () => {
    const h=new History().task().complete().add("signalReceived",{name:"go",input:payload(5)}).task().complete().activity(1).result(1,10).task();
    const body="let done=false;rt.setHandler({name:'go'},async x=>{await rt.activity('double',x,{startToCloseTimeout:'1s'});await Promise.resolve();done=true;});await rt.condition(()=>done);return 7;";
    expect(await run(body,h)).toEqual([{completeRun:{result:payload(7)}}]);
  });
  it("sets workflow time before promise continuations and skips discarded time", async () => {
    const h=new History().task().add("taskFailed",{startedEventId:"3"}).task("2026-09-28T12:00:04Z");
    expect(await run("return [rt.now(),Date.now(),new Date().getTime(),rt.workflowInfo().isReplaying];",h)).toEqual([{completeRun:{result:payload([1790596804000,1790596804000,1790596804000,false])}}]);
  });
  it("unknown or duplicate activity results are SDK errors", async () => {
    const h=new History().task().complete().activity(1).result(1,2).result(1,2).task();
    await expect(run("return await rt.activity('double',1,{startToCloseTimeout:'1s'});",h)).rejects.toThrow(/settled|unknown/i);
  });
  it("unknown timer fires are SDK errors", async () => {
    await expect(run("await rt.sleep('1s');",new History().task().complete().timer(1).fire(9).task())).rejects.toThrow(/unknown/i);
  });
  it("does not invoke a side effect if its historical marker identity mismatches", async () => {
    const h=new History().task().complete().add("markerRecorded",{seq:"1",name:"uuid",details:payload("old")}).task();
    await expect(run("rt.sideEffect(()=>{throw Error('callback escaped');});",h)).rejects.toMatchObject({name:"HistoryMismatchError",eventId:5});
  });
  it("reports the first mismatch even when a later marker also differs", async () => {
    const h=new History().task().complete().activity(1,"old").add("markerRecorded",{seq:"2",name:"uuid",details:payload("old")}).task();
    await expect(run("rt.activity('new',0,{startToCloseTimeout:'1s'});rt.sideEffect(()=>1);",h)).rejects.toMatchObject({name:"HistoryMismatchError",eventId:5});
  });
  it("queueMicrotask exceptions fail the task without escaping to the worker", async () => {
    await expect(run("queueMicrotask(()=>{throw Error('microtask failed');});await rt.nextSignal({name:'forever'});",new History().task())).rejects.toThrow(/microtask failed/);
  });
  it("missing workflow exports fail the task as an SDK error", async () => {
    await expect(replay({bundle:{code:"globalThis.__capstanWorkflows = {};",buildId:"missing"},runId:"missing",history:new History().task().events})).rejects.toThrow(/workflow export/);
  });
  it("synchronous workflow execution is bounded and fails the task", async () => {
    await expect(run("while(true) {}",new History().task())).rejects.toThrow(/timed out/);
  });
  it("disabled code generation fails the task as a sandbox error", async () => {
    await expect(run("return Function('return 1')();",new History().task())).rejects.toThrow(/code generation|Code generation/);
  });
  it("workflow errors become FailRun and sandbox errors remain SDK errors", async () => {
    expect(await run("throw Error('broken');",new History().task())).toMatchObject([{failRun:{failure:{message:"broken",type:"Error"}}}]);
    await expect(run("fetch('https://example.test');",new History().task())).rejects.toMatchObject({name:"SandboxViolationError"});
  });
  it("continueAsNew wins even if user catches its control signal", async () => {
    expect(await run("try {rt.continueAsNew(2);} catch {} return 3;",new History().task())).toEqual([{continueAsNew:{input:payload(2)}}]);
  });
  it("replays a closed history without returning already recorded commands", async () => {
    expect(await run("return 1;",new History().task().complete().add("runCompleted",{result:payload(1)}))).toEqual([]);
  });
  it("replays a 5,000-event history in under 2 seconds", async () => {
    const h=new History().task();
    for(let i=1;i<=1000;i++) h.complete().activity(i).result(i,i).task();
    // 3 initial + 1000*5 = 5,003 entries, all parsed before timing.
    expect(h.events.length).toBeGreaterThanOrEqual(5000);
    const started=performance.now();
    const commands=await run("for(let i=1;i<=1000;i++)await rt.activity('double',i,{startToCloseTimeout:'1s'});return 1000;",h);
    const elapsed=performance.now()-started;
    expect(commands).toEqual([{completeRun:{result:payload(1000)}}]);
    expect(elapsed).toBeLessThan(2000);
  });
});
