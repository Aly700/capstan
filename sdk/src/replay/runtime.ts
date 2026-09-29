import { create } from "@bufbuild/protobuf";
import { randomUUID } from "node:crypto";
import { setImmediate as yieldToHost } from "node:timers/promises";
import { Script } from "node:vm";
import {
  ApprovalOutcome, ApprovalSource, CommandSchema, TimeoutType,
} from "../gen/capstan/v1/capstan_pb.ts";
import type { Command, HistoryEvent } from "../gen/capstan/v1/capstan_pb.ts";
import { durationMs, toProtoDuration } from "../internal/duration.ts";
import { decode, encode } from "../internal/payload.ts";
import { failureFromProto, failureToProto } from "../internal/failure.ts";
import { ActivityFailure, CancelledFailure, HistoryMismatchError, TimeoutFailure } from "../types.ts";
import type { ActivityOptions, Duration } from "../types.ts";
import type { ActivityProxy, ApprovalRequest, ApprovalResolution, SignalDefinition, WorkflowInfo, WorkflowRuntime } from "../workflow/index.ts";
import { createContext, invokeWorkflow } from "../sandbox/context.ts";
import { activations } from "./activation.ts";
import type { Activation } from "./activation.ts";
import { scheduleActivity } from "./commands.ts";
import { matchCommands } from "./match.ts";
import { replayGateDecision } from "../agent/decision.ts";

export interface ReplayOptions {
  bundle: { code: string; buildId: string };
  runId: string;
  workflowType?: string;
  history: HistoryEvent[];
  attempt?: number;
  logger?: (entry: Record<string, unknown>) => void;
}
interface Pending {
  kind: "activity" | "timer" | "approval";
  activityType?: string;
  gateProposal?: { current: unknown; recorded: unknown };
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
}
interface Condition {
  predicate: () => boolean;
  resolve: (value: boolean) => void;
  reject: (reason: unknown) => void;
  timer?: number;
}
function nameOf(error: unknown): string {
  return error !== null && typeof error === "object" && "name" in error ? String(error.name) : "";
}

/** A fresh instance holds the state reconstructed from one task's complete history. */
class ReplayRuntime implements WorkflowRuntime {
  private seq = 0;
  private randomIndex = 0;
  private time = 0;
  private info: WorkflowInfo;
  private pending = new Map<number, Pending>();
  private cancelledTimers = new Set<number>();
  private signals = new Map<string, unknown[]>();
  private handlers = new Map<string, (input: unknown) => unknown>();
  private signalWaiters = new Map<string, ((input: unknown) => void)[]>();
  private conditions = new Set<Condition>();
  private patches = new Map<string, boolean>();
  private cancelled = false;
  private closed = false;
  private completion: { value?: unknown; error?: unknown; failed: boolean } | undefined;
  private fatal: unknown;
  private activation!: Activation;
  commands: Command[] = [];
  revision = 0;

  constructor(private readonly options: ReplayOptions, started: Extract<HistoryEvent["attributes"], {case:"runStarted"}>["value"]) {
    this.info = {runId: options.runId, workflowType: options.workflowType ?? started.workflowType,
      taskQueue: started.taskQueue, attempt: options.attempt ?? 1, isReplaying: true, continuedFromRunId: started.continuedFromRunId};
  }
  begin(activation: Activation): void {
    this.activation = activation;
    this.commands = [];
    this.info = { ...this.info, attempt: activation.attempt, isReplaying: !!activation.completed };
    const timestamp = activation.started.time;
    if (!timestamp) throw new Error(`TaskStarted ${activation.started.eventId} has no time`);
    this.time = Number(timestamp.seconds) * 1_000 + timestamp.nanos / 1_000_000;
    for (const event of activation.external) { this.deliver(event); this.checkConditions(); }
  }
  private emit(command: Command): void {
    if (this.closed) throw new Error("workflow emitted a command after closing");
    const position = this.commands.length;
    this.commands.push(command);
    this.revision++;
    if (this.activation.completed && this.fatal === undefined) {
      const expected = this.activation.recorded[position];
      try { matchCommands([command], expected ? [expected] : [], Number(this.activation.completed.eventId)); }
      catch (error) { this.fatal = error; }
    }
  }
  private newPending<T>(seq: number, kind: Pending["kind"], activityType?: string, gateProposal?: Pending["gateProposal"]): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      this.pending.set(seq, {kind, resolve: resolve as (value: unknown) => void, reject, ...(activityType === undefined ? {} : {activityType}), ...(gateProposal === undefined ? {} : {gateProposal})});
    });
  }
  proxyActivities<A>(options: ActivityOptions): ActivityProxy<A> {
    return new Proxy(Object.create(null) as object, { get: (_target, name) => {
      if (name === "then" || typeof name !== "string") return undefined;
      return (input: unknown) => this.activity(name, input, options);
    } }) as ActivityProxy<A>;
  }
  activity<T>(activityType: string, input: unknown, options: ActivityOptions): Promise<T> {
    if (this.cancelled) return Promise.reject(new CancelledFailure("run cancellation requested"));
    if (!activityType || durationMs(options.startToCloseTimeout) <= 0) throw new TypeError("activity requires a name and positive startToCloseTimeout");
    const seq = ++this.seq;
    let gateProposal: Pending["gateProposal"];
    if (activityType === "capstan.gate.decide") {
      const recorded = this.activation.recorded[this.commands.length]?.attributes;
      gateProposal = { current: input, recorded: recorded?.case === "activityScheduled" ? decode(recorded.value.input) : undefined };
    }
    this.emit(scheduleActivity(seq, activityType, input, options));
    return this.newPending<T>(seq, "activity", activityType, gateProposal);
  }
  sleep(duration: Duration): Promise<void> {
    if (this.cancelled) return Promise.reject(new CancelledFailure("run cancellation requested"));
    if (durationMs(duration) <= 0) throw new RangeError("sleep duration must be positive");
    const seq = ++this.seq;
    this.emit(create(CommandSchema, {attributes:{case:"startTimer",value:{seq:BigInt(seq),fireAfter:toProtoDuration(duration)}}}));
    return this.newPending<void>(seq,"timer");
  }
  now(): number { return this.time; }
  random(): number {
    const seed = `${this.options.runId}/${this.seq}/${this.randomIndex++}`;
    let hash = 2166136261;
    for (let i = 0; i < seed.length; i++) hash = Math.imul(hash ^ seed.charCodeAt(i), 16777619) >>> 0;
    return hash / 4294967296;
  }
  private marker<T>(name: string, markerId: string, value: () => T): T {
    if (this.fatal !== undefined) throw this.fatal;
    const seq = ++this.seq;
    let result: unknown;
    if (this.info.isReplaying) {
      const recorded = this.activation.recorded[this.commands.length];
      const emitted = create(CommandSchema,{attributes:{case:"recordMarker",value:{seq:BigInt(seq),name,markerId}}});
      try { matchCommands([emitted], recorded ? [recorded] : [], Number(this.activation.completed!.eventId)); }
      catch (error) { this.fatal = error; throw error; }
      if (recorded?.attributes.case !== "markerRecorded") throw new Error("marker comparison did not select a marker");
      result = decode(recorded.attributes.value.details);
    } else {
      try {
        const before = this.commands.length;
        result = value();
        if (this.commands.length !== before) throw new Error("sideEffect callbacks cannot emit workflow commands; call the workflow API outside the callback");
        encode(result);
      } catch (error) {
        this.abort(error);
        throw error;
      }
    }
    this.emit(create(CommandSchema,{attributes:{case:"recordMarker",value:{seq:BigInt(seq),name,markerId,details:encode(result)}}}));
    return result as T;
  }
  sideEffect<T>(fn: () => T): T { return this.marker("side_effect", "", fn); }
  uuid(): string { return this.marker("uuid", "", randomUUID); }
  private patch(patchId: string, deprecated: boolean): boolean {
    if (this.patches.has(patchId)) return this.patches.get(patchId)!;
    let name = deprecated ? "deprecated_patch" : "patch";
    if (this.info.isReplaying) {
      const attrs = this.activation.recorded[this.commands.length]?.attributes;
      if (attrs?.case !== "markerRecorded" || !["patch","deprecated_patch"].includes(attrs.value.name) || attrs.value.markerId !== patchId) {
        this.patches.set(patchId, false);
        return false;
      }
      name = attrs.value.name;
    }
    this.marker(name, patchId, () => true);
    this.patches.set(patchId, true);
    return true;
  }
  patched(patchId: string): boolean { return this.patch(patchId, false); }
  deprecatePatch(patchId: string): void { this.patch(patchId, true); }
  setHandler<T>(signal: SignalDefinition<T>, handler: (input: T) => void): void {
    this.handlers.set(signal.name, handler as (input: unknown) => unknown);
    const buffered = this.signals.get(signal.name) ?? [];
    this.signals.delete(signal.name);
    for (const value of buffered) this.callHandler(handler as (input: unknown) => unknown, value);
  }
  private callHandler(handler: (value: unknown) => unknown, value: unknown): void {
    this.revision++;
    try {
      const result = handler(value);
      // A handler is allowed to await even though the public signature returns void.
      if (result && typeof (result as PromiseLike<unknown>).then === "function") {
        void Promise.resolve(result).then(() => { this.revision++; }, (error: unknown) => this.fail(error));
      }
    } catch (error) { this.fail(error); }
  }
  nextSignal<T>(signal: SignalDefinition<T>): Promise<T> {
    const buffered = this.signals.get(signal.name);
    if (buffered?.length) { this.revision++; return Promise.resolve(buffered.shift() as T); }
    return new Promise<T>((resolve) => {
      const waiters = this.signalWaiters.get(signal.name) ?? [];
      waiters.push(resolve as (input: unknown) => void);
      this.signalWaiters.set(signal.name, waiters);
    });
  }
  condition(predicate: () => boolean, timeout?: Duration): Promise<boolean> {
    if (predicate()) return Promise.resolve(true);
    if (timeout !== undefined && durationMs(timeout) === 0) return Promise.resolve(false);
    return new Promise<boolean>((resolve, reject) => {
      const condition: Condition = {predicate,resolve,reject};
      if (timeout !== undefined) {
        if (this.cancelled) { reject(new CancelledFailure("run cancellation requested")); return; }
        const seq = ++this.seq;
        condition.timer = seq;
        this.emit(create(CommandSchema,{attributes:{case:"startTimer",value:{seq:BigInt(seq),fireAfter:toProtoDuration(timeout)}}}));
        this.pending.set(seq,{kind:"timer",resolve:()=>{this.conditions.delete(condition);resolve(false);},reject:(reason)=>{this.conditions.delete(condition);reject(reason);}});
      }
      this.conditions.add(condition);
    });
  }
  checkConditions(): void {
    for (const condition of this.conditions) {
      let ready: boolean;
      try { ready = condition.predicate(); }
      catch (error) { this.conditions.delete(condition); condition.reject(error); this.revision++; continue; }
      if (!ready) continue;
      this.conditions.delete(condition);
      if (condition.timer !== undefined && this.pending.delete(condition.timer)) {
        this.cancelledTimers.add(condition.timer);
        this.emit(create(CommandSchema,{attributes:{case:"cancelTimer",value:{seq:BigInt(condition.timer)}}}));
      }
      condition.resolve(true);
      this.revision++;
    }
  }
  isCancellationRequested(): boolean { return this.cancelled; }
  continueAsNew(input: unknown, options?: {workflowType?:string;taskQueue?:string}): never {
    this.emit(create(CommandSchema,{attributes:{case:"continueAsNew",value:{input:encode(input),workflowType:options?.workflowType ?? "",taskQueue:options?.taskQueue ?? ""}}}));
    this.closed = true;
    const signal = new Error("workflow continued as new"); signal.name = "ContinueAsNew"; throw signal;
  }
  workflowInfo(): WorkflowInfo { return { ...this.info }; }
  log(level: "info" | "warn" | "error", message: string, fields?: Record<string,unknown>): void {
    if (!this.info.isReplaying) this.options.logger?.({ ...fields, level, message, runId:this.info.runId });
  }
  requestApproval(request: ApprovalRequest): Promise<ApprovalResolution> {
    const seq=++this.seq;
    this.emit(create(CommandSchema,{attributes:{case:"requestApproval",value:{seq:BigInt(seq),approvalId:request.approvalId,
      source:request.source === "gate" ? ApprovalSource.GATE : ApprovalSource.HUMAN,
      gateDecisionId:request.gateDecisionId ?? "",tool:request.tool ?? "",arguments:encode(request.arguments),
      prompt:request.prompt ?? "",options:request.options ?? [],
      ...(request.timeout === undefined ? {} : {timeout:toProtoDuration(request.timeout)}),
    }}}));
    return this.newPending<ApprovalResolution>(seq,"approval");
  }
  abort(error: unknown): void { this.fatal ??= error ?? new Error("workflow callback threw an empty value"); this.revision++; }
  finish(value: unknown): void { if (!this.closed && !this.completion?.failed) { this.completion={failed:false,value}; this.revision++; } }
  fail(error: unknown): void {
    if (this.closed) return;
    const code = error && typeof error === "object" && "code" in error ? error.code : undefined;
    const message = error && typeof error === "object" && "message" in error ? String(error.message) : "";
    if (["SandboxViolationError","HistoryMismatchError"].includes(nameOf(error)) || code === "ERR_SCRIPT_EXECUTION_TIMEOUT" ||
        (nameOf(error) === "EvalError" && message.includes("Code generation from strings disallowed"))) this.fatal ??= error;
    else this.completion={failed:true,error};
    this.revision++;
  }
  private deliver(event: HistoryEvent): void {
    const attrs=event.attributes;
    if (attrs.case === "signalReceived") {
      const {name,input}=attrs.value;
      const value=decode(input);
      const handler=this.handlers.get(name);
      if (handler) this.callHandler(handler,value);
      else {
        const waiter=this.signalWaiters.get(name)?.shift();
        if (waiter) {waiter(value);this.revision++;}
        else {const buffer=this.signals.get(name) ?? [];buffer.push(value);this.signals.set(name,buffer);}
      }
      return;
    }
    if (attrs.case === "runCancelRequested") {
      this.cancelled=true;
      for (const [seq,pending] of this.pending) {
        if (pending.kind === "approval") continue;
        this.pending.delete(seq);
        pending.reject(new CancelledFailure(attrs.value.reason || "run cancellation requested"));
        this.revision++;
      }
      return;
    }
    if (!attrs.value || !("seq" in attrs.value)) throw new Error(`unexpected external event ${event.eventId}`);
    const seq=Number(attrs.value.seq);
    if (attrs.case === "timerFired" && this.cancelledTimers.has(seq)) return;
    const pending=this.pending.get(seq);
    const kind=attrs.case === "timerFired" ? "timer" : attrs.case === "approvalResolved" ? "approval" : "activity";
    if (!pending || pending.kind !== kind) throw new Error(`event ${event.eventId} refers to unknown or already-settled ${kind} seq ${seq}`);
    this.pending.delete(seq);
    this.revision++;
    switch(attrs.case) {
      case "activityCompleted": {
        let result = decode(attrs.value.result);
        if (pending.gateProposal) {
          const currentProposal = pending.gateProposal.current;
          const recordedProposal = pending.gateProposal.recorded;
          const failureEventId = Number(event.eventId);
          // D29 validates the proposal before user code receives the recorded decision.
          result = replayGateDecision(currentProposal, recordedProposal, result, seq, failureEventId);
        }
        pending.resolve(result);
        break;
      }
      case "activityFailed": pending.reject(new ActivityFailure(`activity ${pending.activityType} failed`,pending.activityType!,seq,
        attrs.value.failure ? {cause:failureFromProto(attrs.value.failure)} : {}));break;
      case "activityTimedOut": pending.reject(new TimeoutFailure(`activity ${pending.activityType} timed out`,
        (TimeoutType[attrs.value.timeoutType] || "START_TO_CLOSE") as TimeoutFailure["timeoutType"],
        attrs.value.lastFailure ? {cause:failureFromProto(attrs.value.lastFailure)} : {}));break;
      case "activityCancelled": pending.reject(new CancelledFailure(`activity ${pending.activityType} cancelled`,{details:decode(attrs.value.details)}));break;
      case "timerFired": pending.resolve(undefined);break;
      case "approvalResolved": pending.resolve({outcome:attrs.value.outcome === ApprovalOutcome.APPROVED ? "approved" : attrs.value.outcome === ApprovalOutcome.DENIED ? "denied" : "expired",
        choice:attrs.value.choice,resolver:attrs.value.resolver,note:attrs.value.note} satisfies ApprovalResolution);break;
      default: throw new Error(`unsupported external event ${event.eventId}`);
    }
  }
  async drain(): Promise<void> {
    let stable=0;
    while(stable<2) {
      this.checkConditions();
      const revision=this.revision;
      const commands=this.commands.length;
      await yieldToHost();
      this.checkConditions();
      if (this.fatal !== undefined) throw this.fatal;
      const revisionUnchanged = revision === this.revision;
      const commandsUnchanged = commands === this.commands.length;
      if (revisionUnchanged && commandsUnchanged) {
        stable++;
      } else {
        stable = 0;
      }
    }
    if (this.closed || !this.completion) return;
    const done=this.completion;
    if (!done.failed) this.emit(create(CommandSchema,{attributes:{case:"completeRun",value:{result:encode(done.value)}}}));
    else if (nameOf(done.error) === "CancelledFailure" && this.cancelled) this.emit(create(CommandSchema,{attributes:{case:"cancelRun",value:{}}}));
    else this.emit(create(CommandSchema,{attributes:{case:"failRun",value:{failure:failureToProto(done.error)}}}));
    this.closed=true;
  }
}

export async function replay(options: ReplayOptions): Promise<Command[]> {
  const started=options.history[0]?.attributes;
  if (started?.case !== "runStarted") throw new Error("history must begin with RunStarted");
  const runtime=new ReplayRuntime(options,started.value);
  let loaded=false;
  for (const activation of activations(options.history)) {
    runtime.begin(activation);
    if (!loaded) {
      const context=createContext(runtime, (error) => runtime.abort(error));
      new Script(options.bundle.code,{filename:"capstan-workflows.js"}).runInContext(context,{timeout:1_000});
      loaded=true;
      const workflowName = options.workflowType ?? started.value.workflowType;
      if (typeof context.__capstanWorkflows?.[workflowName] !== "function") throw new Error(`workflow export is not a function: ${workflowName}`);
      try {
        const result=invokeWorkflow(context,options.workflowType ?? started.value.workflowType,decode(started.value.input));
        void Promise.resolve(result).then((value:unknown)=>runtime.finish(value),(error:unknown)=>runtime.fail(error));
      } catch(error) { runtime.fail(error); }
    }
    await runtime.drain();
    if (activation.completed) matchCommands(runtime.commands,activation.recorded,Number(activation.completed.eventId));
    else return runtime.commands;
  }
  return [];
}
