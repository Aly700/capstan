import { setTimeout as delay } from "node:timers/promises";
import { Code, ConnectError } from "@connectrpc/connect";

export type Logger = (entry: Record<string, unknown>) => void;

export async function pollLoop<T extends { taskToken: Uint8Array }>(options: {
  poll: () => Promise<T>;
  execute: (task: T) => Promise<void>;
  signal: AbortSignal;
  logger: Logger;
  kind: string;
}): Promise<void> {
  let errors = 0;
  while (!options.signal.aborted) {
    try {
      const task = await options.poll();
      errors = 0;
      if (options.signal.aborted) return;
      if (task.taskToken.length) await options.execute(task);
      else await delay(10, undefined, { signal: options.signal });
    } catch (error) {
      if (options.signal.aborted) return;
      options.logger({ level: "warn", message: "task poll failed", kind: options.kind, error: error instanceof Error ? error.message : String(error) });
      const backoff = Math.min(100 * 2 ** Math.min(errors++, 7), 10_000);
      await delay(Math.min(10_000, backoff * (0.8 + Math.random() * 0.4)), undefined, { signal: options.signal }).catch(() => {});
    }
  }
}

export async function report(action: () => Promise<unknown>, logger: Logger, kind: string): Promise<void> {
  try { await action(); }
  catch (error) {
    const stale = error instanceof ConnectError && error.code === Code.FailedPrecondition;
    logger({ level: stale ? "info" : "warn", message: stale ? "stale task result dropped" : "task result could not be reported", kind, error: error instanceof Error ? error.message : String(error) });
  }
}

/** Always observes the underlying work, including rejection after an abort has won. */
export async function untilAborted<T>(work: Promise<T>, signal: AbortSignal): Promise<T> {
  let onAbort: () => void = () => {};
  const aborted = new Promise<never>((_resolve, reject) => {
    onAbort = () => reject(signal.reason);
    if (signal.aborted) onAbort();
    else signal.addEventListener("abort", onAbort, { once: true });
  });
  try { return await Promise.race([work, aborted]); }
  finally { signal.removeEventListener("abort", onAbort); }
}
