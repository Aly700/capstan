// Typed view of the shared real-process harness the evidence scripts use.
import type { ChildProcess } from "node:child_process";
// @ts-expect-error the harness is plain JavaScript shared with scripts/
import * as lib from "../../../scripts/evidence-lib.mjs";

export interface Evidence {
  readonly address: string;
  readonly key: string;
  readonly queue: string;
  readonly logdir: string;
  readonly dsn: string;
  server: ChildProcess;
  setup(): Promise<Evidence>;
  startServer(): Promise<void>;
  child(binary: string, args: string[], name: string, extraEnv?: Record<string, string>, cwd?: string): ChildProcess;
  stop(child: ChildProcess | undefined, signal?: NodeJS.Signals): Promise<void>;
  sql(query: string, args?: string[]): string;
  cleanup(): Promise<void>;
}
export const Evidence: new (name: string, port: number, options?: { databasePrefix?: string }) => Evidence = lib.Evidence;
export const root: string = lib.root;
export const until: <T>(what: string, probe: () => Promise<T | undefined | false> | T | undefined | false, timeout?: number) => Promise<T> = lib.until;
