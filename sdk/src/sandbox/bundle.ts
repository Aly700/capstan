import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { builtinModules } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";

export interface WorkflowBundle { code: string; buildId: string }
const sdkWorkflow = fileURLToPath(new URL("../workflow/index.ts", import.meta.url));
const sdkTypes = fileURLToPath(new URL("../types.ts", import.meta.url));
const builtins = new Set(builtinModules.map((name) => name.replace(/^node:/, "")));
const cache = new Map<string, WorkflowBundle>();

/** Build from source on every call so changes in any dependency invalidate the cache. */
export async function bundleWorkflows(workflowsPath: string): Promise<WorkflowBundle> {
  const entry = path.resolve(workflowsPath);
  const result = await build({
    stdin: {
      contents: `import * as workflows from ${JSON.stringify(entry)};
        import * as failures from ${JSON.stringify(sdkTypes)};
        globalThis[Symbol.for("capstan.sandbox.failures")](failures);
        globalThis.__capstanWorkflows = workflows;`,
      resolveDir: path.dirname(entry), sourcefile: "capstan-workflow-entry.ts", loader: "ts",
    },
    bundle: true, format: "iife", platform: "neutral", target: "es2024", write: false,
    metafile: true, logLevel: "silent",
    plugins: [{ name: "capstan-workflow-imports", setup(builder) {
      builder.onResolve({ filter: /.*/ }, (args) => {
        if (args.path.startsWith("node:") || builtins.has(args.path)) {
          return { errors: [{ text: `workflow cannot import ${JSON.stringify(args.path)}; put Node I/O in an activity and use import type for activity types` }] };
        }
        if (args.path === "@capstan/sdk/workflow") return { path: sdkWorkflow };
        return undefined;
      });
    } }],
  });
  const code = result.outputFiles[0]?.text;
  if (code === undefined) throw new Error("workflow bundle produced no JavaScript");
  const digest = createHash("sha256").update(code);
  for (const input of Object.keys(result.metafile.inputs).sort()) {
    if (path.resolve(input) === path.join(path.dirname(entry), "capstan-workflow-entry.ts")) continue;
    digest.update(await readFile(path.resolve(input)));
  }
  const buildId = digest.digest("hex");
  const cached = cache.get(buildId);
  if (cached) return cached;
  const bundle = { code, buildId };
  cache.set(buildId, bundle);
  if (cache.size > 32) cache.delete(cache.keys().next().value!);
  return bundle;
}
