#!/usr/bin/env node
import { tsImport } from "tsx/esm/api";
const { main } = await tsImport("../src/cli/main.ts", import.meta.url);
const code = await main();
await Promise.all([
  new Promise((resolve) => process.stdout.write("", resolve)),
  new Promise((resolve) => process.stderr.write("", resolve)),
]);
process.exit(code);
