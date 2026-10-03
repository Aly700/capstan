import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { fileURLToPath, pathToFileURL } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { run } from "./build.ts";
import { loadFixtures } from "./load.ts";

let directory: URL;
beforeAll(async () => {
  await mkdir(new URL("../../../.lane/", import.meta.url), { recursive: true });
  const path = await mkdtemp(fileURLToPath(new URL("../../../.lane/delta1-fixtures-", import.meta.url)));
  directory = pathToFileURL(`${path}/`);
  const fixture = run("testWorkflow").task().expectCommands();
  const files = [
    ["005-none.json", { ...fixture, only: [] }],
    ["004-both.json", { ...fixture, only: ["go", "ts"] }],
    ["003-go.json", { ...fixture, only: ["go"] }],
    ["002-ts.json", { ...fixture, only: ["ts"] }],
    ["001-shared.json", fixture],
  ] as const;
  await Promise.all(files.map(([name, value]) => writeFile(new URL(name, directory), JSON.stringify(value))));
  await writeFile(new URL("README.md", directory), "This is not fixture JSON.");
});
afterAll(async () => { if (directory) await rm(directory, { recursive: true, force: true }); });

describe("conformance fixture language selection", () => {
  it.each([
    ["ts", ["001-shared.json", "002-ts.json", "004-both.json"]],
    ["go", ["001-shared.json", "003-go.json", "004-both.json"]],
    ["another-language", ["001-shared.json"]],
  ])("loads shared and explicitly included fixtures for %s in filename order", async (language, expected) => {
    const loaded = await loadFixtures(directory, language as string);
    expect(loaded.map(({ file }) => file)).toEqual(expected);
  });
});
