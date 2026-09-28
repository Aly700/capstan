import { readFile, readdir } from "node:fs/promises";
import type { Fixture } from "./build.ts";

export async function loadFixtures(directory: URL, language: string): Promise<{ file: string; fixture: Fixture }[]> {
  const files = (await readdir(directory)).filter((file) => file.endsWith(".json")).sort();
  const fixtures = await Promise.all(files.map(async (file) => ({
    file,
    fixture: JSON.parse(await readFile(new URL(file, directory), "utf8")) as Fixture,
  })));
  return fixtures.filter(({ fixture }) => fixture.only === undefined || fixture.only.includes(language));
}
