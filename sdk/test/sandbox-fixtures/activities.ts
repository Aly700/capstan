import { readFile } from "node:fs/promises";
export async function outside(path: string) { return readFile(path, "utf8"); }
