import { readFileSync } from "node:fs";
export function bad() { return readFileSync("secret", "utf8"); }
