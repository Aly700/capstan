import { readFileSync } from "fs";
export function bad() { return readFileSync("secret", "utf8"); }
