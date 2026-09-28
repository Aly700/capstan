import { activity, ApplicationFailure } from "../../src/workflow/index.ts";
export async function singleActivity(input: { n: number }) { return { doubled: await activity("double", input.n, { startToCloseTimeout: "10s" }) }; }
export async function throwsApplication() { throw new ApplicationFailure("workflow failed", { type: "UserFailure", nonRetryable: true }); }
export async function forbidden() { return fetch("http://example.invalid"); }
