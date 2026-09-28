// Workflows referenced by the conformance fixtures. Each export here has a Go twin with the
// same name in internal/lab/scenarios. Keep both in step.

import { proxyActivities } from "@capstan/sdk/workflow";

type Activities = {
  double(n: number): number;
};

// fixtures/001-single-activity.json
export async function singleActivity(input: { n: number }): Promise<{ doubled: number }> {
  const acts = proxyActivities<Activities>({ startToCloseTimeout: "10s" });
  const doubled = await acts.double(input.n);
  return { doubled };
}
