import { activity } from "@capstan/sdk/workflow";

// No simulated work or host sleep: measures scheduling, replay, RPC and storage.
export async function loadFive(): Promise<number> {
  let value = 0;
  for (let index = 0; index < 5; index++) {
    value = await activity<number>("step", value, { startToCloseTimeout: "60s" });
  }
  return value;
}
