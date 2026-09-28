import { Code, ConnectError } from "@connectrpc/connect";
import { afterEach, describe, expect, it } from "vitest";
import type { TerminateRunRequest } from "../gen/capstan/v1/capstan_pb.ts";
import { fakeServer } from "../../test/fake-server.ts";
import { Client } from "./index.ts";

const closers: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of closers.reverse()) await close(); closers.length = 0; });
async function setup(client: NonNullable<Parameters<typeof fakeServer>[0]>["client"] = {}) {
  const server = await fakeServer({ ...(client ? { client } : {}) });
  closers.push(server.close);
  return { server, client: new Client(server) };
}

describe("Client.terminate over scripted Connect", () => {
  it.each([undefined, "operator stopped the run"])("sends the run, optional reason (%s), and authentication", async (reason) => {
    const { server, client } = await setup();
    await expect(client.terminate("run-1", reason)).resolves.toBeUndefined();
    expect(server.requests<TerminateRunRequest>("TerminateRun")).toMatchObject([{ runId: "run-1", reason: reason ?? "" }]);
    expect(server.calls).toHaveLength(1);
    expect(server.calls[0]!.authorization).toBe("Bearer test-api-key");
  });

  it.each([Code.NotFound, Code.FailedPrecondition, Code.Unauthenticated])("propagates server error code %s", async (code) => {
    const { server, client } = await setup({ terminateRun: () => { throw new ConnectError("termination rejected", code); } });
    await expect(client.terminate("run-1", "stop")).rejects.toMatchObject({ code, rawMessage: "termination rejected" });
    expect(server.requests<TerminateRunRequest>("TerminateRun")).toMatchObject([{ runId: "run-1", reason: "stop" }]);
  });
});
