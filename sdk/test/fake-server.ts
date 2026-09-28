import { createServer, type ServerHttp2Session } from "node:http2";
import { once } from "node:events";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import type { HandlerContext, ServiceImpl } from "@connectrpc/connect";
import { ClientService, WorkerService } from "../src/gen/capstan/v1/capstan_pb.ts";

export interface RecordedCall {
  method: string;
  request: unknown;
  authorization: string | null;
  timeoutMs: number | undefined;
  at: number;
}

export async function fakeServer(scripts: {
  worker?: Partial<ServiceImpl<typeof WorkerService>>;
  client?: Partial<ServiceImpl<typeof ClientService>>;
} = {}) {
  const calls: RecordedCall[] = [];
  const idle = async (_request: unknown, context: HandlerContext) => {
    if (!context.signal.aborted) await new Promise<void>((resolve) => context.signal.addEventListener("abort", () => resolve(), { once: true }));
    return {};
  };
  function wrap(service: typeof WorkerService | typeof ClientService, implementations: Record<string, unknown>) {
    return Object.fromEntries(Object.entries(service.method).map(([key, method]) => [key, async (request: unknown, context: HandlerContext) => {
      calls.push({ method: method.name, request, authorization: context.requestHeader.get("authorization"), timeoutMs: context.timeoutMs(), at: performance.now() });
      const handler = implementations[key] as ((request: unknown, context: HandlerContext) => unknown) | undefined;
      return handler ? handler(request, context) : {};
    }]));
  }
  const server = createServer(connectNodeAdapter({
    routes(router) {
      router.service(WorkerService, wrap(WorkerService, { pollWorkflowTask: idle, pollActivityTask: idle, ...scripts.worker }) as Partial<ServiceImpl<typeof WorkerService>>);
      router.service(ClientService, wrap(ClientService, { ...scripts.client }) as Partial<ServiceImpl<typeof ClientService>>);
    },
  }));
  const sessions = new Set<ServerHttp2Session>();
  server.on("session", (session) => { sessions.add(session); session.on("close", () => sessions.delete(session)); });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("expected TCP address");
  return {
    address: `http://127.0.0.1:${address.port}`,
    apiKey: "test-api-key",
    calls,
    requests<T>(method: string): T[] { return calls.filter((call) => call.method === method).map((call) => call.request as T); },
    async close() {
      for (const session of sessions) session.destroy();
      await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    },
  };
}
