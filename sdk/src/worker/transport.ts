import { createConnectTransport } from "@connectrpc/connect-node";
import type { Transport } from "@connectrpc/connect";

/** D9/D10: direct servers use h2c; HTTPS gateways use unary Connect over HTTP/1.1. */
export function createTransport(options: { address: string; apiKey: string }): Transport {
  const url = new URL(options.address);
  if (url.protocol !== "http:" && url.protocol !== "https:") throw new TypeError("Capstan address must use http:// or https://");
  return createConnectTransport({
    baseUrl: options.address,
    httpVersion: url.protocol === "http:" ? "2" : "1.1",
    defaultTimeoutMs: 35_000,
    interceptors: [(next) => async (request) => {
      request.header.set("authorization", `Bearer ${options.apiKey}`);
      return next(request);
    }],
  });
}
