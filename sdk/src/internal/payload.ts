import { create } from "@bufbuild/protobuf";
import { PayloadSchema } from "../gen/capstan/v1/capstan_pb.ts";
import type { Payload } from "../gen/capstan/v1/capstan_pb.ts";

/** Undefined is represented by message absence, while JSON null remains a payload. */
export function encode(value: unknown): Payload | undefined {
  if (value === undefined) return undefined;
  const json = JSON.stringify(value);
  if (json === undefined) throw new TypeError("payload must be JSON-serializable");
  return create(PayloadSchema, { contentType: "application/json", data: new TextEncoder().encode(json) });
}

export function decode(payload?: Payload): unknown {
  if (!payload) return undefined;
  if (payload.contentType !== "application/json") {
    throw new TypeError(`unsupported payload content type: ${payload.contentType}`);
  }
  return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(payload.data));
}
