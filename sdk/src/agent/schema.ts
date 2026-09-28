import { transformJSONSchema } from "@anthropic-ai/sdk/lib/transform-json-schema";
import { z } from "zod";

// JSON Schema defaults are annotations, not permission to fill missing required
// output. Only visit schema positions: a property or a const value named "default"
// is user data and must survive unchanged.
function withoutDefaults(value: unknown): unknown {
  if (!value || typeof value !== "object" || Array.isArray(value)) return value;
  const schema = { ...value } as Record<string, unknown>;
  // The provider SDK transforms $defs, but drops legacy definitions while keeping
  // their references. Reject that unsupported form before reserving or calling it.
  if (schema.definitions !== undefined) throw new Error("use $defs instead of legacy definitions");
  delete schema.default;
  for (const key of ["properties", "patternProperties", "$defs", "definitions", "dependentSchemas"]) {
    const entries = schema[key];
    if (entries && typeof entries === "object" && !Array.isArray(entries)) schema[key] = Object.fromEntries(Object.entries(entries).map(([name, child]) => [name, withoutDefaults(child)]));
  }
  for (const key of ["items", "additionalItems", "contains", "additionalProperties", "propertyNames", "not", "if", "then", "else", "unevaluatedItems", "unevaluatedProperties", "contentSchema", "allOf", "anyOf", "oneOf", "prefixItems"]) {
    const child = schema[key];
    if (child !== undefined) schema[key] = Array.isArray(child) ? child.map(withoutDefaults) : withoutDefaults(child);
  }
  return schema;
}

export function prepareSchema(schema: Record<string, unknown>) {
  const validator = z.fromJSONSchema(withoutDefaults(schema) as Record<string, unknown>);
  // This is the installed SDK's own transformation, also used by its JSON Schema
  // helper. Unsupported provider constraints become descriptions; validation below
  // still enforces the original schema, including numeric and string bounds.
  const format = { type: "json_schema" as const, schema: transformJSONSchema(schema) };
  return { format, valid: (value: unknown) => validator.safeParse(value).success };
}
