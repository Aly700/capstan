export type Redactor = (text: string) => string;

/** Only diagnostics are redacted; successful workflow/activity payloads stay opaque. */
export function diagnosticRedactor(keys: Array<string | undefined>, env = process.env): Redactor {
  const values = [...keys, env.CAPSTAN_API_KEY, env.CAPSTAN_GATE_API_KEY, env.ANTHROPIC_API_KEY, env.CAPSTAN_DATABASE_URL];
  try {
    const password = new URL(env.CAPSTAN_DATABASE_URL ?? "").password;
    if (password) values.push(password, decodeURIComponent(password));
  } catch { /* Invalid configuration is still redacted as its complete value. */ }
  const secrets = [...new Set(values.filter((value): value is string => Boolean(value)))].sort((a, b) => b.length - a.length);
  return (text) => {
    for (const secret of secrets) text = text.replaceAll(secret, "[REDACTED]");
    return text;
  };
}

export function redactDiagnosticValue<T>(value: T, redact: Redactor): T {
  return JSON.parse(JSON.stringify(value, (_key, item: unknown) => typeof item === "string" ? redact(item) : item)) as T;
}
