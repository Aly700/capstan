# Cost of a real agent run

Verified 2026-09-28: [recording](demo-cost.cast).

```sh
CAPSTAN_ANTHROPIC_ENV_FILE=<file holding ANTHROPIC_API_KEY> scripts/demo-cost.sh
```

The workflow ([examples/evidence/cost.ts](../../examples/evidence/cost.ts)) makes one real
`model()` call to `claude-haiku-4-5-20251001`, then waits on a signal. The script stops the
worker that made the call, starts a fresh worker, and sends the signal, so the fresh worker
must replay the call from history before it can finish the run.

Result from the recorded run:

| id | model | status | estimate_usd | cost_usd | input_tokens | output_tokens |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | claude-haiku-4-5-20251001 | 2 (finished) | 0.010000 | 0.000034 | 14 | 4 |

- One ledger row after the replay on a second worker, so replay did not call or bill again.
- The server priced it: 14 × $1/M + 4 × $5/M = $0.000034. The `claude-haiku-4-5` family
  price matched the dated model id by prefix (D25). The estimate reserved against the $2.00
  daily cap was $0.01.
- `DescribeRun` reports the same `costUsd` as the ledger, and the workflow received that
  server-priced figure in its result.
- The `ai_call` ledger stores token counts and cost, without prompt or response text.
  Model request and result payloads are persisted separately in workflow history. The
  script never prints the API key (the recording was checked for it).
