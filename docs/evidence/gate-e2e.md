# Tool calls through the real AgentOps Gate

Verified 2026-09-28 by the agent lane: [run log](raw/historical/agent/agent-delta1-gate-e2e.log),
[compose file used](raw/historical/agent/agent-e2e-compose.yml) (environment references only).

```sh
cd sdk && CAPSTAN_E2E_GATE=1 npx vitest run test/agent-e2e.test.ts
```

The test ([sdk/test/agent-e2e.test.ts](../../sdk/test/agent-e2e.test.ts)) runs the real
capstan-server on PostgreSQL and the real AgentOps Gate from its own Docker Compose stack,
under an isolated project name that is torn down afterwards. Four cases pass:

| Case | What it shows |
| --- | --- |
| ALLOW | The Gate allows the call and the registered tool runs once. |
| DENY | The Gate denies it; the run completes and the tool never runs. |
| REQUIRE_APPROVAL | The run waits on a real Gate approval while the single-slot worker does other work; approving it in the Gate lets the tool run. |
| Worker killed while waiting | The worker is SIGKILLed during the approval wait; the approval is observed with no worker running, and the run resumes and finishes once. |

The tool always runs with the arguments the Gate decided on (D29); changed arguments block
the run instead ([replay tests](../../sdk/test/agent-replay.test.ts)).
