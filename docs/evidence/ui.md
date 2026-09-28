# Run viewer

Verified 2026-09-28 against the real Go server and SDK worker processes on PostgreSQL
16.15. Browser: Chrome 148.0.7778.168 via Playwright 1.64.0-alpha-1789764292000
(the ephemeral `@playwright/cli@0.1.21` npx package). No SDK dependencies changed.

```sh
GOTOOLCHAIN=go1.26.4 go test ./internal/server/ui ./internal/server
scripts/check-ui.sh
```

The browser check creates and drops its own database on the shared PostgreSQL, uses
port 7300, starts 12 completed workflows, and blocks a thirteenth by deploying a
worker with an incompatible activity type. These are persisted histories, not UI fixtures.
The test verifies status and exact workflow-type filters, next/previous pages, collapsed
payloads, session reload, disconnect, a replay mismatch, and its resume command.

Screenshots inspected at 1280px desktop and 390px mobile:

- [Run list](ui-list.png): status badges, type filter, timestamp columns and paging.
- [Completed timeline](ui-completed.png): event IDs/times, activity name/attempt, collapsed payloads and final completion.
- [Blocked timeline](ui-blocked.png): mismatch at history event 5, recorded in blocked event 14, plus the explicit resume command.
- [Mobile blocked timeline](ui-mobile.png): wrapped failure text and command, no page overflow or overlapping controls.

The static page is public. Its data uses same-origin HTTP/1.1 JSON Connect POSTs to
`ClientService` with `Authorization: Bearer …`; the existing interceptor accepted a
valid key and rejected missing/invalid keys with HTTP 401. CORS is not involved;
the browser check observed no preflight requests.

The key is kept only in `sessionStorage` and in memory, cleared from the password
field on submission and from storage on disconnect. No cookies or localStorage are
used. Only fixed read RPCs are called. Fetch omits credentials, disallows redirects,
and requests no caching. Errors do not echo server response bodies. The handler
uses a strict embedded-file allowlist, `Cache-Control: no-cache`, a content security
policy and `Referrer-Policy: no-referrer`. The single route registration in
`internal/server/handlers.go` also wires the existing server binary.

The browser check injected HTML into an actual workflow input and confirmed it was
rendered as text, never executed. It also checked that the session key was absent
from request URLs/bodies, page markup, and server/worker logs. Payloads use text nodes
and decoded JSON, never `innerHTML`; the current key is redacted from displayed strings.
These checks cover the viewer's own paths, not a guarantee against a compromised
browser, extensions, or other scripts served by the same origin. Keep real credentials
out of workflow inputs: the viewer cannot remove secrets already persisted by callers.
