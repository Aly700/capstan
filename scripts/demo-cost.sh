#!/usr/bin/env bash
# One real model call through Capstan, recorded in the ai_call ledger. Reads the Anthropic key
# from ANTHROPIC_API_KEY, or from the file named by CAPSTAN_ANTHROPIC_ENV_FILE, without echoing it.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -z "${ANTHROPIC_API_KEY:-}" && -n "${CAPSTAN_ANTHROPIC_ENV_FILE:-}" ]]; then
  ANTHROPIC_API_KEY="$(grep -E '^ANTHROPIC_API_KEY=' "$CAPSTAN_ANTHROPIC_ENV_FILE" | head -1 | cut -d= -f2- | tr -d '"'"'"'')"
  export ANTHROPIC_API_KEY
fi
exec node scripts/demo-cost.mjs
