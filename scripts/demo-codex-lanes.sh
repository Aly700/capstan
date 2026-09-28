#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/.local/bin:$PATH"
export GOTOOLCHAIN=go1.26.4
exec node scripts/demo-codex-lanes.mjs
