#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Ephemeral npx tooling only; the SDK package and lockfile are untouched.
exec npx --yes --package @playwright/cli@0.1.21 -c 'node scripts/check-ui.mjs'
