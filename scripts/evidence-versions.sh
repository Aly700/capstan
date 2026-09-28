#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
sysctl -n machdep.cpu.brand_string hw.memsize hw.logicalcpu
sw_vers
GOTOOLCHAIN=go1.26.4 go version
GOTOOLCHAIN=go1.26.4 go list -m connectrpc.com/connect github.com/jackc/pgx/v5 google.golang.org/protobuf
node --version
npm --version
node -e 'const p = require("./sdk/package.json"); console.log(JSON.stringify({ dependencies: p.dependencies, devDependencies: p.devDependencies }, null, 2))'
psql --version
psql "${CAPSTAN_TEST_DATABASE_URL:-postgres://capstan:capstan@127.0.0.1:55432/postgres?sslmode=disable}" -XAtqc 'select version()'
colima version
colima list
docker info --format '{{.NCPU}} CPUs; {{.MemTotal}} bytes memory; Docker server {{.ServerVersion}}'
buf --version
asciinema --version
vhs --version
