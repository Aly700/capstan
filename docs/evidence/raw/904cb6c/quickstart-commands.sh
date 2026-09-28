export PATH="$HOME/.local/bin:$PATH"
export GOTOOLCHAIN=go1.26.4 COMPOSE_PROJECT_NAME=capstan
make pg-up
npm --prefix sdk ci
mkdir -p .lane
umask 077
go build -o .lane/quickstart-server ./cmd/capstan-server

export CAPSTAN_DEMO_DB="capstan_final_quickstart_$(date +%s)_$$"
PGPASSWORD=capstan createdb -h 127.0.0.1 -p 55432 -U capstan "$CAPSTAN_DEMO_DB"
export CAPSTAN_DATABASE_URL="postgres://capstan:capstan@127.0.0.1:55432/$CAPSTAN_DEMO_DB?sslmode=disable"
export CAPSTAN_API_KEY="$(openssl rand -hex 32)"
printf '%s' "$CAPSTAN_API_KEY" > .lane/quickstart-key
export CAPSTAN_API_KEY_HASHES="local:$(.lane/quickstart-server hash-key < .lane/quickstart-key)"
export CAPSTAN_ADDR=127.0.0.1:7773 CAPSTAN_ADDRESS=http://127.0.0.1:7773
export CAPSTAN_MIGRATE=true

.lane/quickstart-server serve > .lane/quickstart-server.log 2>&1 &
CAPSTAN_DEMO_SERVER_PID=$!
export EVIDENCE_QUEUE=quickstart EVIDENCE_IDENTITY=quickstart-worker
export EVIDENCE_WORKFLOWS="$PWD/examples/evidence/load.ts"
(cd sdk && exec node --import tsx ../examples/evidence/worker.ts) > .lane/quickstart-worker.log 2>&1 &
CAPSTAN_DEMO_WORKER_PID=$!

curl --retry 30 --retry-connrefused --retry-delay 1 --fail --silent --show-error "$CAPSTAN_ADDRESS/readyz"
node sdk/bin/capstan.mjs start loadFive quickstart-1 --queue quickstart
node sdk/bin/capstan.mjs describe quickstart-1

kill "$CAPSTAN_DEMO_WORKER_PID" "$CAPSTAN_DEMO_SERVER_PID"
wait "$CAPSTAN_DEMO_WORKER_PID" "$CAPSTAN_DEMO_SERVER_PID" || true
PGPASSWORD=capstan dropdb -h 127.0.0.1 -p 55432 -U capstan "$CAPSTAN_DEMO_DB"
rm .lane/quickstart-key
