# Capstan build gate. `make verify` is what every lane must pass before merge.
SHELL := /bin/bash
export PATH := $(HOME)/go/bin:$(PATH)
# Gates run on the toolchain go.mod names, as CI does (D26); override with GOTOOLCHAIN=local.
export GOTOOLCHAIN ?= go1.26.4
export CAPSTAN_TEST_DATABASE_URL ?= postgres://capstan:capstan@127.0.0.1:55432/postgres?sslmode=disable

.PHONY: gen gen-check lint test test-go test-sdk pg-up pg-down build verify

gen:
	buf generate

# Generated code is committed; fail if it drifts from the proto.
gen-check: gen
	@git diff --exit-code -- gen sdk/src/gen || (echo "generated code is stale: run make gen and commit" && exit 1)

lint:
	buf lint
	@files=$$(git ls-files --cached --others --exclude-standard '*.go' | grep -v '^gen/'); \
	  if [ -n "$$files" ]; then bad=$$(gofmt -l $$files </dev/null); \
	  if [ -n "$$bad" ]; then echo "gofmt needed:"; echo "$$bad"; exit 1; fi; fi
	go vet ./...
	cd sdk && npx tsc --noEmit

test-go:
	go test -race -count=1 $$(go list ./... | grep -v '/internal/lab$$')
	go test -race -count=1 ./internal/lab -seeds 50
	go test -count=1 ./internal/lab -seeds 2000
	go test -race -count=1 -tags pgengine ./internal/engine/...

test-sdk:
	cd sdk && CAPSTAN_E2E=1 npx vitest run

test: test-go test-sdk

# One shared PostgreSQL for all tests and lanes. Tests create and drop their own databases.
pg-up:
	docker compose up -d --wait postgres

pg-down:
	docker compose down

build:
	go build -o bin/capstan-server ./cmd/capstan-server

verify: gen-check lint test
