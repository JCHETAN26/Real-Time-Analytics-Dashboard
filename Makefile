# StreamSense AI — Makefile
#
# Targets are grouped into three tiers:
#
#   Tier 1 — No dependencies (tests, build, lint): always runnable, runs in CI.
#   Tier 2 — Needs Docker: infra bring-up, connector registration.
#   Tier 3 — Needs Snowflake + Gradient AI creds: full end-to-end smoke test.
#
# Quick start:
#   make test          # run all unit tests + offline eval (no Docker needed)
#   make build         # compile all Go services
#   make verify        # full end-to-end smoke test (see scripts/smoke-test.sh)

SHELL := /bin/bash
.DEFAULT_GOAL := help

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT      := $(shell pwd)
# Auto-detect the macOS SDK; override with: export SDKROOT=$(xcrun --show-sdk-path)
# If you see "Failed to determine realpath of MacOSX14.sdk", run:
#   export SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX15.sdk
SDKROOT   ?= $(shell xcrun --sdk macosx --show-sdk-path 2>/dev/null || \
               echo /Library/Developer/CommandLineTools/SDKs/MacOSX15.sdk)

# Go modules that need CGO (confluent-kafka-go, snowflake driver)
CGO_MODULES := \
	producers/user-events \
	producers/order-events \
	producers/inventory-events \
	processor/stream-processor \
	ai-layer/query-agent

# Go modules that are pure-Go (no CGO needed)
PURE_MODULES := \
	ai-layer/anomaly-detector \
	ai-layer/forecast-engine

# ── Tier 1: Tests (no external dependencies) ──────────────────────────────────

.PHONY: test
test: test-guard test-eval test-diagnostics ## Run all unit tests + offline eval
	@echo ""
	@echo "✅ All tests passed."

.PHONY: test-guard
test-guard: ## SQL safety guardrail unit tests
	@echo "── guard tests ──"
	@cd ai-layer/query-agent && CGO_ENABLED=0 go test ./guard/... -v

.PHONY: test-eval
test-eval: ## Text-to-SQL eval harness (offline, no API key needed)
	@echo "── eval grader tests ──"
	@cd ai-layer/query-agent && CGO_ENABLED=0 go test ./eval/... -v
	@echo "── offline eval benchmark ──"
	@cd ai-layer/query-agent && CGO_ENABLED=0 go run ./eval/cmd/runeval -mode offline

.PHONY: test-diagnostics
test-diagnostics: ## Why Engine diagnostic signal tests
	@echo "── diagnostics tests ──"
	@cd ai-layer/anomaly-detector && CGO_ENABLED=0 go test ./... -v

.PHONY: test-live
test-live: ## Run eval against live Gradient AI (requires GRADIENT_AI_KEY)
	@[ -n "$$GRADIENT_AI_KEY" ] || (echo "❌ GRADIENT_AI_KEY is not set" && exit 1)
	@cd ai-layer/query-agent && CGO_ENABLED=0 go run ./eval/cmd/runeval -mode live -json eval-report.json
	@echo "Report written to ai-layer/query-agent/eval-report.json"

# ── Tier 1: Build ─────────────────────────────────────────────────────────────

.PHONY: build
build: build-pure build-cgo ## Build all Go services

.PHONY: build-pure
build-pure: ## Build pure-Go AI services (no CGO)
	@for mod in $(PURE_MODULES); do \
		echo "── build $$mod ──"; \
		(cd $$mod && CGO_ENABLED=0 go build ./...) || exit 1; \
	done
	@cd ai-layer/query-agent && CGO_ENABLED=0 go build ./guard/... ./eval/...

.PHONY: build-cgo
build-cgo: ## Build CGO services (Kafka producers, processor, query-agent main)
	@for mod in $(CGO_MODULES); do \
		echo "── build $$mod ──"; \
		(cd $$mod && SDKROOT=$(SDKROOT) CGO_ENABLED=1 go build ./...) || exit 1; \
	done

.PHONY: vet
vet: ## go vet all AI modules
	@for mod in ai-layer/query-agent ai-layer/anomaly-detector ai-layer/forecast-engine; do \
		echo "── vet $$mod ──"; \
		(cd $$mod && CGO_ENABLED=0 go vet ./...); \
	done

# ── Tier 1: dbt validation ────────────────────────────────────────────────────

.PHONY: dbt-validate
dbt-validate: ## Validate dbt ref/source graph (no warehouse needed)
	@echo "── dbt graph validation ──"
	@python3 warehouse/dbt/validate_refs.py

.PHONY: dbt-run
dbt-run: ## Run dbt transformations (requires SNOWFLAKE profile configured)
	@cd warehouse/dbt && dbt run

.PHONY: dbt-test
dbt-test: ## Run dbt tests (requires SNOWFLAKE profile configured)
	@cd warehouse/dbt && dbt test

# ── Tier 1: CI gate (everything that runs without Docker or secrets) ───────────

.PHONY: ci
ci: test build vet dbt-validate ## Full CI check — matches GitHub Actions
	@echo ""
	@echo "✅ CI checks passed."

# ── Tier 2: Infrastructure ────────────────────────────────────────────────────

.PHONY: infra-up
infra-up: ## Start Kafka, Zookeeper, Schema Registry, Kafka Connect
	@echo "── starting infrastructure ──"
	@cd infra && docker compose up -d
	@echo "Waiting for Kafka to be ready..."
	@scripts/wait-for-kafka.sh

.PHONY: infra-down
infra-down: ## Stop and remove all infrastructure containers
	@cd infra && docker compose down

.PHONY: infra-status
infra-status: ## Show status of all infrastructure containers
	@cd infra && docker compose ps

.PHONY: infra-logs
infra-logs: ## Tail logs from all infrastructure containers
	@cd infra && docker compose logs -f

.PHONY: connector-register
connector-register: ## Register the Snowflake Sink connector (requires SNOWFLAKE_ACCOUNT_URL + SNOWFLAKE_PRIVATE_KEY)
	@[ -n "$$SNOWFLAKE_ACCOUNT_URL" ] || (echo "❌ SNOWFLAKE_ACCOUNT_URL is not set" && exit 1)
	@[ -n "$$SNOWFLAKE_PRIVATE_KEY" ] || (echo "❌ SNOWFLAKE_PRIVATE_KEY is not set" && exit 1)
	@bash infra/connectors/register.sh

.PHONY: connector-status
connector-status: ## Check Snowflake Sink connector status
	@curl -s http://localhost:8083/connectors/snowflake-sink/status | python3 -m json.tool

.PHONY: keys-gen
keys-gen: ## Generate RSA key pair for Snowflake connector auth
	@bash infra/snowflake/gen_keys.sh

# ── Tier 2: Run services locally ──────────────────────────────────────────────

.PHONY: producers
producers: ## Start all three event producers in the background
	@echo "── starting producers ──"
	@export SDKROOT=$(SDKROOT) && \
	  (cd producers/user-events      && go run main.go &) && \
	  (cd producers/order-events     && go run main.go &) && \
	  (cd producers/inventory-events && go run main.go &)
	@echo "Producers running in background. PID file: .producer.pids"
	@jobs -p > .producer.pids 2>/dev/null || true

.PHONY: processor
processor: ## Start the stream processor in the background
	@export SDKROOT=$(SDKROOT) && \
	  (cd processor/stream-processor && go run main.go &)

.PHONY: ai-services
ai-services: ## Start all AI services in the background
	@export SDKROOT=$(SDKROOT) && \
	  (cd ai-layer/query-agent      && go run main.go &) && \
	  (cd ai-layer/anomaly-detector && go run main.go &) && \
	  (cd ai-layer/forecast-engine  && go run main.go &)

.PHONY: frontend
frontend: ## Start the React dashboard (runs in foreground)
	@cd dashboard/frontend && bun dev

.PHONY: stop
stop: ## Stop all background Go services
	@echo "── stopping services ──"
	@pkill -f "go run main.go" 2>/dev/null && echo "Services stopped" || echo "No services running"

# ── Tier 3: End-to-end smoke test ─────────────────────────────────────────────

.PHONY: verify
verify: ## Full end-to-end smoke test (Docker required; Snowflake/AI creds optional)
	@bash scripts/smoke-test.sh

# ── Housekeeping ──────────────────────────────────────────────────────────────

.PHONY: clean
clean: ## Remove build artifacts and temporary files
	@find . -name '*.test' -delete
	@find . -name '*.out'  -delete
	@find . -name 'eval-report.json' -delete
	@find . -path '*/bin/*' -type f -delete
	@echo "Cleaned."

.PHONY: help
help: ## Show this help message
	@echo "StreamSense AI — available targets:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' \
		| sort
	@echo ""
	@echo "Tiers:  test/build/vet/dbt-validate/ci  — no Docker/secrets needed"
	@echo "        infra-*/connector-*/producers    — needs Docker"
	@echo "        verify                           — needs Docker; Snowflake optional"
