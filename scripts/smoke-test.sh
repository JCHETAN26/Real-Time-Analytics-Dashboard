#!/usr/bin/env bash
# StreamSense end-to-end smoke test
#
# Verifies the full pipeline tier by tier, printing a final summary of every
# check so failures are immediately obvious. Exits 1 if any required check
# fails; optional checks (Snowflake, Gradient AI) emit warnings and skip.
#
# Tiers:
#   1. Static  — unit tests, eval, dbt graph (no Docker, no secrets)
#   2. Infra   — Kafka + Schema Registry healthy, topics exist
#   3. Produce — producers run for 30s, message counts verified per topic
#   4. Process — stream processor starts, SSE endpoint responds
#   5. AI      — each AI service health endpoint returns 200
#   6. Sink    — Snowflake connector registered + RUNNING (skipped if no creds)
#   7. Query   — AI query agent returns structured JSON (skipped if no AI key)

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SDKROOT="${SDKROOT:-/Library/Developer/CommandLineTools/SDKs/MacOSX15.sdk}"

# ── Colour helpers ─────────────────────────────────────────────────────────────
GREEN="\033[0;32m"; YELLOW="\033[0;33m"; RED="\033[0;31m"; RESET="\033[0m"
BOLD="\033[1m"

PASSED=(); SKIPPED=(); FAILED=()
PIDS=()
INFRA_OK=false

record_pass()  { echo -e "${GREEN}  ✅ PASS${RESET} $*"; PASSED+=("$*");  }
record_warn()  { echo -e "${YELLOW}  ⚠️  SKIP${RESET} $*"; SKIPPED+=("$*"); }
record_fail()  { echo -e "${RED}  ❌ FAIL${RESET} $*"; FAILED+=("$*");  }

# ── Cleanup ───────────────────────────────────────────────────────────────────
cleanup() {
    echo ""
    echo "── cleanup ──"
    for pid in "${PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    cd "$ROOT/infra" && docker compose down --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ── Summary (called at the end) ───────────────────────────────────────────────
print_summary() {
    echo ""
    echo -e "${BOLD}════════════════════════════════════════════════════════════"
    echo      "  Smoke Test Summary"
    echo -e   "════════════════════════════════════════════════════════════${RESET}"

    echo -e "${GREEN}Passed (${#PASSED[@]})${RESET}"
    for c in "${PASSED[@]:-}"; do echo "  ✅ $c"; done

    if [[ ${#SKIPPED[@]} -gt 0 ]]; then
        echo ""
        echo -e "${YELLOW}Skipped (${#SKIPPED[@]})${RESET}"
        for c in "${SKIPPED[@]}"; do echo "  ⚠️  $c"; done
    fi

    if [[ ${#FAILED[@]} -gt 0 ]]; then
        echo ""
        echo -e "${RED}Failed (${#FAILED[@]})${RESET}"
        for c in "${FAILED[@]}"; do echo "  ❌ $c"; done
        echo ""
        echo "  Logs: /tmp/streamsense-*.log"
        return 1
    fi

    echo ""
    echo -e "${GREEN}${BOLD}✅ Smoke test passed.${RESET} ${#PASSED[@]} checks, ${#SKIPPED[@]} skipped."
    return 0
}

# ─────────────────────────────────────────────────────────────────────────────
echo -e "\n${BOLD}════════════════════════════════════════════════════════════"
echo      "  StreamSense AI — End-to-End Smoke Test"
echo -e   "════════════════════════════════════════════════════════════${RESET}\n"

# ── TIER 1: Static checks ─────────────────────────────────────────────────────
echo -e "${BOLD}Tier 1 — Static${RESET}"

cd "$ROOT/ai-layer/query-agent"
if CGO_ENABLED=0 go test ./guard/... ./eval/... -count=1 -q 2>&1 \
        | grep -qE "^ok"; then
    record_pass "sql-guardrail-and-eval-tests"
else
    record_fail "sql-guardrail-and-eval-tests"
fi

if CGO_ENABLED=0 go run ./eval/cmd/runeval -mode offline 2>&1 \
        | grep -q "meets the 70% bar"; then
    record_pass "offline-eval-benchmark"
else
    record_fail "offline-eval-benchmark"
fi

cd "$ROOT/ai-layer/anomaly-detector"
if CGO_ENABLED=0 go test ./... -count=1 -q 2>&1 \
        | grep -qE "^ok"; then
    record_pass "diagnostics-tests"
else
    record_fail "diagnostics-tests"
fi

cd "$ROOT"
if python3 warehouse/dbt/validate_refs.py 2>&1 | grep -q "All ref"; then
    record_pass "dbt-graph-validation"
else
    record_fail "dbt-graph-validation"
fi
echo ""

# ── TIER 2: Infrastructure ────────────────────────────────────────────────────
echo -e "${BOLD}Tier 2 — Infrastructure${RESET}"

cd "$ROOT/infra"
docker compose up -d 2>&1 | tail -5

if bash "$SCRIPT_DIR/wait-for-kafka.sh" 2>&1 | grep -q "Schema Registry ready"; then
    record_pass "kafka-and-schema-registry-healthy"
    INFRA_OK=true
else
    record_fail "kafka-and-schema-registry-healthy"
fi

if $INFRA_OK; then
    if curl -sf http://localhost:8083/connectors >/dev/null 2>&1; then
        record_pass "kafka-connect-api"
    else
        record_warn "kafka-connect-api (Connect not reachable — connector JAR may be missing)"
    fi
fi

cd "$ROOT"
echo ""

# ── TIER 3: Event production ──────────────────────────────────────────────────
if $INFRA_OK; then
    echo -e "${BOLD}Tier 3 — Event Production${RESET}"
    echo "  Running producers for 30 seconds ..."
    export SDKROOT

    for producer in producers/user-events producers/order-events producers/inventory-events; do
        (cd "$ROOT/$producer" && CGO_ENABLED=1 SDKROOT=$SDKROOT go run main.go \
            >"$ROOT/logs/$(basename $producer).log" 2>&1) &
        PIDS+=($!)
    done

    sleep 30

    for topic in user-events order-events inventory-events; do
        count=$(docker exec broker kafka-run-class \
            kafka.tools.GetOffsetShell \
            --bootstrap-server broker:29092 \
            --topic "$topic" --time -1 2>/dev/null \
            | awk -F: '{sum+=$3} END {print sum+0}')
        if [[ "$count" -gt 0 ]]; then
            record_pass "events-in-$topic ($count messages)"
        else
            record_fail "events-in-$topic (0 messages — check logs/$(echo $topic | tr - _).log)"
        fi
    done

    # Kill producers before starting downstream services
    for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
    PIDS=()
    echo ""
else
    record_warn "Tier 3 skipped (infrastructure not healthy)"
fi

# ── TIER 4: Stream processor ──────────────────────────────────────────────────
if $INFRA_OK; then
    echo -e "${BOLD}Tier 4 — Stream Processor${RESET}"
    mkdir -p "$ROOT/logs"
    (cd "$ROOT/processor/stream-processor" && \
        CGO_ENABLED=1 SDKROOT=$SDKROOT go run main.go \
        >"$ROOT/logs/stream-processor.log" 2>&1) &
    PIDS+=($!)
    sleep 8

    if curl -sf http://localhost:8088/events \
            --max-time 3 \
            -H "Accept: text/event-stream" >/dev/null 2>&1; then
        record_pass "stream-processor-sse-endpoint"
    else
        record_fail "stream-processor-sse-endpoint (check logs/stream-processor.log)"
    fi
    echo ""
else
    record_warn "Tier 4 skipped (infrastructure not healthy)"
fi

# ── TIER 5: AI services ────────────────────────────────────────────────────────
echo -e "${BOLD}Tier 5 — AI Services${RESET}"
mkdir -p "$ROOT/logs"

for svc in query-agent anomaly-detector forecast-engine; do
    (cd "$ROOT/ai-layer/$svc" && \
        CGO_ENABLED=1 SDKROOT=$SDKROOT go run main.go \
        >"$ROOT/logs/$svc.log" 2>&1) &
    PIDS+=($!)
done

echo "  Waiting 15s for AI services to initialize ..."
sleep 15

declare -A SERVICE_PORTS=([query-agent]=8085 [anomaly-detector]=8086 [forecast-engine]=8087)
for svc in query-agent anomaly-detector forecast-engine; do
    port=${SERVICE_PORTS[$svc]}
    if curl -sf "http://localhost:$port/health" >/dev/null 2>&1; then
        record_pass "health-$svc (port $port)"
    else
        record_fail "health-$svc (port $port — check logs/$svc.log)"
    fi
done
echo ""

# ── TIER 6: Snowflake connector ────────────────────────────────────────────────
echo -e "${BOLD}Tier 6 — Snowflake Sink Connector${RESET}"

if [[ -z "${SNOWFLAKE_ACCOUNT_URL:-}" || -z "${SNOWFLAKE_PRIVATE_KEY:-}" ]]; then
    record_warn "connector-registered (set SNOWFLAKE_ACCOUNT_URL + SNOWFLAKE_PRIVATE_KEY to enable)"
    record_warn "connector-running    (skipped)"
elif ! $INFRA_OK; then
    record_warn "connector-registered (infra not healthy)"
    record_warn "connector-running    (skipped)"
else
    if bash "$ROOT/infra/connectors/register.sh" >"$ROOT/logs/connector.log" 2>&1; then
        record_pass "connector-registered"
        sleep 5
        state=$(curl -sf http://localhost:8083/connectors/snowflake-sink/status 2>/dev/null \
            | python3 -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])" \
            2>/dev/null || echo "UNKNOWN")
        if [[ "$state" == "RUNNING" ]]; then
            record_pass "connector-running (state=$state)"
        else
            record_fail "connector-running (state=$state — check logs/connector.log)"
        fi
    else
        record_fail "connector-registered (check logs/connector.log)"
    fi
fi
echo ""

# ── TIER 7: AI query round-trip ────────────────────────────────────────────────
echo -e "${BOLD}Tier 7 — AI Query Round-trip${RESET}"

if [[ -z "${GRADIENT_AI_KEY:-}" ]]; then
    record_warn "query-agent-response (set GRADIENT_AI_KEY to enable)"
else
    response=$(curl -sf -X POST http://localhost:8085/query \
        -H "Content-Type: application/json" \
        -d '{"question":"How many orders today?","session_id":"smoke-test"}' \
        --max-time 30 2>/dev/null || echo "")

    if echo "$response" | python3 -c \
        "import sys,json; d=json.load(sys.stdin); assert d.get('sql') or d.get('explanation')" \
        2>/dev/null; then
        record_pass "query-agent-response (AI response received)"
    else
        record_fail "query-agent-response (no valid response — check logs/query-agent.log)"
    fi
fi
echo ""

# ── Final summary ─────────────────────────────────────────────────────────────
print_summary
