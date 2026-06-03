#!/usr/bin/env bash
# Wait until the Kafka broker is accepting connections and the Schema Registry
# is healthy. Exits 0 when both are ready, 1 after a timeout.
set -euo pipefail

BROKER="${KAFKA_BROKER:-localhost:9092}"
SCHEMA_REGISTRY="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
KAFKA_UI="${KAFKA_UI_URL:-http://localhost:9000}"
TIMEOUT="${KAFKA_WAIT_TIMEOUT:-120}"

echo "Waiting for Kafka broker at $BROKER (timeout: ${TIMEOUT}s) ..."

elapsed=0
until docker exec broker kafka-topics --bootstrap-server broker:29092 --list >/dev/null 2>&1; do
    if [[ $elapsed -ge $TIMEOUT ]]; then
        echo "❌ Kafka broker did not become ready within ${TIMEOUT}s"
        exit 1
    fi
    sleep 3
    elapsed=$((elapsed + 3))
    printf "."
done
echo ""
echo "✅ Kafka broker ready (${elapsed}s)"

echo "Waiting for Schema Registry at $SCHEMA_REGISTRY ..."
elapsed=0
until curl -sf "$SCHEMA_REGISTRY/subjects" >/dev/null 2>&1; do
    if [[ $elapsed -ge $TIMEOUT ]]; then
        echo "❌ Schema Registry did not become ready within ${TIMEOUT}s"
        exit 1
    fi
    sleep 3
    elapsed=$((elapsed + 3))
    printf "."
done
echo ""
echo "✅ Schema Registry ready (${elapsed}s)"
