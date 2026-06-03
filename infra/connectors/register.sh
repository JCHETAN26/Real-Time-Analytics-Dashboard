#!/usr/bin/env bash
# Register (or update) the Snowflake Sink connector with the local Kafka Connect
# cluster. Reads secrets from the environment, substitutes them into the config
# template, and PUTs the config to the Connect REST API (idempotent).
#
# Required env:
#   SNOWFLAKE_ACCOUNT_URL   e.g. abc12345.us-east-1.snowflakecomputing.com
#   SNOWFLAKE_PRIVATE_KEY   single-line PKCS#8 private key body (no header/footer)
#
# Optional env:
#   CONNECT_URL             default http://localhost:8083
#   CONNECTOR_NAME          default snowflake-sink
#
# Usage:
#   export SNOWFLAKE_ACCOUNT_URL=...
#   export SNOWFLAKE_PRIVATE_KEY="$(grep -v '^-----' infra/snowflake/keys/rsa_key.p8 | tr -d '\n')"
#   bash infra/connectors/register.sh
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_NAME="${CONNECTOR_NAME:-snowflake-sink}"
DIR="$(cd "$(dirname "$0")" && pwd)"
TEMPLATE="$DIR/snowflake-sink.config.template"

: "${SNOWFLAKE_ACCOUNT_URL:?set SNOWFLAKE_ACCOUNT_URL}"
: "${SNOWFLAKE_PRIVATE_KEY:?set SNOWFLAKE_PRIVATE_KEY}"

# Substitute placeholders. Use a pipe-delimited sed so RSA key slashes are safe.
config="$(sed \
  -e "s|\${SNOWFLAKE_ACCOUNT_URL}|${SNOWFLAKE_ACCOUNT_URL}|g" \
  -e "s|\${SNOWFLAKE_PRIVATE_KEY}|${SNOWFLAKE_PRIVATE_KEY}|g" \
  "$TEMPLATE")"

echo "Registering connector '$CONNECTOR_NAME' at $CONNECT_URL ..."

http_code="$(printf '%s' "$config" \
  | curl -s -o /tmp/connect_resp.json -w '%{http_code}' \
      -X PUT \
      -H 'Content-Type: application/json' \
      --data @- \
      "$CONNECT_URL/connectors/$CONNECTOR_NAME/config")"

echo "HTTP $http_code"
cat /tmp/connect_resp.json 2>/dev/null || true
echo

if [[ "$http_code" != "200" && "$http_code" != "201" ]]; then
  echo "Connector registration failed." >&2
  exit 1
fi

echo
echo "Connector status:"
curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" || true
echo
