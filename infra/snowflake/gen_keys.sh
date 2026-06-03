#!/usr/bin/env bash
# Generate an RSA key pair for the Snowflake Kafka Sink connector (key-pair auth).
#
# Produces:
#   rsa_key.p8   — PKCS#8 private key (unencrypted) used by Kafka Connect
#   rsa_key.pub  — public key to register on the Snowflake user
#
# Usage:
#   bash infra/snowflake/gen_keys.sh
#   # then paste rsa_key.pub into the ALTER USER ... SET RSA_PUBLIC_KEY statement
set -euo pipefail

OUT_DIR="$(cd "$(dirname "$0")" && pwd)/keys"
mkdir -p "$OUT_DIR"

echo "Generating unencrypted PKCS#8 private key..."
openssl genrsa 2048 \
  | openssl pkcs8 -topk8 -inform PEM -out "$OUT_DIR/rsa_key.p8" -nocrypt

echo "Deriving public key..."
openssl rsa -in "$OUT_DIR/rsa_key.p8" -pubout -out "$OUT_DIR/rsa_key.pub"

echo
echo "Keys written to $OUT_DIR"
echo "Private key (for Kafka Connect): $OUT_DIR/rsa_key.p8"
echo "Public key  (for Snowflake):     $OUT_DIR/rsa_key.pub"
echo
echo "Single-line public key for ALTER USER ... SET RSA_PUBLIC_KEY:"
grep -v '^-----' "$OUT_DIR/rsa_key.pub" | tr -d '\n'
echo
