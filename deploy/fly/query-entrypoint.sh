#!/bin/sh
set -eu

: "${VERGLAS_CACHE_S3_ENDPOINT:?required}"
: "${VERGLAS_CACHE_METADATA_URI:?required}"

RUNTIME_DIR=${VERGLAS_RUNTIME_DIR:-/run/verglas}
SPILL_DIR=${VERGLAS_QUERY_SPILL_DIR:-/tmp/verglas-query-spill}
CONFIG=${RUNTIME_DIR}/query.toml
ENDPOINT_CREDS=${RUNTIME_DIR}/endpoint-credentials

umask 077
mkdir -p "$RUNTIME_DIR" "$SPILL_DIR"

if [ -n "${VERGLAS_QUERY_MEMORY_LIMIT_BYTES:-}" ]; then
  MEMORY_LIMIT=$VERGLAS_QUERY_MEMORY_LIMIT_BYTES
else
  MEMORY_KIB=$(awk '/^MemTotal:/ { print $2 }' /proc/meminfo)
  MEMORY_LIMIT=$((MEMORY_KIB * 1024 * 3 / 4))
fi

CACHE_CREDENTIALS_LINE=
if [ -n "${VERGLAS_S3_ACCESS_KEY_ID:-}" ] || [ -n "${VERGLAS_S3_SECRET_ACCESS_KEY:-}" ]; then
  : "${VERGLAS_S3_ACCESS_KEY_ID:?required with VERGLAS_S3_SECRET_ACCESS_KEY}"
  : "${VERGLAS_S3_SECRET_ACCESS_KEY:?required with VERGLAS_S3_ACCESS_KEY_ID}"
  printf '[default]\naws_access_key_id = %s\naws_secret_access_key = %s\n' \
    "$VERGLAS_S3_ACCESS_KEY_ID" "$VERGLAS_S3_SECRET_ACCESS_KEY" > "$ENDPOINT_CREDS"
  CACHE_CREDENTIALS_LINE="credentials_file = \"$ENDPOINT_CREDS\""
fi

cat > "$CONFIG" <<EOF
[listen]
admin_port = ${VERGLAS_QUERY_PORT:-8335}

[log]
format = "${VERGLAS_LOG_FORMAT:-json}"
level = "${VERGLAS_LOG_LEVEL:-info}"

[memory]
estimate_on_request = false
limit_bytes = $MEMORY_LIMIT
spill_path = "$SPILL_DIR"

[cache]
s3_endpoint = "$VERGLAS_CACHE_S3_ENDPOINT"
region = "${VERGLAS_CACHE_REGION:-us-east-1}"
$CACHE_CREDENTIALS_LINE

[metadata]
uri = "$VERGLAS_CACHE_METADATA_URI"
EOF

exec verglas-query --config "$CONFIG"
