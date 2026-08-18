#!/bin/sh
# Render the one-process self-host configuration. Provider credentials live in
# owner-only files under the disposable state directory, never in config.toml.
set -eu

umask 077

state_dir="${VERGLAS_STATE_DIR:-/var/lib/verglas}"
node_bin="${VERGLAS_CACHE_NODE_BIN:-verglas-cache-node}"
provider="${VERGLAS_PROVIDER:-verglas-cloud}"
cache_dir="$state_dir/cache"
backend_credentials="$state_dir/backend-credentials"
endpoint_credentials="$state_dir/endpoint-credentials"
catalog_credentials="$state_dir/catalog-credentials"
config="$state_dir/config.toml"

mkdir -p "$cache_dir"

: "${VERGLAS_S3_ACCESS_KEY_ID:?VERGLAS_S3_ACCESS_KEY_ID is required}"
: "${VERGLAS_S3_SECRET_ACCESS_KEY:?VERGLAS_S3_SECRET_ACCESS_KEY is required}"
: "${VERGLAS_CATALOG_WAREHOUSE:?VERGLAS_CATALOG_WAREHOUSE is required}"

printf '[default]\naws_access_key_id = %s\naws_secret_access_key = %s\n' \
  "$VERGLAS_S3_ACCESS_KEY_ID" \
  "$VERGLAS_S3_SECRET_ACCESS_KEY" \
  > "$endpoint_credentials"

storage_region="${VERGLAS_STORAGE_REGION:-us-east-1}"
poll_interval="${VERGLAS_CATALOG_POLL_INTERVAL_SECONDS:-30}"
storage_allow_http="${VERGLAS_STORAGE_ALLOW_HTTP:-false}"

case "$provider" in
  verglas-cloud)
    : "${VERGLAS_CATALOG_URI:?VERGLAS_CATALOG_URI is required}"
    : "${VERGLAS_STORAGE_BUCKET:?VERGLAS_STORAGE_BUCKET is required}"
    : "${VERGLAS_STORAGE_ENDPOINT:?VERGLAS_STORAGE_ENDPOINT is required}"
    : "${VERGLAS_STORAGE_ACCESS_KEY_ID:?VERGLAS_STORAGE_ACCESS_KEY_ID is required}"
    : "${VERGLAS_STORAGE_SECRET_ACCESS_KEY:?VERGLAS_STORAGE_SECRET_ACCESS_KEY is required}"
    : "${VERGLAS_CATALOG_TOKEN:?VERGLAS_CATALOG_TOKEN is required}"
    backend_bucket="$VERGLAS_STORAGE_BUCKET"
    backend_globs="[\"$VERGLAS_STORAGE_BUCKET\"]"
    backend_endpoint="$VERGLAS_STORAGE_ENDPOINT"
    catalog_auth="bearer"
    ;;
  cloudflare)
    : "${VERGLAS_CATALOG_URI:?VERGLAS_CATALOG_URI is required}"
    : "${VERGLAS_STORAGE_BUCKET:?VERGLAS_STORAGE_BUCKET is required}"
    : "${VERGLAS_STORAGE_ENDPOINT:?VERGLAS_STORAGE_ENDPOINT is required}"
    : "${VERGLAS_STORAGE_ACCESS_KEY_ID:?VERGLAS_STORAGE_ACCESS_KEY_ID is required}"
    : "${VERGLAS_STORAGE_SECRET_ACCESS_KEY:?VERGLAS_STORAGE_SECRET_ACCESS_KEY is required}"
    : "${VERGLAS_CATALOG_TOKEN:?VERGLAS_CATALOG_TOKEN is required}"
    backend_bucket="$VERGLAS_STORAGE_BUCKET"
    backend_globs="[\"$VERGLAS_STORAGE_BUCKET\"]"
    backend_endpoint="$VERGLAS_STORAGE_ENDPOINT"
    catalog_auth="bearer"
    ;;
  aws-s3-tables)
    : "${VERGLAS_STORAGE_ACCESS_KEY_ID:?VERGLAS_STORAGE_ACCESS_KEY_ID is required}"
    : "${VERGLAS_STORAGE_SECRET_ACCESS_KEY:?VERGLAS_STORAGE_SECRET_ACCESS_KEY is required}"
    backend_bucket=""
    backend_globs='["*--table-s3"]'
    backend_endpoint="${VERGLAS_STORAGE_ENDPOINT:-https://s3.${storage_region}.amazonaws.com}"
    catalog_uri="${VERGLAS_CATALOG_URI:-https://s3tables.${storage_region}.amazonaws.com/iceberg}"
    catalog_auth="sigv4"
    ;;
  *)
    echo "VERGLAS_PROVIDER must be verglas-cloud, cloudflare, or aws-s3-tables" >&2
    exit 64
    ;;
esac

catalog_uri="${catalog_uri:-$VERGLAS_CATALOG_URI}"

printf '[default]\naws_access_key_id = %s\naws_secret_access_key = %s\n' \
  "$VERGLAS_STORAGE_ACCESS_KEY_ID" \
  "$VERGLAS_STORAGE_SECRET_ACCESS_KEY" \
  > "$backend_credentials"

if [ "$catalog_auth" = "bearer" ]; then
  printf '%s\n' "$VERGLAS_CATALOG_TOKEN" > "$catalog_credentials"
else
  # The REST gateway owns SigV4 for S3 Tables. The same AWS credentials also
  # authorize the cache's local S3 relay to the catalog-managed table buckets.
  cp "$backend_credentials" "$catalog_credentials"
fi

{
  printf '%s\n' \
    '[listen]' \
    's3_port = 8333' \
    'admin_port = 8334' \
    '' \
    '[cache]' \
    "dir = \"$cache_dir\"" \
    "capacity_bytes = \"${VERGLAS_CACHE_CAPACITY:-1GB}\"" \
    "dram_bytes = \"${VERGLAS_CACHE_DRAM:-256MB}\"" \
    '' \
    '[auth]' \
    "credentials_file = \"$endpoint_credentials\"" \
    '' \
    '[backend]' \
    'provider = "s3"'
  if [ -n "$backend_bucket" ]; then
    printf 'bucket = "%s"\n' "$backend_bucket"
  fi
  printf '%s\n' \
    "bucket_globs = $backend_globs" \
    "endpoint = \"$backend_endpoint\"" \
    "region = \"$storage_region\"" \
    "allow_http = $storage_allow_http" \
    "credentials_file = \"$backend_credentials\"" \
    '' \
    '[catalog]' \
    "uri = \"$catalog_uri\"" \
    'consistency = "eventual"' \
    '# VERGLAS_CATALOG_POLL_INTERVAL_SECONDS renders poll_interval_seconds.' \
    "poll_interval_secs = $poll_interval" \
    "warehouse = \"$VERGLAS_CATALOG_WAREHOUSE\"" \
    "credentials_file = \"$catalog_credentials\""
  if [ "$catalog_auth" = "sigv4" ]; then
    printf '%s\n' \
      "sigv4_region = \"$storage_region\"" \
      'sigv4_signing_name = "s3tables"'
  fi
} > "$config"

exec "$node_bin" --config "$config"
