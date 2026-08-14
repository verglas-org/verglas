#!/bin/sh
set -eu

umask 077
mkdir -p /var/lib/verglas/cache
printf '[default]\naws_access_key_id = %s\naws_secret_access_key = %s\n' \
  "$VERGLAS_STORAGE_ACCESS_KEY_ID" \
  "$VERGLAS_STORAGE_SECRET_ACCESS_KEY" \
  > /var/lib/verglas/backend-credentials
printf '[default]\naws_access_key_id = %s\naws_secret_access_key = %s\n' \
  "$VERGLAS_S3_ACCESS_KEY_ID" \
  "$VERGLAS_S3_SECRET_ACCESS_KEY" \
  > /var/lib/verglas/endpoint-credentials
printf '%s\n' \
  '[listen]' \
  's3_port = 8333' \
  'admin_port = 8334' \
  '' \
  '[cache]' \
  'dir = "/var/lib/verglas/cache"' \
  "capacity_bytes = \"${VERGLAS_CACHE_CAPACITY:-1GB}\"" \
  "dram_bytes = \"${VERGLAS_CACHE_DRAM:-256MB}\"" \
  '' \
  '[auth]' \
  'credentials_file = "/var/lib/verglas/endpoint-credentials"' \
  '' \
  '[backend]' \
  'provider = "s3"' \
  "bucket = \"$VERGLAS_STORAGE_BUCKET\"" \
  "bucket_globs = [\"${VERGLAS_CATALOG_ARCHIVE_BUCKET:-$VERGLAS_STORAGE_BUCKET}\"]" \
  "endpoint = \"$VERGLAS_STORAGE_ENDPOINT\"" \
  "region = \"$VERGLAS_STORAGE_REGION\"" \
  "allow_http = ${VERGLAS_STORAGE_ALLOW_HTTP:-false}" \
  'credentials_file = "/var/lib/verglas/backend-credentials"' \
  > /var/lib/verglas/config.toml

if [ -n "${VERGLAS_SAFEKEEPER_ADDR:-}" ]; then
  : "${VERGLAS_CATALOG_ARCHIVE_BUCKET:?VERGLAS_CATALOG_ARCHIVE_BUCKET is required with VERGLAS_SAFEKEEPER_ADDR}"
  printf '%s\n' \
    '' \
    '[catalog_archive]' \
    "bucket = \"$VERGLAS_CATALOG_ARCHIVE_BUCKET\"" \
    "prefix = \"${VERGLAS_CATALOG_ARCHIVE_PREFIX:-_verglas/catalog}\"" \
    >> /var/lib/verglas/config.toml
fi

if [ -n "${VERGLAS_CATALOG_URI:-}" ]; then
  printf '%s\n' \
    '' \
    '[catalog]' \
    "uri = \"$VERGLAS_CATALOG_URI\"" \
    >> /var/lib/verglas/config.toml
  if [ -n "${VERGLAS_CATALOG_WAREHOUSE:-}" ]; then
    printf 'warehouse = "%s"\n' "$VERGLAS_CATALOG_WAREHOUSE" \
      >> /var/lib/verglas/config.toml
  fi
fi

exec verglas-cache-node --config /var/lib/verglas/config.toml
