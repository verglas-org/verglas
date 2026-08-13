#!/bin/sh
set -eu

umask 077
mkdir -p /var/lib/verglas-ec-keeper/cache
printf '[default]\naws_access_key_id = %s\naws_secret_access_key = %s\n' \
  "$VERGLAS_MANAGED_STORAGE_ACCESS_KEY_ID" \
  "$VERGLAS_MANAGED_STORAGE_SECRET_ACCESS_KEY" \
  > /var/lib/verglas-ec-keeper/backend-credentials
printf '%s\n' \
  '[listen]' \
  's3_port = 8333' \
  'admin_port = 8334' \
  '' \
  '[cache]' \
  'dir = "/var/lib/verglas-ec-keeper/cache"' \
  'capacity_bytes = "1GB"' \
  'dram_bytes = "1MB"' \
  '' \
  '[backend]' \
  'provider = "s3"' \
  "bucket = \"$VERGLAS_MANAGED_STORAGE_BUCKET\"" \
  "endpoint = \"$VERGLAS_MANAGED_STORAGE_ENDPOINT\"" \
  "region = \"$VERGLAS_MANAGED_STORAGE_REGION\"" \
  "allow_http = ${VERGLAS_MANAGED_STORAGE_ALLOW_HTTP:-false}" \
  'credentials_file = "/var/lib/verglas-ec-keeper/backend-credentials"' \
  > /var/lib/verglas-ec-keeper/config.toml

exec verglas-ec-keeper --config /var/lib/verglas-ec-keeper/config.toml
