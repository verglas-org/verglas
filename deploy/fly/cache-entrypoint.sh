#!/bin/sh
set -eu

# Fly mounts an already formatted persistent volume at this path. This image
# deliberately does no guest block-device discovery, formatting, or setup.
DATA_DIR=${VERGLAS_CACHE_DIR:-/data/cache}
RUNTIME_DIR=${VERGLAS_RUNTIME_DIR:-/run/verglas}
CONFIG=${RUNTIME_DIR}/cache.toml
BACKEND_CREDS=${RUNTIME_DIR}/backend-credentials
ENDPOINT_CREDS=${RUNTIME_DIR}/endpoint-credentials
CATALOG_CREDS=${RUNTIME_DIR}/catalog-credentials

: "${VERGLAS_MANAGED_STORAGE_BUCKET:?required}"
: "${VERGLAS_MANAGED_STORAGE_ENDPOINT:?required}"
: "${VERGLAS_MANAGED_STORAGE_REGION:?required}"
: "${VERGLAS_MANAGED_STORAGE_ACCESS_KEY_ID:?required}"
: "${VERGLAS_MANAGED_STORAGE_SECRET_ACCESS_KEY:?required}"
: "${VERGLAS_S3_ACCESS_KEY_ID:?required}"
: "${VERGLAS_S3_SECRET_ACCESS_KEY:?required}"

umask 077
mkdir -p "$DATA_DIR" "$RUNTIME_DIR"
chown verglas:verglas "$DATA_DIR" "$RUNTIME_DIR"

printf '[default]\naws_access_key_id = %s\naws_secret_access_key = %s\n' \
  "$VERGLAS_MANAGED_STORAGE_ACCESS_KEY_ID" \
  "$VERGLAS_MANAGED_STORAGE_SECRET_ACCESS_KEY" > "$BACKEND_CREDS"
printf '[default]\naws_access_key_id = %s\naws_secret_access_key = %s\n' \
  "$VERGLAS_S3_ACCESS_KEY_ID" \
  "$VERGLAS_S3_SECRET_ACCESS_KEY" > "$ENDPOINT_CREDS"

# Leave ten percent of the attached NVMe volume for filesystem metadata,
# journals, and runtime bookkeeping unless the control plane assigns an exact
# cache budget.
if [ -n "${VERGLAS_CACHE_CAPACITY:-}" ]; then
  CACHE_CAPACITY=$VERGLAS_CACHE_CAPACITY
else
  AVAILABLE_KIB=$(df -Pk "$DATA_DIR" | awk 'NR == 2 { print $4 }')
  CACHE_CAPACITY=$((AVAILABLE_KIB * 1024 * 9 / 10))
fi

# The control plane normally supplies the paid cache-RAM allocation. The
# fallback reserves half of guest RAM for the process, networking, and EC
# bookkeeping rather than pretending the host's memory is available.
if [ -n "${VERGLAS_CACHE_DRAM:-}" ]; then
  CACHE_DRAM=$VERGLAS_CACHE_DRAM
else
  MEMORY_KIB=$(awk '/^MemTotal:/ { print $2 }' /proc/meminfo)
  CACHE_DRAM=$((MEMORY_KIB * 1024 / 2))
fi

cat > "$CONFIG" <<EOF
[listen]
s3_port = ${VERGLAS_S3_PORT:-8333}
admin_port = ${VERGLAS_ADMIN_PORT:-8334}

[log]
format = "${VERGLAS_LOG_FORMAT:-json}"
level = "${VERGLAS_LOG_LEVEL:-info}"

[cache]
dir = "$DATA_DIR"
capacity_bytes = "$CACHE_CAPACITY"
dram_bytes = "$CACHE_DRAM"

[auth]
credentials_file = "$ENDPOINT_CREDS"

[backend]
provider = "s3"
bucket = "$VERGLAS_MANAGED_STORAGE_BUCKET"
endpoint = "$VERGLAS_MANAGED_STORAGE_ENDPOINT"
region = "$VERGLAS_MANAGED_STORAGE_REGION"
allow_http = ${VERGLAS_MANAGED_STORAGE_ALLOW_HTTP:-false}
credentials_file = "$BACKEND_CREDS"
EOF

if [ -n "${VERGLAS_MANAGED_CATALOG_URI:-}" ]; then
  {
    printf '\n[catalog]\n'
    printf 'uri = "%s"\n' "$VERGLAS_MANAGED_CATALOG_URI"
    printf 'consistency = "%s"\n' "${VERGLAS_CATALOG_CONSISTENCY:-strong}"
    if [ -n "${VERGLAS_MANAGED_CATALOG_TOKEN:-}" ]; then
      printf '%s\n' "$VERGLAS_MANAGED_CATALOG_TOKEN" > "$CATALOG_CREDS"
      printf 'credentials_file = "%s"\n' "$CATALOG_CREDS"
    fi
    if [ -n "${VERGLAS_MANAGED_CATALOG_WAREHOUSE:-}" ]; then
      printf 'warehouse = "%s"\n' "$VERGLAS_MANAGED_CATALOG_WAREHOUSE"
    fi
  } >> "$CONFIG"
fi

chown verglas:verglas "$CONFIG" "$BACKEND_CREDS" "$ENDPOINT_CREDS"
if [ -f "$CATALOG_CREDS" ]; then
  chown verglas:verglas "$CATALOG_CREDS"
fi

exec setpriv --reuid=verglas --regid=verglas --init-groups \
  verglas-cache-node --config "$CONFIG"
