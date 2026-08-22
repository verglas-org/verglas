#!/bin/sh
# Render the one-process self-host configuration: a Verglas cache node that,
# when VERGLAS_CATALOG=on, also serves its own Iceberg REST catalog.
#
# There is no external catalog to track — this process *is* the catalog — so
# nothing here configures a catalog client, a provider, or a poll interval.
# VERGLAS_CATALOG=off (the default) renders a cache-only node: no catalog
# sections, no catalog consensus group, and no authz identity required.
# Credentials live in owner-only files under the state directory, never in
# config.toml.
set -eu

umask 077

state_dir="${VERGLAS_STATE_DIR:-/var/lib/verglas}"
node_bin="${VERGLAS_CACHE_NODE_BIN:-verglas-cache-node}"
cache_dir="${VERGLAS_CACHE_DIR:-$state_dir/cache}"
backend_credentials="$state_dir/backend-credentials"
endpoint_credentials="$state_dir/endpoint-credentials"
config="$state_dir/config.toml"

mkdir -p "$cache_dir"

# The origin this node caches and offloads to.
: "${VERGLAS_STORAGE_BUCKET:?VERGLAS_STORAGE_BUCKET is required}"
: "${VERGLAS_STORAGE_ENDPOINT:?VERGLAS_STORAGE_ENDPOINT is required}"
: "${VERGLAS_STORAGE_ACCESS_KEY_ID:?VERGLAS_STORAGE_ACCESS_KEY_ID is required}"
: "${VERGLAS_STORAGE_SECRET_ACCESS_KEY:?VERGLAS_STORAGE_SECRET_ACCESS_KEY is required}"
# What query engines present to this node's S3 endpoint.
: "${VERGLAS_S3_ACCESS_KEY_ID:?VERGLAS_S3_ACCESS_KEY_ID is required}"
: "${VERGLAS_S3_SECRET_ACCESS_KEY:?VERGLAS_S3_SECRET_ACCESS_KEY is required}"
# Whether this node serves the hosted Iceberg catalog. Off by default: a
# cache-only deployment needs no catalog, no catalog consensus group, and no
# authz identity. The node parses the same contract from this variable.
catalog_mode="${VERGLAS_CATALOG:-off}"
case "$catalog_mode" in
  off | on) ;;
  *)
    printf 'VERGLAS_CATALOG must be "off" or "on", got "%s"\n' "$catalog_mode" >&2
    exit 1
    ;;
esac

# What callers present to the catalog. The catalog verifies external bearer
# tokens against these; it never mints its own. Required only when it runs.
if [ "$catalog_mode" = "on" ]; then
  : "${VERGLAS_CATALOG_AUTHZ_ISSUER:?VERGLAS_CATALOG_AUTHZ_ISSUER is required when VERGLAS_CATALOG=on}"
  : "${VERGLAS_CATALOG_AUTHZ_JWKS:?VERGLAS_CATALOG_AUTHZ_JWKS is required when VERGLAS_CATALOG=on}"
fi

s3_port="${VERGLAS_S3_PORT:-8333}"
admin_port="${VERGLAS_ADMIN_PORT:-8334}"
catalog_port="${VERGLAS_CATALOG_PORT:-8181}"
storage_region="${VERGLAS_STORAGE_REGION:-us-east-1}"
storage_allow_http="${VERGLAS_STORAGE_ALLOW_HTTP:-false}"
tenant="${VERGLAS_CATALOG_TENANT:-local}"
warehouse="${VERGLAS_CATALOG_WAREHOUSE:-warehouse}"
authz_tenant="${VERGLAS_CATALOG_AUTHZ_TENANT_ID:-$tenant}"

printf '[default]\naws_access_key_id = %s\naws_secret_access_key = %s\n' \
  "$VERGLAS_S3_ACCESS_KEY_ID" "$VERGLAS_S3_SECRET_ACCESS_KEY" > "$endpoint_credentials"
printf '[default]\naws_access_key_id = %s\naws_secret_access_key = %s\n' \
  "$VERGLAS_STORAGE_ACCESS_KEY_ID" "$VERGLAS_STORAGE_SECRET_ACCESS_KEY" > "$backend_credentials"

# Table metadata is written through this node's own S3 endpoint, so catalog
# writes take the same cached, offloading path as table data instead of
# bypassing it to the origin.
managed_profile=$(printf '{"bucket":"%s","region":"%s","endpoint":"http://127.0.0.1:%s","path-style-access":true,"sts-enabled":false}' \
  "$VERGLAS_STORAGE_BUCKET" "$storage_region" "$s3_port")

# TOML basic strings: escape backslashes first, then quotes, so an embedded
# JWKS or profile document cannot terminate the string early.
toml_escape() {
  printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'
}

{
  printf '%s\n' \
    '[listen]' \
    "s3_port = $s3_port" \
    "admin_port = $admin_port" \
    '' \
    '[cache]' \
    "dir = \"$(toml_escape "$cache_dir")\"" \
    "capacity_bytes = \"${VERGLAS_CACHE_CAPACITY:-20GB}\"" \
    "dram_bytes = \"${VERGLAS_CACHE_DRAM:-1GB}\"" \
    '' \
    '[auth]' \
    "credentials_file = \"$(toml_escape "$endpoint_credentials")\"" \
    '' \
    '[backend]' \
    'provider = "s3"' \
    "bucket = \"$(toml_escape "$VERGLAS_STORAGE_BUCKET")\"" \
    "bucket_globs = [\"$(toml_escape "$VERGLAS_STORAGE_BUCKET")\"]" \
    "endpoint = \"$(toml_escape "$VERGLAS_STORAGE_ENDPOINT")\"" \
    "region = \"$storage_region\"" \
    "allow_http = $storage_allow_http" \
    "credentials_file = \"$(toml_escape "$backend_credentials")\""

  # The catalog sections exist only when the catalog runs. A cache-only node
  # renders neither, so nothing requires a catalog archive or an authz
  # identity it will never use.
  if [ "$catalog_mode" = "on" ]; then
    printf '%s\n' \
      '' \
      '# Consensus-committed catalog checkpoints, namespaced away from table' \
      '# data in the same bucket.' \
      '[catalog_archive]' \
      "bucket = \"$(toml_escape "$VERGLAS_STORAGE_BUCKET")\"" \
      "prefix = \"${VERGLAS_CATALOG_ARCHIVE_PREFIX:-_verglas/catalog}\"" \
      '' \
      '# The Iceberg REST catalog this node serves itself.' \
      '[catalog_server]' \
      "port = $catalog_port" \
      "tenant = \"$(toml_escape "$tenant")\"" \
      "warehouse = \"$(toml_escape "$warehouse")\"" \
      "managed_s3_profile = \"$(toml_escape "$managed_profile")\"" \
      "authz_issuer = \"$(toml_escape "$VERGLAS_CATALOG_AUTHZ_ISSUER")\"" \
      "authz_jwks = \"$(toml_escape "$VERGLAS_CATALOG_AUTHZ_JWKS")\"" \
      "authz_tenant_id = \"$(toml_escape "$authz_tenant")\""
  fi
} > "$config"

# The catalog's authoritative state lives in consensus. A single node runs one
# voter, which serves catalog commits because they ride inline inside the Raft
# entry (<= 4 KiB) and never construct the coded payload store. `EC_M=0` is not
# a valid coded geometry, so a body over that threshold is refused rather than
# stored without redundancy. Objects on one node pass through to the origin.
# A multi-node deployment overrides these.
# Peer RPC is authenticated even when the only peer is this process. A
# single node generates its own secret once and keeps it with its state; a
# multi-node deployment must supply the same value to every node.
if [ -z "${VERGLAS_CLUSTER_SECRET:-}" ]; then
  secret_file="$state_dir/cluster-secret"
  if [ ! -s "$secret_file" ]; then
    (od -An -tx1 -N32 /dev/urandom | tr -d ' \n'; printf '\n') > "$secret_file"
  fi
  VERGLAS_CLUSTER_SECRET=$(cat "$secret_file")
fi
export VERGLAS_CLUSTER_SECRET
export VERGLAS_NODE_ID="${VERGLAS_NODE_ID:-verglas-1}"
export VERGLAS_RING_PEERS="${VERGLAS_RING_PEERS:-$VERGLAS_NODE_ID=127.0.0.1:${VERGLAS_RING_PORT:-8337}}"
export VERGLAS_RING_ADDR="${VERGLAS_RING_ADDR:-0.0.0.0:${VERGLAS_RING_PORT:-8337}}"
export VERGLAS_SAFEKEEPER_EC_K="${VERGLAS_SAFEKEEPER_EC_K:-1}"
export VERGLAS_SAFEKEEPER_EC_M="${VERGLAS_SAFEKEEPER_EC_M:-0}"
export VERGLAS_SAFEKEEPER_EC_W="${VERGLAS_SAFEKEEPER_EC_W:-1}"

# The catalog writes table metadata through this node's own S3 endpoint, so it
# authenticates as an engine would. Secrets stay out of config.toml; the node
# reads this identity from the environment.
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-$VERGLAS_S3_ACCESS_KEY_ID}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-$VERGLAS_S3_SECRET_ACCESS_KEY}"

exec "$node_bin" --config "$config"
