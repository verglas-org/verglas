#!/usr/bin/env bash
# Snapshot-driven prefetch recovery live demonstration (issue #51).
#
# Proves that after an Iceberg compaction rewrites a partitioned table's small
# files, a Verglas cache with prefetch ON recovers its hit rate on the FIRST
# post-compaction query wave (the coordinator carried the hot columns across the
# rewrite by partition+column identity), while prefetch OFF cold-misses. Driven
# by the real `[catalog]` REST watcher (#47) + prefetch coordinator (#51), not a
# benchmark shortcut.
#
# Two configurations, same seeded table, a fresh cache dir each:
#   A  prefetch OFF  first post-compaction wave cold-misses (baseline)
#   B  prefetch ON   first post-compaction wave already warm (recovery)
# Optionally repeated for an Avro-format fixture (COMPACTION_FORMAT=both). The
# daemon's /admin/stats counters are the mechanism evidence. Nothing here runs in
# PR CI. See README.md for every input; the hermetic CI version of this number is
# crates/verglas-tables/tests/lifecycle.rs::benchmark_hit_rate_recovery_prefetch_on_vs_off.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
VENV="$HERE/.venv"
DRIVER="$HERE/compaction_demo.py"
REQ="$HERE/requirements.txt"
RESULTS_DIR="$HERE/results"
RESULTS="$RESULTS_DIR/recovery.jsonl"

# ----- pinned Polaris (same digest as benchmarks/warming) ------------------- #
POLARIS_IMAGE="${POLARIS_IMAGE:-apache/polaris@sha256:9738b2052dea20aabf0cd42521424ff963fee41b0ee888fef9f512efb256602a}"

# ----- inputs (env- or flag-overridable) ------------------------------------ #
BUCKET="${COMPACTION_BUCKET:-hyperglas}"
PREFIX="${COMPACTION_PREFIX:-bench/compaction-demo}"
NAMESPACE="${COMPACTION_NAMESPACE:-compaction}"
CATALOG="${COMPACTION_CATALOG:-demo}"
PARTITIONS="${COMPACTION_PARTITIONS:-8}"
FILES_PER_PARTITION="${COMPACTION_FILES_PER_PARTITION:-8}"
ROWS_PER_FILE="${COMPACTION_ROWS_PER_FILE:-2000}"
TARGET_FILE_SIZE="${COMPACTION_TARGET_FILE_SIZE:-16777216}"
WAVES="${COMPACTION_WAVES:-3}"
SETTLE_SECS="${COMPACTION_SETTLE_SECS:-8}"
FORMAT="${COMPACTION_FORMAT:-parquet}"   # parquet | avro | both

# Verglas daemon (host). Dedicated ports; admin binds S3_PORT+1.
VG_PORT="${COMPACTION_VG_PORT:-8655}"
VG_ADMIN_PORT="$((VG_PORT + 1))"
VG_KEY="${COMPACTION_VG_KEY:-demokey}"
VG_SECRET="${COMPACTION_VG_SECRET:-demosecret}"
VG_DRAM="${COMPACTION_VG_DRAM:-1GB}"
VG_DISK="${COMPACTION_VG_DISK:-8GB}"
POLL_SECS="${COMPACTION_POLL_SECS:-2}"
VERGLASD_BIN="${VERGLASD_BIN:-$ROOT/target/release/verglasd}"

# Polaris on the host (published ports so verglasd + the driver reach it).
POLARIS_PORT="${COMPACTION_POLARIS_PORT:-8181}"
POLARIS_CONTAINER="verglas-compaction-polaris"
POLARIS_REALM="POLARIS"
POLARIS_CLIENT_ID="root"
POLARIS_CLIENT_SECRET="s3cr3t"
CATALOG_URI="http://127.0.0.1:${POLARIS_PORT}/api/catalog"

VG_CACHE_ROOT="${COMPACTION_VG_CACHE_ROOT:-/tmp/vg-compaction-cache}"
VG_LOG="$HERE/verglasd.log"
VG_PID=""

VERGLAS_ENDPOINT="http://127.0.0.1:${VG_PORT}"
ADMIN_ENDPOINT="http://127.0.0.1:${VG_ADMIN_PORT}"

# ----- origin credentials (.env at repo root; never logged) ----------------- #
if [[ -f "$ROOT/.env" ]]; then
  set +x
  # shellcheck disable=SC1091
  source "$ROOT/.env"
fi
: "${AWS_ENDPOINT:?set AWS_ENDPOINT (origin) in .env}"
: "${AWS_ACCESS_KEY_ID:?set AWS_ACCESS_KEY_ID in .env}"
: "${AWS_SECRET_ACCESS_KEY:?set AWS_SECRET_ACCESS_KEY in .env}"
AWS_REGION="${AWS_REGION:-us-east-1}"

mkdir -p "$RESULTS_DIR"
: > "$RESULTS"

# ----- python venv ---------------------------------------------------------- #
if [[ ! -x "$VENV/bin/python" ]]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --quiet --upgrade pip
  "$VENV/bin/pip" install --quiet -r "$REQ"
fi
PY="$VENV/bin/python"

# ----- driver invocation (common flags) ------------------------------------- #
drive() {
  local cmd="$1"; shift
  "$PY" "$DRIVER" "$cmd" \
    --catalog-uri "$CATALOG_URI" --catalog "$CATALOG" \
    --namespace "$NAMESPACE" \
    --origin-endpoint "$AWS_ENDPOINT" --origin-region "$AWS_REGION" \
    --verglas-endpoint "$VERGLAS_ENDPOINT" \
    --verglas-key "$VG_KEY" --verglas-secret "$VG_SECRET" \
    --admin-endpoint "$ADMIN_ENDPOINT" \
    --partitions "$PARTITIONS" --files-per-partition "$FILES_PER_PARTITION" \
    --rows-per-file "$ROWS_PER_FILE" --waves "$WAVES" \
    --target-file-size "$TARGET_FILE_SIZE" --settle-secs "$SETTLE_SECS" \
    --results "$RESULTS" "$@"
}

# ----- Polaris lifecycle ---------------------------------------------------- #
start_polaris() {
  docker rm -f "$POLARIS_CONTAINER" >/dev/null 2>&1 || true
  docker run -d --name "$POLARIS_CONTAINER" \
    -p "${POLARIS_PORT}:8181" \
    -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    -e AWS_REGION="$AWS_REGION" \
    -e AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED \
    -e AWS_RESPONSE_CHECKSUM_VALIDATION=WHEN_REQUIRED \
    -e POLARIS_BOOTSTRAP_CREDENTIALS="${POLARIS_REALM},${POLARIS_CLIENT_ID},${POLARIS_CLIENT_SECRET}" \
    -e polaris.realm-context.realms="$POLARIS_REALM" \
    -e quarkus.otel.sdk.disabled=true \
    -e polaris.readiness.ignore-severe-issues=true \
    -e 'polaris.features."ALLOW_INSECURE_STORAGE_TYPES"=true' \
    -e 'polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"=["FILE","S3"]' \
    "$POLARIS_IMAGE" >/dev/null
  for _ in $(seq 1 100); do
    curl -sf "http://127.0.0.1:${POLARIS_PORT}/q/health" >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  echo "error: Polaris did not become ready" >&2; exit 1
}
stop_polaris() { docker rm -f "$POLARIS_CONTAINER" >/dev/null 2>&1 || true; }

# ----- verglasd lifecycle --------------------------------------------------- #
write_verglasd_config() {
  local prefetch="$1" cache_dir="$2" token="$3" cfg="$HERE/verglasd.toml"
  {
    echo "[listen]"
    echo "s3_port = $VG_PORT"
    echo "admin_port = $VG_ADMIN_PORT"
    echo "[cache]"
    echo "dir = \"$cache_dir\""
    echo "capacity_bytes = \"$VG_DISK\""
    echo "dram_bytes = \"$VG_DRAM\""
    echo "[cache.prefetch]"
    echo "enabled = $prefetch"
    echo "[auth]"
    echo "access_key_id = \"$VG_KEY\""
    echo "secret_access_key = \"$VG_SECRET\""
    echo "[catalog]"
    echo "uri = \"$CATALOG_URI\""
    echo "poll_interval_secs = $POLL_SECS"
    echo "warehouse = \"$CATALOG\""
    echo "bearer_token = \"$token\""
  } > "$cfg"
  echo "$cfg"
}

start_verglasd() {
  local prefetch="$1" cache_dir="$2" token="$3"
  [[ -x "$VERGLASD_BIN" ]] || { echo "error: build verglasd first (cargo build --release -p verglasd)" >&2; exit 2; }
  stop_verglasd
  rm -rf "$cache_dir"; mkdir -p "$cache_dir"
  local cfg; cfg="$(write_verglasd_config "$prefetch" "$cache_dir" "$token")"
  : > "$VG_LOG"
  "$VERGLASD_BIN" --config "$cfg" >"$VG_LOG" 2>&1 &
  VG_PID=$!
  for _ in $(seq 1 100); do
    curl -sf "${ADMIN_ENDPOINT}/admin/healthz" >/dev/null 2>&1 && { echo "[verglasd] ready (prefetch=$prefetch)" >&2; return 0; }
    sleep 0.2
  done
  echo "error: verglasd did not become ready; see $VG_LOG" >&2; exit 1
}
stop_verglasd() { [[ -n "$VG_PID" ]] && kill "$VG_PID" 2>/dev/null || true; VG_PID=""; }

cleanup() { stop_verglasd; stop_polaris; }
trap cleanup EXIT

# ----- run ------------------------------------------------------------------ #
polaris_token() {
  curl -sf -X POST "${CATALOG_URI}/v1/oauth/tokens" \
    -d grant_type=client_credentials \
    -d "client_id=${POLARIS_CLIENT_ID}" \
    -d "client_secret=${POLARIS_CLIENT_SECRET}" \
    -d "scope=PRINCIPAL_ROLE:ALL" | "$PY" -c 'import sys,json;print(json.load(sys.stdin)["access_token"])'
}

run_format() {
  local fmt="$1"
  echo "== format: $fmt ==" >&2
  # Config A: prefetch OFF. Seed + steady load + compact + measure the first wave.
  start_polaris
  local token; token="$(polaris_token)"
  drive bootstrap --format "$fmt" >/dev/null || true
  start_verglasd false "$VG_CACHE_ROOT/off-$fmt" "$token"
  drive seed --format "$fmt"
  drive load --format "$fmt"
  drive compact --format "$fmt"
  drive measure --format "$fmt" --config "A-off"
  stop_verglasd

  # Config B: prefetch ON, fresh cache + fresh table (same shape).
  token="$(polaris_token)"
  start_verglasd true "$VG_CACHE_ROOT/on-$fmt" "$token"
  drive seed --format "$fmt"
  drive load --format "$fmt"
  drive compact --format "$fmt"
  drive measure --format "$fmt" --config "B-on"
  stop_verglasd
  stop_polaris
}

case "$FORMAT" in
  both) run_format parquet; run_format avro ;;
  *)    run_format "$FORMAT" ;;
esac

drive report >/dev/null || true
"$PY" "$DRIVER" report --catalog-uri "$CATALOG_URI" --catalog "$CATALOG" \
  --namespace "$NAMESPACE" --origin-endpoint "$AWS_ENDPOINT" --results "$RESULTS"
