#!/usr/bin/env bash
# cache-node-smoke.sh — end-to-end smoke test + footprint measurement for
# bins/cache-node (verglas-cache-node).
#
# Stands up a local MinIO as the origin, points a verglas-cache-node at it, drives
# a few hundred write/read operations through the cache's SigV4 S3 surface with
# the AWS CLI, verifies bytes round-trip through the cache (read-through fill on
# miss, write-through to origin), and records the server's resident memory (RSS)
# while serving and prints its stripped binary size.
#
# Runs anywhere with bash + minio + mc + aws + a release build. RSS is read via
# `ps -o rss=` (KB), which works on both Linux and macOS; the fleet's real number
# is the one measured in a linux/amd64 container (see the report). Nothing here
# needs KVM or docker.
#
# Usage:
#   scripts/cache-node-smoke.sh [--bin <verglas-cache-node>] [--ops N]
#
# Env knobs (all optional): OPS (default 300), S3_PORT (18333), ADMIN_PORT
# (18334), MINIO_PORT (19000).
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "${here}/.." && pwd)"

BIN="${repo}/target/release/verglas-cache-node"
OPS="${OPS:-300}"
S3_PORT="${S3_PORT:-18333}"
ADMIN_PORT="${ADMIN_PORT:-18334}"
MINIO_PORT="${MINIO_PORT:-19000}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bin) BIN="$2"; shift 2 ;;
    --ops) OPS="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

for tool in minio mc aws; do
  command -v "$tool" >/dev/null 2>&1 || { echo "error: $tool not found on PATH" >&2; exit 2; }
done
[[ -x "${BIN}" ]] || { echo "error: cache-node binary not found/executable: ${BIN} (build with: cargo build -p verglas-cache-node --release)" >&2; exit 2; }

work="$(mktemp -d)"
cache_dir="${work}/cache"
minio_data="${work}/minio"
mkdir -p "${cache_dir}" "${minio_data}"

# Fixed dev credentials for the whole test. The origin (MinIO) and the cache
# endpoint each get their own keypair, exactly as the fleet renders them.
export MINIO_ROOT_USER="minioadmin"
export MINIO_ROOT_PASSWORD="minioadmin"
CACHE_KEY="VGSMOKEKEY000000"
CACHE_SECRET="vgsmokesecret000000000000000000"
BUCKET="verglas-fleet-cache"

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do kill "${pid}" >/dev/null 2>&1 || true; done
  rm -rf "${work}"
}
trap cleanup EXIT

echo ">> starting MinIO origin on :${MINIO_PORT}"
minio server --quiet --address ":${MINIO_PORT}" "${minio_data}" >"${work}/minio.log" 2>&1 &
pids+=("$!")

# Wait for MinIO, then create the origin bucket.
for _ in $(seq 1 50); do
  curl -fsS "http://127.0.0.1:${MINIO_PORT}/minio/health/ready" >/dev/null 2>&1 && break
  sleep 0.2
done
mc alias set vgsmoke "http://127.0.0.1:${MINIO_PORT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null
mc mb --ignore-existing "vgsmoke/${BUCKET}" >/dev/null

# Origin credentials file (AWS-INI, 0600) and the cache endpoint auth file — the
# same two files cache-boot.sh writes.
umask 077
cat > "${work}/backend-credentials" <<EOF
[default]
aws_access_key_id = ${MINIO_ROOT_USER}
aws_secret_access_key = ${MINIO_ROOT_PASSWORD}
EOF
cat > "${work}/auth-credentials" <<EOF
[default]
aws_access_key_id = ${CACHE_KEY}
aws_secret_access_key = ${CACHE_SECRET}
EOF

# Render exactly what fleet/images/boot/cache-boot.sh renders: the five sections
# [listen] [log] [cache] [backend] [auth], with an http origin (allow_http for
# the local MinIO).
cat > "${work}/config.toml" <<EOF
[listen]
s3_port = ${S3_PORT}
admin_port = ${ADMIN_PORT}

[log]
format = "json"
level = "info"

[cache]
dir = "${cache_dir}"
capacity_bytes = "256MB"
dram_bytes = "128MB"

[backend]
bucket = "${BUCKET}"
endpoint = "http://127.0.0.1:${MINIO_PORT}"
region = "us-east-1"
allow_http = true
credentials_file = "${work}/backend-credentials"

[auth]
credentials_file = "${work}/auth-credentials"
EOF

echo ">> starting verglas-cache-node (s3=${S3_PORT} admin=${ADMIN_PORT})"
"${BIN}" --config "${work}/config.toml" >"${work}/cache-node.log" 2>&1 &
cache_pid="$!"
pids+=("${cache_pid}")

# Wait for readiness: /admin/healthz flips from starting/503 to ok/200 once the
# engine has recovered (serve-gating #16).
ready=""
for _ in $(seq 1 100); do
  code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${ADMIN_PORT}/admin/healthz" || true)"
  if [[ "${code}" == "200" ]]; then ready="yes"; break; fi
  sleep 0.1
done
[[ -n "${ready}" ]] || { echo "error: cache node never became ready" >&2; cat "${work}/cache-node.log" >&2; exit 1; }
echo ">> cache node ready (GET /admin/healthz = 200)"

# Point the AWS CLI at the cache endpoint with the cache's own keypair. Path-style
# addressing (the fleet renders no [listen].domain).
export AWS_ACCESS_KEY_ID="${CACHE_KEY}"
export AWS_SECRET_ACCESS_KEY="${CACHE_SECRET}"
export AWS_DEFAULT_REGION="us-east-1"
export AWS_EC2_METADATA_DISABLED="true"
# AWS CLI v2 defaults to sending `x-amz-checksum-mode: ENABLED` on GET, which the
# cache models as a straight-to-origin direct read (a version/part/checksum view
# the block cache does not model) — bytes are correct but nothing is cached.
# `when_required` drops the default checksum handshake so GETs take the cacheable
# read-through path and actually exercise the tiers.
export AWS_REQUEST_CHECKSUM_CALCULATION="when_required"
export AWS_RESPONSE_CHECKSUM_VALIDATION="when_required"
ENDPOINT="http://127.0.0.1:${S3_PORT}"
s3() { aws --endpoint-url "${ENDPOINT}" s3api "$@"; }

echo ">> driving ${OPS} write+read operations through the cache S3 surface"
payload="${work}/payload.bin"
head -c 65536 /dev/urandom > "${payload}"
n_put=0; n_get_ok=0; n_get_repeat_ok=0
for i in $(seq 1 "${OPS}"); do
  key="smoke/obj-$((i % 50)).bin"   # 50 distinct keys, reused → warm cache hits
  # write-through PUT (cache invalidates its mapping, origin gets the bytes)
  s3 put-object --bucket "${BUCKET}" --key "${key}" --body "${payload}" >/dev/null
  n_put=$((n_put + 1))
  # read-through GET (cold fills from origin, warm serves from cache)
  out="${work}/got.bin"
  s3 get-object --bucket "${BUCKET}" --key "${key}" "${out}" >/dev/null
  cmp -s "${payload}" "${out}" && n_get_ok=$((n_get_ok + 1))
  # a second GET of the same key should be a warm cache hit
  s3 get-object --bucket "${BUCKET}" --key "${key}" "${out}" >/dev/null
  cmp -s "${payload}" "${out}" && n_get_repeat_ok=$((n_get_repeat_ok + 1))
done

# Sample RSS while the server is warm and serving.
rss_kb="$(ps -o rss= -p "${cache_pid}" | tr -d ' ')"
rss_mb=$(( rss_kb / 1024 ))

echo ">> reading /admin/stats"
stats="$(curl -s "http://127.0.0.1:${ADMIN_PORT}/admin/stats")"
echo "${stats}" | python3 -m json.tool 2>/dev/null || echo "${stats}"

# Prove the reads actually flowed through the cache tiers (read-through fill on
# miss, warm hits on repeat) rather than every GET straight-to-origin. Stats are
# passed via an env var so the python program (read from -c) does not fight the
# heredoc for stdin.
cache_check="$(STATS_JSON="${stats}" python3 -c '
import json, os
s = json.loads(os.environ["STATS_JSON"])
c = s["counters"]
warm = c["dram_hits"] + c["disk_hits"]
print(c["backend_fills"], warm, s["dram_usage_bytes"])
')"
read -r cc_fills cc_warm cc_dram <<<"${cache_check}"

echo ">> checking /metrics exposition"
metrics_ct="$(curl -s -o /dev/null -w '%{content_type}' "http://127.0.0.1:${ADMIN_PORT}/metrics")"

# Binary sizes (stripped) for the footprint comparison.
strip_size() {
  local f="$1"
  [[ -f "$f" ]] || { echo "n/a"; return; }
  local tmp; tmp="$(mktemp)"
  cp "$f" "$tmp"
  strip "$tmp" 2>/dev/null || true
  local bytes; bytes="$(wc -c < "$tmp")"
  rm -f "$tmp"
  echo "$(( bytes / 1024 / 1024 ))MB (${bytes} bytes)"
}

echo
echo "==================== SMOKE RESULT ===================="
echo "operations:            PUT=${n_put}  GET-ok=${n_get_ok}/${OPS}  repeat-GET-ok=${n_get_repeat_ok}/${OPS}"
echo "cache tiers exercised: backend_fills=${cc_fills}  warm_hits(dram+disk)=${cc_warm}  dram_resident=${cc_dram}B"
echo "healthz:               ready (200) after serve-gating"
echo "metrics content-type:  ${metrics_ct}"
echo "resident memory (RSS): ${rss_mb} MB (${rss_kb} KB) serving warm"
echo "cache-node binary:     $(strip_size "${BIN}")  [stripped]"
echo "======================================================"

# Fail the smoke if any read came back wrong bytes — wrong is never acceptable.
if [[ "${n_get_ok}" -ne "${OPS}" || "${n_get_repeat_ok}" -ne "${OPS}" ]]; then
  echo "error: some reads returned wrong or missing bytes" >&2
  exit 1
fi
# The cache must have filled from origin and served warm hits; zero would mean
# every read bypassed the tiers (see the checksum note above).
if [[ "${cc_fills}" -eq 0 || "${cc_warm}" -eq 0 ]]; then
  echo "error: cache tiers were never exercised (fills=${cc_fills} warm_hits=${cc_warm})" >&2
  exit 1
fi
echo "OK"
