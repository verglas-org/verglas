#!/usr/bin/env bash
# Single-node write-back crash-recovery evidence (#286): ack-implies-durable.
#
# Proves the load-bearing durability claim end to end against a REAL server and
# a REAL S3 origin (MinIO): a PUT fast-acks from local durability while the
# origin is unreachable, the server is kill -9'd between that ack and the S3
# flush, and after MinIO returns and the server restarts, boot recovery replays
# the buffered segment so the object reaches S3 byte-identically. Nothing was
# ever flushed before the crash (verified: the object is absent from S3 until
# recovery runs), so this is genuine ack-then-crash-then-replay, not a race.
#
# Safety: this NEVER broad-kills. It kills only the exact server PID it spawned
# and stops MinIO by its unique container name. High ports, temp dirs.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"

# Unique, high, and named so cleanup is exact.
CONTAINER="vg-wb286-minio"
MINIO_PORT="${VG_WB286_MINIO_PORT:-9288}"
S3_PORT="${VG_WB286_S3_PORT:-9286}"
ADMIN_PORT="${VG_WB286_ADMIN_PORT:-9287}"
BUCKET="wb286"
KEY="data/segment-1"

ORIGIN_AK="minioadmin"; ORIGIN_SK="minioadmin"
DEV_AK="devkeydevkeydevkey"; DEV_SK="devsecretdevsecretdevsecret"

WORK="$(mktemp -d -t vg-wb286.XXXXXX)"
CACHE_DIR="$WORK/cache"; mkdir -p "$CACHE_DIR"
VGD_PID=""

log() { echo "[wb286] $*" >&2; }

cleanup() {
  [[ -n "$VGD_PID" ]] && kill -9 "$VGD_PID" 2>/dev/null || true
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  rm -rf "$WORK" 2>/dev/null || true
}
trap cleanup EXIT

# aws helpers, one for the Verglas endpoint (dev creds) and one for MinIO direct
# (origin creds). --no-verify-ssl not needed; plain http.
vg_s3()  { AWS_ACCESS_KEY_ID="$DEV_AK"    AWS_SECRET_ACCESS_KEY="$DEV_SK"    AWS_REGION=us-east-1 aws --endpoint-url "http://127.0.0.1:${S3_PORT}"   "$@"; }
minio()  { AWS_ACCESS_KEY_ID="$ORIGIN_AK" AWS_SECRET_ACCESS_KEY="$ORIGIN_SK" AWS_REGION=us-east-1 aws --endpoint-url "http://127.0.0.1:${MINIO_PORT}" "$@"; }

wait_for() { # url attempts
  local url="$1" n="${2:-100}"
  for _ in $(seq 1 "$n"); do curl -sf "$url" >/dev/null 2>&1 && return 0; sleep 0.1; done
  return 1
}

start_minio() {
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  docker run -d --name "$CONTAINER" -p "${MINIO_PORT}:9000" \
    -e "MINIO_ROOT_USER=$ORIGIN_AK" -e "MINIO_ROOT_PASSWORD=$ORIGIN_SK" \
    minio/minio:latest server /data >/dev/null
  wait_for "http://127.0.0.1:${MINIO_PORT}/minio/health/ready" 150 \
    || { log "MinIO did not become ready"; exit 1; }
}

write_config() {
  # Endpoint keypair the S3 endpoint accepts, in AWS-INI format (mode 0600).
  cat > "$WORK/endpoint-creds" <<CREDS
[default]
aws_access_key_id = $DEV_AK
aws_secret_access_key = $DEV_SK
CREDS
  chmod 600 "$WORK/endpoint-creds"
  cat > "$WORK/verglas-cache-node.toml" <<TOML
[listen]
s3_port = $S3_PORT
admin_port = $ADMIN_PORT

[cache]
dir = "$CACHE_DIR"
capacity_bytes = "1GB"
dram_bytes = "256MB"

[cache.writeback]
enabled = true            # single-node: PUTs fast-ack from local durability (#286)
ack_deadline_ms = 2000

[backend]
bucket = "$BUCKET"

[auth]
credentials_file = "$WORK/endpoint-creds"
TOML
}

start_server() {
  AWS_ENDPOINT="http://127.0.0.1:${MINIO_PORT}" \
  AWS_ACCESS_KEY_ID="$ORIGIN_AK" AWS_SECRET_ACCESS_KEY="$ORIGIN_SK" \
  AWS_REGION=us-east-1 AWS_ALLOW_HTTP=true AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false \
  VERGLAS_S3_ADDR="127.0.0.1:${S3_PORT}" VERGLAS_ADMIN_ADDR="127.0.0.1:${ADMIN_PORT}" \
    "$VGD" --config "$WORK/verglas-cache-node.toml" >"$WORK/vgd.log" 2>&1 &
  VGD_PID=$!
  disown "$VGD_PID" 2>/dev/null || true   # keep job control quiet when we kill it
  wait_for "http://127.0.0.1:${ADMIN_PORT}/admin/healthz" 150 \
    || { log "verglas-cache-node did not become healthy"; cat "$WORK/vgd.log" >&2; exit 1; }
}

# ---- build ---------------------------------------------------------------- #
log "building verglas-cache-node"
( cd "$REPO" && cargo build -q -p verglas-cache-node )
VGD="$REPO/target/debug/verglas-cache-node"

# ---- origin up, bucket created -------------------------------------------- #
log "starting MinIO ($CONTAINER) on :$MINIO_PORT"
start_minio
minio s3api create-bucket --bucket "$BUCKET" >/dev/null 2>&1 || true
minio s3api head-bucket --bucket "$BUCKET" || { log "bucket create failed"; exit 1; }

# A known object we will chase all the way to S3.
OBJ="$WORK/segment.bin"
head -c 131072 /dev/urandom > "$OBJ"
WANT="$(shasum -a 256 "$OBJ" | awk '{print $1}')"
log "object sha256=$WANT"

# ---- server up ------------------------------------------------------------ #
log "starting verglas-cache-node (single-node write-back)"
write_config
start_server
grep -q "single-node" "$WORK/vgd.log" && log "server reports single-node write-back mode"

# ---- take the origin down: force the ack-before-flush window --------------- #
log "stopping MinIO — the origin is now unreachable"
docker stop "$CONTAINER" >/dev/null

# ---- PUT: must fast-ack from local durability despite the origin being down  #
log "PUT $KEY through Verglas with the origin down (expect a local fast-ack)"
t0=$(date +%s%N)
vg_s3 s3api put-object --bucket "$BUCKET" --key "$KEY" --body "$OBJ" >/dev/null \
  || { log "FAIL: PUT did not ack with the origin down"; cat "$WORK/vgd.log" >&2; exit 1; }
t1=$(date +%s%N)
log "PUT acked in $(( (t1 - t0) / 1000000 ))ms (no origin round-trip on the path)"

# The ack is backed by a fsynced-dirty journal on local disk.
DIRTY="$(grep -rl '"Dirty"' "$CACHE_DIR"/writeback-journals/ 2>/dev/null | head -1 || true)"
[[ -n "$DIRTY" ]] || { log "FAIL: no dirty journal after ack"; exit 1; }
log "dirty journal on disk: $(basename "$DIRTY")"

# ---- kill -9 the server between the ack and the S3 flush ------------------- #
log "kill -9 verglas-cache-node (PID $VGD_PID) — crash between ack and flush"
kill -9 "$VGD_PID"; wait "$VGD_PID" 2>/dev/null || true; VGD_PID=""

# ---- origin returns; prove the object was NEVER flushed pre-crash ---------- #
log "restarting MinIO"
docker start "$CONTAINER" >/dev/null
wait_for "http://127.0.0.1:${MINIO_PORT}/minio/health/ready" 150 || { log "MinIO not ready"; exit 1; }
if minio s3api head-object --bucket "$BUCKET" --key "$KEY" >/dev/null 2>&1; then
  log "FAIL: object was in S3 before recovery — not a genuine ack-before-flush crash"
  exit 1
fi
log "confirmed: object is ABSENT from S3 immediately after the crash"

# ---- restart the server: boot recovery must replay the segment to S3 ------- #
log "restarting verglas-cache-node — boot recovery replays the unflushed segment"
start_server

log "waiting for the recovered segment to reach S3 (direct MinIO read, no Verglas)"
GOT=""
for _ in $(seq 1 100); do
  if minio s3api get-object --bucket "$BUCKET" --key "$KEY" "$WORK/from-s3.bin" >/dev/null 2>&1; then
    GOT="$(shasum -a 256 "$WORK/from-s3.bin" | awk '{print $1}')"
    break
  fi
  sleep 0.2
done

[[ -n "$GOT" ]] || { log "FAIL: recovered segment never reached S3"; cat "$WORK/vgd.log" >&2; exit 1; }
if [[ "$GOT" == "$WANT" ]]; then
  log "PASS: recovered segment reached S3 byte-identically (sha256=$GOT)"
  log "ack-implies-durable holds across kill -9 between ack and flush."
  exit 0
else
  log "FAIL: S3 object sha256=$GOT != acked object sha256=$WANT"
  exit 1
fi
