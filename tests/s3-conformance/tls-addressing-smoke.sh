#!/usr/bin/env bash
# TLS termination + addressing-style smoke test for Verglas (issue #11).
#
# Proves, end to end against a real MinIO origin, that:
#   1. the S3 endpoint serves over TLS from a self-signed cert (verified two
#      ways: CA injection with --cacert, and --no-verify with -k);
#   2. a virtual-hosted-style request (`bucket.<domain>`) resolves the SAME
#      object as the path-style request (`<domain>/bucket`).
#
# TLS terminates at the server by design (production fronts it with an L4 NLB in
# TCP passthrough), so this is where HTTPS and the signed Host header meet.
#
# Requires: docker (compose v2), openssl, curl with --aws-sigv4 (>= 7.75), a
# Rust toolchain. Self-contained and idempotent; tears everything down on exit.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
WORK="$HERE/.work/tls"
mkdir -p "$WORK"

DOMAIN="s3.verglas.test"
S3_PORT="${VG_TLS_S3_PORT:-8543}"
ADMIN_PORT="${VG_TLS_ADMIN_PORT:-8544}"
MINIO_PORT="${VG_TLS_MINIO_PORT:-9200}"
ORIGIN_AK="minioadmin"
ORIGIN_SK="minioadmin"
DEV_AK="VGTLSSMOKEKEY"
DEV_SK="vgtlssmokesecret0123456789abcd"
REGION="us-east-1"
BUCKET="tls-lake"
KEY="warehouse/part-0.parquet"

# ---- self-signed cert with SANs for both addressing styles ---------------- #
# SANs: the base domain (path-style Host), a wildcard (virtual-hosted Host), and
# 127.0.0.1 (direct-IP checks). Wildcard DNS `*.<domain>` is exactly what a
# production virtual-hosted deployment needs at the NLB.
CERT="$WORK/cert.pem"
KEY_PEM="$WORK/key.pem"
echo "[cert] generating self-signed cert for $DOMAIN (+ *.$DOMAIN, 127.0.0.1)" >&2
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
  -keyout "$KEY_PEM" -out "$CERT" -subj "/CN=$DOMAIN" \
  -addext "subjectAltName=DNS:$DOMAIN,DNS:*.$DOMAIN,IP:127.0.0.1" \
  >/dev/null 2>&1

# ---- origin (MinIO) ------------------------------------------------------- #
COMPOSE=(docker compose -p vg-tls-smoke -f "$HERE/docker-compose.yml")
VGD_PID=""
cleanup() {
  [[ -n "$VGD_PID" ]] && kill "$VGD_PID" 2>/dev/null || true
  VG_MINIO_PORT="$MINIO_PORT" "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[origin] starting MinIO on :$MINIO_PORT" >&2
VG_MINIO_PORT="$MINIO_PORT" VG_ORIGIN_ACCESS_KEY="$ORIGIN_AK" VG_ORIGIN_SECRET_KEY="$ORIGIN_SK" \
  "${COMPOSE[@]}" up -d --wait

MINIO="http://127.0.0.1:$MINIO_PORT"
export AWS_ACCESS_KEY_ID="$ORIGIN_AK" AWS_SECRET_ACCESS_KEY="$ORIGIN_SK" AWS_DEFAULT_REGION="$REGION"

# Seed one known object into the origin bucket (path-style, plain HTTP).
echo "[origin] seeding s3://$BUCKET/$KEY" >&2
aws --endpoint-url "$MINIO" s3api create-bucket --bucket "$BUCKET" >/dev/null 2>&1 || true
BODY="$WORK/body.bin"
head -c 50000 /dev/urandom > "$BODY"
aws --endpoint-url "$MINIO" s3api put-object \
  --bucket "$BUCKET" --key "$KEY" --body "$BODY" >/dev/null

# ---- server (system under test) over TLS ---------------------------------- #
echo "[server] building verglas-server" >&2
( cd "$REPO" && cargo build -q -p verglas-server )
VGD="$REPO/target/debug/verglas-server"

CACHE_DIR="$WORK/cache"; mkdir -p "$CACHE_DIR"
cat > "$WORK/verglas-server.toml" <<TOML
[listen]
s3_port = $S3_PORT
admin_port = $ADMIN_PORT
# The base domain, WITH the non-standard port, so it matches the Host authority
# curl sends (production on :443 needs no port here).
domain = "$DOMAIN:$S3_PORT"

[listen.tls]
cert_path = "$CERT"
key_path = "$KEY_PEM"

[cache]
dir = "$CACHE_DIR"
capacity_bytes = "1GB"
dram_bytes = "256MB"

[auth]
access_key_id = "$DEV_AK"
secret_access_key = "$DEV_SK"
TOML

echo "[server] starting verglas-server (TLS) -> MinIO" >&2
AWS_ENDPOINT="$MINIO" \
AWS_ACCESS_KEY_ID="$ORIGIN_AK" AWS_SECRET_ACCESS_KEY="$ORIGIN_SK" \
AWS_REGION="$REGION" AWS_ALLOW_HTTP=true \
VERGLAS_S3_ADDR="127.0.0.1:$S3_PORT" VERGLAS_ADMIN_ADDR="127.0.0.1:$ADMIN_PORT" \
  "$VGD" --config "$WORK/verglas-server.toml" &
VGD_PID=$!

for _ in $(seq 1 100); do
  curl -sf "http://127.0.0.1:$ADMIN_PORT/admin/healthz" >/dev/null && break
  kill -0 "$VGD_PID" 2>/dev/null || { echo "verglas-server died on startup" >&2; exit 1; }
  sleep 0.1
done

# ---- checks --------------------------------------------------------------- #
SIG=(--aws-sigv4 "aws:amz:$REGION:s3" --user "$DEV_AK:$DEV_SK")
EXPECT="$(openssl dgst -sha256 "$BODY" | awk '{print $2}')"
fail=0

check() {  # name  <curl args...>
  local name="$1"; shift
  local out="$WORK/out-${name//\//-}.bin"
  local errf="$WORK/err-${name//\//-}.txt"
  local code
  code="$(curl -sS -o "$out" -w '%{http_code}' "$@" 2>"$errf")" || {
    echo "[FAIL] $name: curl error: $(tr '\n' ' ' <"$errf")" >&2; fail=1; return;
  }
  local got; got="$(openssl dgst -sha256 "$out" | awk '{print $2}')"
  if [[ "$code" == "200" && "$got" == "$EXPECT" ]]; then
    echo "[ok] $name: HTTP $code, body matches" >&2
  else
    echo "[FAIL] $name: HTTP $code, sha $got (want $EXPECT)" >&2; fail=1
  fi
}

# 1. Path-style over TLS, CA injected (--cacert). Host = <domain>:port.
check "path-style/cacert" "${SIG[@]}" --cacert "$CERT" \
  --resolve "$DOMAIN:$S3_PORT:127.0.0.1" \
  "https://$DOMAIN:$S3_PORT/$BUCKET/$KEY"

# 2. Path-style over TLS, verification off (-k / --no-verify), by direct IP.
check "path-style/no-verify" "${SIG[@]}" -k \
  --resolve "$DOMAIN:$S3_PORT:127.0.0.1" \
  "https://$DOMAIN:$S3_PORT/$BUCKET/$KEY"

# 3. Virtual-hosted style over TLS (bucket.<domain>), CA injected. Must resolve
#    the SAME object as path-style.
check "virtual-hosted/cacert" "${SIG[@]}" --cacert "$CERT" \
  --resolve "$BUCKET.$DOMAIN:$S3_PORT:127.0.0.1" \
  "https://$BUCKET.$DOMAIN:$S3_PORT/$KEY"

if [[ "$fail" == 0 ]]; then
  echo "[PASS] TLS termination + both addressing styles resolve the same object" >&2
else
  echo "[FAIL] one or more checks failed" >&2
fi
exit "$fail"
