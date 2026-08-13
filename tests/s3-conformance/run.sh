#!/usr/bin/env bash
# Ceph s3-tests conformance harness for Verglas (issue #22).
#
# One entrypoint. Brings up MinIO (origin) via docker compose, builds and runs
# verglas-cache-node pointed at it, then runs the pinned ceph/s3-tests suite against the
# VERGLAS endpoint with the dev credentials. The pass/skip surface is defined by
# markers-exclude.txt (feature groups) + skip-list.txt (per-test residue) — the
# documented compatibility contract. See README.md.
#
# Nothing here is customer-facing; the dev keypair is a fixed non-secret used
# only to authenticate the local test client to the local server.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
WORK="$HERE/.work"                 # gitignored scratch: venv, cloned suite, cache
S3TESTS_DIR="$WORK/s3-tests"
VENV="$WORK/venv"

# Pinned ceph/s3-tests commit — the suite is the contract, so the version is too.
S3TESTS_REPO="https://github.com/ceph/s3-tests.git"
S3TESTS_COMMIT="5522d1c351f75bc00ae0f64f742f3f095f5939d9"

# ---- defaults (override via flags/env) ------------------------------------ #
MODE="full"                        # full | smoke | list
PROFILE="release"                  # release | debug (cargo profile for verglas-cache-node)
KEEP=""                            # --keep leaves MinIO + server up for inspection
S3_PORT="${VG_S3_PORT:-8433}"
ADMIN_PORT="${VG_ADMIN_PORT:-8434}"
MINIO_PORT="${VG_MINIO_PORT:-9100}"
ORIGIN_AK="${VG_ORIGIN_ACCESS_KEY:-minioadmin}"
ORIGIN_SK="${VG_ORIGIN_SECRET_KEY:-minioadmin}"
DEV_AK="VGCONFORMANCEKEY"
DEV_SK="vgconformancesecret0123456789abcd"
EXTRA=()                           # extra args forwarded to pytest

usage() {
  cat <<'EOF'
Usage: run.sh [options] [-- <extra pytest args>]

  --full         run the full suite: test_s3.py + test_headers.py, feature-group
                 markers excluded, per-test skip-list applied (default).
  --smoke        run only the fast PR-path subset (smoke-list.txt).
  --list         collect-only: print what WOULD run, then exit (no server).
  --debug        build verglas-cache-node with the debug profile (faster compile).
  --keep         leave MinIO + verglas-cache-node running after the suite (for debugging).
  --s3-port N    verglas-cache-node S3 port (default 8433).
  --minio-port N host port MinIO is published on (default 9100).
  -h|--help      this help.

Prerequisites: docker (compose v2), python3.13 (or python3), a Rust toolchain.
The suite runs against the Verglas endpoint; MinIO is the origin.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --full)  MODE="full"; shift ;;
    --smoke) MODE="smoke"; shift ;;
    --list)  MODE="list"; shift ;;
    --debug) PROFILE="debug"; shift ;;
    --keep)  KEEP=1; shift ;;
    --s3-port)    S3_PORT="$2"; ADMIN_PORT="$(( S3_PORT + 1 ))"; shift 2 ;;
    --minio-port) MINIO_PORT="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    --) shift; EXTRA=("$@"); break ;;
    *) echo "unknown flag: $1" >&2; usage; exit 2 ;;
  esac
done

mkdir -p "$WORK"

# ---- python venv + pinned s3-tests (idempotent) --------------------------- #
if [[ ! -d "$S3TESTS_DIR/.git" ]]; then
  echo "[setup] cloning ceph/s3-tests @ ${S3TESTS_COMMIT:0:12}" >&2
  git clone -q "$S3TESTS_REPO" "$S3TESTS_DIR"
fi
git -C "$S3TESTS_DIR" fetch -q origin "$S3TESTS_COMMIT" 2>/dev/null || true
git -C "$S3TESTS_DIR" checkout -q "$S3TESTS_COMMIT"

if [[ ! -x "$VENV/bin/python" ]]; then
  echo "[setup] creating venv" >&2
  PYBIN="$(command -v python3.13 || command -v python3)"
  "$PYBIN" -m venv "$VENV"
fi
STAMP="$VENV/.reqsha"
NEWSHA="$(shasum "$S3TESTS_DIR/requirements.txt" | awk '{print $1}')"
if [[ ! -f "$STAMP" || "$(cat "$STAMP")" != "$NEWSHA" ]]; then
  echo "[setup] installing s3-tests deps" >&2
  "$VENV/bin/pip" install -q --upgrade pip
  "$VENV/bin/pip" install -q -r "$S3TESTS_DIR/requirements.txt"
  echo "$NEWSHA" > "$STAMP"
fi
PY="$VENV/bin/python"

# ---- collect-only mode exits before touching docker/cargo ----------------- #
export S3TEST_CONF PYTHONPATH VG_SKIP_LIST
PYTHONPATH="$HERE"
VG_SKIP_LIST="$HERE/skip-list.txt"
S3TEST_CONF="$WORK/s3tests.conf"

# AND-join the non-comment marker lines into one -m expression.
MEXPR="$(grep -vE '^\s*#|^\s*$' "$HERE/markers-exclude.txt" | paste -sd '&' - | sed 's/&/ and /g')"

if [[ "$MODE" == "list" ]]; then
  # No origin creds needed to just collect; point at the checked-in conf.
  S3TEST_CONF="$HERE/s3tests.conf"
  VG_ORIGIN_ENDPOINT="http://127.0.0.1:${MINIO_PORT}" \
  VG_ORIGIN_ACCESS_KEY="$ORIGIN_AK" VG_ORIGIN_SECRET_KEY="$ORIGIN_SK" \
    "$VENV/bin/pytest" -p vg_s3tests_plugin -q --collect-only \
      "$S3TESTS_DIR/s3tests/functional/test_s3.py" \
      "$S3TESTS_DIR/s3tests/functional/test_headers.py" \
      -m "$MEXPR" --rootdir "$S3TESTS_DIR" 2>&1 | tail -3
  exit 0
fi

# ---- origin (MinIO) + server lifecycle ------------------------------------ #
COMPOSE=(docker compose -p vg-s3conf -f "$HERE/docker-compose.yml")
VGD_PID=""
cleanup() {
  [[ -n "$VGD_PID" ]] && kill "$VGD_PID" 2>/dev/null || true
  if [[ -z "$KEEP" ]]; then
    VG_MINIO_PORT="$MINIO_PORT" "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
  else
    echo "[keep] MinIO + verglas-cache-node left running (S3 http://127.0.0.1:${S3_PORT})" >&2
  fi
}
trap cleanup EXIT

echo "[origin] starting MinIO on :$MINIO_PORT" >&2
VG_MINIO_PORT="$MINIO_PORT" VG_ORIGIN_ACCESS_KEY="$ORIGIN_AK" VG_ORIGIN_SECRET_KEY="$ORIGIN_SK" \
  "${COMPOSE[@]}" up -d --wait

# Build verglas-cache-node from this repo and run it as the system under test.
echo "[server] building verglas-cache-node ($PROFILE)" >&2
BUILD_FLAGS=(); [[ "$PROFILE" == "release" ]] && BUILD_FLAGS=(--release)
( cd "$REPO" && cargo build -q -p verglas-cache-node ${BUILD_FLAGS[@]+"${BUILD_FLAGS[@]}"} )
VGD="$REPO/target/$PROFILE/verglas-cache-node"

CACHE_DIR="$WORK/cache"; mkdir -p "$CACHE_DIR"
cat > "$WORK/endpoint-credentials" <<CREDS
[default]
aws_access_key_id = $DEV_AK
aws_secret_access_key = $DEV_SK
CREDS
chmod 600 "$WORK/endpoint-credentials"
cat > "$WORK/verglas-cache-node.toml" <<TOML
[listen]
s3_port = $S3_PORT
admin_port = $ADMIN_PORT

[cache]
dir = "$CACHE_DIR"
capacity_bytes = "2GB"
dram_bytes = "512MB"

[backend]
bucket_globs = ["*"]
endpoint = "http://127.0.0.1:${MINIO_PORT}"
region = "us-east-1"
allow_http = true

[auth]
credentials_file = "$WORK/endpoint-credentials"
TOML

echo "[server] starting verglas-cache-node → MinIO" >&2
AWS_ENDPOINT="http://127.0.0.1:${MINIO_PORT}" \
AWS_ACCESS_KEY_ID="$ORIGIN_AK" AWS_SECRET_ACCESS_KEY="$ORIGIN_SK" \
AWS_REGION=us-east-1 AWS_ALLOW_HTTP=true AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false \
VERGLAS_S3_ADDR="127.0.0.1:${S3_PORT}" VERGLAS_ADMIN_ADDR="127.0.0.1:${ADMIN_PORT}" \
  "$VGD" --config "$WORK/verglas-cache-node.toml" &
VGD_PID=$!
disown "$VGD_PID" 2>/dev/null || true   # keep job control quiet when we kill it

for _ in $(seq 1 100); do
  curl -sf "http://127.0.0.1:${ADMIN_PORT}/admin/healthz" >/dev/null && break
  kill -0 "$VGD_PID" 2>/dev/null || { echo "verglas-cache-node died on startup" >&2; exit 1; }
  sleep 0.1
done

# Render the s3tests.conf copy with the chosen S3 port.
sed -E "s/^port = .*/port = ${S3_PORT}/" "$HERE/s3tests.conf" > "$S3TEST_CONF"

# ---- run the suite -------------------------------------------------------- #
# boto3's checksum behaviour is left at its modern default (issue #208):
# Verglas forwards per-part and composite checksums through the raw multipart
# lifecycle, so the checksum feature group runs with the SDK computing and
# sending checksums exactly as a current client does.
export VG_ORIGIN_ENDPOINT="http://127.0.0.1:${MINIO_PORT}"
export VG_ORIGIN_ACCESS_KEY="$ORIGIN_AK"
export VG_ORIGIN_SECRET_KEY="$ORIGIN_SK"

PYTEST=("$VENV/bin/pytest" -p vg_s3tests_plugin -q -p no:cacheprovider --rootdir "$S3TESTS_DIR")

if [[ "$MODE" == "smoke" ]]; then
  echo "[run] smoke subset" >&2
  SMOKE=()
  while IFS= read -r line; do SMOKE+=("$S3TESTS_DIR/$line"); done < <(
    grep -vE '^\s*#|^\s*$' "$HERE/smoke-list.txt")
  "${PYTEST[@]}" "${SMOKE[@]}" ${EXTRA[@]+"${EXTRA[@]}"}
else
  echo "[run] full suite (markers excluded, skip-list applied)" >&2
  VG_SKIP_LIST_STRICT=1 "${PYTEST[@]}" \
    "$S3TESTS_DIR/s3tests/functional/test_s3.py" \
    "$S3TESTS_DIR/s3tests/functional/test_headers.py" \
    -m "$MEXPR" ${EXTRA[@]+"${EXTRA[@]}"}
fi
