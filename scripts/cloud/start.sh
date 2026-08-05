#!/usr/bin/env bash
# Cursor Cloud environment start step.
#
# Launched detached on every VM boot. Brings up the local dev stack so the box is
# ready to serve: a MinIO origin on :9000 and verglas-server (S3 :8333, admin
# :8334) reading through to it. Idempotent — if a service is already listening on
# its port, it is left alone, so re-running is safe.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO"

RUNTIME_DIR="/tmp/verglas-dev"
CONFIG="$REPO/deploy/dev/verglas.dev.toml"
MINIO_LOG="$RUNTIME_DIR/minio.log"
SERVER_LOG="$RUNTIME_DIR/verglas-server.log"
MINIO_DATA="$RUNTIME_DIR/origin"
BUCKET="my-bucket"

mkdir -p "$RUNTIME_DIR/cache" "$MINIO_DATA"

# Local-only dev credentials. Engines present the endpoint keypair to Verglas on
# :8333; the backend keypair is MinIO's default. For a real backend, set AWS_* in
# the Secrets panel — the AWS env chain overrides backend.credentials_file.
if [ ! -f "$RUNTIME_DIR/endpoint-creds" ]; then
  cat > "$RUNTIME_DIR/endpoint-creds" <<'EOF'
[default]
aws_access_key_id = engine-ak
aws_secret_access_key = engine-secret
EOF
  chmod 600 "$RUNTIME_DIR/endpoint-creds"
fi
if [ ! -f "$RUNTIME_DIR/backend-creds" ]; then
  cat > "$RUNTIME_DIR/backend-creds" <<'EOF'
[default]
aws_access_key_id = minioadmin
aws_secret_access_key = minioadmin
EOF
  chmod 600 "$RUNTIME_DIR/backend-creds"
fi

# True when something is already listening on 127.0.0.1:$1 (bash /dev/tcp probe).
port_open() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && exec 3>&- 3<&- && return 0 || return 1; }

# Block until $2 accepts connections on port $1, or fail after ~30s.
wait_for_port() {
  local port="$1" name="$2" i
  for i in $(seq 1 60); do
    if port_open "$port"; then echo "  $name is up on :$port"; return 0; fi
    sleep 0.5
  done
  echo "  ERROR: $name did not open :$port in time" >&2
  return 1
}

echo "== verglas cloud start: MinIO origin (:9000) =="
if port_open 9000; then
  echo "  MinIO already running, leaving it alone"
else
  MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
    nohup minio server "$MINIO_DATA" --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 \
    > "$MINIO_LOG" 2>&1 &
  wait_for_port 9000 MinIO
fi

echo "== verglas cloud start: origin bucket =="
mc alias set localminio http://127.0.0.1:9000 minioadmin minioadmin >/dev/null 2>&1 || true
mc mb -p "localminio/$BUCKET" >/dev/null 2>&1 || true
echo "  bucket ready: $BUCKET"

echo "== verglas cloud start: verglas-server (S3 :8333, admin :8334) =="
if port_open 8334; then
  echo "  verglas-server already running, leaving it alone"
else
  SERVER_BIN="$REPO/target/debug/verglas-server"
  if [ ! -x "$SERVER_BIN" ]; then
    echo "  server binary missing, building it"
    cargo build -p verglas-server
  fi
  # Origin credentials come from the config's backend.credentials_file; allow_http
  # is set in the config. AWS_ALLOW_HTTP is a belt-and-braces echo of that.
  AWS_ALLOW_HTTP=true nohup "$SERVER_BIN" --config "$CONFIG" > "$SERVER_LOG" 2>&1 &
  wait_for_port 8334 verglas-server
fi

echo "== verglas cloud start: ready =="
echo "  S3 endpoint:   http://127.0.0.1:8333  (engine keypair: engine-ak / engine-secret)"
echo "  admin API:     http://127.0.0.1:8334  (VERGLAS_ENDPOINT)"
echo "  MinIO origin:  http://127.0.0.1:9000  (minioadmin / minioadmin)"
echo "  logs:          $SERVER_LOG , $MINIO_LOG"
