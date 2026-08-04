#!/usr/bin/env bash
#
# Part-B: the memory pipeline end to end over a REAL running daemon, a REAL
# Iceberg REST catalog, and REAL S3 (MinIO) — capture -> consolidate ->
# graph + ANN index -> hybrid recall, then the reboot re-serve.
#
# Stands up docker MinIO + an Iceberg REST catalog on HIGH host ports backed by
# MinIO, builds and runs the branch verglasd on HIGH admin/S3 ports with a temp
# cache dir and a temp HOME, drives consolidation through the daemon's loopback
# catalog with a DETERMINISTIC extractor (the Rust driver real_daemon_e2e.rs),
# builds and serves the ANN index over the daemon HTTP surface, and asserts the
# ANN-served hybrid ranking and the reboot re-serve. Tears down only the
# containers and daemon it started.
#
# Skips cleanly (exit 0) when docker is unavailable. Requires jq and curl.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../.." && pwd)"

if ! docker info >/dev/null 2>&1; then
  echo "SKIP: docker unavailable"
  exit 0
fi
for tool in jq curl; do
  command -v "$tool" >/dev/null 2>&1 || { echo "SKIP: $tool not installed"; exit 0; }
done

# --- HIGH ports + temp dirs + temp HOME (never the live 8333/8334 daemon) ------
MINIO_PORT="${MINIO_PORT:-19100}"
REST_PORT="${REST_PORT:-19181}"
DS3="${DS3:-19333}"
DADMIN="${DADMIN:-19334}"
BUCKET="warehouse"
WAREHOUSE="s3://warehouse/"
AGENT="agent"

sfx="$$"
NET="vg-e2e-net-$sfx"
MINIO_C="vg-e2e-minio-$sfx"
REST_C="vg-e2e-rest-$sfx"
WORK="$(mktemp -d)"
export HOME="$WORK/home"; mkdir -p "$HOME"
CACHE="$WORK/cache"; mkdir -p "$CACHE"
DAEMON_PID=""

cleanup() {
  set +e
  [ -n "$DAEMON_PID" ] && { kill "$DAEMON_PID" 2>/dev/null; wait "$DAEMON_PID" 2>/dev/null; }
  docker rm -f "$REST_C" "$MINIO_C" >/dev/null 2>&1
  docker network rm "$NET" >/dev/null 2>&1
  rm -rf "$WORK"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; [ -f "$WORK/daemon.log" ] && { echo "--- daemon.log tail ---" >&2; tail -40 "$WORK/daemon.log" >&2; }; exit 1; }

wait_for() { # url attempts
  local url="$1" n="${2:-60}"
  for _ in $(seq 1 "$n"); do curl -sf "$url" >/dev/null 2>&1 && return 0; sleep 1; done
  return 1
}

echo "== 1. docker MinIO + Iceberg REST catalog on high ports =="
docker network create "$NET" >/dev/null
docker run -d --name "$MINIO_C" --network "$NET" -p "127.0.0.1:$MINIO_PORT:9000" \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data >/dev/null
wait_for "http://127.0.0.1:$MINIO_PORT/minio/health/ready" 60 || fail "MinIO did not become ready"
docker run --rm --network "$NET" --entrypoint sh minio/mc -c \
  "mc alias set m http://$MINIO_C:9000 minioadmin minioadmin && mc mb -p m/$BUCKET" >/dev/null \
  || fail "creating the warehouse bucket failed"

# The apache Iceberg REST fixture, backed by MinIO. The memory/graph/_LOGS table
# creates carry explicit partition field ids, so this strict fixture accepts
# them.
REST_IMAGE="${REST_IMAGE:-apache/iceberg-rest-fixture:latest}"
docker run -d --name "$REST_C" --network "$NET" -p "127.0.0.1:$REST_PORT:8181" \
  -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin -e AWS_REGION=us-east-1 \
  -e CATALOG_WAREHOUSE="$WAREHOUSE" \
  -e CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO \
  -e CATALOG_S3_ENDPOINT="http://$MINIO_C:9000" \
  -e CATALOG_S3_PATH__STYLE__ACCESS=true \
  "$REST_IMAGE" >/dev/null
wait_for "http://127.0.0.1:$REST_PORT/v1/config?warehouse=$WAREHOUSE" 60 || fail "Iceberg REST catalog did not come up"

echo "== 2. build + run the branch verglasd on high ports, temp cache + HOME =="
cat > "$WORK/minio.creds" <<EOF
[default]
aws_access_key_id = minioadmin
aws_secret_access_key = minioadmin
EOF
chmod 600 "$WORK/minio.creds"
cat > "$WORK/endpoint.creds" <<EOF
[default]
aws_access_key_id = dev
aws_secret_access_key = devsecret
EOF
chmod 600 "$WORK/endpoint.creds"
cat > "$WORK/verglas.toml" <<EOF
[listen]
s3_port = $DS3
admin_port = $DADMIN
[cache]
dir = "$CACHE"
[backend]
bucket = "$BUCKET"
endpoint = "http://127.0.0.1:$MINIO_PORT"
region = "us-east-1"
allow_http = true
virtual_hosted_style = false
credentials_file = "$WORK/minio.creds"
[auth]
credentials_file = "$WORK/endpoint.creds"
[catalog]
uri = "http://127.0.0.1:$REST_PORT"
warehouse = "$WAREHOUSE"
poll_interval_secs = 2
EOF

cargo build -p verglasd 2>&1 | tail -1
BIN="$root/target/debug/verglasd"

start_daemon() {
  VERGLAS_RECALL_EMBED=mock "$BIN" --config "$WORK/verglas.toml" >>"$WORK/daemon.log" 2>&1 &
  DAEMON_PID=$!
  wait_for "http://127.0.0.1:$DADMIN/admin/healthz" 60 || fail "daemon did not become healthy"
}
start_daemon
echo "daemon pid $DAEMON_PID on admin:$DADMIN s3:$DS3"

echo "== 3a. capture -> consolidate (deterministic extractor) through the daemon =="
VERGLAS_E2E_CATALOG_URI="http://127.0.0.1:$DADMIN/catalog" \
VERGLAS_E2E_S3_ENDPOINT="http://127.0.0.1:$DS3" \
VERGLAS_E2E_WAREHOUSE="$WAREHOUSE" \
VERGLAS_E2E_REGION="us-east-1" \
VERGLAS_E2E_ACCESS_KEY="dev" \
VERGLAS_E2E_SECRET_KEY="devsecret" \
  cargo test -p verglas-memory-jobs --test real_daemon_e2e -- --nocapture 2>&1 | tee "$WORK/consolidate.log"
grep -q "E2E_CONSOLIDATION_OK" "$WORK/consolidate.log" || fail "consolidation driver did not confirm memories + graph"

recall() { # query -> response json
  curl -sf -XPOST "http://127.0.0.1:$DADMIN/v1/recall" -H 'content-type: application/json' \
    -d "{\"agent_id\":\"$AGENT\",\"query\":\"$1\",\"k\":5}"
}
assert_recall() { # phase-label response
  local label="$1" resp="$2"
  echo "$resp" | jq . >/dev/null 2>&1 || fail "$label: recall response was not JSON: $resp"
  local src reachable unrelated top
  src=$(echo "$resp" | jq -r '.seed_source')
  [ "$src" = "ann" ] || fail "$label: expected seed_source=ann, got '$src' (resp: $resp)"
  top=$(echo "$resp" | jq -r '.hits[0].text')
  echo "$top" | grep -q "storage backend" || fail "$label: top hit was not the seed memory: '$top'"
  reachable=$(echo "$resp" | jq '[.hits[].text] | map(test("rotated token")) | index(true)')
  unrelated=$(echo "$resp" | jq '[.hits[].text] | map(test("office plants")) | index(true)')
  [ "$reachable" != "null" ] || fail "$label: reachable memory missing from recall"
  [ "$unrelated" != "null" ] || fail "$label: unrelated memory missing from recall"
  [ "$reachable" -lt "$unrelated" ] || fail "$label: graph-reachable memory did not outrank the unrelated one"
  echo "$label: seed_source=ann, top='$top', reachable#$reachable < unrelated#$unrelated  OK"
}

echo "== 3b. build the ANN index into the shadow store, assert the registry row =="
curl -sf -XPOST "http://127.0.0.1:$DADMIN/v1/tables/agent_memory.memories/indexes/embedding/refresh" \
  | jq -e '.liveCount >= 3' >/dev/null || fail "index refresh did not build >=3 vectors"
curl -sf "http://127.0.0.1:$DADMIN/v1/indexes" \
  | jq -e '.indexes[] | select(.target=="tbl:agent_memory.memories" and .field=="embedding")' >/dev/null \
  || fail "no verglas_sys.indexes row for the memory index"
echo "verglas_sys.indexes row present; index built"

echo "== 3c. hybrid recall over the daemon, ANN-served =="
RESP1="$(recall 'acme storage backend')"
echo "$RESP1" | jq -c '{seed_source, hits: [.hits[] | {text, score}]}'
assert_recall "recall(pre-reboot)" "$RESP1"

echo "== 3d. reboot the daemon; the index rehydrates and recall still ANN-serves =="
kill "$DAEMON_PID"; wait "$DAEMON_PID" 2>/dev/null || true; DAEMON_PID=""
start_daemon
echo "daemon restarted pid $DAEMON_PID"
RESP2="$(recall 'acme storage backend')"
echo "$RESP2" | jq -c '{seed_source, hits: [.hits[] | {text, score}]}'
assert_recall "recall(post-reboot)" "$RESP2"

echo "== 4. teardown =="
echo "PASS: memory pipeline e2e over a real daemon + real catalog + real S3 (ANN-served, reboot re-serve)"
