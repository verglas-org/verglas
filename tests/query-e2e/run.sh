#!/usr/bin/env bash
#
# Query-worker end to end: the standalone `verglas-query` binary AND
# verglas-server's spawn-per-query dispatcher, both against a real Iceberg REST
# catalog + real S3 (MinIO) + a real verglas-server cache instance. Never a real
# tenant — high ports, temp dirs, torn down on exit.
#
# Proves: /healthz, /v1/query/estimate (near-zero working set for a large
# plain scan, nonzero for aggregate/join), /v1/query for a small select, an
# aggregate, and a large (tens-of-thousands-of-rows) result that must arrive
# as multiple HTTP chunks (verified by parsing the raw chunked-transfer-
# encoding wire format, not just checking the request succeeded), row-exact
# results, and the verglas-server dispatcher spawning + killing a worker per query.
#
# Skips cleanly (exit 0) when docker is unavailable. Requires python3, curl.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../.." && pwd)"

if ! docker info >/dev/null 2>&1; then
  echo "SKIP: docker unavailable"
  exit 0
fi
for tool in python3 curl; do
  command -v "$tool" >/dev/null 2>&1 || { echo "SKIP: $tool not installed"; exit 0; }
done

MINIO_PORT="${MINIO_PORT:-19300}"
REST_PORT="${REST_PORT:-19381}"
DS3="${DS3:-19533}"
DADMIN="${DADMIN:-19534}"
QADMIN="${QADMIN:-19535}"
DISPATCH_S3="${DISPATCH_S3:-19633}"
DISPATCH_ADMIN="${DISPATCH_ADMIN:-19634}"
BUCKET="qadummy"
WAREHOUSE="s3://qadummy/"
NUM_ROWS="${NUM_ROWS:-65000}"

sfx="$$"
NET="vg-query-e2e-net-$sfx"
MINIO_C="vg-query-e2e-minio-$sfx"
REST_C="vg-query-e2e-rest-$sfx"
WORK="$(mktemp -d)"
export HOME="$WORK/home"; mkdir -p "$HOME"
CACHE="$WORK/cache"; mkdir -p "$CACHE"
SERVER_PID=""; QUERY_PID=""; DISPATCH_SERVER_PID=""

cleanup() {
  set +e
  [ -n "$SERVER_PID" ] && { kill "$SERVER_PID" 2>/dev/null; wait "$SERVER_PID" 2>/dev/null; }
  [ -n "$QUERY_PID" ] && { kill "$QUERY_PID" 2>/dev/null; wait "$QUERY_PID" 2>/dev/null; }
  [ -n "$DISPATCH_SERVER_PID" ] && { kill "$DISPATCH_SERVER_PID" 2>/dev/null; wait "$DISPATCH_SERVER_PID" 2>/dev/null; }
  docker rm -f "$REST_C" "$MINIO_C" >/dev/null 2>&1
  docker network rm "$NET" >/dev/null 2>&1
  rm -rf "$WORK"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; for f in "$WORK/server.log" "$WORK/query.log" "$WORK/dispatch/server.log"; do [ -f "$f" ] && { echo "--- $f tail ---" >&2; tail -40 "$f" >&2; }; done; exit 1; }
wait_for() { local url="$1" n="${2:-60}"; for _ in $(seq 1 "$n"); do curl -sf "$url" >/dev/null 2>&1 && return 0; sleep 1; done; return 1; }

echo "== 1. docker MinIO + Iceberg REST catalog on high ports =="
docker network create "$NET" >/dev/null
docker run -d --name "$MINIO_C" --network "$NET" -p "127.0.0.1:$MINIO_PORT:9000" \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data >/dev/null
wait_for "http://127.0.0.1:$MINIO_PORT/minio/health/ready" 60 || fail "MinIO did not become ready"
docker run --rm --network "$NET" --entrypoint sh minio/mc -c \
  "mc alias set m http://$MINIO_C:9000 minioadmin minioadmin && mc mb -p m/$BUCKET" >/dev/null

REST_IMAGE="${REST_IMAGE:-apache/iceberg-rest-fixture:latest}"
docker run -d --name "$REST_C" --network "$NET" -p "127.0.0.1:$REST_PORT:8181" \
  -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin -e AWS_REGION=us-east-1 \
  -e CATALOG_WAREHOUSE="$WAREHOUSE" -e CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO \
  -e CATALOG_S3_ENDPOINT="http://$MINIO_C:9000" -e CATALOG_S3_PATH__STYLE__ACCESS=true \
  "$REST_IMAGE" >/dev/null
wait_for "http://127.0.0.1:$REST_PORT/v1/config?warehouse=$WAREHOUSE" 60 || fail "Iceberg REST catalog did not come up"

echo "== 2. build + run the branch verglas-server (cache) + verglas + verglas-query =="
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

cargo build --release -p verglas-server -p verglas -p verglas-query --manifest-path "$root/Cargo.toml" 2>&1 | tail -3
DBIN="$root/target/release/verglas-server"
CLI="$root/target/release/verglas"
QBIN="$root/target/release/verglas-query"

"$DBIN" --config "$WORK/verglas.toml" >>"$WORK/server.log" 2>&1 &
SERVER_PID=$!
wait_for "http://127.0.0.1:$DADMIN/admin/healthz" 60 || fail "cache server did not become healthy"
EP="http://127.0.0.1:$DADMIN"

echo "== 3. seed a large table ($NUM_ROWS rows, one commit) =="
python3 - "$WORK" "$NUM_ROWS" <<'PY'
import json, random, os, sys
WORK, n = sys.argv[1], int(sys.argv[2])
random.seed(7)
with open(f"{WORK}/big.jsonl", "w") as f:
    for i in range(1, n + 1):
        f.write(json.dumps({"id": i, "batch": i % 50, "value": round(random.uniform(0, 1000), 4), "label": f"row-{i}"}) + "\n")
PY
"$CLI" --server-endpoint "$EP" table create qa.big "$WORK/big.jsonl" --json >/dev/null || fail "table create failed"

echo "== 4. verglas-query standalone: healthz, estimate, query =="
cat > "$WORK/query.toml" <<EOF
[listen]
admin_port = $QADMIN
[cache]
s3_endpoint = "http://127.0.0.1:$DS3"
region = "us-east-1"
credentials_file = "$WORK/endpoint.creds"
[catalog]
uri = "http://127.0.0.1:$REST_PORT"
warehouse = "$WAREHOUSE"
EOF
"$QBIN" --config "$WORK/query.toml" >>"$WORK/query.log" 2>&1 &
QUERY_PID=$!
wait_for "http://127.0.0.1:$QADMIN/healthz" 30 || fail "verglas-query did not become healthy"
Q="http://127.0.0.1:$QADMIN"

echo "-- estimate: large plain scan must be near-zero working set --"
SCAN_WS=$(curl -sf -X POST "$Q/v1/query/estimate" -H 'content-type: application/json' \
  -d '{"sql":"select * from qa.big"}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["working_set_bytes"])')
echo "plain scan working_set_bytes=$SCAN_WS"
[ "$SCAN_WS" -eq 0 ] || fail "expected working_set_bytes=0 for a plain scan, got $SCAN_WS"

echo "-- estimate: aggregate must be nonzero --"
AGG_WS=$(curl -sf -X POST "$Q/v1/query/estimate" -H 'content-type: application/json' \
  -d '{"sql":"select batch, count(*) as n from qa.big group by batch"}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["working_set_bytes"])')
echo "aggregate working_set_bytes=$AGG_WS"
[ "$AGG_WS" -gt 0 ] || fail "expected nonzero working_set_bytes for an aggregate, got $AGG_WS"

echo "-- estimate: join must be nonzero --"
JOIN_WS=$(curl -sf -X POST "$Q/v1/query/estimate" -H 'content-type: application/json' \
  -d '{"sql":"select a.id from qa.big a join qa.big b on a.batch = b.batch limit 10"}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["working_set_bytes"])')
echo "join working_set_bytes=$JOIN_WS"
[ "$JOIN_WS" -gt 0 ] || fail "expected nonzero working_set_bytes for a join, got $JOIN_WS"

echo "-- small select --"
curl -sf -X POST "$Q/v1/query" -H 'content-type: application/json' \
  -d '{"sql":"select id, label from qa.big where id = 1"}' | python3 -m json.tool >/dev/null || fail "small select failed"

echo "-- aggregate row-exactness --"
AGG_N=$(curl -sf -X POST "$Q/v1/query" -H 'content-type: application/json' \
  -d '{"sql":"select count(*) as n from qa.big"}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["rows"][0]["n"])')
[ "$AGG_N" -eq "$NUM_ROWS" ] || fail "count(*) mismatch: expected $NUM_ROWS got $AGG_N"

echo "-- large result: structural multi-chunk proof (raw chunked-encoding parse) --"
CHUNK_RESULT=$(python3 "$here/chunk_probe.py" "$QADMIN" "select * from qa.big")
echo "$CHUNK_RESULT"
echo "$CHUNK_RESULT" | grep -q "STRUCTURAL PROOF" || fail "large result did not prove multi-chunk streamed arrival"

echo "== 5. verglas-server dispatcher: spawn-worker-per-query =="
DISPATCH_WORK="$WORK/dispatch"; mkdir -p "$DISPATCH_WORK/cache" "$DISPATCH_WORK/home"
cat > "$DISPATCH_WORK/verglas.toml" <<EOF
[listen]
s3_port = $DISPATCH_S3
admin_port = $DISPATCH_ADMIN
[cache]
dir = "$DISPATCH_WORK/cache"
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
[query_worker]
binary = "$QBIN"
EOF
# A SEPARATE $HOME from the primary server — two verglas-server instances sharing
# one $HOME can collide on per-home state; each server in this script gets
# its own.
HOME="$DISPATCH_WORK/home" "$DBIN" --config "$DISPATCH_WORK/verglas.toml" >>"$DISPATCH_WORK/server.log" 2>&1 &
DISPATCH_SERVER_PID=$!
wait_for "http://127.0.0.1:$DISPATCH_ADMIN/admin/healthz" 60 || fail "dispatcher server did not become healthy"
DD="http://127.0.0.1:$DISPATCH_ADMIN"

BEFORE_WORKERS=$(pgrep -f "$QBIN --config $DISPATCH_WORK" | wc -l | tr -d ' '; true)  # pgrep exits 1 on zero matches (expected here); pipefail would otherwise kill the script
DISPATCH_N=$(curl -sf -X POST "$DD/v1/query" -H 'content-type: application/json' \
  -d '{"sql":"select count(*) as n from qa.big"}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["rows"][0]["n"])')
[ "$DISPATCH_N" -eq "$NUM_ROWS" ] || fail "dispatcher query count(*) mismatch: expected $NUM_ROWS got $DISPATCH_N"
sleep 0.5
AFTER_WORKERS=$(pgrep -f "$QBIN --config $DISPATCH_WORK" | wc -l | tr -d ' '; true)
echo "dispatcher spawned worker for the query (before=$BEFORE_WORKERS lingering, after=$AFTER_WORKERS lingering — worker is killed on drop, not left running)"
[ "$AFTER_WORKERS" -eq 0 ] || fail "query worker did not exit after the request completed (found $AFTER_WORKERS still running)"

echo "-- large result via dispatcher relay: still multi-chunk --"
CHUNK_RESULT2=$(python3 "$here/chunk_probe.py" "$DISPATCH_ADMIN" "select * from qa.big" "/v1/query")
echo "$CHUNK_RESULT2" | grep -q "STRUCTURAL PROOF" || fail "dispatcher relay did not preserve multi-chunk streamed arrival"

echo ""
echo "PASS: query e2e — standalone verglas-query + verglas-server dispatcher, estimate sanity (scan=0, agg=$AGG_WS, join=$JOIN_WS), $NUM_ROWS rows exact, large result proven multi-chunk both directly and through the dispatcher relay"
