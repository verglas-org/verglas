#!/usr/bin/env bash
#
# Compaction end-to-end: many small commits -> bin-pack compaction -> verify
# file-count collapse, snapshot expiry (keep-last-10), metadata.json shrink,
# exact row preservation (count(*), not snapshot summaries), the table still
# serving reads, and a second pass being a cheap no-op.
#
# Stands up docker MinIO + an Iceberg REST catalog on HIGH host ports (never
# the live 8333/8334 daemon, never a real tenant), builds and runs the branch
# verglasd + verglas against them, seeds NUM_BATCHES small appends (the real
# pathology: hundreds of tiny commits), then compacts twice via the daemon's
# manual compact route (`verglas table compact` → POST /admin/compact).
#
# Skips cleanly (exit 0) when docker is unavailable. Requires python3 and curl.
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

MINIO_PORT="${MINIO_PORT:-19200}"
REST_PORT="${REST_PORT:-19281}"
DS3="${DS3:-19433}"
DADMIN="${DADMIN:-19434}"
BUCKET="qadummy"
WAREHOUSE="s3://qadummy/"
NUM_BATCHES="${NUM_BATCHES:-220}"

sfx="$$"
NET="vg-compact-e2e-net-$sfx"
MINIO_C="vg-compact-e2e-minio-$sfx"
REST_C="vg-compact-e2e-rest-$sfx"
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

wait_for() { local url="$1" n="${2:-60}"; for _ in $(seq 1 "$n"); do curl -sf "$url" >/dev/null 2>&1 && return 0; sleep 1; done; return 1; }

echo "== 1. docker MinIO + Iceberg REST catalog on high ports =="
docker network create "$NET" >/dev/null
docker run -d --name "$MINIO_C" --network "$NET" -p "127.0.0.1:$MINIO_PORT:9000" \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data >/dev/null
wait_for "http://127.0.0.1:$MINIO_PORT/minio/health/ready" 60 || fail "MinIO did not become ready"
docker run --rm --network "$NET" --entrypoint sh minio/mc -c \
  "mc alias set m http://$MINIO_C:9000 minioadmin minioadmin && mc mb -p m/$BUCKET" >/dev/null \
  || fail "creating the warehouse bucket failed"

REST_IMAGE="${REST_IMAGE:-apache/iceberg-rest-fixture:latest}"
docker run -d --name "$REST_C" --network "$NET" -p "127.0.0.1:$REST_PORT:8181" \
  -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin -e AWS_REGION=us-east-1 \
  -e CATALOG_WAREHOUSE="$WAREHOUSE" \
  -e CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO \
  -e CATALOG_S3_ENDPOINT="http://$MINIO_C:9000" \
  -e CATALOG_S3_PATH__STYLE__ACCESS=true \
  "$REST_IMAGE" >/dev/null
wait_for "http://127.0.0.1:$REST_PORT/v1/config?warehouse=$WAREHOUSE" 60 || fail "Iceberg REST catalog did not come up"

echo "== 2. build + run the branch verglasd + verglas on high ports =="
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

cargo build --release -p verglasd -p verglas --manifest-path "$root/Cargo.toml" 2>&1 | tail -3
DBIN="$root/target/release/verglasd"
CLI="$root/target/release/verglas"

"$DBIN" --config "$WORK/verglas.toml" >>"$WORK/daemon.log" 2>&1 &
DAEMON_PID=$!
wait_for "http://127.0.0.1:$DADMIN/admin/healthz" 60 || fail "daemon did not become healthy"
echo "daemon pid $DAEMON_PID on admin:$DADMIN s3:$DS3"
EP="http://127.0.0.1:$DADMIN"

echo "== 3. seed a table with $NUM_BATCHES small commits (the pathology) =="
python3 - "$WORK" "$NUM_BATCHES" <<'PY'
import json, random, os, sys
WORK, num_batches = sys.argv[1], int(sys.argv[2])
random.seed(42)
outdir = f"{WORK}/batches"
os.makedirs(outdir, exist_ok=True)
row_id = 0
for b in range(num_batches):
    n = random.randint(200, 400)
    with open(f"{outdir}/batch_{b:04d}.jsonl", "w") as f:
        for _ in range(n):
            row_id += 1
            f.write(json.dumps({"id": row_id, "batch": b, "value": round(random.uniform(0, 1000), 4), "label": f"row-{row_id}"}) + "\n")
print(f"planned {row_id} rows across {num_batches} batches", file=sys.stderr)
PY

"$CLI" --daemon-endpoint "$EP" table create qa.commits "$WORK/batches/batch_0000.jsonl" --json >/dev/null \
  || fail "table create failed"
for f in "$WORK"/batches/batch_*.jsonl; do
  [ "$f" = "$WORK/batches/batch_0000.jsonl" ] && continue
  "$CLI" --daemon-endpoint "$EP" table append qa.commits "$f" --json >/dev/null || fail "append $f failed"
done

# Robust metadata-size lookup: retries mc stat (tabular, not --json — mc's
# JSON mode occasionally emits an update-check line ahead of the result,
# which breaks strict JSON parsing) up to 3 times, and converts the human
# unit MinIO prints (B/KiB/MiB/GiB) to a plain byte integer.
mc_object_size_bytes() {
  local key="$1"
  local out size unit
  for _ in 1 2 3; do
    out="$(docker run --rm --network "$NET" --entrypoint sh minio/mc -c \
      "mc alias set m http://$MINIO_C:9000 minioadmin minioadmin >/dev/null 2>&1 && mc stat m/$BUCKET/$key" 2>/dev/null \
      | grep -E '^Size' || true)"
    if [ -n "$out" ]; then
      size="$(echo "$out" | awk -F': ' '{print $2}' | awk '{print $1}')"
      unit="$(echo "$out" | awk -F': ' '{print $2}' | awk '{print $2}')"
      python3 -c "
u = '$unit'.strip()
s = float('$size')
mult = {'B': 1, 'KiB': 1024, 'MiB': 1024**2, 'GiB': 1024**3}.get(u, 1)
print(int(s * mult))
"
      return 0
    fi
    sleep 1
  done
  echo "0"
  return 1
}

echo "== 4. before-compaction stats =="
BEFORE="$("$CLI" --daemon-endpoint "$EP" table show qa.commits --json)"
echo "$BEFORE" | python3 -m json.tool
BEFORE_FILES=$(echo "$BEFORE" | python3 -c 'import json,sys;print(json.load(sys.stdin)["file_count"])')
BEFORE_ROWS=$(echo "$BEFORE" | python3 -c 'import json,sys;print(json.load(sys.stdin)["row_count"])')
[ "$BEFORE_FILES" -ge "$NUM_BATCHES" ] || fail "expected >= $NUM_BATCHES data files before compaction, got $BEFORE_FILES"

META_LOC=$(curl -sf "http://127.0.0.1:$REST_PORT/v1/namespaces/qa/tables/commits" | python3 -c "import json,sys;print(json.load(sys.stdin)['metadata-location'])")
BEFORE_META_BYTES="$(mc_object_size_bytes "${META_LOC#s3://$BUCKET/}")"
[ "$BEFORE_META_BYTES" -gt 0 ] || fail "could not read metadata.json size before compaction"
echo "metadata.json before: $BEFORE_META_BYTES bytes"

echo "== 5. compact (daemon: verglas table compact → POST /admin/compact) =="
COMPACT_JSON="$("$CLI" --daemon-endpoint "$EP" table compact --json)"
echo "$COMPACT_JSON" | python3 -c "import json,sys
d = json.load(sys.stdin)
c = d['compacted'][0]
print('groups_committed=%s input_files=%s output_files=%s input_records=%s output_records=%s snapshots_expired=%s' % (c['groups_committed'], c['input_data_files'], c['output_data_files'], c['input_records'], c['output_records'], c['snapshots_expired']))
"
COMPACT_FAILURES=$(echo "$COMPACT_JSON" | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["failures"]))')
[ "$COMPACT_FAILURES" -eq 0 ] || fail "compaction reported $COMPACT_FAILURES failure(s)"

echo "== 6. after-compaction stats + row-exactness (count(*), not snapshot summary) =="
AFTER="$("$CLI" --daemon-endpoint "$EP" table show qa.commits --json)"
echo "$AFTER" | python3 -m json.tool
AFTER_FILES=$(echo "$AFTER" | python3 -c 'import json,sys;print(json.load(sys.stdin)["file_count"])')
AFTER_ROWS=$(echo "$AFTER" | python3 -c 'import json,sys;print(json.load(sys.stdin)["row_count"])')
[ "$AFTER_FILES" -lt "$BEFORE_FILES" ] || fail "file count did not collapse: before=$BEFORE_FILES after=$AFTER_FILES"
[ "$AFTER_ROWS" -eq "$BEFORE_ROWS" ] || fail "row_count changed: before=$BEFORE_ROWS after=$AFTER_ROWS"

META_LOC2=$(curl -sf "http://127.0.0.1:$REST_PORT/v1/namespaces/qa/tables/commits" | python3 -c "import json,sys;print(json.load(sys.stdin)['metadata-location'])")
AFTER_META_BYTES="$(mc_object_size_bytes "${META_LOC2#s3://$BUCKET/}")"
[ "$AFTER_META_BYTES" -gt 0 ] || fail "could not read metadata.json size after compaction"
echo "metadata.json after: $AFTER_META_BYTES bytes (before: $BEFORE_META_BYTES)"
[ "$AFTER_META_BYTES" -lt "$BEFORE_META_BYTES" ] || fail "metadata.json did not shrink: before=$BEFORE_META_BYTES after=$AFTER_META_BYTES"

HISTORY_COUNT=$("$CLI" --daemon-endpoint "$EP" table history qa.commits --json | python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d.get("snapshots", d if isinstance(d, list) else [])))')
echo "snapshot count after: $HISTORY_COUNT (keep-last-10 floor)"
[ "$HISTORY_COUNT" -le 10 ] || fail "expected <=10 retained snapshots (keep-last-10), got $HISTORY_COUNT"

COUNT_STAR="$("$CLI" --daemon-endpoint "$EP" query "select count(*) as n from qa.commits" --json | python3 -c 'import json,sys;print(json.load(sys.stdin)["rows"][0]["n"])')"
echo "count(*) via query engine (not snapshot summary): $COUNT_STAR"
[ "$COUNT_STAR" -eq "$BEFORE_ROWS" ] || fail "count(*) mismatch: expected $BEFORE_ROWS got $COUNT_STAR"

echo "== 7. second compaction pass must be a cheap no-op (verglas table compact CLI path) =="
PASS2_START=$(python3 -c 'import time;print(time.time())')
PASS2_JSON="$("$CLI" --daemon-endpoint "$EP" table compact --json)"
PASS2_ELAPSED=$(python3 -c "import time;print(time.time() - $PASS2_START)")
echo "$PASS2_JSON"
PASS2_GROUPS=$(echo "$PASS2_JSON" | python3 -c 'import json,sys;print(json.load(sys.stdin)["groups_committed"])')
echo "pass 2: groups_committed=$PASS2_GROUPS elapsed=${PASS2_ELAPSED}s"
[ "$PASS2_GROUPS" -eq 0 ] || fail "second pass was not a no-op: groups_committed=$PASS2_GROUPS"

echo "== 8. table still serves reads after both passes =="
curl -sf -X POST "$EP/v1/query" -H 'content-type: application/json' \
  -d '{"sql":"select id, label from qa.commits order by id limit 3"}' | python3 -m json.tool >/dev/null \
  || fail "table did not serve a read after compaction"

echo ""
echo "PASS: compaction e2e — files ${BEFORE_FILES}->${AFTER_FILES}, snapshots ${NUM_BATCHES}+1->${HISTORY_COUNT}, metadata.json ${BEFORE_META_BYTES}->${AFTER_META_BYTES} bytes, rows exact at ${BEFORE_ROWS}, 2nd pass no-op in ${PASS2_ELAPSED}s"
