#!/usr/bin/env bash
# Write-back tier write-path benchmark (#180). Boots a local 3-node dev pod with
# the erasure-coded write-back tier OFF (write-through) and then ON (quorum ack),
# and measures seed-phase PUT p50 and single 32/128 MiB PUT ack latency against
# a live origin, plus a direct-to-origin baseline. It kills only the verglas-server
# children it spawned (matched by its own cache dir) so a benchmark running in
# benchmarks/tpch is never touched.
#
# Requires a live origin: source an .env with AWS_ENDPOINT / AWS_ACCESS_KEY_ID /
# AWS_SECRET_ACCESS_KEY / AWS_REGION before running (never commit it):
#   set -a; source .env; set +a; benchmarks/writeback/run.sh
#
# Output: a results table on stdout and results/<ts>.json.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
BUCKET="${WB_BUCKET:-hyperglas}"
PORT="${WB_PORT:-9333}"          # node 0 S3; admin = +1, then +4 per node
NODES=3
BIG_SAMPLES="${WB_BIG_SAMPLES:-3}"
# Large-object leg (#180): an object bigger than the old 256 MiB DRAM cap, on a
# deliberately small-DRAM / large-NVMe pod. The streaming encoder keeps one
# stripe resident, so write-back must accept it (no DRAM-cap refusal) as long as
# the fragment sub-budget has room. Default 512 MiB.
BIG_MIB="${WB_BIG_MIB:-512}"
BIG_DRAM="${WB_BIG_DRAM:-256MB}"
BIG_CACHE="${WB_BIG_CACHE:-8GB}"
TS="$(date -u +%Y%m%d-%H%M%S)"
RUN_ROOT="$(mktemp -d "/tmp/verglas-wb-bench-${TS}.XXXX")"
RESULTS_DIR="$REPO/benchmarks/writeback/results"
mkdir -p "$RESULTS_DIR"
VERGLAS="$REPO/target/release/verglas"
DEV_PID=""

require() { command -v "$1" >/dev/null 2>&1 || { echo "need $1" >&2; exit 1; }; }
require aws; require python3; require curl
[ -x "$VERGLAS" ] || { echo "build first: cargo build --release -p verglas -p verglas-server" >&2; exit 1; }
: "${AWS_ENDPOINT:?source your .env first}"

now_ms() { python3 -c 'import time;print(int(time.time()*1000))'; }

# Boots the pod for MODE (on|off) into its own cache dir; sets DEV_PID and the
# dev key globals. Waits until all NODES are active in the gossip view.
boot_pod() {
  local mode="$1" cache="$RUN_ROOT/$1"
  local cache_size="${2:-2GB}" dram="${3:-512MB}"
  mkdir -p "$cache"
  local wb=()
  [ "$mode" = on ] && wb=(--writeback --writeback-k 2 --writeback-m 1 --writeback-w 3)
  "$VERGLAS" dev --nodes "$NODES" --port "$PORT" --cache-dir "$cache" \
    --cache-size "$cache_size" --dram "$dram" "${wb[@]+"${wb[@]}"}" >"$cache/dev.log" 2>&1 &
  DEV_PID=$!
  local admin="http://127.0.0.1:$((PORT+1))"
  for _ in $(seq 1 60); do curl -sf "$admin/admin/healthz" >/dev/null 2>&1 && break; sleep 1; done
  # Wait for the gossip view to converge to NODES active members.
  for _ in $(seq 1 60); do
    local n
    n=$(curl -s "$admin/admin/members" | python3 -c 'import sys,json;print(sum(1 for m in json.load(sys.stdin)["members"] if m["state"]=="active"))' 2>/dev/null || echo 0)
    [ "$n" = "$NODES" ] && break
    sleep 1
  done
  KEY=$(grep -m1 'access_key_id=' "$cache/dev.log" | sed -E 's/.*access_key_id=([A-Za-z0-9]+).*/\1/')
  SEC=$(grep -m1 'secret_access_key=' "$cache/dev.log" | sed -E 's/.*secret_access_key=([A-Za-z0-9]+).*/\1/')
}

# Kills only the verglas-server children under this run's cache dir, then the dev
# parent. Never touches another pod.
teardown_pod() {
  [ -n "$DEV_PID" ] && kill "$DEV_PID" 2>/dev/null || true
  sleep 2
  pkill -f "verglas-server.*$RUN_ROOT" 2>/dev/null || true
  sleep 1
  DEV_PID=""
}

# Times a single put-object through an endpoint; echoes milliseconds.
time_put() {
  local endpoint="$1" key="$2" body="$3" ak="${4:-}" sk="${5:-}"
  local t0 t1
  t0=$(now_ms)
  if [ -n "$ak" ]; then
    AWS_ACCESS_KEY_ID="$ak" AWS_SECRET_ACCESS_KEY="$sk" AWS_REGION="${AWS_REGION:-us-east-1}" \
      aws --endpoint-url "$endpoint" s3api put-object --bucket "$BUCKET" --key "$key" --body "$body" >/dev/null 2>&1
  else
    aws --endpoint-url "$endpoint" s3api put-object --bucket "$BUCKET" --key "$key" --body "$body" >/dev/null 2>&1
  fi
  t1=$(now_ms)
  echo $((t1 - t0))
}

# Median of BIG_SAMPLES single PUTs of a file through the node-0 endpoint.
median_put() {
  local endpoint="$1" prefix="$2" body="$3" ak="$4" sk="$5" vals=()
  for i in $(seq 1 "$BIG_SAMPLES"); do
    vals+=("$(time_put "$endpoint" "$prefix-$i.bin" "$body" "$ak" "$sk")")
  done
  printf '%s\n' "${vals[@]}" | sort -n | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]}'
}

# Seed phase: PUT a batch of small objects through the endpoint and one directly
# to the origin, reporting endpoint PUT p50, the direct baseline, and the count.
# Write-back codes opaque objects, so the payload is just random bytes of a
# typical small-data-file size — the Iceberg format is irrelevant to the tier.
# Writes $out with {"write_path":{endpoint_put_p50_ms,direct_put_ms,objects_written}}.
SEED_OBJECTS="${SEED_OBJECTS:-32}"
SEED_OBJECT_BYTES="${SEED_OBJECT_BYTES:-262144}" # 256 KiB
seed_phase() {
  local mode="$1" ep="$2" out="$3"
  local body="$RUN_ROOT/$mode-seed-body.bin" vals=()
  head -c "$SEED_OBJECT_BYTES" /dev/urandom >"$body"
  for i in $(seq 1 "$SEED_OBJECTS"); do
    vals+=("$(time_put "$ep" "bench/wb-seed-$mode/obj-$i.bin" "$body" "$KEY" "$SEC")")
  done
  local p50
  p50=$(printf '%s\n' "${vals[@]}" | sort -n | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]}')
  # Direct-to-origin baseline, when an origin endpoint is configured (ambient
  # AWS creds). Null when unset, so the report simply omits the baseline.
  local direct=null
  [ -n "${AWS_ENDPOINT:-}" ] && direct=$(time_put "$AWS_ENDPOINT" "bench/wb-seed-$mode/direct.bin" "$body")
  python3 - "$p50" "$direct" "$SEED_OBJECTS" >"$out" <<'PY'
import json, sys
p50, direct, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
print(json.dumps({"write_path": {
    "endpoint_put_p50_ms": int(p50),
    "direct_put_ms": None if direct == "null" else int(direct),
    "objects_written": n,
}}))
PY
}

measure() {
  local mode="$1"
  local ep="http://127.0.0.1:$PORT" admin="http://127.0.0.1:$((PORT+1))"
  echo "=== mode=$mode: booting $NODES-node pod on :$PORT ===" >&2
  boot_pod "$mode"
  # Warmup so the tier is in steady state. For ON, keep warming until a quorum
  # ack registers, so we never measure the brief post-boot convergence window.
  head -c 4096 /dev/urandom >"$RUN_ROOT/warmup.bin"
  for i in $(seq 1 8); do
    time_put "$ep" "bench/wb-$mode/warmup-$i.bin" "$RUN_ROOT/warmup.bin" "$KEY" "$SEC" >/dev/null || true
    [ "$mode" = off ] && break
    local q
    q=$(curl -s "$admin/admin/stats" | python3 -c 'import sys,json;print((json.load(sys.stdin).get("writeback") or {}).get("acked_via_quorum",0))' 2>/dev/null || echo 0)
    [ "${q:-0}" -ge 1 ] && break
    sleep 1
  done
  sleep 1

  # Seed-phase PUT p50 through the endpoint vs a direct-to-origin probe.
  local seed_json="$RUN_ROOT/$mode-seed.json"
  seed_phase "$mode" "$ep" "$seed_json"

  # Single 32/128 MiB PUT ack through the endpoint (median of BIG_SAMPLES).
  local p32 p128
  p32=$(median_put "$ep" "bench/wb-$mode/put32" /tmp/verglas-wb-32.bin "$KEY" "$SEC")
  p128=$(median_put "$ep" "bench/wb-$mode/put128" /tmp/verglas-wb-128.bin "$KEY" "$SEC")

  local stats
  stats=$(curl -s "$admin/admin/stats" | python3 -c 'import sys,json;print(json.dumps(json.load(sys.stdin).get("writeback")))' 2>/dev/null || echo null)
  teardown_pod
  # Emit one JSON line for this mode.
  python3 - "$mode" "$seed_json" "$p32" "$p128" "$stats" <<'PY'
import json,sys
mode,seed_path,p32,p128,stats=sys.argv[1],sys.argv[2],int(sys.argv[3]),int(sys.argv[4]),sys.argv[5]
seed={}
try:
    seed=json.load(open(seed_path))
except Exception:
    pass
wp=seed.get("write_path",{}) if isinstance(seed,dict) else {}
out={"mode":mode,
     "seed_endpoint_put_p50_ms":wp.get("endpoint_put_p50_ms"),
     "seed_direct_put_ms":wp.get("direct_put_ms"),
     "seed_objects_written":wp.get("objects_written"),
     "put32_ms":p32,"put128_ms":p128,
     "writeback_counters":json.loads(stats) if stats!="null" else None}
print(json.dumps(out))
PY
}

# Large-object leg (#180): boots an ON pod with small DRAM and large NVMe, then
# PUTs an object bigger than the old 256 MiB cap. Confirms the write is acked via
# quorum (write-back accepted it — the streaming encoder means the object size is
# not a DRAM bound). Echoes one JSON line.
large_object_leg() {
  local ep="http://127.0.0.1:$PORT" admin="http://127.0.0.1:$((PORT+1))"
  echo "=== large-object leg: ${BIG_MIB}MiB PUT on small-DRAM($BIG_DRAM)/large-NVMe($BIG_CACHE) pod ===" >&2
  boot_pod on "$BIG_CACHE" "$BIG_DRAM"
  # Warm until a quorum ack registers so we are in write-back steady state.
  head -c 4096 /dev/urandom >"$RUN_ROOT/big-warmup.bin"
  for _ in $(seq 1 8); do
    time_put "$ep" "bench/wb-big/warmup.bin" "$RUN_ROOT/big-warmup.bin" "$KEY" "$SEC" >/dev/null || true
    local q
    q=$(curl -s "$admin/admin/stats" | python3 -c 'import sys,json;print((json.load(sys.stdin).get("writeback") or {}).get("acked_via_quorum",0))' 2>/dev/null || echo 0)
    [ "${q:-0}" -ge 1 ] && break
    sleep 1
  done
  local q_before q_after
  q_before=$(curl -s "$admin/admin/stats" | python3 -c 'import sys,json;print((json.load(sys.stdin).get("writeback") or {}).get("acked_via_quorum",0))' 2>/dev/null || echo 0)
  local big_ms
  big_ms=$(time_put "$ep" "bench/wb-big/big-${BIG_MIB}.bin" /tmp/verglas-wb-big.bin "$KEY" "$SEC")
  q_after=$(curl -s "$admin/admin/stats" | python3 -c 'import sys,json;print((json.load(sys.stdin).get("writeback") or {}).get("acked_via_quorum",0))' 2>/dev/null || echo 0)
  local stats
  stats=$(curl -s "$admin/admin/stats" | python3 -c 'import sys,json;print(json.dumps(json.load(sys.stdin).get("writeback")))' 2>/dev/null || echo null)
  teardown_pod
  python3 - "$BIG_MIB" "$BIG_DRAM" "$BIG_CACHE" "$big_ms" "$q_before" "$q_after" "$stats" <<'PY'
import json,sys
mib,dram,cache,ms,qb,qa,stats=sys.argv[1],sys.argv[2],sys.argv[3],int(sys.argv[4]),int(sys.argv[5]),int(sys.argv[6]),sys.argv[7]
out={"object_mib":int(mib),"dram":dram,"nvme":cache,"put_ms":ms,
     "acked_via_quorum_before":qb,"acked_via_quorum_after":qa,
     "wrote_back":qa>qb,
     "writeback_counters":json.loads(stats) if stats!="null" else None}
print(json.dumps(out))
PY
}

# Direct-to-origin single PUT baselines (independent of the pod).
direct_baseline() {
  local d32 d128
  d32=$(time_put "$AWS_ENDPOINT" "bench/wb-direct/put32.bin" /tmp/verglas-wb-32.bin)
  d128=$(time_put "$AWS_ENDPOINT" "bench/wb-direct/put128.bin" /tmp/verglas-wb-128.bin)
  echo "{\"direct_put32_ms\":$d32,\"direct_put128_ms\":$d128}"
}

cleanup_origin() {
  aws --endpoint-url "$AWS_ENDPOINT" s3 rm "s3://$BUCKET/bench/wb-seed-off/" --recursive >/dev/null 2>&1 || true
  aws --endpoint-url "$AWS_ENDPOINT" s3 rm "s3://$BUCKET/bench/wb-seed-on/" --recursive >/dev/null 2>&1 || true
  aws --endpoint-url "$AWS_ENDPOINT" s3 rm "s3://$BUCKET/bench/wb-off/" --recursive >/dev/null 2>&1 || true
  aws --endpoint-url "$AWS_ENDPOINT" s3 rm "s3://$BUCKET/bench/wb-on/" --recursive >/dev/null 2>&1 || true
  aws --endpoint-url "$AWS_ENDPOINT" s3 rm "s3://$BUCKET/bench/wb-big/" --recursive >/dev/null 2>&1 || true
  aws --endpoint-url "$AWS_ENDPOINT" s3 rm "s3://$BUCKET/bench/wb-direct/" --recursive >/dev/null 2>&1 || true
}

trap 'teardown_pod' EXIT
dd if=/dev/urandom of=/tmp/verglas-wb-32.bin bs=1m count=32 2>/dev/null
dd if=/dev/urandom of=/tmp/verglas-wb-128.bin bs=1m count=128 2>/dev/null
dd if=/dev/urandom of=/tmp/verglas-wb-big.bin bs=1m count="$BIG_MIB" 2>/dev/null

OFF=$(measure off)
ON=$(measure on)
BIG=$(large_object_leg)
DIRECT=$(direct_baseline)
cleanup_origin

OUT="$RESULTS_DIR/$TS.json"
python3 - "$OFF" "$ON" "$BIG" "$DIRECT" >"$OUT" <<'PY'
import json,sys
off,on,big,direct=json.loads(sys.argv[1]),json.loads(sys.argv[2]),json.loads(sys.argv[3]),json.loads(sys.argv[4])
print(json.dumps({"off":off,"on":on,"large_object":big,"direct":direct},indent=2))
PY
echo
echo "results -> $OUT"
python3 - "$OUT" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]))
off,on,big,direct=d["off"],d["on"],d["large_object"],d["direct"]
def f(x): return "-" if x is None else f"{x:.0f}"
print("write path, 3-node pod, live OCI origin (ms):\n")
print(f"{'metric':<28}{'OFF (write-through)':>22}{'ON (write-back)':>18}")
print(f"{'seed PUT p50':<28}{f(off['seed_endpoint_put_p50_ms']):>22}{f(on['seed_endpoint_put_p50_ms']):>18}")
print(f"{'single 32 MiB PUT ack':<28}{f(off['put32_ms']):>22}{f(on['put32_ms']):>18}")
print(f"{'single 128 MiB PUT ack':<28}{f(off['put128_ms']):>22}{f(on['put128_ms']):>18}")
print(f"\ndirect-to-origin baseline: 32MiB={f(direct['direct_put32_ms'])}ms 128MiB={f(direct['direct_put128_ms'])}ms")
print(f"seed direct probe: OFF={f(off['seed_direct_put_ms'])}ms ON={f(on['seed_direct_put_ms'])}ms")
print(f"\nON write-back counters: {on['writeback_counters']}")
print(f"\nlarge-object leg (streaming encoder, no DRAM cap):")
print(f"  {big['object_mib']} MiB PUT on DRAM={big['dram']} NVMe={big['nvme']}: {f(big['put_ms'])} ms, "
      f"wrote_back={big['wrote_back']} (quorum acks {big['acked_via_quorum_before']}->{big['acked_via_quorum_after']})")
print(f"  counters: {big['writeback_counters']}")
PY
