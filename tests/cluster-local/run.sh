#!/usr/bin/env bash
# Local four-node evaluation harness for the offload work in #164.
#
#   ./run.sh up        build and start MinIO + 4 nodes, wait for readiness
#   ./run.sh down      stop and remove everything including volumes
#   ./run.sh putcount  print the origin PutObject count MinIO has served
#   ./run.sh measure   the frozen evaluator: 1000 x 4 KiB, reports PUTs
#
# The PUT count comes from MinIO's own Prometheus counter, not from Verglas.
# That is the point: the origin is the independent witness.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
COMPOSE=(docker compose -f docker-compose.yml)

S3_ENDPOINT=http://127.0.0.1:18333
MINIO_METRICS=http://127.0.0.1:19000/minio/v2/metrics/cluster
export AWS_ACCESS_KEY_ID=verglas-engine
export AWS_SECRET_ACCESS_KEY=verglas-engine-secret
export AWS_DEFAULT_REGION=us-east-1

# Objects to write and their size. The acceptance criterion in #164 is that the
# resulting PUT count is bounded by total_bytes / size_limit + 1.
COUNT=${COUNT:-1000}
OBJECT_BYTES=${OBJECT_BYTES:-4096}
# Client upload concurrency. The AWS CLI defaults to 10, which overruns the
# ring's peer-RPC timeouts on Docker Desktop's VM network and fails the write
# quorum. 4 is sustained. This is a property of the harness host, not of the
# code under test, but it is part of the frozen protocol so every candidate is
# measured under the same load.
CONCURRENCY=${CONCURRENCY:-4}

# Waits until the origin PUT counter stops moving, so the measurement reads a
# steady state instead of a moving target. A fixed sleep silently rewards a
# candidate that defers uploads past the window; this does not.
quiesce() {
  local prev=-1 stable=0 now
  for _ in $(seq 1 "${QUIESCE_MAX_POLLS:-120}"); do
    now=$(putcount)
    if [ "$now" = "$prev" ]; then
      stable=$(( stable + 1 ))
      [ "$stable" -ge "${QUIESCE_STABLE:-4}" ] && return 0
    else
      stable=0
    fi
    prev=$now
    sleep "${QUIESCE_INTERVAL:-5}"
  done
  echo "WARNING: origin PUT counter never quiesced" >&2
  return 1
}

putcount() {
  # minio_s3_requests_total is labelled by api; the label is lowercase.
  curl -sf "$MINIO_METRICS" \
    | awk '/^minio_s3_requests_total\{.*api="putobject".*\}/ {s+=$NF} END {printf "%d\n", s+0}'
}

case "${1:-}" in
  up)
    "${COMPOSE[@]}" up -d --build
    echo "waiting for the four S3 endpoints..."
    for port in 18333 18343 18353 18363; do
      for _ in $(seq 1 60); do
        if curl -sf -o /dev/null "http://127.0.0.1:$port/" 2>/dev/null \
           || curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/" | grep -qE '^(200|403|404)$'; then
          echo "  :$port ready"; break
        fi
        sleep 2
      done
    done
    "${COMPOSE[@]}" ps
    ;;
  down)
    "${COMPOSE[@]}" down -v --remove-orphans
    ;;
  putcount)
    putcount
    ;;
  measure)
    before=$(putcount)
    # One local tree of COUNT objects, uploaded by a single client process.
    # Per-object `aws s3api` invocations spend more time on process startup
    # than on the request and make the write window meaningless.
    src=$(mktemp -d)
    for i in $(seq 1 "$COUNT"); do head -c "$OBJECT_BYTES" /dev/urandom > "$src/obj-$i"; done
    aws configure set default.s3.max_concurrent_requests "$CONCURRENCY"
    start=$(date +%s)
    aws --endpoint-url "$S3_ENDPOINT" s3 cp "$src" "s3://verglas-test/measure/" \
      --recursive --only-show-errors
    wrote=$(date +%s)
    # Wait for steady state rather than a fixed window.
    quiesced=yes
    quiesce || quiesced=no
    after=$(putcount)
    # G5 has two independent halves. LIST must enumerate the written keys, and
    # GET must return their bytes. A packing implementation can pass one and
    # fail the other, so they are reported separately.
    # Count via a paginated listing, not KeyCount. S3 omits KeyCount entirely
    # when IsTruncated is true, so querying it returns None on any listing over
    # one page and reads as zero keys — a false negative that nearly rejected a
    # working candidate.
    listed=$(aws --endpoint-url "$S3_ENDPOINT" s3 ls "s3://verglas-test/measure/" \
      --recursive 2>/dev/null | wc -l | tr -d ' ')
    out=$(mktemp -d)
    get_mismatched=0
    for i in $(seq 1 "$COUNT"); do
      if aws --endpoint-url "$S3_ENDPOINT" s3api get-object --bucket verglas-test \
           --key "measure/obj-$i" "$out/obj-$i" >/dev/null 2>&1; then
        cmp -s "$src/obj-$i" "$out/obj-$i" || get_mismatched=$((get_mismatched+1))
      else
        get_mismatched=$((get_mismatched+1))
      fi
    done
    total_bytes=$(( COUNT * OBJECT_BYTES ))
    echo "objects_written=$COUNT"
    echo "client_concurrency=$CONCURRENCY"
    echo "object_bytes=$OBJECT_BYTES"
    echo "total_bytes=$total_bytes"
    echo "origin_put_before=$before"
    echo "origin_put_after=$after"
    echo "origin_put_delta=$(( after - before ))"
    echo "client_write_seconds=$(( wrote - start ))"
    echo "quiesced=$quiesced"
    echo "list_key_count=$listed"
    echo "get_mismatched=$get_mismatched"
    rm -rf "$src" "$out"
    ;;
  throughput)
    # M3. Same object set written twice: once through Verglas, once straight to
    # the origin. The write-back path exists to acknowledge on an EC quorum
    # instead of waiting for an origin PUT, so the ratio must exceed 1.0.
    src=$(mktemp -d)
    for i in $(seq 1 "$COUNT"); do head -c "$OBJECT_BYTES" /dev/urandom > "$src/obj-$i"; done
    aws configure set default.s3.max_concurrent_requests "$CONCURRENCY"
    total_bytes=$(( COUNT * OBJECT_BYTES ))

    v_start=$(date +%s%N)
    aws --endpoint-url "$S3_ENDPOINT" s3 cp "$src" "s3://verglas-test/thr-verglas/" \
      --recursive --only-show-errors
    v_ns=$(( $(date +%s%N) - v_start ))

    d_start=$(date +%s%N)
    AWS_ACCESS_KEY_ID=verglas-local AWS_SECRET_ACCESS_KEY=verglas-local-secret \
      aws --endpoint-url http://127.0.0.1:19000 s3 cp "$src" "s3://verglas-test/thr-direct/" \
      --recursive --only-show-errors
    d_ns=$(( $(date +%s%N) - d_start ))

    rm -rf "$src"
    # python, not awk: the shell quoting around awk's BEGIN block did not
    # survive and silently produced empty metrics.
    python3 - "$COUNT" "$OBJECT_BYTES" "$v_ns" "$d_ns" <<'PYEOF'
import sys
count, obj, v_ns, d_ns = (int(x) for x in sys.argv[1:5])
total = count * obj
v_s, d_s = v_ns / 1e9, d_ns / 1e9
print(f"objects={count} object_bytes={obj} total_bytes={total}")
print(f"verglas_seconds={v_s:.3f}")
print(f"direct_seconds={d_s:.3f}")
print(f"verglas_mib_s={(total/1048576)/v_s:.2f}")
print(f"direct_mib_s={(total/1048576)/d_s:.2f}")
print(f"throughput_ratio={d_s/v_s:.3f}")
PYEOF
    ;;
  *)
    echo "usage: $0 {up|down|putcount|measure|throughput}" >&2; exit 64 ;;
esac
