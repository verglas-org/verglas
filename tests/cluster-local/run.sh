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
    listed=$(aws --endpoint-url "$S3_ENDPOINT" s3api list-objects-v2 \
      --bucket verglas-test --prefix "measure/" --query 'KeyCount' --output text 2>/dev/null || echo 0)
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
  *)
    echo "usage: $0 {up|down|putcount|measure}" >&2; exit 64 ;;
esac
