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
    payload=$(mktemp); head -c "$OBJECT_BYTES" /dev/urandom > "$payload"
    start=$(date +%s)
    for i in $(seq 1 "$COUNT"); do
      aws --endpoint-url "$S3_ENDPOINT" s3api put-object \
        --bucket verglas-test --key "measure/obj-$i" --body "$payload" >/dev/null
    done
    wrote=$(date +%s)
    # Give the drain a bounded window to finish before reading the counter.
    sleep "${DRAIN_WAIT:-30}"
    after=$(putcount)
    rm -f "$payload"
    total_bytes=$(( COUNT * OBJECT_BYTES ))
    echo "objects_written=$COUNT"
    echo "object_bytes=$OBJECT_BYTES"
    echo "total_bytes=$total_bytes"
    echo "origin_put_before=$before"
    echo "origin_put_after=$after"
    echo "origin_put_delta=$(( after - before ))"
    echo "client_write_seconds=$(( wrote - start ))"
    ;;
  *)
    echo "usage: $0 {up|down|putcount|measure}" >&2; exit 64 ;;
esac
