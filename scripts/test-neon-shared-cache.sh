#!/usr/bin/env bash
# Container-level smoke for a four-node Verglas ring. It verifies the exact
# Neon page protocol, hit accounting, and quorum write-back with one member down.
set -euo pipefail

image=${VERGLAS_CACHE_NODE_IMAGE:-codex/verglas-cache-node:shared-page}
neon_image=${VERGLAS_NEON_STACK_IMAGE:-}
network="verglas-shared-cache-$RANDOM"
network_octet=$((RANDOM % 180 + 40))
subnet="172.30.${network_octet}.0/24"
work=$(mktemp -d)
nodes=()
neon_node=""

cleanup() {
  for node in ${nodes[@]-}; do docker rm -f "$node" >/dev/null 2>&1 || true; done
  [[ -z "$neon_node" ]] || docker rm -f "$neon_node" >/dev/null 2>&1 || true
  docker rm -f "$network-minio" >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

docker network create --subnet "$subnet" "$network" >/dev/null
docker run -d --name "$network-minio" --network "$network" --ip "172.30.${network_octet}.2" \
  -e MINIO_ROOT_USER=test -e MINIO_ROOT_PASSWORD=testsecret \
  minio/minio:latest server /data >/dev/null

for _ in $(seq 1 60); do
  if docker run --rm --network "$network" minio/mc:latest \
    alias set origin "http://$network-minio:9000" test testsecret >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker run --rm --network "$network" --entrypoint sh minio/mc:latest -c \
  "mc alias set origin http://$network-minio:9000 test testsecret >/dev/null && mc mb --ignore-existing origin/wal-test" >/dev/null

peers="node-0=172.30.${network_octet}.10:8336,node-1=172.30.${network_octet}.11:8336,node-2=172.30.${network_octet}.12:8336,node-3=172.30.${network_octet}.13:8336"
for index in 0 1 2 3; do
  node_dir="$work/node-$index"
  mkdir -p "$node_dir/cache"
  printf '%s\n' '[default]' 'aws_access_key_id = test' \
    'aws_secret_access_key = testsecret' >"$node_dir/credentials"
  cat >"$node_dir/config.toml" <<EOF
[listen]
s3_port = 8333
admin_port = 8334

[cache]
dir = "/data/cache"
capacity_bytes = "1GB"
dram_bytes = "80MB"

[backend]
bucket = "wal-test"
endpoint = "http://$network-minio:9000"
allow_http = true
region = "us-east-1"
credentials_file = "/data/credentials"

[auth]
credentials_file = "/data/credentials"
EOF
  name="$network-cache-$index"
  nodes+=("$name")
  docker run -d --name "$name" --hostname "cache-$index" --network "$network" \
    --ip "172.30.${network_octet}.$((10 + index))" \
    -v "$node_dir:/data" \
    -e VERGLAS_ADMIN_ADDR=0.0.0.0:8334 \
    -e VERGLAS_S3_ADDR=0.0.0.0:8333 \
    -e VERGLAS_BLOCK_ADDR=0.0.0.0:8335 \
    -e VERGLAS_RING_ADDR=0.0.0.0:8336 \
    -e VERGLAS_SAFEKEEPER_ADDR=0.0.0.0:5454 \
    -e VERGLAS_SAFEKEEPER_BROKER_ENDPOINT="http://$network-neon:50051" \
    -e VERGLAS_SAFEKEEPER_ADVERTISE_ADDR="cache-$index:5454" \
    -e VERGLAS_NODE_ID="node-$index" \
    -e VERGLAS_RING_PEERS="$peers" \
    "$image" --config /data/config.toml >/dev/null
done

for index in 0 1 2 3; do
  for _ in $(seq 1 90); do
    status=$(docker run --rm --network "$network" curlimages/curl:8.12.1 \
      -s -o /dev/null -w '%{http_code}' "http://cache-$index:8334/admin/healthz" || true)
    [[ "$status" == 200 ]] && break
    sleep 1
  done
  [[ "$status" == 200 ]] || { docker logs "$network-cache-$index"; exit 1; }
  node_logs=$(docker logs "$network-cache-$index" 2>&1)
  grep -q '4 peers, RS quorum write-back' <<<"$node_logs"
done

page_path='/internal/v1/neon/pages/11111111111111111111111111111111/22222222222222222222222222222222/1663/16384/24576/0/7/100'
put_url="http://cache-0:8334$page_path"
get_url="http://cache-1:8334$page_path"
put_status=$(docker run --rm -i --network "$network" --entrypoint sh curlimages/curl:8.12.1 -c \
  "dd if=/dev/zero bs=8192 count=1 2>/dev/null | curl -s -o /dev/null -w '%{http_code}' -X PUT --data-binary @- '$put_url'")
[[ "$put_status" == 204 ]]
get_bytes=$(docker run --rm --network "$network" curlimages/curl:8.12.1 \
  -s "$get_url" | wc -c | tr -d ' ')
[[ "$get_bytes" == 8192 ]]
for _ in $(seq 1 20); do
  docker run --rm --network "$network" curlimages/curl:8.12.1 -s -o /dev/null "$get_url"
done
stats=$(docker run --rm --network "$network" curlimages/curl:8.12.1 \
  -s 'http://cache-1:8334/admin/stats')
python3 -c 'import json,sys; s=json.load(sys.stdin); assert s["counters"]["dram_hits"] >= 20, s' <<<"$stats"

if [[ -n "$neon_image" ]]; then
  neon_node="$network-neon"
  docker run -d --privileged --name "$neon_node" --network "$network" \
    -e VERGLAS_PG_REMOTE_ENDPOINT="http://$network-minio:9000" \
    -e VERGLAS_PG_REMOTE_BUCKET=wal-test \
    -e VERGLAS_PG_REMOTE_PREFIX=neon-e2e \
    -e VERGLAS_PG_REMOTE_ACCESS_KEY_ID=test \
    -e VERGLAS_PG_REMOTE_SECRET_ACCESS_KEY=testsecret \
    -e VERGLAS_PG_SAFEKEEPERS=cache-0:5454 \
    -e VERGLAS_PG_TENANT_ID=33333333333333333333333333333333 \
    -e VERGLAS_PG_TIMELINE_ID=44444444444444444444444444444444 \
    -e VERGLAS_CATALOG_PASSWORD=catalog-secret \
    -e VERGLAS_ACCESS_PASSWORD=access-secret \
    -e VERGLAS_SCHEDULER_PASSWORD=scheduler-secret \
    "$neon_image" >/dev/null

  for _ in $(seq 1 300); do
    if docker exec -e PGPASSWORD=cloud_admin "$neon_node" \
      /usr/local/bin/pg_isready -h 127.0.0.1 -p 55433 -U cloud_admin >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  docker exec -e PGPASSWORD=cloud_admin "$neon_node" /usr/local/bin/psql \
    'postgresql://cloud_admin@127.0.0.1:55433/postgres' -v ON_ERROR_STOP=1 -c \
    "CREATE TABLE heat_test(id bigint PRIMARY KEY, payload text); INSERT INTO heat_test SELECT i, repeat(md5(i::text), 8) FROM generate_series(1,1000000) AS i; CHECKPOINT; SELECT count(*) FROM heat_test;" >/dev/null
  before_hits=$(docker run --rm --network "$network" curlimages/curl:8.12.1 \
    -s 'http://cache-0:8334/admin/stats' | python3 -c 'import json,sys; c=json.load(sys.stdin)["counters"]; print(c["dram_hits"]+c["disk_hits"]+c["peer_hits"])')
  for _ in 1 2 3; do
    sum=$(docker exec -e PGPASSWORD=cloud_admin "$neon_node" /usr/local/bin/psql \
      'postgresql://cloud_admin@127.0.0.1:55433/postgres' -v ON_ERROR_STOP=1 -tAc \
      'SELECT sum(length(payload)) FROM heat_test')
    [[ "$sum" == 256000000 ]]
  done
  final_stats=$(docker run --rm --network "$network" curlimages/curl:8.12.1 \
    -s 'http://cache-0:8334/admin/stats')
  after_hits=$(python3 -c 'import json,sys; c=json.load(sys.stdin)["counters"]; print(c["dram_hits"]+c["disk_hits"]+c["peer_hits"])' <<<"$final_stats")
  (( after_hits > before_hits )) || {
    echo "Neon scans produced no reconstructed-page cache hits ($before_hits -> $after_hits)" >&2
    docker logs "$neon_node" >&2
    exit 1
  }
  tiers=$(python3 -c 'import json,sys; c=json.load(sys.stdin)["counters"]; print("dram=%d disk=%d peer=%d" % (c["dram_hits"],c["disk_hits"],c["peer_hits"]))' <<<"$final_stats")
  echo "Neon query-after-write and reconstructed-page reuse: PASS ($before_hits -> $after_hits total hits; $tiers)"
fi

# A four-node ring uses w=3. Stop one member, then prove a new S3 PUT still
# crosses the three remaining fsync placements and reaches the origin.
docker stop "$network-cache-3" >/dev/null
docker run --rm -i --network "$network" --entrypoint sh curlimages/curl:8.12.1 -c \
  "printf 'four-node-quorum' | curl --fail --silent --aws-sigv4 aws:amz:us-east-1:s3 --user test:testsecret -X PUT --data-binary @- http://cache-0:8333/wal-test/neon/quorum-after-one-down" >/dev/null
stats=$(docker run --rm --network "$network" curlimages/curl:8.12.1 \
  -s 'http://cache-0:8334/admin/stats')
python3 -c 'import json,sys; s=json.load(sys.stdin); assert s["writeback"]["acked_via_quorum"] >= 1, s' <<<"$stats"
for _ in $(seq 1 60); do
  origin=$(docker run --rm --network "$network" --entrypoint sh minio/mc:latest -c \
    "mc alias set origin http://$network-minio:9000 test testsecret >/dev/null && mc cat origin/wal-test/neon/quorum-after-one-down" 2>/dev/null || true)
  [[ "$origin" == four-node-quorum ]] && break
  sleep 1
done
[[ "$origin" == four-node-quorum ]]

echo "four-node shared Neon cache and one-member-down quorum write: PASS"
