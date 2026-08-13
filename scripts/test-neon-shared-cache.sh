#!/usr/bin/env bash
# Container-level smoke for a four-node Verglas ring. It verifies the exact
# Neon page protocol, hit accounting, and quorum write-back with one member down.
set -euo pipefail

image=${VERGLAS_CACHE_NODE_IMAGE:-codex/verglas-cache-node:shared-page}
neon_image=${VERGLAS_NEON_STACK_IMAGE:-}
network="verglas-shared-cache-$RANDOM"
origin_network="$network-origin"
network_octet=$((RANDOM % 180 + 40))
subnet="172.30.${network_octet}.0/24"
origin_subnet="172.31.${network_octet}.0/24"
work_root=${VERGLAS_TEST_WORK_ROOT:-${TMPDIR:-/tmp}}
mkdir -p "$work_root"
work=$(mktemp -d "$work_root/verglas-shared-cache.XXXXXX")
nodes=()
volumes=()
neon_node=""
replica_ports=(56434 56435)
row_count=${VERGLAS_TEST_ROWS:-1000000}
run_replicas=${VERGLAS_TEST_REPLICAS:-1}

cleanup() {
  status=$?
  if [[ "${VERGLAS_KEEP_FAILED_ENV:-0}" == 1 && "$status" != 0 ]]; then
    echo "preserving failed test environment: network=$network work=$work" >&2
    return
  fi
  for node in ${nodes[@]-}; do docker rm -f "$node" >/dev/null 2>&1 || true; done
  for volume in ${volumes[@]-}; do docker volume rm "$volume" >/dev/null 2>&1 || true; done
  [[ -z "$neon_node" ]] || docker rm -f "$neon_node" >/dev/null 2>&1 || true
  docker rm -f "$network-minio" >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
  docker network rm "$origin_network" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

docker network create --subnet "$subnet" "$network" >/dev/null
docker network create --subnet "$origin_subnet" "$origin_network" >/dev/null
docker run -d --name "$network-minio" --network "$origin_network" --ip "172.31.${network_octet}.2" \
  -e MINIO_ROOT_USER=origin -e MINIO_ROOT_PASSWORD=originsecret \
  minio/minio:latest server /data >/dev/null

for _ in $(seq 1 60); do
  if docker run --rm --network "$origin_network" minio/mc:latest \
    alias set origin "http://$network-minio:9000" origin originsecret >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker run --rm --network "$origin_network" --entrypoint sh minio/mc:latest -c \
  "mc alias set origin http://$network-minio:9000 origin originsecret >/dev/null && mc mb --ignore-existing origin/wal-test" >/dev/null

peers="node-0=172.30.${network_octet}.10:8336,node-1=172.30.${network_octet}.11:8336,node-2=172.30.${network_octet}.12:8336,node-3=172.30.${network_octet}.13:8336"
for index in 0 1 2 3; do
  node_dir="$work/node-$index"
  mkdir -p "$node_dir/cache"
  printf '%s\n' '[default]' 'aws_access_key_id = origin' \
    'aws_secret_access_key = originsecret' >"$node_dir/backend-credentials"
  printf '%s\n' '[default]' 'aws_access_key_id = cache' \
    'aws_secret_access_key = cachesecret' >"$node_dir/cache-credentials"
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
credentials_file = "/data/backend-credentials"

[auth]
credentials_file = "/data/cache-credentials"
EOF
  name="$network-cache-$index"
  volume="$name-data"
  nodes+=("$name")
  volumes+=("$volume")
  docker volume create "$volume" >/dev/null
  docker run --rm -v "$volume:/data/cache" alpine:3.21 \
    chown 999:999 /data/cache
  docker create --name "$name" --hostname "cache-$index" --network "$network" \
    --ip "172.30.${network_octet}.$((10 + index))" \
    -v "$volume:/data/cache" \
    -v "$node_dir/config.toml:/data/config.toml:ro" \
    -v "$node_dir/backend-credentials:/data/backend-credentials:ro" \
    -v "$node_dir/cache-credentials:/data/cache-credentials:ro" \
    -e VERGLAS_ADMIN_ADDR=0.0.0.0:8334 \
    -e VERGLAS_S3_ADDR=0.0.0.0:8333 \
    -e VERGLAS_BLOCK_ADDR=0.0.0.0:8335 \
    -e VERGLAS_RING_ADDR=0.0.0.0:8336 \
    -e VERGLAS_SAFEKEEPER_ADDR=0.0.0.0:5454 \
    -e VERGLAS_SAFEKEEPER_BROKER_ENDPOINT="http://$network-neon:50051" \
    -e VERGLAS_SAFEKEEPER_ADVERTISE_ADDR="cache-$index:5454" \
    -e VERGLAS_NODE_ID="node-$index" \
    -e VERGLAS_RING_PEERS="$peers" \
    --entrypoint verglas-cache-node \
    "$image" --config /data/config.toml >/dev/null
  docker network connect --ip "172.31.${network_octet}.$((10 + index))" "$origin_network" "$name"
  docker start "$name" >/dev/null
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
  grep -q 'fragment plane listening.*(4 nodes)' <<<"$node_logs"
  grep -q 'embedded safekeeper listening.*EC k=2, m=2, ack quorum=3' <<<"$node_logs"
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
    -e VERGLAS_PG_CACHE_ENDPOINT="http://cache-0:8333" \
    -e VERGLAS_PG_CACHE_BUCKET=wal-test \
    -e VERGLAS_PG_CACHE_PREFIX=neon-e2e \
    -e VERGLAS_PG_CACHE_ACCESS_KEY_ID=cache \
    -e VERGLAS_PG_CACHE_SECRET_ACCESS_KEY=cachesecret \
    -e VERGLAS_PG_SAFEKEEPERS=cache-0:5454 \
    -e VERGLAS_PG_REMOTE_ENDPOINT="http://cache-0:8333" \
    -e VERGLAS_PG_REMOTE_BUCKET=wal-test \
    -e VERGLAS_PG_REMOTE_PREFIX=neon-e2e \
    -e VERGLAS_PG_REMOTE_ACCESS_KEY_ID=cache \
    -e VERGLAS_PG_REMOTE_SECRET_ACCESS_KEY=cachesecret \
    -e VERGLAS_RING_S3_ENDPOINTS="http://cache-0:8333,http://cache-1:8333,http://cache-2:8333,http://cache-3:8333" \
    -e VERGLAS_RING_ADMIN_ENDPOINTS="http://cache-0:8334,http://cache-1:8334,http://cache-2:8334,http://cache-3:8334" \
    -e VERGLAS_RING_SAFEKEEPER_ENDPOINTS="cache-0:5454,cache-1:5454,cache-2:5454,cache-3:5454" \
    -e VERGLAS_PG_TENANT_ID=33333333333333333333333333333333 \
    -e VERGLAS_PG_TIMELINE_ID=44444444444444444444444444444444 \
    -e VERGLAS_CATALOG_PASSWORD=catalog-secret \
    -e VERGLAS_ACCESS_PASSWORD=access-secret \
    -e VERGLAS_SCHEDULER_PASSWORD=scheduler-secret \
    "$neon_image" >/dev/null

  # Neon has neither a route to the origin network nor the origin keypair.
  ! docker exec "$neon_node" getent hosts "$network-minio" >/dev/null 2>&1
  ! docker exec "$neon_node" env | grep -q 'originsecret'

  for _ in $(seq 1 300); do
    if docker exec -e PGPASSWORD=cloud_admin "$neon_node" \
      /usr/local/bin/pg_isready -h 127.0.0.1 -p 55433 -U cloud_admin >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  docker exec -e PGPASSWORD=cloud_admin "$neon_node" /usr/local/bin/psql \
    'postgresql://cloud_admin@127.0.0.1:55433/postgres' -v ON_ERROR_STOP=1 -c \
    "CREATE TABLE heat_test(id bigint PRIMARY KEY, payload text); INSERT INTO heat_test SELECT i, repeat(md5(i::text), 8) FROM generate_series(1,$row_count) AS i; CHECKPOINT; SELECT count(*) FROM heat_test;" >/dev/null

  if [[ "$run_replicas" == 1 ]]; then
  # Start two independent read-only computes against the same pageserver,
  # timeline, and Verglas safekeeper. These are real Neon Replica computes,
  # not extra client connections to the primary Postgres process.
  for replica_index in 0 1; do
    replica_port=${replica_ports[$replica_index]}
    replica_http=$((3180 + replica_index * 2))
    replica_internal_http=$((3181 + replica_index * 2))
    replica_vm_monitor=$((10310 + replica_index))
    docker exec \
      -e REPLICA_INDEX="$replica_index" \
      -e REPLICA_PORT="$replica_port" \
      "$neon_node" sh -ceu '
        data=/run/neon/replica-$REPLICA_INDEX
        spec=/run/neon/replica-$REPLICA_INDEX.json
        socket=/run/neon/replica-$REPLICA_INDEX-sock
        mkdir -p "$data" "$socket"
        chown -R postgres:postgres "$data" "$socket"
        primary_conninfo=$(printf "host=cache-0 port=5454 options=\047-c timeline_id=44444444444444444444444444444444 tenant_id=33333333333333333333333333333333\047 application_name=replica replication=true")
        jq --arg port "$REPLICA_PORT" \
          --arg socket "$socket" \
          --arg endpoint "verglas-read-$REPLICA_INDEX" \
          --arg primary_conninfo "$primary_conninfo" \
          '\''
            .spec.mode = "Replica"
            | .spec.skip_pg_catalog_updates = true
            | .spec.endpoint_id = $endpoint
            | .spec.cluster.settings = (
                .spec.cluster.settings
                | map(select(
                    .name != "port"
                    and .name != "unix_socket_directories"
                    and .name != "neon.safekeepers"
                    and .name != "synchronous_standby_names"
                    and .name != "primary_conninfo"
                    and .name != "primary_slot_name"
                    and .name != "hot_standby"
                    and .name != "recovery_prefetch"
                  ))
                + [
                    {"name":"port", "value":$port, "vartype":"integer"},
                    {"name":"unix_socket_directories", "value":$socket, "vartype":"string"},
                    {"name":"primary_conninfo", "value":$primary_conninfo, "vartype":"string"},
                    {"name":"primary_slot_name", "value":"repl_44444444444444444444444444444444_", "vartype":"string"},
                    {"name":"hot_standby", "value":"on", "vartype":"bool"},
                    {"name":"recovery_prefetch", "value":"off", "vartype":"enum"}
                  ]
              )
          '\'' /run/neon/compute-spec.json >"$spec"
        chown postgres:postgres "$spec"
      '
    docker exec -d --user postgres \
      -e OTEL_SDK_DISABLED=true \
      "$neon_node" sh -c \
      "exec /usr/local/bin/compute_ctl \
        --pgdata /run/neon/replica-$replica_index \
        -C postgresql://cloud_admin@127.0.0.1:$replica_port/postgres \
        -b /usr/local/bin/postgres \
        --compute-id verglas-read-$replica_index \
        --config /run/neon/replica-$replica_index.json \
        --external-http-port $replica_http \
        --internal-http-port $replica_internal_http \
        --vm-monitor-addr 127.0.0.1:$replica_vm_monitor \
        --dev >/run/neon/replica-$replica_index.log 2>&1"
  done

  for replica_port in "${replica_ports[@]}"; do
    ready=false
    for _ in $(seq 1 180); do
      if docker exec -e PGPASSWORD=cloud_admin "$neon_node" /usr/local/bin/psql \
        "postgresql://cloud_admin@127.0.0.1:$replica_port/postgres" -tAc \
        'SELECT pg_is_in_recovery()' 2>/dev/null | grep -qx t; then
        ready=true
        break
      fi
      sleep 1
    done
    if [[ "$ready" != true ]]; then
      docker exec "$neon_node" sh -c "tail -n 120 /run/neon/replica-$((replica_port - 56434)).log 2>/dev/null || true" >&2
      docker logs "$neon_node" >&2
      exit 1
    fi
  done

  # Commit on the primary after both mirrors are running. Both mirrors must
  # observe that row through WAL replay before serving the parallel workload.
  marker=$(date +%s%N)
  marker_id=$((row_count + 1))
  expected_sum=$((row_count * 256 + ${#marker}))
  docker exec -e PGPASSWORD=cloud_admin "$neon_node" /usr/local/bin/psql \
    'postgresql://cloud_admin@127.0.0.1:55433/postgres' -v ON_ERROR_STOP=1 -c \
    "INSERT INTO heat_test VALUES ($marker_id, '$marker');" >/dev/null
  for replica_port in "${replica_ports[@]}"; do
    visible=false
    for _ in $(seq 1 120); do
      value=$(docker exec -e PGPASSWORD=cloud_admin "$neon_node" /usr/local/bin/psql \
        "postgresql://cloud_admin@127.0.0.1:$replica_port/postgres" -tAc \
        "SELECT payload FROM heat_test WHERE id=$marker_id" 2>/dev/null || true)
      if [[ "$value" == "$marker" ]]; then
        visible=true
        break
      fi
      sleep 1
    done
    if [[ "$visible" != true ]]; then
      echo "replica on port $replica_port did not observe query-after-write" >&2
      docker exec -e PGPASSWORD=cloud_admin "$neon_node" /usr/local/bin/psql \
        'postgresql://cloud_admin@127.0.0.1:55433/postgres' -x -c \
        'SELECT pg_current_wal_lsn(), pg_current_wal_flush_lsn()' >&2 || true
      docker exec -e PGPASSWORD=cloud_admin "$neon_node" /usr/local/bin/psql \
        "postgresql://cloud_admin@127.0.0.1:$replica_port/postgres" -x -c \
        'SELECT pg_is_in_recovery(), pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn(), pg_last_xact_replay_timestamp()' >&2 || true
      docker exec "$neon_node" sh -c "tail -n 160 /run/neon/replica-$((replica_port - 56434)).log" >&2 || true
      docker logs --tail 160 "$network-cache-0" >&2 || true
      exit 1
    fi
  done

  # Run eight query clients across the two independent computes at once. Record
  # shared-cache activity before the scans because those scans also warm each
  # compute's local PostgreSQL buffers.
  before_hits=$(docker run --rm --network "$network" curlimages/curl:8.12.1 \
    -s 'http://cache-0:8334/admin/stats' | python3 -c 'import json,sys; c=json.load(sys.stdin)["counters"]; print(c["dram_hits"]+c["disk_hits"]+c["peer_hits"])')
  query_outputs=()
  query_pids=()
  for replica_port in "${replica_ports[@]}"; do
    for worker in 1 2 3 4; do
      output="$work/query-$replica_port-$worker.out"
      query_outputs+=("$output")
      docker exec -e PGPASSWORD=cloud_admin "$neon_node" /usr/local/bin/psql \
        "postgresql://cloud_admin@127.0.0.1:$replica_port/postgres" -v ON_ERROR_STOP=1 -tAc \
        'SELECT sum(length(payload)) FROM heat_test' >"$output" &
      query_pids+=("$!")
    done
  done
  for pid in "${query_pids[@]}"; do wait "$pid"; done
  for output in "${query_outputs[@]}"; do
    [[ "$(tr -d '[:space:]' <"$output")" == "$expected_sum" ]]
  done
  final_stats=$(docker run --rm --network "$network" curlimages/curl:8.12.1 \
    -s 'http://cache-0:8334/admin/stats')
  after_hits=$(python3 -c 'import json,sys; c=json.load(sys.stdin)["counters"]; print(c["dram_hits"]+c["disk_hits"]+c["peer_hits"])' <<<"$final_stats")
  (( after_hits > before_hits )) || {
    echo "Neon read-mirror scans produced no shared-cache hits ($before_hits -> $after_hits)" >&2
    docker logs "$neon_node" >&2
    exit 1
  }
  writeback_stats=$(docker run --rm --network "$network" curlimages/curl:8.12.1 \
    -s 'http://cache-0:8334/admin/stats')
  python3 -c 'import json,sys; s=json.load(sys.stdin); assert s["writeback"]["acked_via_quorum"] >= 1, s' <<<"$writeback_stats"
  for _ in 1 2 3; do
    sum=$(docker exec -e PGPASSWORD=cloud_admin "$neon_node" /usr/local/bin/psql \
      'postgresql://cloud_admin@127.0.0.1:55433/postgres' -v ON_ERROR_STOP=1 -tAc \
      'SELECT sum(length(payload)) FROM heat_test')
    [[ "$sum" == "$expected_sum" ]]
  done
  tiers=$(python3 -c 'import json,sys; c=json.load(sys.stdin)["counters"]; print("dram=%d disk=%d peer=%d" % (c["dram_hits"],c["disk_hits"],c["peer_hits"]))' <<<"$final_stats")
  echo "Neon query-after-write and reconstructed-page reuse: PASS ($before_hits -> $after_hits total hits; $tiers)"
  echo "two Neon read mirrors and eight concurrent query clients: PASS"
  else
    count=$(docker exec -e PGPASSWORD=cloud_admin "$neon_node" /usr/local/bin/psql \
      'postgresql://cloud_admin@127.0.0.1:55433/postgres' -v ON_ERROR_STOP=1 -tAc \
      'SELECT count(*) FROM heat_test')
    [[ "$count" == "$row_count" ]]
    pool_stats=""
    for index in 0 1 2 3; do
      pool_stats+=$(docker run --rm --network "$network" curlimages/curl:8.12.1 \
        -s "http://cache-$index:8334/admin/stats")$'\n'
    done
    python3 -c '
import json,sys
stats=[json.loads(line)["writeback"] for line in sys.stdin if line.strip()]
assert sum(item["acked_via_quorum"] for item in stats) >= 1, stats
assert sum(item["acked_via_write_through"] for item in stats) == 0, stats
' <<<"$pool_stats"
    echo "Neon primary SQL, pooled S3 quorum writes, and pooled WAL ingress: PASS"
  fi

  # The remaining storage failure tests deliberately stop cache-0. Tear down
  # Neon first so its fixed test endpoint does not turn a cache-node test into
  # a control-plane failover test.
  docker rm -f "$neon_node" >/dev/null
  neon_node=""
fi

# Dirty data remains readable from its coordinator, survives that coordinator's
# restart while origin is unavailable, and propagates after origin recovers
# without another coordinator restart.
docker stop "$network-minio" >/dev/null
docker run --rm -i --network "$network" --entrypoint sh curlimages/curl:8.12.1 -c \
  "printf 'dirty-recovery' | curl --fail --silent --aws-sigv4 aws:amz:us-east-1:s3 --user cache:cachesecret -X PUT --data-binary @- http://cache-0:8333/wal-test/neon/dirty-recovery" >/dev/null
dirty=$(docker run --rm --network "$network" curlimages/curl:8.12.1 -s --fail \
  --aws-sigv4 aws:amz:us-east-1:s3 --user cache:cachesecret \
  http://cache-0:8333/wal-test/neon/dirty-recovery)
[[ "$dirty" == dirty-recovery ]]
docker stop "$network-cache-0" >/dev/null
docker start "$network-cache-0" >/dev/null
for _ in $(seq 1 90); do
  status=$(docker run --rm --network "$network" curlimages/curl:8.12.1 \
    -s -o /dev/null -w '%{http_code}' 'http://cache-0:8334/admin/healthz' || true)
  [[ "$status" == 200 ]] && break
  sleep 1
done
[[ "$status" == 200 ]]
dirty=$(docker run --rm --network "$network" curlimages/curl:8.12.1 -s --fail \
  --aws-sigv4 aws:amz:us-east-1:s3 --user cache:cachesecret \
  http://cache-0:8333/wal-test/neon/dirty-recovery)
[[ "$dirty" == dirty-recovery ]]
docker start "$network-minio" >/dev/null
for _ in $(seq 1 180); do
  origin=$(docker run --rm --network "$origin_network" --entrypoint sh minio/mc:latest -c \
    "mc alias set origin http://$network-minio:9000 origin originsecret >/dev/null 2>&1 && mc cat origin/wal-test/neon/dirty-recovery" 2>/dev/null || true)
  [[ "$origin" == dirty-recovery ]] && break
  sleep 1
done
[[ "$origin" == dirty-recovery ]]
echo "dirty read, origin-less coordinator restart, and resumed propagation: PASS"

# A four-node ring uses w=3. Rotate the failed member through every node and
# prove each of the other members can coordinate a new quorum write.
for failed in 0 1 2 3; do
  coordinator=$(((failed + 1) % 4))
  docker stop "$network-cache-$failed" >/dev/null
  key="quorum-after-node-$failed-down"
  value="four-node-quorum-$failed"
  docker run --rm -i --network "$network" --entrypoint sh curlimages/curl:8.12.1 -c \
    "printf '$value' | curl --fail --silent --aws-sigv4 aws:amz:us-east-1:s3 --user cache:cachesecret -X PUT --data-binary @- http://cache-$coordinator:8333/wal-test/neon/$key" >/dev/null
  stats=$(docker run --rm --network "$network" curlimages/curl:8.12.1 \
    -s "http://cache-$coordinator:8334/admin/stats")
  python3 -c 'import json,sys; s=json.load(sys.stdin); assert s["writeback"]["acked_via_quorum"] >= 1, s' <<<"$stats"
  docker start "$network-cache-$failed" >/dev/null
  for _ in $(seq 1 90); do
    status=$(docker run --rm --network "$network" curlimages/curl:8.12.1 \
      -s -o /dev/null -w '%{http_code}' "http://cache-$failed:8334/admin/healthz" || true)
    [[ "$status" == 200 ]] && break
    sleep 1
  done
  [[ "$status" == 200 ]]
  for _ in $(seq 1 90); do
    origin=$(docker run --rm --network "$origin_network" --entrypoint sh minio/mc:latest -c \
      "mc alias set origin http://$network-minio:9000 origin originsecret >/dev/null 2>&1 && mc cat origin/wal-test/neon/$key" 2>/dev/null || true)
    [[ "$origin" == "$value" ]] && break
    sleep 1
  done
  [[ "$origin" == "$value" ]]
done

echo "four-node quorum writes with each member failed in turn: PASS"
