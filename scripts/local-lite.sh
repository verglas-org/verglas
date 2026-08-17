#!/usr/bin/env bash
# scripts/local-lite.sh — implements the frozen protocol in
# scripts/LOCAL_LITE_ACCEPTANCE.md (v3). Boots, on localhost, from CURRENT
# source trees: a MinIO object store, FOUR verglas-cache-node processes built
# --release from this tree forming a real fragment ring (the engine's
# ring.rs hard-requires >=3 VERGLAS_RING_PEERS before the embedded safekeeper
# route lakekeeper commits through even starts — see bins/cache-node/src/
# ring.rs and serve.rs's `safekeeper_args` gate), FOUR Lakekeeper
# serve-craft instances built from $LAKEKEEPER_DIR, one colocated per node,
# and ONE `bins/query-node` (verglas-query-node) instance serving POST
# /v1/query, built from this tree.
#
# All eight ring processes launch through the SAME boot.sh scripts
# verglas-cloud images use (env parity is enforced by construction: this
# script patches only the `exec` target inside a scratch copy of each
# boot.sh, never its env-var contract, and threads per-node values through
# extra environment variables the binaries already read directly —
# VERGLAS_NODE_ID, VERGLAS_RING_PEERS, VERGLAS_RING_ADDR,
# VERGLAS_SAFEKEEPER_ADDR — so a fix to either boot script is exercised
# exactly as production would run it). The query node has no comparable
# cloud image/boot script yet (v3 adds the binary itself); it reads its
# coordinates as plain env vars — see `launch_query_node`.
#
# Port layout (node N, 1-indexed):
#   cache-node S3:         18333 / 18343 / 18353 / 18363
#   cache-node admin:      18334 / 18344 / 18354 / 18364
#   cache-node safekeeper: 18335 / 18345 / 18355 / 18365  (VERGLAS_CATALOG_ENDPOINTS target)
#   cache-node ring/gossip:28335 / 28345 / 28355 / 28365  (VERGLAS_RING_PEERS target)
#   lakekeeper:            18181 / 18191 / 18201 / 18211
#   query-node:            18400  (POST /v1/query only; catalog -> cache-node node1 gateway)
#
# JUDGMENT CALL: production wires VERGLAS_RING_PEERS to the fragment-ring
# listener (default :8336, VERGLAS_RING_ADDR) and VERGLAS_CATALOG_ENDPOINTS to
# the safekeeper listener (default :5454, VERGLAS_SAFEKEEPER_ADDR) as TWO
# separate ports on two separate machines (see cloud/control-plane/src/
# tenant_stacks.ts). On one dev machine all four simulated nodes share
# 127.0.0.1, so each needs its own port for EACH listener; a single "safekeeper/
# ring" port cannot serve both roles in one process. This script keeps the
# frozen doc's four safekeeper ports (they are what CATALOG HEALTH and the
# check steps observe) and allocates its own 283xx range for the ring/gossip
# listener, which no check step or peer's env references beyond this script's
# own construction.
#
# Usage: scripts/local-lite.sh {up [--hold]|down|check}
set -euo pipefail

ENGINE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAKEKEEPER_DIR="${LAKEKEEPER_DIR:-$HOME/code/verglas-lakekeeper}"
CLOUD_DIR="${CLOUD_DIR:-$HOME/code/verglas-cloud}"
STATE_DIR="${VERGLAS_LOCAL_LITE_STATE_DIR:-${TMPDIR:-/tmp}/verglas-local-lite}"

CACHE_BOOT="$CLOUD_DIR/images/cache/boot.sh"
LAKEKEEPER_BOOT="$CLOUD_DIR/images/lakekeeper/boot.sh"

MINIO_CONTAINER="${VERGLAS_LOCAL_LITE_MINIO_CONTAINER:-verglas-local-lite-minio}"
MINIO_S3_PORT="${VERGLAS_LOCAL_LITE_MINIO_PORT:-19000}"
MINIO_CONSOLE_PORT="${VERGLAS_LOCAL_LITE_MINIO_CONSOLE_PORT:-19001}"
MINIO_ROOT_USER="local-lite-admin"
MINIO_ROOT_PASSWORD="local-lite-admin-secret"

# Frozen-doc defaults. Override the four arrays together (space-separated) to
# dodge a squatted port on this machine; state which ports an evidence run
# used in its report.
S3_PORTS=(${VERGLAS_LOCAL_LITE_S3_PORTS:-18333 18343 18353 18363})
ADMIN_PORTS=(${VERGLAS_LOCAL_LITE_ADMIN_PORTS:-18334 18344 18354 18364})
SAFEKEEPER_PORTS=(${VERGLAS_LOCAL_LITE_SAFEKEEPER_PORTS:-18335 18345 18355 18365})
RING_PORTS=(${VERGLAS_LOCAL_LITE_RING_PORTS:-28335 28345 28355 28365})
# The block-device NBD listener (VERGLAS_BLOCK_ADDR, default 0.0.0.0:8335) is
# not part of the frozen protocol's port table and no check step touches it,
# but every node opens it once [backend].bucket is set (required for the
# ring gate), so all four still need distinct ports on one host.
BLOCK_PORTS=(${VERGLAS_LOCAL_LITE_BLOCK_PORTS:-8335 8345 8355 8365})
LAKEKEEPER_PORTS=(${VERGLAS_LOCAL_LITE_LAKEKEEPER_PORTS:-18181 18191 18201 18211})
NODE_IDS=(node1 node2 node3 node4)
# The tenant query node (v3): ONE `bins/query-node` instance, POST /v1/query
# only. Frozen doc default; override to dodge a squatted port.
QUERY_PORT="${VERGLAS_LOCAL_LITE_QUERY_PORT:-18400}"
# Frozen doc pins the run's node memory ceiling to 1 GiB.
QUERY_MEMORY_LIMIT_BYTES="${VERGLAS_LOCAL_LITE_QUERY_MEMORY_LIMIT_BYTES:-1073741824}"

TENANT="local-lite"
WAREHOUSE="default"
BACKEND_BUCKET="local-lite-data"
# Must be a bucket [backend] already serves (verglas-cache-node validates
# catalog_archive.bucket against the served backend set) — reuse BACKEND_BUCKET,
# same as boot.sh's own comment: same tenant bucket, archive objects under a
# reserved prefix.
CATALOG_ARCHIVE_BUCKET="$BACKEND_BUCKET"
# boot.sh (images/lakekeeper/boot.sh) renders this exact literal bucket name
# into the managed S3 profile today; local-lite mirrors it rather than
# guessing at a "fix" for behavior outside this task's scope.
METADATA_BUCKET="managed-template"
TABLE="main.pairing_events"

CREDENTIALS_JSON="$STATE_DIR/credentials.json"

log() { printf '[local-lite] %s\n' "$*" >&2; }
fail() { printf '[local-lite] FAIL: %s\n' "$*" >&2; exit 1; }

require_file() {
  [ -f "$1" ] || fail "missing required file: $1"
}

wait_for_http() {
  # wait_for_http URL DEADLINE_SECONDS LABEL
  local url="$1" deadline="$2" label="$3" waited=0
  while ! curl -fsS -o /dev/null -m 2 "$url" 2>/dev/null; do
    waited=$((waited + 1))
    if [ "$waited" -ge "$deadline" ]; then
      fail "$label did not answer $url within ${deadline}s"
    fi
    sleep 1
  done
  log "$label is up ($url, ${waited}s)"
}

# wait_for_query_node URL DEADLINE_SECONDS LABEL — the query node exposes no
# GET health route (POST /v1/query only, per the frozen protocol), so
# readiness is a trivial catalog-reaching query rather than a GET probe.
wait_for_query_node() {
  local url="$1" deadline="$2" label="$3" waited=0 status
  while true; do
    status=$(curl -sS -o /dev/null -m 2 -w '%{http_code}' -X POST "$url" \
      -H 'content-type: application/json' \
      --data '{"sql":"SELECT 1"}' 2>/dev/null || echo 000)
    [ "$status" = "200" ] && break
    waited=$((waited + 1))
    if [ "$waited" -ge "$deadline" ]; then
      fail "$label did not answer $url within ${deadline}s (last status $status)"
    fi
    sleep 1
  done
  log "$label is up ($url, ${waited}s)"
}

pid_alive() {
  [ -n "${1:-}" ] && kill -0 "$1" 2>/dev/null
}

# Milliseconds since epoch. `date +%s%3N` is GNU-only; BSD/macOS `date` has no
# `%N` and appends a literal "N", breaking arithmetic — use python3 instead.
now_ms() {
  python3 -c 'import time; print(int(time.time() * 1000))'
}

ring_peers_value() {
  local i peers=""
  for i in 0 1 2 3; do
    [ -n "$peers" ] && peers="$peers,"
    peers="${peers}${NODE_IDS[$i]}=127.0.0.1:${RING_PORTS[$i]}"
  done
  printf '%s' "$peers"
}

# ---------------------------------------------------------------------------
# up
# ---------------------------------------------------------------------------
cmd_up() {
  mkdir -p "$STATE_DIR"
  require_file "$CACHE_BOOT"
  require_file "$LAKEKEEPER_BOOT"
  [ -d "$LAKEKEEPER_DIR" ] || fail "LAKEKEEPER_DIR does not exist: $LAKEKEEPER_DIR"

  # MinIO starts empty every `up` (no persistent volume — see start_minio), but
  # each node's Raft/consensus log lives on local disk under $STATE_DIR and
  # survives a down/up cycle by default. Replaying an old catalog against a
  # fresh, empty object store reproduces exactly the object-not-found bug
  # this script exists to catch, so wipe both together.
  local node_id
  for node_id in "${NODE_IDS[@]}"; do
    rm -rf "$STATE_DIR/cache-run-$node_id"
  done

  start_minio
  create_buckets
  mint_credentials
  build_engine
  build_query_node
  build_lakekeeper

  local i
  for i in 0 1 2 3; do
    launch_cache_node "$i"
  done
  for i in 0 1 2 3; do
    launch_lakekeeper "$i"
  done
  for i in 0 1 2 3; do
    wait_for_http "http://127.0.0.1:${ADMIN_PORTS[$i]}/admin/healthz" 30 "cache-node ${NODE_IDS[$i]}"
  done
  for i in 0 1 2 3; do
    wait_for_http "http://127.0.0.1:${LAKEKEEPER_PORTS[$i]}/health" 30 "lakekeeper ${NODE_IDS[$i]}"
  done
  launch_query_node
  wait_for_query_node "http://127.0.0.1:$QUERY_PORT/v1/query" 30 "query-node"
  log "stack up: minio=:$MINIO_S3_PORT"
  for i in 0 1 2 3; do
    log "  ${NODE_IDS[$i]}: cache s3=:${S3_PORTS[$i]} admin=:${ADMIN_PORTS[$i]} safekeeper=:${SAFEKEEPER_PORTS[$i]} ring=:${RING_PORTS[$i]} lakekeeper=:${LAKEKEEPER_PORTS[$i]}"
  done
  log "  query-node: :$QUERY_PORT (catalog -> cache-node node1 gateway, S3 -> minio, memory_limit_bytes=$QUERY_MEMORY_LIMIT_BYTES)"
  log "logs: $STATE_DIR/cache-node-*.log , $STATE_DIR/lakekeeper-*.log , $STATE_DIR/query-node.log"
}

start_minio() {
  if docker inspect "$MINIO_CONTAINER" >/dev/null 2>&1; then
    log "minio container already exists, reusing"
    docker start "$MINIO_CONTAINER" >/dev/null 2>&1 || true
  else
    log "starting minio on :$MINIO_S3_PORT (console :$MINIO_CONSOLE_PORT)"
    docker run -d --name "$MINIO_CONTAINER" \
      -p "$MINIO_S3_PORT:9000" -p "$MINIO_CONSOLE_PORT:9001" \
      -e "MINIO_ROOT_USER=$MINIO_ROOT_USER" \
      -e "MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD" \
      minio/minio:latest server /data --console-address :9001 >/dev/null
  fi
  wait_for_http "http://127.0.0.1:$MINIO_S3_PORT/minio/health/live" 30 "minio"
}

create_buckets() {
  export AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER"
  export AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD"
  for bucket in "$BACKEND_BUCKET" "$METADATA_BUCKET" "$CATALOG_ARCHIVE_BUCKET"; do
    if ! aws --endpoint-url "http://127.0.0.1:$MINIO_S3_PORT" s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
      aws --endpoint-url "http://127.0.0.1:$MINIO_S3_PORT" s3 mb "s3://$bucket" >/dev/null
      log "created bucket $bucket"
    fi
  done
  unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
}

# Mints a local Cloudflare-Access-shaped ES256 credential (JWKS + bearer JWT)
# so lakekeeper's fail-closed VerglasAuthorizer (crates/authz-verglas) has
# something real to verify. There is no Cloudflare tunnel in local-lite, so
# this script is the tunnel's stand-in: same JWT shape (tenant_id, sub, jti,
# scope, ES256/P-256), same verification path the production JWKS pin uses.
# One credential, shared by all four lakekeeper instances (they are the same
# stateless tenant behind one LB in production).
mint_credentials() {
  python3 "$ENGINE_DIR/scripts/local_lite_mint_credentials.py" "$TENANT" "https://local-lite.verglas.internal" > "$CREDENTIALS_JSON"
  log "minted local ES256 credential for lakekeeper authorization"
}

credential_field() {
  python3 -c "import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])" "$CREDENTIALS_JSON" "$1"
}

build_engine() {
  log "building verglas-cache-node --release from $ENGINE_DIR"
  ( cd "$ENGINE_DIR" && cargo build --release -p verglas-cache-node )
  CACHE_BIN="$ENGINE_DIR/target/release/verglas-cache-node"
  [ -x "$CACHE_BIN" ] || fail "build did not produce $CACHE_BIN"
}

build_lakekeeper() {
  log "building lakekeeper serve-craft --release from $LAKEKEEPER_DIR"
  ( cd "$LAKEKEEPER_DIR" && cargo build --release -p lakekeeper-bin )
  LAKEKEEPER_BIN="$LAKEKEEPER_DIR/target/release/lakekeeper"
  [ -x "$LAKEKEEPER_BIN" ] || fail "build did not produce $LAKEKEEPER_BIN"
}

build_query_node() {
  log "building verglas-query-node --release from $ENGINE_DIR"
  ( cd "$ENGINE_DIR" && cargo build --release -p verglas-query-node )
  QUERY_NODE_BIN="$ENGINE_DIR/target/release/verglas-query-node"
  [ -x "$QUERY_NODE_BIN" ] || fail "build did not produce $QUERY_NODE_BIN"
}

# Patches only the hardcoded `exec /usr/local/bin/...` install path inside a
# scratch copy of a boot.sh, so the rest of the script (env var contract,
# validation, rendering) is byte-identical to what verglas-cloud ships.
patched_boot_copy() {
  # patched_boot_copy SRC DEST INSTALLED_PATH REAL_BIN
  local src="$1" dest="$2" installed="$3" real="$4"
  sed "s#${installed}#${real}#" "$src" > "$dest"
  chmod +x "$dest"
}

launch_cache_node() {
  local idx="$1" jwt node_id run_dir
  jwt=$(credential_field jwt)
  node_id="${NODE_IDS[$idx]}"
  run_dir="$STATE_DIR/cache-run-$node_id"

  patched_boot_copy "$CACHE_BOOT" "$STATE_DIR/cache-boot-$node_id.sh" "/usr/local/bin/verglasd" "$CACHE_BIN"

  log "launching cache-node $node_id"
  env \
    VERGLAS_CONF_DIR="$run_dir" \
    VERGLAS_CACHE_DIR="$run_dir/cache" \
    VERGLAS_NODE_ID="$node_id" \
    VERGLAS_RING_PEERS="$(ring_peers_value)" \
    VERGLAS_RING_ADDR="0.0.0.0:${RING_PORTS[$idx]}" \
    VERGLAS_SAFEKEEPER_ADDR="0.0.0.0:${SAFEKEEPER_PORTS[$idx]}" \
    VERGLAS_BLOCK_ADDR="0.0.0.0:${BLOCK_PORTS[$idx]}" \
    VERGLAS_SAFEKEEPER_EC_K=2 \
    VERGLAS_SAFEKEEPER_EC_M=2 \
    VERGLAS_SAFEKEEPER_EC_W=3 \
    VERGLAS_BACKEND_BUCKET="$BACKEND_BUCKET" \
    VERGLAS_BACKEND_BUCKET_GLOB="$BACKEND_BUCKET" \
    VERGLAS_BACKEND_S3_ENDPOINT="http://127.0.0.1:$MINIO_S3_PORT" \
    VERGLAS_BACKEND_ALLOW_HTTP=true \
    VERGLAS_BACKEND_ACCESS_KEY_ID="$MINIO_ROOT_USER" \
    VERGLAS_BACKEND_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD" \
    VERGLAS_BACKEND_REGION=us-east-1 \
    VERGLAS_CACHE_S3_PORT="${S3_PORTS[$idx]}" \
    VERGLAS_CACHE_ADMIN_PORT="${ADMIN_PORTS[$idx]}" \
    VERGLAS_CATALOG_URI="http://127.0.0.1:${LAKEKEEPER_PORTS[$idx]}/catalog" \
    VERGLAS_CATALOG_WAREHOUSE="$WAREHOUSE" \
    VERGLAS_CACHE_CONSISTENCY=eventual \
    VERGLAS_CATALOG_EVENT_TOKEN="local-lite-event-token" \
    VERGLAS_CATALOG_BEARER_TOKEN="$jwt" \
    VERGLAS_CATALOG_ARCHIVE_BUCKET="$CATALOG_ARCHIVE_BUCKET" \
    "$STATE_DIR/cache-boot-$node_id.sh" > "$STATE_DIR/cache-node-$node_id.log" 2>&1 &
  echo $! > "$STATE_DIR/cache-node-$node_id.pid"
}

launch_lakekeeper() {
  local idx="$1" issuer jwks node_id
  issuer=$(credential_field issuer)
  jwks=$(credential_field jwks)
  node_id="${NODE_IDS[$idx]}"

  patched_boot_copy "$LAKEKEEPER_BOOT" "$STATE_DIR/lakekeeper-boot-$node_id.sh" "/usr/local/bin/lakekeeper" "$LAKEKEEPER_BIN"

  log "launching lakekeeper $node_id (endpoints -> own loopback safekeeper only)"
  env \
    VERGLAS_CATALOG_ENDPOINTS="http://127.0.0.1:${SAFEKEEPER_PORTS[$idx]}" \
    VERGLAS_CATALOG_TENANT="$TENANT" \
    VERGLAS_CATALOG_WAREHOUSE="$WAREHOUSE" \
    VERGLAS_CREDENTIAL_ISSUER="$issuer" \
    VERGLAS_CREDENTIAL_JWKS="$jwks" \
    VERGLAS_WAREHOUSE_S3_ENDPOINT="http://127.0.0.1:$MINIO_S3_PORT" \
    VERGLAS_WAREHOUSE_REGION=us-east-1 \
    VERGLAS_WAREHOUSE_BUCKET="$BACKEND_BUCKET" \
    VERGLAS_WAREHOUSE_ACCESS_KEY_ID="$MINIO_ROOT_USER" \
    VERGLAS_WAREHOUSE_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD" \
    LAKEKEEPER__LISTEN_PORT="${LAKEKEEPER_PORTS[$idx]}" \
    LAKEKEEPER__BIND_IP="127.0.0.1" \
    "$STATE_DIR/lakekeeper-boot-$node_id.sh" > "$STATE_DIR/lakekeeper-$node_id.log" 2>&1 &
  echo $! > "$STATE_DIR/lakekeeper-$node_id.pid"
}

# Launches the one tenant query node. Coordinates mirror the ones a
# production tenant query machine receives: the cache node's own catalog
# gateway (node1's loopback `/catalog` mount — see
# `bins/cache-node/src/admin.rs::catalog_router`: "Query workers therefore
# consume already-observed metadata without Lakekeeper credentials or an
# independent catalog cache", the exact contract `bins/cache-node/Cargo.toml`
# documents), never Lakekeeper directly. That gateway is unauthenticated
# behind the node-local admin surface, so no bearer token is needed here. The
# ring's S3 endpoint (MinIO here) and its keypair round out the coordinates.
# No admin/health route beyond POST /v1/query itself (org-network-only, v1).
launch_query_node() {
  log "launching query-node (catalog -> cache-node node1 gateway, memory_limit_bytes=$QUERY_MEMORY_LIMIT_BYTES)"
  env \
    VERGLAS_QUERY_LISTEN="0.0.0.0:$QUERY_PORT" \
    VERGLAS_CATALOG_URI="http://127.0.0.1:${ADMIN_PORTS[0]}/catalog" \
    VERGLAS_WAREHOUSE="$WAREHOUSE" \
    VERGLAS_BACKEND_S3_ENDPOINT="http://127.0.0.1:$MINIO_S3_PORT" \
    VERGLAS_BACKEND_REGION=us-east-1 \
    VERGLAS_BACKEND_ACCESS_KEY_ID="$MINIO_ROOT_USER" \
    VERGLAS_BACKEND_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD" \
    VERGLAS_QUERY_MEMORY_LIMIT_BYTES="$QUERY_MEMORY_LIMIT_BYTES" \
    "$QUERY_NODE_BIN" > "$STATE_DIR/query-node.log" 2>&1 &
  echo $! > "$STATE_DIR/query-node.pid"
}

# ---------------------------------------------------------------------------
# down
# ---------------------------------------------------------------------------
cmd_down() {
  local idx node_id pid_file pid
  for pid_file in "$STATE_DIR/query-node.pid"; do
    if [ -f "$pid_file" ]; then
      pid=$(cat "$pid_file")
      if pid_alive "$pid"; then
        log "stopping pid $pid ($pid_file)"
        kill "$pid" 2>/dev/null || true
        for _ in $(seq 1 10); do pid_alive "$pid" || break; sleep 0.5; done
        pid_alive "$pid" && kill -9 "$pid" 2>/dev/null || true
      fi
      rm -f "$pid_file"
    fi
  done
  for idx in 0 1 2 3; do
    node_id="${NODE_IDS[$idx]}"
    for pid_file in "$STATE_DIR/cache-node-$node_id.pid" "$STATE_DIR/lakekeeper-$node_id.pid"; do
      if [ -f "$pid_file" ]; then
        pid=$(cat "$pid_file")
        if pid_alive "$pid"; then
          log "stopping pid $pid ($pid_file)"
          kill "$pid" 2>/dev/null || true
          for _ in $(seq 1 10); do pid_alive "$pid" || break; sleep 0.5; done
          pid_alive "$pid" && kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$pid_file"
      fi
    done
  done
  if docker inspect "$MINIO_CONTAINER" >/dev/null 2>&1; then
    log "removing minio container"
    docker rm -f "$MINIO_CONTAINER" >/dev/null 2>&1 || true
  fi
  log "down complete (logs kept at $STATE_DIR)"
}

# ---------------------------------------------------------------------------
# check
# ---------------------------------------------------------------------------
cmd_check() {
  local primary="http://127.0.0.1:${ADMIN_PORTS[0]}"
  local failures=0
  local -a async_latencies_ms=()

  log "1. CREATE"
  local t0 t1 status body attempt
  # A cold 4-node ring has not necessarily elected a CRaft leader by the
  # moment /admin/healthz answers (that only proves the HTTP listener is up),
  # so the very first catalog write can transiently 503
  # ("CatalogUnavailable") — retry a few times rather than treat that as a
  # hard failure.
  for attempt in 1 2 3 4 5; do
    t0=$(now_ms)
    status=$(curl -sS -o "$STATE_DIR/create.json" -w '%{http_code}' \
      -X POST "$primary/v1/ingest/$TABLE?mode=create&format=jsonl" \
      -H 'content-type: application/x-ndjson' \
      --data-binary '{"id":1,"event":"created"}
')
    t1=$(now_ms)
    [ "$status" = "200" ] && grep -q '"snapshot_id"' "$STATE_DIR/create.json" && break
    sleep 1
  done
  body=$(cat "$STATE_DIR/create.json")
  if [ "$status" != "200" ] || ! grep -q '"snapshot_id"' "$STATE_DIR/create.json" || ! grep -q '"records_added":1' "$STATE_DIR/create.json"; then
    log "CREATE FAILED status=$status body=$body"
    failures=$((failures + 1))
  else
    log "CREATE ok status=$status latency=$((t1 - t0))ms (attempt $attempt)"
  fi

  log "2. ASYNC APPEND x3"
  local i
  for i in 1 2 3; do
    t0=$(now_ms)
    status=$(curl -sS -o "$STATE_DIR/append-$i.json" -w '%{http_code}' \
      -X POST "$primary/v1/ingest/$TABLE?mode=append&format=jsonl" \
      -H 'content-type: application/x-ndjson' \
      --data-binary "{\"id\":$((i + 1)),\"event\":\"appended\"}
")
    t1=$(now_ms)
    body=$(cat "$STATE_DIR/append-$i.json")
    # The ack body's field is `successfulRows` (verglas_api::table::IngestAckResponse
    # is #[serde(rename_all = "camelCase")]); the frozen doc's prose says
    # `successful_rows` but that is not the wire name.
    if { [ "$status" != "200" ] && [ "$status" != "202" ]; } || ! grep -q '"successfulRows"' "$STATE_DIR/append-$i.json"; then
      log "ASYNC APPEND $i FAILED status=$status body=$body"
      failures=$((failures + 1))
    else
      local latency=$((t1 - t0))
      async_latencies_ms+=("$latency")
      log "ASYNC APPEND $i ok status=$status ack_latency=${latency}ms"
    fi
  done

  log "3. SYNC APPEND"
  # commit_data_files (crates/verglas-iceberg/src/write.rs) already retries
  # CatalogCommitConflicts internally, but the just-acked async appends above
  # can still be mid-background-commit (queue.spawn_commit_if_idle) when this
  # fires; retry client-side too rather than race it once.
  for attempt in 1 2 3 4 5; do
    t0=$(now_ms)
    status=$(curl -sS -o "$STATE_DIR/sync-append.json" -w '%{http_code}' \
      -X POST "$primary/v1/ingest/$TABLE?mode=append&format=jsonl&wait=true" \
      -H 'content-type: application/x-ndjson' \
      --data-binary '{"id":5,"event":"appended"}
')
    t1=$(now_ms)
    # verglas_api::table::CommitResponse is #[serde(rename_all = "camelCase")]
    # (unlike AppendReport, used by CREATE above, which is not renamed) — the
    # field on the wire is `snapshotId`.
    [ "$status" = "200" ] && grep -q '"snapshotId"' "$STATE_DIR/sync-append.json" && break
    sleep 1
  done
  body=$(cat "$STATE_DIR/sync-append.json")
  if [ "$status" != "200" ] || ! grep -q '"snapshotId"' "$STATE_DIR/sync-append.json"; then
    log "SYNC APPEND FAILED status=$status body=$body"
    failures=$((failures + 1))
  else
    log "SYNC APPEND ok status=$status latency=$((t1 - t0))ms (attempt $attempt)"
  fi

  log "4. READ-BACK (poll <=10s, primary node)"
  local waited=0 row_count=0
  while [ "$waited" -le 10 ]; do
    status=$(curl -sS -o "$STATE_DIR/rows.json" -w '%{http_code}' "$primary/v1/tables/$TABLE/rows?limit=10")
    if [ "$status" = "200" ]; then
      row_count=$(python3 -c "import json,sys; d=json.load(open(sys.argv[1])); rows=d.get('rows', d if isinstance(d, list) else []); print(len(rows))" "$STATE_DIR/rows.json" 2>/dev/null || echo 0)
      [ "$row_count" -ge 5 ] && break
    fi
    sleep 1
    waited=$((waited + 1))
  done
  if [ "$row_count" -lt 5 ]; then
    log "READ-BACK FAILED rows=$row_count body=$(cat "$STATE_DIR/rows.json" 2>/dev/null)"
    failures=$((failures + 1))
  else
    log "READ-BACK ok rows=$row_count waited=${waited}s"
  fi

  log "5. CATALOG HEALTH (every node)"
  local idx node_id ok=1
  for idx in 0 1 2 3; do
    node_id="${NODE_IDS[$idx]}"
    local clog="$STATE_DIR/cache-node-$node_id.log"
    if grep -q 'WarehouseIdIsNotUUID' "$clog" 2>/dev/null; then
      log "CATALOG HEALTH FAILED on $node_id: WarehouseIdIsNotUUID present in $clog"
      ok=0
    elif grep -q 'catalog gateway error' "$clog" 2>/dev/null; then
      log "CATALOG HEALTH FAILED on $node_id: catalog gateway error present in $clog"
      grep 'catalog gateway error' "$clog" | tail -5 >&2
      ok=0
    fi
  done
  if [ "$ok" -eq 1 ]; then
    log "CATALOG HEALTH ok"
  else
    failures=$((failures + 1))
  fi

  log "6. STATELESS PARITY (:${LAKEKEEPER_PORTS[1]} vs :${LAKEKEEPER_PORTS[0]})"
  local jwt primary_cfg secondary_cfg
  jwt=$(credential_field jwt)
  primary_cfg=$(curl -sS -H "Authorization: Bearer $jwt" "http://127.0.0.1:${LAKEKEEPER_PORTS[0]}/catalog/v1/config?warehouse=$WAREHOUSE")
  secondary_cfg=$(curl -sS -H "Authorization: Bearer $jwt" "http://127.0.0.1:${LAKEKEEPER_PORTS[1]}/catalog/v1/config?warehouse=$WAREHOUSE")
  if [ -z "$primary_cfg" ] || [ "$primary_cfg" != "$secondary_cfg" ]; then
    log "STATELESS PARITY FAILED primary=$primary_cfg secondary=$secondary_cfg"
    failures=$((failures + 1))
  else
    log "STATELESS PARITY ok ($primary_cfg)"
  fi

  log "7. CROSS-NODE READ (admin :${ADMIN_PORTS[1]})"
  local cross_status cross_rows
  cross_status=$(curl -sS -o "$STATE_DIR/rows-cross.json" -w '%{http_code}' "http://127.0.0.1:${ADMIN_PORTS[1]}/v1/tables/$TABLE/rows?limit=10")
  cross_rows=$(python3 -c "import json,sys; d=json.load(open(sys.argv[1])); rows=d.get('rows', d if isinstance(d, list) else []); print(len(rows))" "$STATE_DIR/rows-cross.json" 2>/dev/null || echo 0)
  if [ "$cross_status" != "200" ] || [ "$cross_rows" -lt 5 ]; then
    log "CROSS-NODE READ FAILED status=$cross_status rows=$cross_rows body=$(cat "$STATE_DIR/rows-cross.json" 2>/dev/null)"
    failures=$((failures + 1))
  else
    log "CROSS-NODE READ ok rows=$cross_rows"
  fi

  log "8. Latency report (informational)"
  if [ "${#async_latencies_ms[@]}" -gt 0 ]; then
    local sorted p50
    sorted=$(printf '%s\n' "${async_latencies_ms[@]}" | sort -n)
    p50=$(echo "$sorted" | awk '{a[NR]=$1} END {print a[int((NR+1)/2)]}')
    log "async append p50 = ${p50}ms (samples: $(echo "$sorted" | tr '\n' ' '))"
  else
    log "no successful async appends to report"
  fi

  log "9. SQL (query-node :$QUERY_PORT)"
  local query_url="http://127.0.0.1:$QUERY_PORT/v1/query"
  local n
  status=$(curl -sS -o "$STATE_DIR/query-count.json" -w '%{http_code}' \
    -X POST "$query_url" \
    -H 'content-type: application/json' \
    --data '{"sql":"SELECT COUNT(*) AS n FROM main.pairing_events"}')
  body=$(cat "$STATE_DIR/query-count.json")
  n=$(python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(d['data'][0]['n'])" "$STATE_DIR/query-count.json" 2>/dev/null || echo "")
  if [ "$status" != "200" ] || [ "$n" != "5" ]; then
    log "SQL COUNT FAILED status=$status n=$n body=$body"
    failures=$((failures + 1))
  else
    log "SQL COUNT ok status=$status n=$n"
  fi

  status=$(curl -sS -o "$STATE_DIR/query-syntax-error.json" -w '%{http_code}' \
    -X POST "$query_url" \
    -H 'content-type: application/json' \
    --data '{"sql":"SELEKT this is not valid sql"}')
  body=$(cat "$STATE_DIR/query-syntax-error.json")
  if [ "${status:0:1}" != "4" ] || ! grep -q '"error"' "$STATE_DIR/query-syntax-error.json"; then
    log "SQL SYNTAX ERROR FAILED status=$status body=$body"
    failures=$((failures + 1))
  else
    log "SQL SYNTAX ERROR ok status=$status (clean 4xx JSON, not a hang or 5xx)"
  fi

  if [ "$failures" -gt 0 ]; then
    fail "$failures check step(s) failed"
  fi
  log "check: ALL GREEN"
}

# ---------------------------------------------------------------------------
main() {
  local cmd="${1:-}"
  shift || true
  case "$cmd" in
    up)
      cmd_up
      ;;
    down)
      cmd_down
      ;;
    check)
      cmd_check
      ;;
    *)
      fail "usage: $(basename "$0") {up [--hold]|down|check}"
      ;;
  esac
}

main "$@"
