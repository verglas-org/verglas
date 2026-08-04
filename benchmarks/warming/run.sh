#!/usr/bin/env bash
# Eager-warming live demonstration (issue #168).
#
# Proves that a WATCHED Iceberg table's COLD planning walk collapses toward a
# WARM one because verglas-server warmed the table's metadata into #50's pinned store
# BEFORE any client asked — driven by the real `[catalog]` REST watcher (#47) +
# warming coordinator (#168), not a benchmark shortcut.
#
# Three configurations, same seeded table, a fresh cache dir each:
#   A  warming OFF, cold client planning walk        (baseline; planning ≈ direct)
#   B  warming ON, wait for /admin/stats warming to   (the number that must
#      complete, then cold client planning walk        collapse toward warm)
#   C  warm client walk                               (floor)
# Two runs each; the server's /admin/stats meta counters are recorded as the
# mechanism evidence. Nothing here runs in PR CI. See README.md for every input.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$HERE/.venv"
DRIVER="$HERE/warming_demo.py"
REQ="$HERE/requirements.txt"
RESULTS_DIR="$HERE/results"
RESULTS="$RESULTS_DIR/walks.jsonl"

# ----- pinned Polaris (same digest as benchmarks/polaris) ------------------- #
POLARIS_IMAGE="${POLARIS_IMAGE:-apache/polaris@sha256:9738b2052dea20aabf0cd42521424ff963fee41b0ee888fef9f512efb256602a}"  # apache/polaris:1.6.0

# ----- inputs (env- or flag-overridable) ------------------------------------ #
BUCKET="${WARMING_BUCKET:-hyperglas}"
PREFIX="${WARMING_PREFIX:-bench/warming-demo}"
NAMESPACE="${WARMING_NAMESPACE:-tpch}"
CATALOG="${WARMING_CATALOG:-demo}"
SCALE="${WARMING_SCALE:-1}"
TABLES="${WARMING_TABLES:-}"                 # comma list; empty = all 8
TARGET_FILE_SIZE="${WARMING_TARGET_FILE_SIZE:-16777216}"  # 16 MiB: fan out footers
RUNS="${WARMING_RUNS:-2}"
MACHINE_NOTE="${WARMING_MACHINE_NOTE:-unspecified machine}"
CACHE_NOTE="${WARMING_CACHE_NOTE:-cache medium unspecified}"

# Verglas server (host). Dedicated ports; admin binds S3_PORT+1.
VG_PORT="${WARMING_VG_PORT:-8555}"
VG_ADMIN_PORT="$((VG_PORT + 1))"
VG_KEY="${WARMING_VG_KEY:-demokey}"
VG_SECRET="${WARMING_VG_SECRET:-demosecret}"
VG_DRAM="${WARMING_VG_DRAM:-1GB}"
VG_DISK="${WARMING_VG_DISK:-8GB}"
POLL_SECS="${WARMING_POLL_SECS:-2}"
VERGLAS_SERVER_BIN="${VERGLAS_SERVER_BIN:-$HERE/../../target/release/verglas-server}"

# Polaris on the host (published ports so verglas-server + the driver reach it).
POLARIS_PORT="${WARMING_POLARIS_PORT:-8181}"
POLARIS_MGMT_PORT="${WARMING_POLARIS_MGMT_PORT:-8182}"
POLARIS_CONTAINER="verglas-warming-polaris"
CATALOG_URI="http://127.0.0.1:${POLARIS_PORT}/api/catalog"
MGMT_URI="http://127.0.0.1:${POLARIS_PORT}"

VG_CACHE_ROOT="${WARMING_VG_CACHE_ROOT:-/tmp/vg-warming-cache}"
VG_LOG="$HERE/verglas-server.log"
VG_PID=""

PHASE="all"

usage() {
  cat <<'EOF'
Usage: run.sh [--all | --seed-only | --measure-only | --report | --teardown] [options]

Phases:
  --all           setup, seed once, measure A/B/C x RUNS, report (default)
  --seed-only     start Polaris, bootstrap the catalog, seed the tables, stop
  --measure-only  measure A/B/C x RUNS against an already-seeded catalog + report
  --report        re-render results/report.md from results/walks.jsonl
  --teardown      kill verglas-server, drop tables/catalog, prefix-scoped S3 delete,
                  stop the Polaris container

Options (also env; see README):
  --scale N            TPC-H scale factor (default 1)
  --tables a,b,c       subset (default all 8)
  --runs N             measurements per config (default 2)
  --vg-port N          Verglas S3 port; admin binds N+1 (default 8555)
  --bucket / --prefix  origin bucket + non-empty prefix (default hyperglas / bench/warming-demo)

Origin creds come from the ambient AWS_* env — load .env first:
  cp /path/to/.env benchmarks/warming/.env   # never committed; guarded here
  set -a; source benchmarks/warming/.env; set +a
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --all)          PHASE="all"; shift ;;
    --seed-only)    PHASE="seed"; shift ;;
    --measure-only) PHASE="measure"; shift ;;
    --report)       PHASE="report"; shift ;;
    --teardown)     PHASE="teardown"; shift ;;
    --scale)        SCALE="$2"; shift 2 ;;
    --tables)       TABLES="$2"; shift 2 ;;
    --runs)         RUNS="$2"; shift 2 ;;
    --vg-port)      VG_PORT="$2"; VG_ADMIN_PORT="$((VG_PORT + 1))"; shift 2 ;;
    --bucket)       BUCKET="$2"; shift 2 ;;
    --prefix)       PREFIX="$2"; shift 2 ;;
    -h|--help)      usage; exit 0 ;;
    *) echo "unknown flag: $1" >&2; usage; exit 2 ;;
  esac
done

# --------------------------------------------------------------------------- #
# Guards
# --------------------------------------------------------------------------- #

guard_prefix() {
  # Never operate at the bucket root: teardown lists-and-deletes the prefix.
  if [[ -z "$(printf '%s' "$PREFIX" | tr -d '/')" ]]; then
    echo "error: --prefix must be non-empty (never the bucket root)" >&2
    exit 2
  fi
}

need_origin_env() {
  if [[ -z "${AWS_ACCESS_KEY_ID:-}" || -z "${AWS_ENDPOINT:-}" ]]; then
    echo "error: origin creds required in the environment (AWS_ENDPOINT," >&2
    echo "       AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)." >&2
    echo "       Load your .env first: set -a; source .env; set +a" >&2
    exit 2
  fi
}

REGION="${AWS_REGION:-us-east-1}"

# --------------------------------------------------------------------------- #
# venv
# --------------------------------------------------------------------------- #

setup_venv() {
  if [[ ! -x "$VENV/bin/python" ]]; then
    echo "[setup] creating venv at $VENV" >&2
    "$(command -v python3.13 || command -v python3)" -m venv "$VENV"
  fi
  local stamp="$VENV/.requirements.sha"
  local newsha; newsha="$(shasum "$REQ" | awk '{print $1}')"
  if [[ ! -f "$stamp" || "$(cat "$stamp")" != "$newsha" ]]; then
    echo "[setup] installing pinned dependencies" >&2
    "$VENV/bin/pip" install -q --upgrade pip
    "$VENV/bin/pip" install -q -r "$REQ"
    echo "$newsha" > "$stamp"
  fi
}

PY() { "$VENV/bin/python" "$DRIVER" "$@"; }

common_args=(
  --catalog-uri "$CATALOG_URI"
  --catalog "$CATALOG"
  --namespace "$NAMESPACE"
  --tables "$TABLES"
)

# --------------------------------------------------------------------------- #
# Polaris container
# --------------------------------------------------------------------------- #

check_port_free() {
  if lsof -ti "tcp:$1" >/dev/null 2>&1; then
    echo "error: port $1 is already in use (lsof). Pick a free port." >&2
    exit 2
  fi
}

start_polaris() {
  if docker exec "$POLARIS_CONTAINER" curl -sf http://localhost:8182/q/health >/dev/null 2>&1; then
    echo "[polaris] already healthy" >&2; return 0
  fi
  docker rm -f "$POLARIS_CONTAINER" >/dev/null 2>&1 || true
  check_port_free "$POLARIS_PORT"; check_port_free "$POLARIS_MGMT_PORT"
  echo "[polaris] starting $POLARIS_IMAGE on :$POLARIS_PORT/:$POLARIS_MGMT_PORT" >&2
  docker run -d --name "$POLARIS_CONTAINER" \
    -p "${POLARIS_PORT}:8181" -p "${POLARIS_MGMT_PORT}:8182" \
    -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    -e AWS_REGION="$REGION" \
    `# OCI (and many S3-compatible stores) reject aws-chunked streaming PUTs (501);` \
    `# WHEN_REQUIRED disables it so Polaris's metadata.json writes succeed.` \
    -e AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED \
    -e AWS_RESPONSE_CHECKSUM_VALIDATION=WHEN_REQUIRED \
    -e POLARIS_BOOTSTRAP_CREDENTIALS="POLARIS,root,s3cr3t" \
    -e polaris.realm-context.realms="POLARIS" \
    -e quarkus.log.file.enabled=false \
    -e quarkus.otel.sdk.disabled=true \
    -e polaris.readiness.ignore-severe-issues=true \
    -e 'polaris.features."ALLOW_INSECURE_STORAGE_TYPES"=true' \
    -e 'polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"=["FILE","S3"]' \
    "$POLARIS_IMAGE" >/dev/null
  for _ in $(seq 1 120); do
    if docker exec "$POLARIS_CONTAINER" curl -sf http://localhost:8182/q/health >/dev/null 2>&1; then
      echo "[polaris] healthy" >&2; return 0
    fi
    if [[ "$(docker inspect -f '{{.State.Running}}' "$POLARIS_CONTAINER" 2>/dev/null)" != "true" ]]; then
      echo "error: Polaris exited during startup:" >&2
      docker logs "$POLARIS_CONTAINER" 2>&1 | tail -40 >&2; exit 1
    fi
    sleep 1
  done
  echo "error: Polaris did not become healthy within 120s" >&2
  docker logs "$POLARIS_CONTAINER" 2>&1 | tail -40 >&2; exit 1
}

stop_polaris() {
  docker rm -f "$POLARIS_CONTAINER" >/dev/null 2>&1 || true
}

# --------------------------------------------------------------------------- #
# verglas-server (host) — dedicated ports, explicit PID kill on teardown (#170)
# --------------------------------------------------------------------------- #

# $1=warming(on|off)  $2=cache-dir  $3=bearer-token(may be empty for off)
write_verglas-server_config() {
  local warming="$1" cache_dir="$2" token="$3"
  local cfg="$HERE/verglas-server.toml"
  {
    echo "[listen]"
    echo "s3_port = $VG_PORT"
    echo "admin_port = $VG_ADMIN_PORT"
    echo "[cache]"
    echo "dir = \"$cache_dir\""
    echo "capacity_bytes = \"$VG_DISK\""
    echo "dram_bytes = \"$VG_DRAM\""
    echo "[cache.warming]"
    echo "enabled = $([[ "$warming" == "on" ]] && echo true || echo false)"
    echo "[auth]"
    echo "access_key_id = \"$VG_KEY\""
    echo "secret_access_key = \"$VG_SECRET\""
    if [[ "$warming" == "on" ]]; then
      echo "[catalog]"
      echo "uri = \"$CATALOG_URI\""
      echo "poll_interval_secs = $POLL_SECS"
      echo "warehouse = \"$CATALOG\""
      echo "bearer_token = \"$token\""
    fi
  } > "$cfg"
  echo "$cfg"
}

start_verglas-server() {
  # $1=warming(on|off)  $2=cache-dir  $3=bearer-token
  [[ -x "$VERGLAS_SERVER_BIN" ]] || { echo "error: verglas-server not found at $VERGLAS_SERVER_BIN; run: cargo build --release -p verglas -p verglas-server" >&2; exit 2; }
  stop_verglas-server
  check_port_free "$VG_PORT"; check_port_free "$VG_ADMIN_PORT"
  rm -rf "$2"; mkdir -p "$2"
  local cfg; cfg="$(write_verglas-server_config "$1" "$2" "$3")"
  : > "$VG_LOG"
  "$VERGLAS_SERVER_BIN" --config "$cfg" >"$VG_LOG" 2>&1 &
  VG_PID=$!
  # Detach from job control so the eventual kill in stop_verglas-server does not print
  # a "Terminated" notice to the run transcript.
  disown "$VG_PID" 2>/dev/null || true
  for _ in $(seq 1 100); do
    if curl -sf "http://127.0.0.1:${VG_ADMIN_PORT}/admin/healthz" >/dev/null 2>&1; then
      echo "[verglas-server] ready (warming=$1, pid $VG_PID)" >&2; return 0
    fi
    if ! kill -0 "$VG_PID" 2>/dev/null; then
      echo "error: verglas-server exited during startup; see $VG_LOG" >&2; cat "$VG_LOG" >&2; exit 1
    fi
    sleep 0.2
  done
  echo "error: verglas-server admin never became healthy" >&2; cat "$VG_LOG" >&2; exit 1
}

stop_verglas-server() {
  # #170: verglas-server children can survive the parent's SIGTERM, so kill every PID
  # holding our S3/admin port plus the parent, escalating until the port frees.
  local attempt sig p pids
  [[ -n "$VG_PID" ]] && kill "$VG_PID" 2>/dev/null || true
  for attempt in 1 2 3 4 5 6; do
    pids="$(lsof -ti "tcp:${VG_PORT}" 2>/dev/null; lsof -ti "tcp:${VG_ADMIN_PORT}" 2>/dev/null; [[ -n "$VG_PID" ]] && pgrep -P "$VG_PID" 2>/dev/null || true)"
    pids="$(echo "$pids" | tr ' ' '\n' | sort -u | tr '\n' ' ')"
    [[ -z "$(echo "$pids" | tr -d '[:space:]')" ]] && break
    sig=TERM; [[ $attempt -ge 3 ]] && sig=KILL
    for p in $pids; do kill "-$sig" "$p" 2>/dev/null || true; done
    sleep 0.5
  done
  VG_PID=""
}

polaris_token() {
  curl -s -X POST "${CATALOG_URI}/v1/oauth/tokens" \
    -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t \
    -d scope=PRINCIPAL_ROLE:ALL | jq -r '.access_token'
}

num_tables() {
  if [[ -n "$TABLES" ]]; then echo "$TABLES" | tr ',' '\n' | grep -c .; else echo 8; fi
}

# Poll /admin/stats until the warmer reports >= N tables completed (or time out).
wait_for_warming() {
  local want="$1" deadline=$((SECONDS + 90)) done_n
  while (( SECONDS < deadline )); do
    done_n="$(curl -sf "http://127.0.0.1:${VG_ADMIN_PORT}/admin/stats" | jq -r '.warming.tables_completed // 0')"
    if [[ "$done_n" =~ ^[0-9]+$ ]] && (( done_n >= want )); then
      echo "[warming] tables_completed=$done_n (>=$want)" >&2
      curl -sf "http://127.0.0.1:${VG_ADMIN_PORT}/admin/stats" | jq -c '.warming' >&2
      return 0
    fi
    sleep 1
  done
  echo "error: warming did not complete $want table(s) in 90s" >&2
  curl -sf "http://127.0.0.1:${VG_ADMIN_PORT}/admin/stats" | jq -c '.warming' >&2
  return 1
}

walk_args=(
  --verglas-endpoint "http://127.0.0.1:${VG_PORT}"
  --verglas-access-key-id "$VG_KEY"
  --verglas-secret-access-key "$VG_SECRET"
  --verglas-region us-east-1
  --admin-endpoint "http://127.0.0.1:${VG_ADMIN_PORT}"
  --results "$RESULTS"
)

# --------------------------------------------------------------------------- #
# Phases
# --------------------------------------------------------------------------- #

do_seed() {
  need_origin_env; guard_prefix; setup_venv
  start_polaris
  echo "[bootstrap] creating catalog $CATALOG (storage -> origin)" >&2
  PY bootstrap "${common_args[@]}" --mgmt-uri "$MGMT_URI" --bucket "$BUCKET" --prefix "$PREFIX" \
    --origin-endpoint "$AWS_ENDPOINT" --origin-region "$REGION" >/dev/null
  echo "[seed] dbgen SF$SCALE -> Iceberg through Polaris (data on origin)" >&2
  PY seed "${common_args[@]}" --scale "$SCALE" --target-file-size "$TARGET_FILE_SIZE" \
    --origin-endpoint "$AWS_ENDPOINT" --origin-region "$REGION"
}

do_measure() {
  need_origin_env; guard_prefix; setup_venv
  start_polaris
  mkdir -p "$RESULTS_DIR"; : > "$RESULTS"
  local n; n="$(num_tables)"
  trap 'stop_verglas-server' EXIT
  for run in $(seq 1 "$RUNS"); do
    echo "" >&2; echo "===== run $run/$RUNS =====" >&2

    # --- Config B: warming ON. A fresh server starts against the PRE-EXISTING
    #     seeded tables: its first poll seeds the watched set, the coordinator's
    #     startup pass warms every table, and only then does a client walk. No
    #     commit is fired — this is the pure onboarding path (a commit-driven
    #     re-warm exercises the same warmer; see the #168 integration tests). ---
    local token; token="$(polaris_token)"
    start_verglas-server on "$VG_CACHE_ROOT/b-$run" "$token"
    echo "[B] server started; waiting for the startup warming pass" >&2
    wait_for_warming "$n"
    PY walk "${common_args[@]}" "${walk_args[@]}" --config B --run "$run"
    stop_verglas-server

    # --- Config A (cold baseline) + C (warm floor): warming OFF, fresh cache.
    #     First walk is genuine cold; the immediate re-run is the warm floor. ---
    start_verglas-server off "$VG_CACHE_ROOT/a-$run" ""
    PY walk "${common_args[@]}" "${walk_args[@]}" --config A --run "$run"
    PY walk "${common_args[@]}" "${walk_args[@]}" --config C --run "$run"
    stop_verglas-server
  done
  trap - EXIT
  do_report
}

do_report() {
  setup_venv
  PY report --results "$RESULTS" --out "$RESULTS_DIR/report.md" \
    --machine-note "$MACHINE_NOTE" --cache-note "$CACHE_NOTE" \
    --dataset-note "TPC-H SF$SCALE, tables=$([[ -n "$TABLES" ]] && echo "$TABLES" || echo all-8), through Polaris on origin S3"
}

do_teardown() {
  need_origin_env; guard_prefix; setup_venv
  stop_verglas-server
  start_polaris
  PY teardown "${common_args[@]}" --origin-endpoint "$AWS_ENDPOINT" --origin-region "$REGION" >/dev/null 2>&1 || true
  echo "[teardown] deleting s3://${BUCKET}/${PREFIX}/ (prefix-scoped)" >&2
  aws --endpoint-url "$AWS_ENDPOINT" s3 rm --recursive "s3://${BUCKET}/${PREFIX}/" >/dev/null 2>&1 || true
  stop_polaris
  echo "[teardown] done" >&2
}

case "$PHASE" in
  all)      do_seed; do_measure ;;
  seed)     do_seed ;;
  measure)  do_measure ;;
  report)   do_report ;;
  teardown) do_teardown ;;
esac
