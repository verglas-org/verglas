#!/usr/bin/env bash
# Polaris-over-Verglas catalog-IO benchmark (issue #171).
#
# Drives the OFFICIAL apache/polaris-tools Gatling suites against an unmodified
# Apache Polaris catalog service whose warehouse S3 storage is pointed two ways:
#   leg A (direct):  Polaris -> the origin S3 (OCI) directly.
#   leg B (verglas): Polaris -> the Verglas dev endpoint -> the same origin S3.
# Polaris is itself the S3 client: it writes metadata.json on createTable/commit
# and reads it on loadTable. Leg B routes exactly that metadata IO through
# Verglas, so #50's metadata pinning should accelerate the loadTable read path.
#
# One command per leg (the adoption story: a single storage-config change).
# Nothing here runs in PR CI. See README.md for every input and prerequisite.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ----- pinned versions (never floating; digests recorded in the README) ----- #
POLARIS_IMAGE="${POLARIS_IMAGE:-apache/polaris@sha256:9738b2052dea20aabf0cd42521424ff963fee41b0ee888fef9f512efb256602a}"   # apache/polaris:1.6.0
GATLING_JDK_IMAGE="${GATLING_JDK_IMAGE:-eclipse-temurin@sha256:1eeacc8c295ed4805f6ffead2417b1936aad296b02ea9e56b457230befc9e98d}"  # eclipse-temurin:21-jdk
POLARIS_TOOLS_REPO="${POLARIS_TOOLS_REPO:-https://github.com/apache/polaris-tools}"
POLARIS_TOOLS_COMMIT="${POLARIS_TOOLS_COMMIT:-91c4d09be13221e59401892ce35302bc8187f79e}"

# ----- inputs (env- or flag-overridable) ------------------------------------ #
BUCKET="${POLARIS_BUCKET:-hyperglas}"
PREFIX_BASE="${POLARIS_PREFIX:-bench/polaris}"     # per-leg prefix = <base>-<leg>
RUN_LABEL="${POLARIS_RUN_LABEL:-run1}"
MACHINE_NOTE="${POLARIS_MACHINE_NOTE:-unspecified machine}"
CACHE_NOTE="${POLARIS_CACHE_NOTE:-cache medium unspecified}"

# Dataset shape (the polaris-tools tree; defaults = 15 namespaces, 40 tables,
# 24 views — small, fast, cheap against a real origin). All configurable.
NS_WIDTH="${POLARIS_NS_WIDTH:-2}"
NS_DEPTH="${POLARIS_NS_DEPTH:-4}"
TABLES_PER_NS="${POLARIS_TABLES_PER_NS:-5}"
VIEWS_PER_NS="${POLARIS_VIEWS_PER_NS:-3}"
READ_WRITE_RATIO="${POLARIS_READ_WRITE_RATIO:-0.8}"   # ReadUpdateTreeDataset

# Verglas dev daemon (leg B). Ports: S3 on VG_PORT, admin on VG_PORT+1.
VG_PORT="${POLARIS_VG_PORT:-8455}"
VG_DRAM="${POLARIS_VG_DRAM:-1GB}"
VG_DISK="${POLARIS_VG_DISK:-20GB}"
VG_CACHE_DIR="${POLARIS_VG_CACHE_DIR:-/tmp/vg-polaris-cache}"
VERGLAS_BIN="${VERGLAS_BIN:-$HERE/../../target/release/verglas}"

# Polaris realm + fixed root principal (airgapped-bench convention).
POLARIS_REALM="${POLARIS_REALM:-POLARIS}"
POLARIS_CLIENT_ID="${POLARIS_CLIENT_ID:-root}"
POLARIS_CLIENT_SECRET="${POLARIS_CLIENT_SECRET:-s3cr3t}"

# Docker names.
NET="verglas-polaris-bench-net"
POLARIS_CONTAINER="verglas-polaris-bench"

TOOLS_DIR="$HERE/polaris-tools"
BENCH_DIR="$TOOLS_DIR/benchmarks"
GRADLE_CACHE="$HERE/.gradle-cache"
RESULTS_DIR="$HERE/results"
REPORT_PY="$HERE/polaris_report.py"

PHASE="all"

usage() {
  cat <<'EOF'
Usage: run.sh [--leg direct|verglas | --report | --teardown | --all] [options]

Phases:
  --leg direct     bring up Polaris pointed at the origin S3, run the suites
  --leg verglas    bring up Verglas + Polaris pointed at Verglas, run the suites
                   (the read suite runs twice: cold then warm)
  --report         merge results/direct.json + results/verglas.json into the table
  --teardown       prefix-scoped S3 delete (both legs) + container/network cleanup
  --all            direct leg, verglas leg, then report (default)

Options (also env, see README):
  --bucket NAME        origin bucket (default hyperglas)
  --prefix PATH        per-leg prefix base (default bench/polaris); MUST be non-empty
  --run-label LABEL    stamped on the report for two-run stability (default run1)
  --vg-port N          Verglas S3 port; admin binds N+1 (default 8455)
  --ns-width / --ns-depth / --tables-per-ns / --views-per-ns / --read-write-ratio

Origin creds come from the ambient AWS_* env (load .env first). The Verglas dev
keys are generated and parsed automatically from the daemon banner.
EOF
}

LEG=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --leg)              LEG="$2"; PHASE="leg"; shift 2 ;;
    --report)           PHASE="report"; shift ;;
    --teardown)         PHASE="teardown"; shift ;;
    --all)              PHASE="all"; shift ;;
    --bucket)           BUCKET="$2"; shift 2 ;;
    --prefix)           PREFIX_BASE="$2"; shift 2 ;;
    --run-label)        RUN_LABEL="$2"; shift 2 ;;
    --vg-port)          VG_PORT="$2"; shift 2 ;;
    --ns-width)         NS_WIDTH="$2"; shift 2 ;;
    --ns-depth)         NS_DEPTH="$2"; shift 2 ;;
    --tables-per-ns)    TABLES_PER_NS="$2"; shift 2 ;;
    --views-per-ns)     VIEWS_PER_NS="$2"; shift 2 ;;
    --read-write-ratio) READ_WRITE_RATIO="$2"; shift 2 ;;
    -h|--help)          usage; exit 0 ;;
    *) echo "unknown flag: $1" >&2; usage; exit 2 ;;
  esac
done

# --------------------------------------------------------------------------- #
# Guards
# --------------------------------------------------------------------------- #

guard_prefix() {
  # Never operate at the bucket root: teardown lists-and-deletes the prefix.
  if [[ -z "$(printf '%s' "$PREFIX_BASE" | tr -d '/')" ]]; then
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

leg_prefix() { echo "${PREFIX_BASE}-$1"; }   # $1 = direct|verglas

# --------------------------------------------------------------------------- #
# polaris-tools checkout (pinned; gitignored)
# --------------------------------------------------------------------------- #

clone_tools() {
  if [[ ! -d "$TOOLS_DIR/.git" ]]; then
    echo "[setup] cloning polaris-tools @ $POLARIS_TOOLS_COMMIT" >&2
    git clone "$POLARIS_TOOLS_REPO" "$TOOLS_DIR" >&2
  fi
  git -C "$TOOLS_DIR" fetch --depth 1 origin "$POLARIS_TOOLS_COMMIT" >&2 2>/dev/null || \
    git -C "$TOOLS_DIR" fetch origin >&2
  git -C "$TOOLS_DIR" checkout -q "$POLARIS_TOOLS_COMMIT"
  echo "[setup] polaris-tools at $(git -C "$TOOLS_DIR" rev-parse --short HEAD)" >&2
  patch_gatling_indicators
}

# The suite's gatling.conf reports percentiles 25/50/75/99; the issue asks for
# p50/p95/p99. This is a REPORTING-ONLY override of the charting indicators to
# 50/75/95/99 — it changes no request, injection, ratio, or assertion, so the
# workload the official suite drives is untouched. Idempotent.
patch_gatling_indicators() {
  local conf="$BENCH_DIR/src/gatling/resources/gatling.conf"
  [[ -f "$conf" ]] || return 0
  sed -i.bak -E \
    -e 's/(percentile1[[:space:]]*=[[:space:]]*)[0-9]+/\150/' \
    -e 's/(percentile2[[:space:]]*=[[:space:]]*)[0-9]+/\175/' \
    -e 's/(percentile3[[:space:]]*=[[:space:]]*)[0-9]+/\195/' \
    -e 's/(percentile4[[:space:]]*=[[:space:]]*)[0-9]+/\199/' \
    "$conf"
  rm -f "$conf.bak"
  echo "[setup] gatling.conf report indicators set to 50/75/95/99 (reporting-only)" >&2
}

# --------------------------------------------------------------------------- #
# Verglas dev daemon (leg B only)
# --------------------------------------------------------------------------- #

VG_PARENT_PID=""
VG_KEY=""
VG_SECRET=""
VG_LOG="$HERE/verglas-dev.log"

check_port_free() {
  local port="$1"
  if lsof -ti "tcp:${port}" >/dev/null 2>&1; then
    echo "error: port ${port} is already in use (lsof). Pick a free --vg-port." >&2
    exit 2
  fi
}

start_verglas() {
  [[ -x "$VERGLAS_BIN" ]] || { echo "error: verglas binary not found at $VERGLAS_BIN; run: cargo build --release -p verglas -p verglasd" >&2; exit 2; }
  # #170: use ports nothing else holds. Check both S3 and admin ports up front.
  check_port_free "$VG_PORT"
  check_port_free "$((VG_PORT + 1))"
  rm -rf "$VG_CACHE_DIR"; mkdir -p "$VG_CACHE_DIR"
  echo "[verglas] starting dev daemon on 127.0.0.1:${VG_PORT} (admin $((VG_PORT + 1)))" >&2
  : > "$VG_LOG"
  "$VERGLAS_BIN" dev --port "$VG_PORT" --cache-dir "$VG_CACHE_DIR" \
    --dram "$VG_DRAM" --cache-size "$VG_DISK" >"$VG_LOG" 2>&1 &
  VG_PARENT_PID=$!
  # Wait for the admin health endpoint.
  local admin="http://127.0.0.1:$((VG_PORT + 1))/admin/healthz"
  for _ in $(seq 1 100); do
    if curl -sf "$admin" >/dev/null 2>&1; then break; fi
    if ! kill -0 "$VG_PARENT_PID" 2>/dev/null; then
      echo "error: verglas dev exited during startup; see $VG_LOG" >&2; cat "$VG_LOG" >&2; exit 1
    fi
    sleep 0.2
  done
  # Parse the generated dev keys from the banner.
  VG_KEY="$(grep -o 'access_key_id=[^ ]*' "$VG_LOG" | head -1 | cut -d= -f2 | tr -d '[:space:]')"
  VG_SECRET="$(grep -o 'secret_access_key=[^ ]*' "$VG_LOG" | head -1 | cut -d= -f2 | tr -d '[:space:]')"
  if [[ -z "$VG_KEY" || -z "$VG_SECRET" ]]; then
    echo "error: could not parse Verglas dev keys from $VG_LOG" >&2; exit 1
  fi
  echo "[verglas] ready; dev key ${VG_KEY}" >&2
}

stop_verglas() {
  [[ -n "$VG_PARENT_PID" ]] || return 0
  echo "[verglas] stopping dev daemon (#170: killing verglasd children explicitly)" >&2
  # #170: verglasd children survive the parent's SIGTERM, so the parent alone is
  # never enough. Loop with escalating signals against every PID holding our S3
  # or admin port (the verglasd listener) plus the dev parent, until the S3 port
  # is actually free — then nothing is orphaned for the next leg's port guard.
  kill "$VG_PARENT_PID" 2>/dev/null || true
  local attempt sig p pids
  for attempt in 1 2 3 4 5 6; do
    pids="$(lsof -ti "tcp:${VG_PORT}" 2>/dev/null; lsof -ti "tcp:$((VG_PORT + 1))" 2>/dev/null; pgrep -P "$VG_PARENT_PID" 2>/dev/null || true)"
    pids="$(echo "$pids" | tr ' ' '\n' | sort -u | tr '\n' ' ')"
    [[ -z "$(echo "$pids" | tr -d '[:space:]')" ]] && break
    sig=TERM; [[ $attempt -ge 3 ]] && sig=KILL
    for p in $pids; do kill "-$sig" "$p" 2>/dev/null || true; done
    sleep 0.5
  done
  if lsof -ti "tcp:${VG_PORT}" >/dev/null 2>&1; then
    echo "warning: a process still holds port ${VG_PORT} after teardown" >&2
  fi
  VG_PARENT_PID=""
}

admin_stats_json() {
  curl -sf "http://127.0.0.1:$((VG_PORT + 1))/admin/stats" 2>/dev/null || echo '{}'
}

purge_verglas_cache() {
  curl -sf -X POST "http://127.0.0.1:$((VG_PORT + 1))/cache/purge" >/dev/null 2>&1 || true
}

# --------------------------------------------------------------------------- #
# Polaris container
# --------------------------------------------------------------------------- #

start_polaris() {
  # $1=access_key $2=secret_key $3=region  (the leg's S3 credentials for
  # Polaris's own server-side FileIO — used to write/read metadata.json).
  local akey="$1" skey="$2" region="$3"
  docker network create "$NET" >/dev/null 2>&1 || true
  docker rm -f "$POLARIS_CONTAINER" >/dev/null 2>&1 || true
  echo "[polaris] starting $POLARIS_IMAGE (realm $POLARIS_REALM)" >&2
  docker run -d --name "$POLARIS_CONTAINER" --network "$NET" \
    -e AWS_ACCESS_KEY_ID="$akey" \
    -e AWS_SECRET_ACCESS_KEY="$skey" \
    -e AWS_REGION="$region" \
    `# AWS SDK v2's default checksum uses aws-chunked streaming PUTs, which OCI` \
    `# (and many S3-compatible stores) reject with 501. WHEN_REQUIRED disables it` \
    `# so metadata.json writes succeed. Applied to BOTH legs — a fair constant.` \
    -e AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED \
    -e AWS_RESPONSE_CHECKSUM_VALIDATION=WHEN_REQUIRED \
    -e POLARIS_BOOTSTRAP_CREDENTIALS="${POLARIS_REALM},${POLARIS_CLIENT_ID},${POLARIS_CLIENT_SECRET}" \
    -e polaris.realm-context.realms="$POLARIS_REALM" \
    -e quarkus.log.file.enabled=false \
    -e quarkus.otel.sdk.disabled=true \
    -e polaris.readiness.ignore-severe-issues=true \
    -e 'polaris.features."ALLOW_INSECURE_STORAGE_TYPES"=true' \
    -e 'polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"=["FILE","S3"]' \
    "$POLARIS_IMAGE" >/dev/null
  # Wait for readiness on the management port (curl lives in the image).
  for _ in $(seq 1 120); do
    if docker exec "$POLARIS_CONTAINER" curl -sf http://localhost:8182/q/health >/dev/null 2>&1; then
      echo "[polaris] healthy" >&2; return 0
    fi
    if [[ "$(docker inspect -f '{{.State.Running}}' "$POLARIS_CONTAINER" 2>/dev/null)" != "true" ]]; then
      echo "error: Polaris container exited during startup:" >&2
      docker logs "$POLARIS_CONTAINER" 2>&1 | tail -40 >&2; exit 1
    fi
    sleep 1
  done
  echo "error: Polaris did not become healthy within 120s" >&2
  docker logs "$POLARIS_CONTAINER" 2>&1 | tail -40 >&2; exit 1
}

stop_polaris() {
  docker rm -f "$POLARIS_CONTAINER" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}

# --------------------------------------------------------------------------- #
# Gatling application.conf + run
# --------------------------------------------------------------------------- #

write_appconf() {
  # $1=endpoint $2=region $3=prefix  -> Polaris storage-config-info for the leg.
  local endpoint="$1" region="$2" prefix="$3"
  local base="s3://${BUCKET}/${prefix}"
  # The catalog base becomes "<base>/C_0" (polaris-tools appends the catalog
  # name); the read suite asserts allowedLocations[0] == that, so pin it here.
  local allowed="${base}/C_0"
  local storage
  storage="{\"storageType\":\"S3\",\"allowedLocations\":[\"${allowed}\"],\"endpoint\":\"${endpoint}\",\"pathStyleAccess\":true,\"stsUnavailable\":true,\"region\":\"${region}\"}"
  cat > "$BENCH_DIR/application.conf" <<EOF
http {
  base-url = "http://${POLARIS_CONTAINER}:8181"
  headers { Polaris-Realm = "${POLARIS_REALM}" }
}
auth {
  client-id = "${POLARIS_CLIENT_ID}"
  client-secret = "${POLARIS_CLIENT_SECRET}"
}
dataset.tree {
  num-catalogs = 1
  namespace-width = ${NS_WIDTH}
  namespace-depth = ${NS_DEPTH}
  tables-per-namespace = ${TABLES_PER_NS}
  views-per-namespace = ${VIEWS_PER_NS}
  default-base-location = "${base}"
  storage-config-info = """${storage}"""
}
workload {
  read-update-tree-dataset { read-write-ratio = ${READ_WRITE_RATIO} }
}
EOF
  echo "[gatling] application.conf -> endpoint=${endpoint} base=${base}" >&2
}

gatling_run() {
  # $1=Simulation class shortname  $2=results label  $3=results json  $4=leg
  local sim="$1" label="$2" results="$3" leg="$4"
  mkdir -p "$GRADLE_CACHE"
  echo "[gatling] running $sim ($label)" >&2
  docker run --rm --network "$NET" \
    -v "$TOOLS_DIR":/work \
    -v "$GRADLE_CACHE":/gradle-cache \
    -e GRADLE_USER_HOME=/gradle-cache \
    -w /work/benchmarks \
    "$GATLING_JDK_IMAGE" \
    ./gradlew --no-daemon --console=plain gatlingRun \
      --simulation "org.apache.polaris.benchmarks.simulations.${sim}" \
      -Dconfig.file=/work/benchmarks/application.conf
  # Newest report dir for this simulation (Gatling lowercases the class name).
  local simlc rdir
  simlc="$(echo "$sim" | tr '[:upper:]' '[:lower:]')"
  rdir="$(ls -dt "$BENCH_DIR"/build/reports/gatling/${simlc}-* 2>/dev/null | head -1 || true)"
  [[ -n "$rdir" ]] || { echo "error: no Gatling report dir for $sim" >&2; exit 1; }
  python3 "$REPORT_PY" extract --leg "$leg" --label "$label" \
    --report-dir "$rdir" --results "$results"
}

# --------------------------------------------------------------------------- #
# Legs
# --------------------------------------------------------------------------- #

seed_context() {
  # $1=results json $2=leg $3=endpoint  -> stamp machine/cache/version context.
  python3 - "$1" "$2" "$3" "$POLARIS_IMAGE" "$POLARIS_TOOLS_COMMIT" "$BUCKET" \
      "$(leg_prefix direct)" "$(leg_prefix verglas)" "$MACHINE_NOTE" "$CACHE_NOTE" \
      "$RUN_LABEL" "$NS_WIDTH" "$NS_DEPTH" "$TABLES_PER_NS" "$VIEWS_PER_NS" <<'PY'
import json, sys
p, leg, endpoint, img, commit, bucket, pd, pv, mach, cache, label, w, d, t, v = sys.argv[1:16]
try:
    with open(p) as f: doc = json.load(f)
except FileNotFoundError:
    doc = {"leg": leg, "context": {}, "simulations": {}}
doc["context"] = {
    "machine": mach, "origin": "OCI Object Storage (S3-compatible)",
    "bucket_prefix_direct": f"s3://{bucket}/{pd}", "bucket_prefix_verglas": f"s3://{bucket}/{pv}",
    "verglas_endpoint": endpoint, "verglas_cache_budget": cache,
    "polaris_image": img, "polaris_tools_commit": commit,
    "polaris_cache_config": "Polaris defaults (entity cache on; no extra metadata caching configured)",
    "dataset": f"tree width={w} depth={d} tables/ns={t} views/ns={v}",
    "sampling": "Gatling closed-model injection per suite defaults; percentiles p50/p95/p99",
    "run_label": label,
}
with open(p, "w") as f: json.dump(doc, f, indent=2)
PY
}

run_leg_direct() {
  guard_prefix; need_origin_env; clone_tools
  local prefix; prefix="$(leg_prefix direct)"
  local results="$RESULTS_DIR/direct.json"; rm -f "$results"
  trap 'stop_polaris' EXIT
  start_polaris "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" "${AWS_REGION:-us-east-1}"
  write_appconf "$AWS_ENDPOINT" "${AWS_REGION:-us-east-1}" "$prefix"
  gatling_run CreateTreeDataset      CreateTreeDataset      "$results" direct
  gatling_run ReadTreeDataset        ReadTreeDataset        "$results" direct
  gatling_run ReadUpdateTreeDataset  ReadUpdateTreeDataset  "$results" direct
  seed_context "$results" direct "$AWS_ENDPOINT"
  stop_polaris; trap - EXIT
  echo "[leg direct] done -> $results" >&2
}

run_leg_verglas() {
  guard_prefix; need_origin_env; clone_tools
  local prefix; prefix="$(leg_prefix verglas)"
  local results="$RESULTS_DIR/verglas.json"; rm -f "$results"
  local endpoint="http://host.docker.internal:${VG_PORT}"
  trap 'stop_polaris; stop_verglas' EXIT
  start_verglas
  start_polaris "$VG_KEY" "$VG_SECRET" "us-east-1"
  write_appconf "$endpoint" "us-east-1" "$prefix"
  # Write path THROUGH Verglas: createTable writes metadata.json via the endpoint.
  gatling_run CreateTreeDataset "CreateTreeDataset" "$results" verglas
  # Cold read: purge so the loadTable metadata reads are genuine first-touch.
  purge_verglas_cache
  gatling_run ReadTreeDataset "ReadTreeDataset.cold" "$results" verglas
  # Warm read: immediate re-run; #50's pinned metadata should now serve loadTable.
  gatling_run ReadTreeDataset "ReadTreeDataset.warm" "$results" verglas
  # Mixed read/write (commits new metadata.json alongside reads).
  gatling_run ReadUpdateTreeDataset "ReadUpdateTreeDataset" "$results" verglas
  # Capture the warm-leg meta counters (the mechanism evidence).
  python3 - "$results" "$(admin_stats_json)" <<'PY'
import json, sys
p = sys.argv[1]
with open(p) as f: doc = json.load(f)
doc["admin_stats_warm"] = json.loads(sys.argv[2]) if sys.argv[2].strip() else {}
with open(p, "w") as f: json.dump(doc, f, indent=2)
PY
  seed_context "$results" verglas "$endpoint"
  stop_polaris; stop_verglas; trap - EXIT
  echo "[leg verglas] done -> $results" >&2
}

report() {
  python3 "$REPORT_PY" table \
    --direct "$RESULTS_DIR/direct.json" \
    --verglas "$RESULTS_DIR/verglas.json" \
    --out "$RESULTS_DIR/comparison-${RUN_LABEL}.md"
}

teardown() {
  guard_prefix; need_origin_env
  for leg in direct verglas; do
    local p; p="$(leg_prefix "$leg")"
    echo "[teardown] deleting s3://${BUCKET}/${p}/ (prefix-scoped)" >&2
    aws --endpoint-url "$AWS_ENDPOINT" s3 rm --recursive "s3://${BUCKET}/${p}/" >&2 || true
  done
  stop_polaris; stop_verglas
  echo "[teardown] containers + network removed" >&2
}

# --------------------------------------------------------------------------- #
# Dispatch
# --------------------------------------------------------------------------- #

mkdir -p "$RESULTS_DIR"
case "$PHASE" in
  leg)
    case "$LEG" in
      direct)  run_leg_direct ;;
      verglas) run_leg_verglas ;;
      *) echo "error: --leg must be direct or verglas" >&2; exit 2 ;;
    esac ;;
  report)   report ;;
  teardown) teardown ;;
  all)      run_leg_direct; run_leg_verglas; report ;;
  *) usage; exit 2 ;;
esac
