#!/usr/bin/env bash
# TPC-H-over-Iceberg endpoint-vs-direct benchmark (issue #136).
#
# One entrypoint: idempotent venv setup, then generate -> query -> report over
# an Iceberg dataset written THROUGH the Verglas endpoint, comparing three legs
# (direct-to-origin, Verglas cold, Verglas warm). Nothing here runs in CI.
#
# See README.md for prerequisites, every input, and a copy-paste invocation.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$HERE/.venv"
DRIVER="$HERE/tpch_bench.py"

# ---- defaults (override via flags or env) --------------------------------- #
SCALE="${TPCH_SCALE:-1}"
BUCKET="${TPCH_BUCKET:-hyperglas}"
PREFIX="${TPCH_PREFIX:-}"                 # resolved below once SCALE is known
NAMESPACE="${TPCH_NAMESPACE:-tpch}"
CATALOG_DB="${TPCH_CATALOG_DB:-$HERE/catalog.db}"
READ_MODE="${TPCH_READ_MODE:-auto}"
CACHE_NOTE="${TPCH_CACHE_NOTE:-daemon --cache-dir (medium unspecified)}"
PROFILE="${TPCH_PROFILE:-unspecified}"        # dram-resident | nvme-resident | constrained
ADMIN_ENDPOINT="${TPCH_ADMIN_ENDPOINT:-}"     # daemon /admin/stats URL; empty derives it

# Verglas endpoint + dev keys (from the `verglas dev` banner). The dev command
# also exports these env names, so they can be sourced instead of flagged.
VERGLAS_ENDPOINT="${VERGLAS_ENDPOINT:-http://127.0.0.1:8333}"
VG_KEY="${VERGLAS_DEV_ACCESS_KEY_ID:-}"
VG_SECRET="${VERGLAS_DEV_SECRET_ACCESS_KEY:-}"
VG_REGION="${VERGLAS_DEV_REGION:-us-east-1}"

PHASE="all"                               # all | seed | query | teardown
JSON=""

usage() {
  cat <<'EOF'
Usage: run.sh [options]

Phases (default: all = seed then query):
  --seed-only        generate + write the Iceberg tables through Verglas, then stop
  --query-only       run the 22 queries x three legs and report (tables must exist)
  --teardown         delete the prefix and the SQLite catalog, then stop

Inputs:
  --scale N          TPC-H scale factor (default 1)
  --bucket NAME      origin bucket (default hyperglas)
  --prefix PATH      table prefix within the bucket (default bench/tpch-sf<SCALE>);
                     MUST be non-empty — never the bucket root
  --endpoint URL     Verglas S3 endpoint (default http://127.0.0.1:8333)
  --access-key-id K  Verglas dev access key id  (or env VERGLAS_DEV_ACCESS_KEY_ID)
  --secret-key S     Verglas dev secret key      (or env VERGLAS_DEV_SECRET_ACCESS_KEY)
  --read-mode MODE   auto | duckdb-iceberg | pyiceberg-arrow (default auto)
  --cache-note TEXT  cache-medium disclosure for the report
  --profile LABEL    tier profile: dram-resident | nvme-resident | constrained
  --admin-endpoint U daemon /admin/stats URL (cache budgets + counters);
                     empty derives it from the endpoint's next port
  --json             emit machine-readable JSON

The direct-to-origin leg uses the ambient AWS_* environment (AWS_ENDPOINT,
AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION) — load your .env first.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --seed-only)      PHASE="seed"; shift ;;
    --query-only)     PHASE="query"; shift ;;
    --teardown)       PHASE="teardown"; shift ;;
    --scale)          SCALE="$2"; shift 2 ;;
    --bucket)         BUCKET="$2"; shift 2 ;;
    --prefix)         PREFIX="$2"; shift 2 ;;
    --endpoint)       VERGLAS_ENDPOINT="$2"; shift 2 ;;
    --access-key-id)  VG_KEY="$2"; shift 2 ;;
    --secret-key)     VG_SECRET="$2"; shift 2 ;;
    --read-mode)      READ_MODE="$2"; shift 2 ;;
    --cache-note)     CACHE_NOTE="$2"; shift 2 ;;
    --profile)        PROFILE="$2"; shift 2 ;;
    --admin-endpoint) ADMIN_ENDPOINT="$2"; shift 2 ;;
    --json)           JSON="--json"; shift ;;
    -h|--help)        usage; exit 0 ;;
    *) echo "unknown flag: $1" >&2; usage; exit 2 ;;
  esac
done

# Resolve the default prefix now that SCALE is final; enforce non-empty here so
# the guard fires before any credential is touched.
if [[ -z "$PREFIX" ]]; then
  PREFIX="bench/tpch-sf${SCALE}"
fi
if [[ -z "$(printf '%s' "$PREFIX" | tr -d '/')" ]]; then
  echo "error: --prefix must be non-empty (never the bucket root)" >&2
  exit 2
fi

# ---- idempotent venv ------------------------------------------------------ #
if [[ ! -x "$VENV/bin/python" ]]; then
  echo "[setup] creating venv at $VENV" >&2
  PYBIN="$(command -v python3.13 || command -v python3)"
  "$PYBIN" -m venv "$VENV"
fi
# Install/upgrade deps only when requirements changed (stamp-file guard).
STAMP="$VENV/.requirements.sha"
REQ="$HERE/requirements.txt"
NEWSHA="$(shasum "$REQ" | awk '{print $1}')"
if [[ ! -f "$STAMP" || "$(cat "$STAMP")" != "$NEWSHA" ]]; then
  echo "[setup] installing pinned dependencies" >&2
  "$VENV/bin/pip" install -q --upgrade pip
  "$VENV/bin/pip" install -q -r "$REQ"
  echo "$NEWSHA" > "$STAMP"
fi
PY="$VENV/bin/python"

# ---- credential checks ---------------------------------------------------- #
need_verglas_keys() {
  if [[ -z "$VG_KEY" || -z "$VG_SECRET" ]]; then
    echo "error: Verglas dev keys required (--access-key-id/--secret-key or" >&2
    echo "       VERGLAS_DEV_ACCESS_KEY_ID / VERGLAS_DEV_SECRET_ACCESS_KEY)." >&2
    echo "       Copy them from the 'verglas dev' banner." >&2
    exit 2
  fi
}
need_origin_keys() {
  if [[ -z "${AWS_ACCESS_KEY_ID:-}" || -z "${AWS_ENDPOINT:-}" ]]; then
    echo "error: origin creds required in the environment (AWS_ENDPOINT," >&2
    echo "       AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)." >&2
    echo "       Load your .env first: set -a; source .env; set +a" >&2
    exit 2
  fi
}

common_args=(
  --scale "$SCALE"
  --bucket "$BUCKET"
  --prefix "$PREFIX"
  --namespace "$NAMESPACE"
  --catalog-db "$CATALOG_DB"
  --verglas-endpoint "$VERGLAS_ENDPOINT"
  --verglas-access-key-id "$VG_KEY"
  --verglas-secret-access-key "$VG_SECRET"
  --verglas-region "$VG_REGION"
  --read-mode "$READ_MODE"
  --cache-note "$CACHE_NOTE"
  --profile "$PROFILE"
  --admin-endpoint "$ADMIN_ENDPOINT"
)

run_generate() { need_verglas_keys; "$PY" "$DRIVER" generate "${common_args[@]}" $JSON; }
run_query()    { need_verglas_keys; need_origin_keys; "$PY" "$DRIVER" query "${common_args[@]}" $JSON; }
run_teardown() { need_origin_keys;  "$PY" "$DRIVER" teardown "${common_args[@]}" $JSON; }

echo "[run] phase=$PHASE scale=SF$SCALE bucket=$BUCKET prefix=$PREFIX endpoint=$VERGLAS_ENDPOINT" >&2

case "$PHASE" in
  seed)     run_generate ;;
  query)    run_query ;;
  teardown) run_teardown ;;
  all)      run_generate; run_query ;;
esac
