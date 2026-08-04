#!/usr/bin/env bash
# Engine-matrix result-diffing harness (issue #23).
#
# One entrypoint: idempotent venv setup, then a phase — build the Iceberg
# fixture into MinIO, run the DuckDB + Polars matrix (direct vs Verglas, miss
# and hit paths) diffing results at the Arrow level, or the MinIO-free
# comparator selftest. THIS RUNS IN PR CI (unlike benchmarks/); keep it fast.
#
# See README.md for prerequisites, every input, and a copy-paste invocation.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$HERE/.venv"
DRIVER="$HERE/engine_matrix.py"

# ---- defaults (override via flags or env) --------------------------------- #
BUCKET="${MATRIX_BUCKET:-verglas-test}"
PREFIX="${MATRIX_PREFIX:-engine-matrix}"
NAMESPACE="${MATRIX_NAMESPACE:-matrix}"
CATALOG_DB="${MATRIX_CATALOG_DB:-$HERE/catalog.db}"
SIDECAR="${MATRIX_SIDECAR:-$HERE/fixture.json}"

# Verglas endpoint + dev keys (static config for the CI daemon).
VERGLAS_ENDPOINT="${VERGLAS_ENDPOINT:-http://127.0.0.1:8333}"
VG_KEY="${VERGLAS_ACCESS_KEY_ID:-}"
VG_SECRET="${VERGLAS_SECRET_ACCESS_KEY:-}"
VG_REGION="${VERGLAS_REGION:-us-east-1}"

# Direct-to-MinIO origin (ambient AWS_* from the environment).
ORIGIN_ENDPOINT="${AWS_ENDPOINT:-}"
ORIGIN_KEY="${AWS_ACCESS_KEY_ID:-}"
ORIGIN_SECRET="${AWS_SECRET_ACCESS_KEY:-}"
ORIGIN_REGION="${AWS_REGION:-us-east-1}"

PHASE="all"                                # all | fixture | matrix | selftest
INJECT_FAULT=""
VARIANT=""                                 # empty = all three variants

# The three #23 fixture variants. Parquet is built by pyiceberg; Avro and mixed
# are built by the plain-Java iceberg-core builder in fixture-jvm/ (pyiceberg and
# DuckDB write Parquet data files only). The matrix reads all three identically.
ALL_VARIANTS=(parquet avro mixed)
JAR="$HERE/fixture-jvm/build/libs/fixture-jvm-all.jar"
JAVA_BIN="${JAVA:-java}"

usage() {
  cat <<'EOF'
Usage: run.sh [options]

Phases (default: all = fixture then matrix, for every variant):
  --fixture-only     build the Iceberg fixtures into MinIO, then stop
  --matrix-only      run the DuckDB + Polars matrix and diff (fixtures must exist)
  --selftest         run the MinIO-free comparator selftest, then stop

Inputs:
  --variant NAME         restrict to one variant: parquet|avro|mixed (default all)
  --bucket NAME          origin bucket (default verglas-test)
  --prefix PATH          fixture prefix within the bucket (default engine-matrix;
                         each variant lives under <prefix>-<variant>)
  --endpoint URL         Verglas S3 endpoint (default http://127.0.0.1:8333)
  --access-key-id K      Verglas access key id  (or env VERGLAS_ACCESS_KEY_ID)
  --secret-key S         Verglas secret key      (or env VERGLAS_SECRET_ACCESS_KEY)
  --inject-fault KIND    drop-row|mutate-cell|wrong-schema — corrupt the Verglas
                         leg to PROVE the tripwire fires (PR evidence only)

The Avro and mixed variants need the JVM builder jar (fixture-jvm-all.jar); if it
is missing this script builds it with ./fixture-jvm/gradlew shadowJar. In CI the
jar is built once in a cached step. The direct-to-MinIO leg uses the ambient
AWS_* environment (AWS_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
AWS_REGION).
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fixture-only)   PHASE="fixture"; shift ;;
    --matrix-only)    PHASE="matrix"; shift ;;
    --selftest)       PHASE="selftest"; shift ;;
    --variant)        VARIANT="$2"; shift 2 ;;
    --bucket)         BUCKET="$2"; shift 2 ;;
    --prefix)         PREFIX="$2"; shift 2 ;;
    --endpoint)       VERGLAS_ENDPOINT="$2"; shift 2 ;;
    --access-key-id)  VG_KEY="$2"; shift 2 ;;
    --secret-key)     VG_SECRET="$2"; shift 2 ;;
    --inject-fault)   INJECT_FAULT="$2"; shift 2 ;;
    -h|--help)        usage; exit 0 ;;
    *) echo "unknown flag: $1" >&2; usage; exit 2 ;;
  esac
done

# Which variants to run this invocation.
if [[ -n "$VARIANT" ]]; then
  VARIANTS=("$VARIANT")
else
  VARIANTS=("${ALL_VARIANTS[@]}")
fi

# ---- idempotent venv ------------------------------------------------------ #
if [[ ! -x "$VENV/bin/python" ]]; then
  echo "[setup] creating venv at $VENV" >&2
  PYBIN="$(command -v python3.13 || command -v python3)"
  "$PYBIN" -m venv "$VENV"
fi
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
    echo "error: Verglas keys required (--access-key-id/--secret-key or" >&2
    echo "       VERGLAS_ACCESS_KEY_ID / VERGLAS_SECRET_ACCESS_KEY)." >&2
    exit 2
  fi
}
need_origin_keys() {
  if [[ -z "$ORIGIN_KEY" || -z "$ORIGIN_ENDPOINT" ]]; then
    echo "error: origin creds required (AWS_ENDPOINT, AWS_ACCESS_KEY_ID," >&2
    echo "       AWS_SECRET_ACCESS_KEY, AWS_REGION)." >&2
    exit 2
  fi
}

# Per-variant paths: each variant is isolated under its own bucket prefix, its
# own sidecar, and its own catalog db so the three tables never collide.
variant_prefix()  { echo "${PREFIX}-$1"; }
variant_sidecar() { echo "$HERE/fixture-$1.json"; }
variant_catalog() { echo "$HERE/catalog-$1.db"; }

# Common Python-driver args shared by fixture (parquet) and matrix, for a variant.
py_args() {
  local variant="$1"
  echo \
    --variant "$variant" \
    --bucket "$BUCKET" \
    --prefix "$(variant_prefix "$variant")" \
    --namespace "$NAMESPACE" \
    --catalog-db "$(variant_catalog "$variant")" \
    --sidecar "$(variant_sidecar "$variant")" \
    --verglas-endpoint "$VERGLAS_ENDPOINT" \
    --verglas-access-key-id "$VG_KEY" \
    --verglas-secret-access-key "$VG_SECRET" \
    --verglas-region "$VG_REGION" \
    --origin-endpoint "$ORIGIN_ENDPOINT" \
    --origin-access-key-id "$ORIGIN_KEY" \
    --origin-secret-access-key "$ORIGIN_SECRET" \
    --origin-region "$ORIGIN_REGION"
}

# Build the JVM fixture-builder jar if it is not already present (CI builds it
# once in a cached step; locally this is the convenience path).
ensure_jar() {
  if [[ -f "$JAR" ]]; then return 0; fi
  if [[ -x "$HERE/fixture-jvm/gradlew" ]]; then
    echo "[setup] building fixture-jvm jar (one-time)" >&2
    ( cd "$HERE/fixture-jvm" && ./gradlew --no-daemon shadowJar >&2 )
  else
    echo "error: $JAR missing and fixture-jvm/gradlew not found." >&2
    exit 2
  fi
}

# Build one variant's fixture into the origin: pyiceberg for parquet, the JVM
# iceberg-core builder for avro/mixed (which pyiceberg and DuckDB cannot write).
run_fixture() {
  local variant="$1"
  need_origin_keys
  if [[ "$variant" == "parquet" ]]; then
    # shellcheck disable=SC2046
    "$PY" "$DRIVER" fixture $(py_args "$variant")
  else
    ensure_jar
    "$JAVA_BIN" -jar "$JAR" \
      --variant "$variant" \
      --bucket "$BUCKET" \
      --prefix "$(variant_prefix "$variant")" \
      --namespace "$NAMESPACE" \
      --table events \
      --catalog-db "$(variant_catalog "$variant")" \
      --sidecar "$(variant_sidecar "$variant")" \
      --endpoint "$ORIGIN_ENDPOINT" \
      --access-key-id "$ORIGIN_KEY" \
      --secret-access-key "$ORIGIN_SECRET" \
      --region "$ORIGIN_REGION"
  fi
}

# Run the DuckDB + Polars matrix for one variant and diff (fixture must exist).
run_matrix() {
  local variant="$1"
  need_verglas_keys; need_origin_keys
  local extra=()
  if [[ -n "$INJECT_FAULT" ]]; then extra=(--inject-fault "$INJECT_FAULT"); fi
  # shellcheck disable=SC2046
  "$PY" "$DRIVER" matrix $(py_args "$variant") ${extra[@]+"${extra[@]}"}
}

run_selftest() { "$PY" "$DRIVER" selftest; }

echo "[run] phase=$PHASE variants='${VARIANTS[*]}' bucket=$BUCKET prefix=$PREFIX endpoint=$VERGLAS_ENDPOINT" >&2

case "$PHASE" in
  selftest) run_selftest ;;
  fixture)  for v in "${VARIANTS[@]}"; do run_fixture "$v"; done ;;
  matrix)   for v in "${VARIANTS[@]}"; do run_matrix "$v"; done ;;
  all)      for v in "${VARIANTS[@]}"; do run_fixture "$v"; run_matrix "$v"; done ;;
esac
