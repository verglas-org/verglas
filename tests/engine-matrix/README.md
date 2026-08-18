# Engine matrix — DuckDB + Polars result diffing

The result-correctness tripwire (issue #23). Protocol conformance (#22) proves
Verglas *speaks* S3 correctly; this proves real query engines get **byte-for-byte
identical results** through Verglas versus straight from the origin. Wrong
results are Verglas's existential risk, so **this runs in PR CI on every change**
(manual benchmark harnesses are not kept in-repo).

Two engines, three fixture variants (Parquet, Avro, mixed — see below), three
legs, diffed at the Arrow level:

- **DuckDB** via its `iceberg` extension (real SQL pushdown into the data-file
  scans).
- **Polars** via a pyiceberg scan handed to Polars (`pyiceberg → Arrow → pl`).
- Legs: **direct-to-MinIO** (baseline), **Verglas pass 1 (MISS path**, empty
  server cache), **Verglas pass 2 (HIT path**, immediate re-run). Both Verglas
  legs are compared against the direct baseline.

Every result is **sorted deterministically and compared as Arrow** (schema +
values) — never printed output. Any difference fails the build with a readable
dump (schema diff, or the first N mismatched rows shown direct-vs-Verglas).

## Three fixture variants (Parquet, Avro, mixed) — #159

The matrix runs **three fixture variants** on the PR path, diffed identically:

| Variant   | Data files                              | Built by |
|-----------|-----------------------------------------|----------|
| `parquet` | all Parquet                             | pyiceberg (`fixture` phase) |
| `avro`    | all Avro (`write.format.default=avro`)  | JVM builder (`fixture-jvm/`) |
| `mixed`   | Avro + Parquet, format switched between snapshots | JVM builder (`fixture-jvm/`) |

**Why a JVM builder.** pyiceberg and DuckDB both write **Parquet data files
only**. This was re-checked against current releases (mid-2026): pyiceberg
**0.11.1** silently *ignores* `write.format.default=avro` and writes Parquet
anyway (an append "succeeds" but produces a `.parquet` file — exactly the fake
#142 refused), and DuckDB-Iceberg's writes are Parquet too. So the Avro and mixed
data files are written by a tiny plain-Java program on **iceberg-core +
iceberg-data** (`GenericAppenderFactory` writes the data files; the table API
commits real manifests) — **no Spark**. See `fixture-jvm/`. #63's Spark rig
inherits these fixtures rather than rebuilding them.

**Per-engine finding — no released reader decodes Avro data files.** Confirmed
against DuckDB **1.5.4** and pyiceberg **0.11.1** (July 2026), not just the
pinned versions:

- **DuckDB** (`iceberg_scan`): `File format 'AVRO' not supported, only supports
  'parquet' currently` — the extension reads Iceberg's Avro *manifests* but not
  Avro *data* files.
- **pyiceberg → Polars**: `Unsupported file format: FileFormat.AVRO` — its
  PyArrow reader handles Parquet (and ORC) data files, not Avro.

This gap is **documented, not hidden**: for the `avro` and `mixed` variants the
matrix records a per-engine `finding` for each engine (grid cells show `n/a`) and
the run passes *with findings* — a reader that cannot do Avro is itself a
finding, and both the direct and the Verglas legs fail identically (Verglas never
returns bytes the origin could not). A future engine that gains Avro data-file
reads will diff automatically, with no harness change.

**Verglas is still tripwired over Avro.** Because no engine decodes Avro, the
matrix adds an **object-level data-file byte check** for every variant: it
enumerates the table's data files through the (spec-complete) manifests — which
pyiceberg *can* read — and asserts each data file is **byte-identical** served
direct-from-MinIO vs through Verglas, on both a cold (miss) and warm (hit) cache.
For `avro`/`mixed` this is the real Verglas correctness signal over Avro and
mixed-format data files; for `parquet` it complements the engine-level diff.

## The fixture

Each variant is the **same** partitioned Iceberg table `matrix.events` — same
schema, same deterministic values, same snapshot history — differing only in the
data-file format, so the harness runs identically across all three. Each variant
lives under its own bucket prefix (`<prefix>-parquet`, `-avro`, `-mixed`) with
its own `fixture-<variant>.json` sidecar. Its shape: **mixed column types** and a
**multi-snapshot history**:

- Columns: `id` (long), `category` (string, the identity **partition** key),
  `name` (string, **nulls**), `amount` (double, nulls), `ratio` (float),
  `ts` (timestamp[us]), `flag` (bool), `price` (**decimal(12,2)**, nulls).
- Snapshots: append → append (**time-travel target**) → **partial overwrite**
  of the `category=A` rows → append. Five snapshots, 500 final rows.

The two metadata locations the matrix reads — the final snapshot's and the
older (time-travel) snapshot's — are recorded in a `fixture.json` sidecar during
the build, so time travel needs no engine-specific snapshot-selection syntax:
the older metadata *is* the older snapshot, read through whichever leg's
endpoint.

## The query list

Run through each engine, on each leg:

| Query                 | Exercises                                        |
|-----------------------|--------------------------------------------------|
| `full_scan`           | whole-table read (all types, all partitions)     |
| `selective_predicate` | `amount >= 400` — **range-read heavy** row-group filtering |
| `projection`          | few columns (`id, category, price`)              |
| `time_travel`         | reads the older snapshot (400 rows, pre-overwrite) |

## Prerequisites

- **Python 3** (3.13 recommended; the pinned wheels are verified there).
  `run.sh` prefers `python3.13` if present and creates a local `.venv`.
- **Java 21** (Temurin in CI) — only for the `avro`/`mixed` variants. `run.sh`
  builds the fixture builder jar via `fixture-jvm/gradlew shadowJar` on first use
  (or use the committed Gradle wrapper directly). Not needed for `--variant
  parquet` or `--selftest`.
- A built cache node: `cargo build -p verglas-cache-node`.
- A running **MinIO** (or any S3-compatible origin) with the target bucket
  created, reachable via the standard `AWS_*` environment.
- A **`verglas-cache-node`** started against that origin with static `[auth]` keys (see
  below). Path-style S3 addressing throughout.

## Inputs

| Input | Flag | Env | Default |
|-------|------|-----|---------|
| Phase | `--fixture-only` / `--matrix-only` / `--selftest` | — | all (fixture then matrix) |
| Origin bucket | `--bucket NAME` | `MATRIX_BUCKET` | `verglas-test` |
| Fixture prefix | `--prefix PATH` | `MATRIX_PREFIX` | `engine-matrix` |
| Verglas endpoint | `--endpoint URL` | `VERGLAS_ENDPOINT` | `http://127.0.0.1:8333` |
| Verglas key id | `--access-key-id K` | `VERGLAS_ACCESS_KEY_ID` | — |
| Verglas secret | `--secret-key S` | `VERGLAS_SECRET_ACCESS_KEY` | — |
| Fault injection | `--inject-fault KIND` | — | off |

The **direct-to-MinIO** leg reads its credentials + endpoint from the ambient
`AWS_*` environment (`AWS_ENDPOINT`, `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_REGION`); the **Verglas** legs use the endpoint +
the server's static dev keys.

## Copy-paste invocation (local)

```bash
# 1. Build the cache node (once).
cargo build -p verglas-cache-node

# 2. Start MinIO and create the bucket.
docker run -d --name minio -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data
docker run --rm --network host --entrypoint /bin/sh minio/mc -c \
  "mc alias set local http://127.0.0.1:9000 minioadmin minioadmin && \
   mc mb --ignore-existing local/verglas-test"

# 3. Start the cache node against MinIO with static keys (leave it running).
cat > /tmp/vg-origin-credentials <<'EOF'
[default]
aws_access_key_id = minioadmin
aws_secret_access_key = minioadmin
EOF
cat > /tmp/vg-endpoint-credentials <<'EOF'
[default]
aws_access_key_id = matrixdev
aws_secret_access_key = matrixdevsecret
EOF
cat > /tmp/vg-matrix.toml <<'EOF'
[listen]
s3_port = 8333
admin_port = 8334
[cache]
dir = "/tmp/vg-matrix-cache"
capacity_bytes = "2GB"
dram_bytes = "512MB"
[backend]
bucket = "verglas-test"
endpoint = "http://127.0.0.1:9000"
region = "us-east-1"
allow_http = true
credentials_file = "/tmp/vg-origin-credentials"
[auth]
credentials_file = "/tmp/vg-endpoint-credentials"
EOF
mkdir -p /tmp/vg-matrix-cache
./target/debug/verglas-cache-node --config /tmp/vg-matrix.toml &

# 4. Run the matrix (fixture then diff). Origin creds come from AWS_*.
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
       AWS_REGION=us-east-1 AWS_ENDPOINT=http://127.0.0.1:9000
tests/engine-matrix/run.sh \
  --endpoint http://127.0.0.1:8333 \
  --access-key-id matrixdev --secret-key matrixdevsecret

# Phase selection:
tests/engine-matrix/run.sh --fixture-only   # build the fixture, stop
tests/engine-matrix/run.sh --matrix-only ...# diff only (fixture must exist)
tests/engine-matrix/run.sh --selftest       # comparator selftest, no MinIO
```

## Proving the tripwire fires

`--inject-fault` deliberately corrupts the Verglas leg before the diff so you
can see the harness catch a wrong-bytes condition (PR-evidence only; never wired
into CI):

```bash
# Each of these exits non-zero with a readable dump:
tests/engine-matrix/run.sh --matrix-only ... --inject-fault mutate-cell   # wrong value
tests/engine-matrix/run.sh --matrix-only ... --inject-fault drop-row      # truncated read
tests/engine-matrix/run.sh --matrix-only ... --inject-fault wrong-schema  # schema drift
```

The MinIO-free `--selftest` is the permanent guard: it asserts the comparator
raises on value, row-count, schema, and type diffs (and passes on reordered-but-
equal data), so the comparator can never silently degrade to a no-op.

## Runtime

The whole PR job (build the two binaries with a warm cache, start MinIO, start
the server, build the fixtures, run both engines × four queries × three legs ×
three variants, plus the data-file byte check) is designed to stay **under ~5
minutes** so it remains on the PR path. The fixtures are thousands of Arrow cells
(500 rows), not millions — the diff, not the scale, is the point.

Measured locally (warm venv + built server, docker MinIO), the three variants
add little over the original Parquet-only run:

| Step                                   | Time |
|----------------------------------------|------|
| Parquet only (fixture + matrix)        | ~1.7s |
| **All three** (fixtures + matrices)    | ~7.5s |
| → harness delta for avro + mixed       | **~+5.8s** |
| JVM fixture builder jar — one-time build | ~10s (deps cached) / ~2.8s incremental |
| setup-java (cached)                    | a few seconds |

So the added PR-path cost is a one-time ~10-30s cached Gradle build plus ~6s of
extra harness runtime — the job stays dominated by the Rust `cargo build` and
well within budget.

## Files

- `run.sh` — the single entrypoint (venv, JVM-jar build, per-variant dispatch,
  credential guards).
- `engine_matrix.py` — the driver (parquet fixture / matrix / selftest, the
  comparator, the per-engine finding logic, the data-file byte check, the fault
  injector).
- `fixture-jvm/` — the plain-Java iceberg-core builder for the Avro and mixed
  data-file variants (Gradle project + committed wrapper; build output is
  gitignored).
- `requirements.txt` — pinned Python dependencies.
