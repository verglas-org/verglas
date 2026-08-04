# Polaris-over-Verglas — catalog-service IO benchmark

The second resident of `benchmarks/` (issue #171). Where `benchmarks/tpch`
proves the **engine data path**, this benchmark proves Verglas benefits an
**unmodified Iceberg *catalog service*** — Apache Polaris — with a single
storage-configuration change and no code change to Polaris.

Apache Polaris is itself an S3 client: on `createTable`/commit it **writes**
`metadata.json` to the warehouse storage, and on `loadTable` it **reads** that
`metadata.json` back to serve the response. The official
[apache/polaris-tools](https://github.com/apache/polaris-tools) Gatling suites
hammer exactly those catalog operations. Point Polaris's warehouse storage at
the Verglas endpoint (one config change — the adoption story) and that metadata
IO flows through Verglas, where #50's metadata pinning should accelerate the
`loadTable` read path.

Every benchmark under `benchmarks/` follows the same shape: a folder, one
`run.sh`, and a README documenting every input. **Nothing here runs in PR CI.**

## What it does

Two legs against the **same origin S3 bucket**, one Polaris config change apart:

- **Leg A — direct**: Polaris's warehouse S3 storage points **directly at the
  origin** (OCI Object Storage). Baseline.
- **Leg B — verglas**: Polaris's warehouse S3 storage points at the **Verglas
  dev endpoint**, which passes through to the same origin. The read suite runs
  **twice** — cold (empty cache) then warm — so #50's pinned metadata shows.

Each leg drives three official suites, pinned to a polaris-tools commit:

1. `CreateTreeDataset` — 100% write workload. Every `Create Table` writes a
   `metadata.json` through the storage path (through Verglas on leg B).
2. `ReadTreeDataset` — 100% read workload. `Fetch single Table` is the
   `loadTable` that **reads `metadata.json`** — the operation metadata pinning
   accelerates. Run cold then warm on leg B.
3. `ReadUpdateTreeDataset` — mixed read/write at a configurable read/write ratio
   (default 0.8). `Update Table` commits a **new** `metadata.json`.

The report merges Gatling's own latency percentiles (p50/p95/p99) per operation
across **direct / verglas-cold / verglas-warm**, plus the server's
`/admin/stats` **meta counters** for the warm leg (the mechanism evidence:
`meta_hits` / `meta_misses` = the metadata hit rate), plus a correctness gate
that both legs passed their Gatling assertions with zero failed requests.

## Architecture

```
 Gatling (temurin container)  --HTTP-->  Polaris (container, :8181)
                                              |
                            warehouse S3 storage config (per leg)
                                              |
   leg A: -------------------- endpoint = OCI S3 --------------------> OCI
   leg B: endpoint = http://host.docker.internal:<vg-port> --> Verglas (host) --> OCI
```

Polaris and the Gatling runner share a Docker network (`polaris:8181`). The
Verglas dev server runs on the **host**; the Polaris container reaches it via
`host.docker.internal` (Docker Desktop for Mac forwards this to host loopback,
verified). The Gatling client only ever talks to Polaris's catalog API — it
never touches S3 directly. **Only Polaris does S3 IO**, so this benchmark
isolates the *catalog-service* IO benefit (data-file IO is out of scope by
design — `benchmarks/tpch` covers that).

## Prerequisites

- **Docker** (Docker Desktop on macOS). Pulls three pinned images (below).
- An **S3-compatible origin** reachable via the standard `AWS_*` environment
  (`AWS_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`).
  The bucket must already exist. Path-style addressing is used throughout.
- A **built `verglas` binary** with `verglas-server` beside it:
  `cargo build --release -p verglas -p verglas-server` → `target/release/{verglas,verglas-server}`.
- **aws CLI** (teardown only) and **python3** (stdlib only — no venv).

No JDK on the host is required: the Gatling suites build and run inside a pinned
`eclipse-temurin` container. No polaris-tools code is modified; it is cloned at a
pinned commit into a gitignored checkout dir (venv-style).

## Pinned versions

| Component | Pin | Override |
|-----------|-----|----------|
| Polaris server | `apache/polaris:1.6.0` (`sha256:9738b2052dea…`) | `POLARIS_IMAGE` |
| Gatling JDK | `eclipse-temurin:21-jdk` (`sha256:1eeacc8c295e…`) | `GATLING_JDK_IMAGE` |
| polaris-tools | commit `91c4d09be13221e59401892ce35302bc8187f79e` | `POLARIS_TOOLS_COMMIT` |

Polaris **1.6.0** post-dates the `stsUnavailable` credential-vending bug filed
against 1.3.0-incubating ([apache/polaris#3742](https://github.com/apache/polaris/issues/3742));
the run **verifies** the write path works rather than assuming it (see Caveats).

## Inputs

| Input | Flag | Env | Default |
|-------|------|-----|---------|
| Leg | `--leg direct\|verglas` | — | (required unless `--all`) |
| Origin bucket | `--bucket NAME` | `POLARIS_BUCKET` | `hyperglas` |
| Prefix base | `--prefix PATH` | `POLARIS_PREFIX` | `bench/polaris` (per-leg: `<base>-<leg>`) |
| Run label | `--run-label L` | `POLARIS_RUN_LABEL` | `run1` |
| Verglas S3 port | `--vg-port N` | `POLARIS_VG_PORT` | `8455` (admin = N+1) |
| Namespace width | `--ns-width N` | `POLARIS_NS_WIDTH` | `2` |
| Namespace depth | `--ns-depth N` | `POLARIS_NS_DEPTH` | `4` |
| Tables / namespace | `--tables-per-ns N` | `POLARIS_TABLES_PER_NS` | `5` |
| Views / namespace | `--views-per-ns N` | `POLARIS_VIEWS_PER_NS` | `3` |
| Read/write ratio | `--read-write-ratio R` | `POLARIS_READ_WRITE_RATIO` | `0.8` |
| Verglas DRAM budget | — | `POLARIS_VG_DRAM` | `1GB` |
| Verglas disk budget | — | `POLARIS_VG_DISK` | `20GB` |
| Machine note | — | `POLARIS_MACHINE_NOTE` | (generic) |
| Cache-medium note | — | `POLARIS_CACHE_NOTE` | (generic) |

Defaults produce **15 namespaces, 40 tables, 24 views** — small, fast, and cheap
against a real origin. Each leg writes to its **own prefix** (`<base>-direct`,
`<base>-verglas`) so the legs never collide and teardown is clean. The **prefix
must be non-empty** — the guard refuses the bucket root.

## Copy-paste invocation

```bash
# 0. Build the server binaries (once).
cargo build --release -p verglas -p verglas-server

# 1. Load origin creds (used by the direct leg's Polaris and by Verglas's backend).
cp /path/to/.env benchmarks/polaris/.env      # never committed; guarded in run.sh
set -a; source benchmarks/polaris/.env; set +a

# 2. Full run: direct leg, then verglas leg, then the comparison table.
#    Stamp the report context via env first (machine + cache medium).
export POLARIS_MACHINE_NOTE="Mac Studio M-series, 28 cores"
export POLARIS_CACHE_NOTE="APFS on NVMe (internal SSD)"
benchmarks/polaris/run.sh --all --bucket hyperglas

# Or one command per leg (the acceptance-criteria form):
benchmarks/polaris/run.sh --leg direct
benchmarks/polaris/run.sh --leg verglas
benchmarks/polaris/run.sh --report          # merges results/{direct,verglas}.json

# 3. Teardown: prefix-scoped S3 delete (both legs) + container/network cleanup.
benchmarks/polaris/run.sh --teardown
```

The comparison table is written to `results/comparison-<run-label>.md` and
printed to stdout. Run twice with `--run-label run1` and `--run-label run2` for
the two-run stability check.

## Polaris behind Verglas — configuration (customer-facing)

This is the whole adoption story: putting Polaris behind Verglas is **one
storage-config change**. Both are copy-paste usable.

### 1. Point Polaris's server-side S3 credentials at Verglas

Polaris does its metadata IO with the standard AWS SDK credential chain. Give
the Polaris process the **Verglas dev keys** (printed by `verglas dev`) and the
Verglas endpoint's region (any region works — Verglas validates SigV4 against
whatever the client signs with; use `us-east-1`):

```bash
# On the Polaris container/process:
AWS_ACCESS_KEY_ID=<verglas-dev-access-key>
AWS_SECRET_ACCESS_KEY=<verglas-dev-secret-key>
AWS_REGION=us-east-1
```

Enable S3-compatible endpoints server-side (the benchmark sets these as env,
using the quoted feature-flag segment form Quarkus requires):

```
polaris.features."ALLOW_INSECURE_STORAGE_TYPES"    = true            # http:// endpoints
polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES" = ["FILE","S3"]   # allow S3 catalogs
```

**Required for OCI (and most S3-compatible stores):** disable AWS SDK v2's
default aws-chunked/trailing-checksum PUTs, which those stores reject with
`501 AWS chunked encoding not supported`. Set on the Polaris process (it applies
to Polaris's own metadata IO, and equally to both legs, so it is a fair
constant):

```bash
AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED
AWS_RESPONSE_CHECKSUM_VALIDATION=WHEN_REQUIRED
```

### 2. Set the catalog's `storageConfigInfo` to the Verglas endpoint

The one knob that matters — per-catalog storage config. `endpoint` is the
Verglas endpoint, `pathStyleAccess` is required, and **`stsUnavailable: true`**
tells Polaris to skip STS/AssumeRole and use the static credentials directly
against the custom endpoint (Verglas and most S3-compatible origins have no STS):

```json
{
  "storageType": "S3",
  "allowedLocations": ["s3://your-bucket/your-prefix/C_0"],
  "endpoint": "http://host.docker.internal:8455",
  "pathStyleAccess": true,
  "stsUnavailable": true,
  "region": "us-east-1"
}
```

The **only difference** between the direct leg and the Verglas leg is the
`endpoint` field (origin S3 endpoint vs the Verglas endpoint) and the matching
credentials. Everything else — Polaris, the catalog, the suites — is identical.
That is the point: an unmodified Polaris, one line of storage config.

For the polaris-tools benchmark this JSON is supplied via
`dataset.tree.storage-config-info` in `application.conf`; `run.sh` generates it
per leg. In a real deployment it is the `storageConfigInfo` you pass to
`POST /api/management/v1/catalogs` (or `polaris catalogs create --endpoint …
--path-style-access …`).

## Polaris caching (state up front, not discovered later)

Polaris maintains its own **entity cache** in the catalog service. The
interesting comparison is against Polaris's own caching in its **default
configuration**, because that is what a real deployment runs — so **both legs
leave Polaris caching at its defaults** (no extra metadata caching configured,
entity cache at its shipped default). This is recorded in every report's context
line (`polaris_cache_config`). The consequence: any `loadTable` that Polaris
serves from its own entity cache never reaches S3 at all, on either leg — so the
Verglas benefit shows on the metadata reads that *do* reach storage (cold
misses, cache-expired entries, and cross-process reads). The `/admin/stats`
`meta_hits` counter on the warm leg is the direct evidence of how much metadata
IO Verglas served, independent of Polaris's own cache.

## Reading the report

- **Latency percentiles (ms)**: per operation, p50/p95/p99 for direct vs
  verglas-cold vs verglas-warm, and a `direct/warm` ratio. The headline row is
  `Fetch single Table` (`loadTable` — the metadata read). Write ops
  (`Create Table`, `Update Table`) have no cold/warm split (writes pass through).
- **`/admin/stats` — warm leg**: `meta hit rate` = `meta_hits / (meta_hits +
  meta_misses)`. A high rate on the warm read leg is the mechanism proof that
  #50's pinned metadata store served the `loadTable` reads. Cache budgets
  (`dram`/`disk`) are stamped so no latency number is published without its tier
  context.
- **Correctness**: both legs must pass their own Gatling assertions with zero
  failed (`ko`) requests. The report ends with **result-correctness deviations:
  NONE** when clean — the acceptance criterion.

Every number carries machine + cache-budget context; two runs (`run1`/`run2`)
establish stability; sampling (Gatling's injection model) is disclosed in the
context block.

## Caveats

- **`stsUnavailable` on older Polaris**: Polaris 1.3.0-incubating ignored
  `stsUnavailable=true` and still attempted credential vending
  ([#3742](https://github.com/apache/polaris/issues/3742)), which breaks the
  metadata write path against a static-credential S3-compatible endpoint. This
  benchmark pins **1.6.0**; if the `CreateTreeDataset` leg fails its
  `Create Table` assertion, that is the blocker to check first (the report's
  correctness gate makes it loud rather than silent).
- **Data-file IO is out of scope.** The Gatling suites drive the catalog API
  only. This benchmark isolates the catalog-service metadata IO benefit.
- **Reporting-only indicator override.** The suite's `gatling.conf` charts
  percentiles 25/50/75/99; the issue asks for p50/**p95**/p99. `run.sh` rewrites
  the charting indicators to 50/75/95/99 in the cloned checkout. This changes no
  request, injection rate, read/write ratio, or assertion — the workload the
  official suite drives is byte-for-byte unchanged; only which percentiles the
  HTML report tabulates differs.
- **Cold vs warm discipline — and why cold ≈ warm here.** `run.sh` issues
  `POST /cache/purge` on the server admin API between the write suite and the
  cold read suite. The purge clears the block cache tiers, but the #50 metadata
  store is **hard-isolated and pinned** — and the metadata was pinned **at write
  time**, when `CreateTreeDataset`'s `metadata.json` writes flowed through the
  endpoint. The measured consequence (visible in the results): the "cold" read
  leg's `loadTable` is already as fast as the warm leg, with `meta_misses = 0`
  across the whole leg. That is not a benchmarking artifact — it is the pinning
  claim itself: **a catalog behind Verglas never pays a cold read for metadata
  it wrote**. The direct leg is the true no-cache baseline.

## Files

- `run.sh` — the single entrypoint (pinned checkout, per-leg Polaris + Verglas
  orchestration, dockerized Gatling, report, prefix-scoped teardown, guards).
- `polaris_report.py` — Gatling per-request HTML report parser +
  comparison-table renderer (stdlib only).
- `.gitignore` — the polaris-tools checkout, gradle cache, generated config,
  results, and `.env`.
- `results/` — per-leg JSONs and the merged `comparison-<run-label>.md`.
