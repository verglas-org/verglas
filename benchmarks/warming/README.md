# Eager-warming live demonstration (#168)

The third resident of `benchmarks/`. It delivers the measured proof #168 asks
for: **a WATCHED table's cold planning walk collapses toward a warm one because
the server warmed the table's metadata into #50's pinned store _before any
client asked_** — driven by the real `[catalog]` REST watcher (#47) + warming
coordinator (#168), not a benchmark shortcut.

Every benchmark under `benchmarks/` follows the same shape: a folder, one
`run.sh`, and a README documenting every input. **Nothing here runs in PR CI.**

## The plumbing this closes

Server warming is driven by the **`[catalog]` REST watcher**: verglas-server polls an
Iceberg REST catalog, and when a table's pointer swings (onboarding or a commit)
the coordinator walks that snapshot's `metadata.json` → manifest list →
manifests → Parquet footers through the cache, pinning them. `benchmarks/tpch`
seeds through a **local SQLite catalog**, which the REST watcher cannot see — so
it never exercises warming.

This demo stands up an **Apache Polaris** container as the REST catalog verglas-server
watches (same pinned digest as `benchmarks/polaris`), seeds a TPC-H table
_through Polaris_ with its data on the same origin S3 the server fills from, and
configures verglas-server with `[catalog] uri` → Polaris and `[cache.warming] enabled
= true`. A commit the watcher observes then drives a real warming walk, and a
scripted client planning walk through the Verglas endpoint measures the payoff.

```
 warming_demo.py (host) ──seed (pyiceberg REST)─────────▶ Polaris (:8181) ──▶ origin S3 (OCI)
                        ──planning walk (Verglas S3)───▶ verglas-server (:8555) ──▶ origin S3 (OCI)
                                                             ▲
 verglas-server  ──[catalog] watch (REST poll)──▶ Polaris ; on a pointer swing the
           warming coordinator walks metadata+footers through its own cache,
           pinning them in #50's metadata store BEFORE the client walks.
```

## The measurement

Three configurations, the same seeded table, a **fresh cache dir each**; two
runs each; the server's `/admin/stats` meta counters recorded as mechanism
evidence:

| config | what | expectation |
|--------|------|-------------|
| **A** | warming **OFF**, cold client planning walk | baseline — planning ≈ direct (backend round-trip per object) |
| **B** | warming **ON**; wait for `/admin/stats` `warming.tables_completed`, then cold client planning walk | **collapses toward warm** (ms-class, served from the pinned store) |
| **C** | warm client walk (immediate re-run) | the floor |

The **planning walk** is the honest object set a query engine reads before it
can plan a scan: for every table, `metadata.json` → manifest list → manifests
(via pyiceberg's own planning primitives, FileIO pointed at Verglas) → each
Parquet file's footer as a **suffix-range GET** (`bytes=-65536`, byte-identical
to the warmer's `ReadRange::Suffix` pin, so a warm footer read hits the pinned
suffix-granular entry). The whole walk is timed; the `/admin/stats` counter
delta isolates where the bytes came from.

**Config B trigger.** A fresh warming-on server starts against the
**pre-existing** seeded tables: the watcher's first successful poll seeds the
watched set (and flips the `seeded` signal), the coordinator's startup pass
warms every table, and only then does the client walk — the pure onboarding
path, with **no commit fired**. `run.sh` polls `/admin/stats` until
`warming.tables_completed` reaches the table count before the client walks, so
B measures a genuinely server-warmed table. (Commit-driven re-warms exercise
the same warmer; the #168 integration tests cover that path.)

## Results (TPC-H SF1, all 8 tables, Mac Studio dev, APFS/NVMe)

91 Parquet files across 8 tables; the planning walk reads 24 block-metadata
objects (8 `metadata.json` + 8 manifest lists + 8 manifests) and 91 footers.
Config B is pure **startup warming**: the server started against the
pre-existing tables and completed its warming pass (`tables_completed=8`)
before the client walked — no commit was ever fired.

Planning-walk latency (two runs each):

| config | runs (ms) | mean (ms) |
|--------|-----------|-----------|
| A — warming OFF, cold | 14354.4, 14780.9 | **14567.6** |
| B — warming ON, server-warmed at startup, cold | 469.1, 406.7 | **437.9** |
| C — warm floor | 383.6, 402.7 | **393.1** |

**A/B = 33.3× faster cold once the server warms it. B sits at 1.11× the warm
floor — within ~45 ms of warm.**

Mechanism (first run of each config):

| config | walk_ms | meta_hits | meta_misses | backend_heads | backend_bytes |
|--------|---------|-----------|-------------|---------------|---------------|
| A | 14354.4 | 0 | 24 | 24 | 5,837,354 |
| B | 469.1 | 115 | 0 | 0 | 0 |
| C | 383.6 | 115 | 0 | 0 | 0 |

- **B/C serve the entire walk from the pinned store** — 115 `meta_hits` (24
  blocks + 91 footers), **zero backend bytes**. The server walked the metadata
  before the client asked.
- **A pays the origin** — 24 metadata round-trips plus a footer fetch per
  Parquet file. Its `backend_bytes` (5,837,354) is **byte-for-byte the warmer's
  `footer_bytes_warmed`** in B: the exact bytes A fetches cold are the ones the
  server pre-pinned.
- Warming footer efficiency: 91 footers pinned with 91 `footer_gets`, **0
  refetches** — one GET per file (the ≤2-GET rule, 1 GET for 100% of this
  fixture).

## Prerequisites

- **Docker** (Docker Desktop on macOS) — pulls the pinned Polaris image.
- An **S3-compatible origin** reachable via the standard `AWS_*` environment
  (`AWS_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`).
  The bucket must exist; path-style addressing throughout.
- Built server binaries: `cargo build --release -p verglas -p verglas-server` →
  `target/release/verglas-server`.
- **aws CLI** (teardown only), **jq**, **lsof**, and **python3.13**.

## Pinned versions

| Component | Pin | Override |
|-----------|-----|----------|
| Polaris server | `apache/polaris:1.6.0` (`sha256:9738b2052dea…`) | `POLARIS_IMAGE` |
| Python deps | `requirements.txt` (pyiceberg 0.8.1, duckdb 1.1.3, pyarrow 18.1.0, boto3 1.35.99) | — |

## Inputs

| Input | Flag | Env | Default |
|-------|------|-----|---------|
| Phase | `--all` / `--seed-only` / `--measure-only` / `--report` / `--teardown` | — | `--all` |
| Scale | `--scale N` | `WARMING_SCALE` | `1` |
| Tables | `--tables a,b,c` | `WARMING_TABLES` | all 8 |
| Runs / config | `--runs N` | `WARMING_RUNS` | `2` |
| Origin bucket | `--bucket NAME` | `WARMING_BUCKET` | `hyperglas` |
| Prefix | `--prefix PATH` | `WARMING_PREFIX` | `bench/warming-demo` (must be non-empty) |
| Verglas S3 port | `--vg-port N` | `WARMING_VG_PORT` | `8555` (admin = N+1) |
| Target file size | — | `WARMING_TARGET_FILE_SIZE` | `16777216` (fans footers out) |
| Machine note | — | `WARMING_MACHINE_NOTE` | (generic) |
| Cache-medium note | — | `WARMING_CACHE_NOTE` | (generic) |

Polaris publishes `8181` (REST) and `8182` (management) on the host so both
verglas-server and the driver reach it. The **prefix must be non-empty** — the guard
refuses the bucket root.

## Copy-paste invocation

```bash
# 0. Build the server binaries (once).
cargo build --release -p verglas -p verglas-server

# 1. Load origin creds (used by Polaris's FileIO and by Verglas's backend).
cp /path/to/.env benchmarks/warming/.env      # never committed; guarded in run.sh
set -a; source benchmarks/warming/.env; set +a

# 2. Full run: seed once, measure A/B/C x 2, render results/report.md.
export WARMING_MACHINE_NOTE="Mac Studio M-series, 28 cores"
export WARMING_CACHE_NOTE="APFS on NVMe (internal SSD)"
benchmarks/warming/run.sh --all

# Or split:
benchmarks/warming/run.sh --seed-only
benchmarks/warming/run.sh --measure-only     # re-render is automatic
benchmarks/warming/run.sh --report

# 3. Teardown: kill verglas-server, drop catalog/tables, prefix-scoped S3 delete,
#    stop the Polaris container.
benchmarks/warming/run.sh --teardown
```

The report is written to `results/report.md` and printed. Published numbers go
on the issue/PR, not into the tree (`benchmarks/` convention; `results/` is
gitignored).

## Orphan & credential safety

- **Dedicated ports.** Verglas S3 on `--vg-port` (default 8555), admin on N+1;
  Polaris on 8181/8182. `run.sh` guards each port with `lsof` before binding.
- **Explicit PID kill (#170).** `stop_verglas-server` kills every PID holding the S3
  or admin port plus the parent, escalating `TERM`→`KILL` until the port frees,
  so nothing is orphaned between configs or after `--teardown`. Even if #190's
  supervisor is absent, teardown leaves no verglas-server behind.
- **`.env` is gitignored and never logged.** The driver reads origin secrets
  only from the process environment/flags and never prints them; the generated
  `verglas-server.toml` (which carries a short-lived Polaris bearer token) is
  gitignored.

## A bug this demo caught (fixed in #177)

The first working version of this demo could only trigger warming with a
commit: a table that **already existed** in the catalog when verglas-server started
was never warmed on startup. Root cause: the coordinator's initial `warm_all`
raced the watcher's first poll — it enumerated a still-empty watched set, and
the seeding poll deliberately emits no events, so the pre-existing table waited
for its next commit. Fixed in #177: `CatalogWatcher` now exposes a `seeded()`
signal `PollingWatcher` flips after its first successful poll, and the
coordinator awaits it before the startup pass (reproduction test:
`preexisting_table_is_warmed_on_startup_without_a_commit`). Config B exercises
exactly this once-broken path.

## Files

- `run.sh` — the single entrypoint (venv, Polaris lifecycle, verglas-server lifecycle
  with port guards + explicit PID kill, seed, per-config measurement, report,
  prefix-scoped teardown, guards).
- `warming_demo.py` — the driver (Polaris bootstrap/seed, the planning walk +
  counter capture, report renderer, teardown).
- `requirements.txt` — pinned Python deps.
- `.gitignore` — venv, generated config/log, `results/`, `.env`.
