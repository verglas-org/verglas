# TPC-H over Iceberg — endpoint-vs-direct benchmark

The first resident of the top-level `benchmarks/` directory (issue #136). It
produces the publishable, query-level number: the **22 official TPC-H queries**
run against an Iceberg dataset, three ways — **direct against the origin**,
**Verglas cold**, and **Verglas warm** — with a per-query latency table and
totals.

Every benchmark under `benchmarks/` follows this shape: a folder, one `run.sh`,
and a README that documents every input. **Nothing here runs in PR CI** — these
are run manually or by the future benchmark rig.

## What it does

1. **Generate** — DuckDB's `tpch` extension runs `dbgen` at a scale factor
   (default SF1) locally, then each of the 8 tables is written as an **Iceberg
   table** (Parquet data files) via **pyiceberg** using a **local SQLite
   catalog** whose warehouse is `s3://<bucket>/<prefix>`. Every write goes
   **through the Verglas endpoint** — the one-endpoint principle, exercising the
   write path.
2. **Query** — the 22 canonical TPC-H queries (text taken verbatim from the
   extension's own `tpch_queries()`) run against the Iceberg tables on three
   legs: direct-to-origin, Verglas cold (first touch, empty daemon cache),
   Verglas warm (immediate re-run).
3. **Report** — per-query `direct / cold / warm` milliseconds plus the
   `direct ÷ warm` speedup, totals, and machine + cache-medium context. Every
   report also carries its **tier context** — the daemon's DRAM/disk budget and
   a `profile` label, read from the daemon's `/admin/stats` so no number is ever
   published without the cache sizing that produced it (issue #141). Text table
   by default; `--json` for machine-readable output.
4. **Teardown** — prefix-scoped delete of every object under the prefix (guarded
   so it can never operate at the bucket root) plus removal of the SQLite
   catalog file.

## Prerequisites

- **Python 3** (3.13 recommended; the pinned wheels are verified there — 3.14
  may lack wheels for some deps). `run.sh` prefers `python3.13` if present.
- A **built `verglas` binary** with `verglasd` beside it:
  `cargo build --release -p verglas -p verglasd` → `target/release/{verglas,verglasd}`.
- A running **`verglas dev`** daemon (it prints the endpoint URL and dev keys).
- An **S3-compatible origin** reachable via the standard `AWS_*` environment
  (an `.env` exporting `AWS_ENDPOINT`, `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`). The bucket must already exist.
- Path-style S3 addressing is used throughout (required by OCI and most
  S3-compatible origins).

`run.sh` creates a local `.venv`, installs the pinned `requirements.txt`
(idempotently — a stamp file skips reinstall when unchanged), and drives
everything. No global Python state is touched.

## Inputs

| Input | Flag | Env | Default |
|-------|------|-----|---------|
| Scale factor | `--scale N` | `TPCH_SCALE` | `1` |
| Origin bucket | `--bucket NAME` | `TPCH_BUCKET` | `hyperglas` |
| Table prefix | `--prefix PATH` | `TPCH_PREFIX` | `bench/tpch-sf<SCALE>` |
| Verglas endpoint | `--endpoint URL` | `VERGLAS_ENDPOINT` | `http://127.0.0.1:8333` |
| Verglas dev key id | `--access-key-id K` | `VERGLAS_DEV_ACCESS_KEY_ID` | — |
| Verglas dev secret | `--secret-key S` | `VERGLAS_DEV_SECRET_ACCESS_KEY` | — |
| Read mode | `--read-mode MODE` | `TPCH_READ_MODE` | `auto` |
| Cache-medium note | `--cache-note TEXT` | `TPCH_CACHE_NOTE` | (generic) |
| Tier profile label | `--profile LABEL` | `TPCH_PROFILE` | `unspecified` |
| Daemon admin URL | `--admin-endpoint URL` | `TPCH_ADMIN_ENDPOINT` | (endpoint's next port) |
| JSON output | `--json` | — | off |

The **`--profile`** label and the daemon's cache budgets are recorded on the
report; the budgets and the read-path counters come from the daemon's
`/admin/stats` (`--admin-endpoint`, which defaults to the S3 endpoint's next
port — the layout `verglas dev` uses). If the admin surface is unreachable the
report prints `cache budget: unavailable` rather than guessing.

The **prefix must be non-empty** — the guard refuses the bucket root, because
teardown lists-and-deletes the prefix and an empty prefix would wipe the bucket.

The **direct-to-origin** leg reads its credentials from the ambient `AWS_*`
environment; the **Verglas** legs use the dev endpoint + dev keys.

## Copy-paste invocation

```bash
# 1. Build the binaries (once).
cargo build --release -p verglas -p verglasd

# 2. Start the local endpoint in another shell (leave it running).
#    It prints the endpoint URL and the dev access/secret keys.
set -a; source .env; set +a          # origin creds for the daemon's backend
./target/release/verglas dev --cache-dir /tmp/vg-tpch-cache

# 3. Run the benchmark (this shell). Load .env for the direct leg, then paste
#    the dev keys the banner printed.
set -a; source .env; set +a
export VERGLAS_DEV_ACCESS_KEY_ID=VG...      # from the banner
export VERGLAS_DEV_SECRET_ACCESS_KEY=...    # from the banner

benchmarks/tpch/run.sh --scale 1 --bucket hyperglas \
  --endpoint http://127.0.0.1:8333 \
  --cache-note "APFS on NVMe (Mac Studio internal SSD)"

# Phase selection:
benchmarks/tpch/run.sh --seed-only     # generate + write through Verglas, stop
benchmarks/tpch/run.sh --query-only    # queries + report (tables must exist)
benchmarks/tpch/run.sh --query-only --json > out.json
benchmarks/tpch/run.sh --teardown      # delete the prefix + catalog file
```

## Tier profiles (issue #141)

`verglas dev` runs a **tiered DRAM+NVMe** cache. The published number depends
entirely on how the SF1 dataset (~a few hundred MB of Parquet) sits across those
tiers, so a run is meaningless without its tier context. Three standard profiles
bracket the behavior — run each against a **fresh cache dir** (PR #139's purge is
unmerged, so a clean cache means a new `--cache-dir`), and pass the matching
`--profile` so the report is self-describing.

The engine DRAM floor is **80 MiB** (48 MiB write pipeline + 16 MiB metadata
cache + a 2-block, 16 MiB memory tier); `--dram 80MB` is the smallest budget that
boots. Below it the daemon exits at startup with the engine's budget error.

### 1. dram-resident (best case, upper bound)

DRAM ≥ dataset: the whole working set lives in the memory tier, so warm reads
never touch disk. This is the historical `verglas dev` default and the number
#136 published.

```bash
./target/release/verglas dev --cache-dir /tmp/vg-tpch-dram          # --dram 1GB (default)
benchmarks/tpch/run.sh --profile dram-resident \
  --cache-note "APFS on NVMe — DRAM-resident (1 GB DRAM ≥ SF1)"
```

### 2. nvme-resident (the headline BYOC claim)

DRAM at the engine floor, disk ≥ dataset: the memory tier is far smaller than the
working set, so warm reads are served **from NVMe**, not DRAM. This is the tiered
system the product actually is. Verify it from the report's warm-leg counters:
`disk_hits > 0` and `DRAM tier in use after warm` ≪ the dataset size.

```bash
./target/release/verglas dev --cache-dir /tmp/vg-tpch-nvme --dram 80MB --cache-size 20GB
benchmarks/tpch/run.sh --profile nvme-resident \
  --cache-note "APFS on NVMe — NVMe-resident (80 MB DRAM floor, 20 GB disk ≥ SF1)"
```

### 3. constrained (cache behavior, not cache capacity)

Disk < dataset (~50%): the working set does not fit on disk, so the warm leg
evicts and refills. The report's warm-leg `backend_fills > 0` is the eviction
evidence, and the speedup is an **honest partial** number — it shows cache
behavior under pressure rather than capacity.

**Size `--cache-size` to ~50% of the actual seeded footprint**, which you measure
after seeding (do not guess): list the prefix and sum the object sizes, e.g.

```bash
# after --seed-only, measure the on-origin footprint of the SF1 tables:
aws --endpoint-url "$AWS_ENDPOINT" s3 ls --recursive --summarize \
  s3://hyperglas/bench/tpch-sf1 | tail -2
# then set --cache-size to roughly half of "Total Size", rounded to a byte size.
```

```bash
# measured for this repo's SF1 run: 41 objects, 255,136,794 B (~243 MiB) →
# half ≈ 122 MB. Re-measure for your origin; do not assume these bytes.
./target/release/verglas dev --cache-dir /tmp/vg-tpch-cons --dram 80MB --cache-size 122MB
benchmarks/tpch/run.sh --profile constrained \
  --cache-note "APFS on NVMe — constrained (disk 122 MB ≈ 50% of SF1 footprint)"
```

Each profile is a **separate daemon** with its own fresh `--cache-dir`. Seed once
per daemon (or reuse a seeded prefix across the query legs with `--query-only`, as
below); the query phase's cold-before-warm ordering does the rest.

**Resident-biased admission (issue #164).** The warm leg of the constrained
profile is a **cyclic scan** — the 22 queries repeatedly sweep tables larger than
the cache — which the frequency doorkeeper alone cannot resist (every block
clears the "seen twice" bar by its second cycle, so admission passes the whole
sweep and eviction degenerates to ~0% hits). Add `--admit-probability P` to thin
the sweep: once the cache is full, a block that clears the frequency gate is
admitted only with probability `P`, so a stable resident subset survives instead
of being cyclically overwritten. Omit the flag for the default (`P = 1.0`).

**Measured (post-#50, SF1, two runs per config; full tables on issue #164):** on
this profile `--admit-probability 0.1` made the warm leg **worse**, not better —
warm-leg `backend_fills` 493–500 vs the default's 317–325. With metadata pinned
(#50) and block-level reuse inside the warm leg, the default already beats the
pure-cyclic-scan model this lever assumes (warm fill ratio ~0.32 vs the ~0.5
ceiling); thinning admissions to 10% just leaves hot blocks uncached across
queries. Leave the flag off for this profile; it remains available for workloads
that really are uniform cyclic sweeps.

```bash
./target/release/verglas dev --cache-dir /tmp/vg-tpch-cons \
  --dram 80MB --cache-size 122MB --admit-probability 0.1
```

## Cold vs warm discipline

`verglas dev` starts with an empty cache. Writes go through the write-passthrough
path and admit nothing to the read cache, so after `--seed-only` the read cache
is still cold. The query phase therefore runs the **direct** leg first (never
touches Verglas), then the **cold** leg (genuine first touch), then the **warm**
leg (immediate re-run, cache populated) — a cold-before-warm ordering.

A **truly cold** measurement needs an empty cache. Two ways to get one:
restart `verglas dev` (or point `--cache-dir` at a fresh directory) between
invocations, or purge the running daemon: the harness issues
`POST /cache/purge` on the admin API (the S3 port plus one, printed by
`verglas dev`) between the direct and cold legs — the cold leg then matches a
fresh-daemon cold leg within noise, and the report labels it purged (issue
#138). Re-running `--query-only` with neither measures warm-over-warm — useful
for the **two-run stability** check on the direct and warm legs (which do not
depend on cache state).

## Read modes (honest methodology)

- **`duckdb-iceberg`** — each table is a DuckDB view over `iceberg_scan(<the
  pyiceberg-written metadata.json>)`. DuckDB's `iceberg` extension plans and
  pushes scans/filters down into the Parquet files, fetched over the leg's
  endpoint. This is the real SQL-pushdown path. The metadata location is read
  straight from the catalog, so no `version-hint.text` (which pyiceberg does not
  emit) is required.
- **`pyiceberg-arrow`** — each table is loaded with pyiceberg → Arrow and
  registered in DuckDB. This measures **scan + full fetch over the endpoint plus
  in-memory SQL**, not pushed-down scans. It is the fallback for when DuckDB's
  `iceberg` extension cannot read the pyiceberg metadata (version skew).
- **`auto`** (default) — try `duckdb-iceberg`; on a read failure, fall back to
  `pyiceberg-arrow` for that leg and record it.

Both modes route **every byte through the leg's endpoint over the identical
path**, so the direct-vs-Verglas comparison is valid in either mode. The report
records which mode each leg used; when it is `pyiceberg-arrow`, the timings are
scan+fetch numbers, not full-SQL-pushdown numbers — read them accordingly.

## Avro note

The dataset's **data files are Parquet only**. pyiceberg writes Parquet
exclusively; an Avro-data-file variant is **blocked until a writer supports it**
and is intentionally **not faked here**. (Iceberg's own metadata — manifests and
manifest lists — is Avro, and is written by pyiceberg; this note is about the
*table data files*.)

## Expected runtime (SF1)

On a Mac Studio (M-series, 28 cores) against an OCI Object Storage origin in
`us-ashburn-1`, a validated SF1 run measured:

- **generate** (dbgen + write 8 Iceberg tables through Verglas): ~25 s.
- **query** (22 queries × three legs, `duckdb-iceberg` mode): ~4.5 min, almost
  all of it the **direct** leg's high-latency origin round-trips (~235 s); the
  Verglas warm leg is ~3 s total.

Headline (SF1, `duckdb-iceberg`): direct total **234.9 s**, cold **24.4 s**,
warm **2.96 s** — **≈79× direct/warm**. Two query passes were stable to within
**0.05%** on the (cache-independent) warm leg and **~3%** on the direct leg.
Larger scale factors grow roughly linearly. See the numbers posted on issue #136
for the full per-query table.

The **cold** leg is only genuinely cold on the first query pass after a fresh
`verglas dev` or a `--purge`; a second pass against the same daemon without
either reports warm numbers for the cold column (cache persisted).

## Files

- `run.sh` — the single entrypoint (venv, phase dispatch, guards).
- `tpch_bench.py` — the driver (generate / query / teardown, reporting).
- `requirements.txt` — pinned Python dependencies.
