# TPC-H constrained profile on a multi-node pod

The fourth tier profile for the TPC-H-over-Iceberg benchmark (issues #160, #29,
#141). The three single-node profiles in `benchmarks/tpch` (dram-resident,
nvme-resident, constrained) each measure **one** server. This profile measures a
**pod**: an N-node `verglas dev` cluster where every node carries the
**constrained per-node budget**, so the pod's *aggregate* cache is N x per-node.

It answers the question the single-node constrained profile cannot: does a pod of
memory-constrained nodes behave as **one larger cache**? One node's 80 MiB DRAM /
122 MiB disk is unrealistically small; `3 x 80 MiB` DRAM and `3 x 122 MiB` disk,
wired by gossip (#27) with rendezvous ownership (#28) and peer fetch (#29), is the
actual product shape (issue #160).

Same folder-plus-`run.sh`-plus-README convention as every benchmark. Nothing here
runs in CI.

## What it measures, and the evidence it proves

The benchmark runs the **same three legs** as the single-node constrained profile
— direct-to-origin, Verglas cold, Verglas warm — but **through node 0** of the
pod. The latency table is produced by the shared `benchmarks/tpch` driver.

The point of the pod profile is **not** the latency table; it is the per-node
`/admin/stats` counters. The script prints them in **two parts**, because how the
counters land depends on a subtlety of the current cache engine.

**Peer serving is cache-only (#29), and warm-from-peers (#30) is unmerged.** An
owner serves a peer a block only if that block is *already resident on the owner*;
a peer miss never triggers a fill at the owner. And a node caches a block it owns
only when it is **directly hit** for that block — nothing pushes owned blocks to
their owner in the background yet (that is #30). Two consequences:

- **Part 1 — fresh-pod, node-0 ingress (what the prompt literally asks).** The 22
  queries run through node 0 on a cold pod. Node 0 owns ~1/N of the keys and
  **routes every cross-owner read off-node** — so it produces real peer traffic:
  `peer_misses` climb as node 0 asks owners that are still cold. But because the
  owners never receive a direct client hit (all ingress is node 0), they never
  self-warm, so those fetches **miss**: `peer_hits` stay low and node 0 backfills
  and caches every block itself. Net: with single-endpoint ingress the pod is *no
  better than one constrained node* until #30 lands. This is an honest, useful
  result — the peer-fetch *routing* is proven (peer_misses), and the ceiling it
  hits is exactly the #30 gap.

- **Part 2 — pod-coherence (does the pod act as ONE cache?).** This reproduces the
  realistic pod, where a load balancer spreads clients across nodes (WHITEPAPER
  §3.3) so every node fills the keys it owns. The script warms every owner (a sweep
  of the SF1 objects through *each* node, so each owner is directly hit for its
  share), then reads the whole dataset back **through node 0**. Now cross-owner
  reads are served by peers: `node 0 peer_hits > 0` and the owners'
  `peer_served_blocks > 0`, while the read sweep's **aggregate `backend_fills`
  collapse** — the pod's aggregate disk (`N x 122 MiB`) holds the ~243 MiB
  footprint that no single 122 MiB node can. That is the "aggregate Nx capacity,
  one cache, peers serving cross-owner reads" result.

Ownership is **per object**: each Parquet file is owned wholly by one node
(rendezvous over the object key), so the pod distributes the handful of large SF1
data files across the nodes rather than splitting one file's blocks.

`--admit-probability P` (issue #164) applies **per node** if you want to combine
the pod with resident-biased admission; it is off by default.

## Prerequisites

Identical to `benchmarks/tpch`:

- Built binaries: `cargo build --release` → `target/release/{verglas,verglas-server}`.
- Python 3 (3.13 recommended); `run.sh` reuses `benchmarks/tpch/.venv` and its
  pinned `requirements.txt` (created on first run if absent).
- An S3-compatible origin reachable via the ambient `AWS_*` environment
  (`AWS_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`).
  Load your `.env` first: `set -a; source .env; set +a`. The bucket must exist.
- The **`aws` CLI** for the Part 2 coherence sweep (it lists the seeded objects and
  fetches them through each node). If `aws` is absent, Part 1 still runs and Part 2
  is skipped with a note.

The pod is booted **by this script** (`verglas dev --nodes N ...`) — you do not
start a server yourself. The dev keys are parsed from the pod's banner.

## Ports (uncontended, #194)

`verglas dev` still selects ports by probe-then-bind (issue #194's TOCTOU is
open), so `run.sh` picks the base port with an `lsof` preflight: node `i` takes
the 4-port block `base + i*4 .. base + i*4 + 3` (S3, admin, gossip, peer — the
layout `verglas dev` plans), and the script scans upward from `--base-port`
(default 18400) until it finds a block where **every** port is free right now.
This keeps parallel pod runs off each other and off any squatting orphan.

## Inputs

| Input | Flag | Env | Default |
|-------|------|-----|---------|
| Nodes | `--nodes N` | `POD_NODES` | `3` |
| Per-node DRAM | `--dram SIZE` | `POD_DRAM` | `80MB` |
| Per-node disk | `--cache-size SIZE` | `POD_CACHE_SIZE` | `122MB` |
| Admission fraction | `--admit-probability P` | `POD_ADMIT_PROBABILITY` | off |
| Base port | `--base-port P` | `POD_BASE_PORT` | `18400` |
| Scale factor | `--scale N` | `POD_SCALE` | `1` |
| Origin bucket | `--bucket NAME` | `POD_BUCKET` | `hyperglas` |
| Table prefix | `--prefix PATH` | `POD_PREFIX` | `bench/pod-sf<SCALE>` |
| Catalog file | `--catalog-db PATH` | `POD_CATALOG_DB` | `./catalog.db` |
| Read mode | `--read-mode MODE` | `POD_READ_MODE` | `auto` |
| Cache-medium note | `--cache-note TEXT` | `POD_CACHE_NOTE` | (generic) |
| Output dir | `--out-dir DIR` | `POD_OUT_DIR` | `./out` |

The **prefix must be non-empty** — the guard refuses the bucket root, because
teardown lists-and-deletes the prefix.

## Copy-paste invocation

```bash
# 1. Build once.
cargo build --release

# 2. Load origin creds (both the pod's backend and the direct leg use them).
set -a; source .env; set +a

# 3. Seed SF1 once through the pod's node 0, then run the legs. Each phase boots
#    a FRESH pod and tears it down on exit (ephemeral per-node caches).
benchmarks/pod/run.sh --seed-only  --scale 1 --prefix bench/pod-sf1
benchmarks/pod/run.sh --query-only --scale 1 --prefix bench/pod-sf1 \
  --nodes 3 --dram 80MB --cache-size 122MB \
  --cache-note "APFS on NVMe (Mac Studio internal SSD)"

# Teardown the seeded prefix + catalog when done.
benchmarks/pod/run.sh --teardown --scale 1 --prefix bench/pod-sf1
```

Because seeding writes through the write-passthrough path (it never warms the
read cache), you can seed once and re-run `--query-only` against a fresh pod as
many times as you like; the cold-before-warm ordering inside the query phase does
the rest. For a **two-run stability** check, run `--query-only` twice against a
fresh pod each time.

## Output

`run.sh` writes to `--out-dir` (default `./out`):

- `query.json` — the shared tpch driver's three-leg report (latency table +
  node-0 warm-leg counter delta), machine-readable.
- `stats-before.json` / `stats-after.json` — every node's full `/admin/stats`,
  snapshotted immediately before and after the **Part 1** query phase (the
  before/after delta is the whole three-leg run through node 0).
- `coherence-before.json` / `coherence-after.json` — every node's `/admin/stats`
  bracketing the **Part 2** read-through-node-0 sweep (owners already warm).
- `evidence-part1.txt` / `evidence-part2.txt` — the rendered per-node counter
  deltas and pod aggregate for each part (`pod_stats.py`).

## Files

- `run.sh` — the single entrypoint: port preflight, pod boot/teardown, phase
  dispatch, per-node stats snapshots.
- `pod_stats.py` — stdlib-only per-node `/admin/stats` snapshot + evidence
  renderer (no venv needed).
- The driver itself is reused from `benchmarks/tpch/tpch_bench.py`; this profile
  adds only the pod orchestration and the cross-node evidence.
