# Local lite pairing acceptance (FROZEN RIME evaluation protocol — v3)

Candidates implement `scripts/local-lite.sh` satisfying this protocol exactly;
the coordinator re-runs it independently and probes the held-up stack by hand.

> **v2 amendment (coordinator evaluator repair, 2026-08-17).** v1 mandated ONE
> cache-node. That was an evaluator defect, not the product topology: the lite
> tier is a FOUR-machine ring (4 machines totaling ~1 vCPU). The engine's
> `ring.rs` ≥3-peer requirement is a deliberate erasure-coding durability
> policy (k=2,m=2,w=3) and must NOT be loosened, faked with self-referential
> peers, or worked around. Candidates were correct to refuse both. Only the
> coordinator edited this file.

## `scripts/local-lite.sh up`
Boots, on localhost, from CURRENT source trees:
- An S3 object store (MinIO via docker is fine; any free ports).
- FOUR `verglas-cache-node` processes built `--release` from THIS tree,
  forming the real fragment ring (node N ∈ 1..4):
  - S3 listener: 18333 / 18343 / 18353 / 18363
  - admin:       18334 / 18344 / 18354 / 18364
  - safekeeper/ring: 18335 / 18345 / 18355 / 18365, with
    `VERGLAS_RING_PEERS` listing all four safekeeper addresses on every node.
  Each node gets env parity to `verglas-cloud/images/cache/boot.sh`
  (backend bucket on the local store, auth keypair, `[catalog]` block pointing
  at its LOCAL lakekeeper instance, writeback enabled as boot.sh renders it).
- FOUR Lakekeeper `serve-craft` instances built from `$LAKEKEEPER_DIR`
  (default `~/code/verglas-lakekeeper`, current checkout), one colocated per
  node, on 18181 / 18191 / 18201 / 18211, env parity to
  `verglas-cloud/images/lakekeeper/boot.sh`. Prod contract this mirrors:
  lakekeeper runs stateless on ALL nodes (Cloudflare load-balances across
  them) and each instance's `--endpoints` points ONLY at its own node's
  loopback safekeeper — the loopback write path is the latency design, so the
  local launcher must preserve it (no cross-node endpoint lists).
  The candidate updates BOTH boot scripts and this launcher to one coherent
  contract.
`up --hold` leaves everything running for manual probing. `down` tears down.

## `scripts/local-lite.sh check`
Runs against the booted stack; prints each step's latency; exit 0 only if ALL pass:
1. CREATE:  `POST 127.0.0.1:18334/v1/ingest/main.pairing_events?mode=create&format=jsonl`
   with one NDJSON row → HTTP 200, body contains `"snapshot_id"` and `"records_added":1`.
2. ASYNC APPEND ×3: same URL `mode=append` → HTTP 200/202, `successful_rows`
   in body, ack latency printed.
3. SYNC APPEND: `...&wait=true` (or `commit=sync`) → HTTP 200 with a committed
   snapshot id.
4. READ-BACK (poll ≤10s for async commits to land):
   `GET /v1/tables/main.pairing_events/rows?limit=10` → all 5 rows present.
5. CATALOG HEALTH: no engine log on ANY node contains `WarehouseIdIsNotUUID`,
   `catalog gateway error`, or 5xx catalog poll failures after startup settles.
6. STATELESS PARITY: `GET 127.0.0.1:18191/catalog/v1/config?warehouse=lite`
   (a NON-primary lakekeeper instance, authed) returns the same warehouse
   config as :18181 — proves any instance can serve reads behind the LB.
7. CROSS-NODE READ: step 4's read-back repeated against a different node's
   admin port (e.g. :18344) returns the same 5 rows — commits are
   ring-durable, not node-local.
8. Latency REPORT (informational, not a gate): p50 of the async appends.
9. SQL (v3): `POST 127.0.0.1:18400/v1/query` with `{"sql":"SELECT COUNT(*) AS n FROM main.pairing_events"}`
   → HTTP 200, JSON body whose data rows contain n = 5 (the five committed
   rows from steps 1–3). A second query with an intentional syntax error →
   HTTP 4xx with a JSON error, NOT a hang or 5xx. Query-node memory limit
   set to 1 GiB for the run; a query exceeding it fails cleanly.

> **v3 amendment (coordinator, 2026-08-19).** Adds the tenant query node:
> `up` additionally boots ONE query-node binary (new `bins/query-node`)
> on :18400, configured from env with the local catalog URI, warehouse,
> and ring S3 endpoint + keypair — the same coordinates a tenant query
> machine receives in production. It serves POST /v1/query only, executing
> through crates/verglas-iceberg's PreparedCatalog/query_stream (DataFusion)
> with a configurable memory limit, returning the /v0 result shape
> ({meta, data, rows, statistics}). Candidates may not edit this file.

## Hard gates for a candidate
- `check` green with engine = this tree, lakekeeper = the candidate's
  reconciled branch of verglas-lakekeeper.
- `cargo test -p verglas-iceberg` + cache-node package tests green (engine).
- Lakekeeper: `cargo test -p lakekeeper-storage-verglas -p lakekeeper-bin` green.
- `verglas-cloud` boot scripts (`images/cache/boot.sh`,
  `images/lakekeeper/boot.sh`) updated to the same contract in the same
  change, plus a `VERSIONS.lock` at the verglas-cloud repo root recording
  {engine_sha, lakekeeper_sha, boot_contract} — the image build's pin manifest.
- No candidate edits to this file (v2 was written by the coordinator).
