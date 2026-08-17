# Local lite pairing acceptance (FROZEN RIME evaluation protocol)

Candidates implement `scripts/local-lite.sh` satisfying this protocol exactly;
the coordinator re-runs it independently and probes the held-up stack by hand.

## `scripts/local-lite.sh up`
Boots, on localhost, from CURRENT source trees:
- An S3 object store (MinIO via docker is fine; any free ports).
- ONE `verglas-cache-node` built `--release` from THIS tree: S3 listener :18333,
  admin :18334, configured with env parity to
  `verglas-cloud/images/cache/boot.sh` (backend bucket on the local store,
  auth keypair, `[catalog]` block pointing at the local Lakekeeper,
  writeback enabled as boot.sh renders it).
- Lakekeeper `serve-craft` built from `$LAKEKEEPER_DIR` (default
  `~/code/verglas-lakekeeper`, current checkout) on :18181, launched with env
  parity to `verglas-cloud/images/lakekeeper/boot.sh` — the candidate updates
  BOTH boot scripts and this launcher to one coherent contract.
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
5. CATALOG HEALTH: engine log contains NO `WarehouseIdIsNotUUID`, no
   `catalog gateway error`, no 5xx catalog poll failures after startup settles.
6. Latency REPORT (informational, not a gate): p50 of the async appends.

## Hard gates for a candidate
- `check` green with engine = this tree, lakekeeper = the candidate's
  reconciled branch of verglas-lakekeeper.
- `cargo test -p verglas-iceberg` + cache-node package tests green (engine).
- Lakekeeper: `cargo test -p lakekeeper-storage-verglas -p lakekeeper-bin` green.
- `verglas-cloud` boot scripts (`images/cache/boot.sh`,
  `images/lakekeeper/boot.sh`) updated to the same contract in the same
  change, plus a `VERSIONS.lock` at the verglas-cloud repo root recording
  {engine_sha, lakekeeper_sha, boot_contract} — the image build's pin manifest.
- No edits to this file.
