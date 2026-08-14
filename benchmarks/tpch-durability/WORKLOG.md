# Worklog

- #135: Added the SF10 TPC-H durability harness and its frozen fail-closed
  report validator. It starts the fixed four-voter `k=2,m=2,w=3` topology,
  streams PyIceberg data through Verglas, and records checksums, quorum refusal,
  MinIO parity, restart convergence, and archival evidence instead of accepting
  a reduced or synthetic run.
- #135: Added a small Rust WAL driver that imports the engine's canonical
  `WalRequest` and `WalResponse` codec. The fault protocol kills a declared
  leader during a 256 MiB append stream, verifies reads before and after restart,
  and waits for checkpoint-gated archival of all complete WAL segments.
- #135: Corrected the catalog image build to run from the vendored Lakekeeper workspace so its required Cargo cfg is active. Canonical query checksums now ignore legal tie-order differences while retaining every result row.
- #135: Replaced the deleted catalog-service Docker build with explicit current cache-node and Lakekeeper `serve-craft` images. The topology now supplies the image entrypoint's real storage and WAL archive contract, shares its ephemeral S3 credentials with PyIceberg, and keeps the failure mutation outside the frozen TPC-H rows.
- #135: Required the SF10 catalog client and hosted Lakekeeper process to use real Verglas access credentials. The benchmark now fails before startup when the access endpoint, tenant, policy credential, or caller session is absent instead of running an unauthenticated catalog path.
- #135: Changed SF10 Iceberg ingestion to upload DuckDB's immutable Parquet outputs through the Verglas S3 ingress and register those exact files in one table transaction. This keeps every data byte on the cache/writeback path while avoiding hundreds of redundant decode-and-rewrite catalog commits.
- #135: Probe native Lakekeeper readiness through its public `/health` route. The
  benchmark no longer waits on the deliberately authenticated catalog config
  route and misclassifies a correct 401 response as failed startup. PyIceberg is
  also given the REST root at `/catalog`, leaving it to append `/v1` exactly once.
- #135: Split the benchmark's immutable catalog checkpoints into their own
  MinIO bucket while retaining WAL segments in the Postgres archive bucket.
  The four-node topology now exercises the same distinct durability bindings as cloud.
- #135: Pass DuckDB's Arrow schema directly into PyIceberg table creation so it
  assigns fresh Iceberg field IDs before `add_files` derives the canonical name
  mapping. Existing Parquet conversion cannot run before that mapping exists.
- #135: Read survivor and restarted-voter health from Docker Compose's
  in-container health state. Docker Desktop host-port proxy resets no longer
  hide a healthy voter; catalog reads still prove the real N-1 data path.
- #135: Give hosted Lakekeeper all four Verglas S3 ingresses for immutable
  metadata reads and writes. Killing the catalog leader no longer strands
  metadata verification on that node's DNS name.
- #135: Registered the benchmark timeline's WAL bucket through the authenticated
  cache admin contract before launching the WAL driver. The topology no longer
  relies on a process-global WAL archive bucket.
- #135: Restart every voter stopped by the two-of-four refusal test in one
  Compose operation before waiting for health. This restores fixed peer DNS and
  enough voters to form quorum instead of deadlocking on sequential readiness.
- #135: Round-robin immutable SF10 Parquet uploads, origin parity reads, and the
  22 post-failure DuckDB queries across all four S3 ingresses. The durability
  benchmark no longer pins the customer data path to cache-0.
- #135: Require every uploaded Parquet object to become visible through every
  S3 ingress before publishing its Iceberg snapshot. This makes the benchmark's
  commit barrier cover the write-back propagation window across the full ring.
- #135: Commit the WAL archive binding through the restarted cache container's
  loopback admin listener. Docker Desktop host-port proxy resets can no longer
  turn a healthy control route into a false 404 after the Iceberg fault phase.
