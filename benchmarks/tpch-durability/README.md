# SF10 TPC-H durability benchmark

This is an acceptance harness for issue #135. It is not a smoke profile and it
does not manufacture a report from partial evidence. A successful JSON report
means that a four-voter Verglas ring ran the complete protocol below. Any client,
container, checksum, quorum, or archive error exits nonzero and no report is
written.

## Pinned protocol

The topology is fixed in `compose.yaml`: four `verglas-cache-node` voters,
MinIO `RELEASE.2025-07-23T15-54-02Z`, one consolidated repository-built
`lakekeeper serve-craft`, and no PostgreSQL service or image. The cache voters
are configured exactly as `k=2`, `m=2`, `w=3`; each exposes the canonical
`/wal/v1/{tenant}/{timeline}` ingress. The Lakekeeper service uses its
CRaft-backed Verglas storage adapter and its metadata S3 endpoint is
`cache-0:8333`, so metadata writes as well as data writes traverse Verglas.

The host Python requirements are pinned in `requirements.txt`: DuckDB 1.5.1,
PyArrow 23.0.1, PyIceberg 0.11.1, and boto3 1.42.53. `wal-driver` is a separate
Rust package pinned to the workspace's `verglas-safekeeper` crate. It imports
`WalRequest` and `WalResponse` and therefore uses the canonical strict wire
codec rather than copying the binary frame parser.

Set three fresh, local-only values before running. They are intentionally not
stored in this repository and must not be production credentials.

```sh
export TPCH_DURABILITY_S3_ACCESS_KEY='local-run-access-key'
export TPCH_DURABILITY_S3_SECRET_KEY='replace-with-a-fresh-local-value'
export TPCH_DURABILITY_CLUSTER_SECRET='replace-with-a-fresh-local-value'
python3 -m pip install -r benchmarks/tpch-durability/requirements.txt
```

Start the run with the cache service that is currently elected leader. The
engine's public WAL protocol intentionally does not expose a leader-discovery
operation; identify it from the operator's deployment telemetry before the
destructive step. Passing an arbitrary service invalidates the evidence rather
than silently weakening the test.

```sh
benchmarks/tpch-durability/run.sh \
  --leader cache-0 \
  --output /tmp/tpch-durability-report.json
```

The coordinator performs these operations in order:

1. Starts the fixed topology and refuses a running PostgreSQL container.
2. Uses DuckDB's own `tpch_queries()` corpus and `dbgen(sf = 10)`, exports the
   eight base relations as Parquet, and streams every Parquet record batch into
   PyIceberg tables through the REST catalog and Verglas S3 ingress.
3. Computes all 22 direct DuckDB checksums, kills the declared catalog leader
   while a catalog/object write is in progress, verifies an immediate read with
   three voters, kills two more voters and proves a catalog write is refused,
   then restarts the voters and computes the same 22 Iceberg checksums.
4. Lists every MinIO origin object and compares a full SHA-256 read through
   MinIO against a full SHA-256 read through Verglas. It also requires four
   converged local consensus-state hashes after restart.
5. Uses the canonical Rust driver to append exactly 256 MiB of WAL in 8 MiB
   requests. The coordinator kills the declared leader after four durable
   appends, verifies an immediate three-of-four read, restarts the leader,
   verifies the complete checksum again, and waits for all sixteen 16 MiB
   archive checkpoints.

`validate_report()` is the frozen report gate. It requires exact SF10 row
cardinalities, all 22 matching SHA-256 result values, zero origin mismatches,
four equal replica hashes, no PostgreSQL process, and every WAL/archive quorum
fact. It cannot pass a reduced workload or a boolean-only claim.
