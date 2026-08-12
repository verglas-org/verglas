# DuckDB/Quack durable object-store benchmark

This benchmark answers one narrow question first: with the SQL engine held
constant, what changes when DuckDB served by Quack reads and writes durable
Parquet directly from an S3-compatible origin versus through Verglas?

It does not compare a generated in-memory DuckDB relation with a remote durable
system. The coordinator creates real partitioned Parquet in MinIO, runs two
independently bounded Quack servers, and points one at MinIO and one at the
Verglas S3 endpoint. A third Quack server proves that a new compute process can
reuse the shared Verglas cache.

```bash
benchmarks/duckdb-object-store/run.sh --output /tmp/duckdb-object-store.json
```

The default dataset is deliberately larger than four times DuckDB's 256 MiB
operator-memory limit. The servers run with one CPU and 768 MiB container RAM;
Verglas runs with one CPU, 256 MiB RAM, 80 MiB DRAM cache, and a 256 MiB disk
cache. Every workload runs direct, Verglas cold, Verglas warm, and from a second
Quack process against the shared warm cache. The run is invalid unless it sees
DuckDB spill files, MinIO requests, identical result hashes, and direct-origin
readback of Parquet written both directly and through Verglas.

The workloads cover a full scan/aggregate, an external sort, and a hash join.
MinIO's admin trace is hashed into the report. Container IDs, immutable image
IDs, and Docker's applied CPU/RAM limits are recorded rather than inferred.

## R2

The live R2 profile uses the same SQL, limits, and evidence gates. R2's S3 API
requires three independent values: `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, and
`R2_SECRET_ACCESS_KEY`. A Cloudflare API token value is not silently treated as
an S3 key. The isolated bucket created for this work is
`verglas-duckdb-bench-20260812`; export the three R2 values before running the
live profile once it lands.

## What the number means

This is the storage-path comparison: DuckDB 1.5.5 and Quack are identical on
both sides, so it isolates direct object storage from Verglas cache behavior.
The separate product-path comparison (`verglas_query` through its isolated
DataFusion worker) must be reported as a different engine and must never be
folded into this ratio.
