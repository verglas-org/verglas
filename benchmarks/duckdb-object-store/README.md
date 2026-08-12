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

Pass `--cache-capacity-bytes 2147483648` for a 2 GiB NVMe-resident profile.
The report records the capacity and cache counters returned by the running
Verglas server; the profile label is never inferred from the requested value.

The default dataset uses 40 million rows and is valid only when its measured
Parquet footprint is larger than four times DuckDB's 352 MB operator-memory
limit. The servers run with one CPU and a 2 GiB container ceiling; the larger
process ceiling leaves room for Quack and HTTP buffers without weakening the
352 MB DuckDB allocator limit that leaves QuackStore enough allocator headroom
while still forcing disk spill. Verglas runs with one
CPU, 256 MiB RAM, 80 MiB DRAM cache, and a 256 MiB disk
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
`R2_SECRET_ACCESS_KEY`. The isolated bucket is
`verglas-duckdb-bench-20260812`. `CLOUDFLARE_API_TOKEN` must also be an account
token allowed to read R2 analytics, because the report obtains its request
counts from Cloudflare's GraphQL operations dataset.

```bash
export R2_ACCOUNT_ID=...
export R2_ACCESS_KEY_ID=...
export R2_SECRET_ACCESS_KEY=...
export CLOUDFLARE_API_TOKEN=...
python3 benchmarks/duckdb-object-store/benchmark.py \
  --profile live-r2 \
  --cache-capacity-bytes 268435456 \
  --output /path/on/external/disk/r2-result.json
```

The coordinator mounts S3 credentials into containers as read-only files; it
does not place the live secret in container arguments or the report. The R2
endpoint uses TLS and region `auto`. Dataset generation has a 512 MiB DuckDB
allocator and a 1 GiB setup container because remote multipart upload has a
larger buffering requirement. That setup is not timed. All measured Quack
servers remain single-CPU with a 352 MB DuckDB allocator and 2 GiB process
ceiling.

The live profile runs five isolated repetitions. Each repetition measures seven
legs: direct R2; QuackStore cold, warm, and a newly started Quack process that
reuses its persistent cache; then Verglas cold, warm, and a newly started Quack
process that reuses Verglas. QuackStore is installed in the measured server
with `INSTALL quackstore FROM community; LOAD quackstore`, reads the immutable
`quackstore://s3://...` data URI, and uses the extension's documented global
immutable-cache settings. Its exact 256 MiB logical budget is stored in
`/quackstore-cache/cache.bin` on the external benchmark disk. For each
workload, the primary QuackStore process clears that file, runs cold and warm,
stops, and only then does a fresh process reopen the same file for the shared
warm leg. The report records logical capacity, file length, allocated bytes,
and the non-overlapping container identities for every handoff. It writes five
numbered full raw reports beside the requested summary artifact, and the
summary retains each report's digest and raw evidence. The report rejects a
run without observed nonzero occupancy for both caches, live R2 operation
evidence, container/image provenance, cgroup limits, spill, result
equivalence, and direct-R2 readback of writes.

## What the number means

This is the storage-path comparison: DuckDB 1.5.5 and Quack are identical on
both sides, so it isolates direct object storage from Verglas cache behavior.
The separate product-path comparison (`verglas_query` through its isolated
DataFusion worker) must be reported as a different engine and must never be
folded into this ratio.
