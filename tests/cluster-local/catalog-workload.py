#!/usr/bin/env python3
"""Drives real Iceberg traffic through the Lakekeeper fork over Verglas.

Catalog operations go to serve-craft, whose authoritative state lives in
Verglas consensus. Data files go to the Verglas S3 endpoint, so both the
catalog path and the object path run through Verglas.

Reports the origin PUT count MinIO served, which is the independent witness
for whether small data files are being aggregated.
"""
import os, sys, time, urllib.request

CATALOG = os.environ.get("CATALOG_URI", "http://127.0.0.1:18181/catalog")
S3 = os.environ.get("S3_ENDPOINT", "http://127.0.0.1:18333")
MINIO_METRICS = os.environ.get("MINIO_METRICS_URL", "http://127.0.0.1:19000/minio/v2/metrics/cluster")
WAREHOUSE = os.environ.get("WAREHOUSE", "lite")
FILES = int(os.environ.get("FILES", "50"))
ROWS = int(os.environ.get("ROWS", "100"))


def putcount() -> int:
    with urllib.request.urlopen(MINIO_METRICS, timeout=10) as r:
        total = 0
        for line in r.read().decode().splitlines():
            if line.startswith("minio_s3_requests_total{") and 'api="putobject"' in line:
                total += int(float(line.rsplit(" ", 1)[1]))
        return total


def main() -> int:
    try:
        import pyarrow as pa
        from pyiceberg.catalog.rest import RestCatalog
    except ImportError as e:
        print(f"MISSING DEPENDENCY: {e}", file=sys.stderr)
        return 3

    cat = RestCatalog(
        "verglas",
        uri=CATALOG,
        warehouse=WAREHOUSE,
        **{"s3.endpoint": S3,
           "s3.access-key-id": "verglas-engine",
           "s3.secret-access-key": "verglas-engine-secret",
           "s3.path-style-access": "true"},
    )

    ns = ("bench",)
    try:
        cat.create_namespace(ns)
    except Exception:
        pass

    schema = pa.schema([("id", pa.int64()), ("payload", pa.string())])
    ident = "bench.small_files"
    try:
        cat.drop_table(ident)
    except Exception:
        pass
    tbl = cat.create_table(ident, schema=schema)

    before = putcount()
    start = time.time()
    # Many small appends: each is its own Parquet data file plus a metadata
    # commit. This is the workload #164 section 9 is about.
    for i in range(FILES):
        batch = pa.table(
            {"id": pa.array(range(i * ROWS, (i + 1) * ROWS), pa.int64()),
             "payload": pa.array([f"row-{i}-{j}" for j in range(ROWS)], pa.string())},
            schema=schema,
        )
        tbl.append(batch)
    elapsed = time.time() - start
    time.sleep(int(os.environ.get("DRAIN_WAIT", "20")))
    after = putcount()

    scanned = tbl.scan().to_arrow().num_rows
    files = len(list(tbl.scan().plan_files()))

    print(f"data_files_appended={FILES}")
    print(f"rows_per_file={ROWS}")
    print(f"rows_scanned_back={scanned} (expected {FILES * ROWS})")
    print(f"data_files_in_plan={files}")
    print(f"origin_put_delta={after - before}")
    print(f"commit_seconds={elapsed:.1f}")
    return 0 if scanned == FILES * ROWS else 1


if __name__ == "__main__":
    sys.exit(main())
