#!/usr/bin/env python3
"""Drives real Iceberg traffic through the Catalog fork over Verglas.

Catalog operations go to the node's catalog port, whose authoritative state
lives in Verglas consensus. Data files go to the Verglas S3 endpoint, so both
the catalog path and the object path run through Verglas.

Counts the objects that actually reached the origin under the warehouse
prefix, which is the independent witness for whether small data files are
being aggregated rather than written through one-for-one.
"""
import os, sys, time

CATALOG = os.environ.get("CATALOG_URI", "http://127.0.0.1:18181/catalog")
S3 = os.environ.get("S3_ENDPOINT", "http://127.0.0.1:18333")
WAREHOUSE = os.environ.get("WAREHOUSE", "lite")
FILES = int(os.environ.get("FILES", "50"))
ROWS = int(os.environ.get("ROWS", "100"))
ORIGIN_ENDPOINT = os.environ.get("ORIGIN_ENDPOINT", "")
ORIGIN_BUCKET = os.environ.get("ORIGIN_BUCKET", "cascadelabs")
ORIGIN_PREFIX = os.environ.get("ORIGIN_PREFIX", "_verglas-test/warehouse")


def origin_objects() -> int:
    """Objects present at the origin under the warehouse prefix.

    Counted by listing rather than by a request metric because the origin is a
    real S3 service, not an instrumented local one. `KeyCount` is omitted from
    a truncated response, so this pages and sums instead of reading it.
    """
    if not ORIGIN_ENDPOINT:
        return -1
    import boto3
    client = boto3.client(
        "s3",
        endpoint_url=ORIGIN_ENDPOINT,
        aws_access_key_id=os.environ["ORIGIN_AK"],
        aws_secret_access_key=os.environ["ORIGIN_SK"],
        region_name="auto",
    )
    total, token = 0, None
    while True:
        kwargs = {"Bucket": ORIGIN_BUCKET, "Prefix": ORIGIN_PREFIX}
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        total += len(page.get("Contents", []))
        if not page.get("IsTruncated"):
            return total
        token = page["NextContinuationToken"]


def main() -> int:
    try:
        import pyarrow as pa
        from pyiceberg.catalog.rest import RestCatalog
    except ImportError as e:
        print(f"MISSING DEPENDENCY: {e}", file=sys.stderr)
        return 3

    # The hosted catalog validates an external bearer on every request; the
    # local credential is minted by mint-credential.py against a throwaway key.
    token = os.environ.get("CATALOG_TOKEN", "")
    if not token:
        print("MISSING CATALOG_TOKEN", file=sys.stderr)
        return 3
    cat = RestCatalog(
        "verglas",
        uri=CATALOG,
        warehouse=WAREHOUSE,
        token=token,
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

    before = origin_objects()
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
    after = origin_objects()

    scanned = tbl.scan().to_arrow().num_rows
    files = len(list(tbl.scan().plan_files()))

    print(f"data_files_appended={FILES}")
    print(f"rows_per_file={ROWS}")
    print(f"rows_scanned_back={scanned} (expected {FILES * ROWS})")
    print(f"data_files_in_plan={files}")
    print(f"origin_object_delta={after - before}")
    print(f"commit_seconds={elapsed:.1f}")
    return 0 if scanned == FILES * ROWS else 1


if __name__ == "__main__":
    sys.exit(main())
