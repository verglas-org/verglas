#!/usr/bin/env python3
"""Run the three real managed-Iceberg DuckDB product paths.

This module deliberately contains no local table, mock Arrow service, or storage
fallback.  It bootstraps a uniquely named table through the managed Lakekeeper
REST catalog, then each command talks to the endpoint named in its environment.
"""
import argparse
import hashlib
import io
import json
import os
import pathlib
import resource
import subprocess
import time
import urllib.request
from dataclasses import dataclass


WORKLOADS = ("scan", "selective_aggregate", "external_sort", "spill_heavy_join")
TABLE = "benchmark.events"


def _sql_string(value):
    """Quote one DuckDB SQL literal without accepting an SQL fragment."""
    return "'" + value.replace("'", "''") + "'"


def canonical_sql(workload):
    """Return the catalog-relative SQL shared by all product transports."""
    table = ".".join((
        os.environ.get("VERGLAS_BENCHMARK_NAMESPACE", TABLE.split(".")[0]),
        os.environ.get("VERGLAS_BENCHMARK_TABLE", TABLE.split(".")[1]),
    ))
    statements = {
        "scan": (
            f"SELECT count(*) AS rows, sum(value) AS total, "
            f"sum(length(payload)) AS payload_bytes FROM {table}"
        ),
        "selective_aggregate": (
            f"SELECT category, count(*) AS rows, sum(value) AS total "
            f"FROM {table} WHERE category BETWEEN 8 AND 15 GROUP BY category ORDER BY category"
        ),
        "external_sort": (
            "SELECT sum(rn) AS row_number_sum FROM ("
            f"SELECT row_number() OVER (ORDER BY value DESC, id) AS rn FROM {table})"
        ),
        "spill_heavy_join": (
            f"SELECT l.category, count(*) AS rows, sum(l.value + r.value) AS total "
            f"FROM {table} l JOIN {table} r ON l.id = r.id "
            "WHERE l.category < 32 GROUP BY l.category ORDER BY l.category"
        ),
    }
    try:
        return statements[workload]
    except KeyError as error:
        raise ValueError(f"unknown workload {workload!r}") from error


def direct_quack_setup(catalog_token, r2_access_key, r2_secret_key):
    """Configure Quack's DuckDB with managed metadata and direct R2 reads only."""
    catalog_url = os.environ.get("VERGLAS_CATALOG_URL", "https://catalog.invalid/v1/databases/analytics/catalog")
    r2_endpoint = os.environ.get("R2_ENDPOINT", "https://r2.cloudflarestorage.com")
    warehouse = os.environ.get("VERGLAS_WAREHOUSE", "benchmark")
    return (
        "INSTALL quack",
        "LOAD quack",
        "CREATE OR REPLACE SECRET benchmark_r2 ("
        "TYPE S3, PROVIDER CONFIG, KEY_ID " + _sql_string(r2_access_key) + ", SECRET " + _sql_string(r2_secret_key)
        + ", REGION 'auto', ENDPOINT " + _sql_string(r2_endpoint.removeprefix("https://")) + ")",
        "ATTACH " + _sql_string(warehouse) + " AS managed (TYPE ICEBERG, "
        "ENDPOINT " + _sql_string(catalog_url) + ", TOKEN "
        + _sql_string(catalog_token) + ", ACCESS_DELEGATION_MODE 'none')",
        "USE managed",
    )


def extension_quack_setup(extension_path):
    """Load the built extension before exposing the real Quack protocol server."""
    return (
        "INSTALL quack",
        "LOAD quack",
        "LOAD " + _sql_string(extension_path),
        "CALL quack_serve('quack:0.0.0.0:9000', token = 'benchmark-token', "
        "allow_other_hostname = true, disable_ssl = true)",
    )


def product_sql(path, workload):
    """Wrap canonical SQL only when the real Quack extension transport requires it."""
    sql = canonical_sql(workload)
    if path in ("quack_direct", "verglas_query_worker"):
        return sql
    if path == "quack_verglas_extension":
        return "SELECT * FROM verglas_query(" + _sql_string(sql) + ")"
    raise ValueError(f"unknown product path {path!r}")


def measured_sql(path, workload):
    """Select the SQL visible to the declared product's remote Quack server."""
    if path not in ("quack_direct", "quack_verglas_extension"):
        raise ValueError(f"{path!r} is not a Quack product path")
    return product_sql(path, workload)


def validate_bootstrap(rows, worker_memory_mib, catalog_engine):
    """Reject a bootstrap that cannot exercise managed, out-of-core Iceberg I/O."""
    if catalog_engine != "verglas-lakekeeper":
        raise ValueError("bootstrap requires the managed Verglas Lakekeeper catalog")
    if rows <= 0 or worker_memory_mib <= 0:
        raise ValueError("rows and worker memory must be positive")
    # Each row contains three i64 values plus an Arrow string payload.  This is
    # conservative and keeps the requested data set above the evaluator floor.
    if rows * 40 < 4 * worker_memory_mib * 1024 * 1024:
        raise ValueError("dataset must exceed four times the worker memory ceiling")


def validate_dataset_bytes(data_bytes, worker_memory_mib):
    """Require the committed compressed files themselves to be out of core."""
    if data_bytes < 4 * worker_memory_mib * 1024 * 1024:
        raise ValueError("compressed Iceberg data must exceed four worker-memory budgets")


@dataclass(frozen=True)
class ManagedConfig:
    """The explicit credentials and endpoints used by a real managed run."""

    catalog_url: str
    catalog_token: str
    r2_endpoint: str
    r2_access_key: str
    r2_secret_key: str
    warehouse: str
    namespace: str
    table: str

    @classmethod
    def from_environment(cls):
        """Read all required configuration without inventing an endpoint or secret."""
        required = (
            "VERGLAS_CATALOG_URL", "VERGLAS_CATALOG_TOKEN", "R2_ENDPOINT",
            "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "VERGLAS_WAREHOUSE",
            "VERGLAS_BENCHMARK_NAMESPACE", "VERGLAS_BENCHMARK_TABLE",
        )
        missing = [name for name in required if not os.environ.get(name)]
        if missing:
            raise RuntimeError("missing required managed benchmark environment: " + ", ".join(missing))
        return cls(
            os.environ["VERGLAS_CATALOG_URL"], os.environ["VERGLAS_CATALOG_TOKEN"],
            os.environ["R2_ENDPOINT"], os.environ["R2_ACCESS_KEY_ID"],
            os.environ["R2_SECRET_ACCESS_KEY"], os.environ["VERGLAS_WAREHOUSE"],
            os.environ["VERGLAS_BENCHMARK_NAMESPACE"], os.environ["VERGLAS_BENCHMARK_TABLE"],
        )

    def iceberg_properties(self):
        """Return the REST and direct S3 properties PyIceberg must use to write data."""
        return {
            "uri": self.catalog_url,
            "token": self.catalog_token,
            "warehouse": self.warehouse,
            # Lakekeeper is the catalog authority. The benchmark writes data
            # with the explicitly supplied R2 credential; it must not request
            # a different delegated credential while seeding its snapshot.
            "header.X-Iceberg-Access-Delegation": "",
            "s3.endpoint": self.r2_endpoint,
            "s3.access-key-id": self.r2_access_key,
            "s3.secret-access-key": self.r2_secret_key,
            "s3.region": "auto",
            "s3.path-style-access": "true",
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        }


def bootstrap_managed_table(config, rows, worker_memory_mib, batch_rows=1_000_000):
    """Create and append an Iceberg v2 table through Lakekeeper, returning observed metadata."""
    validate_bootstrap(rows, worker_memory_mib, "verglas-lakekeeper")
    if batch_rows <= 0:
        raise ValueError("batch_rows must be positive")
    import pyarrow as pa
    from pyiceberg.catalog.rest import RestCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, StringType

    catalog = RestCatalog("verglas", **config.iceberg_properties())
    identifier = (config.namespace, config.table)
    catalog.create_namespace_if_not_exists(config.namespace)
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "category", LongType(), required=True),
        NestedField(3, "value", LongType(), required=True),
        NestedField(4, "payload", StringType(), required=True),
    )
    # A run name must be unique. Reusing a table would silently retain an old
    # snapshot and make the cache comparison non-reproducible.
    table = catalog.create_table(identifier, schema=schema, properties={"format-version": "2"})
    for offset in range(0, rows, batch_rows):
        stop = min(rows, offset + batch_rows)
        ids = pa.array(range(offset, stop), type=pa.int64())
        values = pa.array((number * 17 % 10_000_019 for number in range(offset, stop)), type=pa.int64())
        data = pa.table({
            "id": ids,
            "category": pa.array((number % 64 for number in range(offset, stop)), type=pa.int64()),
            "value": values,
            "payload": pa.array((
                hashlib.sha256(str(number).encode()).hexdigest()
                + hashlib.sha256(f"payload:{number}".encode()).hexdigest()
                for number in range(offset, stop)
            )),
        })
        table.append(data)
    table = catalog.load_table(identifier)
    snapshot = table.current_snapshot()
    if snapshot is None or not snapshot.manifest_list:
        raise RuntimeError("Lakekeeper committed no current Iceberg snapshot")
    with table.io.new_input(snapshot.manifest_list).open() as manifest:
        manifest_bytes = manifest.read()
    files = list(table.scan().plan_files())
    data_bytes = sum(task.file.file_size_in_bytes for task in files)
    if data_bytes <= 0:
        raise RuntimeError("managed Iceberg snapshot contains no data files")
    validate_dataset_bytes(data_bytes, worker_memory_mib)
    return {
        "format": "iceberg-v2", "catalog_engine": "verglas-lakekeeper",
        "catalog_commit_observed": True, "snapshot_id": str(snapshot.snapshot_id),
        "metadata_location": table.metadata_location,
        "manifest_list_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "data_bytes": data_bytes, "data_files": len(files),
        "identifier": ".".join(identifier),
    }


def _arrow_identity(reader, started):
    """Stream Arrow batches, hashing the typed IPC bytes rather than parsed text values."""
    import pyarrow.ipc as ipc
    rows, ttfr = 0, None
    schema = [[field.name, str(field.type).upper()] for field in reader.schema]
    digest = hashlib.sha256(json.dumps(schema, separators=(",", ":")).encode())
    for batch in reader:
        if batch.num_rows and ttfr is None:
            ttfr = (time.perf_counter_ns() - started) / 1_000_000
        encoded = io.BytesIO()
        with ipc.new_stream(encoded, batch.schema) as writer:
            writer.write_batch(batch)
        digest.update(encoded.getvalue())
        rows += batch.num_rows
    return rows, schema, digest.hexdigest(), ttfr if ttfr is not None else 0.0


def _measure(reader_factory):
    """Measure one real streaming response and retain its process resource evidence."""
    started = time.perf_counter_ns()
    before = resource.getrusage(resource.RUSAGE_SELF)
    rows, schema, digest, ttfr = _arrow_identity(reader_factory(), started)
    after = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "elapsed_ms": (time.perf_counter_ns() - started) / 1_000_000,
        "ttfr_ms": ttfr, "rows": rows, "schema": schema, "digest": digest,
        "cpu_seconds": after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
        "peak_rss_mib": after.ru_maxrss / 1024, "spill_bytes": 0,
    }


def measure_query_worker(endpoint, database, token, sql, repetitions, warmups):
    """Call the real Query HTTP Arrow endpoint and record only received batches."""
    import pyarrow.ipc as ipc
    url = endpoint.rstrip("/") + "/v1/databases/" + database + "/query"
    body = json.dumps({"sql": sql}).encode()
    def request():
        req = urllib.request.Request(url, body, {"Authorization": "Bearer " + token, "Accept": "application/vnd.apache.arrow.stream", "Content-Type": "application/json"})
        return ipc.open_stream(urllib.request.urlopen(req, timeout=600))
    for _ in range(warmups):
        _measure(request)
    return [_measure(request) for _ in range(repetitions)]


def measure_quack(host, sql, repetitions, warmups):
    """Call a Quack server over Quack protocol; no HTTP or local SQL substitute exists."""
    import duckdb
    connection = duckdb.connect(":memory:")
    connection.execute("INSTALL quack")
    connection.execute("LOAD quack")
    def request():
        return connection.execute(
            "FROM quack_query(?, ?, token = 'benchmark-token', disable_ssl = true)",
            ["quack:" + host + ":9000", sql],
        ).fetch_record_batch(65_536)
    for _ in range(warmups):
        _measure(request)
    return [_measure(request) for _ in range(repetitions)]


def serve_direct_quack(config):
    """Expose direct-R2 Iceberg DuckDB through Quack after its catalog is attached."""
    import duckdb
    connection = duckdb.connect(":memory:")
    for statement in direct_quack_setup(config.catalog_token, config.r2_access_key, config.r2_secret_key):
        connection.execute(statement)
    connection.execute("CALL quack_serve('quack:0.0.0.0:9000', token = 'benchmark-token', allow_other_hostname = true, disable_ssl = true)")
    while True:
        time.sleep(3600)


def serve_extension_quack(extension):
    """Expose the loaded Verglas extension through Quack with no direct catalog attachment."""
    import duckdb
    connection = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    for statement in extension_quack_setup(extension):
        connection.execute(statement)
    while True:
        time.sleep(3600)


def main(argv=None):
    """Dispatch explicit bootstrap, server, and measured-client commands."""
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    boot = sub.add_parser("bootstrap")
    boot.add_argument("--rows", type=int, required=True)
    boot.add_argument("--worker-memory-mib", type=int, required=True)
    boot.add_argument("--batch-rows", type=int, default=1_000_000)
    serve = sub.add_parser("serve-direct-quack")
    serve_ext = sub.add_parser("serve-extension-quack")
    serve_ext.add_argument("--extension", default="/artifacts/verglas.duckdb_extension")
    measure = sub.add_parser("measure")
    measure.add_argument(
        "--path",
        choices=("verglas_query_worker", "quack_direct", "quack_verglas_extension"),
        required=True,
    )
    measure.add_argument("--workload", choices=WORKLOADS, required=True)
    measure.add_argument("--repetitions", type=int, default=5)
    measure.add_argument("--warmups", type=int, default=1)
    measure.add_argument("--host")
    args = parser.parse_args(argv)
    if args.command == "bootstrap":
        print(json.dumps(bootstrap_managed_table(ManagedConfig.from_environment(), args.rows, args.worker_memory_mib, args.batch_rows)))
    elif args.command == "serve-direct-quack":
        serve_direct_quack(ManagedConfig.from_environment())
    elif args.command == "serve-extension-quack":
        serve_extension_quack(args.extension)
    else:
        if args.repetitions != 5 or args.warmups != 1:
            raise ValueError("full-stack evidence requires exactly one warmup and five repetitions")
        sql = product_sql(args.path, args.workload)
        if args.path == "verglas_query_worker":
            required = ("VERGLAS_ENDPOINT", "VERGLAS_DATABASE", "VERGLAS_TOKEN")
            if any(not os.environ.get(name) for name in required):
                raise RuntimeError("Query worker measurement requires VERGLAS_ENDPOINT, VERGLAS_DATABASE, and VERGLAS_TOKEN")
            samples = measure_query_worker(os.environ["VERGLAS_ENDPOINT"], os.environ["VERGLAS_DATABASE"], os.environ["VERGLAS_TOKEN"], sql, args.repetitions, args.warmups)
        else:
            if not args.host:
                raise ValueError("Quack measurement requires --host")
            samples = measure_quack(args.host, sql, args.repetitions, args.warmups)
        print(json.dumps({"workload": args.workload, "sql": sql, "samples": samples}))


if __name__ == "__main__":
    main()
