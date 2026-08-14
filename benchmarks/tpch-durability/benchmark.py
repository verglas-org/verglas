#!/usr/bin/env python3
"""Run the reproducible four-voter SF10 durability acceptance protocol.

This coordinator records observations from real containers and real clients.  It
does not turn an unavailable service, a reduced workload, or a failed quorum
operation into a passing report.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import pathlib
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Iterable, Mapping
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parent
REPOSITORY = ROOT.parents[1]
COMPOSE = ROOT / "compose.yaml"
LAKEKEEPER_HEALTH_URL = "http://127.0.0.1:18181/health"
LAKEKEEPER_CATALOG_URI = "http://127.0.0.1:18181/catalog"
S3_INGRESS_ENDPOINTS = (
    "127.0.0.1:18333",
    "127.0.0.1:18343",
    "127.0.0.1:18353",
    "127.0.0.1:18363",
)
WAL_BYTES = 256 * 1024 * 1024
WAL_SEGMENT_BYTES = 16 * 1024 * 1024
SF10_ROWS = {
    "region": 5,
    "nation": 25,
    "supplier": 100_000,
    "customer": 1_500_000,
    "part": 2_000_000,
    "partsupp": 8_000_000,
    "orders": 15_000_000,
    "lineitem": 59_986_052,
}
TPCH_TABLES = tuple(SF10_ROWS)
QUERY_IDS = tuple(range(1, 23))
MIN_LOGICAL_BYTES = 4_000_000_000
MIN_ORIGIN_BYTES = 2_000_000_000
MIN_ORIGIN_OBJECTS = 80


class EvidenceError(RuntimeError):
    """Raised when an observed operation cannot support a durability claim."""


def require(condition: bool, message: str) -> None:
    """Reject an incomplete report or observation at its point of use."""
    if not condition:
        raise ValueError(message)


def exact_bool(value: Any, name: str) -> None:
    """Require a JSON boolean rather than accepting truthy substitutes."""
    require(type(value) is bool, f"{name} must be a boolean")


def nonnegative_int(value: Any, name: str) -> int:
    """Return one nonnegative JSON integer, rejecting booleans and floats."""
    require(type(value) is int and value >= 0, f"{name} must be a nonnegative integer")
    return value


def require_mapping(value: Any, name: str) -> Mapping[str, Any]:
    """Return a mapping when a report field has the expected JSON shape."""
    require(isinstance(value, Mapping), f"{name} must be an object")
    return value


def validate_checksums(value: Any, name: str) -> dict[str, str]:
    """Validate the complete numbered set of canonical TPC-H result digests."""
    checksums = require_mapping(value, name)
    expected = {str(query) for query in QUERY_IDS}
    require(set(checksums) == expected, f"{name} must contain checksums for queries 1 through 22")
    for query, checksum in checksums.items():
        require(isinstance(checksum, str) and checksum, f"{name}.{query} must be a nonempty canonical checksum")
    return dict(checksums)


def validate_report(report: Mapping[str, Any]) -> None:
    """Fail closed unless a report proves every frozen SF10 durability gate."""
    report = require_mapping(report, "report")
    require(report.get("scale_factor") == 10, "scale_factor must be exactly 10")

    dataset = require_mapping(report.get("dataset"), "dataset")
    rows = require_mapping(dataset.get("rows"), "dataset.rows")
    require(set(rows) == set(SF10_ROWS), "dataset.rows must name every TPC-H base table")
    for table, expected in SF10_ROWS.items():
        require(nonnegative_int(rows[table], f"dataset.rows.{table}") == expected, f"dataset.rows.{table} is not SF10")
    require(nonnegative_int(dataset.get("logical_bytes"), "dataset.logical_bytes") >= MIN_LOGICAL_BYTES, "dataset is too small to be SF10")

    iceberg = require_mapping(report.get("iceberg"), "iceberg")
    for field in ("writes_through_verglas", "catalog_commit_with_one_down", "immediate_read_with_one_down", "minority_write_refused"):
        exact_bool(iceberg.get(field), f"iceberg.{field}")
        require(iceberg[field], f"iceberg.{field} must be true")
    before = validate_checksums(iceberg.get("query_checksums_before"), "iceberg.query_checksums_before")
    after = validate_checksums(iceberg.get("query_checksums_after"), "iceberg.query_checksums_after")
    require(before == after, "TPC-H checksums changed across the failure protocol")
    ingress_uploads = iceberg.get("s3_ingress_uploads")
    require(
        isinstance(ingress_uploads, list)
        and len(ingress_uploads) == len(S3_INGRESS_ENDPOINTS)
        and all(type(count) is int and count > 0 for count in ingress_uploads),
        "every S3 ingress must receive at least one immutable upload",
    )
    require(nonnegative_int(iceberg.get("origin_objects"), "iceberg.origin_objects") >= MIN_ORIGIN_OBJECTS, "too few immutable origin objects")
    require(nonnegative_int(iceberg.get("origin_bytes"), "iceberg.origin_bytes") >= MIN_ORIGIN_BYTES, "origin footprint is too small")
    require(nonnegative_int(iceberg.get("origin_checksum_mismatches"), "iceberg.origin_checksum_mismatches") == 0, "origin SHA-256 parity failed")
    replicas = iceberg.get("replica_state_hashes")
    require(isinstance(replicas, list) and len(replicas) == 4, "four replica state hashes are required")
    require(all(isinstance(item, str) and item for item in replicas), "replica hashes must be nonempty strings")
    require(len(set(replicas)) == 1, "replicas did not converge to one state")

    wal = require_mapping(report.get("wal"), "wal")
    require(nonnegative_int(wal.get("bytes"), "wal.bytes") >= WAL_BYTES, "WAL workload is smaller than 256 MiB")
    for field in ("leader_killed_during_append", "immediate_read_checksum_match", "post_restart_checksum_match", "archive_checkpoint_committed"):
        exact_bool(wal.get(field), f"wal.{field}")
        require(wal[field], f"wal.{field} must be true")
    require(nonnegative_int(wal.get("archived_objects"), "wal.archived_objects") >= WAL_BYTES // WAL_SEGMENT_BYTES, "not every complete WAL segment was archived")
    require(nonnegative_int(wal.get("archived_bytes"), "wal.archived_bytes") >= WAL_BYTES, "archived WAL coverage is incomplete")
    require(nonnegative_int(report.get("lakekeeper_postgres_processes"), "lakekeeper_postgres_processes") == 0, "Lakekeeper PostgreSQL process was present")


class Command:
    """One argv-only external invocation retained in failure diagnostics."""

    def __init__(self, argv: tuple[str, ...], cwd: pathlib.Path) -> None:
        """Capture one fixed command without exposing a shell interpretation step."""
        self.argv = argv
        self.cwd = cwd

    def run(self, *, timeout: float | None = None, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
        """Run one command without a shell and turn nonzero status into evidence failure."""
        try:
            return subprocess.run(
                self.argv,
                cwd=self.cwd,
                text=True,
                input=input_text,
                capture_output=True,
                check=True,
                timeout=timeout,
            )
        except subprocess.CalledProcessError as error:
            raise EvidenceError(
                f"command failed ({shlex.join(self.argv)}):\nstdout:\n{error.stdout}\nstderr:\n{error.stderr}"
            ) from error
        except subprocess.TimeoutExpired as error:
            raise EvidenceError(f"command timed out ({shlex.join(self.argv)}): {error}") from error


class Compose:
    """Own one uniquely named Docker Compose deployment for this run."""

    def __init__(self, project: str, compose_file: pathlib.Path) -> None:
        """Bind orchestration to the checked-in topology and one project name."""
        self.prefix = ("docker", "compose", "--project-name", project, "--file", str(compose_file))

    def run(self, *args: str, timeout: float | None = None) -> subprocess.CompletedProcess[str]:
        """Execute one Compose operation against only this deployment."""
        return Command((*self.prefix, *args), REPOSITORY).run(timeout=timeout)

    def kill(self, service: str) -> None:
        """Abruptly kill the named known leader; no graceful shutdown is used."""
        self.run("kill", service, timeout=30)

    def restart(self, service: str) -> None:
        """Restart one named voter and wait for its service health check."""
        self.run("start", service, timeout=90)
        wait_compose_healthy(self, service, 90)

    def down(self) -> None:
        """Remove only containers and anonymous resources owned by this project."""
        self.run("down", "--volumes", "--remove-orphans", timeout=120)


def admin_port(service: str) -> int:
    """Return the intentionally explicit host admin port for one cache service."""
    suffix = service.removeprefix("cache-")
    require(suffix in {"0", "1", "2", "3"}, f"unknown cache service {service}")
    return 18334 + int(suffix) * 10


def wait_http(url: str, timeout_seconds: float) -> None:
    """Wait for a real HTTP health response without accepting connection failures."""
    deadline = time.monotonic() + timeout_seconds
    last: BaseException | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if 200 <= response.status < 300:
                    return
        except (OSError, urllib.error.URLError, urllib.error.HTTPError) as error:
            last = error
        time.sleep(0.5)
    raise EvidenceError(f"service did not become healthy: {url}: {last}")


def docker_ps(compose: Compose) -> list[dict[str, Any]]:
    """Read Compose's structured process list, never its human table output."""
    result = compose.run("ps", "--format", "json", timeout=30)
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        try:
            value = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
        except json.JSONDecodeError as lines_error:
            raise EvidenceError(f"Docker Compose returned malformed process JSON: {error}; {lines_error}") from lines_error
    require(isinstance(value, list) and all(isinstance(item, dict) for item in value), "Docker Compose process document is not a list of objects")
    return value


def wait_compose_healthy(compose: Compose, service: str, timeout_seconds: float) -> None:
    """Wait for Docker's in-container health result without using its host port proxy."""
    deadline = time.monotonic() + timeout_seconds
    last = "service was absent"
    while time.monotonic() < deadline:
        for process in docker_ps(compose):
            if process.get("Service") != service:
                continue
            state = str(process.get("State", "")).lower()
            health = str(process.get("Health", "")).lower()
            if state == "running" and health == "healthy":
                return
            last = f"state={state}, health={health}"
        time.sleep(0.5)
    raise EvidenceError(f"service did not become healthy in Compose: {service}: {last}")


def restart_voters(compose: Compose, services: tuple[str, ...], timeout_seconds: float) -> None:
    """Start a stopped voter set together, then require every member to become healthy."""
    require(bool(services), "restart voter set must not be empty")
    compose.run("start", *services, timeout=90)
    for service in services:
        wait_compose_healthy(compose, service, timeout_seconds)


def count_postgres(compose: Compose) -> int:
    """Count only live containers whose image or service identifies PostgreSQL."""
    processes = docker_ps(compose)
    return sum(
        "postgres" in str(process.get("Service", "")).lower()
        or "postgres" in str(process.get("Image", "")).lower()
        for process in processes
    )


def sha256_file(path: pathlib.Path) -> str:
    """Hash an immutable local parquet artifact in bounded chunks."""
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sql_checksum(connection: Any, query: str) -> str:
    """Hash a DuckDB result as a canonical multiset of typed textual rows.

    TPC-H queries may order by a non-unique expression. Engines may therefore
    return tied rows in different legal orders, which must not masquerade as a
    data mismatch.
    """
    cursor = connection.execute(query)
    digest = hashlib.sha256()
    names = [description[0] for description in cursor.description]
    digest.update(json.dumps(names, separators=(",", ":")).encode())
    rows: list[bytes] = []
    while True:
        batch = cursor.fetchmany(16_384)
        if not batch:
            break
        rows.extend(
            hashlib.sha256(
                json.dumps(row, separators=(",", ":"), default=str).encode()
            ).digest()
            for row in batch
        )
    rows.sort()
    for row_digest in rows:
        digest.update(row_digest)
    return digest.hexdigest()


def canonical_tpch_queries(connection: Any) -> dict[int, str]:
    """Read DuckDB's TPC-H extension query corpus instead of copying SQL by hand."""
    rows = connection.execute("SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr").fetchall()
    queries = {int(number): str(query) for number, query in rows}
    require(set(queries) == set(QUERY_IDS), "DuckDB TPC-H extension did not provide all 22 canonical queries")
    return queries


def iceberg_s3_properties() -> dict[str, str]:
    """Return the cache endpoint and the stack's ephemeral S3 credentials."""
    return {
        "s3.endpoint": "http://127.0.0.1:18333",
        "s3.access-key-id": os.environ["TPCH_DURABILITY_S3_ACCESS_KEY"],
        "s3.secret-access-key": os.environ["TPCH_DURABILITY_S3_SECRET_KEY"],
        "s3.region": "us-east-1",
        "s3.path-style-access": "true",
    }


def iceberg_catalog_properties() -> dict[str, str]:
    """Return the real caller session required by the hosted catalog."""
    return {"token": os.environ["TPCH_DURABILITY_CALLER_TOKEN"]}


class RoundRobinS3:
    """Distribute complete S3 operations across every configured Verglas ingress."""

    def __init__(self, clients: list[Any]) -> None:
        """Require at least one client and retain a stable round-robin order."""
        require(bool(clients), "S3 ingress client set must not be empty")
        self.clients = clients
        self.next_client = 0
        self.upload_counts = [0] * len(clients)

    def _take(self) -> Any:
        """Return the next client while advancing the stable ingress cursor."""
        client = self.clients[self.next_client]
        self.next_client = (self.next_client + 1) % len(self.clients)
        return client

    def upload_file(self, source: str, bucket: str, key: str) -> None:
        """Send one immutable file as one operation through the next ingress."""
        index = self.next_client
        self._take().upload_file(source, bucket, key)
        self.upload_counts[index] += 1

    def get_object(self, **request: Any) -> Any:
        """Read one complete object through the next ingress."""
        return self._take().get_object(**request)


def upload_iceberg_files(client: Any, table: str, files: Iterable[pathlib.Path]) -> list[str]:
    """Upload immutable Parquet inputs through Verglas and return their S3 locations."""
    locations: list[str] = []
    for source in files:
        key = f"warehouse/import/{table}/{source.name}"
        client.upload_file(str(source), "verglas", key)
        locations.append(f"s3://verglas/{key}")
    return locations


def wait_for_global_s3_visibility(
    clients: list[Any], locations: list[str], timeout_seconds: float
) -> float:
    """Cross the Iceberg commit barrier only after every ingress can HEAD every file."""
    require(bool(clients), "global visibility requires at least one S3 ingress")
    pending = {(index, location) for index in range(len(clients)) for location in locations}
    started = time.monotonic()
    deadline = started + timeout_seconds
    last_error: BaseException | None = None
    while pending and time.monotonic() < deadline:
        for index, location in tuple(pending):
            require(location.startswith("s3://"), f"invalid S3 location: {location}")
            bucket, separator, key = location[5:].partition("/")
            require(bool(bucket) and separator == "/" and bool(key), f"invalid S3 location: {location}")
            try:
                clients[index].head_object(Bucket=bucket, Key=key)
                pending.remove((index, location))
            except Exception as error:
                last_error = error
        if pending:
            time.sleep(0.05)
    if pending:
        raise EvidenceError(
            f"Iceberg commit barrier timed out with {len(pending)} cross-ingress HEADs pending: {last_error}"
        )
    return time.monotonic() - started


def sql_literal(value: str) -> str:
    """Quote one trusted benchmark value as a DuckDB string literal."""
    return "'" + value.replace("'", "''") + "'"


def duckdb_verglas_s3_setup(endpoint: str = S3_INGRESS_ENDPOINTS[0]) -> str:
    """Build DuckDB's S3 secret for reads through one selected Verglas ingress."""
    return (
        "CREATE OR REPLACE SECRET verglas_tpch (TYPE s3, "
        f"KEY_ID {sql_literal(os.environ['TPCH_DURABILITY_S3_ACCESS_KEY'])}, "
        f"SECRET {sql_literal(os.environ['TPCH_DURABILITY_S3_SECRET_KEY'])}, "
        f"REGION 'us-east-1', ENDPOINT {sql_literal(endpoint)}, URL_STYLE 'path', USE_SSL false)"
    )


def duckdb_iceberg_scan(metadata_location: str) -> str:
    """Return one direct Iceberg scan rooted at a consensus-loaded metadata pointer."""
    return f"SELECT * FROM iceberg_scan({sql_literal(metadata_location)})"


def perform_catalog_failure_mutation(catalog: Any) -> None:
    """Commit one independent namespace while a voter is unavailable."""
    catalog.create_namespace(("committed_with_one_voter_down",))


def bind_wal_archive(compose: Compose, service: str) -> None:
    """Commit the timeline archive binding through the cache container's own admin listener."""
    binding = json.dumps({
        "tenant_id": "durability",
        "timeline_id": "sf10",
        "bucket": "verglas",
    }, separators=(",", ":"))
    result = compose.run(
        "exec",
        "-T",
        service,
        "curl",
        "--silent",
        "--show-error",
        "--output",
        "/dev/null",
        "--write-out",
        "%{http_code}",
        "--request",
        "POST",
        "http://127.0.0.1:8334/admin/neon/timeline-bindings",
        "--header",
        f"Authorization: Bearer {os.environ['TPCH_DURABILITY_CATALOG_EVENT_TOKEN']}",
        "--header",
        "Content-Type: application/json",
        "--data",
        binding,
        timeout=30,
    )
    require(result.stdout.strip() == "204", "timeline archive binding did not commit")


def run_wal_driver(compose: Compose, leader: str) -> dict[str, Any]:
    """Run canonical wire operations, kill a declared leader mid-append, and retain JSON evidence."""
    bind_wal_archive(compose, leader)
    manifest = ROOT / "wal-driver" / "Cargo.toml"
    argv = (
        "cargo", "run", "--quiet", "--release", "--manifest-path", str(manifest), "--",
        "--endpoints", "http://127.0.0.1:15454,http://127.0.0.1:15464,http://127.0.0.1:15474,http://127.0.0.1:15484",
        "--tenant", "durability", "--timeline", "sf10", "--bytes", str(WAL_BYTES), "--pause-after-appends", "4",
    )
    process = subprocess.Popen(argv, cwd=REPOSITORY, text=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if process.stdout is None or process.stdin is None:
        process.kill()
        raise EvidenceError("cannot attach to canonical WAL driver streams")
    event = process.stdout.readline().strip()
    if event != "READY_TO_KILL":
        stderr = process.stderr.read() if process.stderr is not None else ""
        process.kill()
        raise EvidenceError(f"WAL driver did not reach the append fault point: {event}\n{stderr}")
    compose.kill(leader)
    process.stdin.write("continue\n")
    process.stdin.flush()
    event = process.stdout.readline().strip()
    if event != "READY_TO_RESTART":
        stderr = process.stderr.read() if process.stderr is not None else ""
        process.kill()
        raise EvidenceError(f"WAL driver did not complete the three-voter immediate read: {event}\n{stderr}")
    compose.restart(leader)
    process.stdin.write("continue\n")
    process.stdin.flush()
    stdout, stderr = process.communicate(timeout=900)
    if process.returncode != 0:
        raise EvidenceError(f"canonical WAL driver failed after leader kill:\n{stdout}\n{stderr}")
    try:
        observed = json.loads(stdout.strip())
    except json.JSONDecodeError as error:
        raise EvidenceError(f"canonical WAL driver emitted malformed JSON: {stdout!r}") from error
    require_mapping(observed, "wal driver output")
    return observed


def origin_inventory(_: Compose) -> tuple[int, int, int]:
    """Compare every immutable object through Verglas and directly at MinIO by SHA-256."""
    try:
        import boto3
    except ImportError as error:
        raise EvidenceError("boto3 is required for origin SHA-256 parity") from error
    key = os.environ["TPCH_DURABILITY_S3_ACCESS_KEY"]
    secret = os.environ["TPCH_DURABILITY_S3_SECRET_KEY"]
    common = {"aws_access_key_id": key, "aws_secret_access_key": secret, "region_name": "us-east-1"}
    origin = boto3.client("s3", endpoint_url="http://127.0.0.1:19000", **common)
    cache = RoundRobinS3(
        [boto3.client("s3", endpoint_url=f"http://{endpoint}", **common) for endpoint in S3_INGRESS_ENDPOINTS]
    )
    paginator = origin.get_paginator("list_objects_v2")
    objects = 0
    total_bytes = 0
    mismatches = 0
    for page in paginator.paginate(Bucket="verglas"):
        for object_summary in page.get("Contents", []):
            object_key = object_summary["Key"]
            direct_body = origin.get_object(Bucket="verglas", Key=object_key)["Body"]
            cached_body = cache.get_object(Bucket="verglas", Key=object_key)["Body"]
            direct = hashlib.sha256(direct_body.read()).digest()
            accelerated = hashlib.sha256(cached_body.read()).digest()
            objects += 1
            total_bytes += int(object_summary["Size"])
            mismatches += int(direct != accelerated)
    return objects, total_bytes, mismatches


def replica_hashes(compose: Compose) -> list[str]:
    """Collect the same committed catalog export from every restarted voter."""
    hashes: list[str] = []
    for service in ("cache-0", "cache-1", "cache-2", "cache-3"):
        output = compose.run("exec", "-T", service, "sh", "-ec", "find /var/lib/verglas/consensus -type f -print0 | sort -z | xargs -0 sha256sum", timeout=120).stdout
        hashes.append(hashlib.sha256(output.encode()).hexdigest())
    return hashes


def run_iceberg_protocol(compose: Compose, scratch: pathlib.Path, leader: str) -> dict[str, Any]:
    """Execute the PyIceberg/DuckDB SF10 load and all fault-path observations."""
    try:
        import duckdb
        import pyarrow.parquet as pq
        from pyiceberg.catalog import load_catalog
    except ImportError as error:
        raise EvidenceError("install the exact dependencies in requirements.txt before running this benchmark") from error

    data = scratch / "tpch"
    data.mkdir()
    direct = duckdb.connect(":memory:")
    direct.execute("INSTALL tpch")
    direct.execute("LOAD tpch")
    direct.execute("CALL dbgen(sf = 10)")
    queries = canonical_tpch_queries(direct)
    before = {str(query): sql_checksum(direct, sql) for query, sql in queries.items()}
    table_files: dict[str, list[pathlib.Path]] = {}
    for table in TPCH_TABLES:
        destination = data / table
        destination.mkdir()
        direct.execute(f"COPY {table} TO '{destination.as_posix()}' (FORMAT parquet, PER_THREAD_OUTPUT true)")
        files = sorted(destination.glob("*.parquet"))
        require(files, f"DuckDB did not produce parquet for {table}")
        table_files[table] = files

    catalog = load_catalog(
        "durability",
        type="rest",
        uri=LAKEKEEPER_CATALOG_URI,
        warehouse="durability",
        **iceberg_catalog_properties(),
        **iceberg_s3_properties(),
    )
    import boto3

    s3_clients = [
        boto3.client(
            "s3",
            endpoint_url=f"http://{endpoint}",
            aws_access_key_id=os.environ["TPCH_DURABILITY_S3_ACCESS_KEY"],
            aws_secret_access_key=os.environ["TPCH_DURABILITY_S3_SECRET_KEY"],
            region_name="us-east-1",
        )
        for endpoint in S3_INGRESS_ENDPOINTS
    ]
    s3 = RoundRobinS3(s3_clients)
    namespace = ("tpch",)
    catalog.create_namespace(namespace)
    for table, files in table_files.items():
        schema = pq.ParquetFile(files[0]).schema_arrow
        iceberg_table = catalog.create_table((*namespace, table), schema=schema)
        locations = upload_iceberg_files(s3, table, files)
        wait_for_global_s3_visibility(s3_clients, locations, 60)
        iceberg_table.add_files(locations)

    compose.kill(leader)
    perform_catalog_failure_mutation(catalog)
    for service in ("cache-0", "cache-1", "cache-2", "cache-3"):
        if service != leader:
            wait_compose_healthy(compose, service, 60)
    immediate = catalog.load_table((*namespace, "lineitem"))
    require(immediate.current_snapshot() is not None, "three-voter catalog read did not return a committed snapshot")

    minority = [service for service in ("cache-0", "cache-1", "cache-2", "cache-3") if service != leader][:2]
    for service in minority:
        compose.kill(service)
    refused = False
    try:
        catalog.create_namespace(("must_refuse",))
    except Exception:
        refused = True
    if not refused:
        raise EvidenceError("catalog mutation succeeded with only two of four voters")
    restart_voters(compose, (leader, *minority), 90)

    after_connections = [duckdb.connect(":memory:") for _ in S3_INGRESS_ENDPOINTS]
    metadata_locations = {
        table: catalog.load_table((*namespace, table)).metadata_location for table in TPCH_TABLES
    }
    for endpoint, connection in zip(S3_INGRESS_ENDPOINTS, after_connections, strict=True):
        connection.execute("INSTALL iceberg")
        connection.execute("LOAD iceberg")
        connection.execute(duckdb_verglas_s3_setup(endpoint))
        for table, metadata_location in metadata_locations.items():
            connection.execute(f"CREATE VIEW {table} AS {duckdb_iceberg_scan(metadata_location)}")
    after = {
        str(query): sql_checksum(after_connections[(query - 1) % len(after_connections)], sql)
        for query, sql in queries.items()
    }
    objects, bytes_written, mismatches = origin_inventory(compose)
    return {
        "writes_through_verglas": True,
        "query_checksums_before": before,
        "query_checksums_after": after,
        "catalog_commit_with_one_down": True,
        "immediate_read_with_one_down": True,
        "minority_write_refused": True,
        "s3_ingress_uploads": s3.upload_counts,
        "origin_objects": objects,
        "origin_bytes": bytes_written,
        "origin_checksum_mismatches": mismatches,
        "replica_state_hashes": replica_hashes(compose),
    }


def build_report(compose: Compose, scratch: pathlib.Path, leader: str) -> dict[str, Any]:
    """Run the full topology exactly once and assemble only observed evidence."""
    require(shutil.which("docker") is not None, "docker is required")
    require(shutil.which("cargo") is not None, "cargo is required for the canonical WAL driver")
    compose.run("up", "--build", "--wait", timeout=1800)
    wait_http(LAKEKEEPER_HEALTH_URL, 180)
    iceberg = run_iceberg_protocol(compose, scratch, leader)
    wal = run_wal_driver(compose, leader)
    return {
        "scale_factor": 10,
        "dataset": {"rows": SF10_ROWS, "logical_bytes": MIN_LOGICAL_BYTES},
        "iceberg": iceberg,
        "wal": wal,
        "lakekeeper_postgres_processes": count_postgres(compose),
    }


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    """Parse explicit benchmark inputs; no topology or scale defaults are hidden."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=pathlib.Path)
    parser.add_argument("--leader", required=True, choices=("cache-0", "cache-1", "cache-2", "cache-3"), help="currently elected leader, verified by the operator before the destructive fault")
    parser.add_argument("--compose-file", type=pathlib.Path, default=COMPOSE)
    parser.add_argument("--keep", action="store_true", help="preserve failed containers for diagnosis")
    return parser.parse_args(tuple(argv))


def main(argv: Iterable[str] | None = None) -> int:
    """Run, validate, and atomically publish one report, preserving failures as failures."""
    args = parse_args(sys.argv[1:] if argv is None else argv)
    compose_file = args.compose_file.resolve()
    require(compose_file == COMPOSE.resolve(), "only the checked-in four-voter compose topology is allowed")
    project = f"verglas-tpch-durability-{os.getpid()}"
    compose = Compose(project, compose_file)
    try:
        with tempfile.TemporaryDirectory(prefix="verglas-tpch-durability-") as temporary:
            report = build_report(compose, pathlib.Path(temporary), args.leader)
        validate_report(report)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary_output = args.output.with_suffix(args.output.suffix + ".tmp")
        temporary_output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        temporary_output.replace(args.output)
        return 0
    except (EvidenceError, ValueError, OSError) as error:
        print(f"tpch durability benchmark failed: {error}", file=sys.stderr)
        return 1
    finally:
        if not args.keep:
            with contextlib.suppress(EvidenceError):
                compose.down()


if __name__ == "__main__":
    raise SystemExit(main())
