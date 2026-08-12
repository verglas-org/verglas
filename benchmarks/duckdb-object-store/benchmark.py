#!/usr/bin/env python3
"""Run Quack directly on object storage and through the Verglas S3 cache.

The coordinator uses Docker for every service and measured client.  Worker
subcommands live in this file too so the exact same image creates the Parquet
dataset, serves Quack, and executes the measured requests.
"""

from __future__ import annotations

import argparse
import configparser
import datetime
import hashlib
import json
import os
import pathlib
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.request


HERE = pathlib.Path(__file__).resolve().parent
IMAGE = "verglas-duckdb-object-store:1.5.5"
VERGLAS_IMAGE = os.environ.get("VERGLAS_BENCH_IMAGE", "verglas/verglas-server:local")
NETWORK = "verglas-duckdb-object-store-bench"
BUCKET = "verglas-benchmark"
PREFIX = "issue-126"
R2_BUCKET = "verglas-duckdb-bench-20260812"
ENGINE_MEMORY_BYTES = 256 * 1024 * 1024
ENGINE_MEMORY = "256MB"
ROWS = 40_000_000
QUACK_CONTAINER_MEMORY_BYTES = 2 * 1024**3
DEFAULT_CACHE_CAPACITY_BYTES = 256 * 1024**2
QUACK_TOKEN = "benchmark-only-token"
ORIGIN_KEY = "minio-benchmark"
ORIGIN_SECRET = "minio-benchmark-secret"
VERGLAS_KEY = "verglas-benchmark"
VERGLAS_SECRET = "verglas-benchmark-secret"
LEGS = (
    "direct",
    "quackstore_cold",
    "quackstore_warm",
    "quackstore_shared_warm",
    "verglas_cold",
    "verglas_warm",
    "verglas_shared_warm",
)
LEGACY_LEGS = ("direct", "verglas_cold", "verglas_warm", "verglas_shared_warm")
WORKLOADS = {
    "scan_aggregate": "SELECT count(*), sum(id), sum(metric), min(payload) FROM benchmark_data",
    "external_sort": (
        "SELECT count(*), sum(id), sum(sort_position) FROM "
        "(SELECT id, row_number() OVER (ORDER BY payload) sort_position FROM benchmark_data)"
    ),
    "spill_join": (
        "SELECT count(*), sum(a.metric + b.metric) FROM benchmark_data a "
        "JOIN benchmark_data b ON a.id = b.id"
    ),
}


def quackstore_settings(cache_path: str) -> dict[str, str | int | bool]:
    """Return the fixed persistent QuackStore configuration for one measured server."""
    return {
        "cache_path": cache_path,
        "cache_size": DEFAULT_CACHE_CAPACITY_BYTES,
        "data_mutable": False,
    }


def quackstore_location(bucket: str) -> str:
    """Return the immutable QuackStore URI for the benchmark's Parquet objects."""
    return f"quackstore://s3://{bucket}/{PREFIX}/data/*.parquet"


def run(command: list[str], *, capture: bool = True) -> subprocess.CompletedProcess[str]:
    """Run a command and fail immediately with its stderr intact."""
    result = subprocess.run(command, text=True, capture_output=capture)
    if result.returncode:
        detail = result.stderr if capture else "see command output above"
        raise RuntimeError(f"command failed ({result.returncode}): {' '.join(command)}\n{detail}")
    return result


def docker(*arguments: str, capture: bool = True) -> subprocess.CompletedProcess[str]:
    """Run Docker with the benchmark's explicit argument list."""
    return run(["docker", *arguments], capture=capture)


def r2_credentials(environment: dict[str, str]) -> dict[str, str]:
    """Return a complete R2 S3 credential triple without guessing token roles."""
    required = ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ACCOUNT_ID")
    missing = [name for name in required if not environment.get(name)]
    if missing:
        raise ValueError("R2 requires " + ", ".join(missing))
    return {name: environment[name] for name in required}


def r2_endpoint_settings(account_id: str) -> dict[str, str | bool]:
    """Return the TLS and region settings required by Cloudflare R2's S3 API."""
    endpoint = f"{account_id}.r2.cloudflarestorage.com"
    return {
        "endpoint": endpoint,
        "endpoint_url": f"https://{endpoint}",
        "region": "auto",
        "use_ssl": True,
    }


def validate_report(report: dict, *, require_repetitions: bool = True) -> None:
    """Reject a report unless it proves a durable, out-of-core comparison."""
    dataset = report["dataset"]
    if dataset["format"] != "parquet" or dataset["storage"] != "s3-compatible":
        raise ValueError("dataset must be Parquet on S3-compatible storage")
    if dataset["bytes"] <= 4 * dataset["worker_memory_bytes"]:
        raise ValueError("dataset must be larger than 4x the worker memory limit")
    if dataset["object_count"] < 1:
        raise ValueError("dataset must contain durable objects")

    quackstore_comparison = "quackstore" in report.get("cache_stats", {})
    required_services = {
        "verglas",
        "quack_direct",
        "quackstore_cached",
        "quackstore_shared",
        "verglas_cached",
        "verglas_shared",
    } if quackstore_comparison else {"verglas", "quack_direct", "quack_cached", "quack_shared"}
    services = report["runtime"]["services"]
    missing_services = required_services.difference(services)
    if missing_services:
        raise ValueError(
            "runtime service provenance is incomplete: "
            + ", ".join(sorted(missing_services))
        )
    for name in required_services:
        if not services[name].get("container_id") or not services[name].get("image_id"):
            raise ValueError(f"runtime provenance missing for {name}")
    local_origin = services.get("minio")
    external_origin = report["runtime"].get("external_origin")
    if not local_origin and not external_origin:
        raise ValueError("origin provenance is missing")
    if local_origin and (
        not local_origin.get("container_id") or not local_origin.get("image_id")
    ):
        raise ValueError("runtime provenance missing for minio")
    if external_origin and external_origin.get("provider") != "cloudflare-r2":
        raise ValueError("external origin provenance must identify Cloudflare R2")
    limits = report["runtime"]["limits"]
    for name in required_services.difference({"verglas"}):
        if name not in limits:
            raise ValueError(f"runtime limit missing for {name}")
        if limits[name].get("cpus") != 1.0:
            raise ValueError(f"{name} must run with one CPU")
        if limits[name].get("memory_bytes") != QUACK_CONTAINER_MEMORY_BYTES:
            raise ValueError(f"{name} must run with a 2 GiB container memory ceiling")

    traffic = report["object_store"]
    if traffic["request_count"] < 1 or len(traffic["request_log_sha256"]) != 64:
        raise ValueError("object-store request evidence is missing")
    cache_stats = report.get("cache_stats")
    if not cache_stats:
        raise ValueError("cache stats are missing")
    verglas_cache = cache_stats.get("verglas", cache_stats)
    quackstore_cache = cache_stats.get("quackstore", {})
    if quackstore_comparison:
        if verglas_cache.get("cache", {}).get("capacity_bytes") != DEFAULT_CACHE_CAPACITY_BYTES:
            raise ValueError("Verglas cache capacity must be exactly 256 MiB")
        if quackstore_cache.get("capacity_bytes") != DEFAULT_CACHE_CAPACITY_BYTES:
            raise ValueError("QuackStore cache capacity must be exactly 256 MiB")
        if verglas_cache.get("occupancy_bytes", 0) < 1 or quackstore_cache.get("occupancy_bytes", 0) < 1:
            raise ValueError("cache occupancy evidence is missing")
        if quackstore_cache.get("data_mutable") is not False:
            raise ValueError("QuackStore data must be explicitly immutable")
        if not quackstore_cache.get("cache_path"):
            raise ValueError("cache capacity evidence is missing")
        storage = cache_stats.get("storage", {})
        if not storage.get("verglas_device") or storage.get("verglas_device") != storage.get("quackstore_device"):
            raise ValueError("persistent cache storage must use the same external disk class")
    elif verglas_cache.get("cache", {}).get("capacity_bytes", 0) < 1:
        raise ValueError("cache capacity evidence is missing")
    if "backend_fills" not in verglas_cache.get("counters", {}):
        raise ValueError("cache counters are missing")
    if not report["spill"]["observed"] or report["spill"]["peak_bytes"] < 1:
        raise ValueError("spill was not observed")

    for workload, legs in report["workloads"].items():
        required_legs = LEGS if quackstore_comparison else LEGACY_LEGS
        if set(legs) != set(required_legs):
            raise ValueError(f"read legs missing for {workload}")
        digests = {legs[leg]["result_digest"] for leg in required_legs}
        if len(digests) != 1:
            raise ValueError(f"result mismatch for {workload}")

    repetitions = report.get("repetitions", [])
    if quackstore_comparison and require_repetitions and (
        len(repetitions) != 5 or {item.get("number") for item in repetitions} != set(range(1, 6))
    ):
        raise ValueError("report requires five independent repetitions")
    if quackstore_comparison and report.get("cache_resets") != {
        "quackstore": len(WORKLOADS),
        "verglas": len(WORKLOADS),
    }:
        raise ValueError("explicit cache reset evidence is missing")

    writes = report["durable_writes"]
    for leg in ("direct", "through_verglas"):
        if writes[leg]["origin_bytes"] < 1 or len(writes[leg]["readback_sha256"]) != 64:
            raise ValueError(f"durable origin readback missing for {leg}")


def duckdb_connection(
    endpoint: str,
    access_key: str,
    secret: str,
    bucket: str,
    *,
    register_data: bool = True,
    memory_limit: str = ENGINE_MEMORY,
    region: str = "us-east-1",
    use_ssl: bool = False,
    quackstore_cache_path: str | None = None,
):
    """Create a bounded DuckDB connection pointed at exactly one S3 endpoint."""
    import duckdb

    connection = duckdb.connect()
    connection.execute("INSTALL httpfs; LOAD httpfs")
    connection.execute(f"SET memory_limit='{memory_limit}'")
    connection.execute("SET threads=1")
    connection.execute("SET preserve_insertion_order=false")
    connection.execute("SET temp_directory='/spill'")
    connection.execute(f"SET s3_endpoint='{endpoint}'")
    connection.execute("SET s3_url_style='path'")
    connection.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'}")
    connection.execute(f"SET s3_region='{region}'")
    connection.execute(f"SET s3_access_key_id='{access_key}'")
    connection.execute(f"SET s3_secret_access_key='{secret}'")
    escaped_endpoint = endpoint.replace("'", "''")
    escaped_key = access_key.replace("'", "''")
    escaped_secret = secret.replace("'", "''")
    connection.execute(
        "CREATE OR REPLACE PERSISTENT SECRET benchmark_s3 ("
        "TYPE s3, PROVIDER config, "
        f"KEY_ID '{escaped_key}', SECRET '{escaped_secret}', REGION '{region}', "
        f"ENDPOINT '{escaped_endpoint}', URL_STYLE 'path', "
        f"USE_SSL {'true' if use_ssl else 'false'})"
    )
    if quackstore_cache_path:
        settings = quackstore_settings(quackstore_cache_path)
        connection.execute("INSTALL quackstore FROM community; LOAD quackstore")
        connection.execute(f"SET quackstore_cache_enabled = true")
        connection.execute(f"SET quackstore_cache_path = '{settings['cache_path']}'")
        connection.execute(f"SET quackstore_cache_size = {settings['cache_size']}")
        connection.execute("SET quackstore_data_mutable = false")
    if register_data:
        location = (
            quackstore_location(bucket)
            if quackstore_cache_path
            else f"s3://{bucket}/{PREFIX}/data/*.parquet"
        )
        connection.execute(
            f"CREATE VIEW benchmark_data AS SELECT id, metric, payload FROM read_parquet('{location}')"
        )
    return connection


def seed(
    endpoint: str,
    access_key: str,
    secret: str,
    bucket: str,
    rows: int,
    *,
    region: str = "us-east-1",
    use_ssl: bool = False,
) -> None:
    """Stream an incompressible partitioned Parquet dataset to the origin."""
    connection = duckdb_connection(
        endpoint,
        access_key,
        secret,
        bucket,
        register_data=False,
        memory_limit="512MB",
        region=region,
        use_ssl=use_ssl,
    )
    file_count = 4
    chunk_rows = (rows + file_count - 1) // file_count
    for part in range(file_count):
        start = part * chunk_rows
        stop = min(rows, start + chunk_rows)
        if start >= stop:
            break
        destination = f"s3://{bucket}/{PREFIX}/data/part-{part:02d}.parquet"
        connection.execute(
            f"COPY (SELECT i::BIGINT id, (i % 100000)::BIGINT metric, "
            f"md5(i::VARCHAR) || md5((i * 17)::VARCHAR) payload "
            f"FROM range({start}, {stop}) t(i)) TO '{destination}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
        )
    connection.close()


def serve_quack(
    endpoint: str,
    access_key: str,
    secret: str,
    bucket: str,
    *,
    region: str = "us-east-1",
    use_ssl: bool = False,
    quackstore_cache_path: str | None = None,
) -> None:
    """Expose one configured DuckDB instance through the real Quack extension."""
    connection = duckdb_connection(
        endpoint, access_key, secret, bucket, region=region, use_ssl=use_ssl,
        quackstore_cache_path=quackstore_cache_path,
    )
    connection.execute("INSTALL quack; LOAD quack")
    connection.execute(
        "CALL quack_serve('quack:0.0.0.0:9000', token = ?, "
        "allow_other_hostname = true, disable_ssl = true)",
        [QUACK_TOKEN],
    )
    while True:
        time.sleep(3600)


def query(host: str, sql: str) -> dict:
    """Execute one SQL statement over Quack and hash its materialized result."""
    import duckdb

    connection = duckdb.connect()
    connection.execute("INSTALL quack; LOAD quack")
    started = time.monotonic()
    rows = connection.execute(
        "SELECT * FROM quack_query(?, ?, token = ?, disable_ssl = true)",
        [f"quack:{host}:9000", sql, QUACK_TOKEN],
    ).fetchall()
    elapsed_ms = (time.monotonic() - started) * 1000
    encoded = json.dumps(rows, separators=(",", ":"), default=str).encode()
    return {
        "elapsed_ms": round(elapsed_ms, 3),
        "rows": len(rows),
        "result_digest": hashlib.sha256(encoded).hexdigest(),
    }


def write_probe(
    endpoint: str,
    access_key: str,
    secret: str,
    bucket: str,
    leg: str,
    *,
    region: str = "us-east-1",
    use_ssl: bool = False,
) -> None:
    """Write a Parquet object through one benchmark leg."""
    connection = duckdb_connection(
        endpoint,
        access_key,
        secret,
        bucket,
        register_data=False,
        memory_limit="256MB",
        region=region,
        use_ssl=use_ssl,
    )
    destination = f"s3://{bucket}/{PREFIX}/writes/{leg}.parquet"
    connection.execute(
        f"COPY (SELECT i::BIGINT id, md5(i::VARCHAR) payload FROM range(100000) t(i)) "
        f"TO '{destination}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    connection.close()


def origin_inventory(
    endpoint_url: str,
    access_key: str,
    secret: str,
    bucket: str,
    *,
    region: str = "us-east-1",
) -> dict:
    """List dataset objects and hash write probes read directly from the origin."""
    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret,
        region_name=region,
    )
    paginator = client.get_paginator("list_objects_v2")
    objects = [item for page in paginator.paginate(Bucket=bucket, Prefix=f"{PREFIX}/data/") for item in page.get("Contents", [])]
    writes = {}
    for leg in ("direct", "through_verglas"):
        key = f"{PREFIX}/writes/{leg}.parquet"
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        writes[leg] = {"origin_bytes": len(body), "readback_sha256": hashlib.sha256(body).hexdigest()}
    return {
        "dataset": {"bytes": sum(item["Size"] for item in objects), "object_count": len(objects)},
        "writes": writes,
    }


def cgroup(container: str) -> dict:
    """Read the CPU and memory ceilings applied to a running container."""
    inspection = json.loads(docker("inspect", container).stdout)[0]
    host = inspection["HostConfig"]
    return {"cpus": host["NanoCpus"] / 1_000_000_000, "memory_bytes": host["Memory"]}


def provenance(container: str) -> dict:
    """Record immutable container and image identities."""
    inspection = json.loads(docker("inspect", container).stdout)[0]
    return {"container_id": inspection["Id"], "image_id": inspection["Image"]}


def wait_http(url: str, timeout: float = 45.0) -> None:
    """Wait for an HTTP endpoint or raise with a bounded timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1):
                return
        except Exception:
            time.sleep(0.25)
    raise RuntimeError(f"endpoint did not become ready: {url}")


def fetch_json(url: str) -> dict:
    """Fetch one JSON evidence document from a benchmark service."""
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.load(response)


def r2_operation_evidence(
    api_token: str,
    account_id: str,
    bucket: str,
    started_at: str,
    ended_at: str,
) -> dict:
    """Query Cloudflare's R2 operations dataset for this benchmark window."""
    query_text = """
    query R2Ops($accountTag: string!, $startDate: Time, $endDate: Time, $bucketName: string) {
      viewer {
        accounts(filter: { accountTag: $accountTag }) {
          r2OperationsAdaptiveGroups(
            limit: 10000
            filter: {
              datetime_geq: $startDate
              datetime_leq: $endDate
              bucketName: $bucketName
            }
          ) {
            sum { requests }
            dimensions { actionType }
          }
        }
      }
    }
    """
    payload = json.dumps(
        {
            "query": query_text,
            "variables": {
                "accountTag": account_id,
                "startDate": started_at,
                "endDate": ended_at,
                "bucketName": bucket,
            },
        }
    ).encode()
    request = urllib.request.Request(
        "https://api.cloudflare.com/client/v4/graphql",
        data=payload,
        headers={
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        raw = response.read()
    document = json.loads(raw)
    if document.get("errors"):
        messages = "; ".join(error.get("message", "unknown") for error in document["errors"])
        raise RuntimeError(f"R2 analytics query failed: {messages}")
    accounts = document["data"]["viewer"]["accounts"]
    groups = accounts[0]["r2OperationsAdaptiveGroups"] if accounts else []
    requests = sum(group.get("sum", {}).get("requests", 0) for group in groups)
    return {
        "request_count": requests,
        "request_log_sha256": hashlib.sha256(raw).hexdigest(),
        "evidence_source": "cloudflare-r2-graphql",
        "window": {"started_at": started_at, "ended_at": ended_at},
        "operations": {
            group.get("dimensions", {}).get("actionType", "unknown"): group.get("sum", {}).get("requests", 0)
            for group in groups
        },
    }


def wait_container(name: str, needle: str, timeout: float = 45.0) -> None:
    """Wait until a service log contains its readiness marker."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = docker("logs", name)
        logs = result.stdout + result.stderr
        if needle in logs:
            return
        time.sleep(0.25)
    raise RuntimeError(f"container did not become ready: {name}")


def wait_quack(name: str, timeout: float = 45.0) -> None:
    """Prove Quack readiness with a real remote SQL round trip."""
    deadline = time.monotonic() + timeout
    last_error = ""
    while time.monotonic() < deadline:
        try:
            result = client(["query", "--host", name, "--sql", "SELECT 1"])
            if result["rows"] == 1:
                return
        except Exception as error:
            last_error = str(error)
        time.sleep(0.25)
    raise RuntimeError(f"Quack did not become ready: {name}: {last_error}")


def start_container(name: str, image: str, command: list[str], *, cpus: str, memory: str, extra: list[str] | None = None) -> str:
    """Start one named, bounded service on the isolated benchmark network."""
    arguments = ["run", "-d", "--rm", "--name", name, "--network", NETWORK, "--cpus", cpus, "--memory", memory]
    arguments.extend(extra or [])
    arguments.extend([image, *command])
    return docker(*arguments).stdout.strip()


def client(
    command: list[str], *, extra: list[str] | None = None, memory: str = "512m"
) -> dict:
    """Run one bounded benchmark worker and parse its sole JSON result."""
    arguments = [
        "run", "--rm", "--network", NETWORK, "--cpus", "1", "--memory", memory
    ]
    arguments.extend(extra or [])
    arguments.extend([IMAGE, "python", "/bench/benchmark.py", *command])
    output = docker(
        *arguments,
    ).stdout
    return json.loads(output)


def spill_directory_bytes(directory: pathlib.Path) -> int:
    """Return allocated bytes under one host-side DuckDB spill bind mount."""
    total = 0
    for path in directory.rglob("*"):
        try:
            if path.is_file():
                total += path.stat().st_blocks * 512
        except FileNotFoundError:
            # DuckDB removes partitions while the coordinator samples them.
            continue
    return total


def persistent_cache_bytes(directory: pathlib.Path) -> int:
    """Return the occupied bytes of one persistent cache on the external disk."""
    return spill_directory_bytes(directory)


def measured_query(spill_directory: pathlib.Path, host: str, sql: str) -> tuple[dict, int]:
    """Measure a Quack query while polling its host-side spill bind mount."""
    process = subprocess.Popen(
        ["docker", "run", "--rm", "--network", NETWORK, "--cpus", "1", "--memory", "512m", IMAGE,
         "python", "/bench/benchmark.py", "query", "--host", host, "--sql", sql],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    peak = 0
    while process.poll() is None:
        peak = max(peak, spill_directory_bytes(spill_directory))
        time.sleep(0.05)
    stdout, stderr = process.communicate()
    if process.returncode:
        raise RuntimeError(f"query failed for SQL {sql!r}: {stderr}")
    return json.loads(stdout), peak


def purge_cache() -> None:
    """Purge Verglas so every workload has an independently cold first leg."""
    request = urllib.request.Request("http://127.0.0.1:18434/cache/purge", method="POST")
    with urllib.request.urlopen(request, timeout=10) as response:
        if response.status >= 300:
            raise RuntimeError(f"cache purge failed: {response.status}")


def clear_quackstore_cache(host: str) -> None:
    """Clear QuackStore through its extension API before a measured cold leg."""
    client(["query", "--host", host, "--sql", "SELECT * FROM quackstore_clear_cache()"])


def local_smoke(output: pathlib.Path, rows: int, cache_capacity_bytes: int) -> None:
    """Run the complete MinIO-backed, cgroup-bounded comparison."""
    names = ["vg-bench-minio", "vg-bench-verglas", "vg-bench-direct", "vg-bench-cached", "vg-bench-shared", "vg-bench-trace"]
    external_root = pathlib.Path(
        os.environ.get("VERGLAS_BENCH_ROOT", "/Volumes/data_cache/verglas/rime-benchmarks")
    )
    scratch_parent = external_root if external_root.is_dir() else None
    scratch = pathlib.Path(
        tempfile.mkdtemp(prefix="verglas-duckdb-object-store-", dir=scratch_parent)
    )
    config = scratch / "verglas.toml"
    origin_creds = scratch / "origin-creds"
    endpoint_creds = scratch / "endpoint-creds"
    cache_dir = scratch / "cache"
    cache_dir.mkdir()
    origin_dir = scratch / "origin"
    origin_dir.mkdir()
    spill_dirs = {name: scratch / f"spill-{name}" for name in names[2:5]}
    for directory in spill_dirs.values():
        directory.mkdir()
    origin_creds.write_text(f"[default]\naws_access_key_id={ORIGIN_KEY}\naws_secret_access_key={ORIGIN_SECRET}\n")
    endpoint_creds.write_text(f"[default]\naws_access_key_id={VERGLAS_KEY}\naws_secret_access_key={VERGLAS_SECRET}\n")
    config.write_text(
        "[listen]\ns3_port=8333\nadmin_port=8334\n"
        f"[cache]\ndir='/cache'\ncapacity_bytes='{cache_capacity_bytes}B'\ndram_bytes='80MB'\n"
        "[auth]\ncredentials_file='/config/endpoint-creds'\n"
        f"[backend]\nprovider='s3'\nbucket='{BUCKET}'\nendpoint='http://vg-bench-minio:9000'\n"
        "region='us-east-1'\nallow_http=true\ncredentials_file='/config/origin-creds'\n"
    )
    containers: list[str] = []
    try:
        subprocess.run(["docker", "network", "rm", NETWORK], capture_output=True)
        docker("network", "create", NETWORK)
        containers.append(start_container(
            names[0], "minio/minio:latest", ["server", "/data", "--console-address", ":9001"], cpus="1", memory="512m",
            extra=["-e", f"MINIO_ROOT_USER={ORIGIN_KEY}", "-e", f"MINIO_ROOT_PASSWORD={ORIGIN_SECRET}", "-v", f"{origin_dir}:/data"],
        ))
        wait_container(names[0], "API:")
        docker("run", "--rm", "--network", NETWORK, "--entrypoint", "sh", "minio/mc:latest", "-c",
               f"mc alias set local http://{names[0]}:9000 {ORIGIN_KEY} {ORIGIN_SECRET} && mc mb --ignore-existing local/{BUCKET}")
        containers.append(start_container(
            names[5], "minio/mc:latest", ["-c", f"mc alias set local http://{names[0]}:9000 {ORIGIN_KEY} {ORIGIN_SECRET} >/dev/null && mc admin trace --json local"],
            cpus="0.25", memory="128m", extra=["--entrypoint", "sh"],
        ))
        docker("build", "-t", IMAGE, "-f", str(HERE / "Dockerfile"), str(HERE.parents[1]), capture=False)
        client(["seed", "--endpoint", f"{names[0]}:9000", "--access-key", ORIGIN_KEY, "--secret", ORIGIN_SECRET, "--bucket", BUCKET, "--rows", str(rows)])
        containers.append(start_container(
            names[1], VERGLAS_IMAGE, ["--config", "/config/verglas.toml"], cpus="1", memory="256m",
            extra=["--entrypoint", "verglas-server", "-p", "18434:8334", "-e", "VERGLAS_ADMIN_ADDR=0.0.0.0:8334", "-v", f"{scratch}:/config:ro", "-v", f"{cache_dir}:/cache"],
        ))
        wait_http("http://127.0.0.1:18434/admin/healthz")
        server_specs = (
            (names[2], f"{names[0]}:9000", ORIGIN_KEY, ORIGIN_SECRET),
            (names[3], f"{names[1]}:8333", VERGLAS_KEY, VERGLAS_SECRET),
            (names[4], f"{names[1]}:8333", VERGLAS_KEY, VERGLAS_SECRET),
        )
        for name, endpoint, key, secret in server_specs:
            containers.append(start_container(
                name, IMAGE, ["python", "/bench/benchmark.py", "serve", "--endpoint", endpoint, "--access-key", key, "--secret", secret, "--bucket", BUCKET],
                cpus="1", memory="2g", extra=["-v", f"{spill_dirs[name]}:/spill"],
            ))
            wait_quack(name)

        workloads = {}
        peak_spill = 0
        for workload, sql in WORKLOADS.items():
            purge_cache()
            direct, spill = measured_query(spill_dirs[names[2]], names[2], sql)
            cold, cold_spill = measured_query(spill_dirs[names[3]], names[3], sql)
            warm, warm_spill = measured_query(spill_dirs[names[3]], names[3], sql)
            shared, shared_spill = measured_query(spill_dirs[names[4]], names[4], sql)
            peak_spill = max(peak_spill, spill, cold_spill, warm_spill, shared_spill)
            workloads[workload] = dict(zip(LEGS, (direct, cold, warm, shared)))

        client(["write", "--endpoint", f"{names[0]}:9000", "--access-key", ORIGIN_KEY, "--secret", ORIGIN_SECRET, "--bucket", BUCKET, "--leg", "direct"])
        client(["write", "--endpoint", f"{names[1]}:8333", "--access-key", VERGLAS_KEY, "--secret", VERGLAS_SECRET, "--bucket", BUCKET, "--leg", "through_verglas"])
        inventory = client(["inventory", "--endpoint-url", f"http://{names[0]}:9000", "--access-key", ORIGIN_KEY, "--secret", ORIGIN_SECRET, "--bucket", BUCKET])
        trace_result = docker("logs", names[5])
        trace = trace_result.stdout + trace_result.stderr
        request_lines = [line for line in trace.splitlines() if line.strip()]
        services = {
            "minio": provenance(names[0]), "verglas": provenance(names[1]),
            "quack_direct": provenance(names[2]), "quack_cached": provenance(names[3]),
            "quack_shared": provenance(names[4]),
        }
        report = {
            "comparison_scope": "same DuckDB 1.5.5 + Quack engine; direct object store versus Verglas S3 cache",
            "dataset": {"format": "parquet", "storage": "s3-compatible", "worker_memory_bytes": ENGINE_MEMORY_BYTES, **inventory["dataset"]},
            "runtime": {"services": services, "limits": {
                "quack_direct": cgroup(names[2]), "quack_cached": cgroup(names[3]),
                "quack_shared": cgroup(names[4]), "verglas": cgroup(names[1])}},
            "object_store": {"request_count": len(request_lines), "request_log_sha256": hashlib.sha256(trace.encode()).hexdigest()},
            "cache_stats": fetch_json("http://127.0.0.1:18434/admin/stats"),
            "workloads": workloads,
            "spill": {"observed": peak_spill > 0, "peak_bytes": peak_spill},
            "durable_writes": inventory["writes"],
        }
        validate_report(report)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n")
    finally:
        for name in reversed(names):
            subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        subprocess.run(["docker", "network", "rm", NETWORK], capture_output=True)
        shutil.rmtree(scratch, ignore_errors=True)


def live_r2_once(output: pathlib.Path, rows: int, cache_capacity_bytes: int) -> None:
    """Run the complete comparison against the isolated durable R2 bucket."""
    if cache_capacity_bytes != DEFAULT_CACHE_CAPACITY_BYTES:
        raise ValueError("the live R2 comparison requires an equal 256 MiB logical cache budget")
    credentials = r2_credentials(os.environ)
    api_token = os.environ.get("CLOUDFLARE_API_TOKEN")
    if not api_token:
        raise ValueError("live R2 evidence requires CLOUDFLARE_API_TOKEN")
    account_id = credentials["R2_ACCOUNT_ID"]
    settings = r2_endpoint_settings(account_id)
    names = [
        "vg-bench-verglas", "vg-bench-direct", "vg-bench-quackstore",
        "vg-bench-quackstore-shared", "vg-bench-verglas-cached",
        "vg-bench-verglas-shared",
    ]
    external_root = pathlib.Path(
        os.environ.get("VERGLAS_BENCH_ROOT", "/Volumes/data_cache/verglas/rime-benchmarks")
    )
    scratch_parent = external_root if external_root.is_dir() else None
    scratch = pathlib.Path(tempfile.mkdtemp(prefix="verglas-duckdb-r2-", dir=scratch_parent))
    config = scratch / "verglas.toml"
    origin_creds = scratch / "origin-creds"
    endpoint_creds = scratch / "endpoint-creds"
    cache_dir = scratch / "cache"
    cache_dir.mkdir()
    quackstore_cache_dir = scratch / "quackstore-cache"
    quackstore_cache_dir.mkdir()
    spill_dirs = {name: scratch / f"spill-{name}" for name in names[1:]}
    for directory in spill_dirs.values():
        directory.mkdir()
    origin_creds.write_text(
        "[default]\n"
        f"aws_access_key_id={credentials['R2_ACCESS_KEY_ID']}\n"
        f"aws_secret_access_key={credentials['R2_SECRET_ACCESS_KEY']}\n"
    )
    endpoint_creds.write_text(
        f"[default]\naws_access_key_id={VERGLAS_KEY}\naws_secret_access_key={VERGLAS_SECRET}\n"
    )
    origin_creds.chmod(0o600)
    endpoint_creds.chmod(0o600)
    config.write_text(
        "[listen]\ns3_port=8333\nadmin_port=8334\n"
        f"[cache]\ndir='/cache'\ncapacity_bytes='{cache_capacity_bytes}B'\ndram_bytes='80MB'\n"
        "[auth]\ncredentials_file='/config/endpoint-creds'\n"
        f"[backend]\nprovider='s3'\nbucket='{R2_BUCKET}'\n"
        f"endpoint='{settings['endpoint_url']}'\nregion='auto'\n"
        "credentials_file='/config/origin-creds'\n"
    )
    worker_origin_mount = ["-v", f"{origin_creds}:/run/secrets/origin:ro"]
    worker_endpoint_mount = ["-v", f"{endpoint_creds}:/run/secrets/endpoint:ro"]
    started_at = datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z")
    try:
        subprocess.run(["docker", "network", "rm", NETWORK], capture_output=True)
        docker("network", "create", NETWORK)
        docker("build", "-t", IMAGE, "-f", str(HERE / "Dockerfile"), str(HERE.parents[1]), capture=False)
        client(
            [
                "seed", "--endpoint", str(settings["endpoint"]),
                "--credentials-file", "/run/secrets/origin", "--bucket", R2_BUCKET,
                "--rows", str(rows), "--region", "auto", "--use-ssl",
            ],
            extra=worker_origin_mount,
            memory="1g",
        )
        start_container(
            names[0], VERGLAS_IMAGE, ["--config", "/config/verglas.toml"], cpus="1", memory="256m",
            extra=[
                "--entrypoint", "verglas-server", "-p", "18434:8334",
                "-e", "VERGLAS_ADMIN_ADDR=0.0.0.0:8334", "-v", f"{scratch}:/config:ro",
                "-v", f"{cache_dir}:/cache",
            ],
        )
        wait_http("http://127.0.0.1:18434/admin/healthz")
        server_specs = (
            (names[1], str(settings["endpoint"]), "/run/secrets/origin", "auto", True, worker_origin_mount, None),
            (names[2], str(settings["endpoint"]), "/run/secrets/origin", "auto", True, worker_origin_mount, "/quackstore-cache"),
            (names[4], f"{names[0]}:8333", "/run/secrets/endpoint", "us-east-1", False, worker_endpoint_mount, None),
        )
        service_evidence = {"verglas": provenance(names[0])}
        service_limits = {"verglas": cgroup(names[0])}
        for name, endpoint, credentials_file, region, use_ssl, mounts, quackstore_path in server_specs:
            command = [
                "python", "/bench/benchmark.py", "serve", "--endpoint", endpoint,
                "--credentials-file", credentials_file, "--bucket", R2_BUCKET,
                "--region", region,
            ]
            if use_ssl:
                command.append("--use-ssl")
            if quackstore_path:
                command.extend(["--quackstore-cache-path", quackstore_path])
            start_container(
                name, IMAGE, command, cpus="1", memory="2g",
                extra=[*mounts, "-v", f"{spill_dirs[name]}:/spill", *(
                    ["-v", f"{quackstore_cache_dir}:/quackstore-cache"] if quackstore_path else []
                )],
            )
            wait_quack(name)
            role = {names[1]: "quack_direct", names[2]: "quackstore_cached", names[4]: "verglas_cached"}[name]
            service_evidence[role] = provenance(name)
            service_limits[role] = cgroup(name)

        workloads = {}
        peak_spill = 0
        for workload, sql in WORKLOADS.items():
            purge_cache()
            clear_quackstore_cache(names[2])
            direct, direct_spill = measured_query(spill_dirs[names[1]], names[1], sql)
            quackstore_cold, quackstore_cold_spill = measured_query(spill_dirs[names[2]], names[2], sql)
            quackstore_warm, quackstore_warm_spill = measured_query(spill_dirs[names[2]], names[2], sql)
            start_container(
                names[3], IMAGE,
                ["python", "/bench/benchmark.py", "serve", "--endpoint", str(settings["endpoint"]),
                 "--credentials-file", "/run/secrets/origin", "--bucket", R2_BUCKET,
                 "--region", "auto", "--use-ssl", "--quackstore-cache-path", "/quackstore-cache"],
                cpus="1", memory="2g", extra=[*worker_origin_mount, "-v", f"{spill_dirs[names[3]]}:/spill", "-v", f"{quackstore_cache_dir}:/quackstore-cache"],
            )
            wait_quack(names[3])
            service_evidence["quackstore_shared"] = provenance(names[3])
            service_limits["quackstore_shared"] = cgroup(names[3])
            quackstore_shared, quackstore_shared_spill = measured_query(spill_dirs[names[3]], names[3], sql)
            docker("rm", "-f", names[3])
            verglas_cold, verglas_cold_spill = measured_query(spill_dirs[names[4]], names[4], sql)
            verglas_warm, verglas_warm_spill = measured_query(spill_dirs[names[4]], names[4], sql)
            start_container(
                names[5], IMAGE,
                ["python", "/bench/benchmark.py", "serve", "--endpoint", f"{names[0]}:8333",
                 "--credentials-file", "/run/secrets/endpoint", "--bucket", R2_BUCKET],
                cpus="1", memory="2g", extra=[*worker_endpoint_mount, "-v", f"{spill_dirs[names[5]]}:/spill"],
            )
            wait_quack(names[5])
            service_evidence["verglas_shared"] = provenance(names[5])
            service_limits["verglas_shared"] = cgroup(names[5])
            verglas_shared, verglas_shared_spill = measured_query(spill_dirs[names[5]], names[5], sql)
            docker("rm", "-f", names[5])
            peak_spill = max(
                peak_spill, direct_spill, quackstore_cold_spill, quackstore_warm_spill,
                quackstore_shared_spill, verglas_cold_spill, verglas_warm_spill,
                verglas_shared_spill,
            )
            workloads[workload] = dict(zip(LEGS, (
                direct, quackstore_cold, quackstore_warm, quackstore_shared,
                verglas_cold, verglas_warm, verglas_shared,
            )))

        client(
            [
                "write", "--endpoint", str(settings["endpoint"]),
                "--credentials-file", "/run/secrets/origin", "--bucket", R2_BUCKET,
                "--leg", "direct", "--region", "auto", "--use-ssl",
            ],
            extra=worker_origin_mount,
        )
        client(
            [
                "write", "--endpoint", f"{names[0]}:8333",
                "--credentials-file", "/run/secrets/endpoint", "--bucket", R2_BUCKET,
                "--leg", "through_verglas",
            ],
            extra=worker_endpoint_mount,
        )
        inventory = client(
            [
                "inventory", "--endpoint-url", str(settings["endpoint_url"]),
                "--credentials-file", "/run/secrets/origin", "--bucket", R2_BUCKET,
                "--region", "auto",
            ],
            extra=worker_origin_mount,
        )
        ended_at = datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z")
        object_store = r2_operation_evidence(
            api_token, account_id, R2_BUCKET, started_at, ended_at
        )
        verglas_stats = fetch_json("http://127.0.0.1:18434/admin/stats")
        verglas_stats["occupancy_bytes"] = persistent_cache_bytes(cache_dir)
        report = {
            "comparison_scope": "same DuckDB 1.5.5 + Quack engine; direct R2, QuackStore, and Verglas persistent caches",
            "dataset": {
                "format": "parquet", "storage": "s3-compatible",
                "worker_memory_bytes": ENGINE_MEMORY_BYTES, **inventory["dataset"],
            },
            "runtime": {
                "services": service_evidence,
                "external_origin": {
                    "provider": "cloudflare-r2", "bucket": R2_BUCKET,
                    "endpoint": settings["endpoint"],
                },
                "limits": service_limits,
            },
            "object_store": object_store,
            "cache_stats": {
                "verglas": verglas_stats,
                "quackstore": {
                    **quackstore_settings(str(quackstore_cache_dir)),
                    "capacity_bytes": DEFAULT_CACHE_CAPACITY_BYTES,
                    "occupancy_bytes": persistent_cache_bytes(quackstore_cache_dir),
                },
                "storage": {
                    "verglas_device": str(cache_dir.stat().st_dev),
                    "quackstore_device": str(quackstore_cache_dir.stat().st_dev),
                },
            },
            "workloads": workloads,
            "spill": {"observed": peak_spill > 0, "peak_bytes": peak_spill},
            "durable_writes": inventory["writes"],
            "cache_resets": {"quackstore": len(WORKLOADS), "verglas": len(WORKLOADS)},
        }
        validate_report(report, require_repetitions=False)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n")
    finally:
        for name in reversed(names):
            subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        subprocess.run(["docker", "network", "rm", NETWORK], capture_output=True)
        shutil.rmtree(scratch, ignore_errors=True)


def live_r2(output: pathlib.Path, rows: int, cache_capacity_bytes: int) -> None:
    """Run five isolated R2 repetitions and publish their independently valid evidence."""
    output.parent.mkdir(parents=True, exist_ok=True)
    reports = []
    with tempfile.TemporaryDirectory(prefix="verglas-duckdb-r2-reports-", dir=output.parent) as directory:
        report_directory = pathlib.Path(directory)
        for number in range(1, 6):
            path = report_directory / f"repetition-{number}.json"
            live_r2_once(path, rows, cache_capacity_bytes)
            report = json.loads(path.read_text())
            validate_report(report, require_repetitions=False)
            reports.append(report)
    final_report = reports[-1]
    final_report["repetitions"] = [
        {
            "number": number,
            "report_sha256": hashlib.sha256(
                json.dumps(report, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
            "workloads": report["workloads"],
        }
        for number, report in enumerate(reports, start=1)
    ]
    validate_report(final_report)
    output.write_text(json.dumps(final_report, indent=2) + "\n")


def file_credentials(path: str | None, access_key: str | None, secret: str | None) -> tuple[str, str]:
    """Resolve worker credentials from a mounted AWS file or explicit local-test arguments."""
    if path:
        parser = configparser.ConfigParser()
        if not parser.read(path) or "default" not in parser:
            raise ValueError(f"credentials file has no default profile: {path}")
        return (
            parser["default"]["aws_access_key_id"],
            parser["default"]["aws_secret_access_key"],
        )
    if not access_key or not secret:
        raise ValueError("worker requires --credentials-file or both --access-key and --secret")
    return access_key, secret


def main() -> None:
    """Dispatch coordinator and container worker commands."""
    parser = argparse.ArgumentParser()
    parser.add_argument("command", nargs="?", choices=("local-smoke", "seed", "serve", "query", "write", "inventory"))
    parser.add_argument("--profile", choices=("local-smoke", "live-r2"))
    parser.add_argument("--output", type=pathlib.Path, default=HERE / "result.json")
    parser.add_argument("--endpoint")
    parser.add_argument("--endpoint-url")
    parser.add_argument("--access-key")
    parser.add_argument("--secret")
    parser.add_argument("--credentials-file")
    parser.add_argument("--bucket", default=BUCKET)
    parser.add_argument("--rows", type=int, default=ROWS)
    parser.add_argument("--cache-capacity-bytes", type=int, default=DEFAULT_CACHE_CAPACITY_BYTES)
    parser.add_argument("--host")
    parser.add_argument("--sql")
    parser.add_argument("--leg")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--use-ssl", action="store_true")
    parser.add_argument("--quackstore-cache-path")
    args = parser.parse_args()
    if args.profile:
        if args.command:
            parser.error("choose either a command or --profile")
        args.command = args.profile
    if not args.command:
        parser.error("a command or --profile is required")
    if args.command == "local-smoke":
        local_smoke(args.output, args.rows, args.cache_capacity_bytes)
    elif args.command == "live-r2":
        live_r2(args.output, args.rows, args.cache_capacity_bytes)
    elif args.command == "seed":
        access_key, secret = file_credentials(args.credentials_file, args.access_key, args.secret)
        seed(
            args.endpoint, access_key, secret, args.bucket, args.rows,
            region=args.region, use_ssl=args.use_ssl,
        )
        print("{}")
    elif args.command == "serve":
        access_key, secret = file_credentials(args.credentials_file, args.access_key, args.secret)
        serve_quack(
            args.endpoint, access_key, secret, args.bucket,
            region=args.region, use_ssl=args.use_ssl,
            quackstore_cache_path=args.quackstore_cache_path,
        )
    elif args.command == "query":
        print(json.dumps(query(args.host, args.sql)))
    elif args.command == "write":
        access_key, secret = file_credentials(args.credentials_file, args.access_key, args.secret)
        write_probe(
            args.endpoint, access_key, secret, args.bucket, args.leg,
            region=args.region, use_ssl=args.use_ssl,
        )
        print("{}")
    else:
        access_key, secret = file_credentials(args.credentials_file, args.access_key, args.secret)
        print(
            json.dumps(
                origin_inventory(
                    args.endpoint_url, access_key, secret, args.bucket, region=args.region
                )
            )
        )


if __name__ == "__main__":
    main()
