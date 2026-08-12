#!/usr/bin/env python3
"""Run Quack directly on object storage and through the Verglas S3 cache.

The coordinator uses Docker for every service and measured client.  Worker
subcommands live in this file too so the exact same image creates the Parquet
dataset, serves Quack, and executes the measured requests.
"""

from __future__ import annotations

import argparse
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
ENGINE_MEMORY_BYTES = 256 * 1024 * 1024
ENGINE_MEMORY = "256MB"
ROWS = 20_000_000
QUACK_TOKEN = "benchmark-only-token"
ORIGIN_KEY = "minio-benchmark"
ORIGIN_SECRET = "minio-benchmark-secret"
VERGLAS_KEY = "verglas-benchmark"
VERGLAS_SECRET = "verglas-benchmark-secret"
LEGS = ("direct", "verglas_cold", "verglas_warm", "verglas_shared_warm")
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


def validate_report(report: dict) -> None:
    """Reject a report unless it proves a durable, out-of-core comparison."""
    dataset = report["dataset"]
    if dataset["format"] != "parquet" or dataset["storage"] != "s3-compatible":
        raise ValueError("dataset must be Parquet on S3-compatible storage")
    if dataset["bytes"] <= 4 * dataset["worker_memory_bytes"]:
        raise ValueError("dataset must be larger than 4x the worker memory limit")
    if dataset["object_count"] < 1:
        raise ValueError("dataset must contain durable objects")

    required_services = {"minio", "verglas", "quack_direct", "quack_cached"}
    services = report["runtime"]["services"]
    if not required_services.issubset(services):
        raise ValueError("runtime service provenance is incomplete")
    for name in required_services:
        if not services[name].get("container_id") or not services[name].get("image_id"):
            raise ValueError(f"runtime provenance missing for {name}")

    traffic = report["object_store"]
    if traffic["request_count"] < 1 or len(traffic["request_log_sha256"]) != 64:
        raise ValueError("object-store request evidence is missing")
    if not report["spill"]["observed"] or report["spill"]["peak_bytes"] < 1:
        raise ValueError("spill was not observed")

    for workload, legs in report["workloads"].items():
        if set(legs) != set(LEGS):
            raise ValueError(f"read legs missing for {workload}")
        digests = {legs[leg]["result_digest"] for leg in LEGS}
        if len(digests) != 1:
            raise ValueError(f"result mismatch for {workload}")

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
    connection.execute("SET s3_use_ssl=false")
    connection.execute("SET s3_region='us-east-1'")
    connection.execute(f"SET s3_access_key_id='{access_key}'")
    connection.execute(f"SET s3_secret_access_key='{secret}'")
    escaped_endpoint = endpoint.replace("'", "''")
    escaped_key = access_key.replace("'", "''")
    escaped_secret = secret.replace("'", "''")
    connection.execute(
        "CREATE OR REPLACE PERSISTENT SECRET benchmark_s3 ("
        "TYPE s3, PROVIDER config, "
        f"KEY_ID '{escaped_key}', SECRET '{escaped_secret}', REGION 'us-east-1', "
        f"ENDPOINT '{escaped_endpoint}', URL_STYLE 'path', USE_SSL false)"
    )
    if register_data:
        location = f"s3://{bucket}/{PREFIX}/data/*.parquet"
        connection.execute(
            f"CREATE VIEW benchmark_data AS SELECT id, metric, payload FROM read_parquet('{location}')"
        )
    return connection


def seed(endpoint: str, access_key: str, secret: str, bucket: str, rows: int) -> None:
    """Stream an incompressible partitioned Parquet dataset to the origin."""
    connection = duckdb_connection(
        endpoint, access_key, secret, bucket, register_data=False, memory_limit="256MB"
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


def serve_quack(endpoint: str, access_key: str, secret: str, bucket: str) -> None:
    """Expose one configured DuckDB instance through the real Quack extension."""
    connection = duckdb_connection(endpoint, access_key, secret, bucket)
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


def write_probe(endpoint: str, access_key: str, secret: str, bucket: str, leg: str) -> None:
    """Write a Parquet object through one benchmark leg."""
    connection = duckdb_connection(
        endpoint, access_key, secret, bucket, register_data=False, memory_limit="256MB"
    )
    destination = f"s3://{bucket}/{PREFIX}/writes/{leg}.parquet"
    connection.execute(
        f"COPY (SELECT i::BIGINT id, md5(i::VARCHAR) payload FROM range(100000) t(i)) "
        f"TO '{destination}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    connection.close()


def origin_inventory(endpoint_url: str, access_key: str, secret: str, bucket: str) -> dict:
    """List dataset objects and hash write probes read directly from the origin."""
    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret,
        region_name="us-east-1",
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


def client(command: list[str]) -> dict:
    """Run one bounded benchmark worker and parse its sole JSON result."""
    output = docker(
        "run", "--rm", "--network", NETWORK, "--cpus", "1", "--memory", "512m",
        IMAGE, "python", "/bench/benchmark.py", *command,
    ).stdout
    return json.loads(output)


def spill_bytes(container: str) -> int:
    """Return current DuckDB temporary-file bytes without trusting host mounts."""
    output = docker("exec", container, "sh", "-c", "du -sk /spill 2>/dev/null | cut -f1 || true").stdout.strip()
    return int(output or "0") * 1024


def measured_query(container: str, host: str, sql: str) -> tuple[dict, int]:
    """Measure a Quack query while polling its server-side spill directory."""
    process = subprocess.Popen(
        ["docker", "run", "--rm", "--network", NETWORK, "--cpus", "1", "--memory", "512m", IMAGE,
         "python", "/bench/benchmark.py", "query", "--host", host, "--sql", sql],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    peak = 0
    while process.poll() is None:
        peak = max(peak, spill_bytes(container))
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


def local_smoke(output: pathlib.Path, rows: int) -> None:
    """Run the complete MinIO-backed, cgroup-bounded comparison."""
    names = ["vg-bench-minio", "vg-bench-verglas", "vg-bench-direct", "vg-bench-cached", "vg-bench-shared", "vg-bench-trace"]
    scratch = pathlib.Path(tempfile.mkdtemp(prefix="verglas-duckdb-object-store-"))
    config = scratch / "verglas.toml"
    origin_creds = scratch / "origin-creds"
    endpoint_creds = scratch / "endpoint-creds"
    cache_dir = scratch / "cache"
    cache_dir.mkdir()
    spill_dirs = {name: scratch / f"spill-{name}" for name in names[2:5]}
    for directory in spill_dirs.values():
        directory.mkdir()
    origin_creds.write_text(f"[default]\naws_access_key_id={ORIGIN_KEY}\naws_secret_access_key={ORIGIN_SECRET}\n")
    endpoint_creds.write_text(f"[default]\naws_access_key_id={VERGLAS_KEY}\naws_secret_access_key={VERGLAS_SECRET}\n")
    config.write_text(
        "[listen]\ns3_port=8333\nadmin_port=8334\n"
        "[cache]\ndir='/cache'\ncapacity_bytes='256MB'\ndram_bytes='80MB'\n"
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
            extra=["-e", f"MINIO_ROOT_USER={ORIGIN_KEY}", "-e", f"MINIO_ROOT_PASSWORD={ORIGIN_SECRET}"],
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
                cpus="1", memory="768m", extra=["-v", f"{spill_dirs[name]}:/spill"],
            ))
            wait_quack(name)

        workloads = {}
        peak_spill = 0
        for workload, sql in WORKLOADS.items():
            purge_cache()
            direct, spill = measured_query(names[2], names[2], sql)
            cold, cold_spill = measured_query(names[3], names[3], sql)
            warm, warm_spill = measured_query(names[3], names[3], sql)
            shared, shared_spill = measured_query(names[4], names[4], sql)
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
                "quack_direct": cgroup(names[2]), "quack_cached": cgroup(names[3]), "verglas": cgroup(names[1])}},
            "object_store": {"request_count": len(request_lines), "request_log_sha256": hashlib.sha256(trace.encode()).hexdigest()},
            "workloads": workloads,
            "spill": {"observed": peak_spill > 0, "peak_bytes": peak_spill},
            "durable_writes": inventory["writes"],
        }
        validate_report(report)
        output.write_text(json.dumps(report, indent=2) + "\n")
    finally:
        for name in reversed(names):
            subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        subprocess.run(["docker", "network", "rm", NETWORK], capture_output=True)
        shutil.rmtree(scratch, ignore_errors=True)


def main() -> None:
    """Dispatch coordinator and container worker commands."""
    parser = argparse.ArgumentParser()
    parser.add_argument("command", nargs="?", choices=("local-smoke", "seed", "serve", "query", "write", "inventory"))
    parser.add_argument("--profile", choices=("local-smoke",))
    parser.add_argument("--output", type=pathlib.Path, default=HERE / "result.json")
    parser.add_argument("--endpoint")
    parser.add_argument("--endpoint-url")
    parser.add_argument("--access-key")
    parser.add_argument("--secret")
    parser.add_argument("--bucket", default=BUCKET)
    parser.add_argument("--rows", type=int, default=ROWS)
    parser.add_argument("--host")
    parser.add_argument("--sql")
    parser.add_argument("--leg")
    args = parser.parse_args()
    if args.profile:
        if args.command:
            parser.error("choose either a command or --profile")
        args.command = args.profile
    if not args.command:
        parser.error("a command or --profile is required")
    if args.command == "local-smoke":
        local_smoke(args.output, args.rows)
    elif args.command == "seed":
        seed(args.endpoint, args.access_key, args.secret, args.bucket, args.rows)
        print("{}")
    elif args.command == "serve":
        serve_quack(args.endpoint, args.access_key, args.secret, args.bucket)
    elif args.command == "query":
        print(json.dumps(query(args.host, args.sql)))
    elif args.command == "write":
        write_probe(args.endpoint, args.access_key, args.secret, args.bucket, args.leg)
        print("{}")
    else:
        print(json.dumps(origin_inventory(args.endpoint_url, args.access_key, args.secret, args.bucket)))


if __name__ == "__main__":
    main()
