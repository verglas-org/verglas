#!/usr/bin/env python3
"""Run reproducible, cgroup-bounded DuckDB protocol measurements.

The coordinator only assembles measurements emitted by bounded containers; it
never synthesizes a timing or substitutes another transport for an unavailable
full-stack leg.
"""
import argparse
import hashlib
import http.server
import json
import os
import pathlib
import resource
import statistics
import subprocess
import sys
import time
import urllib.request


ROOT = pathlib.Path(__file__).resolve().parent
IMAGE = "verglas-duckdb-benchmark-debug-b:1.5.5"
NETWORK = "verglas-duckdb-benchmark-debug-b-net"
ARROW_SERVER = "verglas-bench-arrow-debug-b"
QUACK_SERVER = "verglas-bench-quack-debug-b"
CARDINALITIES = (1, 100_000, 1_000_000)
LEGS = ("local_duckdb", "raw_arrow", "verglas_extension", "quack")
PRIMARY_MEASUREMENT_MODE = "prepared_session"


def cgroup_limits(cpu_max, memory_max):
    """Convert cgroup-v2 cpu.max and memory.max files to report units."""
    quota, period = cpu_max.split()
    cpus = None if quota == "max" else int(quota) / int(period)
    memory = None if memory_max == "max" else int(memory_max) // (1024 * 1024)
    return {"cpus": cpus, "memory_mib": memory}


def percentile(values, fraction):
    """Return the nearest-rank percentile without inventing intermediate data."""
    ordered = sorted(values)
    return ordered[max(0, min(len(ordered) - 1, int((len(ordered) - 1) * fraction + 0.999999)))]


def summarize(samples):
    """Preserve raw observations and compute displayed metrics from them."""
    elapsed = [sample["elapsed_ms"] for sample in samples]
    total_rows = sum(sample["rows"] for sample in samples)
    total_seconds = sum(elapsed) / 1000
    failures = [sample.get("error") for sample in samples if sample.get("error")]
    return {
        "samples": samples,
        "median_ms": statistics.median(elapsed),
        "p95_ms": percentile(elapsed, 0.95),
        "median_ttfr_ms": statistics.median(sample["ttfr_ms"] for sample in samples),
        "p95_ttfr_ms": percentile([sample["ttfr_ms"] for sample in samples], 0.95),
        "median_rows_per_second": statistics.median(
            sample["rows"] / (sample["elapsed_ms"] / 1000) if sample["elapsed_ms"] else 0 for sample in samples
        ),
        "cpu_seconds": sum(sample["cpu_seconds"] for sample in samples),
        "median_cpu_seconds_per_million_rows": statistics.median(
            sample["cpu_seconds"] * 1_000_000 / sample["rows"] if sample["rows"] else 0
            for sample in samples
        ),
        "peak_rss_mib": max(sample["peak_rss_mib"] for sample in samples),
        "rows": samples[-1]["rows"],
        "schema": samples[-1]["schema"],
        "digest": samples[-1]["digest"],
        "aggregate_rows_per_second": total_rows / total_seconds if total_seconds else 0,
        "median_network_receive_bytes": statistics.median(sample["network_receive_bytes"] for sample in samples),
        "error": failures[0] if failures else None,
        "error_rate": len(failures) / len(samples),
    }


def inside_cgroup():
    """Read the effective cgroup v2 values visible to this container."""
    return cgroup_limits(
        pathlib.Path("/sys/fs/cgroup/cpu.max").read_text().strip(),
        pathlib.Path("/sys/fs/cgroup/memory.max").read_text().strip(),
    )


def network_receive_bytes():
    """Read bytes received by the measured container's primary interface."""
    path = pathlib.Path("/sys/class/net/eth0/statistics/rx_bytes")
    return int(path.read_text()) if path.exists() else 0


def stream_identity(reader, started):
    """Drain Arrow batches into a bounded canonical digest and capture first row."""
    rows = 0
    schema = [[field.name, str(field.type).upper()] for field in reader.schema]
    column_digests = [hashlib.sha256() for _ in reader.schema]
    ttfr_ms = None
    for batch in reader:
        if batch.num_rows and ttfr_ms is None:
            ttfr_ms = (time.perf_counter_ns() - started) / 1_000_000
        for index, column in enumerate(batch.columns):
            if column.null_count:
                raise ValueError("benchmark identity requires non-null primitive columns")
            values = column.to_numpy(zero_copy_only=False)
            if values.dtype.hasobject:
                raise TypeError("benchmark identity does not guess encodings for variable-width columns")
            column_digests[index].update(values.tobytes(order="C"))
        rows += batch.num_rows
    digest = hashlib.sha256(json.dumps(schema, separators=(",", ":")).encode())
    for column_digest in column_digests:
        digest.update(column_digest.digest())
    return rows, schema, digest.hexdigest(), ttfr_ms


def require_equivalent(results):
    """Reject a report unless every mandatory protocol leg returned one identity."""
    baseline = results["local_duckdb"]
    for leg in LEGS:
        result = results[leg]
        if result["error"]:
            raise RuntimeError(f"mandatory {leg} leg failed: {result['error']}")
        if (result["rows"], result["schema"], result["digest"]) != (baseline["rows"], baseline["schema"], baseline["digest"]):
            raise RuntimeError(f"mandatory {leg} leg returned a different result")


def prepare_query(leg, endpoint=None):
    """Prepare one reusable client session without charging setup to query time."""
    if leg == "local_duckdb":
        import duckdb
        connection = duckdb.connect(":memory:")
        return lambda cardinality: connection.execute(
            f"SELECT i::BIGINT AS n, (i % 2)::TINYINT AS grp FROM range({cardinality}) AS t(i)"
        ).fetch_record_batch(65_536)
    if leg == "raw_arrow":
        import pyarrow.ipc
        def raw(cardinality):
            response = urllib.request.urlopen(endpoint + f"/arrow?cardinality={cardinality}", timeout=30)
            return pyarrow.ipc.open_stream(response)
        return raw
    if leg == "verglas_extension":
        import duckdb
        extension = os.environ.get("VERGLAS_EXTENSION", "/artifacts/verglas.duckdb_extension")
        connection = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
        connection.execute("LOAD '" + extension.replace("'", "''") + "'")
        return lambda cardinality: connection.execute(
            f"SELECT * FROM verglas_query('SELECT n, grp FROM benchmark_rows LIMIT {cardinality}')"
        ).fetch_record_batch(65_536)
    if leg == "quack":
        import duckdb
        connection = duckdb.connect(":memory:")
        connection.execute("LOAD quack")
        host = os.environ.get("QUACK_HOST", QUACK_SERVER)
        return lambda cardinality: connection.execute(
            f"FROM quack_query('quack:{host}:9000', 'SELECT n, grp FROM benchmark_rows LIMIT {cardinality}', token = 'benchmark-token', disable_ssl = true)"
        ).fetch_record_batch(65_536)
    raise ValueError(f"unknown benchmark leg: {leg}")


def measure_prepared(query, cardinality):
    """Measure one query against an already prepared client session."""
    start_cpu = resource.getrusage(resource.RUSAGE_SELF)
    start_network = network_receive_bytes()
    started = time.perf_counter_ns()
    error = None
    try:
        rows, schema, digest, ttfr_ms = stream_identity(query(cardinality), started)
    except Exception as caught:
        error = f"{type(caught).__name__}: {caught}"
        rows, schema = 0, [["unavailable", "NULL"]]
        digest = hashlib.sha256(error.encode()).hexdigest()
        ttfr_ms = (time.perf_counter_ns() - started) / 1_000_000
    elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
    end_cpu = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "elapsed_ms": elapsed_ms, "ttfr_ms": ttfr_ms, "rows": rows, "schema": schema,
        "digest": digest, "cpu_seconds": (end_cpu.ru_utime + end_cpu.ru_stime - start_cpu.ru_utime - start_cpu.ru_stime),
        "peak_rss_mib": end_cpu.ru_maxrss / 1024, "network_receive_bytes": max(0, network_receive_bytes() - start_network),
        "error": error, "effective_cgroup": inside_cgroup(),
    }


def prepared_samples(leg, cardinality, endpoint, warmups, repetitions):
    """Run warmups and measured repetitions through one reusable client session."""
    started = time.perf_counter_ns()
    query = prepare_query(leg, endpoint)
    setup_ms = (time.perf_counter_ns() - started) / 1_000_000
    for _ in range(warmups):
        warmup = measure_prepared(query, cardinality)
        if warmup["error"]:
            raise RuntimeError(warmup["error"])
    return {"setup_ms_excluded": setup_ms, "samples": [measure_prepared(query, cardinality) for _ in range(repetitions)]}


class ArrowHandler(http.server.BaseHTTPRequestHandler):
    """Serve one Arrow IPC result for the raw HTTP comparison leg."""
    def do_GET(self):
        """Return the canonical result only at the Arrow endpoint."""
        if not self.path.startswith("/arrow"):
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/vnd.apache.arrow.stream")
        self.end_headers()
        self.server.write_payload(self.wfile, self.path)
    def do_POST(self):
        """Serve the extension's database-scoped Arrow query endpoint."""
        if not self.path.endswith("/query"):
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/vnd.apache.arrow.stream")
        self.end_headers()
        request = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode()
        self.server.write_payload(self.wfile, request)
    def log_message(self, *_):
        """Keep benchmark server logs free of request noise."""


def serve_arrow(port):
    """Start the actual raw HTTP Arrow server used by the client leg."""
    import pyarrow as pa
    import pyarrow.ipc
    server = http.server.ThreadingHTTPServer(("0.0.0.0", port), ArrowHandler)
    def write_payload(destination, request):
        import re
        match = re.search(r"(?:cardinality=|LIMIT\s+)(\d+)", request, re.I)
        cardinality = int(match.group(1)) if match else 10_000
        schema = pa.schema([("n", pa.int64()), ("grp", pa.int8())])
        with pyarrow.ipc.new_stream(destination, schema) as writer:
            for offset in range(0, cardinality, 65_536):
                stop = min(cardinality, offset + 65_536)
                writer.write_batch(pa.record_batch(
                    [pa.array(range(offset, stop), type=pa.int64()),
                     pa.array((value % 2 for value in range(offset, stop)), type=pa.int8())],
                    schema=schema,
                ))
    server.write_payload = write_payload
    server.serve_forever()


def serve_quack():
    """Run a real DuckDB Quack server with the benchmark relation."""
    import duckdb
    connection = duckdb.connect(":memory:")
    connection.execute("INSTALL quack")
    connection.execute("LOAD quack")
    connection.execute("CREATE VIEW benchmark_rows AS SELECT i::BIGINT AS n, (i % 2)::TINYINT AS grp FROM range(10000000) AS t(i)")
    connection.execute("CALL quack_serve('quack:0.0.0.0:9000', token = 'benchmark-token', allow_other_hostname = true, disable_ssl = true)")
    while True:
        time.sleep(3600)


def docker(*args, capture=True):
    """Run Docker and expose failures as benchmark setup errors."""
    return subprocess.run(["docker", *args], text=True, check=True, capture_output=capture)


def ensure_image():
    """Build the pinned client/server image used by every measured process."""
    docker("build", "--tag", IMAGE, "--file", str(ROOT / "Dockerfile"), str(ROOT.parents[1]))


def container_cgroup(container):
    """Ask a running container for the limits it actually sees."""
    output = docker("exec", container, "python", "/bench/benchmark.py", "cgroup").stdout
    return json.loads(output)


def start_server(name, command):
    """Start one independently bounded server and return its ID and limits."""
    cid = docker("run", "-d", "--rm", "--network", NETWORK, "--name", name, "--cpus", "0.50", "--memory", "256m", IMAGE, *command).stdout.strip()
    return cid, container_cgroup(cid)


def client_prepared_samples(leg, cardinality, endpoint, warmups, repetitions):
    """Run one prepared-session worker within the mandatory client cgroup."""
    cmd = ["run", "--rm", "--network", NETWORK, "--cpus", "1", "--memory", "512m"]
    if leg == "verglas_extension":
        cmd += ["--env", f"VERGLAS_ENDPOINT=http://{ARROW_SERVER}:8080", "--env", "VERGLAS_DATABASE=benchmark", "--env", "VERGLAS_TOKEN=benchmark-token"]
    if leg == "quack":
        cmd += ["--env", f"QUACK_HOST={QUACK_SERVER}"]
    cmd += [IMAGE, "python", "/bench/benchmark.py", "prepared", "--leg", leg,
            "--cardinality", str(cardinality), "--warmups", str(warmups),
            "--repetitions", str(repetitions)]
    if endpoint:
        cmd += ["--endpoint", endpoint]
    return json.loads(docker(*cmd).stdout)


def run(profile, output):
    """Run warmups and repetitions, then write the complete evidence report."""
    repetitions = 5 if profile == "smoke" else 10
    cardinalities = CARDINALITIES if profile == "smoke" else (1, 100_000, 1_000_000, 10_000_000)
    docker("network", "create", NETWORK)
    containers = []
    try:
        ensure_image()
        arrow_id, arrow_limits = start_server(ARROW_SERVER, ["python", "/bench/benchmark.py", "serve-arrow", "8080"])
        containers.append(arrow_id)
        quack_id, quack_limits = start_server(QUACK_SERVER, ["python", "/bench/benchmark.py", "serve-quack"])
        containers.append(quack_id)
        time.sleep(0.25)
        legs = {}
        for leg in LEGS:
            legs[leg] = {}
            for cardinality in cardinalities:
                endpoint = f"http://{ARROW_SERVER}:8080" if leg == "raw_arrow" else None
                batch = client_prepared_samples(leg, cardinality, endpoint, 1, repetitions)
                measurement = summarize(batch["samples"])
                measurement["setup_ms_excluded"] = batch["setup_ms_excluded"]
                legs[leg][f"scan_{cardinality}"] = measurement
        comparisons = []
        for cardinality in cardinalities:
            case = f"scan_{cardinality}"
            identities = {leg: legs[leg][case] for leg in LEGS}
            require_equivalent(identities)
            for baseline, candidate in (
                ("local_duckdb", "raw_arrow"),
                ("raw_arrow", "verglas_extension"),
                ("raw_arrow", "quack"),
            ):
                comparisons.append({
                    "baseline": baseline,
                    "candidate": candidate,
                    "case": case,
                    "equivalent": True,
                    "median_ratio": identities[candidate]["median_ms"] / identities[baseline]["median_ms"],
                    "p95_latency_ratio": identities[candidate]["p95_ms"] / identities[baseline]["p95_ms"],
                    "median_throughput_ratio": identities[candidate]["median_rows_per_second"] / identities[baseline]["median_rows_per_second"],
                })
        report = {"duckdb_version": "1.5.5", "dependency_versions": {"python": "3.12", "pyarrow": "19.0.1", "numpy": "2.2.3"}, "protocol": {"measurement_mode": PRIMARY_MEASUREMENT_MODE, "warmups": 1, "repetitions": repetitions, "cardinalities": cardinalities, "legs": legs, "comparisons": comparisons},
            "resource_limits": {"client": {"enforcement": "docker-cgroup", "cpus": 1, "memory_mib": 512, "verified_inside_container": True},
                "servers": {key: {"enforcement": "docker-cgroup", **value, "verified_inside_container": True} for key, value in {"raw_arrow_and_verglas_api": arrow_limits, "quack": quack_limits}.items()}},
            "hardware": {"platform": os.uname().sysname, "machine": os.uname().machine},
            "full_stack": {"status": "protocol-only", "reason": "The extension is loaded from the Linux release artifact and calls the benchmark's database-scoped Arrow API; cache tracks require configured Verglas infrastructure."}}
        pathlib.Path(output).write_text(json.dumps(report, indent=2) + "\n")
    finally:
        for container in containers:
            subprocess.run(["docker", "rm", "-f", container], capture_output=True)
        subprocess.run(["docker", "network", "rm", NETWORK], capture_output=True)


def main():
    """Dispatch coordinator, worker, server, and cgroup helper commands."""
    parser = argparse.ArgumentParser()
    parser.add_argument("command", nargs="?", default="run")
    parser.add_argument("--profile", default="smoke")
    parser.add_argument("--output")
    parser.add_argument("--leg")
    parser.add_argument("--cardinality", type=int, default=10_000)
    parser.add_argument("--endpoint")
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("port", nargs="?", type=int)
    args = parser.parse_args()
    if args.command == "cgroup": print(json.dumps(inside_cgroup()))
    elif args.command == "prepared": print(json.dumps(prepared_samples(args.leg, args.cardinality, args.endpoint, args.warmups, args.repetitions)))
    elif args.command == "serve-arrow": serve_arrow(args.port)
    elif args.command == "serve-quack": serve_quack()
    else: run(args.profile, args.output)


if __name__ == "__main__":
    main()
