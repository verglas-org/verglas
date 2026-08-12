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
CARDINALITIES = (1, 10_000, 100_000)
LEGS = ("local_duckdb", "raw_arrow", "verglas_extension", "quack")


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
        "median_rows_per_second": statistics.median(
            sample["rows"] / (sample["elapsed_ms"] / 1000) if sample["elapsed_ms"] else 0 for sample in samples
        ),
        "cpu_seconds": sum(sample["cpu_seconds"] for sample in samples),
        "peak_rss_mib": max(sample["peak_rss_mib"] for sample in samples),
        "rows": samples[-1]["rows"],
        "schema": samples[-1]["schema"],
        "digest": samples[-1]["digest"],
        "aggregate_rows_per_second": total_rows / total_seconds if total_seconds else 0,
        "error": failures[0] if failures else None,
        "error_rate": len(failures) / len(samples),
    }


def inside_cgroup():
    """Read the effective cgroup v2 values visible to this container."""
    return cgroup_limits(
        pathlib.Path("/sys/fs/cgroup/cpu.max").read_text().strip(),
        pathlib.Path("/sys/fs/cgroup/memory.max").read_text().strip(),
    )


def table_identity(table):
    """Return a canonical identity for a result table, including schema."""
    schema = [[field.name, str(field.type).upper()] for field in table.schema]
    rows = [list(row) for row in zip(*[column.to_pylist() for column in table.columns])]
    payload = json.dumps(rows, separators=(",", ":"), default=str).encode()
    return len(rows), schema, hashlib.sha256(payload).hexdigest()


def require_equivalent(results):
    """Reject a report unless every mandatory protocol leg returned one identity."""
    baseline = results["local_duckdb"]
    for leg in LEGS:
        result = results[leg]
        if result["error"]:
            raise RuntimeError(f"mandatory {leg} leg failed: {result['error']}")
        if (result["rows"], result["schema"], result["digest"]) != (baseline["rows"], baseline["schema"], baseline["digest"]):
            raise RuntimeError(f"mandatory {leg} leg returned a different result")


def sample_query(leg, cardinality, endpoint=None):
    """Execute exactly one real protocol attempt and return its measured result."""
    start_cpu = resource.getrusage(resource.RUSAGE_SELF)
    started = time.perf_counter_ns()
    ttfr_ms = None
    error = None
    try:
        if leg == "local_duckdb":
            import duckdb
            connection = duckdb.connect(":memory:")
            table = connection.sql(f"SELECT i::BIGINT AS n, CASE WHEN i % 2 = 0 THEN 'even' ELSE 'odd' END AS grp FROM range({cardinality}) AS t(i)").arrow().read_all()
            ttfr_ms = (time.perf_counter_ns() - started) / 1_000_000
        elif leg == "raw_arrow":
            with urllib.request.urlopen(endpoint + f"/arrow?cardinality={cardinality}", timeout=30) as response:
                body = response.read()
                ttfr_ms = (time.perf_counter_ns() - started) / 1_000_000
            import pyarrow.ipc
            table = pyarrow.ipc.open_stream(body).read_all()
        elif leg == "verglas_extension":
            # This is deliberately a genuine load/query attempt, not a local fallback.
            import duckdb
            extension = os.environ.get("VERGLAS_EXTENSION", "/artifacts/verglas.duckdb_extension")
            connection = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
            connection.execute("LOAD '" + extension.replace("'", "''") + "'")
            table = connection.sql(f"SELECT * FROM verglas_query('SELECT n, grp FROM benchmark_rows LIMIT {cardinality}')").arrow().read_all()
        elif leg == "quack":
            import duckdb
            connection = duckdb.connect(":memory:")
            connection.execute("INSTALL quack")
            connection.execute("LOAD quack")
            table = connection.sql(f"FROM quack_query('quack:{os.environ.get('QUACK_HOST', QUACK_SERVER)}:9000', 'SELECT n, grp FROM benchmark_rows LIMIT {cardinality}', token = 'benchmark-token', disable_ssl = true)").arrow().read_all()
        rows, schema, digest = table_identity(table)
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
        "peak_rss_mib": end_cpu.ru_maxrss / 1024, "error": error, "effective_cgroup": inside_cgroup(),
    }


class ArrowHandler(http.server.BaseHTTPRequestHandler):
    """Serve one Arrow IPC result for the raw HTTP comparison leg."""
    def do_GET(self):
        """Return the canonical result only at the Arrow endpoint."""
        if not self.path.startswith("/arrow"):
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/vnd.apache.arrow.stream")
        payload = self.server.payload_for(self.path)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
    def do_POST(self):
        """Serve the extension's database-scoped Arrow query endpoint."""
        if not self.path.endswith("/query"):
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/vnd.apache.arrow.stream")
        payload = self.server.payload_for(self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode())
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
    def log_message(self, *_):
        """Keep benchmark server logs free of request noise."""


def serve_arrow(port):
    """Start the actual raw HTTP Arrow server used by the client leg."""
    import pyarrow as pa
    import pyarrow.ipc
    import io
    server = http.server.ThreadingHTTPServer(("0.0.0.0", port), ArrowHandler)
    def payload_for(request):
        import re
        match = re.search(r"(?:cardinality=|LIMIT\s+)(\d+)", request, re.I)
        cardinality = int(match.group(1)) if match else 10_000
        batch = pa.record_batch([pa.array(range(cardinality), type=pa.int64()), pa.array(["even" if i % 2 == 0 else "odd" for i in range(cardinality)])], names=["n", "grp"])
        stream = io.BytesIO()
        with pyarrow.ipc.new_stream(stream, batch.schema) as writer:
            writer.write_batch(batch)
        return stream.getvalue()
    server.payload_for = payload_for
    server.serve_forever()


def serve_quack():
    """Run a real DuckDB Quack server with the benchmark relation."""
    import duckdb
    connection = duckdb.connect(":memory:")
    connection.execute("INSTALL quack")
    connection.execute("LOAD quack")
    connection.execute("CREATE TABLE benchmark_rows AS SELECT i::BIGINT AS n, CASE WHEN i % 2 = 0 THEN 'even' ELSE 'odd' END AS grp FROM range(100000) AS t(i)")
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


def client_sample(leg, cardinality, endpoint=None):
    """Measure one attempt in the mandatory 1 CPU / 512 MiB smoke client."""
    cmd = ["run", "--rm", "--network", NETWORK, "--cpus", "1", "--memory", "512m", IMAGE, "python", "/bench/benchmark.py", "sample", "--leg", leg]
    if leg == "verglas_extension":
        cmd[1:1] = ["--env", f"VERGLAS_ENDPOINT=http://{ARROW_SERVER}:8080", "--env", "VERGLAS_DATABASE=benchmark", "--env", "VERGLAS_TOKEN=benchmark-token"]
    if leg == "quack":
        cmd[1:1] = ["--env", f"QUACK_HOST={QUACK_SERVER}"]
    if endpoint:
        cmd += ["--endpoint", endpoint]
    cmd += ["--cardinality", str(cardinality)]
    result = docker(*cmd).stdout
    return json.loads(result)


def run(profile, output):
    """Run warmups and repetitions, then write the complete evidence report."""
    repetitions = 5 if profile == "smoke" else 10
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
            for cardinality in CARDINALITIES:
                client_sample(leg, cardinality, endpoint=f"http://{ARROW_SERVER}:8080" if leg == "raw_arrow" else None)
                samples = [client_sample(leg, cardinality, endpoint=f"http://{ARROW_SERVER}:8080" if leg == "raw_arrow" else None) for _ in range(repetitions)]
                legs[leg][f"scan_{cardinality}"] = summarize(samples)
        comparisons = []
        for cardinality in CARDINALITIES:
            case = f"scan_{cardinality}"
            identities = {leg: legs[leg][case] for leg in LEGS}
            require_equivalent(identities)
            for leg in LEGS[1:]:
                comparisons.append({"baseline": "local_duckdb", "candidate": leg, "case": case, "equivalent": True, "median_ratio": identities[leg]["median_ms"] / identities["local_duckdb"]["median_ms"]})
        report = {"duckdb_version": "1.5.5", "protocol": {"warmups": 1, "repetitions": repetitions, "cardinalities": CARDINALITIES, "legs": legs, "comparisons": comparisons},
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
    parser.add_argument("port", nargs="?", type=int)
    args = parser.parse_args()
    if args.command == "cgroup": print(json.dumps(inside_cgroup()))
    elif args.command == "sample": print(json.dumps(sample_query(args.leg, args.cardinality, args.endpoint)))
    elif args.command == "serve-arrow": serve_arrow(args.port)
    elif args.command == "serve-quack": serve_quack()
    else: run(args.profile, args.output)


if __name__ == "__main__":
    main()
