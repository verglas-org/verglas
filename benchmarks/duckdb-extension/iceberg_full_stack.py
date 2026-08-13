#!/usr/bin/env python3
"""Orchestrate and validate object-backed Iceberg full-stack evidence.

The coordinator never executes a substitute engine or invents observations.
Its manifest names the real catalog bootstrap, containers, and measured clients.
"""
import argparse
import json
import pathlib
import subprocess
import time
from collections.abc import Mapping


PRODUCT_PATHS = ("quack_direct", "verglas_query_worker", "quack_verglas_extension")
CACHE_CONTROL_LEGS = (
    "quackstore_cold", "quackstore_warm", "quackstore_shared_warm",
    "verglas_s3_cold", "verglas_s3_warm", "verglas_s3_shared_warm",
)
WORKLOADS = ("scan", "selective_aggregate", "external_sort", "spill_heavy_join")
WARMUPS, REPETITIONS, DEFAULT_CACHE_MIB = 1, 5, 256


def _object(value, where):
    """Require a JSON object before consuming evidence from it."""
    if not isinstance(value, Mapping):
        raise ValueError(f"{where} must be an object")
    return value


def _field(value, field, where):
    """Return a required field without turning absence into a default."""
    if field not in value:
        raise ValueError(f"{where} is missing {field}")
    return value[field]


def cgroup_limits(cpu_max, memory_max):
    """Normalize finite cgroup-v2 values into report units."""
    quota, period = cpu_max.split()
    if quota == "max" or memory_max == "max":
        raise ValueError("benchmark limits must be finite")
    return {"cpus": int(quota) / int(period), "memory_mib": int(memory_max) // 1024 // 1024, "verified": True}


def _sample(sample, where):
    """Validate one measured result emitted by the real client container."""
    sample = _object(sample, where)
    for name in ("elapsed_ms", "ttfr_ms", "cpu_seconds", "peak_rss_mib", "spill_bytes", "origin_bytes"):
        value = _field(sample, name, where)
        if not isinstance(value, (int, float)) or value < 0:
            raise ValueError(f"{where}.{name} must be non-negative")
    if not isinstance(_field(sample, "rows", where), int) or sample["rows"] < 0:
        raise ValueError(f"{where}.rows must be a non-negative integer")
    if not isinstance(sample.get("schema"), list) or not sample["schema"]:
        raise ValueError(f"{where} has no observed schema")
    if not isinstance(sample.get("digest"), str) or not sample["digest"]:
        raise ValueError(f"{where} has no result digest")
    if sample.get("error") is not None:
        raise ValueError(f"{where} failed: {sample['error']}")
    return sample


def _limits(entries):
    """Require identical, actually observed cgroup limits for all measured legs."""
    baseline = None
    for name, entry in entries.items():
        current = _object(_field(entry, "effective_cgroup", name), f"{name}.effective_cgroup")
        if current.get("verified") is not True:
            raise ValueError(f"{name} cgroup was not verified")
        cpus, memory = current.get("cpus"), current.get("memory_mib")
        if not isinstance(cpus, (int, float)) or cpus <= 0 or not isinstance(memory, int) or memory <= 0:
            raise ValueError(f"{name} has invalid cgroup values")
        normalized = (float(cpus), memory)
        if baseline is None:
            baseline = normalized
        elif normalized != baseline:
            raise ValueError("all product and cache controls need equal cgroups")
    return baseline


def validate_report(report):
    """Reject a report that cannot substantiate every requested comparison."""
    report = _object(report, "report")
    dataset = _object(_field(report, "dataset", "report"), "dataset")
    if dataset.get("format") != "iceberg-v2" or dataset.get("catalog_engine") != "verglas-lakekeeper":
        raise ValueError("dataset must be Iceberg v2 committed by Verglas Lakekeeper")
    if dataset.get("catalog_commit_observed") is not True:
        raise ValueError("catalog commit must be observed")
    for name in ("snapshot_id", "metadata_location", "manifest_list_sha256"):
        if not dataset.get(name):
            raise ValueError(f"dataset has no {name}")
    if not isinstance(dataset.get("data_bytes"), int) or dataset["data_bytes"] <= 0:
        raise ValueError("dataset data_bytes must be observed")
    protocol = _object(_field(report, "protocol", "report"), "protocol")
    if protocol.get("warmups") != WARMUPS or protocol.get("repetitions") != REPETITIONS:
        raise ValueError("requires exactly one warmup and five repetitions")
    paths = _object(_field(report, "product_paths", "report"), "product_paths")
    if tuple(paths) != PRODUCT_PATHS:
        raise ValueError("must report exactly the three product paths")
    expected = {
        "quack_direct": (False, False, False, True),
        "verglas_query_worker": (True, True, False, False),
        "quack_verglas_extension": (True, True, True, True),
    }
    for name, proof in expected.items():
        entry = _object(paths[name], name)
        for field, wanted in zip(("uses_verglas", "uses_query_worker", "loads_verglas_extension", "uses_quack_protocol"), proof, strict=True):
            if entry.get(field) is not wanted:
                raise ValueError(f"{name} does not prove {field}={wanted}")
    cache = _object(_field(report, "cache_controls", "report"), "cache_controls")
    if tuple(cache) != CACHE_CONTROL_LEGS:
        raise ValueError("must report every cold, warm, and shared-warm cache control")
    for name in CACHE_CONTROL_LEGS:
        entry = _object(cache[name], name)
        if entry.get("capacity_mib") != DEFAULT_CACHE_MIB or entry.get("persistent") is not True:
            raise ValueError(f"{name} needs a persistent equal 256 MiB cache")
        origin = entry.get("origin_bytes")
        if not isinstance(origin, Mapping) or "cold" not in origin or "warm" not in origin:
            raise ValueError(f"{name} lacks origin byte evidence")
    for name in ("quackstore_shared_warm", "verglas_s3_shared_warm"):
        if cache[name].get("fresh_process") is not True:
            raise ValueError(f"{name} must use a fresh process")
    warm = cache["verglas_s3_warm"]
    if warm.get("warming_complete") is not True or warm.get("metadata_hits", 0) < 1:
        raise ValueError("Verglas warm cache lacks catalog warming evidence")
    if warm.get("backend_fill_bytes", -1) < 0 or warm.get("client_bytes", -1) < 0:
        raise ValueError("Verglas warm cache byte evidence is invalid")
    cpus, memory = _limits({**paths, **cache})
    if dataset["data_bytes"] < 4 * memory * 1024 * 1024:
        raise ValueError("dataset must exceed four times the worker RAM ceiling")
    workloads = _object(_field(report, "workloads", "report"), "workloads")
    if tuple(workloads) != WORKLOADS:
        raise ValueError("all fixed out-of-core workloads are required")
    for workload in WORKLOADS:
        legs = _object(workloads[workload], workload)
        if tuple(legs) != PRODUCT_PATHS + CACHE_CONTROL_LEGS:
            raise ValueError(f"{workload} lacks a required leg")
        identity = None
        for leg, result in legs.items():
            result = _object(result, f"{workload}.{leg}")
            if result.get("snapshot_id") != dataset["snapshot_id"] or result.get("manifest_list_sha256") != dataset["manifest_list_sha256"]:
                raise ValueError(f"{workload}.{leg} did not read the committed snapshot")
            samples = result.get("samples")
            if not isinstance(samples, list) or len(samples) != REPETITIONS:
                raise ValueError(f"{workload}.{leg} needs five measured samples")
            for number, item in enumerate(samples):
                item = _sample(item, f"{workload}.{leg}.samples[{number}]")
                observed = (item["rows"], item["schema"], item["digest"])
                if identity is None:
                    identity = observed
                elif identity != observed:
                    raise ValueError(f"{workload} result identity drifted at {leg}")


def _docker(*args, capture=True, check=True):
    """Invoke Docker without a competing orchestration implementation."""
    return subprocess.run(["docker", *args], text=True, check=check, capture_output=capture)


def observed_cgroup(container):
    """Read effective controls from the measured container's cgroup namespace."""
    lines = _docker("exec", container, "sh", "-c", "cat /sys/fs/cgroup/cpu.max; cat /sys/fs/cgroup/memory.max").stdout.splitlines()
    if len(lines) != 2:
        raise RuntimeError(f"unable to read cgroup evidence from {container}")
    return cgroup_limits(lines[0], lines[1])


class DockerLifecycle:
    """Own just this benchmark run's network and containers."""

    def __init__(self, run_id):
        """Set deterministic run-specific resource names."""
        self.run_id, self.network, self.containers = run_id, f"{run_id}-net", []

    def __enter__(self):
        """Create a labelled private network for the declared services."""
        _docker("network", "rm", self.network, check=False)
        _docker("network", "create", "--label", f"verglas.benchmark={self.run_id}", self.network)
        return self

    def start(self, name, image, command, cpus, memory_mib, volumes=(), environment=()):
        """Start one service with hard limits and retain its name for cleanup."""
        args = ["run", "-d", "--rm", "--name", name, "--network", self.network, "--cpus", str(cpus), "--memory", f"{memory_mib}m"]
        for value in volumes:
            args.extend(("-v", value))
        for value in environment:
            args.extend(("-e", value))
        _docker(*args, image, *command)
        self.containers.append(name)
        return observed_cgroup(name)

    def __exit__(self, _type, _value, _traceback):
        """Stop owned services and remove the private network on every outcome."""
        for name in reversed(self.containers):
            _docker("rm", "-f", name, check=False)
        _docker("network", "rm", self.network, check=False)


def _command_json(command, where):
    """Run an explicit evidence producer and decode its sole JSON document."""
    output = subprocess.run(command, text=True, check=True, capture_output=True).stdout
    try:
        return _object(json.loads(output), where)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"{where} did not emit JSON") from error


def run_manifest(manifest):
    """Run an explicit container plan, preserving only observed report evidence."""
    manifest = _object(manifest, "manifest")
    limits = _object(_field(manifest, "limits", "manifest"), "limits")
    bootstrap = _command_json(_field(manifest, "catalog_bootstrap_command", "manifest"), "catalog bootstrap")
    services = _object(_field(manifest, "services", "manifest"), "services")
    commands = _object(_field(manifest, "measurement_commands", "manifest"), "measurement commands")
    paths, cache, workloads, service_evidence = {}, {}, {name: {} for name in WORKLOADS}, {}
    with DockerLifecycle(f"iceberg-full-stack-{time.time_ns()}") as lifecycle:
        for name, service in services.items():
            service = _object(service, f"services.{name}")
            service_evidence[name] = {
                "image": _field(service, "image", name),
                "effective_cgroup": lifecycle.start(
                    name, _field(service, "image", name), _field(service, "command", name),
                    limits["cpus"], limits["memory_mib"], service.get("volumes", ()),
                    service.get("environment", ()),
                ),
            }
        for leg in PRODUCT_PATHS + CACHE_CONTROL_LEGS:
            evidence = _command_json(_field(commands, leg, "measurement commands"), leg)
            evidence["effective_cgroup"] = observed_cgroup(_field(evidence, "container", leg))
            (paths if leg in PRODUCT_PATHS else cache)[leg] = evidence
            for workload in WORKLOADS:
                workloads[workload][leg] = _field(evidence, workload, leg)
    report = {"dataset": bootstrap, "protocol": {"warmups": WARMUPS, "repetitions": REPETITIONS}, "runtime": {"services": service_evidence}, "product_paths": paths, "cache_controls": cache, "workloads": workloads}
    validate_report(report)
    return report


def main(argv=None):
    """Read one strict manifest and write its validated full-stack report."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args(argv)
    report = run_manifest(json.loads(args.manifest.read_text()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
