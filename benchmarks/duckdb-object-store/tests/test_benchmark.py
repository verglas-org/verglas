"""Acceptance tests for the durable, out-of-core DuckDB benchmark."""

import importlib.util
import copy
import pathlib
import tempfile
import unittest


ROOT = pathlib.Path(__file__).parents[1]
SPEC = importlib.util.spec_from_file_location("benchmark", ROOT / "benchmark.py")
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


class BenchmarkContractTest(unittest.TestCase):
    """Reject reports that could recreate the old in-memory comparison."""

    def valid_report(self):
        """Return the smallest report satisfying every evidence gate."""
        report = {
            "dataset": {
                "format": "parquet",
                "storage": "s3-compatible",
                "bytes": 1_500_000_000,
                "worker_memory_bytes": benchmark.ENGINE_MEMORY_BYTES,
                "object_count": 8,
            },
            "runtime": {
                "duckdb_operator": {
                    "memory_limit": benchmark.ENGINE_MEMORY,
                    "memory_limit_bytes": benchmark.ENGINE_MEMORY_BYTES,
                    "threads": 1,
                    "read_legs": list(benchmark.LEGS),
                },
                "services": {
                    name: {"container_id": name * 2, "image_id": "sha256:" + name}
                    for name in (
                        "minio",
                        "verglas",
                        "quack_direct",
                        "quackstore_cached",
                        "quackstore_shared",
                        "verglas_cached",
                        "verglas_shared",
                    )
                },
                "limits": {
                    "quack_direct": {"cpus": 1.0, "memory_bytes": 2 * 1024**3},
                    "quackstore_cached": {"cpus": 1.0, "memory_bytes": 2 * 1024**3},
                    "quackstore_shared": {"cpus": 1.0, "memory_bytes": 2 * 1024**3},
                    "verglas_cached": {"cpus": 1.0, "memory_bytes": 2 * 1024**3},
                    "verglas_shared": {"cpus": 1.0, "memory_bytes": 2 * 1024**3},
                    "verglas": {"cpus": 1.0, "memory_bytes": 256 * 1024 * 1024},
                },
                "quackstore_processes": {
                    workload: {
                        "primary": {
                            "container_id": f"{workload}-primary",
                            "image_id": "sha256:quackstore",
                            "started_at_ns": 10,
                            "stopped_at_ns": 20,
                        },
                        "shared": {
                            "container_id": f"{workload}-shared",
                            "image_id": "sha256:quackstore",
                            "started_at_ns": 30,
                            "stopped_at_ns": 40,
                        },
                    }
                    for workload in benchmark.WORKLOADS
                },
            },
            "object_store": {"request_count": 10, "request_log_sha256": "1" * 64},
            "cache_stats": {
                "verglas": {
                    "cache": {"capacity_bytes": 256 * 1024**2, "dram_bytes": 80 * 1024**2},
                    "counters": {"backend_fills": 1, "disk_hits": 1},
                    "occupancy_bytes": 1,
                },
                "quackstore": {
                    "capacity_bytes": 256 * 1024**2,
                    "logical_capacity_bytes": 256 * 1024**2,
                    "actual_file_bytes": 256 * 1024**2,
                    "allocated_bytes": 256 * 1024**2,
                    "occupancy_bytes": 1,
                    "cache_path": "/external/quackstore/cache.bin",
                    "data_mutable": False,
                },
                "storage": {"verglas_device": "external-disk", "quackstore_device": "external-disk"},
            },
            "workloads": {
                name: {
                    leg: {"elapsed_ms": 1.0, "result_digest": "a" * 64}
                    for leg in benchmark.LEGS
                }
                for name in ("scan_aggregate", "external_sort", "spill_join")
            },
            "spill": {"observed": True, "peak_bytes": 1},
            "durable_writes": {
                leg: {"origin_bytes": 1, "readback_sha256": "b" * 64}
                for leg in ("direct", "through_verglas")
            },
            "repetitions": [],
            "cache_resets": {"quackstore": len(benchmark.WORKLOADS), "verglas": len(benchmark.WORKLOADS)},
        }
        raw_report = copy.deepcopy(report)
        raw_report.pop("repetitions")
        report["repetitions"] = [
            {"number": number, "raw_report": copy.deepcopy(raw_report)}
            for number in range(1, 6)
        ]
        return report

    def test_valid_report_requires_real_out_of_core_evidence(self):
        """A complete report passes all frozen evidence checks."""
        benchmark.validate_report(self.valid_report())

    def test_dataset_must_exceed_four_times_worker_memory(self):
        """A RAM-sized dataset is rejected even when every other field exists."""
        report = self.valid_report()
        report["dataset"]["bytes"] = 4 * report["dataset"]["worker_memory_bytes"]
        with self.assertRaisesRegex(ValueError, "larger than 4x"):
            benchmark.validate_report(report)

    def test_report_requires_the_uniform_320_mb_duckdb_operator_limit(self):
        """Every measured server must use the fixed operator limit that leaves QuackStore headroom."""
        report = self.valid_report()
        report["runtime"]["duckdb_operator"]["memory_limit_bytes"] = 256 * 1_000_000
        with self.assertRaisesRegex(ValueError, "320 MB"):
            benchmark.validate_report(report)

        report = self.valid_report()
        del report["runtime"]["duckdb_operator"]
        with self.assertRaisesRegex(ValueError, "DuckDB operator"):
            benchmark.validate_report(report)

        report = self.valid_report()
        report["runtime"]["duckdb_operator"]["read_legs"] = list(benchmark.LEGACY_LEGS)
        with self.assertRaisesRegex(ValueError, "uniformly"):
            benchmark.validate_report(report)

    def test_dataset_gate_uses_the_reported_operator_limit(self):
        """The out-of-core size proof cannot be decoupled from the measured server setting."""
        report = self.valid_report()
        report["dataset"]["worker_memory_bytes"] = 256 * 1_000_000
        with self.assertRaisesRegex(ValueError, "must match"):
            benchmark.validate_report(report)

    def test_spill_and_origin_traffic_must_be_observed(self):
        """Declared limits without observed spill or storage traffic are not evidence."""
        report = self.valid_report()
        report["spill"]["observed"] = False
        with self.assertRaisesRegex(ValueError, "spill"):
            benchmark.validate_report(report)
        report = self.valid_report()
        report["object_store"]["request_count"] = 0
        with self.assertRaisesRegex(ValueError, "object-store"):
            benchmark.validate_report(report)

    def test_report_requires_actual_cache_capacity_and_counters(self):
        """A warm-cache claim must carry the server's observed cache configuration."""
        report = self.valid_report()
        del report["cache_stats"]
        with self.assertRaisesRegex((KeyError, ValueError), "cache"):
            benchmark.validate_report(report)

    def test_all_read_legs_must_have_equivalent_results(self):
        """Cold, warm, shared-warm, and direct results must agree."""
        report = self.valid_report()
        report["workloads"]["spill_join"]["verglas_warm"]["result_digest"] = "c" * 64
        with self.assertRaisesRegex(ValueError, "result mismatch"):
            benchmark.validate_report(report)

    def test_every_quack_process_must_have_provenance_and_one_cpu(self):
        """The shared-warm result is valid only when its server is bounded too."""
        report = self.valid_report()
        del report["runtime"]["services"]["verglas_shared"]
        with self.assertRaisesRegex(ValueError, "verglas_shared"):
            benchmark.validate_report(report)

        report = self.valid_report()
        report["runtime"]["limits"]["quackstore_shared"]["cpus"] = 2.0
        with self.assertRaisesRegex(ValueError, "one CPU"):
            benchmark.validate_report(report)

    def test_every_quack_process_has_the_same_two_gib_container_ceiling(self):
        """A leg cannot gain an unreported RAM advantage outside DuckDB's allocator."""
        report = self.valid_report()
        report["runtime"]["limits"]["quackstore_cached"]["memory_bytes"] = 3 * 1024**3
        with self.assertRaisesRegex(ValueError, "2 GiB"):
            benchmark.validate_report(report)

    def test_r2_origin_uses_external_provenance_instead_of_a_fake_container(self):
        """A live R2 report identifies the remote origin and still passes every gate."""
        report = copy.deepcopy(self.valid_report())
        del report["runtime"]["services"]["minio"]
        report["runtime"]["external_origin"] = {
            "provider": "cloudflare-r2",
            "bucket": "verglas-duckdb-bench-20260812",
            "endpoint": "example.r2.cloudflarestorage.com",
        }
        report["object_store"]["evidence_source"] = "cloudflare-r2-graphql"
        benchmark.validate_report(report)

    def test_report_rejects_an_origin_without_container_or_external_provenance(self):
        """Storage timing is invalid when the measured origin cannot be identified."""
        report = copy.deepcopy(self.valid_report())
        del report["runtime"]["services"]["minio"]
        with self.assertRaisesRegex(ValueError, "origin provenance"):
            benchmark.validate_report(report)

    def test_r2_endpoint_enables_tls_and_auto_region(self):
        """R2 must not inherit the local MinIO endpoint's insecure transport settings."""
        settings = benchmark.r2_endpoint_settings("0123456789abcdef")
        self.assertEqual(settings["endpoint"], "0123456789abcdef.r2.cloudflarestorage.com")
        self.assertEqual(settings["endpoint_url"], "https://0123456789abcdef.r2.cloudflarestorage.com")
        self.assertEqual(settings["region"], "auto")
        self.assertTrue(settings["use_ssl"])

    def test_r2_requires_all_three_s3_credential_parts(self):
        """A token value alone is never guessed into an R2 credential pair."""
        with self.assertRaisesRegex(ValueError, "R2_ACCESS_KEY_ID"):
            benchmark.r2_credentials({"CLOUDFLARE_API_TOKEN": "secret"})

    def test_spill_sampling_reads_the_host_mount_without_docker(self):
        """Spill evidence comes from the bind mount, not transient docker exec calls."""
        with tempfile.TemporaryDirectory() as directory:
            spill = pathlib.Path(directory) / "duckdb.tmp"
            spill.write_bytes(b"x" * 8192)
            self.assertGreaterEqual(benchmark.spill_directory_bytes(pathlib.Path(directory)), 8192)

    def test_quackstore_uses_its_persistent_cache_protocol(self):
        """QuackStore must use its real community extension and immutable URI form."""
        cache_file = benchmark.quackstore_cache_file(pathlib.Path("/external/quackstore"))
        settings = benchmark.quackstore_settings(str(cache_file))
        self.assertEqual(settings["cache_size"], benchmark.DEFAULT_CACHE_CAPACITY_BYTES)
        self.assertFalse(settings["data_mutable"])
        self.assertEqual(cache_file, pathlib.Path("/external/quackstore/cache.bin"))
        self.assertEqual(
            benchmark.quackstore_setting_statements(settings),
            (
                "SET GLOBAL quackstore_cache_enabled = true",
                "SET GLOBAL quackstore_cache_path = '/external/quackstore/cache.bin'",
                f"SET GLOBAL quackstore_cache_size = {benchmark.DEFAULT_CACHE_CAPACITY_BYTES}",
                "SET GLOBAL quackstore_data_mutable = false",
            ),
        )
        self.assertEqual(
            benchmark.quackstore_location("verglas-benchmark"),
            "quackstore://s3://verglas-benchmark/issue-126/data/*.parquet",
        )

    def test_quackstore_evidence_records_logical_and_physical_cache_bytes(self):
        """The report distinguishes QuackStore's configured budget from its cache file allocation."""
        with tempfile.TemporaryDirectory() as directory:
            cache_file = benchmark.quackstore_cache_file(pathlib.Path(directory))
            cache_file.write_bytes(b"x" * 8192)
            evidence = benchmark.quackstore_cache_evidence(cache_file)
        self.assertEqual(evidence["cache_path"], str(cache_file))
        self.assertEqual(evidence["logical_capacity_bytes"], benchmark.DEFAULT_CACHE_CAPACITY_BYTES)
        self.assertEqual(evidence["actual_file_bytes"], 8192)
        self.assertGreaterEqual(evidence["allocated_bytes"], 8192)

    def test_report_requires_full_raw_repetition_reports(self):
        """A repetition hash alone cannot preserve the evidence needed to audit all five runs."""
        report = self.valid_report()
        report["repetitions"] = [{"number": number, "report_sha256": "a" * 64} for number in range(1, 6)]
        with self.assertRaisesRegex(ValueError, "raw report"):
            benchmark.validate_report(report)

    def test_report_requires_five_independent_repetitions(self):
        """One fortuitous cache run cannot stand in for the five-run protocol."""
        report = self.valid_report()
        report["repetitions"].pop()
        with self.assertRaisesRegex(ValueError, "five independent repetitions"):
            benchmark.validate_report(report)

    def test_report_rejects_unequal_or_unobserved_cache_budget(self):
        """Both persistent caches need exactly the same observed logical budget."""
        report = self.valid_report()
        report["cache_stats"]["quackstore"]["capacity_bytes"] -= 1
        with self.assertRaisesRegex(ValueError, "256 MiB"):
            benchmark.validate_report(report)
        report = self.valid_report()
        report["cache_stats"]["quackstore"]["occupancy_bytes"] = 0
        with self.assertRaisesRegex(ValueError, "occupancy"):
            benchmark.validate_report(report)

    def test_report_requires_shared_external_disk_and_explicit_resets(self):
        """Cache results are comparable only when both caches share storage and reset evidence."""
        report = self.valid_report()
        report["cache_stats"]["storage"]["quackstore_device"] = "other-disk"
        with self.assertRaisesRegex(ValueError, "same external disk"):
            benchmark.validate_report(report)
        report = self.valid_report()
        report["cache_resets"]["quackstore"] -= 1
        with self.assertRaisesRegex(ValueError, "reset"):
            benchmark.validate_report(report)

    def test_report_rejects_overlapping_quackstore_cache_processes(self):
        """The persistent-cache handoff must stop its primary before the fresh reader starts."""
        report = self.valid_report()
        process = report["runtime"]["quackstore_processes"]["scan_aggregate"]
        process["primary"]["stopped_at_ns"] = process["shared"]["started_at_ns"] + 1
        with self.assertRaisesRegex(ValueError, "before fresh shared"):
            benchmark.validate_report(report)

    def test_report_requires_a_quackstore_cache_file_not_a_directory(self):
        """A directory path is not a QuackStore cache-file configuration."""
        report = self.valid_report()
        report["cache_stats"]["quackstore"]["cache_path"] = "/external/quackstore"
        with self.assertRaisesRegex(ValueError, "cache file"):
            benchmark.validate_report(report)


if __name__ == "__main__":
    unittest.main()
