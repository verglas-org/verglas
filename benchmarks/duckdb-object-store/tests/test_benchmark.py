"""Acceptance tests for the durable, out-of-core DuckDB benchmark."""

import importlib.util
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
        return {
            "dataset": {
                "format": "parquet",
                "storage": "s3-compatible",
                "bytes": 600 * 1024 * 1024,
                "worker_memory_bytes": 128 * 1024 * 1024,
                "object_count": 8,
            },
            "runtime": {
                "services": {
                    name: {"container_id": name * 2, "image_id": "sha256:" + name}
                    for name in ("minio", "verglas", "quack_direct", "quack_cached")
                },
                "limits": {
                    "quack_direct": {"cpus": 1.0, "memory_bytes": 128 * 1024 * 1024},
                    "quack_cached": {"cpus": 1.0, "memory_bytes": 128 * 1024 * 1024},
                    "verglas": {"cpus": 1.0, "memory_bytes": 256 * 1024 * 1024},
                },
            },
            "object_store": {"request_count": 10, "request_log_sha256": "1" * 64},
            "workloads": {
                name: {
                    leg: {"elapsed_ms": 1.0, "result_digest": "a" * 64}
                    for leg in ("direct", "verglas_cold", "verglas_warm", "verglas_shared_warm")
                }
                for name in ("scan_aggregate", "external_sort", "spill_join")
            },
            "spill": {"observed": True, "peak_bytes": 1},
            "durable_writes": {
                leg: {"origin_bytes": 1, "readback_sha256": "b" * 64}
                for leg in ("direct", "through_verglas")
            },
        }

    def test_valid_report_requires_real_out_of_core_evidence(self):
        """A complete report passes all frozen evidence checks."""
        benchmark.validate_report(self.valid_report())

    def test_dataset_must_exceed_four_times_worker_memory(self):
        """A RAM-sized dataset is rejected even when every other field exists."""
        report = self.valid_report()
        report["dataset"]["bytes"] = 4 * report["dataset"]["worker_memory_bytes"]
        with self.assertRaisesRegex(ValueError, "larger than 4x"):
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

    def test_all_read_legs_must_have_equivalent_results(self):
        """Cold, warm, shared-warm, and direct results must agree."""
        report = self.valid_report()
        report["workloads"]["spill_join"]["verglas_warm"]["result_digest"] = "c" * 64
        with self.assertRaisesRegex(ValueError, "result mismatch"):
            benchmark.validate_report(report)

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


if __name__ == "__main__":
    unittest.main()
