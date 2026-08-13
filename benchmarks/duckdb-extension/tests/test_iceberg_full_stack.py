"""Freeze the evidence contract for the Iceberg full-stack benchmark."""

import importlib.util
import pathlib
import unittest


MODULE_PATH = pathlib.Path(__file__).parents[1] / "iceberg_full_stack.py"
SPEC = importlib.util.spec_from_file_location("iceberg_full_stack", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load Iceberg full-stack benchmark module")
BENCHMARK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCHMARK)


def sample(elapsed_ms=100.0):
    """Return one canonical measured observation."""
    return {
        "elapsed_ms": elapsed_ms, "ttfr_ms": elapsed_ms / 4,
        "cpu_seconds": 0.1, "peak_rss_mib": 128, "spill_bytes": 0,
        "origin_bytes": 1024, "rows": 1,
        "schema": [["answer", "BIGINT"]], "digest": "canonical-result",
        "error": None,
    }


def valid_report():
    """Return the smallest report satisfying the frozen comparison contract."""
    limits = {"cpus": 1.0, "memory_mib": 512, "verified": True}
    paths = {
        "quack_direct": {"uses_verglas": False, "uses_query_worker": False,
            "loads_verglas_extension": False, "uses_quack_protocol": True,
            "effective_cgroup": limits.copy()},
        "verglas_query_worker": {"uses_verglas": True, "uses_query_worker": True,
            "loads_verglas_extension": False, "uses_quack_protocol": False,
            "effective_cgroup": limits.copy()},
        "quack_verglas_extension": {"uses_verglas": True, "uses_query_worker": True,
            "loads_verglas_extension": True, "uses_quack_protocol": True,
            "effective_cgroup": limits.copy()},
    }
    cache = {}
    for leg in BENCHMARK.CACHE_CONTROL_LEGS:
        cache[leg] = {"capacity_mib": 256, "persistent": True,
            "fresh_process": leg.endswith("shared_warm"),
            "effective_cgroup": limits.copy(),
            "origin_bytes": {"cold": 4096, "warm": 1024}}
    cache["verglas_s3_warm"].update({"warming_complete": True,
        "metadata_hits": 4, "metadata_misses": 1,
        "backend_fill_bytes": 4096, "client_bytes": 2048})
    workloads = {}
    for workload in BENCHMARK.WORKLOADS:
        workloads[workload] = {}
        for leg in (*BENCHMARK.PRODUCT_PATHS, *BENCHMARK.CACHE_CONTROL_LEGS):
            workloads[workload][leg] = {"snapshot_id": 73,
                "manifest_list_sha256": "manifest-identity",
                "samples": [sample(100 + repetition) for repetition in range(5)]}
    return {
        "dataset": {"format": "iceberg-v2", "catalog_engine": "verglas-lakekeeper",
            "catalog_commit_observed": True, "snapshot_id": 73,
            "metadata_location": "s3://benchmark/warehouse/table/metadata/v1.json",
            "manifest_list_sha256": "manifest-identity", "data_bytes": 3 * 1024**3,
            "data_files": 48},
        "protocol": {"warmups": 1, "repetitions": 5},
        "product_paths": paths, "cache_controls": cache, "workloads": workloads,
    }


class IcebergFullStackContractTests(unittest.TestCase):
    """Reject benchmark reports that cannot answer the requested comparisons."""

    def test_valid_report_covers_three_product_paths(self):
        """Accept exactly the required direct, worker, and extension paths."""
        BENCHMARK.validate_report(valid_report())
        self.assertEqual(BENCHMARK.PRODUCT_PATHS, (
            "quack_direct", "verglas_query_worker", "quack_verglas_extension"))

    def test_raw_parquet_or_unobserved_catalog_commit_is_rejected(self):
        """Require Iceberg v2 data committed through the Verglas catalog engine."""
        for field, value in (("format", "parquet"), ("catalog_engine", "filesystem"),
                             ("catalog_commit_observed", False)):
            report = valid_report()
            report["dataset"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                BENCHMARK.validate_report(report)

    def test_fake_worker_or_unloaded_extension_is_rejected(self):
        """Require both product paths to traverse the real worker and extension."""
        for path, field in (("verglas_query_worker", "uses_query_worker"),
                            ("quack_verglas_extension", "uses_query_worker"),
                            ("quack_verglas_extension", "loads_verglas_extension"),
                            ("quack_verglas_extension", "uses_quack_protocol")):
            report = valid_report()
            report["product_paths"][path][field] = False
            with self.subTest(path=path, field=field), self.assertRaises(ValueError):
                BENCHMARK.validate_report(report)

    def test_unverified_or_unequal_resource_limits_are_rejected(self):
        """Keep compute and memory fixed across every product and cache control."""
        report = valid_report()
        report["product_paths"]["quack_direct"]["effective_cgroup"]["cpus"] = 2
        with self.assertRaises(ValueError):
            BENCHMARK.validate_report(report)
        report = valid_report()
        report["cache_controls"]["quackstore_warm"]["effective_cgroup"]["verified"] = False
        with self.assertRaises(ValueError):
            BENCHMARK.validate_report(report)

    def test_missing_iceberg_cache_evidence_is_rejected(self):
        """Require cache persistence, fresh-process reuse, and catalog warming evidence."""
        mutations = (("quackstore_warm", "persistent", False),
            ("quackstore_shared_warm", "fresh_process", False),
            ("verglas_s3_shared_warm", "fresh_process", False),
            ("verglas_s3_warm", "warming_complete", False),
            ("verglas_s3_warm", "metadata_hits", 0))
        for leg, field, value in mutations:
            report = valid_report()
            report["cache_controls"][leg][field] = value
            with self.subTest(leg=leg, field=field), self.assertRaises(ValueError):
                BENCHMARK.validate_report(report)

    def test_fewer_than_five_samples_or_identity_drift_is_rejected(self):
        """Require five repetitions of every workload with one snapshot and result."""
        report = valid_report()
        report["workloads"][BENCHMARK.WORKLOADS[0]]["quack_direct"]["samples"].pop()
        with self.assertRaises(ValueError):
            BENCHMARK.validate_report(report)
        report = valid_report()
        report["workloads"][BENCHMARK.WORKLOADS[0]]["quack_verglas_extension"]["samples"][2]["digest"] = "wrong"
        with self.assertRaises(ValueError):
            BENCHMARK.validate_report(report)


if __name__ == "__main__":
    unittest.main()
