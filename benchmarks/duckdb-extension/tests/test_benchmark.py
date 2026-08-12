"""Contract tests for the DuckDB extension benchmark report."""
import importlib.util
import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("benchmark", ROOT / "benchmark.py")
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


class BenchmarkContractTests(unittest.TestCase):
    """Keep the report math and cgroup parsing independently testable."""

    def test_cgroup_v2_limits_are_normalized(self):
        self.assertEqual(
            benchmark.cgroup_limits("100000 100000", "536870912"),
            {"cpus": 1.0, "memory_mib": 512},
        )

    def test_summary_keeps_all_raw_samples_and_percentiles(self):
        samples = [
            {"elapsed_ms": value, "ttfr_ms": value / 2, "rows": 10,
             "cpu_seconds": 0.01, "peak_rss_mib": 12.0,
             "digest": "same", "schema": [["n", "BIGINT"]]}
            for value in (10, 20, 30, 40, 50)
        ]
        summary = benchmark.summarize(samples)
        self.assertEqual(summary["median_ms"], 30)
        self.assertEqual(summary["p95_ms"], 50)
        self.assertEqual(len(summary["samples"]), 5)
        self.assertEqual(summary["digest"], "same")

    def test_report_refuses_a_failed_or_non_equivalent_mandatory_leg(self):
        good = {"rows": 1, "schema": [["n", "BIGINT"]], "digest": "x", "error": None}
        bad = {**good, "error": "extension did not load"}
        self.assertRaises(RuntimeError, benchmark.require_equivalent, {"local_duckdb": good, "raw_arrow": good, "verglas_extension": bad, "quack": good})


if __name__ == "__main__":
    unittest.main()
