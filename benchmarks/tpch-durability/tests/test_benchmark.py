"""Frozen acceptance tests for the SF10 cluster durability report."""

import copy
import importlib.util
import pathlib
import unittest


MODULE = pathlib.Path(__file__).parents[1] / "benchmark.py"
SPEC = importlib.util.spec_from_file_location("tpch_durability_benchmark", MODULE)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load benchmark module")
BENCHMARK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCHMARK)


def passing_report() -> dict:
    """Return the smallest report that satisfies every durability gate."""
    checksums = {str(query): f"sha-{query}" for query in range(1, 23)}
    return {
        "scale_factor": 10,
        "dataset": {
            "rows": {
                "region": 5,
                "nation": 25,
                "supplier": 100_000,
                "customer": 1_500_000,
                "part": 2_000_000,
                "partsupp": 8_000_000,
                "orders": 15_000_000,
                "lineitem": 59_986_052,
            },
            "logical_bytes": 4_000_000_000,
        },
        "iceberg": {
            "writes_through_verglas": True,
            "query_checksums_before": checksums,
            "query_checksums_after": checksums,
            "catalog_commit_with_one_down": True,
            "immediate_read_with_one_down": True,
            "minority_write_refused": True,
            "origin_objects": 80,
            "origin_bytes": 2_000_000_000,
            "origin_checksum_mismatches": 0,
            "replica_state_hashes": ["same"] * 4,
        },
        "wal": {
            "bytes": 268_435_456,
            "leader_killed_during_append": True,
            "immediate_read_checksum_match": True,
            "post_restart_checksum_match": True,
            "archive_checkpoint_committed": True,
            "archived_objects": 16,
            "archived_bytes": 268_435_456,
        },
        "lakekeeper_postgres_processes": 0,
    }


class ReportGates(unittest.TestCase):
    """Reject any report that omits a required durability mechanism."""

    def test_complete_sf10_report_passes(self) -> None:
        """A complete report satisfies every hard gate."""
        BENCHMARK.validate_report(passing_report())

    def test_scale_query_object_wal_and_quorum_shortcuts_fail(self) -> None:
        """Smaller or partially verified workloads cannot claim success."""
        mutations = [
            ("scale_factor", 1),
            ("iceberg.query_checksums_after", {"1": "sha-1"}),
            ("iceberg.catalog_commit_with_one_down", False),
            ("iceberg.immediate_read_with_one_down", False),
            ("iceberg.minority_write_refused", False),
            ("iceberg.origin_checksum_mismatches", 1),
            ("iceberg.replica_state_hashes", ["a", "a", "a", "b"]),
            ("wal.bytes", 16 * 1024 * 1024),
            ("wal.leader_killed_during_append", False),
            ("wal.immediate_read_checksum_match", False),
            ("wal.archive_checkpoint_committed", False),
            ("lakekeeper_postgres_processes", 1),
        ]
        for path, value in mutations:
            with self.subTest(path=path):
                report = copy.deepcopy(passing_report())
                target = report
                parts = path.split(".")
                for part in parts[:-1]:
                    target = target[part]
                target[parts[-1]] = value
                with self.assertRaises(ValueError):
                    BENCHMARK.validate_report(report)


if __name__ == "__main__":
    unittest.main()
