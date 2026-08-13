"""Contract tests for the repository-owned managed full-stack runner."""

import importlib.util
import pathlib
import unittest


PATH = pathlib.Path(__file__).parents[1] / "managed_runner.py"
SPEC = importlib.util.spec_from_file_location("managed_runner", PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load managed runner")
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


class ManagedRunnerTests(unittest.TestCase):
    """Keep the three product paths distinct and executable."""

    def test_direct_quack_uses_catalog_but_bypasses_verglas_storage(self):
        """Configure direct Quack with Lakekeeper metadata and direct R2 data I/O."""
        sql = "\n".join(RUNNER.direct_quack_setup("token", "access", "secret"))
        self.assertIn("TYPE ICEBERG", sql)
        self.assertIn("ACCESS_DELEGATION_MODE 'none'", sql)
        self.assertIn("r2.cloudflarestorage.com", sql)
        self.assertNotIn("verglas-cache", sql)
        self.assertNotIn("verglas_query(", sql)

    def test_extension_quack_loads_real_artifact_and_calls_worker(self):
        """Require the extension-hosting Quack server to use the compiled table function."""
        sql = "\n".join(RUNNER.extension_quack_setup("/artifacts/verglas.duckdb_extension"))
        self.assertIn("LOAD '/artifacts/verglas.duckdb_extension'", sql)
        self.assertIn("quack_serve", sql)
        query = RUNNER.product_sql("quack_verglas_extension", "scan")
        self.assertIn("verglas_query(", query)

    def test_product_workloads_are_same_semantics_for_all_paths(self):
        """Change only the transport wrapper around each canonical workload."""
        for workload in RUNNER.WORKLOADS:
            canonical = RUNNER.canonical_sql(workload)
            self.assertEqual(RUNNER.product_sql("verglas_query_worker", workload), canonical)
            self.assertEqual(RUNNER.product_sql("quack_direct", workload), canonical)
            self.assertIn(canonical.replace("'", "''"),
                          RUNNER.product_sql("quack_verglas_extension", workload))

    def test_dataset_builder_requires_managed_catalog_and_out_of_core_size(self):
        """Reject raw-file and in-memory-sized bootstrap declarations."""
        with self.assertRaises(ValueError):
            RUNNER.validate_bootstrap(rows=1, worker_memory_mib=512,
                                      catalog_engine="filesystem")
        with self.assertRaises(ValueError):
            RUNNER.validate_bootstrap(rows=1000, worker_memory_mib=512,
                                      catalog_engine="verglas-lakekeeper")


if __name__ == "__main__":
    unittest.main()
