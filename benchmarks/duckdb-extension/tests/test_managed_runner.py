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
        self.assertIn("ENDPOINT ", sql)
        self.assertIn("ACCESS_DELEGATION_MODE 'none'", sql)
        self.assertIn("USE managed.benchmark", sql)
        self.assertIn("r2.cloudflarestorage.com", sql)
        self.assertNotIn("verglas-cache", sql)
        self.assertNotIn("verglas_query(", sql)

    def test_managed_bootstrap_commits_catalog_metadata_with_explicit_r2_io(self):
        """Use catalog commits while signing object writes with the bound R2 keypair."""
        config = RUNNER.ManagedConfig(
            "https://catalog.example/v1", "token", "https://r2.example",
            "access", "secret", "warehouse", "benchmark", "events",
        )
        properties = config.iceberg_properties()
        self.assertEqual(properties["uri"], "https://catalog.example/v1")
        self.assertEqual(properties["header.X-Iceberg-Access-Delegation"], "")
        self.assertEqual(properties["s3.endpoint"], "https://r2.example")
        self.assertEqual(properties["s3.access-key-id"], "access")
        self.assertEqual(properties["s3.secret-access-key"], "secret")
        self.assertEqual(properties["s3.region"], "auto")
        self.assertEqual(properties["s3.path-style-access"], "true")
        self.assertEqual(properties["py-io-impl"], "pyiceberg.io.pyarrow.PyArrowFileIO")

    def test_catalog_remote_signer_cannot_replace_explicit_r2_file_io(self):
        """Rebind loaded tables because REST response properties outrank client properties."""
        class Table:
            io = "catalog-remote-signer"

        table = Table()
        observed = {}

        def factory(properties):
            observed.update(properties)
            return "explicit-signed-r2"

        returned = RUNNER.bind_explicit_storage_io(table, factory, {"s3.access-key-id": "access"})
        self.assertIs(returned, table)
        self.assertEqual(table.io, "explicit-signed-r2")
        self.assertEqual(observed, {"s3.access-key-id": "access"})

    def test_extension_quack_loads_real_artifact_and_calls_worker(self):
        """Require the extension-hosting Quack server to use the compiled table function."""
        sql = "\n".join(RUNNER.extension_quack_setup("/artifacts/verglas.duckdb_extension"))
        self.assertIn("LOAD '/artifacts/verglas.duckdb_extension'", sql)
        self.assertIn("quack_serve", sql)
        query = RUNNER.product_sql("quack_verglas_extension", "scan")
        self.assertIn("verglas_query(", query)

    def test_quack_measurement_selects_the_declared_product_transport(self):
        """Do not accidentally run direct SQL against the extension-only server."""
        self.assertEqual(RUNNER.measured_sql("quack_direct", "scan"),
                         RUNNER.canonical_sql("scan"))
        self.assertIn("verglas_query(",
                      RUNNER.measured_sql("quack_verglas_extension", "scan"))

    def test_duckdb_servers_apply_equal_operator_limits(self):
        """Bound both Quack product servers to the Query Worker's operator budget."""
        class Connection:
            def __init__(self):
                self.statements = []

            def execute(self, statement):
                self.statements.append(statement)

        connection = Connection()
        RUNNER.apply_duckdb_limits(connection)
        self.assertEqual(connection.statements, ["SET threads = 1", "SET memory_limit = '512 MiB'"])

    def test_quack_measurements_use_one_persistent_remote_connection(self):
        """Warm repetitions must not create and leak a fresh server session per query."""
        statements = RUNNER.quack_client_setup("direct", "benchmark-token")
        self.assertEqual(len(statements), 1)
        self.assertIn("ATTACH 'quack:direct:9000' AS benchmark_remote", statements[0])
        self.assertNotIn("TYPE QUACK", statements[0])
        self.assertIn("DISABLE_SSL true", statements[0])

    def test_direct_server_exposes_fixed_catalog_snapshot_then_detaches_catalog(self):
        """Quack introspection sees a stable view without recursively walking REST metadata."""
        statements = RUNNER.direct_snapshot_view_setup(
            "benchmark", "events", "s3://bucket/table/metadata/00001.json"
        )
        sql = "\n".join(statements)
        self.assertIn("CREATE SCHEMA benchmark", sql)
        self.assertIn("iceberg_scan('s3://bucket/table/metadata/00001.json')", sql)
        self.assertIn("CREATE VIEW benchmark.events", sql)
        self.assertEqual(statements[-1], "DETACH managed")

    def test_product_workloads_are_same_semantics_for_all_paths(self):
        """Change only the transport wrapper around each canonical workload."""
        for workload in RUNNER.WORKLOADS:
            canonical = RUNNER.canonical_sql(workload)
            self.assertEqual(RUNNER.product_sql("verglas_query_worker", workload), canonical)
            self.assertEqual(RUNNER.product_sql("quack_direct", workload), canonical)
            self.assertIn(canonical.replace("'", "''"),
                          RUNNER.product_sql("quack_verglas_extension", workload))

    def test_workload_aggregates_have_transport_stable_bigint_types(self):
        """Avoid engine-specific SUM widening from changing the result schema."""
        for workload in RUNNER.WORKLOADS:
            sql = RUNNER.canonical_sql(workload).lower()
            self.assertNotIn("sum(", sql.replace("cast(sum(", ""))

    def test_dataset_builder_requires_managed_catalog_and_out_of_core_size(self):
        """Reject raw-file and in-memory-sized bootstrap declarations."""
        with self.assertRaises(ValueError):
            RUNNER.validate_bootstrap(rows=1, worker_memory_mib=512,
                                      catalog_engine="filesystem")
        with self.assertRaises(ValueError):
            RUNNER.validate_bootstrap(rows=1000, worker_memory_mib=512,
                                      catalog_engine="verglas-lakekeeper")
        with self.assertRaises(ValueError):
            RUNNER.validate_dataset_bytes(2 * 1024**3 - 1, worker_memory_mib=512)


if __name__ == "__main__":
    unittest.main()
