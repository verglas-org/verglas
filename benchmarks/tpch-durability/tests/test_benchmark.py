"""Frozen acceptance tests for the SF10 cluster durability report."""

import copy
import importlib.util
import os
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
            "s3_ingress_uploads": [20, 20, 20, 20],
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
            ("iceberg.s3_ingress_uploads", [80, 0, 0, 0]),
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

    def test_sql_checksum_is_invariant_to_legal_tie_order(self) -> None:
        """Rows with an unspecified tie order produce one canonical digest."""

        class Cursor:
            def __init__(self, rows: list[tuple[int, str]]) -> None:
                self.description = [("id",), ("value",)]
                self.rows = rows

            def execute(self, _query: str) -> "Cursor":
                return self

            def fetchmany(self, _size: int) -> list[tuple[int, str]]:
                rows, self.rows = self.rows, []
                return rows

        first = Cursor([(2, "same"), (1, "same")])
        second = Cursor([(1, "same"), (2, "same")])
        self.assertEqual(
            BENCHMARK.sql_checksum(first, "select"),
            BENCHMARK.sql_checksum(second, "select"),
        )

    def test_topology_uses_current_prebuilt_engine_images(self) -> None:
        """The benchmark must not build the deleted catalog-service tree."""
        compose = (MODULE.parent / "compose.yaml").read_text()
        self.assertNotIn("verglas-catalog-service", compose)
        self.assertNotIn("Dockerfile.catalog", compose)
        self.assertIn("TPCH_DURABILITY_CACHE_IMAGE", compose)
        self.assertIn("TPCH_DURABILITY_LAKEKEEPER_IMAGE", compose)
        self.assertIn('command: ["mc alias set origin ', compose)
        self.assertNotIn("'$${TPCH_DURABILITY", compose)
        self.assertIn("VERGLAS_STORAGE_BUCKET", compose)
        self.assertNotIn("VERGLAS_MANAGED_STORAGE_BUCKET", compose)
        self.assertNotIn("wget -q", compose)
        self.assertIn("curl --fail --silent", compose)
        start = (MODULE.parents[2] / "deploy" / "cache-node" / "start.sh").read_text()
        self.assertNotIn("[wal_archive]", start)
        self.assertIn("[catalog_archive]", start)
        self.assertIn("/admin/neon/timeline-bindings", compose + MODULE.read_text())
        self.assertIn('"sts-enabled":false', compose)
        self.assertIn('LAKEKEEPER__ENABLE_AWS_SYSTEM_CREDENTIALS: "true"', compose)
        self.assertIn('LAKEKEEPER__S3_ENABLE_DIRECT_SYSTEM_CREDENTIALS: "true"', compose)
        self.assertIn('LAKEKEEPER__AUTHZ_BACKEND: verglas', compose)
        self.assertIn('LAKEKEEPER__VERGLAS__ENDPOINT', compose)
        self.assertIn('TPCH_DURABILITY_LAKEKEEPER_POLICY_TOKEN_FILE', compose)
        self.assertIn('VERGLAS_METADATA_S3_ENDPOINTS: http://cache-0:8333,http://cache-1:8333,http://cache-2:8333,http://cache-3:8333', compose)

    def test_iceberg_catalog_uses_a_real_data_plane_session(self) -> None:
        """REST catalog calls must carry the caller credential minted by Verglas access."""
        old_token = os.environ.get("TPCH_DURABILITY_CALLER_TOKEN")
        try:
            os.environ["TPCH_DURABILITY_CALLER_TOKEN"] = "data-plane-session"
            self.assertEqual(
                BENCHMARK.iceberg_catalog_properties()["token"],
                "data-plane-session",
            )
        finally:
            if old_token is None:
                os.environ.pop("TPCH_DURABILITY_CALLER_TOKEN", None)
            else:
                os.environ["TPCH_DURABILITY_CALLER_TOKEN"] = old_token

    def test_lakekeeper_readiness_uses_the_public_health_route(self) -> None:
        """Startup must not mistake the authenticated catalog config route for health."""
        self.assertEqual(
            BENCHMARK.LAKEKEEPER_HEALTH_URL,
            "http://127.0.0.1:18181/health",
        )

    def test_survivor_health_uses_compose_state_not_host_port_forwarding(self) -> None:
        """Docker Desktop port proxies must not replace container health evidence."""

        class Compose:
            def run(self, *args: str, timeout: float | None = None):
                self.args = args
                return type(
                    "Result",
                    (),
                    {
                        "stdout": '[{"Service":"cache-1","State":"running","Health":"healthy"}]',
                    },
                )()

        compose = Compose()
        BENCHMARK.wait_compose_healthy(compose, "cache-1", 0.1)
        self.assertEqual(compose.args, ("ps", "--format", "json"))

    def test_killed_voters_restart_together_before_health_is_required(self) -> None:
        """Recovery must restore peer DNS for every killed voter before waiting on quorum health."""

        class Compose:
            def __init__(self) -> None:
                self.calls: list[tuple[str, ...]] = []

            def run(self, *args: str, timeout: float | None = None):
                self.calls.append(args)
                if args[:2] == ("ps", "--format"):
                    return type(
                        "Result",
                        (),
                        {
                            "stdout": '[{"Service":"cache-0","State":"running","Health":"healthy"},'
                            '{"Service":"cache-1","State":"running","Health":"healthy"},'
                            '{"Service":"cache-2","State":"running","Health":"healthy"}]',
                        },
                    )()
                return type("Result", (), {"stdout": ""})()

        compose = Compose()
        BENCHMARK.restart_voters(compose, ("cache-0", "cache-1", "cache-2"), 0.1)
        self.assertEqual(compose.calls[0], ("start", "cache-0", "cache-1", "cache-2"))
        self.assertNotIn(("start", "cache-0"), compose.calls)

    def test_wal_archive_binding_uses_container_local_admin_after_restart(self) -> None:
        """Docker Desktop's stale host proxy must not replace the restarted cache control path."""

        class Compose:
            def __init__(self) -> None:
                self.calls: list[tuple[str, ...]] = []

            def run(self, *args: str, timeout: float | None = None):
                self.calls.append(args)
                return type("Result", (), {"stdout": "204", "stderr": ""})()

        old_token = os.environ.get("TPCH_DURABILITY_CATALOG_EVENT_TOKEN")
        try:
            os.environ["TPCH_DURABILITY_CATALOG_EVENT_TOKEN"] = "control-token"
            compose = Compose()
            BENCHMARK.bind_wal_archive(compose, "cache-0")
            self.assertEqual(compose.calls[0][:4], ("exec", "-T", "cache-0", "curl"))
            self.assertNotIn("18334", " ".join(compose.calls[0]))
        finally:
            if old_token is None:
                os.environ.pop("TPCH_DURABILITY_CATALOG_EVENT_TOKEN", None)
            else:
                os.environ["TPCH_DURABILITY_CATALOG_EVENT_TOKEN"] = old_token

    def test_pyiceberg_receives_the_rest_catalog_root(self) -> None:
        """PyIceberg appends `/v1`; its configured URI must stop at `/catalog`."""
        self.assertEqual(
            BENCHMARK.LAKEKEEPER_CATALOG_URI,
            "http://127.0.0.1:18181/catalog",
        )

    def test_iceberg_credentials_follow_the_ephemeral_stack_credentials(self) -> None:
        """PyIceberg must authenticate with the same fresh credentials as the ring."""
        old_key = os.environ.get("TPCH_DURABILITY_S3_ACCESS_KEY")
        old_secret = os.environ.get("TPCH_DURABILITY_S3_SECRET_KEY")
        try:
            os.environ["TPCH_DURABILITY_S3_ACCESS_KEY"] = "test-access"
            os.environ["TPCH_DURABILITY_S3_SECRET_KEY"] = "test-secret"
            self.assertEqual(
                BENCHMARK.iceberg_s3_properties(),
                {
                    "s3.endpoint": "http://127.0.0.1:18333",
                    "s3.access-key-id": "test-access",
                    "s3.secret-access-key": "test-secret",
                    "s3.region": "us-east-1",
                    "s3.path-style-access": "true",
                },
            )
        finally:
            if old_key is None:
                os.environ.pop("TPCH_DURABILITY_S3_ACCESS_KEY", None)
            else:
                os.environ["TPCH_DURABILITY_S3_ACCESS_KEY"] = old_key
            if old_secret is None:
                os.environ.pop("TPCH_DURABILITY_S3_SECRET_KEY", None)
            else:
                os.environ["TPCH_DURABILITY_S3_SECRET_KEY"] = old_secret

    def test_failure_mutation_changes_catalog_without_changing_tpch_rows(self) -> None:
        """The one-node-down write must not corrupt the checksum reference dataset."""

        class Catalog:
            def __init__(self) -> None:
                self.created: list[tuple[str, ...]] = []

            def create_namespace(self, namespace: tuple[str, ...]) -> None:
                self.created.append(namespace)

        catalog = Catalog()
        BENCHMARK.perform_catalog_failure_mutation(catalog)
        self.assertEqual(catalog.created, [("committed_with_one_voter_down",)])

    def test_parquet_imports_are_round_robined_across_all_s3_ingresses(self) -> None:
        """SF10 Parquet uploads must exercise every ingress instead of pinning cache-0."""

        class S3:
            def __init__(self) -> None:
                self.uploads: list[tuple[str, str, str]] = []

            def upload_file(self, source: str, bucket: str, key: str) -> None:
                self.uploads.append((source, bucket, key))

        clients = [S3() for _ in range(4)]
        client = BENCHMARK.RoundRobinS3(clients)
        paths = BENCHMARK.upload_iceberg_files(
            client,
            "orders",
            [pathlib.Path(f"/tmp/chunk-{index}.parquet") for index in range(1, 6)],
        )
        self.assertEqual(
            [client.uploads for client in clients],
            [
                [
                    ("/tmp/chunk-1.parquet", "verglas", "warehouse/import/orders/chunk-1.parquet"),
                    ("/tmp/chunk-5.parquet", "verglas", "warehouse/import/orders/chunk-5.parquet"),
                ],
                [("/tmp/chunk-2.parquet", "verglas", "warehouse/import/orders/chunk-2.parquet")],
                [("/tmp/chunk-3.parquet", "verglas", "warehouse/import/orders/chunk-3.parquet")],
                [("/tmp/chunk-4.parquet", "verglas", "warehouse/import/orders/chunk-4.parquet")],
            ],
        )
        self.assertEqual(len(paths), 5)

    def test_catalog_publication_waits_for_every_ingress_to_see_uploaded_data(self) -> None:
        """The Iceberg commit barrier must cover cross-ingress visibility, not only the writer."""

        class S3:
            def __init__(self, misses: int) -> None:
                self.misses = misses
                self.calls = 0

            def head_object(self, *, Bucket: str, Key: str):
                self.calls += 1
                if self.calls <= self.misses:
                    raise RuntimeError("not visible")
                return {"ContentLength": 1}

        clients = [S3(0), S3(1), S3(2), S3(3)]
        BENCHMARK.wait_for_global_s3_visibility(
            clients,
            ["s3://verglas/warehouse/import/orders/chunk.parquet"],
            1.0,
        )
        self.assertEqual([client.calls for client in clients], [1, 2, 3, 4])

    def test_external_parquet_schema_is_created_before_name_mapping(self) -> None:
        """Table creation must assign IDs before add-files derives its name mapping."""
        source = MODULE.read_text()
        self.assertNotIn("from pyiceberg.io.pyarrow import pyarrow_to_schema", source)
        self.assertIn("schema = pq.ParquetFile(files[0]).schema_arrow", source)

    def test_duckdb_reads_the_committed_iceberg_metadata_through_verglas(self) -> None:
        """Post-failure queries use the committed metadata pointer and cache S3 endpoint."""
        old_key = os.environ.get("TPCH_DURABILITY_S3_ACCESS_KEY")
        old_secret = os.environ.get("TPCH_DURABILITY_S3_SECRET_KEY")
        try:
            os.environ["TPCH_DURABILITY_S3_ACCESS_KEY"] = "key"
            os.environ["TPCH_DURABILITY_S3_SECRET_KEY"] = "secret"
            setup = BENCHMARK.duckdb_verglas_s3_setup("127.0.0.1:18353")
            scan = BENCHMARK.duckdb_iceberg_scan("s3://verglas/warehouse/table/metadata/v1.json")
            self.assertIn("ENDPOINT '127.0.0.1:18353'", setup)
            self.assertIn("USE_SSL false", setup)
            self.assertEqual(
                scan,
                "SELECT * FROM iceberg_scan('s3://verglas/warehouse/table/metadata/v1.json')",
            )
        finally:
            if old_key is None:
                os.environ.pop("TPCH_DURABILITY_S3_ACCESS_KEY", None)
            else:
                os.environ["TPCH_DURABILITY_S3_ACCESS_KEY"] = old_key
            if old_secret is None:
                os.environ.pop("TPCH_DURABILITY_S3_SECRET_KEY", None)
            else:
                os.environ["TPCH_DURABILITY_S3_SECRET_KEY"] = old_secret


if __name__ == "__main__":
    unittest.main()
