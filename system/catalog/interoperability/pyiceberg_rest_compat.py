#!/usr/bin/env python3
"""Black-box PyIceberg interoperability checks for the Catalog Worker adapter."""

from __future__ import annotations

import json
import subprocess
import sys
import unittest
import uuid
from pathlib import Path

import requests
import pyiceberg
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.schema import Schema
from pyiceberg.table.update import SetPropertiesUpdate
from pyiceberg.types import IntegerType, NestedField, StringType


EXPECTED_SITE_PACKAGES = Path(
    "/Users/jfbrown/code/cascadelabs/.venv/lib/python3.13/site-packages"
).resolve()
SERVER = Path(__file__).with_name("adapter-server.mjs")


class RestCompatibilityTest(unittest.TestCase):
    """Runs standard PyIceberg catalog calls against a real HTTP adapter."""

    server: subprocess.Popen[str]
    base_uri: str
    catalog: RestCatalog
    schema: Schema

    @classmethod
    def setUpClass(cls) -> None:
        """Start the Worker/DO adapter and open it through PyIceberg."""
        cls.server = subprocess.Popen(
            ["node", str(SERVER)],
            cwd=SERVER.parents[3],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        assert cls.server.stdout is not None
        ready_line = cls.server.stdout.readline()
        if not ready_line:
            stderr = cls.server.stderr.read() if cls.server.stderr is not None else ""
            raise RuntimeError(f"Catalog adapter exited before readiness: {stderr}")
        try:
            ready = json.loads(ready_line)
        except json.JSONDecodeError as error:
            stderr = cls.server.stderr.read() if cls.server.stderr is not None else ""
            raise RuntimeError(f"Catalog adapter emitted invalid readiness: {ready_line!r}\n{stderr}") from error
        if ready.get("ready") is not True or not isinstance(ready.get("url"), str):
            raise RuntimeError(f"Catalog adapter readiness is invalid: {ready!r}")

        cls.base_uri = ready["url"]
        cls.catalog = RestCatalog(
            "verglas-interop",
            uri=cls.base_uri,
            warehouse="warehouse",
            token="interop-test-token",
        )
        cls.schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="payload", field_type=StringType(), required=False),
        )

    @classmethod
    def tearDownClass(cls) -> None:
        """Stop the adapter and report its stderr only when it failed."""
        catalog = getattr(cls, "catalog", None)
        if catalog is not None:
            catalog.close()
        server = getattr(cls, "server", None)
        if server is None:
            return
        server.terminate()
        try:
            _, stderr = server.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            _, stderr = server.communicate()
        if server.returncode not in (0, -15, 143) and stderr:
            print(f"\nCatalog adapter stderr:\n{stderr}", file=sys.stderr)

    def setUp(self) -> None:
        """Create isolated multipart namespaces for this test method."""
        suffix = self._testMethodName.removeprefix("test_")
        self.namespace = (f"interop_{suffix}_{uuid.uuid4().hex[:8]}",)
        self.child_namespace = (*self.namespace, "raw")
        self.table_identifier = (*self.child_namespace, "events")
        self.catalog.create_namespace(self.namespace, {"owner": "pyiceberg"})
        self.catalog.create_namespace(self.child_namespace, {"format": "iceberg"})

    def test_config_and_multipart_namespaces(self) -> None:
        """Verify config discovery, multipart paths, properties, and listings."""
        config_response = requests.get(f"{self.base_uri}/v1/config", timeout=10)
        self.assertEqual(config_response.status_code, 200, config_response.text)
        config = config_response.json()
        self.assertEqual(config["defaults"], {"warehouse": "warehouse"})
        self.assertEqual(config["overrides"], {})
        self.assertIn("GET /v1/{prefix}/namespaces/{namespace}", config["endpoints"])

        self.assertIn(self.namespace, self.catalog.list_namespaces())
        self.assertIn(self.child_namespace, self.catalog.list_namespaces(self.namespace))
        self.assertEqual(
            self.catalog.load_namespace_properties(self.child_namespace),
            {"format": "iceberg"},
        )
        summary = self.catalog.update_namespace_properties(
            self.child_namespace,
            removals={"format", "missing"},
            updates={"owner": "compatibility"},
        )
        self.assertEqual(summary.removed, ["format"])
        self.assertEqual(summary.updated, ["owner"])
        self.assertEqual(summary.missing, ["missing"])
        self.assertEqual(
            self.catalog.load_namespace_properties(self.child_namespace),
            {"owner": "compatibility"},
        )

    def test_create_load_and_list_table(self) -> None:
        """Create, load, and list a table through PyIceberg's REST client."""
        location = f"s3://lake/{'/'.join(self.table_identifier)}"
        table = self.catalog.create_table(
            self.table_identifier,
            self.schema,
            location=location,
            properties={"owner": "pyiceberg"},
        )
        self.assertEqual(table.name(), self.table_identifier)
        self.assertEqual(table.metadata_location, f"{location}/metadata/00000-interop.json")
        self.assertEqual(table.metadata.location, location)
        self.assertEqual(table.schema().schema_id, 0)
        self.assertEqual([field.name for field in table.schema().fields], ["id", "payload"])

        loaded = self.catalog.load_table(self.table_identifier)
        self.assertEqual(loaded.metadata_location, table.metadata_location)
        self.assertEqual(loaded.metadata.table_uuid, table.metadata.table_uuid)
        self.assertEqual(
            self.catalog.list_tables(self.child_namespace),
            [self.table_identifier],
        )

    def test_standard_errors(self) -> None:
        """Require standard Iceberg exception mapping and error envelopes."""
        with self.assertRaises(NamespaceAlreadyExistsError):
            self.catalog.create_namespace(self.namespace)

        missing_namespace = (*self.namespace, "missing")
        with self.assertRaises(NoSuchNamespaceError):
            self.catalog.list_tables(missing_namespace)

        missing_table = (*self.child_namespace, "missing")
        with self.assertRaises(NoSuchTableError):
            self.catalog.load_table(missing_table)

        self.catalog.create_table(self.table_identifier, self.schema)
        with self.assertRaises(TableAlreadyExistsError):
            self.catalog.create_table(self.table_identifier, self.schema)
        with self.assertRaises(NamespaceNotEmptyError):
            self.catalog.drop_namespace(self.child_namespace)

        response = requests.get(
            f"{self.base_uri}/v1/namespaces/{'%1F'.join(missing_table[:-1])}/tables/missing",
            timeout=10,
        )
        self.assertEqual(response.status_code, 404, response.text)
        self.assertEqual(
            response.json(),
            {
                "error": {
                    "message": "table does not exist",
                    "type": "NoSuchTableException",
                    "code": 404,
                }
            },
        )

    def test_standard_table_commit(self) -> None:
        """Exercise the standard REST table-commit endpoint when advertised."""
        table = self.catalog.create_table(self.table_identifier, self.schema)
        response = self.catalog.commit_table(
            table,
            requirements=(),
            updates=(SetPropertiesUpdate(updates={"compatibility": "pyiceberg"}),),
        )
        self.assertEqual(response.metadata.properties["compatibility"], "pyiceberg")


if __name__ == "__main__":
    if EXPECTED_SITE_PACKAGES not in Path(pyiceberg.__file__).resolve().parents:
        raise RuntimeError(
            f"PyIceberg was imported from {pyiceberg.__file__}, not {EXPECTED_SITE_PACKAGES}"
        )
    print(f"PyIceberg {pyiceberg.__version__} from {pyiceberg.__file__}")
    result = unittest.main(verbosity=2, exit=False)
    raise SystemExit(0 if result.result.wasSuccessful() else 1)
