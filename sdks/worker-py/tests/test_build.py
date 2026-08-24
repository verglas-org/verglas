"""Tests for the Python Durable Object Wrangler subset and build contract."""

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from build import (  # noqa: E402
    ManifestError,
    _update_gateway_manifest,
    build_project,
    load_manifest,
    parse_jsonc,
)


class ManifestTests(unittest.TestCase):
    """Exercise the accepted and rejected Wrangler subset."""

    def setUp(self) -> None:
        """Create a minimal valid Cloudflare-style Python project."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.project = Path(self.temp_dir.name)
        (self.project / "counter.py").write_text(
            "from workers import WorkerEntrypoint\n"
            "class Default(WorkerEntrypoint):\n"
            "    async def fetch(self, request):\n"
            "        return None\n",
            encoding="utf-8",
        )
        (self.project / "wrangler.jsonc").write_text(
            """
            {
              // JSONC comments are accepted by the Wrangler subset.
              "name": "counter",
              "main": "counter.py",
              "compatibility_date": "2025-05-14",
              "compatibility_flags": ["experimental:test"],
              "durable_objects": {
                "bindings": [
                  {"name": "COUNTER", "class_name": "Counter"}
                ]
              },
              "migrations": [
                {"tag": "v1", "new_sqlite_classes": ["Counter"]}
              ],
              "vars": {"GREETING": "hello", "LIMIT": 3},
            }
            """,
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        """Remove the temporary project."""
        self.temp_dir.cleanup()

    def test_load_manifest_accepts_cloudflare_subset(self) -> None:
        """Compatibility, migration, vars, and binding fields are preserved."""
        manifest = load_manifest(self.project)

        self.assertEqual(manifest.name, "counter")
        self.assertEqual(manifest.main, self.project / "counter.py")
        self.assertEqual(
            manifest.bindings, [{"name": "COUNTER", "class_name": "Counter"}]
        )
        self.assertEqual(manifest.compatibility_date, "2025-05-14")
        self.assertEqual(manifest.compatibility_flags, ["experimental:test"])
        self.assertEqual(
            manifest.migrations,
            [{"tag": "v1", "new_sqlite_classes": ["Counter"]}],
        )
        self.assertEqual(manifest.vars, {"GREETING": "hello", "LIMIT": 3})

    def test_load_manifest_accepts_json_manifest_name(self) -> None:
        """The same subset is accepted from wrangler.json as well as JSONC."""
        (self.project / "wrangler.jsonc").rename(self.project / "wrangler.json")

        manifest = load_manifest(self.project)

        self.assertEqual(manifest.name, "counter")

    def test_load_manifest_rejects_unknown_top_level_key(self) -> None:
        """Unknown top-level fields fail instead of being silently ignored."""
        data = parse_jsonc(
            (self.project / "wrangler.jsonc").read_text(encoding="utf-8")
        )
        data["service_bindings"] = []
        (self.project / "wrangler.jsonc").write_text(json.dumps(data), encoding="utf-8")

        with self.assertRaisesRegex(ManifestError, "service_bindings"):
            load_manifest(self.project)

    def test_load_manifest_rejects_unknown_migration_key(self) -> None:
        """Unsupported migration kinds fail at their own manifest path."""
        data = parse_jsonc(
            (self.project / "wrangler.jsonc").read_text(encoding="utf-8")
        )
        data["migrations"][0]["renamed_classes"] = ["Counter"]
        (self.project / "wrangler.jsonc").write_text(json.dumps(data), encoding="utf-8")

        with self.assertRaisesRegex(ManifestError, "renamed_classes"):
            load_manifest(self.project)

    def test_load_manifest_rejects_non_python_main(self) -> None:
        """The Python pipeline accepts only a .py main module."""
        data = parse_jsonc(
            (self.project / "wrangler.jsonc").read_text(encoding="utf-8")
        )
        data["main"] = "counter.js"
        (self.project / "wrangler.jsonc").write_text(json.dumps(data), encoding="utf-8")

        with self.assertRaisesRegex(ManifestError, "main"):
            load_manifest(self.project)

    def test_load_manifest_rejects_malformed_binding(self) -> None:
        """Each durable-object binding must name its object class."""
        data = parse_jsonc(
            (self.project / "wrangler.jsonc").read_text(encoding="utf-8")
        )
        data["durable_objects"]["bindings"] = [{"name": "COUNTER"}]
        (self.project / "wrangler.jsonc").write_text(json.dumps(data), encoding="utf-8")

        with self.assertRaisesRegex(ManifestError, "class_name"):
            load_manifest(self.project)

    def test_build_writes_digest_artifact_and_full_deployment_manifest(self) -> None:
        """The output manifest carries the accepted Wrangler configuration."""
        output = self.project / "out"
        component_bytes = b"component bytes"

        def fake_componentize(project, manifest, work_dir, component_path):
            del project, manifest, work_dir
            component_path.write_bytes(component_bytes)

        with mock.patch("build._run_componentize", side_effect=fake_componentize):
            result = build_project(self.project, output)

        digest = hashlib.sha256(component_bytes).hexdigest()
        self.assertEqual(result.component_digest, digest)
        self.assertEqual((output / f"{digest}.wasm").read_bytes(), component_bytes)
        self.assertEqual(
            json.loads(result.manifest_path.read_text(encoding="utf-8")),
            {
                "name": "counter",
                "main": "counter.py",
                "compatibility_date": "2025-05-14",
                "compatibility_flags": ["experimental:test"],
                "durable_objects": {
                    "bindings": [{"name": "COUNTER", "class_name": "Counter"}]
                },
                "migrations": [
                    {"tag": "v1", "new_sqlite_classes": ["Counter"]}
                ],
                "vars": {"GREETING": "hello", "LIMIT": 3},
                "component_digest": digest,
            },
        )

    def test_gateway_manifest_tracks_digest_and_output_directory(self) -> None:
        """A checked-in gateway points at the bytes emitted by the builder."""
        gateway = self.project / "gateway.json"
        gateway.write_text(
            json.dumps(
                {
                    "name": "counter",
                    "main": "counter.py",
                    "component_digest": "0" * 64,
                    "component_dir": "/stale/output",
                    "data_root": "/persistent/data",
                }
            ),
            encoding="utf-8",
        )
        output = self.project / "out"
        output.mkdir()
        digest = "a" * 64
        _update_gateway_manifest(gateway, output, digest)
        updated = json.loads(gateway.read_text(encoding="utf-8"))
        self.assertEqual(updated["component_digest"], digest)
        self.assertEqual(updated["component_dir"], str(output))
        self.assertEqual(updated["data_root"], "/persistent/data")


if __name__ == "__main__":
    unittest.main()
