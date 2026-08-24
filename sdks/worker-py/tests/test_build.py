"""Tests for the Python Durable Object manifest and build contract."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from build import ManifestError, load_manifest, parse_jsonc  # noqa: E402


class ManifestTests(unittest.TestCase):
    """Exercise the accepted and rejected Wrangler subset."""

    def setUp(self) -> None:
        """Create a minimal valid Python Worker project for each test."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.project = Path(self.temp_dir.name)
        (self.project / "counter.py").write_text(
            "async def fetch(request, env): pass\n", encoding="utf-8"
        )
        (self.project / "wrangler.jsonc").write_text(
            """
            {
              // JSONC comments are accepted by the Wrangler subset.
              "name": "counter",
              "main": "counter.py",
              "durable_objects": {
                "bindings": [
                  {"name": "COUNTER", "class_name": "Counter"}
                ]
              },
            }
            """,
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        """Remove the temporary project."""
        self.temp_dir.cleanup()

    def test_load_manifest_accepts_jsonc_subset(self) -> None:
        """A valid Wrangler subset returns normalized binding records."""
        manifest = load_manifest(self.project)

        self.assertEqual(manifest.name, "counter")
        self.assertEqual(manifest.main, self.project / "counter.py")
        self.assertEqual(
            manifest.bindings, [{"name": "COUNTER", "class_name": "Counter"}]
        )

    def test_load_manifest_rejects_unknown_top_level_key(self) -> None:
        """Unknown top-level fields fail instead of being silently ignored."""
        data = parse_jsonc(
            (self.project / "wrangler.jsonc").read_text(encoding="utf-8")
        )
        data["compatibility_date"] = "2026-01-01"
        (self.project / "wrangler.jsonc").write_text(json.dumps(data), encoding="utf-8")

        with self.assertRaisesRegex(ManifestError, "compatibility_date"):
            load_manifest(self.project)

    def test_load_manifest_rejects_non_python_main(self) -> None:
        """The Python pipeline accepts only a .py main module."""
        (self.project / "wrangler.jsonc").write_text(
            '{"name":"counter","main":"counter.js","durable_objects":{"bindings":[]}}',
            encoding="utf-8",
        )

        with self.assertRaisesRegex(ManifestError, "main"):
            load_manifest(self.project)

    def test_load_manifest_rejects_malformed_binding(self) -> None:
        """Each durable-object binding must name its object class."""
        (self.project / "wrangler.jsonc").write_text(
            '{"name":"counter","main":"counter.py","durable_objects":{"bindings":[{"name":"COUNTER"}]}}',
            encoding="utf-8",
        )

        with self.assertRaisesRegex(ManifestError, "class_name"):
            load_manifest(self.project)


if __name__ == "__main__":
    unittest.main()
