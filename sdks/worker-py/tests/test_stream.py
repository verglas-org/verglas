"""Tests for Python Pipeline Stream bindings and their host protocol."""

from __future__ import annotations

import asyncio
import json
import math
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from workers import (  # noqa: E402
    DurableObjectNamespace,
    PipelineBinding,
    Response,
    WorkerError,
)
from workers._runtime import Environment, Storage  # noqa: E402


STREAM_MAX_REQUEST_BYTES = 5 * 1024 * 1024


class StreamHost:
    """Capture exact flattened bindings requests for one test."""

    def __init__(self, status: int = 202, error: Exception | None = None) -> None:
        """Configure the response or host exception returned by ``do_fetch``."""
        self.status = status
        self.error = error
        self.calls: list[tuple[str, str, object]] = []

    def do_fetch(self, binding: str, object: str, request: object) -> object:
        """Record one host call and return the configured response."""
        self.calls.append((binding, object, request))
        if self.error is not None:
            raise self.error
        return Response(b"accepted", status=self.status)


class StreamBindingTests(unittest.TestCase):
    """Exercise Stream.send validation and the exact WIT request shape."""

    def test_send_posts_compact_utf8_json_to_named_stream_and_accepts_2xx(self) -> None:
        """A successful send waits for a durable 2xx response."""
        host = StreamHost(status=202)
        stream = PipelineBinding("STREAM", "stream-id", host)

        asyncio.run(stream.send([{"event": "é"}, {"count": 2}]))

        self.assertEqual(len(host.calls), 1)
        binding, object_name, request = host.calls[0]
        self.assertEqual((binding, object_name), ("STREAM", "stream-id"))
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.uri, "https://verglas.internal/stream/append")
        self.assertEqual(request.headers, [("content-type", "application/json")])
        self.assertEqual(
            request.body,
            json.dumps(
                [{"event": "é"}, {"count": 2}],
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8"),
        )
        self.assertIsNone(request.ws)

    def test_durable_object_send_stages_records_through_storage(self) -> None:
        """A Durable Object Stream binding records logical rows in its event."""
        calls: list[tuple[str, str, str]] = []

        class StagedHost:
            def stream_send(self, binding: str, stream: str, records: str) -> None:
                calls.append((binding, stream, records))

        stream = PipelineBinding("STREAM", "stream-id", StagedHost(), transactional=True)
        asyncio.run(stream.send([{"event": "staged"}]))
        self.assertEqual(calls, [("STREAM", "stream-id", '[{"event":"staged"}]')])

    def test_durable_object_environment_uses_transactional_stream_transport(self) -> None:
        """A Durable Object environment routes its manifest Stream through storage."""
        calls: list[tuple[str, str, str]] = []

        class StorageHost:
            def stream_send(self, binding: str, stream: str, records: str) -> None:
                calls.append((binding, stream, records))

        storage_host = StorageHost()
        environment = Environment(
            Storage(storage_host),
            None,
            StreamHost(),
            {},
            [],
            [{"binding": "STREAM", "stream": "stream-id"}],
            transactional_streams=True,
        )
        asyncio.run(environment.STREAM.send([{"event": "handler"}]))
        self.assertEqual(calls, [("STREAM", "stream-id", '[{"event":"handler"}]')])

    def test_send_rejects_non_arrays_and_non_json_values_before_host_call(self) -> None:
        """Input validation rejects values JSON.stringify would not accept."""
        host = StreamHost()
        stream = PipelineBinding("STREAM", "stream-id", host)
        cyclic: list[object] = []
        cyclic.append(cyclic)

        for records in (
            {"event": "not-an-array"},
            [b"bytes"],
            [{"values": {1: "not json"}}],
            [[cyclic]],
            [[math.nan]],
        ):
            with self.subTest(records=type(records).__name__):
                with self.assertRaises(TypeError):
                    asyncio.run(stream.send(records))

        self.assertEqual(host.calls, [])

    def test_send_accepts_exact_5_mib_body_and_rejects_one_byte_over(self) -> None:
        """The encoded UTF-8 array limit is inclusive at exactly 5 MiB."""
        host = StreamHost()
        stream = PipelineBinding("STREAM", "stream-id", host)
        prefix = b'[{"payload":"'
        suffix = b'"}]'
        records = [{"payload": "x" * (STREAM_MAX_REQUEST_BYTES - len(prefix) - len(suffix))}]

        asyncio.run(stream.send(records))
        self.assertEqual(len(host.calls[0][2].body), STREAM_MAX_REQUEST_BYTES)

        oversized = [{"payload": records[0]["payload"] + "x"}]
        with self.assertRaisesRegex(ValueError, "5 MiB"):
            asyncio.run(stream.send(oversized))
        self.assertEqual(len(host.calls), 1)

    def test_send_rejects_non_2xx_and_host_errors_without_fallback(self) -> None:
        """Non-success responses and host failures propagate without retry paths."""
        rejected_host = StreamHost(status=503)
        rejected = PipelineBinding("STREAM", "stream-id", rejected_host)
        with self.assertRaisesRegex(WorkerError, "503"):
            asyncio.run(rejected.send([{"value": 1}]))
        self.assertEqual(len(rejected_host.calls), 1)

        host_failure = StreamHost(error=RuntimeError("host unavailable"))
        failed = PipelineBinding("STREAM", "stream-id", host_failure)
        with self.assertRaisesRegex(RuntimeError, "host unavailable"):
            asyncio.run(failed.send([{"value": 1}]))
        self.assertEqual(len(host_failure.calls), 1)

    def test_pipeline_entries_are_separate_from_durable_object_namespaces(self) -> None:
        """Environment injection exposes only send on pipelines, not DO methods."""
        host = StreamHost()
        environment = Environment(
            None,
            None,
            host,
            {},
            [{"name": "COUNTER", "class_name": "Counter"}],
            [{"binding": "STREAM", "stream": "stream-id"}],
        )

        self.assertIsInstance(environment.COUNTER, DurableObjectNamespace)
        self.assertIsInstance(environment.STREAM, PipelineBinding)
        self.assertTrue(callable(environment.STREAM.send))
        self.assertFalse(hasattr(environment.STREAM, "get"))
        self.assertFalse(hasattr(environment.STREAM, "id_from_name"))


if __name__ == "__main__":
    unittest.main()
