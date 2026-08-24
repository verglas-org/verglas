"""Tests for the Python Worker authoring surface."""

from __future__ import annotations

import unittest

from verglas_worker import Environment, Request, Response, WorkerError


class StorageImports:
    """A deterministic host double for storage calls."""

    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.sql_statements: list[str] = []

    def get(self, key: str) -> bytes | None:
        return self.values.get(key)

    def put(self, key: str, value: bytes) -> None:
        self.values[key] = value

    def delete(self, key: str) -> bool:
        return self.values.pop(key, None) is not None

    def list(self, prefix: str, limit: int) -> list[str]:
        return [key for key in self.values if key.startswith(prefix)][:limit]

    def sql_rows(self, statement: str) -> str:
        self.sql_statements.append(statement)
        return '[{"count": 3}]'

    def set_alarm(self, epoch_millis: int) -> None:
        self.alarm = epoch_millis

    def get_alarm(self) -> int | None:
        return getattr(self, "alarm", None)

    def delete_alarm(self) -> None:
        self.alarm = None


class SocketImports:
    """A deterministic host double for socket calls."""

    def __init__(self) -> None:
        self.sent: list[tuple[int, bytes]] = []

    def send(self, socket: int, message: bytes) -> None:
        self.sent.append((socket, message))

    def close(self, socket: int, code: int, reason: str) -> None:
        pass

    def set_attachment(self, socket: int, value: bytes) -> None:
        pass

    def get_attachment(self, socket: int) -> bytes | None:
        return None

    def attached(self) -> list[int]:
        return []


class WorkerSurfaceTests(unittest.TestCase):
    """Exercise bytes, text, SQL, and record conversions."""

    def setUp(self) -> None:
        """Construct host doubles and the public environment."""
        self.storage = StorageImports()
        self.sockets = SocketImports()
        self.env = Environment(self.storage, self.sockets)

    def test_storage_bytes_and_text_helpers(self) -> None:
        """Storage accepts UTF-8 text and exposes explicit byte/text reads."""
        self.env.storage.put("greeting", "hello")

        self.assertEqual(self.env.storage.get_bytes("greeting"), b"hello")
        self.assertEqual(self.env.storage.get_text("greeting"), "hello")

    def test_sql_uses_sql_rows_and_decodes_json(self) -> None:
        """SQL calls the sql-rows WIT verb and returns row dictionaries."""
        self.assertEqual(self.env.sql("SELECT count FROM counter"), [{"count": 3}])
        self.assertEqual(
            self.storage.sql_statements, ["SELECT count FROM counter"]
        )

    def test_socket_send_text_encodes_utf8(self) -> None:
        """Socket text helpers encode exactly one UTF-8 message."""
        self.env.sockets.send_text(7, "hello")

        self.assertEqual(self.sockets.sent, [(7, b"hello")])

    def test_request_and_response_are_wit_records(self) -> None:
        """Request and Response carry the WIT record fields unchanged."""
        request = Request("POST", "/incr", [("x-test", "yes")], b"body")
        response = Response(200, [("content-type", "text/plain")], b"ok")

        self.assertEqual(request.body, b"body")
        self.assertEqual(response.status, 200)

    def test_worker_error_is_exception(self) -> None:
        """WorkerError gives handlers a stable error type to raise."""
        with self.assertRaises(WorkerError):
            raise WorkerError("bad handler")


if __name__ == "__main__":
    unittest.main()
