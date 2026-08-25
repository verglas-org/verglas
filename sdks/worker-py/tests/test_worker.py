"""Tests for the Cloudflare-shaped Python Worker authoring surface."""

from __future__ import annotations

import asyncio
import hashlib
import unittest

from workers import DurableObjectState, Request, Response, ServiceBinding, WebSocketPair
from workers._runtime import Environment, Storage, event_scope, leave_event_scope


class StorageImports:
    """A deterministic host double for the storage capability."""

    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.sql_statements: list[str] = []
        self.alarm: int | None = None

    def get(self, key: str) -> bytes | None:
        return self.values.get(key)

    def put(self, key: str, value: bytes) -> None:
        self.values[key] = bytes(value)

    def delete(self, key: str) -> bool:
        return self.values.pop(key, None) is not None

    def list(self, prefix: str, limit: int) -> list[str]:
        return sorted(key for key in self.values if key.startswith(prefix))[:limit]

    def sql_rows(self, statement: str) -> str:
        self.sql_statements.append(statement)
        return '[{"count": 3, "greeting": "hello"}]'

    def set_alarm(self, epoch_millis: int) -> None:
        self.alarm = epoch_millis

    def get_alarm(self) -> int | None:
        return self.alarm

    def delete_alarm(self) -> None:
        self.alarm = None


class SocketImports:
    """A deterministic host double for commit-gated sockets."""

    def __init__(self) -> None:
        self.sent: list[tuple[int, bytes]] = []
        self.attachments: dict[int, bytes] = {}
        self.closed: list[tuple[int, int, str]] = []
        self.connected = [7]

    def send(self, socket: int, message: bytes) -> None:
        self.sent.append((socket, bytes(message)))

    def close(self, socket: int, code: int, reason: str) -> None:
        self.closed.append((socket, code, reason))

    def set_attachment(self, socket: int, value: bytes) -> None:
        self.attachments[socket] = bytes(value)

    def get_attachment(self, socket: int) -> bytes | None:
        return self.attachments.get(socket)

    def attached(self) -> list[int]:
        return list(self.connected)


class BindingImports:
    """A deterministic host double for flattened Durable Object fetches."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, object]] = []

    def do_fetch(self, binding: str, object: str, request: object) -> object:
        self.calls.append((binding, object, request))
        return Response("from-do", status=201)


class WorkerSurfaceTests(unittest.TestCase):
    """Exercise Cloudflare request, response, storage, SQL, and binding APIs."""

    def setUp(self) -> None:
        """Construct deterministic host capabilities for each test."""
        self.storage_imports = StorageImports()
        self.socket_imports = SocketImports()
        self.binding_imports = BindingImports()
        self.storage = Storage(self.storage_imports, self.socket_imports)
        self.env = Environment(
            self.storage,
            self.socket_imports,
            self.binding_imports,
            {"GREETING": "hello"},
            [{"name": "COUNTER", "class_name": "Counter"}],
        )

    def test_storage_round_trips_deterministic_structured_values(self) -> None:
        """Storage preserves supported values through canonical byte encoding."""
        value = {"count": 3, "payload": b"\x00\xff", "items": [True, None]}

        async def exercise() -> object:
            await self.storage.put("state", value)
            return await self.storage.get("state")

        self.assertEqual(asyncio.run(exercise()), value)
        first = self.storage_imports.values["state"]
        self.assertEqual(
            first,
            b'{"t":"object","v":[["count",{"t":"int","v":"3"}],'
            b'["items",{"t":"array","v":[{"t":"bool","v":true},{"t":"null"}]}],'
            b'["payload",{"t":"bytes","v":"AP8="}]]}',
        )
        asyncio.run(self.storage.put("state", value))
        self.assertEqual(self.storage_imports.values["state"], first)

        async def bulk() -> object:
            await self.storage.put({"a": 1, "b": 2})
            return await self.storage.get(["a", "b"])

        self.assertEqual(asyncio.run(bulk()), {"a": 1, "b": 2})

    def test_storage_list_returns_cloudflare_style_mapping(self) -> None:
        """Storage list exposes keys and decoded values at one snapshot."""

        async def exercise() -> object:
            await self.storage.put("b", 2)
            await self.storage.put("a", 1)
            return await self.storage.list()

        listed = asyncio.run(exercise())
        self.assertEqual(list(listed.keys()), ["a", "b"])
        self.assertEqual(listed["a"], 1)
        self.assertEqual(list(listed.values()), [1, 2])

    def test_sql_exec_cursor_supports_one_and_to_array(self) -> None:
        """SQL executes through sql-rows and exposes row attributes."""
        cursor = self.storage.sql.exec("SELECT greeting, count FROM counter")

        self.assertEqual(cursor.one().greeting, "hello")
        second = self.storage.sql.exec("SELECT greeting, count FROM counter")
        self.assertEqual(second.one().count, 3)
        self.assertEqual(self.storage_imports.sql_statements, [
            "SELECT greeting, count FROM counter",
            "SELECT greeting, count FROM counter",
        ])

    def test_binding_id_from_name_and_fetch_uses_sha256_name(self) -> None:
        """A binding stub hashes names locally and routes the original name."""

        async def exercise() -> Response:
            identifier = self.env.COUNTER.id_from_name("global")
            self.assertIs(identifier, self.env.COUNTER.id_from_name("global"))
            self.assertEqual(identifier.to_string(), hashlib.sha256(b"global").hexdigest())
            self.assertTrue(identifier.equals(self.env.COUNTER.id_from_string(identifier.to_string())))
            return await self.env.COUNTER.get(identifier).fetch(Request("GET", "/"))

        response = asyncio.run(exercise())
        self.assertEqual(response.status, 201)
        binding, object_name, request = self.binding_imports.calls[0]
        self.assertEqual((binding, object_name), ("COUNTER", "global"))
        self.assertEqual(request.uri, "/")

    def test_service_binding_fetch_uses_configured_service_target(self) -> None:
        """A service binding forwards directly with its configured target."""
        environment = Environment(
            self.storage,
            self.socket_imports,
            self.binding_imports,
            {},
            [{"name": "COUNTER", "class_name": "Counter"}],
            services=[{"binding": "CATALOG", "service": "catalog-service"}],
        )

        async def exercise() -> Response:
            return await environment.CATALOG.fetch(Request("POST", "/commit"))

        response = asyncio.run(exercise())
        self.assertEqual(response.status, 201)
        self.assertIsInstance(environment.CATALOG, ServiceBinding)
        self.assertFalse(hasattr(environment.CATALOG, "id_from_name"))
        self.assertFalse(hasattr(environment.CATALOG, "get"))
        binding, object_name, request = self.binding_imports.calls[0]
        self.assertEqual((binding, object_name), ("CATALOG", "catalog-service"))
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.uri, "/commit")

    def test_service_binding_requires_a_request_or_url(self) -> None:
        """Direct service fetch rejects values outside the Request surface."""
        environment = Environment(
            self.storage,
            self.socket_imports,
            self.binding_imports,
            {},
            [],
            services=[{"binding": "CATALOG", "service": "catalog-service"}],
        )

        async def exercise() -> None:
            await environment.CATALOG.fetch(object())

        with self.assertRaises(TypeError):
            asyncio.run(exercise())
        self.assertEqual(self.binding_imports.calls, [])

    def test_alarms_use_transactional_storage_imports(self) -> None:
        """Alarm methods stage, read, and clear one deadline."""

        async def exercise() -> int | None:
            await self.storage.set_alarm(1234)
            self.assertEqual(await self.storage.get_alarm(), 1234)
            await self.storage.delete_alarm()
            return await self.storage.get_alarm()

        self.assertIsNone(asyncio.run(exercise()))

    def test_websocket_accept_is_carried_by_response(self) -> None:
        """Accepting the server end marks the response with the pending id."""
        state = DurableObjectState(self.storage, self.socket_imports, pending_ws=7)
        token = event_scope(state, 7, None)
        try:
            client, server = WebSocketPair.new()
            state.accept_websocket(server)
            response = Response(None, status=101, web_socket=client)
        finally:
            leave_event_scope(token)

        self.assertEqual(response.accept_ws, 7)
        server.send("hello")
        server.serialize_attachment({"session": 7})
        self.assertEqual(server.deserialize_attachment(), {"session": 7})
        self.assertEqual(self.socket_imports.sent, [(7, b"hello")])

    def test_request_and_response_are_fetch_records(self) -> None:
        """Request and Response retain URL, headers, body, and status fields."""
        request = Request("POST", "/incr", {"x-test": "yes"}, b"body", ws=7)
        response = Response.json({"ok": True}, status=200)

        self.assertEqual(request.url, "/incr")
        self.assertEqual(request.headers.get("X-Test"), "yes")
        self.assertEqual(request.ws, 7)
        self.assertEqual(response.body, b'{"ok":true}')
        self.assertEqual(response.status, 200)


if __name__ == "__main__":
    unittest.main()
