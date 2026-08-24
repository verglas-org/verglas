"""Authoring helpers for Python components targeting Verglas Durable Objects.

The public module contains only plain Python records and host-capability wrappers.
The componentize-py-specific export adapter lives in :mod:`verglas_worker._component`
and is loaded by the build entry module after generated WIT bindings exist.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Protocol, TypeAlias


BytesLike: TypeAlias = bytes | bytearray | memoryview
Value: TypeAlias = str | BytesLike


def _require_uint(value: int, name: str, maximum: int) -> int:
    """Validate one WIT unsigned integer before crossing the component boundary."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value < 0 or value > maximum:
        raise ValueError(f"{name} must be between 0 and {maximum}")
    return value


class WorkerError(Exception):
    """Reports a handler or host capability failure to the DO host."""


@dataclass(frozen=True)
class Request:
    """Represents one HTTP request delivered to a Worker handler."""

    method: str
    uri: str
    headers: list[tuple[str, str]]
    body: bytes

    def __post_init__(self) -> None:
        """Normalize mutable WIT list input without changing its contents."""
        object.__setattr__(self, "headers", list(self.headers))
        object.__setattr__(self, "body", bytes(self.body))


@dataclass(frozen=True)
class Response:
    """Represents one HTTP response returned by a Worker handler."""

    status: int
    headers: list[tuple[str, str]]
    body: bytes

    def __post_init__(self) -> None:
        """Normalize mutable WIT list input without changing its contents."""
        _require_uint(self.status, "Response.status", 0xFFFF)
        object.__setattr__(self, "headers", list(self.headers))
        object.__setattr__(self, "body", bytes(self.body))


class _StorageImports(Protocol):
    """Describes generated storage import functions used by the wrapper."""

    def get(self, key: str) -> bytes | None:
        """Read one key from the event snapshot."""

    def put(self, key: str, value: bytes) -> None:
        """Stage one key write in the current event."""

    def delete(self, key: str) -> bool:
        """Stage one key deletion in the current event."""

    def list(self, prefix: str, limit: int) -> list[str]:
        """List keys matching one prefix."""

    def sql_rows(self, statement: str) -> str:
        """Execute SQL and return its JSON row encoding."""

    def set_alarm(self, epoch_millis: int) -> None:
        """Stage the DO alarm deadline."""

    def get_alarm(self) -> int | None:
        """Read the currently armed DO alarm."""

    def delete_alarm(self) -> None:
        """Stage removal of the DO alarm."""


class _SocketImports(Protocol):
    """Describes generated socket import functions used by the wrapper."""

    def send(self, socket: int, message: bytes) -> None:
        """Stage one WebSocket message."""

    def close(self, socket: int, code: int, reason: str) -> None:
        """Stage one WebSocket close."""

    def set_attachment(self, socket: int, value: bytes) -> None:
        """Stage one WebSocket attachment write."""

    def get_attachment(self, socket: int) -> bytes | None:
        """Read one WebSocket attachment."""

    def attached(self) -> list[int]:
        """List every currently attached WebSocket."""


def _as_bytes(value: Value) -> bytes:
    """Encode text as UTF-8 and copy every accepted bytes-like value."""
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    raise TypeError("value must be str, bytes, bytearray, or memoryview")


def _host_error_message(value: object) -> str | None:
    """Extract a generated componentize-py Err payload when one is present."""
    payload = getattr(value, "value", None)
    message = getattr(payload, "message", None)
    if isinstance(message, str) and type(value).__name__ == "Err":
        return message
    return None


def _call_host(function: Any, *args: Any) -> Any:
    """Call one generated import and map its handler-error to WorkerError."""
    try:
        result = function(*args)
    except Exception as error:
        message = _host_error_message(error)
        if message is None:
            raise
        raise WorkerError(message) from error

    message = _host_error_message(result)
    if message is not None:
        raise WorkerError(message)
    if type(result).__name__ == "Ok" and hasattr(result, "value"):
        return result.value
    return result


class Storage:
    """Wrap the generated transactional storage imports."""

    def __init__(self, imports: _StorageImports) -> None:
        """Bind this wrapper to one generated storage import module."""
        self._imports = imports

    def get(self, key: str) -> bytes | None:
        """Read a key as bytes, or return ``None`` when it is absent."""
        value = _call_host(self._imports.get, key)
        return None if value is None else bytes(value)

    def get_bytes(self, key: str) -> bytes | None:
        """Read a key as bytes using an explicit bytes-oriented name."""
        return self.get(key)

    def get_text(self, key: str, encoding: str = "utf-8") -> str | None:
        """Read a key and decode it with the requested text encoding."""
        value = self.get(key)
        return None if value is None else value.decode(encoding)

    def put(self, key: str, value: Value) -> None:
        """Stage a key write, encoding text values as UTF-8."""
        _call_host(self._imports.put, key, _as_bytes(value))

    def put_bytes(self, key: str, value: BytesLike) -> None:
        """Stage a key write from bytes without text conversion."""
        self.put(key, value)

    def put_text(self, key: str, value: str) -> None:
        """Stage a UTF-8 text key write using an explicit text name."""
        self.put(key, value)

    def delete(self, key: str) -> bool:
        """Stage a key deletion and report whether a value existed."""
        return bool(_call_host(self._imports.delete, key))

    def list(self, prefix: str = "", limit: int = 1000) -> list[str]:
        """List at most ``limit`` keys with the requested prefix."""
        limit = _require_uint(limit, "Storage.list limit", 0xFFFFFFFF)
        return list(_call_host(self._imports.list, prefix, limit))


class Sockets:
    """Wrap the generated commit-gated WebSocket imports."""

    def __init__(self, imports: _SocketImports) -> None:
        """Bind this wrapper to one generated socket import module."""
        self._imports = imports

    def send(self, socket: int, message: Value) -> None:
        """Stage a binary or UTF-8 text message for one socket."""
        socket = _require_uint(socket, "WebSocket id", 0xFFFFFFFFFFFFFFFF)
        _call_host(self._imports.send, socket, _as_bytes(message))

    def send_text(self, socket: int, message: str) -> None:
        """Stage a UTF-8 text message using an explicit text name."""
        self.send(socket, message)

    def close(self, socket: int, code: int = 1000, reason: str = "") -> None:
        """Stage a WebSocket close with its code and reason."""
        socket = _require_uint(socket, "WebSocket id", 0xFFFFFFFFFFFFFFFF)
        code = _require_uint(code, "WebSocket close code", 0xFFFF)
        _call_host(self._imports.close, socket, code, reason)

    def set_attachment(self, socket: int, value: Value) -> None:
        """Persist a binary or UTF-8 text attachment for one socket."""
        socket = _require_uint(socket, "WebSocket id", 0xFFFFFFFFFFFFFFFF)
        _call_host(self._imports.set_attachment, socket, _as_bytes(value))

    def get_attachment(self, socket: int) -> bytes | None:
        """Read a socket attachment as bytes, when one exists."""
        socket = _require_uint(socket, "WebSocket id", 0xFFFFFFFFFFFFFFFF)
        value = _call_host(self._imports.get_attachment, socket)
        return None if value is None else bytes(value)

    def get_attachment_text(
        self, socket: int, encoding: str = "utf-8"
    ) -> str | None:
        """Read a socket attachment and decode it as text."""
        value = self.get_attachment(socket)
        return None if value is None else value.decode(encoding)

    def attached(self) -> list[int]:
        """Return every socket currently attached to this Durable Object."""
        return [
            _require_uint(socket, "WebSocket id", 0xFFFFFFFFFFFFFFFF)
            for socket in _call_host(self._imports.attached)
        ]


class Environment:
    """Expose the storage, SQL, alarm, and socket capabilities to a Worker."""

    def __init__(self, storage_imports: _StorageImports, socket_imports: _SocketImports):
        """Create one environment backed by generated host import modules."""
        self.storage = Storage(storage_imports)
        self.sockets = Sockets(socket_imports)
        self._storage_imports = storage_imports

    def sql(self, statement: str) -> list[dict[str, Any]]:
        """Execute SQL and decode the WIT ``sql-rows`` JSON array."""
        encoded = _call_host(self._storage_imports.sql_rows, statement)
        try:
            rows = json.loads(encoded)
        except (TypeError, ValueError) as error:
            raise WorkerError("storage.sql_rows returned invalid JSON") from error
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise WorkerError("storage.sql_rows returned a non-object row array")
        return rows

    def set_alarm(self, epoch_millis: int) -> None:
        """Stage the Durable Object alarm at an epoch-millisecond deadline."""
        epoch_millis = _require_uint(
            epoch_millis, "alarm epoch milliseconds", 0xFFFFFFFFFFFFFFFF
        )
        _call_host(self._storage_imports.set_alarm, epoch_millis)

    def get_alarm(self) -> int | None:
        """Read the currently armed Durable Object alarm, if any."""
        value = _call_host(self._storage_imports.get_alarm)
        return None if value is None else _require_uint(
            value, "alarm epoch milliseconds", 0xFFFFFFFFFFFFFFFF
        )

    def delete_alarm(self) -> None:
        """Stage deletion of the Durable Object alarm."""
        _call_host(self._storage_imports.delete_alarm)


__all__ = [
    "Environment",
    "Request",
    "Response",
    "Storage",
    "Sockets",
    "WorkerError",
]
