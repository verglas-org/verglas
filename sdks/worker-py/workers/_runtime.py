"""Cloudflare-shaped Python Workers runtime objects over the component imports.

The module contains the public fetch, Durable Object, storage, SQL, binding, and
WebSocket objects used by ``workers``.  The component adapter supplies generated
WIT imports at wake time; application code never imports this module directly.
"""

from __future__ import annotations

import base64
import contextvars
import hashlib
import inspect
import json
import math
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Protocol, TypeAlias


BytesLike: TypeAlias = bytes | bytearray | memoryview
StructuredValue: TypeAlias = None | bool | int | float | str | bytes | list[Any] | tuple[Any, ...] | dict[str, Any]


class WorkerError(Exception):
    """Reports an application or host capability failure."""


class _StorageImports(Protocol):
    """Describes the generated storage imports supplied by componentize-py."""

    def get(self, key: str) -> bytes | None:
        """Read one encoded value from the current transaction snapshot."""

    def put(self, key: str, value: bytes) -> None:
        """Stage one encoded value in the current transaction."""

    def delete(self, key: str) -> bool:
        """Stage deletion of one key and report whether it existed."""

    def list(self, prefix: str, limit: int) -> list[str]:
        """List keys at the current transaction snapshot fence."""

    def sql_rows(self, statement: str) -> str:
        """Execute SQL and return its JSON row array."""

    def stream_send(self, stream_binding: str, stream_name: str, records: str) -> None:
        """Stage a JSON record array for one configured Stream."""

    def set_alarm(self, epoch_millis: int) -> None:
        """Stage the Durable Object alarm deadline."""

    def get_alarm(self) -> int | None:
        """Read the current Durable Object alarm deadline."""

    def delete_alarm(self) -> None:
        """Stage removal of the Durable Object alarm."""


class _SocketImports(Protocol):
    """Describes the generated WebSocket imports supplied by componentize-py."""

    def send(self, socket: int, message: bytes) -> None:
        """Stage a message for commit-gated delivery."""

    def close(self, socket: int, code: int, reason: str) -> None:
        """Stage a WebSocket close operation."""

    def set_attachment(self, socket: int, value: bytes) -> None:
        """Stage a serialized WebSocket attachment."""

    def get_attachment(self, socket: int) -> bytes | None:
        """Read a serialized WebSocket attachment."""

    def attached(self) -> list[int]:
        """List sockets attached to the current Durable Object."""


class _BindingImports(Protocol):
    """Describes the generated flattened Durable Object binding import."""

    def do_fetch(self, binding: str, object: str, request: Any) -> Any:
        """Forward one request to the named Durable Object instance."""

    def stream_send(self, binding: str, stream: str, records: str) -> Any:
        """Stage one Stream record array in the current Durable Object event."""


_wit_types: Any = None


@dataclass
class _EventState:
    """Tracks the current component event for WebSocket response acceptance."""

    state: DurableObjectState | None
    pending_ws: int | None
    execution: ExecutionContext | None


_current_event: contextvars.ContextVar[_EventState | None] = contextvars.ContextVar(
    "workers_current_event", default=None
)


def set_wit_types(types_module: Any) -> None:
    """Install generated WIT records used when a binding stub calls the host."""
    global _wit_types
    _wit_types = types_module


def event_scope(
    state: DurableObjectState | None,
    pending_ws: int | None,
    execution: ExecutionContext | None,
) -> contextvars.Token[_EventState | None]:
    """Enter one event scope and expose its pending WebSocket identity."""
    return _current_event.set(_EventState(state, pending_ws, execution))


def leave_event_scope(token: contextvars.Token[_EventState | None]) -> None:
    """Leave an event scope without retaining request-local WebSocket state."""
    _current_event.reset(token)


def _require_uint(value: int, name: str, maximum: int) -> int:
    """Validate an unsigned WIT integer before passing it to the host."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value < 0 or value > maximum:
        raise ValueError(f"{name} must be between 0 and {maximum}")
    return value


def _host_error_message(value: object) -> str | None:
    """Extract a componentize-py ``Err`` payload when one is present."""
    payload = getattr(value, "value", None)
    message = getattr(payload, "message", None)
    if isinstance(message, str) and type(value).__name__ == "Err":
        return message
    return None


def _call_host(function: Any, *args: Any) -> Any:
    """Call one generated import and convert its handler error to WorkerError."""
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


def _as_bytes(value: str | BytesLike) -> bytes:
    """Encode text as UTF-8 and copy every accepted bytes-like value."""
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    raise TypeError("value must be str, bytes, bytearray, or memoryview")


def _encode_value(value: StructuredValue) -> bytes:
    """Encode a supported storage value into deterministic tagged JSON bytes.

    Cloudflare storage uses JavaScript structured clone, while the WIT surface
    carries only ``list<u8>``.  Tagged canonical JSON preserves the Python
    scalar/container types used by Workers examples, including bytes, but it
    deliberately rejects JavaScript-only values such as ``undefined``, ``Date``,
    ``Map``, and object identity graphs.
    """

    def encode(item: Any) -> dict[str, Any]:
        if item is None:
            return {"t": "null"}
        if isinstance(item, bool):
            return {"t": "bool", "v": item}
        if isinstance(item, int):
            return {"t": "int", "v": str(item)}
        if isinstance(item, float):
            if not math.isfinite(item):
                raise TypeError("storage values cannot contain non-finite floats")
            return {"t": "float", "v": repr(item)}
        if isinstance(item, str):
            return {"t": "str", "v": item}
        if isinstance(item, (bytes, bytearray, memoryview)):
            return {
                "t": "bytes",
                "v": base64.b64encode(bytes(item)).decode("ascii"),
            }
        if isinstance(item, (list, tuple)):
            return {"t": "array", "v": [encode(value) for value in item]}
        if isinstance(item, dict):
            if any(not isinstance(key, str) for key in item):
                raise TypeError("storage object keys must be strings")
            entries = [
                [key, encode(item[key])] for key in sorted(item)
            ]
            return {"t": "object", "v": entries}
        raise TypeError(f"unsupported storage value: {type(item).__name__}")

    return json.dumps(
        encode(value), ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def _decode_value(encoded: bytes) -> StructuredValue:
    """Decode and validate one tagged storage value."""
    try:
        raw = json.loads(bytes(encoded).decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as error:
        raise WorkerError("storage value is not valid tagged JSON") from error

    def decode(item: Any) -> StructuredValue:
        if not isinstance(item, dict) or not isinstance(item.get("t"), str):
            raise WorkerError("storage value has an invalid tag")
        tag = item["t"]
        if tag == "null" and set(item) == {"t"}:
            return None
        if tag == "bool" and set(item) == {"t", "v"} and isinstance(item["v"], bool):
            return item["v"]
        if tag == "int" and set(item) == {"t", "v"} and isinstance(item["v"], str):
            try:
                return int(item["v"], 10)
            except ValueError as error:
                raise WorkerError("storage integer is invalid") from error
        if tag == "float" and set(item) == {"t", "v"} and isinstance(item["v"], str):
            try:
                value = float(item["v"])
            except ValueError as error:
                raise WorkerError("storage float is invalid") from error
            if not math.isfinite(value):
                raise WorkerError("storage float is non-finite")
            return value
        if tag == "str" and set(item) == {"t", "v"} and isinstance(item["v"], str):
            return item["v"]
        if tag == "bytes" and set(item) == {"t", "v"} and isinstance(item["v"], str):
            try:
                return base64.b64decode(item["v"], validate=True)
            except ValueError as error:
                raise WorkerError("storage bytes are invalid base64") from error
        if tag == "array" and set(item) == {"t", "v"} and isinstance(item["v"], list):
            return [decode(value) for value in item["v"]]
        if tag == "object" and set(item) == {"t", "v"} and isinstance(item["v"], list):
            result: dict[str, StructuredValue] = {}
            previous: str | None = None
            for entry in item["v"]:
                if (
                    not isinstance(entry, list)
                    or len(entry) != 2
                    or not isinstance(entry[0], str)
                    or (previous is not None and entry[0] <= previous)
                ):
                    raise WorkerError("storage object entries are not canonical")
                previous = entry[0]
                result[entry[0]] = decode(entry[1])
            return result
        raise WorkerError("storage value has an invalid tagged payload")

    return decode(raw)


class Headers(Mapping[str, str]):
    """Case-insensitive HTTP headers with deterministic insertion order."""

    def __init__(self, values: Mapping[str, str] | list[tuple[str, str]] | Headers | None = None):
        """Build headers from a mapping or a sequence of header pairs."""
        self._values: list[tuple[str, str]] = []
        if values is not None:
            pairs = values.items() if isinstance(values, Mapping) else values
            for name, value in pairs:
                self._set(name, value)

    def _set(self, name: str, value: str) -> None:
        """Set one header while retaining the first spelling and position."""
        if not isinstance(name, str) or not isinstance(value, str):
            raise TypeError("header names and values must be strings")
        lowered = name.lower()
        for index, (existing, _) in enumerate(self._values):
            if existing.lower() == lowered:
                self._values[index] = (existing, value)
                return
        self._values.append((name, value))

    def get(self, name: str, default: str | None = None) -> str | None:
        """Return one header value using case-insensitive lookup."""
        lowered = name.lower()
        for existing, value in self._values:
            if existing.lower() == lowered:
                return value
        return default

    def __getitem__(self, name: str) -> str:
        """Read one header or raise KeyError when it is absent."""
        value = self.get(name)
        if value is None:
            raise KeyError(name)
        return value

    def __iter__(self) -> Iterator[str]:
        """Iterate over header names in insertion order."""
        return (name for name, _ in self._values)

    def __len__(self) -> int:
        """Return the number of distinct header names."""
        return len(self._values)

    def items(self) -> list[tuple[str, str]]:
        """Return header pairs in insertion order."""
        return list(self._values)

    def __contains__(self, name: object) -> bool:
        """Report whether a header name is present case-insensitively."""
        return isinstance(name, str) and self.get(name) is not None


class Request:
    """A Python representation of the Workers ``Request`` object."""

    def __init__(
        self,
        input: str | Request,
        url: str | None = None,
        headers: Mapping[str, str] | list[tuple[str, str]] | None = None,
        body: bytes | bytearray | memoryview = b"",
        ws: int | None = None,
        *,
        method: str | None = None,
    ):
        """Construct a request from a URL or explicit WIT request fields."""
        if isinstance(input, Request):
            if any(value is not None for value in (url, headers, ws)) or body != b"" or method is not None:
                raise ValueError("options cannot be supplied when cloning a Request")
            self.method = input.method
            self.url = input.url
            self.headers = Headers(input.headers.items())
            self._body = bytes(input._body)
            self.ws = input.ws
            self._body_used = False
            return

        if method is not None:
            if url is not None:
                raise TypeError("url must be omitted when method is supplied by keyword")
            request_method = method
            request_url = input
        elif url is None:
            request_method = "GET"
            request_url = input
        else:
            request_method = input
            request_url = url
        if not isinstance(request_method, str) or not isinstance(request_url, str):
            raise TypeError("request method and URL must be strings")
        self.method = request_method
        self.url = request_url
        self.headers = Headers(headers)
        self._body = bytes(body)
        self.ws = None if ws is None else _require_uint(ws, "Request.ws", 0xFFFFFFFFFFFFFFFF)
        self._body_used = False

    @property
    def body(self) -> bytes:
        """Return the immutable request body bytes used by this component ABI."""
        return self._body

    async def bytes(self) -> bytes:
        """Consume and return the request body as bytes."""
        self._check_body()
        return self._body

    async def text(self) -> str:
        """Consume and decode the request body as UTF-8 text."""
        return (await self.bytes()).decode("utf-8")

    async def json(self, **kwargs: Any) -> Any:
        """Consume and decode the request body as JSON."""
        return json.loads(await self.text(), **kwargs)

    def clone(self) -> Request:
        """Clone a request while leaving the original body usable."""
        return Request(self.method, self.url, self.headers.items(), self._body, self.ws)

    def _check_body(self) -> None:
        """Enforce the one-consumer body rule for request convenience methods."""
        if self._body_used:
            raise WorkerError("request body has already been consumed")
        self._body_used = True


class Response:
    """A Python representation of the Workers ``Response`` object."""

    def __init__(
        self,
        body: str | bytes | bytearray | memoryview | None = None,
        status: int = 200,
        status_text: str = "",
        headers: Mapping[str, str] | list[tuple[str, str]] | None = None,
        web_socket: WebSocket | None = None,
    ):
        """Construct a response with optional WebSocket client acceptance."""
        self.status = _require_uint(status, "Response.status", 0xFFFF)
        if not isinstance(status_text, str):
            raise TypeError("Response.status_text must be a string")
        self.status_text = status_text
        self.headers = Headers(headers)
        if body is None:
            self.body = b""
        elif isinstance(body, str):
            self.body = body.encode("utf-8")
        elif isinstance(body, (bytes, bytearray, memoryview)):
            self.body = bytes(body)
        else:
            raise TypeError("Response body must be text, bytes, or None")
        self.web_socket = web_socket
        self.accept_ws = web_socket.accept_ws if web_socket is not None else None

    def __repr__(self) -> str:
        """Render a compact response diagnostic."""
        return f"Response(status={self.status}, body={self.body!r})"

    @staticmethod
    def from_json(
        value: Any,
        status: int = 200,
        status_text: str = "",
        headers: Mapping[str, str] | list[tuple[str, str]] | None = None,
    ) -> Response:
        """Encode one JSON value and add the JSON content type when absent."""
        response_headers = Headers(headers)
        if "content-type" not in response_headers:
            response_headers._set("content-type", "application/json")
        return Response(
            json.dumps(value, ensure_ascii=False, separators=(",", ":")),
            status=status,
            status_text=status_text,
            headers=response_headers.items(),
        )

    def json(self_or_value: Response | Any, **kwargs: Any) -> Any:
        """Parse an instance body or construct a JSON response from the class.

        Cloudflare's Python examples use ``Response.json(value)`` while the
        underlying Fetch object also exposes ``response.json()``.  One Python
        descriptor supports both spellings without a second public API name.
        """
        if isinstance(self_or_value, Response):
            return json.loads(self_or_value.body.decode("utf-8"), **kwargs)
        return Response.from_json(self_or_value, **kwargs)

    async def text(self) -> str:
        """Return the response body as UTF-8 text."""
        return self.body.decode("utf-8")

    async def bytes(self) -> bytes:
        """Return the response body bytes."""
        return self.body


class StorageList(Mapping[str, StructuredValue]):
    """Snapshot mapping returned by ``DurableObjectStorage.list``."""

    def __init__(self, values: dict[str, StructuredValue]):
        """Store an immutable key/value snapshot in sorted key order."""
        self._values = {key: values[key] for key in sorted(values)}

    def __getitem__(self, key: str) -> StructuredValue:
        """Return one decoded storage value."""
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        """Iterate over keys in lexicographic order."""
        return iter(self._values)

    def __len__(self) -> int:
        """Return the number of keys in the snapshot."""
        return len(self._values)

    def keys(self) -> list[str]:
        """Return snapshot keys in deterministic order."""
        return list(self._values)

    def values(self) -> list[StructuredValue]:
        """Return decoded snapshot values in key order."""
        return list(self._values.values())

    def items(self) -> list[tuple[str, StructuredValue]]:
        """Return decoded key/value pairs in key order."""
        return list(self._values.items())


class SqlRow(dict[str, Any]):
    """One SQL row with both mapping and attribute access."""

    def __getattr__(self, name: str) -> Any:
        """Return a column by attribute name, matching Cloudflare examples."""
        try:
            return self[name]
        except KeyError as error:
            raise AttributeError(name) from error


class SqlCursor:
    """Synchronous cursor over one JSON-encoded SQL result set."""

    def __init__(self, rows: list[dict[str, Any]]):
        """Create a cursor whose rows are consumed in query order."""
        self._rows = [SqlRow(row) for row in rows]
        self._index = 0

    def __iter__(self) -> Iterator[SqlRow]:
        """Iterate over all remaining rows synchronously."""
        while self._index < len(self._rows):
            row = self._rows[self._index]
            self._index += 1
            yield row

    def __next__(self) -> SqlRow:
        """Return the next row or raise StopIteration at end of cursor."""
        if self._index >= len(self._rows):
            raise StopIteration
        row = self._rows[self._index]
        self._index += 1
        return row

    def to_array(self) -> list[SqlRow]:
        """Consume and return every remaining row as attribute rows."""
        return list(self)

    def one(self) -> SqlRow:
        """Consume exactly one row, raising when cardinality differs."""
        remaining = self.to_array()
        if len(remaining) != 1:
            raise WorkerError(f"SQL cursor expected one row, got {len(remaining)}")
        return remaining[0]

    def raw(self) -> Iterator[list[Any]]:
        """Consume remaining rows as positional column arrays."""
        return iter([[row[key] for key in row] for row in self])


class SqlStorage:
    """Synchronous SQL facade backed by the WIT JSON-row import."""

    def __init__(self, imports: _StorageImports):
        """Bind SQL calls to one Durable Object storage import module."""
        self._imports = imports

    def exec(self, statement: str, *bindings: Any) -> SqlCursor:
        """Execute SQL and expose a Cloudflare-style synchronous cursor."""
        if not isinstance(statement, str) or not statement.strip():
            raise TypeError("SQL statement must be a non-empty string")
        statement = _bind_sql(statement, bindings)
        encoded = _call_host(self._imports.sql_rows, statement)
        if isinstance(encoded, bytes):
            encoded = encoded.decode("utf-8")
        try:
            rows = json.loads(encoded)
        except (TypeError, ValueError) as error:
            raise WorkerError("storage.sql returned invalid JSON") from error
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise WorkerError("storage.sql returned a non-object row array")
        return SqlCursor(rows)


def _bind_sql(statement: str, bindings: tuple[Any, ...]) -> str:
    """Substitute positional SQL bindings because WIT v2 carries one string.

    The host import intentionally remains a single ``statement`` argument.  The
    adapter emits SQLite literals with strict type checks rather than allowing
    string concatenation from application code to alter the query text.
    """
    if not bindings:
        return statement

    def literal(value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, int) and not isinstance(value, bool):
            return str(value)
        if isinstance(value, float) and math.isfinite(value):
            return repr(value)
        if isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"
        if isinstance(value, (bytes, bytearray, memoryview)):
            return "X'" + bytes(value).hex().upper() + "'"
        raise TypeError(f"unsupported SQL binding: {type(value).__name__}")

    output: list[str] = []
    binding_index = 0
    quote: str | None = None
    index = 0
    while index < len(statement):
        character = statement[index]
        if quote is not None:
            output.append(character)
            if character == quote:
                if index + 1 < len(statement) and statement[index + 1] == quote:
                    output.append(statement[index + 1])
                    index += 1
                else:
                    quote = None
            index += 1
            continue
        if character in "'\"":
            quote = character
            output.append(character)
        elif character == "?":
            if binding_index >= len(bindings):
                raise ValueError("SQL statement has more placeholders than bindings")
            output.append(literal(bindings[binding_index]))
            binding_index += 1
        else:
            output.append(character)
        index += 1
    if binding_index != len(bindings):
        raise ValueError("SQL statement has fewer placeholders than bindings")
    return "".join(output)


_MISSING = object()


class Storage:
    """Durable Object storage API with deterministic structured values."""

    def __init__(self, imports: _StorageImports, sockets: _SocketImports | None = None):
        """Bind storage and SQL operations to generated host imports."""
        self._imports = imports
        self.sql = SqlStorage(imports)
        self._sockets = sockets

    def _get_sync(self, key: str) -> StructuredValue | None:
        """Read and decode one value for list snapshot assembly."""
        encoded = _call_host(self._imports.get, key)
        return None if encoded is None else _decode_value(encoded)

    async def get(
        self,
        key: str | list[str],
        options: Mapping[str, Any] | None = None,
    ) -> StructuredValue | dict[str, StructuredValue]:
        """Read one value or a mapping of values from storage."""
        del options
        if isinstance(key, list):
            values: dict[str, StructuredValue] = {}
            for item in key:
                if not isinstance(item, str):
                    raise TypeError("storage.get keys must be strings")
                encoded = _call_host(self._imports.get, item)
                if encoded is not None:
                    values[item] = _decode_value(encoded)
            return values
        if not isinstance(key, str):
            raise TypeError("storage.get key must be a string")
        return self._get_sync(key)

    async def put(
        self,
        key: str | Mapping[str, StructuredValue],
        value: StructuredValue | object = _MISSING,
        options: Mapping[str, Any] | None = None,
    ) -> None:
        """Stage one value or a mapping of values in the current transaction."""
        del options
        if isinstance(key, Mapping):
            if value is not _MISSING:
                raise TypeError("storage.put mapping form accepts no value argument")
            for entry_key, entry_value in key.items():
                if not isinstance(entry_key, str):
                    raise TypeError("storage.put keys must be strings")
                _call_host(self._imports.put, entry_key, _encode_value(entry_value))
            return
        if not isinstance(key, str) or value is _MISSING:
            raise TypeError("storage.put expects a string key and value")
        _call_host(self._imports.put, key, _encode_value(value))

    async def delete(
        self,
        key: str | list[str],
        options: Mapping[str, Any] | None = None,
    ) -> bool | int:
        """Delete one key or a list of keys and report deletion count."""
        del options
        if isinstance(key, list):
            deleted = 0
            for item in key:
                if not isinstance(item, str):
                    raise TypeError("storage.delete keys must be strings")
                deleted += int(bool(_call_host(self._imports.delete, item)))
            return deleted
        if not isinstance(key, str):
            raise TypeError("storage.delete key must be a string")
        return bool(_call_host(self._imports.delete, key))

    async def list(
        self,
        options: Mapping[str, Any] | None = None,
        *,
        prefix: str = "",
        limit: int = 1000,
        reverse: bool = False,
        start: str | None = None,
        start_after: str | None = None,
        end: str | None = None,
    ) -> StorageList:
        """Return a deterministic mapping of keys to decoded snapshot values.

        The WIT import lists keys only, so values are read at the same event
        fence.  ``reverse``, ``start``, ``start_after``, and ``end`` are applied
        in Python because the host contract intentionally exposes only prefix and
        limit.  The storage value encoding is documented in ``README.md``.
        """
        if options is not None:
            if not isinstance(options, Mapping):
                raise TypeError("storage.list options must be a mapping")
            allowed = {"prefix", "limit", "reverse", "start", "start_after", "end"}
            unknown = set(options) - allowed
            if unknown:
                raise TypeError(f"unknown storage.list option: {sorted(unknown)[0]}")
            prefix = options.get("prefix", prefix)
            limit = options.get("limit", limit)
            reverse = options.get("reverse", reverse)
            start = options.get("start", start)
            start_after = options.get("start_after", start_after)
            end = options.get("end", end)
        if not isinstance(prefix, str):
            raise TypeError("storage.list prefix must be a string")
        limit = _require_uint(limit, "storage.list limit", 0xFFFFFFFF)
        if not isinstance(reverse, bool):
            raise TypeError("storage.list reverse must be a boolean")
        keys = list(_call_host(self._imports.list, prefix, limit if not any((reverse, start, start_after, end)) else 0xFFFFFFFF))
        keys = sorted(keys, reverse=reverse)
        if start is not None:
            keys = [key for key in keys if key >= start]
        if start_after is not None:
            keys = [key for key in keys if key > start_after]
        if end is not None:
            keys = [key for key in keys if key < end]
        keys = keys[:limit]
        values = {key: self._get_sync(key) for key in keys}
        return StorageList(values)

    async def set_alarm(self, epoch_millis: int) -> None:
        """Stage the Durable Object alarm at an epoch-millisecond deadline."""
        epoch_millis = _require_uint(epoch_millis, "alarm epoch milliseconds", 0xFFFFFFFFFFFFFFFF)
        _call_host(self._imports.set_alarm, epoch_millis)

    async def get_alarm(self) -> int | None:
        """Read the current Durable Object alarm deadline."""
        value = _call_host(self._imports.get_alarm)
        return None if value is None else _require_uint(value, "alarm epoch milliseconds", 0xFFFFFFFFFFFFFFFF)

    async def delete_alarm(self) -> None:
        """Stage deletion of the Durable Object alarm."""
        _call_host(self._imports.delete_alarm)


@dataclass(frozen=True, init=False, eq=False)
class DurableObjectId:
    """A deterministic idFromName result retaining the host routing name."""

    name: str | None
    _hex: str

    def __init__(self, value: str, object_name: str | None = None):
        """Construct an id from hex or from a named hash result."""
        if object_name is None:
            identifier = value
            name = None
        else:
            name = value
            identifier = object_name
        if not isinstance(identifier, str) or len(identifier) != 64:
            raise ValueError("DurableObjectId must be a 64-character hexadecimal string")
        try:
            int(identifier, 16)
        except ValueError as error:
            raise ValueError("DurableObjectId must be hexadecimal") from error
        if name is not None and not isinstance(name, str):
            raise TypeError("DurableObjectId name must be a string")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "_hex", identifier.lower())

    def to_string(self) -> str:
        """Return the lowercase SHA-256 id string."""
        return self._hex

    def equals(self, other: object) -> bool:
        """Compare ids by their canonical hexadecimal identity."""
        return isinstance(other, DurableObjectId) and self._hex == other._hex

    def __eq__(self, other: object) -> bool:
        """Compare ids by canonical hex even when one has no source name."""
        return self.equals(other)

    def __hash__(self) -> int:
        """Hash ids by canonical hex so named and string ids agree."""
        return hash(self._hex)

    def __str__(self) -> str:
        """Render the deterministic id string."""
        return self._hex


STREAM_MAX_REQUEST_BYTES = 5 * 1024 * 1024
STREAM_APPEND_URI = "https://verglas.internal/stream/append"


def _assert_json_value(value: Any, ancestors: set[int]) -> None:
    """Reject values that are not strict JSON values or that contain cycles."""
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise TypeError("Pipeline.send requires JSON-serializable records: numbers must be finite")
        return
    if isinstance(value, list):
        identity = id(value)
        if identity in ancestors:
            raise TypeError("Pipeline.send requires JSON-serializable records: cyclic value")
        ancestors.add(identity)
        try:
            for entry in value:
                _assert_json_value(entry, ancestors)
        finally:
            ancestors.remove(identity)
        return
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("Pipeline.send requires JSON-serializable records: object keys must be strings")
        identity = id(value)
        if identity in ancestors:
            raise TypeError("Pipeline.send requires JSON-serializable records: cyclic value")
        ancestors.add(identity)
        try:
            for entry in value.values():
                _assert_json_value(entry, ancestors)
        finally:
            ancestors.remove(identity)
        return
    raise TypeError(
        "Pipeline.send requires JSON-serializable records: "
        f"unsupported {type(value).__name__}"
    )


class PipelineBinding:
    """A fixed Pipeline Stream binding exposing only asynchronous ``send``."""

    def __init__(
        self,
        binding_name: str,
        stream_name: str,
        imports: _BindingImports,
        *,
        transactional: bool = False,
    ):
        """Bind one Wrangler name to one immutable Stream identity."""
        if not isinstance(binding_name, str) or not binding_name.strip():
            raise TypeError("Pipeline binding name must be a non-empty string")
        if not isinstance(stream_name, str) or not stream_name.strip():
            raise TypeError("Stream identity must be a non-empty string")
        if transactional:
            if not hasattr(imports, "stream_send") or not callable(imports.stream_send):
                raise TypeError("Durable Object Stream binding requires the WIT storage.stream-send transport")
        elif not hasattr(imports, "do_fetch") or not callable(imports.do_fetch):
            raise TypeError("Worker Stream binding requires the WIT bindings.do-fetch transport")
        self._binding_name = binding_name
        self._stream_name = stream_name
        self._imports = imports
        self._transactional = transactional

    async def send(self, records: list[Any]) -> None:
        """Append a JSON record array and wait for a durable 2xx acknowledgement."""
        if not isinstance(records, list):
            raise TypeError("Pipeline.send requires a list of JSON-serializable records")
        _assert_json_value(records, set())
        try:
            encoded = json.dumps(
                records,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, UnicodeError, ValueError, OverflowError) as error:
            raise TypeError(
                f"Pipeline.send requires JSON-serializable records: {error}"
            ) from error
        if len(encoded) > STREAM_MAX_REQUEST_BYTES:
            raise ValueError(
                "Pipeline.send request exceeds the 5 MiB encoded request limit "
                f"({len(encoded)} bytes)"
            )
        if self._transactional:
            _call_host(
                self._imports.stream_send,
                self._binding_name,
                self._stream_name,
                encoded.decode("utf-8"),
            )
            return

        if _wit_types is None:
            host_request: Any = SimpleNamespace(
                method="POST",
                uri=STREAM_APPEND_URI,
                headers=[("content-type", "application/json")],
                body=encoded,
                ws=None,
            )
        else:
            host_request = _wit_types.Request(
                "POST",
                STREAM_APPEND_URI,
                [("content-type", "application/json")],
                encoded,
                None,
            )
        raw = _call_host(
            self._imports.do_fetch,
            self._binding_name,
            self._stream_name,
            host_request,
        )
        status = getattr(raw, "status", None)
        if isinstance(status, bool) or not isinstance(status, int) or not 200 <= status < 300:
            raise WorkerError(
                "Pipeline.send did not receive a durable ACK: "
                f"HTTP {status!s}"
            )


def _request_to_host(request: Request) -> Any:
    """Convert one public Request into the generated bindings request record."""
    if _wit_types is None:
        return SimpleNamespace(
            method=request.method,
            uri=request.url,
            headers=request.headers.items(),
            body=request.body,
            ws=request.ws,
        )
    return _wit_types.Request(
        request.method,
        request.url,
        request.headers.items(),
        request.body,
        request.ws,
    )


def _response_from_host(raw: Any) -> Response:
    """Convert one generated bindings response record into a public Response."""
    if isinstance(raw, Response):
        return raw
    response = Response(
        bytes(getattr(raw, "body", b"")),
        status=int(getattr(raw, "status", 200)),
        headers=list(getattr(raw, "headers", [])),
    )
    response.accept_ws = getattr(raw, "accept_ws", None)
    return response


class ServiceBinding:
    """A direct Wrangler service binding exposing asynchronous ``fetch`` only."""

    def __init__(self, binding: str, service: str, imports: _BindingImports):
        """Bind one Wrangler name to one immutable service target."""
        if not isinstance(binding, str) or not binding.strip():
            raise TypeError("Service binding name must be a non-empty string")
        if not isinstance(service, str) or not service.strip():
            raise TypeError("Service target must be a non-empty string")
        if not hasattr(imports, "do_fetch") or not callable(imports.do_fetch):
            raise TypeError("Service binding requires the WIT bindings.do-fetch transport")
        self._binding = binding
        self._service = service
        self._imports = imports

    async def fetch(self, request: Request | str) -> Response:
        """Forward one Request or URL string to the configured service target."""
        if isinstance(request, str):
            request = Request(request)
        if not isinstance(request, Request):
            raise TypeError("ServiceBinding.fetch expects a Request or URL")
        raw = _call_host(
            self._imports.do_fetch,
            self._binding,
            self._service,
            _request_to_host(request),
        )
        return _response_from_host(raw)


class DurableObjectStub:
    """A flattened Durable Object stub exposing asynchronous ``fetch``."""

    def __init__(
        self,
        binding: str,
        object_name: str,
        imports: _BindingImports,
        identifier: DurableObjectId | None = None,
    ):
        """Bind one stub to a namespace, object name, and host import."""
        self.id = identifier
        self._binding = binding
        self._object_name = object_name
        self._imports = imports

    async def fetch(self, request: Request | str) -> Response:
        """Forward one Request or URL string through ``bindings.do-fetch``."""
        if isinstance(request, str):
            request = Request(request)
        if not isinstance(request, Request):
            raise TypeError("DurableObjectStub.fetch expects a Request or URL")
        host_request: Any
        if _wit_types is None:
            host_request = SimpleNamespace(
                method=request.method,
                uri=request.url,
                headers=request.headers.items(),
                body=request.body,
                ws=request.ws,
            )
        else:
            host_request = _wit_types.Request(
                request.method,
                request.url,
                request.headers.items(),
                request.body,
                request.ws,
            )
        raw = _call_host(self._imports.do_fetch, self._binding, self._object_name, host_request)
        if isinstance(raw, Response):
            return raw
        response = Response(
            bytes(getattr(raw, "body", b"")),
            status=int(getattr(raw, "status", 200)),
            headers=list(getattr(raw, "headers", [])),
        )
        response.accept_ws = getattr(raw, "accept_ws", None)
        return response


class DurableObjectNamespace:
    """Cloudflare-style namespace binding with deterministic named ids."""

    def __init__(self, name: str, imports: _BindingImports):
        """Bind one manifest namespace name to the flattened host import."""
        self._name = name
        self._imports = imports
        self._ids_by_name: dict[str, DurableObjectId] = {}

    def id_from_name(self, name: str) -> DurableObjectId:
        """Hash a UTF-8 name while retaining that name for host routing."""
        if not isinstance(name, str):
            raise TypeError("DurableObjectNamespace.id_from_name expects a string")
        if name not in self._ids_by_name:
            self._ids_by_name[name] = DurableObjectId(
                name, hashlib.sha256(name.encode("utf-8")).hexdigest()
            )
        return self._ids_by_name[name]

    def id_from_string(self, identifier: str) -> DurableObjectId:
        """Construct an id from a validated lowercase or uppercase hex string."""
        return DurableObjectId(identifier)

    def get(self, identifier: DurableObjectId) -> DurableObjectStub:
        """Return a stub for one idFromName result."""
        if not isinstance(identifier, DurableObjectId):
            raise TypeError("DurableObjectNamespace.get expects a DurableObjectId")
        return DurableObjectStub(
            self._name,
            identifier.name if identifier.name is not None else identifier.to_string(),
            self._imports,
            identifier,
        )

    def get_by_name(self, name: str) -> DurableObjectStub:
        """Return a stub using the Python spelling of the named-id helper."""
        return self.get(self.id_from_name(name))


class Environment:
    """Worker environment populated from Wrangler vars, DO, Stream, and service bindings."""

    def __init__(
        self,
        storage: Storage | None,
        sockets: _SocketImports | None,
        binding_imports: _BindingImports,
        variables: Mapping[str, Any],
        binding_records: list[Mapping[str, str]],
        pipeline_records: list[Mapping[str, str]] | None = None,
        services: list[Mapping[str, str]] | None = None,
        *,
        transactional_streams: bool = False,
    ):
        """Create one environment view for a Worker or Durable Object."""
        self._storage = storage
        self._variables = dict(variables)
        self._bindings = {
            str(record["name"]): DurableObjectNamespace(str(record["name"]), binding_imports)
            for record in binding_records
        }
        self._pipelines: dict[str, PipelineBinding] = {}
        for record in pipeline_records or []:
            binding = str(record["binding"])
            if binding in self._bindings or binding in self._pipelines:
                raise ValueError(f"duplicate binding name: {binding}")
            self._pipelines[binding] = PipelineBinding(
                binding,
                str(record["stream"]),
                self._storage._imports if transactional_streams and self._storage is not None else binding_imports,
                transactional=transactional_streams,
            )
        self._services: dict[str, ServiceBinding] = {}
        for record in services or []:
            binding = str(record["binding"])
            if binding in self._bindings or binding in self._pipelines or binding in self._services:
                raise ValueError(f"duplicate binding name: {binding}")
            self._services[binding] = ServiceBinding(
                binding,
                str(record["service"]),
                binding_imports,
            )
        self._sockets = sockets

    def __getattr__(self, name: str) -> Any:
        """Resolve a Wrangler variable or namespace, Stream, or service binding."""
        if name in self._bindings:
            return self._bindings[name]
        if name in self._pipelines:
            return self._pipelines[name]
        if name in self._services:
            return self._services[name]
        if name in self._variables:
            return self._variables[name]
        raise AttributeError(name)

    def __getitem__(self, name: str) -> Any:
        """Resolve environment entries using mapping syntax."""
        try:
            return getattr(self, name)
        except AttributeError as error:
            raise KeyError(name) from error


class ExecutionContext:
    """Request execution context with wait-until completion semantics."""

    def __init__(self) -> None:
        """Create an empty request-local wait-until queue."""
        self._waits: list[Any] = []

    def wait_until(self, awaitable: Any) -> None:
        """Queue an awaitable and complete it before the WIT event returns."""
        if not inspect.isawaitable(awaitable):
            raise TypeError("ExecutionContext.wait_until expects an awaitable")
        self._waits.append(awaitable)

    def _drain(self) -> None:
        """Resolve all queued work before event completion."""
        while self._waits:
            waits = self._waits
            self._waits = []
            for awaitable in waits:
                _run_awaitable(awaitable)


class DurableObjectState:
    """Durable Object context exposing storage and hibernating WebSockets."""

    def __init__(
        self,
        storage: Storage,
        sockets: _SocketImports,
        pending_ws: int | None = None,
        object_id: DurableObjectId | None = None,
    ):
        """Bind one object state to generated imports and a pending request."""
        self.storage = storage
        self._sockets = sockets
        self._pending_ws = pending_ws
        self._object_id = object_id
        self._accepted_ws: int | None = None

    @property
    def id(self) -> DurableObjectId | None:
        """Return the object id when the host supplied one."""
        return self._object_id

    def _set_pending_ws(self, pending_ws: int | None) -> None:
        """Set the request-local pending WebSocket identity."""
        self._pending_ws = pending_ws
        self._accepted_ws = None

    def accept_websocket(self, websocket: WebSocket, tags: list[str] | None = None) -> None:
        """Accept the server socket and make its id eligible for response accept-ws."""
        del tags
        if not isinstance(websocket, WebSocket):
            raise TypeError("accept_websocket expects a WebSocket")
        if websocket._socket_id is None or self._pending_ws is None:
            raise WorkerError("no pending WebSocket upgrade is available")
        if websocket._socket_id != self._pending_ws:
            raise WorkerError("WebSocket does not belong to the pending upgrade")
        self._accepted_ws = websocket._socket_id
        websocket._accepted = True
        if websocket._peer is not None:
            websocket._peer._accept_ws = self._accepted_ws

    def get_websockets(self, tag: str | None = None) -> list[WebSocket]:
        """Return currently attached WebSocket objects, optionally ignoring tags."""
        del tag
        return [WebSocket(socket, self) for socket in _call_host(self._sockets.attached)]


class WebSocket:
    """Cloudflare WebSocket object backed by the socket WIT imports."""

    def __init__(self, socket_id: int | None, state: DurableObjectState | None):
        """Create one endpoint reference for a pending or attached socket."""
        self._socket_id = socket_id
        self._state = state
        self._peer: WebSocket | None = None
        self._accepted = False
        self._accept_ws: int | None = None

    @property
    def accept_ws(self) -> int | None:
        """Return the accepted upgrade id attached to the client endpoint."""
        return self._accept_ws

    def accept(self) -> None:
        """Accept a standard endpoint through the current Durable Object state."""
        if self._state is None:
            raise WorkerError("WebSocket has no Durable Object state")
        self._state.accept_websocket(self)

    def send(self, message: str | BytesLike) -> None:
        """Stage one UTF-8 text or binary message for output-gated delivery."""
        socket_id, imports = self._require_socket()
        _call_host(imports.send, socket_id, _as_bytes(message))

    def close(self, code: int = 1000, reason: str = "") -> None:
        """Stage one WebSocket close operation."""
        socket_id, imports = self._require_socket()
        code = _require_uint(code, "WebSocket close code", 0xFFFF)
        _call_host(imports.close, socket_id, code, reason)

    def serialize_attachment(self, value: StructuredValue) -> None:
        """Persist a deterministic structured attachment blob."""
        socket_id, imports = self._require_socket()
        encoded = _encode_value(value)
        if len(encoded) > 16 * 1024:
            raise ValueError("WebSocket attachment exceeds 16 KiB")
        _call_host(imports.set_attachment, socket_id, encoded)

    def deserialize_attachment(self) -> StructuredValue | None:
        """Read and decode the most recent structured attachment."""
        socket_id, imports = self._require_socket()
        encoded = _call_host(imports.get_attachment, socket_id)
        return None if encoded is None else _decode_value(encoded)

    def _require_socket(self) -> tuple[int, _SocketImports]:
        """Return the socket id and imports or raise for an unbound endpoint."""
        if self._socket_id is None or self._state is None:
            raise WorkerError("WebSocket endpoint is not attached to a Durable Object")
        return self._socket_id, self._state._sockets


class WebSocketPair:
    """Pair of client and server WebSocket endpoints for a pending upgrade."""

    def __init__(self) -> None:
        """Create a pair using the current event's pending WebSocket id."""
        event = _current_event.get()
        state = event.state if event is not None else None
        pending = event.pending_ws if event is not None else None
        self.client = WebSocket(pending, state)
        self.server = WebSocket(pending, state)
        self.client._peer = self.server
        self.server._peer = self.client

    @classmethod
    def new(cls) -> WebSocketPair:
        """Construct a pair using the Python Workers SDK spelling."""
        return cls()

    def object_values(self) -> tuple[WebSocket, WebSocket]:
        """Return client and server endpoints in Cloudflare object order."""
        return self.client, self.server

    def __iter__(self) -> Iterator[WebSocket]:
        """Allow direct tuple unpacking of client and server endpoints."""
        return iter(self.object_values())


class WorkerEntrypoint:
    """Base class for the Cloudflare Python ``Default`` Worker entrypoint."""

    def __init__(self, ctx: ExecutionContext | None = None, env: Environment | None = None):
        """Expose the request execution context and environment on ``self``."""
        self.ctx = ctx if ctx is not None else ExecutionContext()
        self.env = env


class DurableObject:
    """Base class for Cloudflare Python Durable Object classes."""

    def __init__(self, ctx: DurableObjectState, env: Environment):
        """Expose object state and environment on ``self``."""
        self.ctx = ctx
        self.env = env


def _run_awaitable(value: Any) -> Any:
    """Drive one immediate Cloudflare coroutine without a clocked event loop."""
    iterator = value.__await__()
    try:
        yielded = next(iterator)
        while True:
            if yielded is None:
                resolved = None
            elif inspect.isawaitable(yielded):
                resolved = _run_awaitable(yielded)
            else:
                raise RuntimeError("Cloudflare coroutine suspended on an unsupported scheduler primitive")
            yielded = iterator.send(resolved)
    except StopIteration as completed:
        return completed.value


def invoke_awaitable(value: Any, execution: ExecutionContext | None) -> Any:
    """Resolve a sync or immediate async handler and drain wait-until work."""
    result = _run_awaitable(value) if inspect.isawaitable(value) else value
    if execution is not None:
        execution._drain()
    return result


def invoke_callback(callback: Any, args: tuple[Any, ...], execution: ExecutionContext | None) -> Any:
    """Call a handler and synchronously resolve its Cloudflare coroutine."""
    return invoke_awaitable(callback(*args), execution)


__all__ = [
    "DurableObject",
    "DurableObjectId",
    "DurableObjectNamespace",
    "DurableObjectState",
    "DurableObjectStub",
    "Environment",
    "PipelineBinding",
    "ExecutionContext",
    "Headers",
    "Request",
    "Response",
    "SqlCursor",
    "SqlStorage",
    "Storage",
    "StorageList",
    "WebSocket",
    "WebSocketPair",
    "WorkerEntrypoint",
    "WorkerError",
    "event_scope",
    "invoke_callback",
    "leave_event_scope",
    "set_wit_types",
]
