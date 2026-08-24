"""componentize-py export adapter for the public Worker authoring surface.

This module is imported only by the temporary build entry module, after
componentize-py has generated the ``wit_world`` package. The adapter keeps the
WIT export methods synchronous, matching the generated bindings and the
serialized Durable Object event contract.
"""

from __future__ import annotations

from types import ModuleType
from typing import Any, Callable, NoReturn

from componentize_py_types import Err
from wit_world import exports
from wit_world.imports import sockets as socket_imports
from wit_world.imports import storage as storage_imports
from wit_world.imports import types as wit_types

from . import Environment, Request, Response, WorkerError


_worker_module: ModuleType | None = None
_environment = Environment(storage_imports, socket_imports)


def set_worker_module(module: ModuleType) -> None:
    """Select the user's module and keep one environment for all events."""
    global _worker_module
    _worker_module = module


def _error_message(error: Exception) -> str:
    """Return a stable message for a user exception."""
    if isinstance(error, WorkerError):
        return str(error)
    detail = str(error)
    return detail if detail else type(error).__name__


def _raise_handler_error(error: Exception) -> NoReturn:
    """Raise the generated result error expected by the WIT export wrapper."""
    raise Err(wit_types.HandlerError(_error_message(error))) from error


def _invoke(function: Callable[..., Any], *args: Any) -> Any:
    """Invoke one synchronous user callback and map failures to handler-error."""
    try:
        return function(*args)
    except Exception as error:
        _raise_handler_error(error)


def _module_function(name: str, required: bool) -> Callable[..., Any] | None:
    """Find one required or optional callback in the configured module."""
    if _worker_module is None:
        raise RuntimeError("Worker module was not configured by the build entry")
    function = getattr(_worker_module, name, None)
    if function is None and required:
        raise RuntimeError(f"Worker module must define {name}(...)")
    if function is not None and not callable(function):
        raise RuntimeError(f"Worker module attribute {name} is not callable")
    return function


def _callback(name: str, required: bool) -> Callable[..., Any] | None:
    """Find one callback and map configuration failures to handler-error."""
    try:
        return _module_function(name, required)
    except Exception as error:
        _raise_handler_error(error)


def _to_request(request: Any) -> Request:
    """Convert a generated WIT request record to the public request record."""
    return Request(request.method, request.uri, list(request.headers), bytes(request.body))


def _to_wit_response(response: Response) -> Any:
    """Convert a public response record to the generated WIT response record."""
    if not isinstance(response, Response):
        raise WorkerError("fetch must return verglas_worker.Response")
    return wit_types.Response(response.status, list(response.headers), bytes(response.body))


class Handler(exports.Handler):
    """Adapt module-level Worker callbacks to the generated handler export."""

    def init(self) -> None:
        """Run the optional initialization callback once per component wake."""
        callback = _callback("init", required=False)
        if callback is not None:
            _invoke(callback, _environment)

    def fetch(self, request: Any) -> Any:
        """Dispatch one HTTP request and convert its response record."""
        callback = _callback("fetch", required=True)
        if callback is None:
            raise RuntimeError("Worker module must define fetch(...)")
        response = _invoke(callback, _to_request(request), _environment)
        try:
            return _to_wit_response(response)
        except Exception as error:
            _raise_handler_error(error)

    def alarm(self, scheduled_epoch_millis: int) -> None:
        """Dispatch an alarm callback when the author defined one."""
        callback = _callback("alarm", required=False)
        if callback is not None:
            _invoke(callback, scheduled_epoch_millis, _environment)

    def websocket_message(self, socket: int, message: bytes) -> None:
        """Dispatch one WebSocket message callback when present."""
        callback = _callback("websocket_message", required=False)
        if callback is not None:
            _invoke(callback, socket, bytes(message), _environment)

    def websocket_close(self, socket: int, code: int, reason: str) -> None:
        """Dispatch one WebSocket close callback when present."""
        callback = _callback("websocket_close", required=False)
        if callback is not None:
            _invoke(callback, socket, code, reason, _environment)
