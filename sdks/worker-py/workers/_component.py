"""componentize-py adapter for the Cloudflare Python Workers surface.

The module is imported only by the temporary build entry.  It implements both
v2 WIT exports: ``worker.fetch`` dispatches a stateless Worker entrypoint, while
``handler`` owns one Durable Object instance and its serialized event state.
"""

from __future__ import annotations

import inspect
from types import ModuleType
from typing import Any, Callable, NoReturn

from componentize_py_types import Err
from wit_world import exports
from wit_world.imports import bindings as binding_imports
from wit_world.imports import sockets as socket_imports
from wit_world.imports import storage as storage_imports
from wit_world.imports import types as wit_types

from ._runtime import (
    DurableObject,
    DurableObjectState,
    Environment,
    ExecutionContext,
    Request,
    Response,
    Storage,
    WebSocket,
    WorkerEntrypoint,
    event_scope,
    invoke_callback,
    leave_event_scope,
    set_wit_types,
)


_project_module: ModuleType | None = None
_binding_records: list[dict[str, str]] = []
_pipeline_records: list[dict[str, str]] = []
_service_records: list[dict[str, str]] = []
_variables: dict[str, Any] = {}
_storage = Storage(storage_imports, socket_imports)
_environment: Environment | None = None
_object_state: DurableObjectState | None = None
_object_instance: DurableObject | None = None

set_wit_types(wit_types)


def set_project(
    module: ModuleType,
    binding_records: list[dict[str, str]],
    variables: dict[str, Any],
    pipeline_records: list[dict[str, str]] | None = None,
    service_records: list[dict[str, str]] | None = None,
) -> None:
    """Configure the imported project and its validated Wrangler bindings."""
    global _project_module, _binding_records, _pipeline_records, _service_records, _variables, _environment
    _project_module = module
    _binding_records = list(binding_records)
    _pipeline_records = list(pipeline_records or [])
    _service_records = list(service_records or [])
    _variables = dict(variables)
    _environment = Environment(
        _storage,
        socket_imports,
        binding_imports,
        _variables,
        _binding_records,
        _pipeline_records,
        _service_records,
    )


def _error_message(error: Exception) -> str:
    """Return a stable handler-error message for one application exception."""
    detail = str(error)
    return detail if detail else type(error).__name__


def _raise_handler_error(error: Exception) -> NoReturn:
    """Raise the generated handler-error result expected by the WIT wrapper."""
    raise Err(wit_types.HandlerError(_error_message(error))) from error


def _configured_module() -> ModuleType:
    """Return the configured project module or fail the current event honestly."""
    if _project_module is None or _environment is None:
        raise RuntimeError("Python Worker project was not configured")
    return _project_module


def _invoke_user(callback: Callable[..., Any], args: tuple[Any, ...], execution: ExecutionContext) -> Any:
    """Invoke one sync or async Cloudflare callback and drain waitUntil work."""
    return invoke_callback(callback, args, execution)


def _handler_arguments(function: Callable[..., Any], request: Request, execution: ExecutionContext) -> tuple[Any, ...]:
    """Select the documented on_fetch arity without changing user callables."""
    parameters = list(inspect.signature(function).parameters.values())
    positional = [
        parameter
        for parameter in parameters
        if parameter.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    has_varargs = any(parameter.kind == inspect.Parameter.VAR_POSITIONAL for parameter in parameters)
    if has_varargs or len(positional) >= 3:
        return request, _environment, execution
    if len(positional) == 2:
        return request, _environment
    if len(positional) == 1:
        return (request,)
    raise TypeError("on_fetch must accept request, optionally env and ctx")


def _to_request(request: Any) -> Request:
    """Convert a generated v2 WIT request record to the public Request object."""
    return Request(
        request.method,
        request.uri,
        list(request.headers),
        bytes(request.body),
        request.ws,
    )


def _to_wit_response(response: Any) -> Any:
    """Convert a public Response into the generated v2 WIT response record."""
    if not isinstance(response, Response):
        raise TypeError("Worker handlers must return workers.Response")
    return wit_types.Response(
        response.status,
        response.headers.items(),
        bytes(response.body),
        response.accept_ws,
    )


def _entry_response(callback: Callable[..., Any], request: Request, execution: ExecutionContext) -> Any:
    """Invoke one Worker callback and convert its response record."""
    result = _invoke_user(callback, _handler_arguments(callback, request, execution), execution)
    return _to_wit_response(result)


def _worker_callback(module: ModuleType) -> Callable[..., Any]:
    """Resolve the canonical module on_fetch or Default entrypoint callback."""
    on_fetch = getattr(module, "on_fetch", None)
    if on_fetch is not None:
        if not callable(on_fetch):
            raise TypeError("on_fetch must be callable")
        return on_fetch

    entrypoint = getattr(module, "Default", None)
    if (
        entrypoint is None
        or not inspect.isclass(entrypoint)
        or not issubclass(entrypoint, WorkerEntrypoint)
    ):
        raise TypeError("Python Worker must define on_fetch or Default(WorkerEntrypoint)")
    if _environment is None:
        raise RuntimeError("Python Worker environment was not configured")
    return entrypoint


def _ensure_object() -> DurableObject:
    """Instantiate the manifest's single declared Durable Object class once."""
    global _object_state, _object_instance
    module = _configured_module()
    if _object_instance is not None:
        return _object_instance
    if len(_binding_records) != 1:
        raise RuntimeError(
            "the v2 component requires exactly one durable_objects binding for handler export"
        )
    class_name = _binding_records[0]["class_name"]
    object_class = getattr(module, class_name, None)
    if object_class is None or not inspect.isclass(object_class):
        raise RuntimeError(f"declared Durable Object class is not exported: {class_name}")
    if not issubclass(object_class, DurableObject):
        raise TypeError(f"declared Durable Object {class_name} must extend DurableObject")
    if _environment is None:
        raise RuntimeError("Python Worker environment was not configured")
    _object_state = DurableObjectState(_storage, socket_imports)
    object_environment = Environment(
        _storage,
        socket_imports,
        binding_imports,
        _variables,
        _binding_records,
        _pipeline_records,
        _service_records,
        transactional_streams=True,
    )
    _object_instance = object_class(_object_state, object_environment)
    if not isinstance(_object_instance, DurableObject):
        raise TypeError(f"declared Durable Object {class_name} did not construct correctly")
    return _object_instance


def _callback_for_object(name: str, required: bool) -> Callable[..., Any] | None:
    """Resolve one Durable Object method by its Cloudflare Python name."""
    object_instance = _ensure_object()
    callback = getattr(object_instance, name, None)
    if callback is None and required:
        raise RuntimeError(f"Durable Object must define {name}(...)")
    if callback is not None and not callable(callback):
        raise TypeError(f"Durable Object attribute {name} is not callable")
    return callback


def _invoke_object(callback: Callable[..., Any], args: tuple[Any, ...], execution: ExecutionContext) -> Any:
    """Invoke one object callback within the request's WebSocket state scope."""
    if _object_state is None:
        raise RuntimeError("Durable Object state was not initialized")
    return invoke_callback(callback, args, execution)


class Worker(exports.Worker):
    """Adapt the Cloudflare Worker entrypoint to ``worker.fetch``."""

    def fetch(self, request: Any) -> Any:
        """Dispatch one stateless Worker request through on_fetch or Default."""
        try:
            module = _configured_module()
            public_request = _to_request(request)
            execution = ExecutionContext()
            callback_or_class = _worker_callback(module)
            if inspect.isclass(callback_or_class):
                if _environment is None:
                    raise RuntimeError("Python Worker environment was not configured")
                entrypoint = callback_or_class(execution, _environment)
                callback = getattr(entrypoint, "fetch", None)
                if callback is None or not callable(callback):
                    raise TypeError("Default(WorkerEntrypoint) must define fetch(...)" )
                response = _invoke_user(callback, (public_request,), execution)
                return _to_wit_response(response)
            return _entry_response(callback_or_class, public_request, execution)
        except Exception as error:
            _raise_handler_error(error)


class Handler(exports.Handler):
    """Adapt one Durable Object class to the serialized handler export."""

    def init(self) -> None:
        """Construct the declared Durable Object before its first event."""
        try:
            _ensure_object()
        except Exception as error:
            _raise_handler_error(error)

    def fetch(self, request: Any) -> Any:
        """Dispatch one Durable Object fetch event and preserve accept-ws."""
        try:
            object_instance = _ensure_object()
            if _object_state is None:
                raise RuntimeError("Durable Object state was not initialized")
            public_request = _to_request(request)
            _object_state._set_pending_ws(public_request.ws)
            execution = ExecutionContext()
            token = event_scope(_object_state, public_request.ws, execution)
            try:
                callback = getattr(object_instance, "fetch", None)
                if callback is None or not callable(callback):
                    raise TypeError("Durable Object must define fetch(...)")
                response = _invoke_object(callback, (public_request,), execution)
                return _to_wit_response(response)
            finally:
                leave_event_scope(token)
        except Exception as error:
            _raise_handler_error(error)

    def alarm(self, scheduled_epoch_millis: int) -> None:
        """Dispatch one Durable Object alarm when the class defines alarm."""
        try:
            if _object_state is None:
                _ensure_object()
            execution = ExecutionContext()
            token = event_scope(_object_state, None, execution)
            try:
                callback = _callback_for_object("alarm", required=False)
                if callback is not None:
                    _invoke_object(callback, (scheduled_epoch_millis,), execution)
            finally:
                leave_event_scope(token)
        except Exception as error:
            _raise_handler_error(error)

    def websocket_message(self, socket: int, message: bytes) -> None:
        """Dispatch the Cloudflare ``webSocketMessage`` callback."""
        try:
            object_instance = _ensure_object()
            if _object_state is None:
                raise RuntimeError("Durable Object state was not initialized")
            execution = ExecutionContext()
            token = event_scope(_object_state, None, execution)
            try:
                callback = _callback_for_object("webSocketMessage", required=False)
                if callback is not None:
                    _invoke_object(callback, (WebSocket(socket, _object_state), bytes(message)), execution)
            finally:
                leave_event_scope(token)
            del object_instance
        except Exception as error:
            _raise_handler_error(error)

    def websocket_close(self, socket: int, code: int, reason: str) -> None:
        """Dispatch the Cloudflare ``webSocketClose`` callback."""
        try:
            object_instance = _ensure_object()
            if _object_state is None:
                raise RuntimeError("Durable Object state was not initialized")
            execution = ExecutionContext()
            token = event_scope(_object_state, None, execution)
            try:
                callback = _callback_for_object("webSocketClose", required=False)
                if callback is not None:
                    _invoke_object(
                        callback,
                        (WebSocket(socket, _object_state), code, reason, True),
                        execution,
                    )
            finally:
                leave_event_scope(token)
            del object_instance
        except Exception as error:
            _raise_handler_error(error)
