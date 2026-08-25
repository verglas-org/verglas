"""Cloudflare-shaped Python Workers authoring surface.

Application modules import this package exactly as they would import
``workers-runtime-sdk``.  The component builder injects the host-backed
implementation at componentize time; no Verglas-specific module is required in
a project.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

from ._runtime import (
    DurableObject,
    DurableObjectId,
    DurableObjectNamespace,
    DurableObjectState,
    DurableObjectStub,
    PipelineBinding,
    ExecutionContext,
    Headers,
    Request,
    Response,
    ServiceBinding,
    SqlCursor,
    SqlStorage,
    StorageList,
    WebSocket,
    WebSocketPair,
    WorkerEntrypoint,
    WorkerError,
)

_F = TypeVar("_F", bound=Callable[..., Any])


def handler(function: _F) -> _F:
    """Mark an ``on_fetch`` function for the Cloudflare Python entry form."""
    setattr(function, "__workers_handler__", True)
    return function


__all__ = [
    "DurableObject",
    "DurableObjectId",
    "DurableObjectNamespace",
    "DurableObjectState",
    "DurableObjectStub",
    "PipelineBinding",
    "ExecutionContext",
    "Headers",
    "Request",
    "Response",
    "ServiceBinding",
    "SqlCursor",
    "SqlStorage",
    "StorageList",
    "WebSocket",
    "WorkerEntrypoint",
    "WorkerError",
    "WebSocketPair",
    "handler",
]
