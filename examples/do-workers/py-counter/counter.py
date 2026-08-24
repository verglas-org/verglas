"""SQL-backed Python Durable Object counter example."""

from __future__ import annotations

import json

from verglas_worker import Environment, Request, Response


COUNTER_ID = "global"


def _json_response(value: object, status: int = 200) -> Response:
    """Encode one JSON value as the response body used by the counter."""
    return Response(
        status,
        [("content-type", "application/json")],
        json.dumps(value, separators=(",", ":")).encode("utf-8"),
    )


def _count(env: Environment) -> int:
    """Count committed counter rows from the SQL table."""
    rows = env.sql("SELECT COUNT(*) AS count FROM counter WHERE id = 'global'")
    return int(rows[0]["count"]) if rows else 0


def init(env: Environment) -> None:
    """Create the counter table before the first event for this object."""
    env.sql(
        "CREATE TABLE IF NOT EXISTS counter "
        "(id VARCHAR NOT NULL, count BIGINT NOT NULL)"
    )


def fetch(request: Request, env: Environment) -> Response:
    """Serve counter reads and increments from the Durable Object SQL state."""
    path = request.uri.split("?", 1)[0]

    if request.method == "POST" and path == "/incr":
        env.sql(f"INSERT INTO counter (id, count) VALUES ('{COUNTER_ID}', 1)")
        return _json_response({"count": _count(env)})

    if request.method == "GET" and path == "/":
        return _json_response({"count": _count(env)})

    return _json_response({"error": "not found"}, 404)


def websocket_message(socket: int, message: bytes, env: Environment) -> None:
    """Echo one WebSocket message and then send the current counter value."""
    env.sockets.send(socket, message)
    env.sockets.send(
        socket,
        json.dumps({"count": _count(env)}, separators=(",", ":")),
    )
