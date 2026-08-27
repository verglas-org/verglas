"""Cloudflare-style Python Worker and SQL-backed Durable Object counter."""

from __future__ import annotations

from urllib.parse import urlparse

from workers import DurableObject, Response, WorkerEntrypoint


COUNTER_ID = "global"


class Counter(DurableObject):
    """Persist counter increments in the Durable Object's SQL storage."""

    def __init__(self, ctx, env):
        """Create the counter table before the first Durable Object event."""
        super().__init__(ctx, env)
        self.ctx.storage.sql.exec(
            "CREATE TABLE IF NOT EXISTS counter "
            "(id TEXT NOT NULL, count INTEGER NOT NULL)"
        )

    async def fetch(self, request):
        """Serve counter reads and increments through the SQL cursor API."""
        path = urlparse(request.url).path
        if request.method == "POST" and path == "/incr":
            self.ctx.storage.sql.exec(
                "INSERT INTO counter (id, count) VALUES (?, 1)",
                COUNTER_ID,
            )
            row = self.ctx.storage.sql.exec(
                "SELECT COUNT(*) AS count FROM counter WHERE id = ?",
                COUNTER_ID,
            ).one()
            return Response.json({"count": int(row.count)})

        if request.method == "GET" and path == "/":
            row = self.ctx.storage.sql.exec(
                "SELECT COUNT(*) AS count FROM counter WHERE id = ?",
                COUNTER_ID,
            ).one()
            return Response.json({"count": int(row.count)})

        return Response.json({"error": "not found"}, status=404)


class Default(WorkerEntrypoint):
    """Route public Worker requests to the named counter Durable Object."""

    async def fetch(self, request):
        """Forward every request to the deterministic counter stub."""
        identifier = self.env.COUNTER.id_from_name(COUNTER_ID)
        stub = self.env.COUNTER.get(identifier)
        return await stub.fetch(request)
