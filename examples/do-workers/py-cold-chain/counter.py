"""Cloudflare-style Python Worker used by the six-product cold-restart proof."""

from __future__ import annotations

from urllib.parse import urlparse

from workers import DurableObject, Request, Response, WorkerEntrypoint


COUNTER_ID = "global"


class Counter(DurableObject):
    """Persist increments and stage one Stream record in each event."""

    def __init__(self, ctx, env):
        """Create the counter table before serving the first event."""
        super().__init__(ctx, env)
        self.ctx.storage.sql.exec(
            "CREATE TABLE IF NOT EXISTS counter "
            "(id TEXT NOT NULL, count INTEGER NOT NULL)"
        )

    async def fetch(self, request):
        """Serve counter reads and transactionally publish increment records."""
        path = urlparse(request.url).path
        if request.method == "POST" and path == "/incr":
            self.ctx.storage.sql.exec(
                "INSERT INTO counter (id, count) VALUES (?, 1)",
                COUNTER_ID,
            )
            await self.env.STREAM.send([{
                "kind": "increment",
                "payload": COUNTER_ID,
            }])
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
    """Route public requests to the counter and declared product bindings."""

    async def fetch(self, request):
        """Forward each control request through its declared binding."""
        path = urlparse(request.url).path
        if path == "/process" and request.method == "POST":
            return await self.env.PIPELINE.fetch(
                Request("https://verglas.internal/pipeline/process-now", method="POST")
            )
        if path == "/pipeline-status" and request.method == "GET":
            return await self.env.PIPELINE.fetch(
                Request("https://verglas.internal/pipeline/status")
            )
        if path == "/sink-status" and request.method == "GET":
            return await self.env.SINK_A.fetch(
                Request("https://verglas.internal/sink/status")
            )
        if path == "/catalog-status" and request.method == "GET":
            return await self.env.CATALOG.fetch(
                Request("https://verglas.internal/catalog/status")
            )
        identifier = self.env.COUNTER.id_from_name(COUNTER_ID)
        return await self.env.COUNTER.get(identifier).fetch(request)
