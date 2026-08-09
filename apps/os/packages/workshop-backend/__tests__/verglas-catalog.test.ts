import {describe, expect, it, vi} from "vitest";
import {VerglasCatalogClient} from "../src/verglas-catalog.js";

const env = {
  VERGLAS_ADMIN_URL: "http://localhost:8334/",
  VERGLAS_CONTAINER_RUNTIME_URL: "http://localhost:8360/",
  VERGLAS_CONTAINER_RUNTIME_TOKEN: "runtime-secret",
};

describe("VerglasCatalogClient", () => {
  it("projects active worker rows without returning executable configuration", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(new Response(JSON.stringify([{
      name: "daily-ingest",
      state: "running",
      placement: "local",
      output: "lake.events",
      triggers: '[{"type":"cron","schedule":"0 1 * * *"}]',
      created_by: "test",
      revision: 3,
      created_at: "2026-08-06T00:00:00Z",
      code: "secret executable details",
      config: "secret configuration",
    }]), {headers: {"content-type": "application/json"}}));

    const result = await new VerglasCatalogClient(env, fetcher).listWorkers();

    expect(fetcher).toHaveBeenCalledWith(
      "http://localhost:8334/v1/workers?view=active",
      expect.objectContaining({method: "GET"}),
    );
    expect(result).toEqual([{
      name: "daily-ingest",
      state: "running",
      placement: "local",
      output: "lake.events",
      triggers: '[{"type":"cron","schedule":"0 1 * * *"}]',
      createdBy: "test",
      revision: 3,
      createdAt: "2026-08-06T00:00:00Z",
    }]);
  });

  it("discovers bounded Iceberg namespaces and tables", async () => {
    const fetcher = vi.fn<typeof fetch>(async (input) => {
      const url = String(input);
      if (url.endsWith("/admin/access")) {
        return new Response(JSON.stringify({warehouse: "local"}));
      }
      if (url.includes("/catalog/v1/config?warehouse=local")) {
        return new Response(JSON.stringify({overrides: {prefix: "catalog-prefix"}}));
      }
      if (url.endsWith("/catalog/v1/catalog-prefix/namespaces")) {
        return new Response(JSON.stringify({namespaces: [["rlean"], ["agent_data"]]}));
      }
      if (url.endsWith("/namespaces/rlean/tables")) {
        return new Response(JSON.stringify({identifiers: [
          {namespace: ["rlean"], name: "runs"},
        ]}));
      }
      if (url.endsWith("/namespaces/agent_data/tables")) {
        return new Response(JSON.stringify({identifiers: [
          {namespace: ["agent_data"], name: "documents"},
        ]}));
      }
      return new Response("not found", {status: 404});
    });

    await expect(new VerglasCatalogClient(env, fetcher).listTables()).resolves.toEqual([
      {namespace: ["agent_data"], name: "documents", qualifiedName: '"agent_data"."documents"'},
      {namespace: ["rlean"], name: "runs", qualifiedName: '"rlean"."runs"'},
    ]);
  });

  it("keeps empty Iceberg namespaces visible as databases", async () => {
    const fetcher = vi.fn<typeof fetch>(async (input) => {
      const url = String(input);
      if (url.endsWith("/admin/access")) return Response.json({warehouse: "local"});
      if (url.includes("/catalog/v1/config?warehouse=local")) {
        return Response.json({overrides: {prefix: "catalog-prefix"}});
      }
      if (url.endsWith("/catalog/v1/catalog-prefix/namespaces")) {
        return Response.json({namespaces: [["empty"], ["events"]]});
      }
      if (url.endsWith("/namespaces/empty/tables")) return Response.json({identifiers: []});
      if (url.endsWith("/namespaces/events/tables")) {
        return Response.json({identifiers: [{namespace: ["events"], name: "log"}]});
      }
      if (url.endsWith("/v1/indexes")) return Response.json({indexes: []});
      return new Response("not found", {status: 404});
    });

    await expect(new VerglasCatalogClient(env, fetcher).getCatalog()).resolves.toMatchObject({
      databases: [
        {name: "empty", tableCount: 0, vectorCount: 0, graph: false},
        {name: "events", tableCount: 1, vectorCount: 0, graph: false},
      ],
    });
  });

  it("creates and deletes namespaces and explicit tables through the Iceberg catalog", async () => {
    const fetcher = vi.fn<typeof fetch>(async (input) => {
      const url = String(input);
      if (url.endsWith("/admin/access")) return Response.json({warehouse: "local"});
      if (url.includes("/catalog/v1/config?warehouse=local")) {
        return Response.json({overrides: {prefix: "catalog-prefix"}});
      }
      return new Response(null, {status: 204});
    });
    const client = new VerglasCatalogClient(env, fetcher);

    await client.createDatabase("analytics");
    await client.createTable({
      namespace: ["analytics"],
      name: "events",
      columns: [{name: "id", type: "int64", nullable: false}],
    });
    await client.deleteTable(["analytics"], "events");
    await client.deleteDatabase("analytics");

    const requests = fetcher.mock.calls.map(([input, init]) => [String(input), init?.method, init?.body]);
    expect(requests).toContainEqual([
      "http://localhost:8334/catalog/v1/catalog-prefix/namespaces",
      "POST",
      JSON.stringify({namespace: ["analytics"], properties: {}}),
    ]);
    expect(requests).toContainEqual([
      "http://localhost:8334/catalog/v1/catalog-prefix/namespaces/analytics/tables",
      "POST",
      JSON.stringify({
        name: "events",
        schema: {type: "struct", "schema-id": 0, fields: [
          {id: 1, name: "id", required: true, type: "long"},
        ]},
        "partition-spec": {"spec-id": 0, fields: []},
      }),
    ]);
    expect(requests).toContainEqual([
      "http://localhost:8334/catalog/v1/catalog-prefix/namespaces/analytics/tables/events",
      "DELETE",
      undefined,
    ]);
    expect(requests).toContainEqual([
      "http://localhost:8334/catalog/v1/catalog-prefix/namespaces/analytics",
      "DELETE",
      undefined,
    ]);
  });

  it("builds a lakehouse catalog with table, vector, and graph assets", async () => {
    const fetcher = vi.fn<typeof fetch>(async (input) => {
      const url = String(input);
      if (url.endsWith("/admin/access")) return new Response(JSON.stringify({warehouse: "local"}));
      if (url.includes("/catalog/v1/config?warehouse=local")) {
        return new Response(JSON.stringify({overrides: {prefix: "catalog-prefix"}}));
      }
      if (url.endsWith("/catalog/v1/catalog-prefix/namespaces")) {
        return new Response(JSON.stringify({namespaces: [["knowledge"], ["rlean"]]}));
      }
      if (url.endsWith("/namespaces/knowledge/tables")) {
        return new Response(JSON.stringify({identifiers: [
          {namespace: ["knowledge"], name: "nodes"},
          {namespace: ["knowledge"], name: "edges"},
        ]}));
      }
      if (url.endsWith("/namespaces/rlean/tables")) {
        return new Response(JSON.stringify({identifiers: [{namespace: ["rlean"], name: "runs"}]}));
      }
      if (url.endsWith("/v1/indexes")) {
        return new Response(JSON.stringify({indexes: [{
          target: "tbl:rlean.runs",
          field: "embedding",
          metric: "cosine",
          reflected_snapshot: 42,
          live_count: 1200,
        }]}));
      }
      return new Response("not found", {status: 404});
    });

    await expect(new VerglasCatalogClient(env, fetcher).getCatalog()).resolves.toMatchObject({
      tables: expect.arrayContaining([
        {namespace: ["rlean"], name: "runs", qualifiedName: '"rlean"."runs"'},
      ]),
      graphs: [{
        namespace: "knowledge",
        nodesTable: '"knowledge"."nodes"',
        edgesTable: '"knowledge"."edges"',
      }],
      vectors: [{
        target: "tbl:rlean.runs",
        field: "embedding",
        metric: "cosine",
        reflectedSnapshot: 42,
        liveCount: 1200,
      }],
    });
  });

  it("returns selected database tables with physical and cache-usage metrics", async () => {
    const fetcher = vi.fn<typeof fetch>(async (input) => {
      const url = String(input);
      if (url.endsWith("/admin/access")) return Response.json({warehouse: "local"});
      if (url.includes("/catalog/v1/config?warehouse=local")) {
        return Response.json({overrides: {prefix: "catalog-prefix"}});
      }
      if (url.endsWith("/catalog/v1/catalog-prefix/namespaces")) {
        return Response.json({namespaces: [["analytics"]]});
      }
      if (url.endsWith("/namespaces/analytics/tables")) {
        return Response.json({identifiers: [{namespace: ["analytics"], name: "events"}]});
      }
      if (url.endsWith("/v1/indexes")) return Response.json({indexes: []});
      if (url.endsWith("/v1/metering/tables")) return Response.json({tables: [{
        table: "analytics.events",
        hits: 9,
        misses: 1,
        bytes_served: 4096,
        cache_bytes: 3072,
        requests_avoided: 9,
        latency_saved_seconds: 0.25,
      }]});
      if (url.endsWith("/v1/tables/analytics.events/describe")) return Response.json({
        row_count: 42,
        file_count: 2,
        size_bytes: 8192,
        current_snapshot_id: 7,
      });
      return new Response("not found", {status: 404});
    });

    await expect(new VerglasCatalogClient(env, fetcher).getDatabase("analytics")).resolves.toMatchObject({
      name: "analytics",
      tables: [{
        name: "events",
        physical: {rowCount: 42, fileCount: 2, sizeBytes: 8192, currentSnapshotId: 7},
        usage: {hits: 9, bytesServed: 4096, cacheBytes: 3072},
      }],
    });
  });

  it("wraps read SQL with a limit and reports truncation", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(new Response(JSON.stringify({
      columns: ["value"],
      rows: [{value: 1}, {value: 2}, {value: 3}],
      row_count: 3,
    }), {headers: {"content-type": "application/json"}}));

    const result = await new VerglasCatalogClient(env, fetcher).query("SELECT value FROM numbers", 2);

    expect(fetcher).toHaveBeenCalledWith("http://localhost:8334/v1/query", expect.objectContaining({
      method: "POST",
      body: JSON.stringify({sql: "SELECT * FROM (SELECT value FROM numbers) AS __verglas_query LIMIT 3"}),
    }));
    expect(result).toEqual({
      columns: ["value"],
      rows: [{value: 1}, {value: 2}],
      rowCount: 2,
      truncated: true,
    });
  });

  it("authenticates Vessel discovery and exposes previews only for applications", async () => {
    const fetcher = vi.fn<typeof fetch>()
      .mockResolvedValueOnce(new Response(JSON.stringify([
        {name: "linear", role: "integration", image: "linear:local", state: "running", health: "ready"},
        {name: "dashboard", role: "application", image: "dashboard:local", state: "running", health: "ready"},
      ]), {headers: {"content-type": "application/json"}}))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        title: "Linear",
        description: "Connect Linear workspaces.",
      }), {headers: {"content-type": "application/json"}}));

    const result = await new VerglasCatalogClient(env, fetcher).listVessels();

    const requestInit = fetcher.mock.calls[0]?.[1];
    expect(requestInit).toBeDefined();
    expect(new Headers(requestInit!.headers).get("authorization"))
      .toBe("Bearer runtime-secret");
    expect(result[0]).toMatchObject({title: "Linear", description: "Connect Linear workspaces."});
    expect(result[0].previewUrl).toBeUndefined();
    expect(result[1].previewUrl).toBe("http://localhost:8360/apps/dashboard/");
  });

  it("persists lifecycle state only for Application Vessels", async () => {
    const fetcher = vi.fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json([
        {name: "dashboard", role: "application", image: "dashboard:local", health: "ready"},
      ]))
      .mockResolvedValueOnce(new Response(null, {status: 204}));

    await new VerglasCatalogClient(env, fetcher).setApplicationState("dashboard", "stopped");

    expect(fetcher.mock.calls.map(([input, init]) => [String(input), init?.method])).toEqual([
      ["http://localhost:8360/v1/vessels", "GET"],
      ["http://localhost:8360/v1/vessels/dashboard/stop", "POST"],
    ]);
  });

  it("combines an Integration Vessel's schema with its configured state", async () => {
    const fetcher = vi.fn<typeof fetch>()
      .mockResolvedValueOnce(new Response(JSON.stringify({
        title: "Linear",
        description: "Connect Linear.",
        instructions: ["Create a key."],
        fields: [{name: "apiToken", label: "API key", type: "password", required: true, secret: true}],
      }), {headers: {"content-type": "application/json"}}))
      .mockResolvedValueOnce(new Response(JSON.stringify({configured: true}), {
        headers: {"content-type": "application/json"},
      }));

    await expect(new VerglasCatalogClient(env, fetcher).getIntegrationConfiguration("linear"))
      .resolves.toMatchObject({title: "Linear", configured: true});
  });
});
