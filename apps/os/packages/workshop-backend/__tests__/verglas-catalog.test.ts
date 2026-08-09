import {describe, expect, it, vi} from "vitest";
import {VerglasCatalogClient} from "../src/verglas-catalog.js";

const env = {
  VERGLAS_ACCESS_URI: "http://localhost:8345/",
  VERGLAS_ACCESS_SERVICE_TOKEN: "access-secret",
  VERGLAS_ADMIN_URL: "http://localhost:8334/",
  VERGLAS_SCHEDULER_URL: "http://localhost:8340/",
  VERGLAS_SCHEDULER_CONTROL_TOKEN: "scheduler-secret",
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
      "http://localhost:8340/v1/workers?view=active",
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

  it("discovers databases as resources and scopes Iceberg tables to their lakehouse", async () => {
    const fetcher = vi.fn<typeof fetch>(async (input) => {
      const url = String(input);
      if (url.endsWith("/v1/databases")) {
        return Response.json({databases: [{
          type: "lakehouse",
          name: "analytics",
          storage: {mode: "managed"},
          catalog: {mode: "managed-lakekeeper"},
        }, {
          type: "postgres",
          name: "operations",
          engine: {mode: "managed-neon"},
        }]});
      }
      if (url.endsWith("/v1/databases/analytics/catalog/v1/config")) {
        return Response.json({defaults: {prefix: "analytics-warehouse"}});
      }
      if (url.endsWith("/v1/databases/analytics/catalog/v1/analytics-warehouse/namespaces")) {
        return Response.json({namespaces: [["events"], ["knowledge"]]});
      }
      if (url.endsWith("/v1/databases/analytics/catalog/v1/analytics-warehouse/namespaces/events/tables")) {
        return Response.json({identifiers: [{namespace: ["events"], name: "log"}]});
      }
      if (url.endsWith("/v1/databases/analytics/catalog/v1/analytics-warehouse/namespaces/knowledge/tables")) {
        return Response.json({identifiers: [
          {namespace: ["knowledge"], name: "nodes"},
          {namespace: ["knowledge"], name: "edges"},
        ]});
      }
      return new Response("not found", {status: 404});
    });

    await expect(new VerglasCatalogClient(env, fetcher).getCatalog()).resolves.toMatchObject({
      databases: [
        {
          type: "lakehouse",
          name: "analytics",
          tableCount: 3,
          capabilities: {catalog: true, tableCrud: true, tableMetrics: false, vectors: false, graphs: true, query: true},
        },
        {
          type: "postgres",
          name: "operations",
          tableCount: 0,
          capabilities: {catalog: false, tableCrud: false, tableMetrics: false, vectors: false, graphs: false, query: false},
        },
      ],
      tables: [
        {database: "analytics", namespace: ["events"], name: "log"},
        {database: "analytics", namespace: ["knowledge"], name: "edges"},
        {database: "analytics", namespace: ["knowledge"], name: "nodes"},
      ],
      graphs: [{database: "analytics", namespace: "knowledge"}],
      vectors: [],
    });
    expect(fetcher).toHaveBeenCalledTimes(5);
  });

  it("creates database resources and manages tables through the selected database catalog", async () => {
    const fetcher = vi.fn<typeof fetch>(async (input) => {
      const url = String(input);
      if (url.endsWith("/v1/databases/analytics")) return Response.json({
        type: "lakehouse",
        name: "analytics",
        storage: {mode: "managed"},
        catalog: {mode: "managed-lakekeeper"},
      });
      if (url.endsWith("/v1/databases/analytics/catalog/v1/config")) {
        return Response.json({defaults: {prefix: "analytics-warehouse"}});
      }
      if (url.endsWith("/v1/databases/analytics/catalog/v1/analytics-warehouse/namespaces")) {
        return Response.json({namespaces: [["events"]]});
      }
      if (url.endsWith("/v1/databases/analytics/catalog/v1/analytics-warehouse/namespaces/events/tables")) {
        return Response.json({identifiers: []});
      }
      if (url.endsWith("/v1/databases") && !input.toString().endsWith("/analytics")) {
        return Response.json({
          type: "lakehouse",
          name: "analytics",
          storage: {mode: "managed"},
          catalog: {mode: "managed-lakekeeper"},
        }, {status: 201});
      }
      return new Response(null, {status: 204});
    });
    const client = new VerglasCatalogClient(env, fetcher);

    await client.createDatabase({
      type: "lakehouse",
      name: "analytics",
      storage: {mode: "managed"},
      catalog: {mode: "managed-lakekeeper"},
    });
    await client.createTable({
      database: "analytics",
      namespace: ["events"],
      name: "events",
      columns: [{name: "id", type: "int64", nullable: false}],
    });
    await client.deleteTable("analytics", ["events"], "events");
    await client.deleteDatabase("analytics");

    const requests = fetcher.mock.calls.map(([input, init]) => [String(input), init?.method, init?.body]);
    expect(requests).toContainEqual([
      "http://localhost:8345/v1/databases",
      "POST",
      JSON.stringify({
        type: "lakehouse",
        name: "analytics",
        storage: {mode: "managed"},
        catalog: {mode: "managed-lakekeeper"},
      }),
    ]);
    expect(requests).toContainEqual([
      "http://localhost:8334/v1/databases/analytics/catalog/v1/analytics-warehouse/namespaces/events/tables",
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
      "http://localhost:8334/v1/databases/analytics/catalog/v1/analytics-warehouse/namespaces/events/tables/events",
      "DELETE",
      undefined,
    ]);
    expect(requests).toContainEqual([
      "http://localhost:8345/v1/databases/analytics",
      "DELETE",
      undefined,
    ]);
  });

  it("rejects deleting a non-empty lakehouse before calling the access service delete", async () => {
    const fetcher = vi.fn<typeof fetch>(async (input) => {
      const url = String(input);
      if (url.endsWith("/v1/databases/analytics")) return Response.json({
        type: "lakehouse", name: "analytics", storage: {mode: "managed"},
        catalog: {mode: "managed-lakekeeper"},
      });
      if (url.endsWith("/v1/databases/analytics/catalog/v1/config")) {
        return Response.json({defaults: {prefix: "analytics-warehouse"}});
      }
      if (url.endsWith("/v1/databases/analytics/catalog/v1/analytics-warehouse/namespaces")) {
        return Response.json({namespaces: [["events"]]});
      }
      if (url.endsWith("/v1/databases/analytics/catalog/v1/analytics-warehouse/namespaces/events/tables")) {
        return Response.json({identifiers: [{namespace: ["events"], name: "log"}]});
      }
      return new Response("not found", {status: 404});
    });

    await expect(new VerglasCatalogClient(env, fetcher).deleteDatabase("analytics"))
      .rejects.toThrow("contains 1 table");
    expect(fetcher.mock.calls.some(([input, init]) =>
      String(input).endsWith("/v1/databases/analytics") && init?.method === "DELETE")).toBe(false);
  });

  it("creates a missing namespace as part of the first table creation", async () => {
    const fetcher = vi.fn<typeof fetch>(async (input) => {
      const url = String(input);
      if (url.endsWith("/v1/databases/analytics")) return Response.json({
        type: "lakehouse", name: "analytics", storage: {mode: "managed"},
        catalog: {mode: "managed-lakekeeper"},
      });
      if (url.endsWith("/v1/databases/analytics/catalog/v1/config")) {
        return Response.json({defaults: {prefix: "analytics-warehouse"}});
      }
      if (url.endsWith("/v1/databases/analytics/catalog/v1/analytics-warehouse/namespaces")) {
        return Response.json({namespaces: []});
      }
      return new Response(null, {status: 204});
    });

    await new VerglasCatalogClient(env, fetcher).createTable({
      database: "analytics",
      namespace: ["events"],
      name: "log",
      columns: [{name: "id", type: "int64"}],
    });

    expect(fetcher.mock.calls.map(([input, init]) => [String(input), init?.method, init?.body]))
      .toContainEqual([
        "http://localhost:8334/v1/databases/analytics/catalog/v1/analytics-warehouse/namespaces",
        "POST",
        JSON.stringify({namespace: ["events"], properties: {}}),
      ]);
  });

  it("reports Postgres catalog operations as unsupported without probing Iceberg routes", async () => {
    const fetcher = vi.fn<typeof fetch>().mockImplementation(async () => Response.json({
      type: "postgres", name: "operations", engine: {mode: "managed-neon"},
    }));
    const client = new VerglasCatalogClient(env, fetcher);

    await expect(client.getDatabase("operations")).resolves.toMatchObject({
      type: "postgres",
      name: "operations",
      capabilities: {catalog: false, tableCrud: false, query: false},
      tables: [],
    });
    await expect(client.createTable({
      database: "operations",
      namespace: ["public"],
      name: "events",
      columns: [{name: "id", type: "int64"}],
    })).rejects.toThrow("does not expose Iceberg table management");
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it("wraps read SQL with a limit on the selected Lakehouse query route", async () => {
    const fetcher = vi.fn<typeof fetch>(async (input) => {
      if (String(input).endsWith("/v1/databases/analytics")) return Response.json({
        type: "lakehouse", name: "analytics", storage: {mode: "managed"},
        catalog: {mode: "managed-lakekeeper"},
      });
      return Response.json({
        columns: ["value"],
        rows: [{value: 1}, {value: 2}, {value: 3}],
        row_count: 3,
      });
    });

    const result = await new VerglasCatalogClient(env, fetcher)
      .query("analytics", "SELECT value FROM numbers", 2);

    expect(fetcher).toHaveBeenCalledWith("http://localhost:8334/v1/databases/analytics/query", expect.objectContaining({
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

  it("rejects Postgres queries without probing the data service", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json({
      type: "postgres", name: "operations", engine: {mode: "managed-neon"},
    }));

    await expect(new VerglasCatalogClient(env, fetcher).query("operations", "SELECT 1"))
      .rejects.toThrow("does not expose SQL query execution");
    expect(fetcher).toHaveBeenCalledTimes(1);
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
