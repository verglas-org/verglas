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
