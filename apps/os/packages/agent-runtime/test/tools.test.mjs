import assert from "node:assert/strict";
import test from "node:test";
import {createToolExecutor} from "../src/tools.mjs";

test("listLakehouse discovers resources and uses only database-scoped catalogs", async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (input, init) => {
    const url = String(input);
    calls.push([url, init?.method ?? "GET"]);
    if (url === "http://access/v1/access/check") return Response.json({allowed: true});
    if (url === "http://access/v1/databases") return Response.json({databases: [{
      type: "lakehouse",
      name: "analytics",
      storage: {mode: "managed"},
      catalog: {mode: "managed-lakekeeper"},
    }, {
      type: "postgres",
      name: "operations",
      engine: {mode: "managed-neon"},
    }]});
    if (url === "http://data/v1/databases/analytics/catalog/v1/namespaces") {
      return Response.json({namespaces: [["events"]]});
    }
    if (url === "http://data/v1/databases/analytics/catalog/v1/namespaces/events/tables") {
      return Response.json({identifiers: [{namespace: ["events"], name: "log"}]});
    }
    return new Response("not found", {status: 404});
  };
  try {
    const execute = createToolExecutor({
      VERGLAS_DATA_ENDPOINT: "http://data",
      VERGLAS_DATA_TOKEN: "data-token",
      VERGLAS_CONTAINER_RUNTIME_URL: "http://runtime",
      VERGLAS_CONTAINER_RUNTIME_TOKEN: "runtime-token",
      VERGLAS_ACCESS_URI: "http://access",
      VERGLAS_ACCESS_SERVICE_TOKEN: "access-token",
      VERGLAS_TENANT_ID: "tenant",
      VERGLAS_AGENT_PRINCIPAL_ID: "agent/session",
    }, async () => {});

    const result = await execute("listLakehouse", {});

    assert.deepEqual(result.databases.map(({name, type}) => ({name, type})), [
      {name: "analytics", type: "lakehouse"},
      {name: "operations", type: "postgres"},
    ]);
    assert.deepEqual(result.tables, [{
      database: "analytics",
      namespace: ["events"],
      name: "log",
      qualifiedName: "events.log",
    }]);
    assert.equal(calls.length, 4);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
