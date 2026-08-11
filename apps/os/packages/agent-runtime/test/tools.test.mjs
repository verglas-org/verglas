import assert from "node:assert/strict";
import test from "node:test";
import {createToolExecutor} from "../src/tools.mjs";

test("permission requests reject resource identifiers the access service cannot grant", async () => {
  const emitted = [];
  const execute = createToolExecutor({
    VERGLAS_DATA_ENDPOINT: "http://data",
    VERGLAS_CONTAINER_RUNTIME_URL: "http://runtime",
    VERGLAS_ACCESS_URI: "http://access",
    VERGLAS_TOKEN: "scoped-token",
    VERGLAS_AGENT_PRINCIPAL_ID: "agent/session",
    VERGLAS_AGENT_CHAT_ID: "1",
  }, async message => emitted.push(message));

  await assert.rejects(
    execute("requestPermission", {
      resourceId: "tool:listLakehouse",
      actions: ["execute"],
      reason: "Inspect the lakehouse.",
    }),
    /valid resource identifier/,
  );
  assert.deepEqual(emitted, []);
});

test("listLakehouse discovers resources and uses only database-scoped catalogs", async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (input, init) => {
    const url = String(input);
    calls.push([url, init?.method ?? "GET", init?.body, init?.headers]);
    if (url === "http://access/v1/access/authorize") return Response.json({allowed: true});
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
      VERGLAS_CONTAINER_RUNTIME_URL: "http://runtime",
      VERGLAS_ACCESS_URI: "http://access",
      VERGLAS_TOKEN: "scoped-token",
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
    assert.equal(calls.length, 5);
    assert.ok(calls.every(([url]) => url.startsWith("http://access") || url.startsWith("http://data")));
    assert.ok(calls.every(([, , , headers]) => headers.Authorization === "Bearer scoped-token"));
    assert.deepEqual(calls.filter(([url]) => url === "http://access/v1/access/authorize")
      .map(([, , body]) => JSON.parse(body)), [
      {audience: "data-plane", resource_id: "tenant", action: "discover"},
      {audience: "data-plane", resource_id: "database/analytics", action: "describe"},
    ]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("queryLakehouse sends SQL only to the selected database runtime", async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (input, init) => {
    const url = String(input);
    calls.push([url, init?.method ?? "GET", init?.body, init?.headers]);
    if (url === "http://access/v1/access/authorize") return Response.json({allowed: true});
    if (url === "http://access/v1/databases/analytics") return Response.json({
      type: "lakehouse", name: "analytics", storage: {mode: "managed"},
      catalog: {mode: "managed-lakekeeper"},
    });
    if (url === "http://data/v1/databases/analytics/query") {
      return Response.json({columns: ["id"], rows: [{id: 1}], row_count: 1});
    }
    return new Response("not found", {status: 404});
  };
  try {
    const execute = createToolExecutor({
      VERGLAS_DATA_ENDPOINT: "http://data",
      VERGLAS_CONTAINER_RUNTIME_URL: "http://runtime",
      VERGLAS_ACCESS_URI: "http://access",
      VERGLAS_TOKEN: "scoped-token",
      VERGLAS_AGENT_PRINCIPAL_ID: "agent/session",
    }, async () => {});

    const result = await execute("queryLakehouse", {database: "analytics", sql: "SELECT 1 AS id"});

    assert.deepEqual(result, {columns: ["id"], rows: [{id: 1}], row_count: 1});
    assert.deepEqual(calls.at(-1), [
      "http://data/v1/databases/analytics/query",
      "POST",
      JSON.stringify({sql: "SELECT 1 AS id"}),
      {
        Authorization: "Bearer scoped-token",
        "Content-Type": "application/json",
      },
    ]);
    assert.deepEqual(calls[0].slice(0, 3), [
      "http://access/v1/access/authorize",
      "POST",
      JSON.stringify({audience: "data-plane", resource_id: "database/analytics", action: "query"}),
    ]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
