import { describe, expect, it } from "vitest";
import {
  connectAccess,
  connectAdmin,
  connectRuntime,
  connectScheduler,
  extractWorkerSource,
} from "../src/control";

/** Records requests and returns JSON responses selected by pathname. */
function recordingFetch(
  requests: Request[],
  responses: Record<string, unknown> = {},
): typeof fetch {
  return (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const request = new Request(input, init);
    requests.push(request);
    const body = responses[new URL(request.url).pathname];
    return body === undefined
      ? new Response(null, { status: 204 })
      : Response.json(body);
  }) as typeof fetch;
}

describe("control-plane connectors", () => {
  it("requires a scoped bearer token for every control-plane connector", () => {
    const options = { endpoint: "http://verglas.test", token: "" };
    expect(() => connectAdmin(options)).toThrow(/token is required/);
    expect(() => connectAccess(options)).toThrow(/token is required/);
    expect(() => connectScheduler(options)).toThrow(/token is required/);
    expect(() => connectRuntime(options)).toThrow(/token is required/);
  });

  it("routes scheduler worker operations with bearer authorization", async () => {
    const requests: Request[] = [];
    const client = connectScheduler({
      endpoint: "http://scheduler.test/",
      token: "scheduler-token",
      fetch: recordingFetch(requests, { "/v1/workers": [] }),
    });

    await client.listWorkers("all");
    await client.registerWorker({
      name: "sync-linear",
      code: "typescript",
      triggers: "[]",
      output: "lakehouse.linear.issues",
      config: "{}",
      created_by: "test",
    });

    expect(requests.map((request) => [request.method, request.url])).toEqual([
      ["GET", "http://scheduler.test/v1/workers?view=all"],
      ["POST", "http://scheduler.test/v1/workers"],
    ]);
    expect(requests[0]?.headers.get("authorization")).toBe("Bearer scheduler-token");
    expect(await requests[1]?.json()).toMatchObject({ name: "sync-linear" });
  });

  it("routes admin SQL queries through their validated database scope", async () => {
    const requests: Request[] = [];
    const admin = connectAdmin({
      endpoint: "http://verglas.test",
      token: "scoped-admin-token",
      fetch: recordingFetch(requests, {
        "/v1/databases/analytics-dev/query": { columns: [], rows: [], row_count: 0 },
      }),
    });

    expect(() => admin.query("", "select 1")).toThrow(/database must begin/);
    expect(() => admin.query("analytics/dev", "select 1")).toThrow(/database must begin/);
    expect(() => admin.query("analytics", "")).toThrow(/sql is required/);
    await admin.query("analytics-dev", "select 1", {reference: "42", table: "events"});

    expect(await requests[0]?.json()).toEqual({
      sql: "select 1",
      at: {reference: "42", table: "events"},
    });

    expect(requests.map((request) => [request.method, new URL(request.url).pathname])).toEqual([
      ["POST", "/v1/databases/analytics-dev/query"],
    ]);
  });


  it("manages one-shot scoped access tokens without exposing their secret after creation", async () => {
    const requests: Request[] = [];
    const metadata = {
      id: "local-cli",
      tenant_id: "tenant-demo",
      parent_principal_id: "user-j-brown",
      principal_id: "token-local-cli",
      name: "Local CLI",
      audience: "data-plane",
      policy_version: 7,
      created_at: 1_785_628_800,
      expires_at: 1_788_221_600,
    };
    const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const request = new Request(input, init);
      requests.push(request);
      if (request.method === "POST" && new URL(request.url).pathname === "/v1/access/tokens") {
        return Response.json({token: "visible-once", ...metadata});
      }
      if (request.method === "GET" && new URL(request.url).pathname === "/v1/access/tokens") {
        return Response.json([metadata]);
      }
      return Response.json(metadata);
    }) as typeof globalThis.fetch;
    const access = connectAccess({
      endpoint: "http://access.test",
      token: "owner-token",
      fetch: fetchImpl,
    });

    const minted = await access.createToken({
      name: "Local CLI",
      audience: "data-plane",
      expires_in_seconds: 2_592_800,
      grants: [{resource_id: "database/analytics", actions: ["query", "describe"]}],
    });
    expect(minted.token).toBe("visible-once");
    expect(minted.id).toBe("local-cli");
    expect(await access.listTokens()).toHaveLength(1);
    await access.revokeToken("local-cli");

    expect(requests.map((request) => [request.method, new URL(request.url).pathname])).toEqual([
      ["POST", "/v1/access/tokens"],
      ["GET", "/v1/access/tokens"],
      ["DELETE", "/v1/access/tokens/local-cli"],
    ]);
    expect(requests.every((request) => request.headers.get("authorization") === "Bearer owner-token")).toBe(true);
    expect(await requests[0]?.json()).toEqual({
      name: "Local CLI",
      audience: "data-plane",
      expires_in_seconds: 2_592_800,
      grants: [{resource_id: "database/analytics", actions: ["query", "describe"]}],
    });
  });

  it("checks a database action against the presented token identity", async () => {
    const requests: Request[] = [];
    const access = connectAccess({
      endpoint: "http://access.test",
      token: "scoped-token",
      fetch: recordingFetch(requests, {
        "/v1/access/authorize": {
          identity: {
            tenant_id: "tenant-demo",
            principal_id: "token-local-cli",
            token_id: "local-cli",
            audience: "data-plane",
          },
          decision: {
            allowed: true,
            reason: "exact_grant",
            grant_id: "grant-analytics-read",
            matched_resource_id: "database/analytics",
            policy_version: 7,
          },
        },
      }),
    });

    const result = await access.authorize({
      audience: "data-plane",
      resource_id: "database/analytics",
      action: "query",
    });

    expect(result.identity.principal_id).toBe("token-local-cli");
    expect(result.decision.allowed).toBe(true);
    expect(await requests[0]?.json()).toEqual({
      audience: "data-plane",
      resource_id: "database/analytics",
      action: "query",
    });
    expect(requests[0]?.headers.get("authorization")).toBe("Bearer scoped-token");
  });

  it("mints one direct database credential without routing through a singleton database endpoint", async () => {
    const requests: Request[] = [];
    const access = connectAccess({
      endpoint: "http://access.test",
      token: "owner-token",
      fetch: recordingFetch(requests, {
        "/v1/access/database-tokens": {token: "database-secret", expires_at: 1_785_632_400},
      }),
    });

    const credential = await access.createDatabaseToken({
      database_id: "operations",
      expires_in_seconds: 3_600,
    });

    expect(credential).toEqual({token: "database-secret", expires_at: 1_785_632_400});
    expect(requests).toHaveLength(1);
    expect(requests[0]?.method).toBe("POST");
    expect(new URL(requests[0]?.url ?? "http://invalid.test").pathname).toBe("/v1/access/database-tokens");
    expect(requests[0]?.headers.get("authorization")).toBe("Bearer owner-token");
    expect(await requests[0]?.json()).toEqual({
      database_id: "operations",
      expires_in_seconds: 3_600,
    });
  });

  it("routes scheduler secrets and runtime Vessel requests", async () => {
    const requests: Request[] = [];
    const fetch = recordingFetch(requests, { "/v1/secrets": { secrets: ["linear-token"] } });
    const scheduler = connectScheduler({
      endpoint: "http://scheduler.test",
      token: "control-token",
      fetch,
    });
    const runtime = connectRuntime({
      endpoint: "http://runtime.test/",
      token: "control-token",
      fetch,
    });

    expect(await scheduler.listSecretNames()).toEqual(["linear-token"]);
    await runtime.putVesselProject("linear-app", { files: {} });

    expect(requests.map((request) => [request.method, request.url])).toEqual([
      ["GET", "http://scheduler.test/v1/secrets"],
      ["PUT", "http://runtime.test/v1/vessels/linear-app/project"],
    ]);
    expect(runtime.previewUrl("linear app")).toBe("http://runtime.test/apps/linear%20app/");
  });

  it("routes persisted Vessel lifecycle commands", async () => {
    const requests: Request[] = [];
    const runtime = connectRuntime({
      endpoint: "http://runtime.test",
      token: "control-token",
      fetch: recordingFetch(requests),
    });

    await runtime.stopVessel("warehouse ui");
    await runtime.resumeVessel("warehouse ui");

    expect(requests.map((request) => [request.method, new URL(request.url).pathname])).toEqual([
      ["POST", "/v1/vessels/warehouse%20ui/stop"],
      ["POST", "/v1/vessels/warehouse%20ui/resume"],
    ]);
  });

  it("routes typed dynamic database CRUD without exposing tenant or secret identifiers", async () => {
    const requests: Request[] = [];
    const database = {
      type: "lakehouse" as const,
      name: "analytics",
      storage: { mode: "managed" as const },
      catalog: { mode: "managed-lakekeeper" as const },
    };
    const access = connectAccess({
      endpoint: "http://access.test",
      token: "access-token",
      fetch: recordingFetch(requests, {
        "/v1/databases": { databases: [database] },
        "/v1/databases/analytics": database,
      }),
    });

    expect(await access.listDatabases()).toEqual([database]);
    expect(await access.getDatabase("analytics")).toEqual(database);
    await access.createDatabase({
      type: "postgres",
      name: "operational",
      engine: { mode: "managed-neon" },
    });
    await access.deleteDatabase("analytics");

    expect(requests.map((request) => [request.method, new URL(request.url).pathname])).toEqual([
      ["GET", "/v1/databases"],
      ["GET", "/v1/databases/analytics"],
      ["POST", "/v1/databases"],
      ["DELETE", "/v1/databases/analytics"],
    ]);
    expect(requests[0]?.headers.get("authorization")).toBe("Bearer access-token");
  });
});

describe("extractWorkerSource", () => {
  it("selects a conventional TypeScript entrypoint and tolerates invalid JSON", () => {
    expect(
      extractWorkerSource(JSON.stringify({ files: { "src/worker.ts": "export default 1" } })),
    ).toBe("export default 1");
    expect(extractWorkerSource("not-json")).toBeUndefined();
  });
});
