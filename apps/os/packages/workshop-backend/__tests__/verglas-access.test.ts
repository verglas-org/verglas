import {describe, expect, it, vi} from "vitest";
import {
  resolveVerglasAccessConfig,
  userPrincipalId,
  VerglasAccessClient,
} from "../src/verglas-access.js";

const ASSERTION_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

describe("VerglasAccessClient", () => {
  it("requires an endpoint and identity assertion key without accepting a service token", () => {
    expect(resolveVerglasAccessConfig({})).toBeNull();
    expect(() => resolveVerglasAccessConfig({VERGLAS_ACCESS_URI: "http://access:8345"}))
      .toThrow(/identity assertion key/);
    expect(resolveVerglasAccessConfig({
      VERGLAS_ACCESS_URI: "http://access:8345/",
      VERGLAS_IDENTITY_ASSERTION_KEY: ASSERTION_KEY,
      VERGLAS_TENANT_ID: "tenant-a",
    })).toEqual({
      endpoint: "http://access:8345",
      identityAssertionKey: ASSERTION_KEY,
      tenantId: "tenant-a",
    });
  });

  it("exchanges a signed OS identity assertion before making actor-free access requests", async () => {
    const requests: Array<{path: string; init?: RequestInit}> = [];
    const sessionAudiences: string[] = [];
    const fetcher = vi.fn<typeof fetch>(async (input, init) => {
      const path = new URL(String(input)).pathname;
      requests.push({path, init});
      if (path === "/v1/access/sessions") {
        expect(new Headers(init?.headers).has("authorization")).toBe(false);
        const {assertion, audience} = JSON.parse(String(init?.body)) as {
          assertion: string;
          audience: string;
        };
        expect(["access", "data-plane"]).toContain(audience);
        sessionAudiences.push(audience);
        const [, encodedPayload] = assertion.split(".");
        const claims = JSON.parse(Buffer.from(encodedPayload!, "base64url").toString()) as {
          sub: string;
          tenant_id: string;
          aud: string;
          exp: number;
          iat: number;
        };
        expect(claims).toMatchObject({
          sub: userPrincipalId("Person@Example.com"),
          tenant_id: "tenant-a",
          aud: "verglas-access",
        });
        expect(claims.exp - claims.iat).toBe(60);
        return Response.json({token: `${audience}-session-token`, expires_at: claims.exp + 840});
      }
      expect(new Headers(init?.headers).get("authorization")).toBe("Bearer access-session-token");
      if (path === "/v1/access/authorize") {
        expect(JSON.parse(String(init?.body))).toEqual({
          audience: "access",
          resource_id: "tenant",
          action: "own",
        });
        return Response.json({
          identity: {
            tenant_id: "tenant-a",
            principal_id: userPrincipalId("Person@Example.com"),
            token_id: "session/1",
            audience: "access",
          },
          decision: {allowed: true, policy_version: 4},
        });
      }
      return Response.json({}, {status: 201});
    });
    const config = resolveVerglasAccessConfig({
      VERGLAS_ACCESS_URI: "http://access:8345/",
      VERGLAS_IDENTITY_ASSERTION_KEY: ASSERTION_KEY,
      VERGLAS_TENANT_ID: "tenant-a",
    });
    expect(config).not.toBeNull();

    const access = new VerglasAccessClient(config!, "Person@Example.com", fetcher);
    await expect(access.identity())
      .resolves.toEqual({
        tenantId: "tenant-a",
        principalId: "user/person@example.com",
        tenantOwner: true,
        policyVersion: 4,
      });
    await expect(access.sessionToken("data-plane")).resolves.toBe("data-plane-session-token");
    await expect(access.sessionToken("access")).resolves.toBe("access-session-token");
    expect(sessionAudiences).toEqual(["access", "data-plane"]);
    expect(requests.map(({path}) => path)).toEqual([
      "/v1/access/sessions",
      "/v1/access/authorize",
      "/v1/access/sessions",
    ]);
  });

  it("creates, lists, and revokes tokens without accepting an actor principal", async () => {
    const fetcher = vi.fn<typeof fetch>(async (input, init) => {
      const url = new URL(String(input));
      if (url.pathname === "/v1/access/sessions") {
        return Response.json({token: "session-token", expires_at: 9_999_999_999});
      }
      if (url.pathname === "/v1/access/tokens" && init?.method === "POST") {
        const body = JSON.parse(String(init.body));
        expect(body).toEqual({
          name: "Local CLI",
          audience: "data-plane",
          expires_in_seconds: 3600,
          grants: [{resource_id: "database/analytics", actions: ["query"]}],
        });
        expect(body.actor_principal_id).toBeUndefined();
        return Response.json({
          token: "plain-token-shown-once",
          id: "token/cli",
          name: "Local CLI",
          principal_id: "token/cli",
          parent_principal_id: "user/owner@example.com",
          audience: "data-plane",
          created_at: 100,
          expires_at: 3700,
        }, {status: 201});
      }
      if (url.pathname === "/v1/access/tokens" && (init?.method === undefined || init.method === "GET")) {
        return Response.json([{id: "token/cli", name: "Local CLI", principal_id: "token/cli",
          parent_principal_id: "user/owner@example.com",
          audience: "data-plane", created_at: 100, expires_at: 3700}]);
      }
      if (url.pathname === "/v1/access/tokens/token%2Fcli" && init?.method === "DELETE") {
        return new Response(null, {status: 204});
      }
      return new Response("unexpected request", {status: 500});
    });
    const config = resolveVerglasAccessConfig({
      VERGLAS_ACCESS_URI: "http://access:8345",
      VERGLAS_IDENTITY_ASSERTION_KEY: ASSERTION_KEY,
      VERGLAS_TENANT_ID: "tenant-a",
    })!;
    const access = new VerglasAccessClient(config, "owner@example.com", fetcher);

    await expect(access.createToken({
      name: "Local CLI",
      audience: "data-plane",
      expiresInSeconds: 3600,
      grants: [{resourceId: "database/analytics", actions: ["query"]}],
    })).resolves.toMatchObject({token: "plain-token-shown-once", id: "token/cli"});
    await expect(access.listTokens()).resolves.toEqual([expect.objectContaining({id: "token/cli"})]);
    await expect(access.revokeToken("token/cli")).resolves.toBeUndefined();
  });

  it("derives the actor and tenant instead of accepting them in delegation bodies", async () => {
    const fetcher = vi.fn<typeof fetch>(async (input, init) => {
      const path = new URL(String(input)).pathname;
      if (path === "/v1/access/sessions") {
        return Response.json({token: "session-token", expires_at: 9_999_999_999});
      }
      if (path === "/v1/access/grants") return Response.json([]);
      const body = JSON.parse(String(init?.body));
      expect(body.actor_principal_id).toBeUndefined();
      expect(body.tenant_id).toBeUndefined();
      expect(body.grant.actor_principal_id).toBeUndefined();
      expect(body.grant.tenant_id).toBeUndefined();
      return Response.json({
        id: body.grant.id,
        tenant_id: "tenant-a",
        principal_id: body.grant.principal_id,
        resource_id: body.grant.resource_id,
        actions: body.grant.actions,
      }, {status: 201});
    });
    const config = resolveVerglasAccessConfig({
      VERGLAS_ACCESS_URI: "http://access:8345",
      VERGLAS_IDENTITY_ASSERTION_KEY: ASSERTION_KEY,
      VERGLAS_TENANT_ID: "tenant-a",
    })!;

    await expect(new VerglasAccessClient(config, "owner@example.com", fetcher).delegate({
      principalId: "job/ingest",
      resourceId: "database/analytics",
      actions: ["query"],
    })).resolves.toMatchObject({
      tenantId: "tenant-a",
      principalId: "job/ingest",
      resourceId: "database/analytics",
      actions: ["query"],
    });
  });

  it("reuses an existing grant that already covers the requested delegation", async () => {
    const existing = {
      id: "delegated/existing",
      tenant_id: "tenant-a",
      principal_id: "agent/workspace-1",
      resource_id: "tenant",
      actions: ["discover", "describe"],
    };
    const fetcher = vi.fn<typeof fetch>(async (input, init) => {
      const url = new URL(String(input));
      if (url.pathname === "/v1/access/sessions") {
        return Response.json({token: "session-token", expires_at: 9_999_999_999});
      }
      if (url.pathname === "/v1/access/grants" && init?.method === undefined) {
        expect(url.searchParams.get("principal_id")).toBe("agent/workspace-1");
        return Response.json([existing]);
      }
      return new Response("duplicate delegation must not be posted", {status: 500});
    });
    const config = resolveVerglasAccessConfig({
      VERGLAS_ACCESS_URI: "http://access:8345",
      VERGLAS_IDENTITY_ASSERTION_KEY: ASSERTION_KEY,
      VERGLAS_TENANT_ID: "tenant-a",
    })!;

    await expect(new VerglasAccessClient(config, "owner@example.com", fetcher).delegate({
      principalId: "agent/workspace-1",
      resourceId: "tenant",
      actions: ["discover"],
    })).resolves.toMatchObject({
      id: "delegated/existing",
      actions: ["discover", "describe"],
    });
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it("replaces a partial grant with the union of its existing and requested actions", async () => {
    const requests: Array<{path: string; body?: unknown}> = [];
    const fetcher = vi.fn<typeof fetch>(async (input, init) => {
      const url = new URL(String(input));
      if (url.pathname === "/v1/access/sessions") {
        return Response.json({token: "session-token", expires_at: 9_999_999_999});
      }
      if (url.pathname === "/v1/access/grants") {
        return Response.json([{
          id: "delegated/existing",
          tenant_id: "tenant-a",
          principal_id: "agent/workspace-1",
          resource_id: "tenant",
          actions: ["discover"],
        }]);
      }
      requests.push({
        path: url.pathname,
        body: init?.body ? JSON.parse(String(init.body)) : undefined,
      });
      if (url.pathname === "/v1/access/revocations") return new Response(null, {status: 204});
      if (url.pathname === "/v1/access/delegations") {
        const body = JSON.parse(String(init?.body));
        return Response.json({
          id: body.grant.id,
          tenant_id: "tenant-a",
          principal_id: body.grant.principal_id,
          resource_id: body.grant.resource_id,
          actions: body.grant.actions,
        }, {status: 201});
      }
      return new Response("unexpected request", {status: 500});
    });
    const config = resolveVerglasAccessConfig({
      VERGLAS_ACCESS_URI: "http://access:8345",
      VERGLAS_IDENTITY_ASSERTION_KEY: ASSERTION_KEY,
      VERGLAS_TENANT_ID: "tenant-a",
    })!;

    await expect(new VerglasAccessClient(config, "owner@example.com", fetcher).delegate({
      principalId: "agent/workspace-1",
      resourceId: "tenant",
      actions: ["describe"],
    })).resolves.toMatchObject({actions: ["discover", "describe"]});
    expect(requests).toEqual([{
      path: "/v1/access/revocations",
      body: {grant_id: "delegated/existing"},
    }, {
      path: "/v1/access/delegations",
      body: {grant: expect.objectContaining({actions: ["discover", "describe"]})},
    }]);
  });
});
