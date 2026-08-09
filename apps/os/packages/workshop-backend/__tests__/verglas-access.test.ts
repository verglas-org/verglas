import {describe, expect, it, vi} from "vitest";
import {
  resolveVerglasAccessConfig,
  userPrincipalId,
  VerglasAccessClient,
} from "../src/verglas-access.js";

describe("VerglasAccessClient", () => {
  it("requires endpoint and backend credential together", () => {
    expect(resolveVerglasAccessConfig({})).toBeNull();
    expect(() => resolveVerglasAccessConfig({VERGLAS_ACCESS_URI: "http://access:8345"}))
      .toThrow(/must be configured together/);
  });

  it("maps one local OS user to a stable tenant owner without exposing the service token", async () => {
    const fetcher = vi.fn<typeof fetch>(async (input, init) => {
      const path = new URL(String(input)).pathname;
      expect(new Headers(init?.headers).get("authorization")).toBe("Bearer service-secret");
      if (path === "/v1/access/check") {
        return new Response(JSON.stringify({allowed: true, policy_version: 4}));
      }
      return new Response(JSON.stringify({}), {status: 201});
    });
    const config = resolveVerglasAccessConfig({
      VERGLAS_ACCESS_URI: "http://access:8345/",
      VERGLAS_ACCESS_SERVICE_TOKEN: "service-secret",
      VERGLAS_TENANT_ID: "tenant-a",
      VERGLAS_LOCAL_OWNER_BOOTSTRAP: "true",
    });
    expect(config).not.toBeNull();

    await expect(new VerglasAccessClient(config!, fetcher).ensureUser("Person@Example.com"))
      .resolves.toEqual({
        tenantId: "tenant-a",
        principalId: "user/person%40example.com",
        tenantOwner: true,
        policyVersion: 4,
      });
    expect(fetcher).toHaveBeenCalledTimes(4);
  });

  it("sends user-approved grants through delegation rather than unrestricted grant creation", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(new Response(JSON.stringify({
      id: "delegated/1",
      tenant_id: "tenant-a",
      principal_id: "job/ingest",
      resource_id: "table/raw.events",
      actions: ["query"],
    }), {status: 201}));
    const config = resolveVerglasAccessConfig({
      VERGLAS_ACCESS_URI: "http://access:8345",
      VERGLAS_ACCESS_SERVICE_TOKEN: "service-secret",
      VERGLAS_TENANT_ID: "tenant-a",
    })!;

    await new VerglasAccessClient(config, fetcher).delegate("owner", {
      principalId: "job/ingest",
      resourceId: "table/raw.events",
      actions: ["query"],
    });

    const [url, init] = fetcher.mock.calls[0];
    expect(String(url)).toBe("http://access:8345/v1/access/delegations");
    expect(JSON.parse(String(init?.body))).toMatchObject({
      actor_principal_id: userPrincipalId("owner"),
      grant: {
        principal_id: "job/ingest",
        resource_id: "table/raw.events",
        actions: ["query"],
      },
    });
  });
});
