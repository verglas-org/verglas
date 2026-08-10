import {describe, expect, it, vi} from "vitest";
import {
  IntegrationVerificationFailed,
  VerglasIntegrationRuntimeClient,
  parseIntegrationVerificationResult,
  validateApplicationProject,
  validateGeneratedIntegrationModule,
} from "../src/verglas-integration-runtime";

const env = {
  VERGLAS_CONTAINER_RUNTIME_URL: "http://runtime:8360",
  VERGLAS_CONTAINER_RUNTIME_TOKEN: "runtime-token",
  VERGLAS_DATA_ENDPOINT: "http://verglas:8334",
};

const module = `export default {
  async verify(ctx) { return {ok: Boolean(ctx.config.API_KEY), message: "verified"}; },
  async start(ctx) { await ctx.emit({type: "example.event", data: {ok: true}}); }
};`;

describe("VerglasIntegrationRuntimeClient", () => {
  it("requires a user-scoped token argument instead of reading a deployment-wide data token", () => {
    expect(() => new VerglasIntegrationRuntimeClient(env, vi.fn<typeof fetch>()))
      .toThrow("user-scoped Verglas token");
  });

  it("rejects generated modules without a real verifier and executable surface", () => {
    expect(() => validateGeneratedIntegrationModule("export default {}"))
      .toThrow("verify(ctx)");
  });

  it("deploys source and setup metadata into an isolated Integration Vessel", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(new Response("{}"));
    await new VerglasIntegrationRuntimeClient(env, fetcher, "data-token").deploy({
      name: "ais",
      title: "AIS Stream",
      description: "Streams vessel positions.",
      module,
      instructions: [{title: "Create a key", description: "Register for a free key."}],
      fields: [{name: "API_KEY", label: "API key", type: "secret", required: true}],
    });

    const [url, init] = fetcher.mock.calls[0];
    expect(url).toBe("http://runtime:8360/v1/vessels/ais");
    expect(new Headers(init?.headers).get("authorization")).toBe("Bearer runtime-token");
    const body = JSON.parse(String(init?.body));
    expect(body.role).toBe("integration");
    expect(body.entrypoint).toEqual([
      "/usr/local/bin/bun",
      "/opt/verglas-integration-runtime/runtime.mjs",
    ]);
    expect(body.environment.VERGLAS_TOKEN).toBe("data-token");
    expect(body.environment.VERGLAS_INTEGRATION_MODULE).not.toContain("API_KEY");
  });

  it("returns only a passing live verification result", async () => {
    const verification = {
      ok: true,
      message: "Connected to AISStream",
      testedAt: "2026-08-06T00:00:00Z",
      latencyMs: 42,
    };
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(
      Response.json({configured: true, verification}),
    );

    await expect(new VerglasIntegrationRuntimeClient(env, fetcher, "data-token")
      .configure("ais", {API_KEY: "secret"})).resolves.toEqual(verification);
  });

  it("preserves structured verification details on failure", async () => {
    const verification = {
      ok: false,
      message: "AISStream verify timed out",
      testedAt: "2026-08-07T00:00:00Z",
      latencyMs: 12004,
      details: {wsOpen: true, subscribed: true, firstMessageType: null, timeoutMs: 12000},
    };
    const fetcher = vi.fn<typeof fetch>().mockImplementation(() =>
      Promise.resolve(
        Response.json({configured: false, verification, error: verification.message}, {status: 422}),
      ));

    try {
      await new VerglasIntegrationRuntimeClient(env, fetcher, "data-token").test("ais");
      expect.unreachable("expected IntegrationVerificationFailed");
    } catch (error) {
      expect(error).toBeInstanceOf(IntegrationVerificationFailed);
      expect((error as IntegrationVerificationFailed).verification).toEqual(verification);
    }
  });

  it("parseIntegrationVerificationResult keeps details when ok is false", () => {
    expect(() => parseIntegrationVerificationResult({
      configured: false,
      verification: {
        ok: false,
        message: "timed out",
        testedAt: "2026-08-07T00:00:00Z",
        details: {wsOpen: true},
      },
    }, "test Integration", false)).toThrow(IntegrationVerificationFailed);

    try {
      parseIntegrationVerificationResult({
        configured: false,
        verification: {
          ok: false,
          message: "timed out",
          testedAt: "2026-08-07T00:00:00Z",
          details: {wsOpen: true},
        },
      }, "test Integration", false);
    } catch (error) {
      expect((error as IntegrationVerificationFailed).verification.details).toEqual({wsOpen: true});
    }
  });

  it("builds an Application project with declared npm dependencies", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(new Response("{}"));
    const previewUrl = await new VerglasIntegrationRuntimeClient(env, fetcher, "data-token").deployApplication({
      name: "shipping-map",
      files: {
        "package.json": JSON.stringify({
          scripts: {start: "bun src/server.ts", build: "vite build"},
          dependencies: {"@deck.gl/core": "9.1.14", vite: "7.1.1"},
        }),
        "src/server.ts": "Bun.serve({port: Number(process.env.PORT), fetch: () => new Response('ok')});",
      },
    });

    expect(previewUrl).toBe("http://runtime:8360/apps/shipping-map/");
    const [url, init] = fetcher.mock.calls[0];
    expect(url).toBe("http://runtime:8360/v1/vessels/shipping-map/project");
    const body = JSON.parse(String(init?.body));
    expect(body.role).toBe("application");
    expect(body.project.files["package.json"]).toContain("@deck.gl/core");
    expect(body.environment.VERGLAS_TOKEN).toBe("data-token");
  });

  it("rejects an Application without a standalone start contract", () => {
    expect(() => validateApplicationProject({
      "package.json": "{}",
      "src/server.ts": "export {};",
    })).toThrow("scripts.start");
  });

  it("applies a complete compositional Vessel through one runtime request", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json({
      name: "hormuz-tracker",
      version: "1.0.0",
      digest: "release-digest",
      components: [],
      integrations: [{
        name: "ais",
        version: "2.1.0",
        runtimeName: "hormuz-tracker-ais",
        config: {fields: [], setup: []},
      }],
      interfaceRuntime: "hormuz-tracker-map",
      previewUrl: "/apps/hormuz-tracker-map/",
      outcome: "created",
    }));
    const client = new VerglasIntegrationRuntimeClient(env, fetcher, "data-token");
    const result = await client.deployVessel({
      name: "hormuz-tracker",
      manifest: "apiVersion: verglas.io/v1alpha1\nkind: Vessel\n",
      projects: {application: {files: {"package.json": "{}"}}},
    });

    expect(result.previewUrl).toBe("http://runtime:8360/apps/hormuz-tracker-map/");
    const [url, init] = fetcher.mock.calls[0];
    expect(url).toBe("http://runtime:8360/v1/vessels/hormuz-tracker/composition");
    const body = JSON.parse(String(init?.body));
    expect(body.dataEndpoint).toBe("http://verglas:8334");
    expect(body.dataToken).toBe("data-token");
    expect(body.projects.application.files["package.json"]).toBe("{}");
  });

  it("deletes a Vessel via DELETE /v1/vessels/{name}", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, {status: 204}));
    await new VerglasIntegrationRuntimeClient(env, fetcher, "data-token").deleteVessel("shipping-map");
    const [url, init] = fetcher.mock.calls[0];
    expect(url).toBe("http://runtime:8360/v1/vessels/shipping-map");
    expect(init?.method).toBe("DELETE");
  });

  it("treats a missing Vessel as already deleted", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(new Response("gone", {status: 404}));
    await expect(new VerglasIntegrationRuntimeClient(env, fetcher, "data-token").deleteVessel("gone"))
      .resolves.toBeUndefined();
  });
});
