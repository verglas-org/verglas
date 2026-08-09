import { describe, expect, it } from "vitest";
import {
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
  it("routes admin worker operations with bearer authorization", async () => {
    const requests: Request[] = [];
    const client = connectAdmin({
      endpoint: "http://verglas.test/",
      token: "admin-token",
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
      ["GET", "http://verglas.test/v1/workers?view=all"],
      ["POST", "http://verglas.test/v1/workers"],
    ]);
    expect(requests[0]?.headers.get("authorization")).toBe("Bearer admin-token");
    expect(await requests[1]?.json()).toMatchObject({ name: "sync-linear" });
  });

  it("requires credentials for scheduler and runtime control", () => {
    const options = { endpoint: "http://verglas.test", token: "" };
    expect(() => connectScheduler(options)).toThrow(/token is required/);
    expect(() => connectRuntime(options)).toThrow(/token is required/);
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
});

describe("extractWorkerSource", () => {
  it("selects a conventional TypeScript entrypoint and tolerates invalid JSON", () => {
    expect(
      extractWorkerSource(JSON.stringify({ files: { "src/worker.ts": "export default 1" } })),
    ).toBe("export default 1");
    expect(extractWorkerSource("not-json")).toBeUndefined();
  });
});
