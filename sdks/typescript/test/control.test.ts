import { describe, expect, it } from "vitest";
import { connectScheduler, extractWorkerSource } from "../src/control";

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

describe("scheduler control-plane connector", () => {
  it("requires a scoped bearer token", () => {
    const options = { endpoint: "http://verglas.test", token: "" };
    expect(() => connectScheduler(options)).toThrow(/token is required/);
  });

  it("routes worker operations to /v0/workers with bearer authorization", async () => {
    const requests: Request[] = [];
    const client = connectScheduler({
      endpoint: "http://scheduler.test/",
      token: "scheduler-token",
      fetch: recordingFetch(requests, { "/v0/workers": [] }),
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
      ["GET", "http://scheduler.test/v0/workers?view=all"],
      ["POST", "http://scheduler.test/v0/workers"],
    ]);
    expect(requests[0]?.headers.get("authorization")).toBe("Bearer scheduler-token");
    expect(await requests[1]?.json()).toMatchObject({ name: "sync-linear" });
  });

  it("gets one worker and dispatches a run by name, both under /v0/workers", async () => {
    const requests: Request[] = [];
    const client = connectScheduler({
      endpoint: "http://scheduler.test",
      token: "scheduler-token",
      fetch: recordingFetch(requests, {
        "/v0/workers/sync-linear": { name: "sync-linear" },
      }),
    });

    await client.getWorker("sync-linear");
    await client.setWorkerState("sync-linear", "archived");
    await client.runWorker("sync-linear", "run-key-1");

    expect(requests.map((request) => [request.method, new URL(request.url).pathname])).toEqual([
      ["GET", "/v0/workers/sync-linear"],
      ["PUT", "/v0/workers/sync-linear/state"],
      ["POST", "/v0/workers/sync-linear/run"],
    ]);
    expect(await requests[1]?.json()).toEqual({ state: "archived" });
    expect(requests[2]?.headers.get("idempotency-key")).toBe("run-key-1");
  });

  it("routes scheduler secrets over /v1/secrets", async () => {
    const requests: Request[] = [];
    const scheduler = connectScheduler({
      endpoint: "http://scheduler.test",
      token: "control-token",
      fetch: recordingFetch(requests, { "/v1/secrets": { secrets: ["linear-token"] } }),
    });

    expect(await scheduler.listSecretNames()).toEqual(["linear-token"]);
    await scheduler.putSecret("linear-token", "value");
    await scheduler.deleteSecret("linear-token");

    expect(requests.map((request) => [request.method, new URL(request.url).pathname])).toEqual([
      ["GET", "/v1/secrets"],
      ["PUT", "/v1/secrets/linear-token"],
      ["DELETE", "/v1/secrets/linear-token"],
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
