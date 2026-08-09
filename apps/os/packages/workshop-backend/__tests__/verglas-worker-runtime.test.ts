import {describe, expect, it, vi} from "vitest";
import {
  VerglasWorkerRuntimeClient,
  resolveVerglasWorkerRuntimeConfig,
  validateVerglasWorkerModule,
} from "../src/verglas-worker-runtime.js";

describe("resolveVerglasWorkerRuntimeConfig", () => {
  it("requires the complete worker control plane", () => {
    expect(resolveVerglasWorkerRuntimeConfig({})).toBeNull();
    expect(() => resolveVerglasWorkerRuntimeConfig({
      VERGLAS_SCHEDULER_URL: "http://localhost:8340",
    })).toThrow(/must be configured together/);
  });

  it("normalizes configured endpoints", () => {
    expect(resolveVerglasWorkerRuntimeConfig({
      VERGLAS_SCHEDULER_URL: "http://localhost:8340/",
      VERGLAS_SCHEDULER_CONTROL_TOKEN: "control",
    })).toEqual({
      schedulerEndpoint: "http://localhost:8340",
      schedulerToken: "control",
    });
  });
});

describe("VerglasWorkerRuntimeClient", () => {
  it("keeps secret values out of the worker declaration", async () => {
    const requests: Request[] = [];
    const fetcher = vi.fn<typeof fetch>(async (input, init) => {
      requests.push(input instanceof Request ? input : new Request(input, init));
      return new Response(null, {status: 204});
    });
    const client = new VerglasWorkerRuntimeClient({
      schedulerEndpoint: "http://localhost:8340",
      schedulerToken: "control",
    }, fetcher);

    await client.putSecret("source.API_TOKEN", "do-not-persist");
    await client.register({
      name: "source",
      code: "{}",
      triggers: "[]",
      output: "app.events",
      config: JSON.stringify({env: {API_TOKEN: "@secret:source.API_TOKEN"}}),
      created_by: "test",
    });

    expect(requests[0].headers.get("authorization")).toBe("Bearer control");
    expect(await requests[0].json()).toEqual({value: "do-not-persist"});
    const declaration = await requests[1].text();
    expect(declaration).toContain("@secret:source.API_TOKEN");
    expect(declaration).not.toContain("do-not-persist");
  });

  it("sends a fresh idempotency key when manually running a Source", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(new Response(
      JSON.stringify({job_id: "job-1", created: true}),
      {status: 202, headers: {"content-type": "application/json"}},
    ));
    const client = new VerglasWorkerRuntimeClient({
      schedulerEndpoint: "http://localhost:8340",
      schedulerToken: "control",
    }, fetcher);

    await expect(client.run("source", "request-1"))
      .resolves.toEqual({job_id: "job-1", created: true});
    expect(fetcher.mock.calls[0][1]?.headers).toMatchObject({"idempotency-key": "request-1"});
  });
});

describe("validateVerglasWorkerModule", () => {
  const valid = `
    import {defineWorker} from "/sdks/typescript/src/index.ts";
    export default defineWorker({
      async handler(ctx) {
        await ctx.client.ensureTable(ctx.output, {schema: [{name: "id", type: "utf8"}]});
        const result = await ctx.client.table(ctx.output).append([{id: "one"}]);
        return {rowsWritten: result.rowsCommitted};
      },
    });
  `;

  it("accepts the executable Source contract", () => {
    expect(() => validateVerglasWorkerModule(valid)).not.toThrow();
  });

  it("rejects the obsolete run method", () => {
    expect(() => validateVerglasWorkerModule(valid.replace("handler", "run")))
      .toThrow(/handler\(ctx\)/);
  });

  it("rejects treating ctx.output as a table object", () => {
    expect(() => validateVerglasWorkerModule(
      valid.replace("ctx.client.table(ctx.output).append", "ctx.output.append"),
    )).toThrow(/ctx\.client\.table/);
  });
});
