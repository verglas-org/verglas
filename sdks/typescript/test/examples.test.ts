import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { connect, runWorker, type CloudEvent, type WorkerContext, type VerglasClient } from "../src/index";
import { httpPollWorker, webhookWorker, changeFanoutWorker } from "../src/examples/index";
import { startMockEndpoint, type MockEndpoint } from "./mock-endpoint";

let endpoint: MockEndpoint;
let client: VerglasClient;

beforeEach(async () => {
  endpoint = await startMockEndpoint();
  client = connect({ endpoint: endpoint.url, token: endpoint.token });
});
afterEach(() => endpoint.close());

/** A test WorkerContext with a captured log, the given output, trigger, and env. */
function ctx<Env>(output: string, env: Env, trigger: CloudEvent): WorkerContext<Env> & { logs: { message: string }[] } {
  const logs: { message: string }[] = [];
  return {
    verglas: client,
    client,
    trigger,
    output,
    outputs: [output],
    env,
    log: (message) => logs.push({ message }),
    logs,
  };
}

/** Spins up a throwaway HTTP server returning `handler`'s output. */
async function tinyServer(handler: (body: string) => { code: number; json: unknown }): Promise<{
  url: string;
  received: unknown[];
  close: () => Promise<void>;
}> {
  const received: unknown[] = [];
  const server: Server = createServer((req, res) => {
    let raw = "";
    req.on("data", (c) => (raw += c));
    req.on("end", () => {
      if (raw) received.push(JSON.parse(raw));
      const { code, json } = handler(raw);
      res.writeHead(code, { "content-type": "application/json" });
      res.end(JSON.stringify(json));
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const { port } = server.address() as AddressInfo;
  return { url: `http://127.0.0.1:${port}`, received, close: () => new Promise((r) => server.close(() => r())) };
}

describe("httpPollWorker", () => {
  it("fetches from an upstream URL and appends the rows in the interval", async () => {
    const upstream = await tinyServer(() => ({
      code: 200,
      json: { items: [{ id: "1", t: "2026-08-01T01:00:00Z" }, { id: "2", t: "2026-08-02T00:00:00Z" }] },
    }));
    const env = {
      POLL_URL: `${upstream.url}/data`,
      POLL_ITEMS_KEY: "items",
      POLL_TIME_FIELD: "t",
    };
    const trigger: CloudEvent = {
      specversion: "1.0", id: "tick-1", source: "urn:test",
      type: "org.verglas.schedule.tick",
      data: {
        logicalDate: "2026-08-02T00:00:00Z",
        intervalStart: "2026-08-01T00:00:00Z",
        intervalEnd: "2026-08-02T00:00:00Z", // exclusive: excludes the second row
      },
    };
    const result = await runWorker(httpPollWorker, ctx("ext.points", env, trigger));
    await upstream.close();

    expect(result?.rowsWritten).toBe(1); // only the in-interval row
    expect(endpoint.tableState("ext.points").rows).toHaveLength(1);
  });
});

describe("webhookWorker", () => {
  it("records the request body as a row", async () => {
    const trigger: CloudEvent = {
      specversion: "1.0", id: "http-1", source: "urn:test",
      type: "org.verglas.http.request",
      data: {
        method: "POST", path: "/ingest",
        headers: { "content-type": "application/json", "x-webhook-token": "s3cr3t" },
        body: Array.from(new TextEncoder().encode(JSON.stringify({ event: "signup", user: "u1" }))),
      },
    };
    const result = await runWorker(webhookWorker, ctx("app.events", { WEBHOOK_TOKEN: "s3cr3t" }, trigger));
    expect(result?.rowsWritten).toBe(1);
    const stored = endpoint.tableState("app.events").rows[0].row as Record<string, unknown>;
    expect(stored).toMatchObject({ event: "signup", user: "u1" });
    expect(stored.received_at).toBeTruthy();
  });

  it("rejects a bad webhook token", async () => {
    const trigger: CloudEvent = {
      specversion: "1.0", id: "http-2", source: "urn:test",
      type: "org.verglas.http.request",
      data: { method: "POST", path: "/ingest", headers: {}, body: [123, 125] },
    };
    await expect(
      runWorker(webhookWorker, ctx("app.events", { WEBHOOK_TOKEN: "s3cr3t" }, trigger), { logging: false }),
    ).rejects.toThrow(/x-webhook-token/);
  });
});

describe("changeFanoutWorker", () => {
  it("transforms rows the commit added and appends them idempotently", async () => {
    const input = client.table<{ v: number }>("app.input");
    await input.append([{ v: 2 }, { v: 3 }]);

    const trigger: CloudEvent = {
      specversion: "1.0", id: "snap-2", source: "urn:test",
      type: "org.apache.iceberg.snapshot.committed", subject: "app.input",
      data: { snapshotId: "snap-2", table: "app.input" },
    };
    const result = await runWorker(
      changeFanoutWorker,
      ctx("app.doubled", { INPUT_TABLE: "app.input" }, trigger),
    );
    expect(result?.rowsWritten).toBe(2);
    const out = endpoint.tableState("app.doubled").rows.map((r) => (r.row as { v: number }).v);
    expect(out).toEqual([4, 6]);
  });
});
