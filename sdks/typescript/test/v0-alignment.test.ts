// Frozen RIME evaluator. Candidates must make this pass without editing it.
//
// The SDK speaks the /v0 surface natively: ONE append-ingest shape
// (POST /v0/events?name=<datasource>, NDJSON) for table writes, vector
// writes, and log shipping alike; SQL through /v0/sql. Breaking changes to
// the old client shapes are expected — this is the contract now.

import { describe, expect, it, vi } from "vitest";
import { createDataClient } from "../src/data";

function client() {
  const calls: Array<{ url: string; init: RequestInit }> = [];
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ url: String(input), init: init ?? {} });
    return Response.json({ successful_rows: 2, quarantined_rows: 0 }, { status: 202 });
  });
  const data = createDataClient({
    baseUrl: "https://api.verglas.dev",
    token: "p.test-token",
    fetch: fetchImpl as unknown as typeof fetch,
  });
  return { data, calls };
}

describe("SDK /v0 data client", () => {
  it("appends events as NDJSON to /v0/events with the workspace token", async () => {
    const { data, calls } = client();
    await data.appendEvents("app_logs", [{ level: "info", msg: "boot" }, { level: "warn", msg: "slow" }]);
    expect(calls.length).toBe(1);
    const { url, init } = calls[0];
    const parsed = new URL(url);
    expect(parsed.pathname).toBe("/v0/events");
    expect(parsed.searchParams.get("name")).toBe("app_logs");
    expect(init.method).toBe("POST");
    expect(new Headers(init.headers).get("authorization")).toBe("Bearer p.test-token");
    expect(String(init.body)).toBe('{"level":"info","msg":"boot"}\n{"level":"warn","msg":"slow"}\n');
  });

  it("tableWrite and vectorWrite are the same append shape, per-datasource", async () => {
    const { data, calls } = client();
    await data.tableWrite("analytics.events", [{ path: "/" }]);
    await data.vectorWrite("embeddings", [{ id: "a", vector: [0.1, 0.2] }]);
    expect(calls.map((c) => new URL(c.url).searchParams.get("name"))).toEqual(["analytics.events", "embeddings"]);
    for (const { url, init } of calls) {
      expect(new URL(url).pathname).toBe("/v0/events");
      expect(init.method).toBe("POST");
    }
    expect(String(calls[1].init.body)).toBe('{"id":"a","vector":[0.1,0.2]}\n');
  });

  it("runs SQL through /v0/sql", async () => {
    const { data, calls } = client();
    await data.sql("SELECT count() FROM analytics_events");
    const { url, init } = calls[0];
    expect(new URL(url).pathname).toBe("/v0/sql");
    expect(init.method).toBe("POST");
    expect(new Headers(init.headers).get("authorization")).toBe("Bearer p.test-token");
  });
});
