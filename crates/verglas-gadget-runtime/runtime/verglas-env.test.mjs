import { describe, expect, test } from "bun:test";

import { makeVerglasEnvironment } from "./verglas-env.mjs";

describe("Gadget Verglas environment", () => {
  test("exposes the SDK client without credentials and routes queries through captured fetch", async () => {
    const requests = [];
    const fetchImpl = async (url, init) => {
      requests.push({ url, init });
      return Response.json({ columns: ["answer"], rows: [{ answer: 42 }], row_count: 1 });
    };

    const env = makeVerglasEnvironment({
      endpoint: "http://verglas-server:8334",
      token: "gadget-scoped-secret",
      fetchImpl,
    });

    expect(Object.keys(env)).toEqual(["VERGLAS"]);
    expect("token" in env.VERGLAS).toBe(false);
    expect("transport" in env.VERGLAS).toBe(false);
    expect(JSON.stringify(env)).not.toContain("gadget-scoped-secret");

    expect(await env.VERGLAS.query("SELECT 42 AS answer")).toEqual({
      columns: ["answer"],
      rows: [{ answer: 42 }],
      row_count: 1,
    });
    expect(requests).toHaveLength(1);
    expect(requests[0].url).toBe("http://verglas-server:8334/v1/query");
    expect(requests[0].init.headers.authorization).toBe("Bearer gadget-scoped-secret");
  });
});
