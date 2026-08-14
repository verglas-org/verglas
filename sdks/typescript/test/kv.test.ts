// Thin KV SDK contract: raw bytes and headers only, with no client-side cache,
// cursor interpretation, idempotency retry, or storage-key encoding.

import { describe, expect, it } from "vitest";
import { connect } from "../src/index";

describe("native KV handle", () => {
  it("sets, gets, deletes, and lists raw values with TTL and conditions", async () => {
    const calls: Array<{ method: string; url: string; headers: Headers; body?: Uint8Array }> = [];
    const fetchImpl: typeof fetch = async (input, init) => {
      const headers = new Headers(init?.headers);
      const body = init?.body ? new Uint8Array(await new Response(init.body).arrayBuffer()) : undefined;
      calls.push({ method: init?.method ?? "GET", url: String(input), headers, body });
      if (init?.method === "PUT") {
        return new Response(null, { status: 201, headers: { etag: '"7"', "x-verglas-idempotent": "false" } });
      }
      if (init?.method === "DELETE") {
        return Response.json({ removed: true });
      }
      if (String(input).includes("?")) {
        return Response.json({
          entries: [{ key: "user/a", version: '"7"', modified_at_ms: 11, metadata: {} }],
          next_cursor: "opaque",
        });
      }
      return new Response(new Uint8Array([1, 2, 3]), {
        headers: {
          etag: '"7"',
          "content-type": "application/octet-stream",
          "x-verglas-modified-at-ms": "11",
          "x-verglas-expires-at-ms": "999",
          "x-verglas-meta-kind": "demo",
          "x-verglas-kv-tier": "ram",
        },
      });
    };
    const client = connect({ endpoint: "https://tenant.example", token: "scoped", fetch: fetchImpl });
    const cache = client.kv("workshop.blueprints");
    const put = await cache.put("featured", new Uint8Array([1, 2, 3]), {
      ttlSeconds: 300,
      contentType: "application/octet-stream",
      metadata: { kind: "demo" },
      ifNoneMatch: true,
      idempotencyKey: "request-1",
    });
    expect(put).toEqual({ version: '"7"', idempotent: false });
    expect(calls[0].headers.get("x-verglas-ttl-seconds")).toBe("300");
    expect(calls[0].headers.get("if-none-match")).toBe("*");
    expect(calls[0].headers.get("idempotency-key")).toBe("request-1");
    expect([...calls[0].body!]).toEqual([1, 2, 3]);

    const value = await cache.get("featured");
    expect([...value!.bytes]).toEqual([1, 2, 3]);
    expect(value).toMatchObject({ version: '"7"', contentType: "application/octet-stream", metadata: { kind: "demo" }, tier: "ram" });
    expect(await cache.delete("featured", { ifMatch: value!.version })).toEqual({ removed: true });
    expect(calls[2].headers.get("if-match")).toBe('"7"');

    const page = await cache.list({ prefix: "user/", limit: 25, cursor: "opaque-in" });
    expect(page.entries[0].key).toBe("user/a");
    expect(page.nextCursor).toBe("opaque");
    expect(calls[3].url).toContain("prefix=user%2F");
    expect(calls[3].url).toContain("cursor=opaque-in");
  });
});
