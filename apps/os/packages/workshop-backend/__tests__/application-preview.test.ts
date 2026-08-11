import {describe, expect, it, vi} from "vitest";
import {proxyApplicationPreview} from "../src/application-preview.js";

describe("proxyApplicationPreview", () => {
  it("forwards the app path without leaking Workshop credentials", async () => {
    const fetcher = vi.fn<typeof fetch>(async (input, init) => {
      expect(String(input)).toBe("http://runtime:8360/apps/shipping-map/api/status?fresh=1");
      const headers = new Headers(init?.headers);
      expect(headers.has("authorization")).toBe(false);
      expect(headers.has("cookie")).toBe(false);
      expect(headers.get("accept")).toBe("application/json");
      return Response.json({ok: true});
    });

    const response = await proxyApplicationPreview(new Request(
      "https://os.example/apps/shipping-map/api/status?fresh=1",
      {headers: {accept: "application/json", authorization: "Bearer private", cookie: "session=private"}},
    ), {VERGLAS_CONTAINER_RUNTIME_URL: "http://runtime:8360"}, fetcher);

    expect(await response.json()).toEqual({ok: true});
  });

  it("returns unavailable when no container runtime is configured", async () => {
    const response = await proxyApplicationPreview(
      new Request("https://os.example/apps/shipping-map/"),
      {},
    );
    expect(response.status).toBe(503);
  });
});
