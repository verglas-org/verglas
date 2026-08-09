import {afterEach, describe, expect, it, vi} from "vitest";
import {ModelRuntimeManager} from "../src/model-runtimes";

const originalFetch = globalThis.fetch;
const env = {
  LOCAL_MODEL_RUNTIME_URL: "http://127.0.0.1:8790",
  LOCAL_MODEL_RUNTIME_TOKEN: "runtime-token",
} as Cloudflare.Env;

afterEach(() => {
  globalThis.fetch = originalFetch;
});

describe("ModelRuntimeManager", () => {
  it("proves subscription status and inference before linking a runtime", async () => {
    const fetcher = vi.fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json({
        runtimes: [{
          id: "codex", available: true, linked: true,
          detail: "Logged in using ChatGPT", supportsGuidedLogin: true,
        }],
      }))
      .mockResolvedValueOnce(Response.json({ok: true}));
    globalThis.fetch = fetcher;

    await new ModelRuntimeManager(env).verifyLinked("codex", "gpt-5.6-sol");

    expect(fetcher).toHaveBeenCalledTimes(2);
    expect(fetcher.mock.calls[1][0]).toBe("http://127.0.0.1:8790/v1/runtimes/codex/verify");
    expect(new Headers(fetcher.mock.calls[1][1]?.headers).get("authorization"))
      .toBe("Bearer runtime-token");
    expect(JSON.parse(String(fetcher.mock.calls[1][1]?.body)))
      .toEqual({model: "gpt-5.6-sol"});
  });

  it("lets the adapter verify a Cursor API token without a CLI subscription", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json({ok: true}));
    globalThis.fetch = fetcher;

    await new ModelRuntimeManager(env).verifyLinked("cursor", "auto", "cursor-token");

    expect(fetcher).toHaveBeenCalledOnce();
    expect(JSON.parse(String(fetcher.mock.calls[0][1]?.body)))
      .toEqual({model: "auto", apiToken: "cursor-token"});
  });

  it("returns the runtime's provider-owned model catalog", async () => {
    const models = [{id: "gpt-5.6-sol", name: "GPT-5.6-Sol", isDefault: true}];
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json({models}));
    globalThis.fetch = fetcher;

    await expect(new ModelRuntimeManager(env).listModels("codex")).resolves.toEqual(models);

    expect(fetcher.mock.calls[0][0])
      .toBe("http://127.0.0.1:8790/v1/runtimes/codex/models");
  });

  it("uses a Cursor API token while discovering that account's models", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(Response.json({models: []}));
    globalThis.fetch = fetcher;

    await new ModelRuntimeManager(env).listModels("cursor", "cursor-token");

    expect(fetcher.mock.calls[0][1]?.method).toBe("POST");
    expect(JSON.parse(String(fetcher.mock.calls[0][1]?.body)))
      .toEqual({apiToken: "cursor-token"});
  });
});
