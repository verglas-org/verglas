import { afterEach, describe, expect, it, vi } from "vitest";
import { ModelRuntimeManager } from "../src/model-runtimes";

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
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        Response.json({
          runtimes: [
            {
              id: "codex",
              available: true,
              linked: true,
              detail: "Logged in using ChatGPT",
              supportsGuidedLogin: true,
            },
          ],
        }),
      )
      .mockResolvedValueOnce(Response.json({ ok: true }));
    globalThis.fetch = fetcher;

    await new ModelRuntimeManager(env, "user-123").verifyLinked(
      "codex",
      "gpt-5.6-sol",
    );

    expect(fetcher).toHaveBeenCalledTimes(2);
    expect(fetcher.mock.calls[1][0]).toBe(
      "http://127.0.0.1:8790/v1/runtimes/codex/verify",
    );
    expect(
      new Headers(fetcher.mock.calls[1][1]?.headers).get("authorization"),
    ).toBe("Bearer runtime-token");
    expect(
      new Headers(fetcher.mock.calls[1][1]?.headers).get(
        "x-verglas-credential-scope",
      ),
    ).toBe("user-123");
    expect(JSON.parse(String(fetcher.mock.calls[1][1]?.body))).toEqual({
      model: "gpt-5.6-sol",
    });
  });

  it("returns the runtime's provider-owned model catalog", async () => {
    const models = [
      { id: "gpt-5.6-sol", name: "GPT-5.6-Sol", isDefault: true },
    ];
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValue(Response.json({ models }));
    globalThis.fetch = fetcher;

    await expect(
      new ModelRuntimeManager(env, "user-123").listModels("codex"),
    ).resolves.toEqual(models);

    expect(fetcher.mock.calls[0][0]).toBe(
      "http://127.0.0.1:8790/v1/runtimes/codex/models",
    );
  });

  it("lists GitHub Copilot models through the scoped Pi provider", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValue(Response.json({ models: [] }));
    globalThis.fetch = fetcher;

    await new ModelRuntimeManager(env, "user-123").listModels("github-copilot");

    expect(fetcher.mock.calls[0][0]).toBe(
      "http://127.0.0.1:8790/v1/runtimes/github-copilot/models",
    );
    expect(fetcher.mock.calls[0][1]?.method).toBe("GET");
  });
});
