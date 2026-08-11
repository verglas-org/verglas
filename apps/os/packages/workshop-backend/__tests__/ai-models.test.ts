import { beforeEach, describe, expect, it } from "vitest";
import type {
  AiChatAuthorInfo,
  AiModelConfig,
} from "@verglas/workshop-shared/api";
import { getModel, type ModelHandle } from "../src/ai-models.js";

// These tests exercise the real pi-ai stack: no module mocks. Routing decisions are asserted on
// the returned handle's model descriptor (baseUrl/id/api), and request-level behavior (URLs and
// auth headers) is asserted by driving `handle.stream` with an
// injected `options.fetch` stub. pi streams never reject; a stubbed 400 simply ends the stream
// with an error-stop message once the request has been captured.

const INITIATOR: AiChatAuthorInfo = {
  type: "user",
  id: "user-123",
  name: "User",
};

const ANTHROPIC_CONFIG: AiModelConfig = {
  provider: "anthropic",
  model: "claude-sonnet-4-5",
  apiToken: "direct-anthropic-token",
};

const WORKERS_AI_CONFIG: AiModelConfig = {
  provider: "cloudflare",
  model: "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
  apiToken: "direct-workers-token",
};

function env(overrides: Partial<Cloudflare.Env> = {}): Cloudflare.Env {
  return overrides as Cloudflare.Env;
}

type CapturedRequest = { url: string; headers: Headers; body: string };

const capturedRequests: CapturedRequest[] = [];

const fetchStub = (async (input: RequestInfo | URL, init?: RequestInit) => {
  const request = new Request(input as RequestInfo, init);
  capturedRequests.push({
    url: request.url,
    headers: request.headers,
    body: await request.text(),
  });
  // A non-retryable client error: the provider SDK reports it, pi converts it into an
  // error-stop assistant message, and the request stays captured for assertions.
  return Response.json(
    { error: { type: "bad_request", message: "stubbed" } },
    { status: 400 },
  );
}) as typeof fetch;

// Runs one request through the handle with the fetch stub and returns what was sent.
async function captureRequest(handle: ModelHandle): Promise<CapturedRequest> {
  const stream = await handle.stream(
    handle.model,
    {
      messages: [{ role: "user", content: "hello", timestamp: 0 }],
    },
    { fetch: fetchStub, maxRetries: 0 },
  );
  const message = await stream.result();
  expect(message.stopReason).toBe("error");
  expect(capturedRequests.length).toBeGreaterThan(0);
  return capturedRequests[0];
}

describe("getModel direct routing", () => {
  beforeEach(() => {
    capturedRequests.length = 0;
  });

  it("uses the provider defaults and the config's own credentials", async () => {
    const handle = getModel(
      env(),
      {
        provider: "anthropic",
        model: "claude-sonnet-4-5",
        apiToken: "direct-api-token",
      },
      INITIATOR,
    );

    expect(handle.model.api).toBe("anthropic-messages");
    expect(handle.model.baseUrl).toBe("https://api.anthropic.com");

    const request = await captureRequest(handle);
    expect(request.url).toBe("https://api.anthropic.com/v1/messages");
    expect(request.headers.get("x-api-key")).toBe("direct-api-token");
  }, 15000);

  it("uses the config's own account and token for direct Workers AI", async () => {
    // Workers AI is configured like every other direct provider.
    const handle = getModel(
      env(),
      {
        ...WORKERS_AI_CONFIG,
        accountId: "user-account-id",
        apiToken: "user-token",
      },
      INITIATOR,
    );

    expect(handle.model.api).toBe("openai-completions");
    expect(handle.model.baseUrl).toBe(
      "https://api.cloudflare.com/client/v4/accounts/user-account-id/ai/v1",
    );

    const request = await captureRequest(handle);
    expect(request.url).toBe(
      "https://api.cloudflare.com/client/v4/accounts/user-account-id/ai/v1/chat/completions",
    );
    expect(request.headers.get("authorization")).toBe("Bearer user-token");
  }, 15000);

  it.each([
    { accountId: undefined, apiToken: "user-token" },
    { accountId: "user-account-id", apiToken: "" },
  ])("requires config credentials for direct Workers AI", (overrides) => {
    // Old Workers AI configs without direct credentials fail with a clear message.
    expect(() =>
      getModel(env(), { ...WORKERS_AI_CONFIG, ...overrides }, INITIATOR),
    ).toThrow("This Workers AI model has no Cloudflare credentials.");
  });

  it("appends /v1 to an Ollama server base URL", () => {
    const handle = getModel(
      env(),
      {
        provider: "ollama",
        model: "qwen3:8b",
        apiToken: "",
        apiUrl: "http://my-ollama:11434/",
      },
      INITIATOR,
    );

    expect(handle.model.api).toBe("openai-completions");
    expect(handle.model.baseUrl).toBe("http://my-ollama:11434/v1");
  });

  it("sends no Authorization header for an Ollama config without an API key", async () => {
    // An empty token means local auth: a strict local proxy may reject an unexpected bearer
    // token, so no Authorization header is sent at all (matching the pre-pi provider).
    const handle = getModel(
      env(),
      {
        provider: "ollama",
        model: "qwen3:8b",
        apiToken: "",
        apiUrl: "http://my-ollama:11434",
      },
      INITIATOR,
    );

    const request = await captureRequest(handle);
    expect(request.url).toBe("http://my-ollama:11434/v1/chat/completions");
    expect(request.headers.get("authorization")).toBeNull();
  }, 15000);

  it("sends the configured Ollama API key as a bearer token", async () => {
    const handle = getModel(
      env(),
      {
        provider: "ollama",
        model: "qwen3:8b",
        apiToken: "ollama-token",
        apiUrl: "http://my-ollama:11434",
      },
      INITIATOR,
    );

    const request = await captureRequest(handle);
    expect(request.headers.get("authorization")).toBe("Bearer ollama-token");
  }, 15000);

  it("strips a legacy /api (or /v1) suffix from an Ollama base URL", () => {
    // Configs saved before the pi migration store the native-API base (".../api").
    for (const apiUrl of [
      "http://my-ollama:11434/api",
      "http://my-ollama:11434/v1/",
    ]) {
      const handle = getModel(
        env(),
        {
          provider: "ollama",
          model: "qwen3:8b",
          apiToken: "",
          apiUrl,
        },
        INITIATOR,
      );
      expect(handle.model.baseUrl).toBe("http://my-ollama:11434/v1");
    }
  });
  it("routes subscriptions through Pi's native messages protocol", async () => {
    const handle = getModel(
      env({
        LOCAL_MODEL_RUNTIME_TOKEN: "adapter-token",
      }),
      {
        provider: "local-runtime",
        runtime: "codex",
        model: "gpt-5.6-terra",
        apiToken: "",
        apiUrl: "http://127.0.0.1:8790/v1/",
        credentialScope: "owner-scope",
      },
      INITIATOR,
      { sessionAffinity: "chat-session" },
    );

    expect(handle.model.api).toBe("pi-messages");
    expect(handle.model.provider).toBe("openai-codex");
    expect(handle.model.baseUrl).toBe("http://127.0.0.1:8790");

    const request = await captureRequest(handle);
    expect(request.url).toBe("http://127.0.0.1:8790/messages");
    expect(request.headers.get("authorization")).toBe("Bearer adapter-token");
    expect(request.headers.get("x-verglas-credential-scope")).toBe(
      "owner-scope",
    );
    expect(request.headers.get("x-model-runtime")).toBe("codex");
    expect(JSON.parse(request.body)).toMatchObject({
      model: "gpt-5.6-terra",
      options: { sessionId: "chat-session" },
    });
  }, 15000);

  it("uses Pi's provider identity for GitHub Copilot subscriptions", async () => {
    const handle = getModel(
      env({
        LOCAL_MODEL_RUNTIME_URL: "http://127.0.0.1:8790",
        LOCAL_MODEL_RUNTIME_TOKEN: "deployment-token",
      }),
      {
        provider: "local-runtime",
        runtime: "github-copilot",
        model: "gpt-5.6-sol",
        apiToken: "",
      },
      INITIATOR,
    );

    const request = await captureRequest(handle);
    expect(request.headers.get("authorization")).toBe(
      "Bearer deployment-token",
    );
    expect(handle.model.provider).toBe("github-copilot");
    expect(request.headers.get("x-model-runtime")).toBe("github-copilot");
    expect(request.headers.get("x-provider-api-key")).toBeNull();
  }, 15000);

  it("requires the deployment-owned native adapter configuration", () => {
    expect(() =>
      getModel(
        env(),
        {
          provider: "local-runtime",
          model: "codex",
          apiToken: "",
        },
        INITIATOR,
      ),
    ).toThrow("Pi model runtime is not configured");
  });
});

describe("PDF attachment bridging", () => {
  beforeEach(() => {
    capturedRequests.length = 0;
  });

  // PDFs ride pi ImageContent parts (pi has no document part); every handle's onPayload hook
  // rewrites them into the provider's native document blocks (see chat-attachment-pdf.ts).
  // These tests drive the real pi adapters and assert on the outgoing request body.
  const PDF_PART = {
    type: "image" as const,
    data: "JVBERi0=",
    mimeType: "application/pdf",
  };
  const PNG_PART = {
    type: "image" as const,
    data: "iVBOR",
    mimeType: "image/png",
  };

  async function capturePdfRequest(handle: ModelHandle): Promise<unknown> {
    const stream = handle.stream(
      handle.model,
      {
        messages: [
          {
            role: "user",
            content: [
              { type: "text", text: "Summarize the attached PDF." },
              PDF_PART,
              PNG_PART,
            ],
            timestamp: 0,
          },
        ],
      },
      { fetch: fetchStub, maxRetries: 0 },
    );
    const message = await stream.result();
    expect(message.stopReason).toBe("error");
    return JSON.parse(capturedRequests[0].body);
  }

  it("sends Anthropic PDFs as document blocks", async () => {
    const handle = getModel(env(), ANTHROPIC_CONFIG, INITIATOR);
    const body = (await capturePdfRequest(handle)) as {
      messages: {
        content: { type: string; source?: { media_type: string } }[];
      }[];
    };

    const blocks = body.messages[0].content;
    expect(blocks).toContainEqual(
      expect.objectContaining({
        type: "document",
        source: expect.objectContaining({
          media_type: "application/pdf",
          data: "JVBERi0=",
        }),
      }),
    );
    // A real image in the same message stays an image block.
    expect(blocks).toContainEqual(
      expect.objectContaining({
        type: "image",
        source: expect.objectContaining({ media_type: "image/png" }),
      }),
    );
    expect(
      blocks.some(
        (block) =>
          block.source?.media_type === "application/pdf" &&
          block.type !== "document",
      ),
    ).toBe(false);
  }, 15000);

  it("sends OpenAI PDFs as input_file parts", async () => {
    const handle = getModel(
      env(),
      {
        provider: "openai",
        model: "gpt-5.2",
        apiToken: "direct-api-token",
      },
      INITIATOR,
    );
    expect(handle.model.api).toBe("openai-responses");
    const body = (await capturePdfRequest(handle)) as {
      input: {
        role?: string;
        content: { type: string; image_url?: string }[];
      }[];
    };

    const parts = body.input.find((item) => item.role === "user")!.content;
    expect(parts).toContainEqual({
      type: "input_file",
      filename: "attachment.pdf",
      file_data: "data:application/pdf;base64,JVBERi0=",
    });
    expect(parts).toContainEqual(
      expect.objectContaining({
        type: "input_image",
        image_url: "data:image/png;base64,iVBOR",
      }),
    );
  }, 15000);
});
