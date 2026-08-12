#!/usr/bin/env node

// Trusted local Pi model service. It owns subscription OAuth credentials and provider sessions;
// the Workshop Worker owns the Pi agent loop and communicates using Pi's native `pi-messages`
// protocol. No coding-agent CLI is launched and no assistant message is synthesized through JSON.

import { createServer } from "node:http";
import { join } from "node:path";
import { createModels } from "@earendil-works/pi-ai";
import { anthropicProvider } from "@earendil-works/pi-ai/providers/anthropic";
import { githubCopilotProvider } from "@earendil-works/pi-ai/providers/github-copilot";
import { openaiCodexProvider } from "@earendil-works/pi-ai/providers/openai-codex";
import { ScopedCredentialStore } from "./pi-credential-store.mjs";

const host = process.env.LOCAL_MODEL_RUNTIME_HOST || "127.0.0.1";
const port = Number.parseInt(
  process.env.LOCAL_MODEL_RUNTIME_PORT || "8790",
  10,
);
const token = process.env.LOCAL_MODEL_RUNTIME_TOKEN;
const credentialsDirectory =
  process.env.PI_CREDENTIALS_DIR ||
  join(process.cwd(), ".verglas", "pi-credentials");

if (!token) throw new Error("LOCAL_MODEL_RUNTIME_TOKEN is required.");
if (!Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error("LOCAL_MODEL_RUNTIME_PORT must be a valid TCP port.");
}

const PROVIDERS = {
  codex: {
    providerId: "openai-codex",
    name: "Codex",
    accountName: "ChatGPT Plus/Pro",
    create: openaiCodexProvider,
  },
  "claude-code": {
    providerId: "anthropic",
    name: "Claude",
    accountName: "Claude Pro/Max",
    create: anthropicProvider,
  },
  "github-copilot": {
    providerId: "github-copilot",
    name: "GitHub Copilot",
    accountName: "GitHub Copilot",
    create: githubCopilotProvider,
  },
};

const scopedModels = new Map();
const loginSessions = new Map();

function runtimeDefinition(id) {
  const definition = PROVIDERS[id];
  if (!definition) throw new Error(`Unknown Pi subscription provider: ${id}`);
  return definition;
}

function modelsForScope(scope) {
  let models = scopedModels.get(scope);
  if (models) return models;
  models = createModels({
    credentials: new ScopedCredentialStore(credentialsDirectory, scope),
  });
  for (const definition of Object.values(PROVIDERS)) {
    models.setProvider(definition.create());
  }
  scopedModels.set(scope, models);
  return models;
}

function requestScope(request) {
  const scope = request.headers["x-verglas-credential-scope"];
  if (typeof scope !== "string" || !scope.trim()) {
    throw new Error("A credential scope is required.");
  }
  return scope;
}

function sendJson(response, status, value) {
  const body = JSON.stringify(value);
  response.writeHead(status, {
    "content-type": "application/json",
    "content-length": Buffer.byteLength(body),
    "cache-control": "no-store",
  });
  response.end(body);
}

async function readJson(request, limit = 16 * 1024 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > limit) throw new Error("Request body is too large.");
    chunks.push(chunk);
  }
  return chunks.length === 0
    ? {}
    : JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

function zeroSubscriptionCost(usage) {
  return {
    ...usage,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  };
}

function wireEvent(event) {
  switch (event.type) {
    case "start":
      return { type: "start" };
    case "text_start":
    case "thinking_start":
      return { type: event.type, contentIndex: event.contentIndex };
    case "text_delta":
    case "thinking_delta":
    case "toolcall_delta":
      return {
        type: event.type,
        contentIndex: event.contentIndex,
        delta: event.delta,
      };
    case "text_end": {
      const block = event.partial.content[event.contentIndex];
      return {
        type: "text_end",
        contentIndex: event.contentIndex,
        content: event.content,
        ...(block?.type === "text" && block.textSignature
          ? { contentSignature: block.textSignature }
          : {}),
      };
    }
    case "thinking_end": {
      const block = event.partial.content[event.contentIndex];
      return {
        type: "thinking_end",
        contentIndex: event.contentIndex,
        content: event.content,
        ...(block?.type === "thinking" && block.thinkingSignature
          ? { contentSignature: block.thinkingSignature }
          : {}),
        ...(block?.type === "thinking" && block.redacted
          ? { redacted: true }
          : {}),
      };
    }
    case "toolcall_start": {
      const block = event.partial.content[event.contentIndex];
      if (block?.type !== "toolCall")
        throw new Error("Pi emitted an invalid tool-call start.");
      return {
        type: "toolcall_start",
        contentIndex: event.contentIndex,
        id: block.id,
        toolName: block.name,
      };
    }
    case "toolcall_end":
      return {
        type: "toolcall_end",
        contentIndex: event.contentIndex,
        toolCall: event.toolCall,
      };
    case "done":
      return {
        type: "done",
        reason: event.reason,
        usage: zeroSubscriptionCost(event.message.usage),
        ...(event.message.responseId
          ? { responseId: event.message.responseId }
          : {}),
      };
    case "error":
      return {
        type: "error",
        reason: event.reason,
        usage: zeroSubscriptionCost(event.error.usage),
        ...(event.error.errorMessage
          ? { errorMessage: event.error.errorMessage }
          : {}),
        ...(event.error.responseId
          ? { responseId: event.error.responseId }
          : {}),
      };
    default:
      throw new Error(`Unsupported Pi event: ${event.type}`);
  }
}

async function streamMessages(request, response) {
  const scope = requestScope(request);
  const runtimeId = request.headers["x-model-runtime"];
  if (typeof runtimeId !== "string")
    throw new Error("A Pi subscription provider is required.");
  const definition = runtimeDefinition(runtimeId);
  const body = await readJson(request);
  const models = modelsForScope(scope);
  const model = models.getModel(definition.providerId, body.model);
  if (!model)
    throw new Error(`${definition.name} does not provide model ${body.model}.`);

  const auth = await models.checkAuth(definition.providerId);
  if (!auth) throw new Error(`${definition.accountName} is not linked.`);

  response.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-store",
    connection: "keep-alive",
  });

  const disconnect = new AbortController();
  request.once("aborted", () =>
    disconnect.abort(new Error("Client disconnected.")),
  );
  response.once("close", () => {
    if (!response.writableEnded)
      disconnect.abort(new Error("Client disconnected."));
  });
  const stream = models.stream(model, body.context, {
    ...body.options,
    signal: AbortSignal.any([
      AbortSignal.timeout(15 * 60_000),
      disconnect.signal,
    ]),
  });
  for await (const event of stream) {
    response.write(`data: ${JSON.stringify(wireEvent(event))}\n\n`);
  }
  response.end();
}

async function runtimeStatus(scope, id) {
  const definition = runtimeDefinition(id);
  const auth = await modelsForScope(scope).checkAuth(definition.providerId);
  return {
    id,
    available: true,
    linked: Boolean(auth),
    detail: auth
      ? `${definition.accountName} is linked through Pi OAuth.`
      : `${definition.accountName} is available for sign-in through Pi OAuth.`,
    supportsGuidedLogin: true,
  };
}

async function catalogFor(scope, id) {
  const definition = runtimeDefinition(id);
  const scoped = modelsForScope(scope);
  const models = (await scoped.checkAuth(definition.providerId))
    ? await scoped.getAvailable(definition.providerId)
    : scoped.getModels(definition.providerId);
  const preferred =
    id === "codex"
      ? "gpt-5.6-sol"
      : id === "claude-code"
        ? "claude-sonnet-5"
        : "gpt-5.6-sol";
  const defaultId = models.some((model) => model.id === preferred)
    ? preferred
    : models[0]?.id;
  return models.map((model) => ({
    id: model.id,
    name: model.name,
    ...(model.id === defaultId ? { isDefault: true } : {}),
    contextWindow: model.contextWindow,
  }));
}

function sessionKey(scope, sessionId) {
  return `${scope}\u0000${sessionId}`;
}

function sessionResult(session) {
  const definition = runtimeDefinition(session.runtimeId);
  const step =
    session.step ??
    (!session.done
      ? {
          id: "oauth-starting",
          type: "progress",
          title: `Sign in to ${definition.name}`,
          message: "Starting secure sign-in…",
        }
      : undefined);
  return {
    sessionId: session.id,
    done: session.done,
    status: session.error ? "error" : session.done ? "done" : "running",
    ...(step ? { step } : {}),
    ...(session.error ? { error: session.error } : {}),
  };
}

function markSessionChanged(session) {
  for (const resolve of session.waiters.splice(0)) resolve();
}

async function waitForSessionChange(session, timeoutMs = 750) {
  if (session.done || session.step) return;
  await new Promise((resolve) => {
    const timer = setTimeout(resolve, timeoutMs);
    session.waiters.push(() => {
      clearTimeout(timer);
      resolve();
    });
  });
}

function startLogin(scope, runtimeId, sessionId) {
  const key = sessionKey(scope, sessionId);
  if (loginSessions.has(key)) throw new Error("Login session already exists.");
  const definition = runtimeDefinition(runtimeId);
  const controller = new AbortController();
  const session = {
    id: sessionId,
    scope,
    runtimeId,
    controller,
    step: undefined,
    lastExternalUrl: undefined,
    pendingPrompt: undefined,
    done: false,
    error: undefined,
    waiters: [],
  };
  loginSessions.set(key, session);

  const interaction = {
    signal: controller.signal,
    notify(event) {
      if (event.type === "auth_url") {
        session.lastExternalUrl = event.url;
        session.step = {
          id: "oauth-browser",
          type: "action",
          title: `Sign in to ${definition.name}`,
          message:
            event.instructions ||
            `Complete ${definition.accountName} sign-in in your browser.`,
          externalUrl: event.url,
        };
      } else if (event.type === "device_code") {
        session.lastExternalUrl = event.verificationUri;
        session.step = {
          id: "oauth-device-code",
          type: "progress",
          title: `Sign in to ${definition.name}`,
          message: `Enter the device code at ${event.verificationUri}.`,
          externalUrl: event.verificationUri,
          deviceCode: {
            code: event.userCode,
            ...(event.expiresInSeconds
              ? { expiresInMinutes: Math.ceil(event.expiresInSeconds / 60) }
              : {}),
          },
        };
      } else if (event.type === "progress") {
        session.step = {
          id: "oauth-progress",
          type: "progress",
          title: `Sign in to ${definition.name}`,
          message: event.message,
          ...(session.lastExternalUrl
            ? { externalUrl: session.lastExternalUrl }
            : {}),
        };
      } else if (event.type === "info") {
        session.step = {
          id: "oauth-info",
          type: "note",
          title: `Sign in to ${definition.name}`,
          message: event.message,
          ...(event.links?.[0]?.url ? { externalUrl: event.links[0].url } : {}),
        };
      }
      markSessionChanged(session);
    },
    prompt(prompt) {
      return new Promise((resolve, reject) => {
        const stepId = `oauth-prompt-${Date.now()}`;
        session.pendingPrompt = { stepId, resolve, reject };
        session.step = {
          id: stepId,
          type: prompt.type === "select" ? "select" : "text",
          title: `Sign in to ${definition.name}`,
          message: prompt.message,
          ...(prompt.placeholder ? { placeholder: prompt.placeholder } : {}),
          ...(prompt.type === "secret" ? { sensitive: true } : {}),
          ...(prompt.type === "select"
            ? {
                options: prompt.options.map((option) => ({
                  value: option.id,
                  label: option.label,
                  ...(option.description ? { hint: option.description } : {}),
                })),
              }
            : {}),
          ...(session.lastExternalUrl
            ? { externalUrl: session.lastExternalUrl }
            : {}),
        };
        const abort = () =>
          reject(
            prompt.signal?.reason ||
              controller.signal.reason ||
              new Error("Login cancelled."),
          );
        prompt.signal?.addEventListener("abort", abort, { once: true });
        controller.signal.addEventListener("abort", abort, { once: true });
        markSessionChanged(session);
      });
    },
  };

  void modelsForScope(scope)
    .login(definition.providerId, "oauth", interaction)
    .then(() => {
      session.step = undefined;
      session.pendingPrompt = undefined;
      session.done = true;
      markSessionChanged(session);
    })
    .catch((error) => {
      session.step = undefined;
      session.pendingPrompt = undefined;
      session.done = true;
      session.error = controller.signal.aborted
        ? "Login cancelled."
        : error instanceof Error
          ? error.message
          : String(error);
      markSessionChanged(session);
    });
  return session;
}

async function verifyRuntime(scope, runtimeId, modelId) {
  const definition = runtimeDefinition(runtimeId);
  const models = modelsForScope(scope);
  const model = models.getModel(definition.providerId, modelId);
  if (!model)
    throw new Error(`${definition.name} does not provide model ${modelId}.`);
  const message = await models.complete(
    model,
    {
      messages: [
        {
          role: "user",
          content: "Reply with the single word ready.",
          timestamp: Date.now(),
        },
      ],
    },
    { maxTokens: 8, signal: AbortSignal.timeout(60_000) },
  );
  if (message.stopReason === "error" || message.stopReason === "aborted") {
    throw new Error(
      message.errorMessage || `${definition.name} verification failed.`,
    );
  }
}

const server = createServer(async (request, response) => {
  try {
    if (request.headers.authorization !== `Bearer ${token}`) {
      sendJson(response, 401, { error: "Unauthorized." });
      return;
    }
    const url = new URL(request.url, `http://${host}:${port}`);
    if (request.method === "GET" && url.pathname === "/health") {
      sendJson(response, 200, { ok: true });
      return;
    }

    const scope = requestScope(request);
    if (request.method === "GET" && url.pathname === "/v1/runtimes") {
      sendJson(response, 200, {
        runtimes: await Promise.all(
          Object.keys(PROVIDERS).map((id) => runtimeStatus(scope, id)),
        ),
      });
      return;
    }

    const modelsMatch = url.pathname.match(/^\/v1\/runtimes\/([^/]+)\/models$/);
    if (request.method === "GET" && modelsMatch) {
      const id = decodeURIComponent(modelsMatch[1]);
      sendJson(response, 200, { models: await catalogFor(scope, id) });
      return;
    }

    const loginMatch = url.pathname.match(/^\/v1\/runtimes\/([^/]+)\/login$/);
    if (request.method === "POST" && loginMatch) {
      const id = decodeURIComponent(loginMatch[1]);
      const { sessionId } = await readJson(request);
      if (
        typeof sessionId !== "string" ||
        sessionId.length < 1 ||
        sessionId.length > 128
      ) {
        throw new Error("A valid sessionId is required.");
      }
      const session = startLogin(scope, id, sessionId);
      await waitForSessionChange(session);
      sendJson(response, 200, sessionResult(session));
      return;
    }

    const sessionMatch = url.pathname.match(/^\/v1\/login-sessions\/([^/]+)$/);
    if (sessionMatch) {
      const sessionId = decodeURIComponent(sessionMatch[1]);
      const key = sessionKey(scope, sessionId);
      const session = loginSessions.get(key);
      if (!session) throw new Error("Unknown login session.");
      if (request.method === "POST") {
        const answer = await readJson(request);
        if (
          session.pendingPrompt &&
          answer?.stepId === session.pendingPrompt.stepId
        ) {
          const pending = session.pendingPrompt;
          session.pendingPrompt = undefined;
          session.step = undefined;
          pending.resolve(String(answer.value ?? ""));
        }
        await waitForSessionChange(session);
        const result = sessionResult(session);
        if (session.done) loginSessions.delete(key);
        sendJson(response, 200, result);
        return;
      }
      if (request.method === "DELETE") {
        session.controller.abort(new Error("Login cancelled."));
        loginSessions.delete(key);
        sendJson(response, 200, { ok: true });
        return;
      }
    }

    const verifyMatch = url.pathname.match(/^\/v1\/runtimes\/([^/]+)\/verify$/);
    if (request.method === "POST" && verifyMatch) {
      const id = decodeURIComponent(verifyMatch[1]);
      const { model } = await readJson(request);
      if (typeof model !== "string" || !model.trim())
        throw new Error("A model is required.");
      await verifyRuntime(scope, id, model);
      sendJson(response, 200, { ok: true });
      return;
    }

    if (request.method === "POST" && url.pathname === "/messages") {
      await streamMessages(request, response);
      return;
    }

    sendJson(response, 404, { error: "Not found." });
  } catch (error) {
    if (!response.headersSent) {
      sendJson(response, 400, {
        error: error instanceof Error ? error.message : String(error),
      });
    } else {
      response.write(
        `data: ${JSON.stringify({
          type: "error",
          reason: "error",
          usage: zeroSubscriptionCost({
            input: 0,
            output: 0,
            cacheRead: 0,
            cacheWrite: 0,
            totalTokens: 0,
            cost: {},
          }),
          errorMessage: error instanceof Error ? error.message : String(error),
        })}\n\n`,
      );
      response.end();
    }
  }
});

server.listen(port, host, () => {
  console.log(`Pi model runtime listening on http://${host}:${port}`);
});

function shutdown() {
  for (const session of loginSessions.values()) {
    session.controller.abort(new Error("Pi model runtime stopped."));
  }
  server.close(() => process.exit(0));
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
