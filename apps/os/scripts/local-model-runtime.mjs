#!/usr/bin/env node

// Loopback adapter for subscription-backed coding-agent CLIs. The Workshop Worker cannot spawn
// host processes, so local deployments keep that authority in this small companion process.

import { execFile, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { discoverRuntimeModels } from "./model-runtime-catalog.mjs";
import { parseRuntimeOutput } from "./model-runtime-response.mjs";

const execFileAsync = promisify(execFile);
const host = process.env.LOCAL_MODEL_RUNTIME_HOST || "127.0.0.1";
const port = Number.parseInt(
  process.env.LOCAL_MODEL_RUNTIME_PORT || "8790",
  10,
);
const token = process.env.LOCAL_MODEL_RUNTIME_TOKEN;

if (!token) throw new Error("LOCAL_MODEL_RUNTIME_TOKEN is required.");
if (!Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error("LOCAL_MODEL_RUNTIME_PORT must be a valid TCP port.");
}

const runtimes = {
  codex: {
    executable: "codex",
    statusArgs: ["login", "status"],
    // Match OpenClaw's app-guided flow: show a browser URL plus short-lived device code rather
    // than depending on a hidden localhost callback owned by a child CLI process.
    loginArgs: ["login", "--device-auth"],
    linked: (output) => /logged in/i.test(output),
    linkedDetail: (output) => output.trim() || "Signed in with Codex.",
  },
  "claude-code": {
    executable: "claude",
    statusArgs: ["auth", "status", "--json"],
    loginArgs: ["auth", "login", "--claudeai"],
    linked: (output) => {
      try {
        return JSON.parse(output).loggedIn === true;
      } catch {
        return false;
      }
    },
    linkedDetail: (output) => {
      try {
        const status = JSON.parse(output);
        return status.email
          ? `Signed in as ${status.email}.`
          : "Signed in with Claude Code.";
      } catch {
        return "Signed in with Claude Code.";
      }
    },
  },
  cursor: {
    executable: "cursor-agent",
    statusArgs: ["status"],
    // The adapter owns this generated workspace. Cursor must not stop an app-guided login on a
    // workspace-trust prompt that the user cannot answer inside the Workshop UI.
    loginArgs: ["--trust", "login"],
    linked: (output) => /logged in as/i.test(output),
    linkedDetail: (output) =>
      output.replace(/^\s*[✓✔]\s*/u, "").trim() || "Signed in with Cursor.",
  },
};

const loginSessions = new Map();
const inferenceCwd = mkdtempSync(join(tmpdir(), "verglas-model-runtime-"));
function outputSchema(tools) {
  // Mirror an OpenAI assistant message. Codecs/CLIs that support --output-schema /
  // --json-schema constrain to this; Cursor only gets it as a prompt target.
  const toolNames = tools.flatMap((tool) => {
    const fn = tool?.function;
    return fn && typeof fn.name === "string" ? [fn.name] : [];
  });
  const toolCallSchema = {
    type: "object",
    properties: {
      name:
        toolNames.length > 0
          ? { type: "string", enum: toolNames }
          : { type: "string" },
      arguments: { type: "string" },
    },
    required: ["name", "arguments"],
    additionalProperties: false,
  };
  return {
    type: "object",
    properties: {
      content: { type: ["string", "null"] },
      // Codex structured outputs intentionally support a strict JSON Schema subset. Keep the
      // tool name constrained, and carry its provider-specific arguments as a JSON string that
      // parseRuntimeOutput normalizes back into an object.
      tool_calls:
        toolNames.length > 0
          ? {
              type: "array",
              items: toolCallSchema,
            }
          : { type: "array", items: toolCallSchema, maxItems: 0 },
    },
    required: ["content", "tool_calls"],
    additionalProperties: false,
  };
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

async function readJson(request, limit = 64 * 1024) {
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

function runCli(executable, args, input, env = process.env) {
  return new Promise((resolve, reject) => {
    const child = spawn(executable, args, {
      cwd: inferenceCwd,
      env,
      stdio: ["pipe", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    const timer = setTimeout(() => {
      child.kill("SIGTERM");
      reject(new Error(`${executable} inference timed out.`));
    }, 900_000);
    const append = (current, chunk) => {
      const next = current + chunk;
      if (next.length > 8 * 1024 * 1024) {
        child.kill("SIGTERM");
        reject(new Error(`${executable} returned too much output.`));
      }
      return next;
    };
    child.stdout.on("data", (chunk) => {
      stdout = append(stdout, chunk);
    });
    child.stderr.on("data", (chunk) => {
      stderr = append(stderr, chunk);
    });
    child.on("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.on("exit", (code) => {
      clearTimeout(timer);
      if (code === 0) resolve(stdout.trim());
      else
        reject(
          new Error(
            stderr.trim() ||
              stdout.trim() ||
              `${executable} exited with code ${code}.`,
          ),
        );
    });
    child.stdin.end(input);
  });
}

function inferencePrompt(body) {
  const tools = Array.isArray(body.tools) ? body.tools : [];
  return [
    "You are the model behind an OpenAI-compatible chat.completions endpoint.",
    "Do not inspect files, run shell commands, or use native Cursor/Codex/Claude tools.",
    "Return exactly one JSON object shaped like an assistant message — no markdown fence.",
    tools.length > 0
      ? [
          'Shape: {"content": string|null, "tool_calls": [{"name": string, "arguments": string}]}.',
          "Each arguments value must be a JSON-encoded object string matching that tool's schema.",
          "If the Workshop conversation needs a listed tool, put it in tool_calls and set content to null.",
          "If you are answering the user with no tool, put the reply in content and set tool_calls to [].",
          "Use only tool names from Available tools. Prefer modest createIntegration / createApplication",
          "payloads over one giant createVessel blob.",
        ].join(" ")
      : 'Shape: {"content": "<reply>", "tool_calls": []}.',
    `Conversation:\n${JSON.stringify(body.messages ?? [])}`,
    `Available tools:\n${JSON.stringify(tools)}`,
  ].join("\n\n");
}

async function invokeRuntime(runtimeId, modelId, body, providerApiKey) {
  const tools = Array.isArray(body.tools) ? body.tools : [];
  let prompt = inferencePrompt(body);
  const schema = outputSchema(tools);
  const schemaPath = join(inferenceCwd, `response-schema-${randomUUID()}.json`);
  writeFileSync(schemaPath, JSON.stringify(schema));
  for (let attempt = 0; attempt < 2; attempt++) {
    let output;
    if (runtimeId === "codex") {
      output = await runCli(
        "codex",
        [
          "exec",
          "--ignore-user-config",
          "--ignore-rules",
          "--ephemeral",
          "--skip-git-repo-check",
          "--sandbox",
          "read-only",
          "--color",
          "never",
          "--model",
          modelId,
          "--output-schema",
          schemaPath,
          "-",
        ],
        prompt,
      );
    } else if (runtimeId === "claude-code") {
      output = await runCli(
        "claude",
        [
          "-p",
          "--model",
          modelId,
          "--tools",
          "",
          "--permission-mode",
          "bypassPermissions",
          "--strict-mcp-config",
          "--output-format",
          "json",
          "--json-schema",
          JSON.stringify(schema),
        ],
        prompt,
      );
    } else if (runtimeId === "cursor") {
      // No --mode plan: that forces read-only narration. Empty inferenceCwd + --trust only.
      output = await runCli(
        "cursor-agent",
        ["--trust", "-p", "--model", modelId, "--output-format", "json"],
        prompt,
        providerApiKey
          ? { ...process.env, CURSOR_API_KEY: providerApiKey }
          : process.env,
      );
    } else {
      throw new Error(`Unknown local model runtime: ${runtimeId}`);
    }
    const result = parseRuntimeOutput(output);
    if (result) {
      const names = result.tool_calls.map((call) => call.name).join(",");
      console.log(
        `[model-runtime] ${runtimeId}/${modelId} ok` +
          (names ? ` tool_calls=${names}` : " content"),
      );
      return result;
    }
    console.warn(
      `[model-runtime] ${runtimeId}/${modelId} attempt ${attempt + 1} produced unusable output ` +
        `(${output.length} chars); retrying with a tighter instruction.`,
    );
    prompt += [
      "",
      "Your previous response was empty, truncated, or not valid assistant-message JSON.",
      'Return exactly {"content": string|null, "tool_calls": [...]} with no trailing junk.',
      tools.length > 0
        ? "If work remains, put Workshop tool calls in tool_calls and set content to null."
        : "Put the user-visible reply in content and set tool_calls to [].",
    ].join("\n");
  }
  throw new Error(`${runtimeId}/${modelId} returned no assistant message.`);
}

function completion(body, result) {
  const id = `chatcmpl_${randomUUID().replaceAll("-", "")}`;
  const toolCalls =
    result.tool_calls.length > 0
      ? result.tool_calls.map((call, index) => ({
          id: `call_${randomUUID().replaceAll("-", "")}`,
          type: "function",
          index,
          function: {
            name: String(call.name),
            arguments: JSON.stringify(call.arguments ?? {}),
          },
        }))
      : undefined;
  return {
    id,
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model: body.model,
    choices: [
      {
        index: 0,
        message: {
          role: "assistant",
          content: toolCalls ? null : result.content,
          ...(toolCalls
            ? {
                tool_calls: toolCalls.map(({ index: _index, ...call }) => call),
              }
            : {}),
        },
        finish_reason: toolCalls ? "tool_calls" : "stop",
      },
    ],
    usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
  };
}

function sendCompletion(response, body, value) {
  if (!body.stream) {
    sendJson(response, 200, value);
    return;
  }
  response.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-store",
    connection: "keep-alive",
  });
  const message = value.choices[0].message;
  const chunk = {
    id: value.id,
    object: "chat.completion.chunk",
    created: value.created,
    model: value.model,
    choices: [
      {
        index: 0,
        delta: {
          role: "assistant",
          ...(message.content ? { content: message.content } : {}),
          ...(message.tool_calls
            ? {
                tool_calls: message.tool_calls.map((call, index) => ({
                  ...call,
                  index,
                })),
              }
            : {}),
        },
        finish_reason: value.choices[0].finish_reason,
      },
    ],
  };
  response.write(`data: ${JSON.stringify(chunk)}\n\n`);
  response.end("data: [DONE]\n\n");
}

async function inspectRuntime(id) {
  const runtime = runtimes[id];
  try {
    const { stdout, stderr } = await execFileAsync(
      runtime.executable,
      runtime.statusArgs,
      {
        timeout: 10_000,
        maxBuffer: 256 * 1024,
      },
    );
    const output = `${stdout}${stderr}`.trim();
    const linked = runtime.linked(output);
    return {
      id,
      available: true,
      linked,
      detail: linked
        ? runtime.linkedDetail(output)
        : `${runtime.executable} is installed but not signed in.`,
      supportsGuidedLogin: true,
    };
  } catch (error) {
    if (error?.code === "ENOENT") {
      return {
        id,
        available: false,
        linked: false,
        detail: `${runtime.executable} is not installed on this machine.`,
        supportsGuidedLogin: false,
      };
    }
    const output = `${error?.stdout || ""}${error?.stderr || ""}`.trim();
    return {
      id,
      available: true,
      linked: runtime.linked(output),
      detail:
        output || `${runtime.executable} login status could not be determined.`,
      supportsGuidedLogin: true,
    };
  }
}

function startLogin(id, sessionId) {
  const runtime = runtimes[id];
  if (!runtime) throw new Error(`Unknown local model runtime: ${id}`);
  if (loginSessions.has(sessionId))
    throw new Error("Login session already exists.");

  const child = spawn(runtime.executable, runtime.loginArgs, {
    env: process.env,
    stdio: ["ignore", "pipe", "pipe"],
  });
  const session = {
    child,
    runtimeId: id,
    output: "",
    done: false,
    error: undefined,
  };
  loginSessions.set(sessionId, session);
  const append = (chunk) => {
    session.output = `${session.output}${chunk}`.slice(-16_384);
  };
  child.stdout.on("data", append);
  child.stderr.on("data", append);
  child.on("error", (error) => {
    session.error = error.message;
    session.done = true;
  });
  child.on("exit", (code) => {
    if (code && !session.error)
      session.error =
        session.output.trim() || `Login exited with code ${code}.`;
    session.done = true;
  });
  return session;
}

async function loginResult(sessionId) {
  const session = loginSessions.get(sessionId);
  if (!session) throw new Error("Unknown login session.");
  const id = session.runtimeId;
  const status = await inspectRuntime(id);
  if (status.linked) {
    session.child.kill();
    loginSessions.delete(sessionId);
    return { sessionId, done: true, status: "done" };
  }
  if (session.done) {
    loginSessions.delete(sessionId);
    return {
      sessionId,
      done: true,
      status: "error",
      error: session.error || status.detail,
    };
  }
  const externalUrl = session.output
    .match(/https:\/\/\S+/)?.[0]
    .split(String.fromCharCode(27))[0];
  const deviceCode =
    id === "codex"
      ? session.output.match(/(?:code|enter)\s*[: ]\s*([A-Z0-9-]{6,})/i)?.[1]
      : undefined;
  return {
    sessionId,
    done: false,
    status: "running",
    step: {
      id: "wait-for-browser-login",
      type: "progress",
      title: `Sign in to ${id === "codex" ? "Codex" : id === "claude-code" ? "Claude" : "Cursor"}`,
      message:
        session.output.trim() ||
        "Complete the sign-in flow in the browser, then continue.",
      ...(externalUrl ? { externalUrl } : {}),
      ...(deviceCode ? { deviceCode: { code: deviceCode } } : {}),
    },
  };
}

async function verifyRuntime(runtimeId, modelId, providerApiKey) {
  const status = await inspectRuntime(runtimeId);
  if (!status.available) throw new Error(status.detail);
  if (!status.linked && !providerApiKey) throw new Error(status.detail);
  const result = await invokeRuntime(
    runtimeId,
    modelId,
    {
      model: modelId,
      messages: [
        { role: "user", content: "Reply with the single word ready." },
      ],
      tools: [],
    },
    providerApiKey,
  );
  if (result.tool_calls.length > 0 || !result.content?.trim()) {
    throw new Error(`${runtimeId} did not return a verification response.`);
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
    if (request.method === "GET" && url.pathname === "/v1/runtimes") {
      const statuses = await Promise.all(
        Object.keys(runtimes).map(inspectRuntime),
      );
      sendJson(response, 200, { runtimes: statuses });
      return;
    }
    const modelsMatch = url.pathname.match(/^\/v1\/runtimes\/([^/]+)\/models$/);
    if (
      (request.method === "GET" || request.method === "POST") &&
      modelsMatch
    ) {
      const id = decodeURIComponent(modelsMatch[1]);
      if (!runtimes[id]) throw new Error("A supported runtime is required.");
      const body = request.method === "POST" ? await readJson(request) : {};
      sendJson(response, 200, {
        models: await discoverRuntimeModels(
          id,
          typeof body.apiToken === "string" ? body.apiToken : undefined,
        ),
      });
      return;
    }
    if (request.method === "POST" && url.pathname === "/v1/chat/completions") {
      const body = await readJson(request, 16 * 1024 * 1024);
      const runtimeId =
        request.headers["x-model-runtime"] ||
        (runtimes[body.model] ? body.model : undefined);
      if (typeof runtimeId !== "string" || !runtimes[runtimeId]) {
        throw new Error("A supported model runtime is required.");
      }
      if (typeof body.model !== "string" || !body.model.trim()) {
        throw new Error("A provider model is required.");
      }
      const result = await invokeRuntime(
        runtimeId,
        body.model,
        body,
        request.headers["x-provider-api-key"],
      );
      sendCompletion(response, body, completion(body, result));
      return;
    }

    const verifyMatch = url.pathname.match(/^\/v1\/runtimes\/([^/]+)\/verify$/);
    if (request.method === "POST" && verifyMatch) {
      const id = decodeURIComponent(verifyMatch[1]);
      if (!runtimes[id]) throw new Error("A supported runtime is required.");
      const body = await readJson(request);
      const models = await discoverRuntimeModels(id);
      const modelId =
        typeof body.model === "string" && body.model.trim()
          ? body.model.trim()
          : (models.find((model) => model.isDefault) || models[0])?.id;
      if (!modelId) throw new Error(`${id} has no available models.`);
      await verifyRuntime(
        id,
        modelId,
        typeof body.apiToken === "string" ? body.apiToken : undefined,
      );
      sendJson(response, 200, { ok: true });
      return;
    }

    const startMatch = url.pathname.match(/^\/v1\/runtimes\/([^/]+)\/login$/);
    if (request.method === "POST" && startMatch) {
      const id = decodeURIComponent(startMatch[1]);
      if (!runtimes[id]) throw new Error("A supported runtime is required.");
      const { sessionId } = await readJson(request);
      if (
        typeof sessionId !== "string" ||
        sessionId.length < 1 ||
        sessionId.length > 128
      ) {
        throw new Error("A valid sessionId is required.");
      }
      const status = await inspectRuntime(id);
      if (status.linked) {
        sendJson(response, 200, { sessionId, done: true, status: "done" });
        return;
      }
      startLogin(id, sessionId);
      sendJson(response, 200, await loginResult(sessionId));
      return;
    }

    const sessionMatch = url.pathname.match(/^\/v1\/login-sessions\/([^/]+)$/);
    if (sessionMatch) {
      const sessionId = decodeURIComponent(sessionMatch[1]);
      if (request.method === "POST") {
        sendJson(response, 200, await loginResult(sessionId));
        return;
      }
      if (request.method === "DELETE") {
        const session = loginSessions.get(sessionId);
        session?.child.kill();
        loginSessions.delete(sessionId);
        sendJson(response, 200, { ok: true });
        return;
      }
    }

    sendJson(response, 404, { error: "Not found." });
  } catch (error) {
    sendJson(response, 400, {
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

server.listen(port, host, () => {
  console.log(
    `Native model runtime adapter listening on http://${host}:${port}`,
  );
});

function shutdown() {
  for (const session of loginSessions.values()) session.child.kill();
  server.close(() => process.exit(0));
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
