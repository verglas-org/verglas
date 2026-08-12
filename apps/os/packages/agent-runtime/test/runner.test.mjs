import assert from "node:assert/strict";
import test from "node:test";
import {
  createPiTools,
  localRuntimeModel,
  messagesForPi,
} from "../src/runner.mjs";

test("Pi receives the generic tool surface and permission requests terminate the run", async () => {
  const calls = [];
  const tools = createPiTools(async (name, args) => {
    calls.push([name, args]);
    return name === "requestPermission"
      ? { permissionRequested: true, requestId: "1:request" }
      : { ok: true };
  });

  assert.deepEqual(
    tools.map((tool) => tool.name),
    [
      "describeEnvironment",
      "execute",
      "readFile",
      "writeFile",
      "editFile",
      "webFetch",
      "requestPermission",
    ],
  );
  const permission = tools.find((tool) => tool.name === "requestPermission");
  const result = await permission.execute("call-1", {
    resourceId: "database/analytics",
    actions: ["query"],
    reason: "Answer the user's question.",
  });
  assert.equal(result.terminate, true);
  assert.deepEqual(calls, [
    [
      "requestPermission",
      {
        resourceId: "database/analytics",
        actions: ["query"],
        reason: "Answer the user's question.",
      },
    ],
  ]);
});

test("Pi model routing uses one stable native-runtime session", () => {
  assert.deepEqual(
    localRuntimeModel(
      { provider: "local-runtime", runtime: "codex", model: "gpt-5.6-sol" },
      "http://models:8790/",
    ),
    {
      id: "gpt-5.6-sol",
      name: "gpt-5.6-sol",
      api: "pi-messages",
      provider: "openai-codex",
      baseUrl: "http://models:8790",
      reasoning: true,
      input: ["text"],
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      contextWindow: 128_000,
      maxTokens: 16_384,
    },
  );
});

test("persisted chat history becomes a Pi transcript", () => {
  const model = localRuntimeModel(
    { provider: "local-runtime", runtime: "codex", model: "gpt-5.6-sol" },
    "http://models",
  );
  const messages = messagesForPi(
    [
      {
        author: { type: "user" },
        timestamp: "2026-08-11T00:00:00.000Z",
        body: { type: "message", message: "Inspect sales." },
      },
      {
        author: { type: "agent" },
        timestamp: "2026-08-11T00:00:01.000Z",
        body: { type: "message", message: "Done." },
      },
    ],
    model,
  );
  assert.equal(messages[0].role, "user");
  assert.equal(messages[1].role, "assistant");
  assert.equal(messages[1].api, "pi-messages");
});
