import assert from "node:assert/strict";
import { mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { createToolExecutor, toolDefinitions } from "../src/tools.mjs";

function environment(workspace, overrides = {}) {
  return {
    VERGLAS_AGENT_WORKSPACE: workspace,
    VERGLAS_DATA_ENDPOINT: "http://data",
    VERGLAS_CONTAINER_RUNTIME_URL: "http://runtime",
    VERGLAS_ACCESS_URI: "http://access",
    VERGLAS_TOKEN: "scoped-token",
    VERGLAS_AGENT_PRINCIPAL_ID: "agent/session",
    VERGLAS_AGENT_CHAT_ID: "1",
    ...overrides,
  };
}

test("agent tools expose the environment rather than domain-specific API wrappers", () => {
  assert.deepEqual(
    toolDefinitions.map((definition) => definition.function.name),
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
  for (const removed of [
    "listLakehouse",
    "queryLakehouse",
    "deployApplication",
    "deployIntegration",
    "deployJob",
  ]) {
    assert.equal(
      toolDefinitions.some(
        (definition) => definition.function.name === removed,
      ),
      false,
    );
  }
});

test("describeEnvironment advertises the SDK and capabilities without exposing secrets", async () => {
  const workspace = await mkdtemp(join(tmpdir(), "verglas-agent-"));
  try {
    const execute = createToolExecutor(environment(workspace), async () => {});
    const result = await execute("describeEnvironment", {});

    assert.equal(result.workspace, workspace);
    assert.equal(result.sdk.package, "@verglas/sdk");
    assert.equal(result.sdk.dataEndpoint, "VERGLAS_DATA_ENDPOINT");
    assert.equal(JSON.stringify(result).includes("scoped-token"), false);
  } finally {
    await rm(workspace, { recursive: true, force: true });
  }
});

test("file tools edit files inside the run workspace and reject path traversal", async () => {
  const workspace = await mkdtemp(join(tmpdir(), "verglas-agent-"));
  try {
    const execute = createToolExecutor(environment(workspace), async () => {});
    await execute("writeFile", {
      path: "src/index.ts",
      content: "old value\n",
    });
    await execute("editFile", {
      path: "src/index.ts",
      oldText: "old",
      newText: "new",
    });
    assert.deepEqual(await execute("readFile", { path: "src/index.ts" }), {
      path: "src/index.ts",
      content: "new value\n",
      truncated: false,
    });
    await assert.rejects(
      execute("readFile", { path: "../secret" }),
      /inside the agent workspace/,
    );
    await assert.rejects(
      execute("writeFile", { path: "/tmp/escape", content: "no" }),
      /inside the agent workspace/,
    );
  } finally {
    await rm(workspace, { recursive: true, force: true });
  }
});

test("execute runs without a shell, uses the workspace, and isolates controller credentials", async () => {
  const workspace = await mkdtemp(join(tmpdir(), "verglas-agent-"));
  try {
    const execute = createToolExecutor(
      environment(workspace, {
        LOCAL_MODEL_RUNTIME_TOKEN: "model-controller-secret",
        VERGLAS_AGENT_CONTROLLER_URL: "http://controller",
      }),
      async () => {},
    );
    const result = await execute("execute", {
      command: process.execPath,
      args: [
        "-e",
        "console.log(process.cwd()); console.error(process.env.VERGLAS_TOKEN); console.log(String(process.env.LOCAL_MODEL_RUNTIME_TOKEN)); console.log(String(process.env.VERGLAS_AGENT_CONTROLLER_URL))",
      ],
    });

    assert.equal(result.exitCode, 0);
    const stdoutLines = result.stdout.trim().split("\n");
    assert.equal(stdoutLines[0], await realpath(workspace));
    assert.match(result.stderr, /\[REDACTED\]/);
    assert.equal(result.stderr.includes("scoped-token"), false);
    assert.equal(result.stdout.includes("model-controller-secret"), false);
    assert.deepEqual(stdoutLines.slice(1), ["undefined", "undefined"]);
  } finally {
    await rm(workspace, { recursive: true, force: true });
  }
});

test("webFetch accepts HTTPS only and returns a bounded response", async () => {
  const workspace = await mkdtemp(join(tmpdir(), "verglas-agent-"));
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () =>
    new Response("x".repeat(140_000), {
      headers: { "content-type": "text/plain" },
    });
  try {
    const execute = createToolExecutor(environment(workspace), async () => {});
    await assert.rejects(
      execute("webFetch", { url: "http://example.com" }),
      /HTTPS/,
    );
    const result = await execute("webFetch", { url: "https://example.com" });
    assert.equal(result.status, 200);
    assert.equal(result.truncated, true);
    assert.ok(result.body.length < 140_000);
  } finally {
    globalThis.fetch = originalFetch;
    await rm(workspace, { recursive: true, force: true });
  }
});

test("permission requests reject resource identifiers the access service cannot grant", async () => {
  const workspace = await mkdtemp(join(tmpdir(), "verglas-agent-"));
  const emitted = [];
  try {
    const execute = createToolExecutor(
      environment(workspace),
      async (message) => emitted.push(message),
    );
    await assert.rejects(
      execute("requestPermission", {
        resourceId: "tool:listLakehouse",
        actions: ["execute"],
        reason: "Inspect the lakehouse.",
      }),
      /valid resource identifier/,
    );
    assert.deepEqual(emitted, []);
  } finally {
    await rm(workspace, { recursive: true, force: true });
  }
});
