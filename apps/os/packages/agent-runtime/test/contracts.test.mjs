import assert from "node:assert/strict";
import test from "node:test";
import {
  boundedPrompt, requireIdentifier, runCapabilityEnvironment, runDeploymentId,
} from "../src/contracts.mjs";

test("agent runs use the container runtime ephemeral namespace", () => {
  assert.equal(runDeploymentId("abc123"), "run-agent-abc123");
  assert.throws(() => runDeploymentId("../../escape"), /run id/);
});

test("prompts are non-empty and bounded", () => {
  assert.equal(boundedPrompt("  inspect data  "), "inspect data");
  assert.throws(() => boundedPrompt(""), /required/);
  assert.equal(requireIdentifier("workspace-a.1", "workspace"), "workspace-a.1");
});

test("agent runs receive one per-run capability rather than controller credentials", () => {
  const environment = runCapabilityEnvironment({
    runId: "abc123",
    runToken: "one-run-only",
    tenantId: "local",
    principalId: "agent/workspace-a",
    chatId: 4,
    modelUrl: "http://models",
    modelToken: "model-only",
  });
  assert.equal(environment.VERGLAS_DATA_TOKEN, "one-run-only");
  assert.equal(environment.VERGLAS_CONTAINER_RUNTIME_TOKEN, "one-run-only");
  assert.match(environment.VERGLAS_DATA_ENDPOINT, /run-gateway\/abc123\/data$/);
  assert.equal("DATABASE_URL" in environment, false);
  assert.equal("VERGLAS_AGENT_RUNTIME_TOKEN" in environment, false);
});
