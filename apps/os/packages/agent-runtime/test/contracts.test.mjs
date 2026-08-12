import assert from "node:assert/strict";
import test from "node:test";
import {
  authorizationDecision,
  boundedPrompt,
  gatewayTargetToken,
  requireIdentifier,
  requireScopedToken,
  runtimeGatewayAuthorization,
  runCapabilityEnvironment,
  runDeploymentId,
} from "../src/contracts.mjs";

test("authorizationDecision unwraps the access service response", () => {
  assert.deepEqual(
    authorizationDecision({
      identity: { principal_id: "token/example" },
      decision: { allowed: true, policy_version: 7 },
    }),
    { allowed: true, policy_version: 7 },
  );
});

test("agent runs use the container runtime ephemeral namespace", () => {
  assert.equal(runDeploymentId("abc123"), "run-agent-abc123");
  assert.throws(() => runDeploymentId("../../escape"), /run id/);
});

test("prompts are non-empty and bounded", () => {
  assert.equal(boundedPrompt("  inspect data  "), "inspect data");
  assert.throws(() => boundedPrompt(""), /required/);
  assert.equal(
    requireIdentifier("workspace-a.1", "workspace"),
    "workspace-a.1",
  );
});

test("agent runs receive one scoped token rather than controller credentials", () => {
  const environment = runCapabilityEnvironment({
    runId: "abc123",
    scopedToken: "one-run-only",
    principalId: "agent/workspace-a",
    chatId: 4,
    modelUrl: "http://models",
    modelToken: "model-only",
  });
  assert.equal(environment.VERGLAS_TOKEN, "one-run-only");
  assert.match(environment.VERGLAS_DATA_ENDPOINT, /run-gateway\/abc123\/data$/);
  assert.equal("VERGLAS_DATA_TOKEN" in environment, false);
  assert.equal("VERGLAS_ACCESS_SERVICE_TOKEN" in environment, false);
  assert.equal("VERGLAS_CONTAINER_RUNTIME_TOKEN" in environment, false);
  assert.equal("VERGLAS_AGENT_CONTROLLER_TOKEN" in environment, false);
  assert.equal("DATABASE_URL" in environment, false);
  assert.equal("VERGLAS_AGENT_RUNTIME_TOKEN" in environment, false);
  assert.equal(environment.VERGLAS_AGENT_WORKSPACE, "/workspace");
});

test("data and access APIs retain the run token while runtime routes require explicit RBAC", () => {
  assert.equal(
    runtimeGatewayAuthorization("data", "POST", "/v1/anything"),
    null,
  );
  assert.equal(
    runtimeGatewayAuthorization("access", "GET", "/v1/databases"),
    null,
  );
  assert.deepEqual(
    runtimeGatewayAuthorization(
      "runtime",
      "PUT",
      "/v1/vessels/dashboard/project",
    ),
    { resourceId: "tenant", action: "deploy" },
  );
  assert.deepEqual(
    runtimeGatewayAuthorization("runtime", "GET", "/v1/vessels"),
    { resourceId: "tenant", action: "discover" },
  );
  assert.deepEqual(
    runtimeGatewayAuthorization(
      "runtime",
      "POST",
      "/v1/vessels/dashboard/http/api/refresh",
    ),
    { resourceId: "vessel/dashboard", action: "execute" },
  );
  assert.throws(
    () => runtimeGatewayAuthorization("runtime", "POST", "/v1/internal"),
    /not exposed/,
  );
});

test("agent runtime accepts only a caller-minted scoped token", () => {
  assert.equal(
    requireScopedToken("already-minted-token"),
    "already-minted-token",
  );
  assert.throws(() => requireScopedToken(""), /scoped token/);
  assert.throws(() => requireScopedToken(undefined), /scoped token/);
});

test("gateway translates to its private container token only after authorization", () => {
  assert.equal(
    gatewayTargetToken("runtime", true, "run-token", "container-runtime-token"),
    "container-runtime-token",
  );
  assert.equal(
    gatewayTargetToken("data", true, "run-token", "container-runtime-token"),
    "run-token",
  );
  assert.throws(
    () =>
      gatewayTargetToken(
        "runtime",
        false,
        "run-token",
        "container-runtime-token",
      ),
    /permission denied/,
  );
});
