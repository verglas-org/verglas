const identifierPattern = /^[a-zA-Z0-9_.-]+$/;

export function requireIdentifier(value, label) {
  if (typeof value !== "string" || !identifierPattern.test(value)) {
    throw new Error(
      `${label} must contain only letters, numbers, dots, underscores, and dashes.`,
    );
  }
  return value;
}

export function runDeploymentId(runId) {
  return `run-agent-${requireIdentifier(runId, "run id")}`;
}

export function runCapabilityEnvironment({
  runId,
  scopedToken,
  principalId,
  chatId,
  modelUrl,
  modelToken,
}) {
  const gateway = `http://verglas-agent-runtime:8390/v1/run-gateway/${runId}`;
  return {
    VERGLAS_AGENT_CONTROLLER_URL: `${gateway}/control`,
    VERGLAS_DATA_ENDPOINT: `${gateway}/data`,
    VERGLAS_CONTAINER_RUNTIME_URL: `${gateway}/runtime`,
    VERGLAS_TOKEN: scopedToken,
    LOCAL_MODEL_RUNTIME_URL: modelUrl,
    LOCAL_MODEL_RUNTIME_TOKEN: modelToken,
    VERGLAS_ACCESS_URI: `${gateway}/access`,
    VERGLAS_AGENT_PRINCIPAL_ID: principalId,
    VERGLAS_AGENT_CHAT_ID: String(chatId),
    VERGLAS_AGENT_WORKSPACE: "/workspace",
  };
}

/** Returns the additional authorization required before proxying a gateway call. */
export function runtimeGatewayAuthorization(service, method, path) {
  if (service === "data" || service === "access") return null;
  if (service !== "runtime" || !/^\/v1\/vessels(?:\/|$)/.test(path)) {
    throw new Error("operation is not exposed to agent runs");
  }
  const vesselHttp = path.match(/^\/v1\/vessels\/([^/]+)\/http(?:\/|$)/);
  if (vesselHttp) {
    const vessel = requireIdentifier(
      decodeURIComponent(vesselHttp[1]),
      "vessel name",
    );
    return { resourceId: `vessel/${vessel}`, action: "execute" };
  }
  if (method === "GET" || method === "HEAD") {
    return { resourceId: "tenant", action: "discover" };
  }
  if (["PUT", "POST", "PATCH", "DELETE"].includes(method)) {
    return { resourceId: "tenant", action: "deploy" };
  }
  throw new Error("operation is not exposed to agent runs");
}

export function requireScopedToken(value) {
  if (typeof value !== "string" || !value) {
    throw new Error(
      "A caller-minted scoped token is required for each agent run.",
    );
  }
  return value;
}

export function gatewayTargetToken(
  service,
  allowed,
  scopedToken,
  containerRuntimeToken,
) {
  if (!allowed) throw new Error("permission denied");
  if (service === "runtime") return containerRuntimeToken;
  return requireScopedToken(scopedToken);
}

export function authorizationDecision(response) {
  return response?.decision ?? response;
}

export function boundedPrompt(value) {
  if (typeof value !== "string" || !value.trim())
    throw new Error("A prompt is required.");
  if (value.length > 256 * 1024) throw new Error("The prompt exceeds 256 KiB.");
  return value.trim();
}

export function bearerAuthorized(request, token) {
  return request.headers.get("authorization") === `Bearer ${token}`;
}

export function safeJson(value) {
  return JSON.parse(JSON.stringify(value));
}
