const identifierPattern = /^[a-zA-Z0-9_.-]+$/;

export function requireIdentifier(value, label) {
  if (typeof value !== "string" || !identifierPattern.test(value)) {
    throw new Error(`${label} must contain only letters, numbers, dots, underscores, and dashes.`);
  }
  return value;
}

export function runDeploymentId(runId) {
  return `run-agent-${requireIdentifier(runId, "run id")}`;
}

export function runCapabilityEnvironment({runId, runToken, tenantId, principalId, chatId,
  modelUrl, modelToken}) {
  const gateway = `http://verglas-agent-runtime:8390/v1/run-gateway/${runId}`;
  return {
    VERGLAS_AGENT_CONTROLLER_URL: `${gateway}/control`,
    VERGLAS_AGENT_CONTROLLER_TOKEN: runToken,
    VERGLAS_DATA_ENDPOINT: `${gateway}/data`,
    VERGLAS_DATA_TOKEN: runToken,
    VERGLAS_CONTAINER_RUNTIME_URL: `${gateway}/runtime`,
    VERGLAS_CONTAINER_RUNTIME_TOKEN: runToken,
    LOCAL_MODEL_RUNTIME_URL: modelUrl,
    LOCAL_MODEL_RUNTIME_TOKEN: modelToken,
    VERGLAS_ACCESS_URI: `${gateway}/access`,
    VERGLAS_ACCESS_SERVICE_TOKEN: runToken,
    VERGLAS_TENANT_ID: tenantId,
    VERGLAS_AGENT_PRINCIPAL_ID: principalId,
    VERGLAS_AGENT_CHAT_ID: String(chatId),
  };
}

export function boundedPrompt(value) {
  if (typeof value !== "string" || !value.trim()) throw new Error("A prompt is required.");
  if (value.length > 256 * 1024) throw new Error("The prompt exceeds 256 KiB.");
  return value.trim();
}

export function bearerAuthorized(request, token) {
  return request.headers.get("authorization") === `Bearer ${token}`;
}

export function safeJson(value) {
  return JSON.parse(JSON.stringify(value));
}
