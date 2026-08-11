import { createToolExecutor, toolDefinitions } from "./tools.mjs";

const SYSTEM_PROMPT = `You are the Verglas data-lakehouse builder. You build complete, working
data workflows from Integration API containers, Verglas worker Jobs, and Application Vessel
interfaces. Inspect existing data first. Keep durable data in Verglas. Applications and
Integrations must be standalone projects with a package.json start script and all source files.
Vessel servers run on Bun: use \`bun src/index.ts\` directly, never tsx, and listen on
\`process.env.PORT || 8380\`. Browser code must use relative API URLs such as \`./api/status\`
because Applications are mounted below \`/apps/<name>/\`.
Never invent successful live data: expose configuration requirements clearly and verify APIs.
Use the available tools to perform the work; do not merely describe code the user should write.
Your process has its own Verglas principal. If a tool reports missing access, call
requestPermission with the exact resource and actions. Never ask for broader access than the task
requires, and stop the turn after requesting it.`;

function modelMessages(rows) {
  return rows
    .flatMap(row => {
      if (row.body?.type === "message" && row.body.message) {
        return [{
          role: row.author?.type === "agent" ? "assistant" : "user",
          content: row.body.message,
        }];
      }
      if (row.body?.type === "permissionRequest") {
        return [{
          role: "user",
          content: `Permission request ${row.body.requestId} for ${row.body.actions.join(", ")} ` +
            `on ${row.body.resourceId} is ${row.body.state}.`,
        }];
      }
      return [];
    });
}

async function invokeLocalModel(config, messages) {
  const endpoint = process.env.LOCAL_MODEL_RUNTIME_URL;
  const token = process.env.LOCAL_MODEL_RUNTIME_TOKEN;
  if (!endpoint || !token) throw new Error("The model-runtime endpoint is not configured.");
  if (config.provider !== "local-runtime" || !config.runtime) {
    throw new Error(`Agent microVM does not yet support direct provider ${config.provider}.`);
  }
  const response = await fetch(`${endpoint.replace(/\/+$/, "")}/v1/chat/completions`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      "x-model-runtime": config.runtime,
      ...(config.apiToken ? { "x-provider-api-key": config.apiToken } : {}),
    },
    body: JSON.stringify({ model: config.model, messages, tools: toolDefinitions }),
    signal: AbortSignal.timeout(15 * 60_000),
  });
  const body = await response.json();
  if (!response.ok) throw new Error(body.error || `Model runtime failed: HTTP ${response.status}`);
  const message = body.choices?.[0]?.message;
  if (!message) throw new Error("Model runtime returned no assistant message.");
  return message;
}

export async function runAgent(runId) {
  const controller = process.env.VERGLAS_AGENT_CONTROLLER_URL;
  const controllerToken = process.env.VERGLAS_TOKEN;
  if (!controller || !controllerToken) throw new Error("Agent controller capability is missing.");
  const request = async (path, options = {}) => {
    const response = await fetch(`${controller.replace(/\/+$/, "")}${path}`, {
      ...options,
      headers: {
        Authorization: `Bearer ${controllerToken}`,
        ...(options.body === undefined ? {} : {"Content-Type": "application/json"}),
      },
    });
    const text = await response.text();
    if (!response.ok) throw new Error(`Agent controller ${path} failed: ${response.status} ${text}`);
    return text ? JSON.parse(text) : null;
  };
  const store = {
    claimRun: () => request("/claim", {method: "POST"}),
    historyForModel: () => request("/history"),
    appendAssistantMessage: (_workspaceId, _chatId, author, body) =>
      request("/messages", {method: "POST", body: JSON.stringify({author, body})}),
    finishRun: (_id, error = null) =>
      request("/finish", {method: "POST", body: JSON.stringify({error})}),
  };
  const run = await store.claimRun(runId);
  if (!run) throw new Error(`Agent run ${runId} is not pending.`);
  const author = run.model_profile;
  const history = await store.historyForModel(run.workspace_id, Number(run.chat_id));
  const messages = [{ role: "system", content: SYSTEM_PROMPT }, ...modelMessages(history)];
  const emit = body => store.appendAssistantMessage(
    run.workspace_id, Number(run.chat_id), author, body,
  );
  const execute = createToolExecutor(process.env, emit);

  try {
    for (let step = 0; step < 30; step++) {
      const assistant = await invokeLocalModel(run.model_config, messages);
      const calls = Array.isArray(assistant.tool_calls) ? assistant.tool_calls : [];
      if (typeof assistant.content === "string" && assistant.content.trim()) {
        await emit({ type: "message", message: assistant.content.trim() });
      }
      if (calls.length === 0) {
        await store.finishRun(runId);
        return;
      }
      messages.push({ role: "assistant", content: assistant.content ?? null, tool_calls: calls });
      for (const call of calls) {
        const name = call.function?.name;
        let args;
        try {
          args = JSON.parse(call.function?.arguments || "{}");
          const result = await execute(name, args);
          messages.push({ role: "tool", tool_call_id: call.id, content: JSON.stringify(result) });
          if (result?.permissionRequested) {
            await store.finishRun(runId);
            return;
          }
        } catch (error) {
          messages.push({
            role: "tool",
            tool_call_id: call.id,
            content: JSON.stringify({ error: error instanceof Error ? error.message : String(error) }),
          });
        }
      }
    }
    throw new Error("Agent exceeded the 30-step turn limit.");
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    try {
      await emit({ type: "message", message: `Agent run failed: ${message}` });
    } finally {
      await store.finishRun(runId, message);
    }
    throw error;
  }
}
