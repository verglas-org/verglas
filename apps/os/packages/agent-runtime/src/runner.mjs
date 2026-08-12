import { Agent } from "@earendil-works/pi-agent-core";
import { streamSimple as streamPiMessages } from "@earendil-works/pi-ai/api/pi-messages";
import { createToolExecutor, toolDefinitions } from "./tools.mjs";

const SYSTEM_PROMPT = `You are a Verglas agent running inside an isolated Bun workspace. Start by
using describeEnvironment when you need to inspect your capabilities. You can create files and run
programs in the workspace, fetch HTTPS resources, and use the installed @verglas/sdk package to
work with Verglas data, workers, access control, and Vessels. Prefer small TypeScript programs that
call the SDK over manually constructing HTTP requests. Endpoint variable names and your scoped token
are available in the environment; never print, persist, or disclose credential values.

Use tools to perform the requested work and verify the result instead of merely describing commands
for the user to run. Keep durable data in Verglas. Applications and Integrations must be standalone
projects with a package.json start script and all source files. Vessel servers run on Bun: use
\`bun src/index.ts\` directly and listen on \`process.env.PORT\`. Browser code must use relative API
URLs because Applications are mounted below \`/apps/<name>/\`. Never fabricate live data or claim
success without checking it.

Your process has its own Verglas principal and the API enforces its scoped grants. When an SDK or API
operation is denied, call requestPermission with the exact resource and minimum required actions,
then stop the turn. Do not request broader access than the task requires.`;

const ZERO_COST = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 };
const ZERO_USAGE = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { ...ZERO_COST, total: 0 },
};

function timestamp(value) {
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : Date.now();
}

export function localRuntimeModel(config, endpoint) {
  if (config.provider !== "local-runtime" || !config.runtime) {
    throw new Error(
      `Agent runtime requires a linked native model runtime, not ${config.provider}.`,
    );
  }
  if (!endpoint) throw new Error("The native model runtime is not configured.");
  const base = endpoint.replace(/\/+$/, "").replace(/\/v1$/, "");
  const providers = {
    codex: "openai-codex",
    "claude-code": "anthropic",
    "github-copilot": "github-copilot",
  };
  const provider = providers[config.runtime];
  if (!provider) {
    throw new Error(`Unsupported Pi subscription provider ${config.runtime}.`);
  }
  return {
    id: config.model,
    name: config.model,
    api: "pi-messages",
    provider,
    baseUrl: base,
    reasoning: true,
    input: ["text"],
    cost: ZERO_COST,
    contextWindow: 128_000,
    maxTokens: 16_384,
  };
}

export function messagesForPi(rows, model) {
  return rows.flatMap((row) => {
    if (row.body?.type === "message" && row.body.message) {
      if (row.author?.type !== "agent") {
        return [
          {
            role: "user",
            content: row.body.message,
            timestamp: timestamp(row.timestamp),
          },
        ];
      }
      return [
        {
          role: "assistant",
          content: [{ type: "text", text: row.body.message }],
          api: model.api,
          provider: model.provider,
          model: model.id,
          usage: ZERO_USAGE,
          stopReason: "stop",
          timestamp: timestamp(row.timestamp),
        },
      ];
    }
    if (
      row.body?.type === "permissionRequest" &&
      row.body.state !== "pending"
    ) {
      return [
        {
          role: "user",
          content:
            `Permission request ${row.body.requestId} for ${row.body.actions.join(", ")} ` +
            `on ${row.body.resourceId} was ${row.body.state}.`,
          timestamp: timestamp(row.timestamp),
        },
      ];
    }
    return [];
  });
}

export function createPiTools(execute) {
  return toolDefinitions.map((definition) => {
    const tool = definition.function;
    return {
      name: tool.name,
      label: tool.name,
      description: tool.description,
      parameters: tool.parameters,
      executionMode: "sequential",
      execute: async (_toolCallId, args) => {
        const result = await execute(tool.name, args);
        return {
          content: [{ type: "text", text: JSON.stringify(result) }],
          details: result,
          ...(result?.permissionRequested ? { terminate: true } : {}),
        };
      },
    };
  });
}

function textFromAssistant(message) {
  if (message?.role !== "assistant") return "";
  return message.content
    .filter((part) => part.type === "text")
    .map((part) => part.text)
    .join("")
    .trim();
}

function streamNativeRuntime(config, sessionId, credentialScope) {
  const token = process.env.LOCAL_MODEL_RUNTIME_TOKEN;
  if (!token)
    throw new Error("The native model runtime token is not configured.");
  return (model, context, options = {}) =>
    streamPiMessages(model, context, {
      ...options,
      apiKey: token,
      sessionId,
      headers: {
        ...options.headers,
        "x-model-runtime": config.runtime,
        "x-verglas-credential-scope": credentialScope,
      },
      timeoutMs: 15 * 60_000,
    });
}

export async function runAgent(runId) {
  const controller = process.env.VERGLAS_AGENT_CONTROLLER_URL;
  const controllerToken = process.env.VERGLAS_TOKEN;
  if (!controller || !controllerToken)
    throw new Error("Agent controller capability is missing.");
  const request = async (path, options = {}) => {
    const response = await fetch(`${controller.replace(/\/+$/, "")}${path}`, {
      ...options,
      headers: {
        Authorization: `Bearer ${controllerToken}`,
        ...(options.body === undefined
          ? {}
          : { "Content-Type": "application/json" }),
      },
    });
    const text = await response.text();
    if (!response.ok)
      throw new Error(
        `Agent controller ${path} failed: ${response.status} ${text}`,
      );
    return text ? JSON.parse(text) : null;
  };
  const store = {
    claimRun: () => request("/claim", { method: "POST" }),
    historyForModel: () => request("/history"),
    appendAssistantMessage: (author, body) =>
      request("/messages", {
        method: "POST",
        body: JSON.stringify({ author, body }),
      }),
    finishRun: (error = null) =>
      request("/finish", { method: "POST", body: JSON.stringify({ error }) }),
  };
  const run = await store.claimRun();
  if (!run) throw new Error(`Agent run ${runId} is not pending.`);
  const author = run.model_profile;
  const history = await store.historyForModel();
  const emit = (body) => store.appendAssistantMessage(author, body);
  const execute = createToolExecutor(process.env, emit);
  const model = localRuntimeModel(
    run.model_config,
    process.env.LOCAL_MODEL_RUNTIME_URL,
  );
  const sessionId = `${run.workspace_id}:${run.chat_id}`;
  const agent = new Agent({
    initialState: {
      systemPrompt: SYSTEM_PROMPT,
      model,
      messages: messagesForPi(history, model),
      tools: createPiTools(execute),
    },
    streamFn: streamNativeRuntime(
      run.model_config,
      sessionId,
      run.model_config.credentialScope ?? run.principal_id,
    ),
    sessionId,
    toolExecution: "sequential",
  });
  agent.subscribe(async (event) => {
    if (event.type !== "message_end") return;
    const text = textFromAssistant(event.message);
    if (text) await emit({ type: "message", message: text });
  });

  try {
    await agent.continue();
    const failure = agent.state.messages
      .toReversed()
      .find(
        (message) =>
          message.role === "assistant" &&
          (message.stopReason === "error" || message.stopReason === "aborted"),
      );
    if (failure) {
      throw new Error(failure.errorMessage || "Pi agent inference failed.");
    }
    await store.finishRun();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    try {
      await emit({ type: "message", message: `Agent run failed: ${message}` });
    } finally {
      await store.finishRun(message);
    }
    throw error;
  }
}
