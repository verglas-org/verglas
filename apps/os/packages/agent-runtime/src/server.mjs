import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import { AgentStore } from "./store.mjs";
import {
  authorizationDecision,
  bearerAuthorized,
  boundedPrompt,
  gatewayTargetToken,
  requireIdentifier,
  requireScopedToken,
  runCapabilityEnvironment,
  runDeploymentId,
  safeJson,
} from "./contracts.mjs";
import { runAgent } from "./runner.mjs";

const mode = process.argv[2] ?? "serve";

if (mode === "run") {
  const runId = requireIdentifier(process.argv[3], "run id");
  await runAgent(runId);
  process.exit(0);
}

const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) throw new Error("DATABASE_URL is required.");

const token = process.env.VERGLAS_AGENT_RUNTIME_TOKEN;
const containerRuntimeUrl = process.env.VERGLAS_CONTAINER_RUNTIME_URL;
const containerRuntimeToken = process.env.VERGLAS_CONTAINER_RUNTIME_TOKEN;
const agentImage =
  process.env.VERGLAS_AGENT_RUNNER_IMAGE ||
  "verglas/verglas-agent-runtime:local";
if (
  !token ||
  !containerRuntimeUrl ||
  !containerRuntimeToken ||
  !process.env.VERGLAS_DATA_ENDPOINT ||
  !process.env.VERGLAS_ACCESS_URI
) {
  throw new Error(
    "Agent, container, data, and access runtime configuration is required.",
  );
}

const store = new AgentStore(databaseUrl);
await store.migrate();

async function cleanupRuns() {
  const active = new Set((await store.listActiveRuns()).map((run) => run.id));
  const runs = [
    ...(await store.listRunsForCleanup()),
    ...[...active].map((id) => ({ id })),
  ];
  for (const run of runs) {
    const deploymentId = runDeploymentId(run.id);
    const inspected = await fetch(
      `${containerRuntimeUrl.replace(/\/+$/, "")}/v1/runs/${deploymentId}`,
      { headers: { Authorization: `Bearer ${containerRuntimeToken}` } },
    );
    if (inspected.ok) {
      const status = await inspected.json();
      if (status?.state === "running") continue;
    } else if (inspected.status !== 404) {
      continue;
    }
    if (active.has(run.id)) {
      await store.finishRun(
        run.id,
        "Agent container exited without completing the run.",
      );
    }
    const removed = await fetch(
      `${containerRuntimeUrl.replace(/\/+$/, "")}/v1/runs/${deploymentId}`,
      {
        method: "DELETE",
        headers: { Authorization: `Bearer ${containerRuntimeToken}` },
      },
    );
    if (removed.ok || removed.status === 404)
      await store.markRunCleaned(run.id);
  }
}
setInterval(() => cleanupRuns().catch(() => {}), 2_000);

function response(value, status = 200) {
  return Response.json(safeJson(value), { status });
}

async function body(request) {
  const length = Number(request.headers.get("content-length") || 0);
  if (length > 2 * 1024 * 1024) throw new Error("Request exceeds 2 MiB.");
  return await request.json();
}

async function startRun(workspaceId, chatId, principalId, scopedToken) {
  const runId = randomUUID().replaceAll("-", "");
  scopedToken = requireScopedToken(scopedToken);
  await store.createRun({
    id: runId,
    workspaceId,
    chatId,
    principalId,
    tokenHash: createHash("sha256").update(scopedToken).digest("hex"),
  });
  const deploymentId = runDeploymentId(runId);
  const specification = {
    deployment_id: deploymentId,
    image: agentImage,
    command: ["run", runId],
    environment: runCapabilityEnvironment({
      runId,
      scopedToken,
      principalId,
      chatId,
      modelUrl: process.env.LOCAL_MODEL_RUNTIME_URL,
      modelToken: process.env.LOCAL_MODEL_RUNTIME_TOKEN,
    }),
    bind_mounts: [],
    network: process.env.VERGLAS_RUNTIME_NETWORK || "verglas-runtime",
    published_ports: [],
  };
  const placed = await fetch(
    `${containerRuntimeUrl.replace(/\/+$/, "")}/v1/runs/${deploymentId}`,
    {
      method: "PUT",
      headers: {
        Authorization: `Bearer ${containerRuntimeToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(specification),
    },
  );
  if (!placed.ok) {
    const error = await placed.text();
    await store.finishRun(runId, error);
    throw new Error(
      `Failed to place agent run: HTTP ${placed.status} — ${error}`,
    );
  }
  return runId;
}

function tokensEqual(left, rightHash) {
  const actual = createHash("sha256").update(left).digest();
  const expected = Buffer.from(rightHash || "", "hex");
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

async function checkRunAccess(scopedToken, resourceId, action) {
  const accessResponse = await fetch(
    `${process.env.VERGLAS_ACCESS_URI}/v1/access/authorize`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${scopedToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        audience: "data-plane",
        resource_id: resourceId,
        action,
      }),
    },
  );
  if (!accessResponse.ok) {
    throw new Error(
      `Authorization check failed: HTTP ${accessResponse.status}`,
    );
  }
  return authorizationDecision(await accessResponse.json());
}

async function proxyRunGateway(request, url, match) {
  const run = await store.getGatewayRun(match[1]);
  const supplied =
    request.headers.get("authorization")?.replace(/^Bearer\s+/i, "") || "";
  if (!run || !tokensEqual(supplied, run.token_hash)) {
    return response({ error: "unauthorized run credential" }, 401);
  }
  const service = match[2];
  const suffix = `/${match[3]}`;
  if (service === "control") {
    if (request.method === "POST" && suffix === "/claim") {
      const claimed = await store.claimRun(run.id);
      return claimed
        ? response(claimed)
        : response({ error: "run is not pending" }, 409);
    }
    if (run.state !== "running")
      return response({ error: "run is not active" }, 409);
    if (request.method === "GET" && suffix === "/history") {
      return response(
        await store.historyForModel(run.workspace_id, Number(run.chat_id)),
      );
    }
    if (request.method === "POST" && suffix === "/messages") {
      const input = await body(request);
      await store.appendAssistantMessage(
        run.workspace_id,
        Number(run.chat_id),
        input.author,
        input.body,
      );
      return new Response(null, { status: 204 });
    }
    if (request.method === "POST" && suffix === "/finish") {
      const input = await body(request);
      await store.finishRun(run.id, input.error || null);
      return new Response(null, { status: 204 });
    }
    return response({ error: "unknown agent control operation" }, 404);
  }
  if (run.state !== "running")
    return response({ error: "run is not active" }, 409);
  if (service === "access") {
    if (suffix === "/v1/access/authorize" && request.method === "POST") {
      const input = await request.clone().json();
      if (input.audience !== "data-plane") {
        return response(
          { error: "run cannot select another authorization audience" },
          403,
        );
      }
      return response(
        await checkRunAccess(supplied, input.resource_id, input.action),
      );
    }
    if (request.method !== "GET" || suffix !== "/v1/databases") {
      return response({ error: "operation is not exposed to agent runs" }, 403);
    }
  }

  let action;
  let resourceId = "tenant";
  const catalogMatch = suffix.match(/^\/v1\/databases\/([^/]+)\/catalog\//);
  const queryMatch = suffix.match(/^\/v1\/databases\/([^/]+)\/query$/);
  if (service === "data" && request.method === "GET" && catalogMatch) {
    action = "describe";
    resourceId = `database/${decodeURIComponent(catalogMatch[1])}`;
  } else if (
    service === "access" &&
    request.method === "GET" &&
    suffix === "/v1/databases"
  ) {
    action = "discover";
  } else if (service === "data" && request.method === "POST" && queryMatch) {
    action = "query";
    resourceId = `database/${decodeURIComponent(queryMatch[1])}`;
  } else if (
    service === "data" &&
    request.method === "POST" &&
    suffix === "/v1/workers"
  ) {
    action = "deploy";
  } else if (
    service === "runtime" &&
    request.method === "PUT" &&
    suffix.startsWith("/v1/vessels/")
  ) {
    action = "deploy";
  } else {
    return response({ error: "operation is not exposed to agent runs" }, 403);
  }
  const decision = await checkRunAccess(supplied, resourceId, action);
  let targetToken;
  try {
    targetToken = gatewayTargetToken(
      service,
      decision.allowed,
      supplied,
      containerRuntimeToken,
    );
  } catch {
    return response(
      { error: `permission denied: ${action} on ${resourceId}` },
      403,
    );
  }

  const targetBase =
    service === "data"
      ? process.env.VERGLAS_DATA_ENDPOINT
      : service === "access"
        ? process.env.VERGLAS_ACCESS_URI
        : containerRuntimeUrl;
  const target = new URL(suffix, targetBase.replace(/\/+$/, "") + "/");
  target.search = url.search;
  const headers = new Headers(request.headers);
  headers.set("Authorization", `Bearer ${targetToken}`);
  headers.delete("host");
  const proxied = await fetch(target, {
    method: request.method,
    headers,
    body:
      request.method === "GET" || request.method === "HEAD"
        ? undefined
        : request.body,
  });
  return new Response(proxied.body, {
    status: proxied.status,
    headers: proxied.headers,
  });
}

async function handler(request) {
  const url = new URL(request.url);
  if (request.method === "GET" && url.pathname === "/healthz") {
    return new Response(null, { status: 204 });
  }
  const runGatewayMatch = url.pathname.match(
    /^\/v1\/run-gateway\/([a-f0-9]+)\/(data|runtime|access|control)\/(.+)$/,
  );
  if (runGatewayMatch)
    return await proxyRunGateway(request, url, runGatewayMatch);
  if (!bearerAuthorized(request, token))
    return response({ error: "unauthorized" }, 401);

  const workspaceMatch = url.pathname.match(/^\/v1\/workspaces\/([^/]+)$/);
  if (workspaceMatch) {
    const id = requireIdentifier(
      decodeURIComponent(workspaceMatch[1]),
      "workspace id",
    );
    if (request.method === "PUT") {
      const input = await body(request);
      return response(
        await store.createWorkspace({
          id,
          tenantId: requireIdentifier(input.tenantId, "tenant id"),
          ownerId: String(input.ownerId),
          title: String(input.title || "Untitled Workspace"),
        }),
        201,
      );
    }
    const ownerId = url.searchParams.get("ownerId");
    if (!ownerId) return response({ error: "ownerId is required" }, 400);
    if (request.method === "GET") {
      const workspace = await store.getWorkspace(id, ownerId);
      return workspace
        ? response(workspace)
        : response({ error: "workspace not found" }, 404);
    }
    if (request.method === "PATCH") {
      const workspace = await store.updateWorkspace(
        id,
        ownerId,
        await body(request),
      );
      return workspace
        ? response(workspace)
        : response({ error: "workspace not found" }, 404);
    }
    if (request.method === "DELETE") {
      await store.deleteWorkspace(id, ownerId);
      return new Response(null, { status: 204 });
    }
  }

  const chatsMatch = url.pathname.match(/^\/v1\/workspaces\/([^/]+)\/chats$/);
  if (chatsMatch) {
    const workspaceId = requireIdentifier(
      decodeURIComponent(chatsMatch[1]),
      "workspace id",
    );
    if (request.method === "GET")
      return response(await store.listChats(workspaceId));
    if (request.method === "POST") {
      const input = await body(request);
      const prompt = boundedPrompt(input.prompt);
      const chatId = await store.createChat({
        workspaceId,
        profile: input.profile,
        modelProfile: input.modelProfile,
        modelConfig: input.modelConfig,
        prompt,
      });
      if (input.modelConfig)
        await startRun(
          workspaceId,
          chatId,
          input.principalId,
          input.scopedToken,
        );
      return response({ chatId }, 201);
    }
  }

  const chatMatch = url.pathname.match(
    /^\/v1\/workspaces\/([^/]+)\/chats\/(\d+)$/,
  );
  if (chatMatch) {
    const workspaceId = requireIdentifier(
      decodeURIComponent(chatMatch[1]),
      "workspace id",
    );
    const chatId = Number(chatMatch[2]);
    if (request.method === "DELETE") {
      await store.deleteChat(workspaceId, chatId);
      return new Response(null, { status: 204 });
    }
    if (request.method === "PATCH") {
      const input = await body(request);
      await store.setChatTitle(workspaceId, chatId, String(input.title));
      return new Response(null, { status: 204 });
    }
  }

  const messagesMatch = url.pathname.match(
    /^\/v1\/workspaces\/([^/]+)\/chats\/(\d+)\/messages$/,
  );
  if (messagesMatch) {
    const workspaceId = requireIdentifier(
      decodeURIComponent(messagesMatch[1]),
      "workspace id",
    );
    const chatId = Number(messagesMatch[2]);
    if (request.method === "GET") {
      return response(
        await store.listMessages(
          workspaceId,
          chatId,
          Number(url.searchParams.get("afterSequence") ?? -1),
        ),
      );
    }
    if (request.method === "POST") {
      const input = await body(request);
      await store.appendUserMessage({
        workspaceId,
        chatId,
        profile: input.profile,
        modelProfile: input.modelProfile,
        modelConfig: input.modelConfig,
        prompt: boundedPrompt(input.prompt),
      });
      if (input.modelConfig)
        await startRun(
          workspaceId,
          chatId,
          input.principalId,
          input.scopedToken,
        );
      return new Response(null, { status: 202 });
    }
  }

  const permissionMatch = url.pathname.match(
    /^\/v1\/workspaces\/([^/]+)\/permission-requests\/([^/]+)$/,
  );
  if (permissionMatch && request.method === "PATCH") {
    const workspaceId = requireIdentifier(
      decodeURIComponent(permissionMatch[1]),
      "workspace id",
    );
    const requestId = decodeURIComponent(permissionMatch[2]);
    const input = await body(request);
    if (input.state !== "approved" && input.state !== "denied") {
      return response({ error: "state must be approved or denied" }, 400);
    }
    const existing = await store.getPermissionRequest(workspaceId, requestId);
    if (!existing)
      return response({ error: "permission request not found" }, 404);
    if (existing.body.state !== "pending") {
      return response({ error: "permission request is not pending" }, 409);
    }
    const decided = await store.decidePermissionRequest(
      workspaceId,
      requestId,
      input.state,
    );
    if (!decided)
      return response(
        { error: "permission request changed concurrently" },
        409,
      );
    if (input.state === "approved") {
      await startRun(
        workspaceId,
        Number(decided.chat_id),
        decided.body.principalId,
        input.scopedToken,
      );
    }
    return response(decided);
  }

  const stopMatch = url.pathname.match(
    /^\/v1\/workspaces\/([^/]+)\/chats\/(\d+)\/stop$/,
  );
  if (stopMatch && request.method === "POST") {
    const workspaceId = requireIdentifier(
      decodeURIComponent(stopMatch[1]),
      "workspace id",
    );
    const chatId = Number(stopMatch[2]);
    const runIds = await store.cancelActiveRun(workspaceId, chatId);
    await Promise.all(
      runIds.map((runId) =>
        fetch(
          `${containerRuntimeUrl.replace(/\/+$/, "")}/v1/runs/${runDeploymentId(runId)}`,
          {
            method: "DELETE",
            headers: { Authorization: `Bearer ${containerRuntimeToken}` },
          },
        ),
      ),
    );
    return new Response(null, { status: 204 });
  }

  const retryMatch = url.pathname.match(
    /^\/v1\/workspaces\/([^/]+)\/chats\/(\d+)\/retry$/,
  );
  if (retryMatch && request.method === "POST") {
    const workspaceId = requireIdentifier(
      decodeURIComponent(retryMatch[1]),
      "workspace id",
    );
    const chatId = Number(retryMatch[2]);
    const input = await body(request);
    if (!input.modelConfig)
      return response({ error: "model configuration is required" }, 400);
    if (
      !(await store.setChatModel(
        workspaceId,
        chatId,
        input.modelProfile,
        input.modelConfig,
      ))
    ) {
      return response({ error: "chat not found" }, 404);
    }
    await startRun(workspaceId, chatId, input.principalId, input.scopedToken);
    return new Response(null, { status: 202 });
  }

  return response({ error: "not found" }, 404);
}

Bun.serve({
  hostname: process.env.VERGLAS_AGENT_RUNTIME_HOST || "0.0.0.0",
  port: Number(process.env.VERGLAS_AGENT_RUNTIME_PORT || 8390),
  fetch(request) {
    return handler(request).catch((error) =>
      response(
        {
          error: error instanceof Error ? error.message : String(error),
        },
        500,
      ),
    );
  },
});
