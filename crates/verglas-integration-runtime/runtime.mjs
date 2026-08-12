import {connect} from "./sdk/client.ts";
import {describeIntegration, invokeIntegration} from "./contract.mjs";

const listenPort = Number.parseInt(process.env.VERGLAS_INTEGRATION_PORT || "8370", 10);
const name = requiredEnv("VERGLAS_INTEGRATION_NAME");
const definition = process.env.VERGLAS_INTEGRATION_DEFINITION_JSON
  ? JSON.parse(process.env.VERGLAS_INTEGRATION_DEFINITION_JSON)
  : JSON.parse(decode("VERGLAS_INTEGRATION_DEFINITION"));
const dataEndpoint = requiredEnv("VERGLAS_DATA_ENDPOINT").replace(/\/+$/, "");
const dataToken = requiredEnv("VERGLAS_DATA_TOKEN");
const verglas = connect({endpoint: dataEndpoint, token: dataToken});
const namespace = `integration.${name}`;
const configKey = "configuration";
const entrypoint = process.env.VERGLAS_INTEGRATION_ENTRYPOINT?.trim();
const moduleSource = entrypoint ? null : decode("VERGLAS_INTEGRATION_MODULE");
for (const key of [
  "VERGLAS_DATA_TOKEN",
  "VERGLAS_INTEGRATION_MODULE",
  "VERGLAS_INTEGRATION_DEFINITION",
  "VERGLAS_INTEGRATION_DEFINITION_JSON",
]) {
  delete process.env[key];
}
const generated = entrypoint
  ? await import(entrypoint)
  : await import(`data:text/javascript;base64,${Buffer.from(moduleSource).toString("base64")}`);
delete process.env.VERGLAS_INTEGRATION_ENTRYPOINT;
const integration = generated.default;

if (!integration || typeof integration !== "object" || typeof integration.verify !== "function") {
  throw new Error("generated Integration must default-export an object with verify(ctx)");
}
if (!integration.api) throw new Error("generated Integration must declare api");
const manifest = describeIntegration(integration, name);

let configured = false;
let verification = null;
let running = false;
let abortController = null;

function requiredEnv(key) {
  const value = process.env[key]?.trim();
  if (!value) throw new Error(`${key} is required`);
  return value;
}

function decode(key) {
  return Buffer.from(requiredEnv(key), "base64").toString("utf8");
}

function json(value, status = 200) {
  return Response.json(value, {status});
}

function errorText(error) {
  return error instanceof Error ? error.message : String(error);
}

function safeDetails(details) {
  if (!details || typeof details !== "object" || Array.isArray(details)) return undefined;
  return Object.fromEntries(Object.entries(details).slice(0, 20).flatMap(([key, value]) => {
    if (typeof value === "string") return [[key.slice(0, 80), value.slice(0, 500)]];
    if (typeof value === "number" || typeof value === "boolean" || value === null) {
      return [[key.slice(0, 80), value]];
    }
    return [];
  }));
}

async function kvRequest(method, key, body) {
  return fetch(`${dataEndpoint}/v1/kv/${encodeURIComponent(namespace)}/${encodeURIComponent(key)}`, {
    method,
    headers: {authorization: `Bearer ${dataToken}`, "content-type": "application/json"},
    body,
  });
}

async function loadConfig() {
  const response = await kvRequest("GET", configKey);
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`Verglas KV read failed: HTTP ${response.status}`);
  return await response.json();
}

async function saveConfig(config) {
  const response = await kvRequest("PUT", configKey, JSON.stringify(config));
  if (!response.ok) throw new Error(`Verglas KV write failed: HTTP ${response.status}`);
}

function validateConfig(values) {
  const allowed = new Set(definition.fields.map((field) => field.name));
  for (const key of Object.keys(values)) {
    if (!allowed.has(key)) throw new Error(`unknown configuration field ${key}`);
  }
  const config = {};
  for (const field of definition.fields) {
    const value = values[field.name] ?? field.defaultValue ?? "";
    if (field.required && String(value).trim().length === 0) {
      throw new Error(`${field.label} is required`);
    }
    config[field.name] = String(value);
  }
  return config;
}

function context(config, signal) {
  return Object.freeze({
    name,
    verglas,
    config: Object.freeze({...config}),
    signal,
    async emit(event) {
      const envelope = {
        specversion: "1.0",
        id: event.id || crypto.randomUUID(),
        source: event.source || `urn:verglas:integration:${name}`,
        type: event.type,
        time: event.time || new Date().toISOString(),
        ...(event.subject ? {subject: event.subject} : {}),
        data: event.data,
      };
      if (!envelope.type) throw new Error("CloudEvent type is required");
      const response = await fetch(`${dataEndpoint}/v1/events`, {
        method: "POST",
        headers: {authorization: `Bearer ${dataToken}`, "content-type": "application/cloudevents+json"},
        body: JSON.stringify(envelope),
      });
      if (!response.ok) throw new Error(`Verglas event ingress failed: HTTP ${response.status}`);
      return await response.json();
    },
    async enqueue(queue, rows) {
      const response = await fetch(`${dataEndpoint}/v1/queues/${encodeURIComponent(queue)}/enqueue`, {
        method: "POST",
        headers: {authorization: `Bearer ${dataToken}`, "content-type": "application/json"},
        body: JSON.stringify({messages: rows}),
      });
      if (!response.ok) throw new Error(`Verglas queue enqueue failed: HTTP ${response.status}`);
      return await response.json();
    },
  });
}

async function verify(config) {
  const startedAt = Date.now();
  try {
    const ctx = context(config);
    const result = await integration.verify.call(ctx, ctx);
    if (result?.ok === false) throw new Error(result.message || "Integration verification failed");
    verification = {
      ok: true,
      message: result?.message || "Connection verified",
      details: safeDetails(result?.details),
      testedAt: new Date().toISOString(),
      latencyMs: Date.now() - startedAt,
    };
    return verification;
  } catch (error) {
    verification = {
      ok: false,
      message: errorText(error),
      testedAt: new Date().toISOString(),
      latencyMs: Date.now() - startedAt,
    };
    throw error;
  }
}

async function start(config) {
  if (typeof integration.start !== "function") return;
  abortController?.abort();
  abortController = new AbortController();
  running = true;
  const ctx = context(config, abortController.signal);
  integration.start.call(ctx, ctx).catch((error) => {
    running = false;
    verification = {...verification, ok: false, message: errorText(error)};
    console.error("integration background task failed", error);
  });
}

async function invoke(request, method) {
  const config = await loadConfig();
  if (!config) return json({error: "Integration is not configured"}, 409);
  const text = await request.text();
  const input = text ? JSON.parse(text) : undefined;
  const ctx = context(config);
  try {
    const result = await invokeIntegration(integration, method, input, ctx);
    if (manifest.methods[method]?.mode === "stream") {
      if (!result || typeof result[Symbol.asyncIterator] !== "function") {
        throw new Error(`stream method ${method} must return an AsyncIterable`);
      }
      const iterator = result[Symbol.asyncIterator]();
      const stream = new ReadableStream({
        async pull(controller) {
          try {
            const next = await iterator.next();
            if (next.done) return controller.close();
            controller.enqueue(new TextEncoder().encode(`${JSON.stringify(next.value)}\n`));
          } catch (error) {
            controller.error(error);
          }
        },
        async cancel() {
          await iterator.return?.();
        },
      });
      return new Response(stream, {headers: {"content-type": "application/x-ndjson"}});
    }
    return json(result ?? null);
  } catch (error) {
    return json({error: errorText(error)}, 502);
  }
}

async function configure(request) {
  const config = validateConfig(await request.json());
  await saveConfig(config);
  configured = true;
  try {
    const result = await verify(config);
    await start(config);
    return json({configured: result.ok, verification: result});
  } catch (error) {
    return json({configured: false, verification, error: errorText(error)}, 422);
  }
}

async function test() {
  const config = await loadConfig();
  if (!config) return json({configured: false, error: "Integration is not configured"}, 409);
  configured = true;
  try {
    const result = await verify(config);
    await start(config);
    return json({configured: result.ok, verification: result});
  } catch (error) {
    return json({configured: false, verification, error: errorText(error)}, 422);
  }
}

async function route(request) {
  const url = new URL(request.url);
  if (url.pathname === "/health") return json({ok: true});
  if (url.pathname === "/v1/namespace" && request.method === "GET") return json(manifest);
  if (url.pathname.startsWith("/v1/namespace/invoke/") && request.method === "POST") {
    return await invoke(request, decodeURIComponent(url.pathname.slice("/v1/namespace/invoke/".length)));
  }
  if (url.pathname === "/v1/config/schema" && request.method === "GET") return json(definition);
  if (url.pathname === "/v1/config" && request.method === "GET") {
    return json({configured: configured && verification?.ok === true, verification, running});
  }
  if (url.pathname === "/v1/config" && request.method === "PUT") return await configure(request);
  if (url.pathname === "/v1/test" && request.method === "POST") return await test();
  if (url.pathname === "/v1/status" && request.method === "GET") {
    return json({configured: configured && verification?.ok === true, verification, running});
  }
  if (url.pathname.startsWith("/v1/api/") && typeof integration.fetch === "function") {
    const config = await loadConfig();
    if (!config) return json({error: "Integration is not configured"}, 409);
    return await integration.fetch(request, context(config));
  }
  return json({error: "not found"}, 404);
}

Bun.serve({port: listenPort, fetch: route});

loadConfig().then(async (config) => {
  if (!config) return;
  configured = true;
  await verify(config);
  await start(config);
}).catch((error) => {
  verification = {ok: false, message: errorText(error), testedAt: new Date().toISOString()};
  console.error("integration restore failed", error);
});
