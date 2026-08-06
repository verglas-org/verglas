import {
  RpcStub,
  RpcTarget,
  newBunWebSocketRpcHandler,
  newHttpBatchRpcResponse,
} from "capnweb";
import { pathToFileURL } from "node:url";
import { resolve } from "node:path";

import { makeVerglasEnvironment } from "./verglas-env.mjs";

const bundleRoot = process.argv.at(-1);
if (!bundleRoot) throw new Error("missing Gadget bundle directory");

const gadgetId = requireEnvironment("VERGLAS_GADGET_ID");
const gadgetVersion = requireEnvironment("VERGLAS_GADGET_VERSION");
const capabilityEndpoint = requireEnvironment("VERGLAS_GADGET_CAPABILITY_ENDPOINT")
  .replace(/\/$/, "");
const capabilityToken = requireEnvironment("VERGLAS_GADGET_CAPABILITY_TOKEN");
delete process.env.VERGLAS_GADGET_CAPABILITY_ENDPOINT;
delete process.env.VERGLAS_GADGET_CAPABILITY_TOKEN;
const nativeFetch = globalThis.fetch.bind(globalThis);

globalThis.__verglasCapnWeb = Object.freeze({ RpcStub, RpcTarget });
denyAmbientNetwork();

const moduleUrl = pathToFileURL(resolve(bundleRoot, "server.js"));
moduleUrl.searchParams.set("version", gadgetVersion);
const gadgetModule = await import(moduleUrl.href);
if (typeof gadgetModule.Gadget !== "function") {
  throw new Error("server.js must export a Gadget class");
}

const storage = new VerglasKvStorage(
  nativeFetch,
  capabilityEndpoint,
  capabilityToken,
  `gadget.${gadgetId}`,
);
const context = Object.freeze({
  storage,
  blockConcurrencyWhile(callback) {
    return callback();
  },
  waitUntil(promise) {
    Promise.resolve(promise).catch((error) => {
      console.error("Gadget background task failed", safeError(error));
    });
  },
});
const environment = makeVerglasEnvironment({
  endpoint: capabilityEndpoint,
  token: capabilityToken,
  fetchImpl: nativeFetch,
});
const gadget = new gadgetModule.Gadget(context, environment);
const rpcHandler = newBunWebSocketRpcHandler(() => gadget, {
  maxMessageSize: 4 * 1024 * 1024,
});

const server = Bun.serve({
  hostname: "127.0.0.1",
  port: 0,
  maxRequestBodySize: 4 * 1024 * 1024,
  async fetch(request, bunServer) {
    const url = new URL(request.url);
    if (url.pathname === "/healthz") return new Response("ok");
    if (url.pathname !== "/api") return new Response("not found", { status: 404 });
    if (request.headers.get("upgrade")?.toLowerCase() === "websocket") {
      if (bunServer.upgrade(request)) return;
      return new Response("WebSocket upgrade failed", { status: 500 });
    }
    return newHttpBatchRpcResponse(request, gadget, {
      maxMessageSize: 4 * 1024 * 1024,
    });
  },
  websocket: rpcHandler,
});

console.log(`VERGLAS_GADGET_READY=127.0.0.1:${server.port}`);

/** Returns one required process setting without disclosing its value. */
function requireEnvironment(name) {
  const value = process.env[name];
  if (!value) throw new Error(`missing ${name}`);
  return value;
}

/** Removes ambient network constructors from Gadget global scope. */
function denyAmbientNetwork() {
  const denied = () => Promise.reject(new Error(
    "ambient network access is denied; use a declared Gadget binding",
  ));
  Object.defineProperty(globalThis, "fetch", {
    value: denied,
    configurable: false,
    writable: false,
  });
  for (const name of ["WebSocket", "EventSource"]) {
    Object.defineProperty(globalThis, name, {
      value: class {
        constructor() {
          throw new Error("ambient network access is denied; use a declared Gadget binding");
        }
      },
      configurable: false,
      writable: false,
    });
  }
}

/** Reduces a caught value to a bounded message without object traversal. */
function safeError(error) {
  const message = error instanceof Error ? error.message : String(error);
  return message.slice(0, 512);
}

/** Durable Object storage subset backed by the deployment-scoped Verglas KV API. */
class VerglasKvStorage {
  #fetch;
  #endpoint;
  #token;
  #namespace;

  constructor(fetchImpl, endpoint, token, namespace) {
    this.#fetch = fetchImpl;
    this.#endpoint = endpoint;
    this.#token = token;
    this.#namespace = namespace;
  }

  /** Reads one structured value or returns undefined for an absent key. */
  async get(key) {
    const response = await this.#request("GET", key);
    if (response.status === 404) return undefined;
    if (!response.ok) throw new Error(`Verglas KV read failed with ${response.status}`);
    return response.json();
  }

  /** Durably replaces one structured value. */
  async put(key, value) {
    const response = await this.#request("PUT", key, JSON.stringify(value));
    if (!response.ok) throw new Error(`Verglas KV write failed with ${response.status}`);
  }

  /** Idempotently deletes one key. */
  async delete(key) {
    const response = await this.#request("DELETE", key);
    if (!response.ok && response.status !== 404) {
      throw new Error(`Verglas KV delete failed with ${response.status}`);
    }
    return response.status !== 404;
  }

  /** Sends one authenticated request through the captured runtime transport. */
  async #request(method, key, body) {
    if (!this.#endpoint || !this.#token) {
      throw new Error("Verglas KV is not configured for this Gadget runtime");
    }
    const namespace = encodeURIComponent(this.#namespace);
    const encodedKey = encodeURIComponent(String(key));
    return this.#fetch(`${this.#endpoint}/v1/kv/${namespace}/${encodedKey}`, {
      method,
      body,
      headers: {
        authorization: `Bearer ${this.#token}`,
        "content-type": "application/json",
      },
    });
  }
}
