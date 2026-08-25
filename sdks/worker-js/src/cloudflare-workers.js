/**
 * The `cloudflare:workers` module surface bundled into a guest component.
 * Cloudflare classes remain ordinary guest JavaScript; this module only adapts
 * their fetch, storage, alarm, binding, and WebSocket calls to WIT records.
 */

import {
  decodeStructuredClone,
  DurableObject,
  DurableObjectId,
  DurableObjectNamespace,
  DurableObjectState,
  DurableObjectStorage,
  DurableObjectStub,
  SqlStorageCursor,
  encodeStructuredClone,
  sha256Hex,
} from './durable-objects.js';
import {
  bytesFromValue,
  makeRequest,
  makeResponse,
  requestToRecord,
  responseFromRecord,
} from './http.js';
import { unwrapOption } from './transport-core.js';
import { createStreamBinding, PipelineBinding } from './streams.js';

const ZERO_ID = '0'.repeat(64);

export {
  DurableObject,
  DurableObjectId,
  DurableObjectNamespace,
  DurableObjectState,
  DurableObjectStorage,
  DurableObjectStub,
  SqlStorageCursor,
  PipelineBinding,
  createStreamBinding,
};

/**
 * Creates the remote namespace object exposed by a Worker `env` binding.
 * `stub.fetch` is the only RPC surface in WIT v2; the host routes the named
 * object and remains responsible for component selection and durability.
 * @param {string} bindingName
 * @param {object} transport
 * @returns {object}
 */
export function createDurableObjectBinding(bindingName, transport) {
  const idsByName = new Map();
  return {
    idFromName(name) {
      if (typeof name !== 'string') throw new TypeError('DurableObjectNamespace.idFromName requires a string name');
      if (!idsByName.has(name)) idsByName.set(name, new DurableObjectId(sha256Hex(name), name));
      return idsByName.get(name);
    },
    idFromString(hex) {
      return new DurableObjectId(hex);
    },
    newUniqueId() {
      const bytes = new Uint8Array(32);
      if (!globalThis.crypto || typeof globalThis.crypto.getRandomValues !== 'function') {
        throw new Error('DurableObjectNamespace.newUniqueId requires Web Crypto getRandomValues');
      }
      globalThis.crypto.getRandomValues(bytes);
      return new DurableObjectId(bytesToHex(bytes));
    },
    get(id) {
      if (!(id instanceof DurableObjectId)) throw new TypeError('DurableObjectNamespace.get requires a DurableObjectId');
      const object = id.name ?? id.toString();
      const stub = new DurableObjectStub(id, async (input, init) => {
        const request = await requestToRecord(input, init);
        const result = await transport.doFetch(bindingName, object, request);
        return responseFromRecord(result);
      });
      return stub;
    },
  };
}

/**
 * Creates a direct service binding over the existing declared-binding fetch ABI.
 * @param {string} bindingName
 * @param {string} serviceName
 * @param {object} transport
 * @returns {{fetch: Function}}
 */
export function createServiceBinding(bindingName, serviceName, transport) {
  return Object.freeze({
    async fetch(input, init) {
      const request = await requestToRecord(input, init);
      const result = await transport.doFetch(bindingName, serviceName, request);
      return responseFromRecord(result);
    },
  });
}

/**
 * Builds the Worker-tier export for the component world. It receives no
 * storage or socket object; those capabilities exist only on DO handler calls.
 * @param {object} project
 * @param {object} manifest
 * @param {object} [options]
 * @returns {{fetch: Function, env: object}}
 */
export function createWorker(project, manifest, options = {}) {
  const transport = requireTransport(options);
  const env = createEnvironment(manifest, transport);
  const worker = project?.default ?? project;
  if (!worker || typeof worker !== 'object' || typeof worker.fetch !== 'function') {
    throw new TypeError('Worker module must default-export an object with fetch(request, env, ctx)');
  }
  return {
    env,
    async fetch(record) {
      const request = makeRequest(record);
      const context = new WorkerExecutionContext();
      const response = await worker.fetch(request, env, context);
      await context.waitUntilSettled();
      return await makeResponse(response, response?.acceptWebSocketId);
    },
  };
}

/**
 * Builds the Durable Object handler export around the class named by the
 * manifest. The v2 WIT handler has one class entry per component artifact;
 * the host selects the artifact from the manifest binding.
 * @param {object} project
 * @param {object} manifest
 * @param {object} [options]
 * @returns {{init: Function, fetch: Function, alarm: Function, websocketMessage: Function, websocketClose: Function}}
 */
export function createHandler(project, manifest, options = {}) {
  const transport = requireTransport(options);
  const env = createEnvironment(manifest, transport, { transactionalStreams: true });
  const classSelection = selectClassName(manifest);
  const className = classSelection.name;
  const objectConstructor = className ? project?.[className] : undefined;
  if (className && typeof objectConstructor !== 'function') {
    throw new TypeError(`Durable Object class ${className} is not exported by the Worker module`);
  }
  let object;
  let state;

  async function initialize() {
    if (object) return;
    if (classSelection.error) throw new Error(classSelection.error);
    if (!objectConstructor) throw new Error('Durable Object handler requires a durable_objects binding class');
    const id = options.objectId ?? new DurableObjectId(ZERO_ID);
    const storage = new DurableObjectStorage(id, transport);
    state = new DurableObjectState(id, storage);
    state.getWebSocket = (socket) => createSocket(socket, transport);
    object = new objectConstructor(state, env);
    await state.waitForConcurrency();
  }

  return {
    async init() {
      await initialize();
    },
    async fetch(record) {
      await initialize();
      state.beginRequest(optionNumber(record?.ws), (socket) => createSocket(socket, transport));
      const response = await object.fetch(makeRequest(record));
      await state.waitForWaitUntil();
      return await makeResponse(response, state.acceptedWebSocketId());
    },
    async alarm(scheduledEpochMillis) {
      await initialize();
      await object.alarm(Number(scheduledEpochMillis));
      await state.waitForWaitUntil();
    },
    async websocketMessage(socket, message) {
      await initialize();
      if (typeof object.webSocketMessage === 'function') {
        await object.webSocketMessage(createSocket(socket, transport), new Uint8Array(message));
      }
      await state.waitForWaitUntil();
    },
    async websocketClose(socket, code, reason) {
      await initialize();
      if (typeof object.webSocketClose === 'function') {
        await object.webSocketClose(createSocket(socket, transport), code, reason);
      }
      await state.waitForWaitUntil();
    },
  };
}

/** A Cloudflare execution context whose retained work is awaited at event end. */
class WorkerExecutionContext {
  #tasks = new Set();
  #passThrough = false;

  /** @param {Promise<unknown>} promise */
  waitUntil(promise) {
    const task = Promise.resolve(promise);
    this.#tasks.add(task);
    void task.then(() => this.#tasks.delete(task), () => this.#tasks.delete(task));
  }

  /** Records the Cloudflare pass-through request; the host has no pass-through path in v2. */
  passThroughOnException() {
    this.#passThrough = true;
  }

  /** @returns {boolean} */
  get passThroughRequested() {
    return this.#passThrough;
  }

  /** @returns {Promise<void>} */
  async waitUntilSettled() {
    await Promise.all([...this.#tasks]);
  }
}

/** @param {object} options @returns {object} */
function requireTransport(options) {
  if (!options.transport) throw new Error('component transport is required; use the WIT-backed shim entry');
  return options.transport;
}

/** @param {object} manifest @param {object} transport @param {{transactionalStreams?: boolean}} [options] @returns {object} */
function createEnvironment(manifest, transport, options = {}) {
  const env = { ...(manifest?.vars ?? {}) };
  for (const binding of manifest?.bindings ?? []) {
    env[binding.name] = createDurableObjectBinding(binding.name, transport);
  }
  for (const pipeline of manifest?.pipelines ?? []) {
    env[pipeline.binding] = createStreamBinding(
      pipeline.binding,
      pipeline.stream,
      transport,
      { transactional: options.transactionalStreams === true },
    );
  }
  for (const service of manifest?.services ?? []) {
    env[service.binding] = createServiceBinding(service.binding, service.service, transport);
  }
  return Object.freeze(env);
}

/** @param {object} manifest @returns {{name:string|undefined,error:string|undefined}} */
function selectClassName(manifest) {
  const names = [...new Set((manifest?.bindings ?? []).map((binding) => binding.class_name))];
  if (names.length > 1) {
    return {
      name: undefined,
      error: `one component artifact cannot expose multiple Durable Object classes: ${names.join(', ')}`,
    };
  }
  return { name: names[0], error: undefined };
}

/** @param {unknown} value @returns {number|bigint|undefined} */
function optionNumber(value) {
  const option = unwrapOption(value);
  if (option === undefined) return undefined;
  return typeof option === 'bigint' ? option : Number(option);
}

/** @param {number|bigint} socket @param {object} transport @returns {object} */
function createSocket(socket, transport) {
  const id = typeof socket === 'bigint' ? socket : BigInt(socket);
  return {
    id,
    readyState: 1,
    bufferedAmount: 0,
    send(data) {
      return transport.send(id, bytesFromValue(data));
    },
    close(code = 1000, reason = '') {
      return transport.close(id, code, reason);
    },
    serializeAttachment(value) {
      return transport.setAttachment(id, encodeStructuredClone(value));
    },
    deserializeAttachment() {
      const result = transport.getAttachment(id);
      if (result && typeof result.then === 'function') {
        return result.then((value) => {
          const bytes = unwrapOption(value);
          return bytes === undefined ? null : decodeStructuredClone(bytes);
        });
      }
      const bytes = unwrapOption(result);
      return bytes === undefined ? null : decodeStructuredClone(bytes);
    },
    get attachment() {
      throw new Error('WebSocket attachment reads must use await deserializeAttachment()');
    },
  };
}

/** @param {Uint8Array} bytes @returns {string} */
function bytesToHex(bytes) {
  return Array.from(bytes, (value) => value.toString(16).padStart(2, '0')).join('');
}
