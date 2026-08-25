/**
 * Cloudflare-shaped Pipeline binding support for durable JSON Streams.
 * A binding owns only its fixed stream identity and one send method; it is not a
 * Durable Object namespace and never creates a local or fallback transport.
 */

import { unwrapResult } from './transport-core.js';

/** Cloudflare's hard encoded request ceiling for one Stream.send call. */
export const STREAM_MAX_REQUEST_BYTES = 5 * 1024 * 1024;

/** The one internal route understood by a Stream Durable Object. */
export const STREAM_APPEND_URI = 'https://verglas.internal/stream/append';

const textEncoder = new TextEncoder();

/**
 * A fixed Cloudflare Pipeline binding whose send call targets one Stream object.
 */
export class PipelineBinding {
  /** @param {string} bindingName @param {string} streamName @param {object} transport @param {{transactional?: boolean}} [options] */
  constructor(bindingName, streamName, transport, options = {}) {
    if (typeof bindingName !== 'string' || bindingName.trim() === '') {
      throw new TypeError('Pipeline binding name must be a non-empty string');
    }
    if (typeof streamName !== 'string' || streamName.trim() === '') {
      throw new TypeError('Stream identity must be a non-empty string');
    }
    const transactional = options.transactional === true;
    if (transactional) {
      if (!transport || typeof transport.streamSend !== 'function') {
        throw new TypeError('Durable Object Stream binding requires the WIT storage.stream-send transport');
      }
    } else if (!transport || typeof transport.doFetch !== 'function') {
      throw new TypeError('Worker Stream binding requires the WIT bindings.do-fetch transport');
    }
    this.#bindingName = bindingName;
    this.#streamName = streamName;
    this.#transport = transport;
    this.#transactional = transactional;
    Object.freeze(this);
  }

  #bindingName;
  #streamName;
  #transport;
  #transactional;

  /**
   * Sends JSON records and resolves only after the Stream returns a 2xx ACK.
   * @param {unknown} records
   * @returns {Promise<void>}
   */
  async send(records) {
    if (!Array.isArray(records)) {
      throw new TypeError('Stream.send requires an Array of JSON-serializable records');
    }
    assertJsonValue(records, new WeakSet());

    let serialized;
    let encoded;
    try {
      serialized = JSON.stringify(records);
      encoded = textEncoder.encode(serialized);
    } catch (error) {
      throw new TypeError(`Stream.send requires JSON-serializable records: ${error.message}`);
    }
    if (encoded.byteLength > STREAM_MAX_REQUEST_BYTES) {
      throw new RangeError(`Stream.send request exceeds the 5 MiB encoded request limit (${encoded.byteLength} bytes)`);
    }
    if (this.#transactional) {
      await this.#transport.streamSend(this.#bindingName, this.#streamName, serialized);
      return;
    }

    const rawResult = await this.#transport.doFetch(
      this.#bindingName,
      this.#streamName,
      {
        method: 'POST',
        uri: STREAM_APPEND_URI,
        headers: [['content-type', 'application/json']],
        body: encoded,
        ws: undefined,
      },
    );
    const result = unwrapResult(rawResult, 'bindings.do-fetch');
    const status = Number(result?.status);
    if (!Number.isInteger(status) || status < 200 || status >= 300) {
      throw new Error(`Stream.send did not receive a durable ACK: HTTP ${String(result?.status)}`);
    }
  }
}

/**
 * Creates one fixed Pipeline binding for the generated Worker environment.
 * @param {string} bindingName
 * @param {string} streamName
 * @param {object} transport
 * @param {{transactional?: boolean}} [options]
 * @returns {PipelineBinding}
 */
export function createStreamBinding(bindingName, streamName, transport, options) {
  return new PipelineBinding(bindingName, streamName, transport, options);
}

/**
 * Rejects values that JSON.stringify would silently drop or rewrite.
 * @param {unknown} value
 * @param {WeakSet<object>} ancestors
 */
function assertJsonValue(value, ancestors) {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return;
  if (typeof value === 'number') {
    if (Number.isFinite(value)) return;
    throw new TypeError('Stream.send requires JSON-serializable records: numbers must be finite');
  }
  if (typeof value !== 'object') {
    throw new TypeError(`Stream.send requires JSON-serializable records: unsupported ${typeof value}`);
  }
  if (ancestors.has(value)) {
    throw new TypeError('Stream.send requires JSON-serializable records: cyclic value');
  }
  ancestors.add(value);
  if (Array.isArray(value)) {
    for (const entry of value) assertJsonValue(entry, ancestors);
  } else {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new TypeError('Stream.send requires JSON-serializable records: objects must be plain');
    }
    for (const key of Object.keys(value)) assertJsonValue(value[key], ancestors);
    for (const symbol of Object.getOwnPropertySymbols(value)) {
      if (Object.prototype.propertyIsEnumerable.call(value, symbol)) {
        throw new TypeError('Stream.send requires JSON-serializable records: symbol keys are not supported');
      }
    }
  }
  ancestors.delete(value);
}
