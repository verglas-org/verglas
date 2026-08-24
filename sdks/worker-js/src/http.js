/**
 * Small request, response, header, and byte helpers used by the Worker shim.
 * The helpers keep the WIT boundary byte-oriented while exposing the familiar
 * parts of the Fetch API to tenant code.
 */

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * Converts one supported authoring value to an owned byte array.
 * @param {unknown} value
 * @returns {Uint8Array}
 */
export function bytesFromValue(value) {
  if (value instanceof Uint8Array) {
    return new Uint8Array(value);
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value.slice(0));
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength));
  }
  if (typeof value === 'string') {
    return encoder.encode(value);
  }
  if (Array.isArray(value)) {
    return new Uint8Array(value);
  }
  if (value === undefined || value === null) {
    return new Uint8Array();
  }
  throw new TypeError('Expected a string, Uint8Array, ArrayBuffer, or byte array');
}

/**
 * Decodes bytes using the requested storage helper representation.
 * @param {Uint8Array} bytes
 * @param {'bytes'|'string'|'json'} representation
 * @returns {Uint8Array|string|unknown}
 */
export function valueFromBytes(bytes, representation = 'bytes') {
  const owned = bytesFromValue(bytes);
  if (representation === 'bytes') {
    return owned;
  }
  if (representation === 'string') {
    return decoder.decode(owned);
  }
  if (representation === 'json') {
    return JSON.parse(decoder.decode(owned));
  }
  throw new TypeError(`Unknown byte representation: ${representation}`);
}

/**
 * Converts a Headers-like value to the WIT tuple representation.
 * @param {unknown} headers
 * @returns {Array<[string, string]>}
 */
export function headersToTuples(headers) {
  if (headers === undefined || headers === null) {
    return [];
  }
  if (Array.isArray(headers)) {
    return headers.map((entry) => {
      if (!Array.isArray(entry) || entry.length !== 2) {
        throw new TypeError('Headers arrays must contain [name, value] pairs');
      }
      return [String(entry[0]), String(entry[1])];
    });
  }
  if (typeof headers.entries === 'function') {
    return Array.from(headers.entries(), ([name, value]) => [String(name), String(value)]);
  }
  if (typeof headers === 'object') {
    return Object.entries(headers).map(([name, value]) => [name, String(value)]);
  }
  throw new TypeError('Headers must be an object, iterable Headers, or tuple array');
}

/**
 * A minimal case-insensitive Headers surface that does not depend on a host
 * Web API implementation.
 */
export class HeaderBag {
  /** @param {Array<[string, string]>} tuples */
  constructor(tuples) {
    this._tuples = tuples.map(([name, value]) => [String(name), String(value)]);
  }

  /** @param {string} name @returns {string|null} */
  get(name) {
    const lower = String(name).toLowerCase();
    const values = this._tuples
      .filter(([key]) => key.toLowerCase() === lower)
      .map(([, value]) => value);
    return values.length === 0 ? null : values.join(', ');
  }

  /** @param {string} name @returns {boolean} */
  has(name) {
    return this.get(name) !== null;
  }

  /** @param {string} name @param {string} value */
  set(name, value) {
    const lower = String(name).toLowerCase();
    this._tuples = this._tuples.filter(([key]) => key.toLowerCase() !== lower);
    this._tuples.push([String(name), String(value)]);
  }

  /** @param {string} name @param {string} value */
  append(name, value) {
    this._tuples.push([String(name), String(value)]);
  }

  /** @returns {Array<[string, string]>} */
  entries() {
    return this._tuples[Symbol.iterator]();
  }

  /** @returns {Array<[string, string]>} */
  [Symbol.iterator]() {
    return this.entries();
  }

  /** @returns {string[]} */
  keys() {
    return this._tuples.map(([name]) => name)[Symbol.iterator]();
  }

  /** @returns {string[]} */
  values() {
    return this._tuples.map(([, value]) => value)[Symbol.iterator]();
  }
}

/**
 * Creates the standard-ish request object passed to a Worker fetch hook.
 * @param {{method: string, uri: string, headers: unknown, body: unknown}} record
 * @returns {object}
 */
export function makeRequest(record) {
  const body = bytesFromValue(record.body);
  const request = {
    method: String(record.method),
    url: String(record.uri),
    headers: new HeaderBag(headersToTuples(record.headers)),
    body,
    async text() {
      return decoder.decode(body);
    },
    async json() {
      return JSON.parse(decoder.decode(body));
    },
  };
  return request;
}

/**
 * Converts a Response-like authoring value to the WIT response record.
 * @param {unknown} value
 * @returns {Promise<{status: number, headers: Array<[string, string]>, body: Uint8Array}>}
 */
export async function makeResponse(value) {
  if (value === undefined || value === null) {
    throw new TypeError('fetch must return a Response-like value');
  }

  const status = Number(value.status ?? 200);
  if (!Number.isInteger(status) || status < 0 || status > 65535) {
    throw new TypeError('Response status must be an integer between 0 and 65535');
  }

  const responseHeaders = headersToTuples(value.headers);
  let body = value.body;
  if (typeof value.arrayBuffer === 'function') {
    body = await value.arrayBuffer();
  } else if (body && typeof body.arrayBuffer === 'function') {
    body = await body.arrayBuffer();
  } else if (body && typeof body.getReader === 'function') {
    throw new TypeError('ReadableStream response bodies are not supported; return bytes or text');
  }

  return {
    status,
    headers: responseHeaders,
    body: bytesFromValue(body),
  };
}

/**
 * Converts a WIT u64-like value to a stable JavaScript bigint.
 * @param {number|string|bigint} value
 * @returns {bigint}
 */
export function u64(value) {
  if (typeof value === 'number' && !Number.isSafeInteger(value)) {
    throw new RangeError('u64 values must be non-negative safe integers or bigint values');
  }
  const converted = BigInt(value);
  if (converted < 0n) {
    throw new RangeError('u64 values must be non-negative');
  }
  return converted;
}

/**
 * Returns a string suitable for a handler-error payload or thrown exception.
 * @param {unknown} error
 * @returns {string}
 */
export function errorMessage(error) {
  if (error && typeof error === 'object' && 'message' in error) {
    return String(error.message);
  }
  return error instanceof Error ? error.message : String(error);
}
