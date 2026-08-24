/**
 * WHATWG fetch conversions at the WIT boundary. Guest code uses StarlingMonkey's
 * real Request, Response, and Headers globals; only byte records cross WIT.
 */

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const RELATIVE_REQUEST_ORIGIN = 'http://verglas.invalid';

/** @param {unknown} value @returns {Uint8Array} */
export function bytesFromValue(value) {
  if (value instanceof Uint8Array) return new Uint8Array(value);
  if (value instanceof ArrayBuffer) return new Uint8Array(value.slice(0));
  if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength));
  if (typeof value === 'string') return encoder.encode(value);
  if (Array.isArray(value)) return new Uint8Array(value);
  if (value === undefined || value === null) return new Uint8Array();
  throw new TypeError('Expected a string, Uint8Array, ArrayBuffer, or byte array');
}

/** @param {unknown} bytes @param {'bytes'|'string'|'json'} [representation] @returns {unknown} */
export function valueFromBytes(bytes, representation = 'bytes') {
  const owned = bytesFromValue(bytes);
  if (representation === 'bytes') return owned;
  if (representation === 'string') return decoder.decode(owned);
  if (representation === 'json') return JSON.parse(decoder.decode(owned));
  throw new TypeError(`Unknown byte representation: ${representation}`);
}

/** @param {unknown} headers @returns {Array<[string,string]>} */
export function headersToTuples(headers) {
  if (headers === undefined || headers === null) return [];
  if (Array.isArray(headers)) {
    return headers.map((entry) => {
      if (!Array.isArray(entry) || entry.length !== 2) throw new TypeError('Headers arrays must contain [name, value] pairs');
      return [String(entry[0]), String(entry[1])];
    });
  }
  if (typeof headers.entries === 'function') return Array.from(headers.entries(), ([name, value]) => [String(name), String(value)]);
  if (typeof headers === 'object') return Object.entries(headers).map(([name, value]) => [name, String(value)]);
  throw new TypeError('Headers must be a Headers object, iterable, or tuple array');
}

/**
 * Converts one WIT request record to a real WHATWG Request. Relative gateway
 * paths receive an opaque origin because Request requires an absolute URL.
 * @param {{method:string,uri:string,headers:unknown,body:unknown}} record
 * @returns {Request}
 */
export function makeRequest(record) {
  const method = String(record.method);
  const body = bytesFromValue(record.body);
  const rawUrl = String(record.uri);
  const url = /^[a-z][a-z\d+.-]*:/iu.test(rawUrl) ? rawUrl : new URL(rawUrl, RELATIVE_REQUEST_ORIGIN).toString();
  const init = { method, headers: new Headers(headersToTuples(record.headers)) };
  if (method !== 'GET' && method !== 'HEAD' && body.byteLength > 0) init.body = body;
  const request = new Request(url, init);
  const pendingWebSocket = optionValue(record.ws);
  if (pendingWebSocket !== undefined) Object.defineProperty(request, '__verglasWebSocketId', { value: pendingWebSocket, enumerable: false });
  return request;
}

/**
 * Converts a user Request into a WIT request record for bindings.do-fetch.
 * @param {RequestInfo|URL|string} input
 * @param {RequestInit} [init]
 * @returns {Promise<{method:string,uri:string,headers:Array<[string,string]>,body:Uint8Array,ws:undefined}>}
 */
export async function requestToRecord(input, init) {
  const request = input instanceof Request && init === undefined ? input : new Request(input, init);
  const body = request.body === null ? new Uint8Array() : new Uint8Array(await request.arrayBuffer());
  return {
    method: request.method,
    uri: request.url,
    headers: headersToTuples(request.headers),
    body,
    ws: request.__verglasWebSocketId,
  };
}

/**
 * Converts a user Response into the WIT response record. The WebSocket accept
 * identity is supplied by the Durable Object handler after ctx.acceptWebSocket.
 * @param {Response} value
 * @param {number|bigint|undefined} [acceptedWebSocket]
 * @returns {Promise<{status:number,headers:Array<[string,string]>,body:Uint8Array,acceptWs:number|bigint|undefined}>}
 */
export async function makeResponse(value, acceptedWebSocket) {
  if (typeof Response !== 'function' || !(value instanceof Response)) throw new TypeError('fetch must return a WHATWG Response');
  const status = Number(value.status);
  if (!Number.isInteger(status) || status < 0 || status > 65535) throw new TypeError('Response status must be an integer between 0 and 65535');
  const headers = headersToTuples(value.headers);
  let body = new Uint8Array();
  if (typeof value.arrayBuffer === 'function') {
    body = new Uint8Array(await value.arrayBuffer());
  } else if (value.body !== undefined && value.body !== null) {
    if (value.body && typeof value.body.getReader === 'function') throw new TypeError('ReadableStream response bodies must be consumed by Response');
    body = bytesFromValue(value.body);
  }
  return {
    status,
    headers,
    body,
    acceptWs: status === 101 ? acceptedWebSocket : undefined,
  };
}

/**
 * Converts a WIT response record returned by a host binding into a real
 * WHATWG Response for the caller's stub.fetch promise.
 * @param {{status:number,headers:unknown,body:unknown,acceptWs?:unknown}} record
 * @returns {Response}
 */
export function responseFromRecord(record) {
  const status = Number(record.status);
  const body = bytesFromValue(record.body);
  const response = new Response(body.byteLength === 0 ? null : body, {
    status,
    headers: new Headers(headersToTuples(record.headers)),
  });
  if (record.acceptWs !== undefined && record.acceptWs !== null) {
    Object.defineProperty(response, 'acceptWebSocketId', { value: record.acceptWs, enumerable: false });
  }
  return response;
}

/** @param {unknown} value @returns {unknown} */
function optionValue(value) {
  if (value && typeof value === 'object' && 'tag' in value) {
    if (value.tag === 'none') return undefined;
    if (value.tag === 'some') return value.val;
  }
  return value === null ? undefined : value;
}

/** @param {number|string|bigint} value @returns {bigint} */
export function u64(value) {
  if (typeof value === 'number' && !Number.isSafeInteger(value)) throw new RangeError('u64 values must be safe integers or bigint values');
  const result = BigInt(value);
  if (result < 0n) throw new RangeError('u64 values must be non-negative');
  return result;
}

/** @param {unknown} error @returns {string} */
export function errorMessage(error) {
  if (error instanceof Error) return error.message;
  if (error && typeof error === 'object' && 'message' in error) return String(error.message);
  return String(error);
}
