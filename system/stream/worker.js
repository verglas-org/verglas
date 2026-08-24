/**
 * Prebuilt ordered JSON Stream Worker and Durable Object.
 * The Worker is the optional HTTP edge; the object owns only SQL-backed records
 * and bounded reads. The host event transaction supplies serialization and commit.
 */

import { DurableObject } from 'cloudflare:workers';

export const MAX_INGEST_BYTES = 5 * 1024 * 1024;
export const MAX_READ_LIMIT = 1000;
export const APPEND_PATH = '/stream/append';
export const READ_PATH = '/stream/read';
export const APPEND_URI = `https://verglas.internal${APPEND_PATH}`;

const TABLE = 'stream_records';
const EVENT_ID_HEADER = 'x-verglas-producer-event-id';
const textDecoder = new TextDecoder('utf-8', { fatal: true });

/**
 * Routes public HTTP ingestion to the configured named Stream object.
 * @param {Request} request
 * @param {Record<string, unknown>} env
 * @returns {Promise<Response>}
 */
async function fetch(request, env) {
  if (request.method === 'OPTIONS') {
    return withCors(new Response(null, { status: 204 }), env);
  }

  const authFailure = authorize(request, env);
  if (authFailure) return withCors(authFailure, env);

  if (request.method !== 'POST') {
    return withCors(new Response('method not allowed', { status: 405 }), env);
  }

  const streamName = env.STREAM_NAME;
  const namespace = env.STREAM_DO;
  if (typeof streamName !== 'string' || streamName.trim() === '' || !namespace
      || typeof namespace.idFromName !== 'function' || typeof namespace.get !== 'function') {
    return withCors(new Response('Stream binding is not configured', { status: 500 }), env);
  }

  const body = new Uint8Array(await request.arrayBuffer());
  const headers = new Headers(request.headers);
  const internalRequest = new Request(APPEND_URI, {
    method: 'POST',
    headers,
    body,
  });
  const id = namespace.idFromName(streamName);
  const response = await namespace.get(id).fetch(internalRequest);
  return withCors(response, env);
}

/**
 * A single ordered Stream object. Its sequence and identity uniqueness are
 * enforced by SQLite inside the host's serialized Durable Object event.
 */
export class Stream extends DurableObject {
  /** @param {DurableObjectState} ctx @param {Record<string, unknown>} env */
  constructor(ctx, env) {
    super(ctx, env);
    this.#ready = ctx.blockConcurrencyWhile(async () => {
      await execute(ctx, `CREATE TABLE IF NOT EXISTS ${TABLE} (
        sequence INTEGER PRIMARY KEY,
        record_json TEXT NOT NULL,
        producer_event_id TEXT UNIQUE
      )`);
    });
  }

  #ready;

  /**
   * Handles only the two internal routes used by the Worker and Pipeline.
   * @param {Request} request
   * @returns {Promise<Response>}
   */
  async fetch(request) {
    await this.#ready;
    const url = new URL(request.url);
    const method = request.method.toUpperCase();
    if (method === 'POST' && url.pathname === APPEND_PATH) return this.#append(request);
    if (method === 'GET' && url.pathname === READ_PATH) return this.#read(url);
    return new Response('not found', { status: 404 });
  }

  /**
   * Appends one JSON array in the current serialized event.
   * @param {Request} request
   * @returns {Promise<Response>}
   */
  async #append(request) {
    const bytes = new Uint8Array(await request.arrayBuffer());
    if (bytes.byteLength > MAX_INGEST_BYTES) {
      return new Response('request exceeds the 5 MiB limit', { status: 413 });
    }

    let records;
    try {
      records = JSON.parse(textDecoder.decode(bytes));
      assertJsonValue(records, new WeakSet());
    } catch (error) {
      return new Response(`invalid JSON records: ${error.message}`, { status: 400 });
    }
    if (!Array.isArray(records)) {
      return new Response('request body must be a JSON array', { status: 400 });
    }

    let eventIds;
    try {
      eventIds = parseEventIds(request.headers.get(EVENT_ID_HEADER), records.length);
    } catch (error) {
      return new Response(`invalid producer event identity: ${error.message}`, { status: 400 });
    }

    const sequences = [];
    for (let index = 0; index < records.length; index += 1) {
      const eventId = eventIds[index];
      if (eventId !== undefined) {
        const existing = await execute(ctxFor(this), `SELECT sequence FROM ${TABLE} WHERE producer_event_id = ?`, eventId);
        if (existing.length > 0) {
          sequences.push(sequenceNumber(existing[0].sequence));
          continue;
        }
      }

      const next = await execute(ctxFor(this), `SELECT COALESCE(MAX(sequence), 0) + 1 AS next_sequence FROM ${TABLE}`);
      const sequence = sequenceNumber(next[0]?.next_sequence);
      await execute(
        ctxFor(this),
        `INSERT INTO ${TABLE} (sequence, record_json, producer_event_id) VALUES (?, ?, ?)`,
        sequence,
        JSON.stringify(records[index]),
        eventId ?? null,
      );
      sequences.push(sequence);
    }

    return jsonResponse({ accepted: records.length, sequences });
  }

  /**
   * Reads a bounded exclusive sequence range without storing a consumer cursor.
   * @param {URL} url
   * @returns {Promise<Response>}
   */
  async #read(url) {
    let after;
    let limit;
    try {
      after = parseNonNegativeInteger(url.searchParams.get('after'), 'after');
      limit = parsePositiveInteger(url.searchParams.get('limit'), 'limit');
      if (limit > MAX_READ_LIMIT) throw new Error(`limit must be at most ${MAX_READ_LIMIT}`);
    } catch (error) {
      return new Response(`invalid read range: ${error.message}`, { status: 400 });
    }

    const rows = await execute(
      ctxFor(this),
      `SELECT sequence, record_json, producer_event_id FROM ${TABLE} WHERE sequence > ? ORDER BY sequence ASC LIMIT ?`,
      after,
      limit,
    );
    const records = [];
    let expected = after + 1;
    for (const row of rows) {
      const sequence = sequenceNumber(row.sequence);
      if (sequence !== expected) {
        return new Response('stored Stream sequence is not contiguous', { status: 500 });
      }
      const item = { sequence, record: JSON.parse(row.record_json) };
      if (row.producer_event_id !== null && row.producer_event_id !== undefined) {
        item.producer_event_id = String(row.producer_event_id);
      }
      records.push(item);
      expected += 1;
    }
    const nextAfter = records.length === 0 ? after : records[records.length - 1].sequence;
    return jsonResponse({ records, next_after: nextAfter });
  }
}

/**
 * The Worker export uses a fixed namespace binding from the system manifest.
 */
export default { fetch };

/** @param {DurableObject} object @returns {DurableObjectState} */
function ctxFor(object) {
  return object.ctx;
}

/** @param {DurableObjectState} ctx @param {string} statement @param {...unknown} bindings @returns {Promise<object[]>} */
async function execute(ctx, statement, ...bindings) {
  const cursor = await ctx.storage.sql.exec(statement, ...bindings);
  return cursor.toArray();
}

/** @param {unknown} value @returns {number} */
function sequenceNumber(value) {
  const sequence = Number(value);
  if (!Number.isSafeInteger(sequence) || sequence < 1) throw new Error('stored sequence is invalid');
  return sequence;
}

/** @param {string|null} header @param {number} count @returns {Array<string|undefined>} */
function parseEventIds(header, count) {
  if (header === null) return Array.from({ length: count }, () => undefined);
  let values;
  if (count === 1 && !header.trimStart().startsWith('[')) {
    values = [header];
  } else {
    values = JSON.parse(header);
    if (!Array.isArray(values) || values.length !== count) throw new Error('identity list must match record count');
  }
  for (const value of values) {
    if (typeof value !== 'string' || value.length === 0 || value.length > 512) {
      throw new Error('identities must be non-empty strings of at most 512 characters');
    }
  }
  return values;
}

/** @param {string|null} value @param {string} name @returns {number} */
function parseNonNegativeInteger(value, name) {
  if (value === null || !/^\d+$/u.test(value)) throw new Error(`${name} must be a non-negative integer`);
  const result = Number(value);
  if (!Number.isSafeInteger(result)) throw new Error(`${name} is too large`);
  return result;
}

/** @param {string|null} value @param {string} name @returns {number} */
function parsePositiveInteger(value, name) {
  const result = parseNonNegativeInteger(value, name);
  if (result < 1) throw new Error(`${name} must be positive`);
  return result;
}

/** @param {unknown} value @param {WeakSet<object>} ancestors */
function assertJsonValue(value, ancestors) {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return;
  if (typeof value === 'number') {
    if (Number.isFinite(value)) return;
    throw new TypeError('numbers must be finite');
  }
  if (typeof value !== 'object') throw new TypeError(`unsupported ${typeof value}`);
  if (ancestors.has(value)) throw new TypeError('cyclic value');
  ancestors.add(value);
  if (Array.isArray(value)) {
    for (const entry of value) assertJsonValue(entry, ancestors);
  } else {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) throw new TypeError('records must contain plain objects');
    for (const key of Object.keys(value)) assertJsonValue(value[key], ancestors);
  }
  ancestors.delete(value);
}

/** @param {unknown} value @param {number} status @param {HeadersInit} [headers] @returns {Response} */
function jsonResponse(value, status = 200, headers) {
  return Response.json(value, { status, headers });
}

/** @param {Request} request @param {Record<string, unknown>} env @returns {Response|undefined} */
function authorize(request, env) {
  if (env.STREAM_AUTH_TOKEN === undefined) return undefined;
  if (typeof env.STREAM_AUTH_TOKEN !== 'string' || env.STREAM_AUTH_TOKEN.length === 0) {
    return new Response('Stream authentication is misconfigured', { status: 500 });
  }
  if (request.headers.get('authorization') !== `Bearer ${env.STREAM_AUTH_TOKEN}`) {
    return new Response('unauthorized', { status: 401 });
  }
  return undefined;
}

/** @param {Response} response @param {Record<string, unknown>} env @returns {Response} */
function withCors(response, env) {
  const origin = env.STREAM_CORS_ORIGIN;
  if (origin === undefined) return response;
  if (typeof origin !== 'string' || origin.length === 0) {
    return new Response('Stream CORS is misconfigured', { status: 500 });
  }
  const headers = new Headers(response.headers);
  headers.set('access-control-allow-origin', origin);
  headers.set('access-control-allow-methods', 'POST, OPTIONS');
  headers.set('access-control-allow-headers', 'content-type, authorization, x-verglas-producer-event-id');
  return new Response(response.body, { status: response.status, headers });
}
