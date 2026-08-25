/**
 * Prebuilt ordered JSON Stream Worker and Durable Object.
 * The Worker is the optional HTTP edge; the object owns only SQL-backed records
 * and bounded reads. The host event transaction supplies serialization and commit.
 */

import { DurableObject } from 'cloudflare:workers';
import {
  documentedUserErrors,
  emptyUserErrors,
  incrementUserError,
  MAX_RECORDS_PER_REQUEST,
  validateRecord,
  validateSchema,
} from './schema.js';

export const MAX_INGEST_BYTES = 5 * 1024 * 1024;
export const MAX_READ_LIMIT = 1000;
export const METRICS_PATH = '/stream/metrics';
export const APPEND_PATH = '/stream/append';
export const READ_PATH = '/stream/read';
export const APPEND_URI = `https://verglas.internal${APPEND_PATH}`;

export {
  MAX_FIELD_NAME_BYTES,
  MAX_LIST_ITEMS,
  MAX_RECORD_BYTES,
  MAX_RECORD_FIELDS,
  MAX_RECORDS_PER_REQUEST,
  MAX_SCHEMA_BYTES,
  MAX_SCHEMA_DEPTH,
  MAX_SCHEMA_FIELDS,
  USER_ERROR_FAMILIES,
} from './schema.js';

const TABLE = 'stream_records';
const VALIDATION_TABLE = 'stream_record_validation';
const CONFIG_TABLE = 'stream_config';
const METRICS_TABLE = 'stream_metrics';
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

  const method = request.method.toUpperCase();
  const url = new URL(request.url);
  const targetPath = method === 'POST' ? APPEND_PATH : method === 'GET' && url.pathname === METRICS_PATH ? METRICS_PATH : undefined;
  if (targetPath === undefined) return withCors(new Response('method not allowed', { status: 405 }), env);

  const streamName = env.STREAM_NAME;
  const namespace = env.STREAM_DO;
  if (typeof streamName !== 'string' || streamName.trim() === '' || !namespace
      || typeof namespace.idFromName !== 'function' || typeof namespace.get !== 'function') {
    return withCors(new Response('Stream binding is not configured', { status: 500 }), env);
  }

  const body = method === 'POST' ? new Uint8Array(await request.arrayBuffer()) : undefined;
  const target = new URL(`https://verglas.internal${targetPath}${method === 'GET' ? url.search : ''}`);
  const internalRequest = new Request(target, {
    method,
    headers: new Headers(request.headers),
    ...(body === undefined ? {} : { body }),
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
    const schema = validateSchema(env.STREAM_SCHEMA);
    this.#schema = schema;
    this.#ready = ctx.blockConcurrencyWhile(async () => {
      await createTables(ctx);
      await installOrCheckConfiguration(ctx, schema);
    });
  }

  #ready;
  #schema;

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
    if (method === 'GET' && url.pathname === METRICS_PATH) return this.#metrics();
    return new Response('not found', { status: 404 });
  }

  /**
   * Appends one JSON array in the current serialized event.
   * @param {Request} request
   * @returns {Promise<Response>}
   */
  async #append(request) {
    const bytes = new Uint8Array(await request.arrayBuffer());
    const ctx = ctxFor(this);
    if (bytes.byteLength > MAX_INGEST_BYTES) {
      await updateMetrics(ctx, bytes.byteLength, 0, { request_limit: 1 });
      return new Response('request exceeds the 5 MiB limit', { status: 413 });
    }

    let records;
    try {
      records = JSON.parse(textDecoder.decode(bytes));
      assertJsonValue(records, new WeakSet());
    } catch (error) {
      await updateMetrics(ctx, bytes.byteLength, 0, { invalid_json: 1 });
      return new Response(`invalid JSON records: ${error.message}`, { status: 400 });
    }
    if (!Array.isArray(records)) {
      await updateMetrics(ctx, bytes.byteLength, 0, { not_array: 1 });
      return new Response('request body must be a JSON array', { status: 400 });
    }
    if (records.length > MAX_RECORDS_PER_REQUEST) {
      await updateMetrics(ctx, bytes.byteLength, records.length, { record_limit: 1 });
      return new Response(`request exceeds the ${MAX_RECORDS_PER_REQUEST}-record ceiling`, { status: 413 });
    }

    let eventIds;
    try {
      eventIds = parseEventIds(request.headers.get(EVENT_ID_HEADER), records.length);
    } catch (error) {
      return new Response(`invalid producer event identity: ${error.message}`, { status: 400 });
    }

    const sequences = Array.from({ length: records.length }, () => null);
    const outcomes = Array.from({ length: records.length }, () => undefined);
    const requestedFamilies = records.map((record) => validateRecord(record, this.#schema));

    for (let index = 0; index < records.length; index += 1) {
      const eventId = eventIds[index];
      let existing;
      if (eventId !== undefined) {
        existing = await execute(
          ctx,
          `SELECT r.sequence, v.sequence AS validation_sequence, v.validation_family FROM ${TABLE} r LEFT JOIN ${VALIDATION_TABLE} v ON v.sequence = r.sequence WHERE r.producer_event_id = ?`,
          eventId,
        );
      }
      if (existing?.length > 0) {
        sequences[index] = sequenceNumber(existing[0].sequence);
        outcomes[index] = existing[0].validation_sequence === null || existing[0].validation_sequence === undefined
          ? requestedFamilies[index]
          : existing[0].validation_family ?? undefined;
        continue;
      }

      const next = await execute(ctx, `SELECT COALESCE(MAX(sequence), 0) + 1 AS next_sequence FROM ${TABLE}`);
      const sequence = sequenceNumber(next[0]?.next_sequence);
      await execute(
        ctx,
        `INSERT INTO ${TABLE} (sequence, record_json, producer_event_id) VALUES (?, ?, ?)`,
        sequence,
        JSON.stringify(records[index]),
        eventId ?? null,
      );
      await execute(
        ctx,
        `INSERT INTO ${VALIDATION_TABLE} (sequence, validation_family) VALUES (?, ?)`,
        sequence,
        requestedFamilies[index] ?? null,
      );
      sequences[index] = sequence;
      outcomes[index] = requestedFamilies[index];
    }
    await updateMetrics(ctx, bytes.byteLength, records.length, emptyUserErrors());

    const errors = outcomes.flatMap((family, index) => family ? [{ index, family }] : []);
    if (this.#schema === undefined && errors.length === 0) {
      return jsonResponse({ accepted: records.length, sequences });
    }
    return jsonResponse({
      accepted: records.length,
      invalid: errors.length,
      sequences,
      errors,
    });
  }

  /**
   * Reads a bounded exclusive range, omitting validation-invalid rows while
   * advancing next_after across every scanned sequence. No consumer cursor is stored.
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

    const ctx = ctxFor(this);
    const rows = await execute(
      ctx,
      `SELECT r.sequence, r.record_json, r.producer_event_id, v.validation_family FROM ${TABLE} r LEFT JOIN ${VALIDATION_TABLE} v ON v.sequence = r.sequence WHERE r.sequence > ? ORDER BY r.sequence ASC LIMIT ?`,
      after,
      limit,
    );
    const records = [];
    const skipped = [];
    const userErrors = emptyUserErrors();
    let expected = after + 1;
    for (const row of rows) {
      const sequence = sequenceNumber(row.sequence);
      if (sequence !== expected) {
        await incrementExtension(ctx, 'ordering_violations', 1);
        return new Response('stored Stream sequence is not contiguous', { status: 500 });
      }
      if (row.validation_family !== null && row.validation_family !== undefined) {
        const family = String(row.validation_family);
        incrementUserError(userErrors, family);
        skipped.push({ sequence, family });
      } else {
        const item = { sequence, record: JSON.parse(row.record_json) };
        if (row.producer_event_id !== null && row.producer_event_id !== undefined) {
          item.producer_event_id = String(row.producer_event_id);
        }
        records.push(item);
      }
      expected += 1;
    }
    await updateMetrics(ctx, 0, 0, userErrors);
    const nextAfter = rows.length === 0 ? after : expected - 1;
    if (skipped.length === 0 && this.#schema === undefined) return jsonResponse({ records, next_after: nextAfter });
    return jsonResponse({ records, next_after: nextAfter, skipped });
  }

  /**
   * Returns durable operator counters and explicitly labeled Verglas extensions.
   * @returns {Promise<Response>}
   */
  async #metrics() {
    const row = await readMetrics(ctxFor(this));
    return jsonResponse({
      input_bytes: Number(row.input_bytes),
      input_records: Number(row.input_records),
      decode_errors: Number(row.decode_errors),
      user_errors: documentedUserErrors(JSON.parse(row.user_errors_json)),
      extensions: {
        ordering_violations: Number(row.ordering_violations),
        backpressure_events: Number(row.backpressure_events),
        lag_records: Number(row.lag_records),
      },
    });
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

/** @param {DurableObjectState} ctx @returns {Promise<void>} */
async function createTables(ctx) {
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${TABLE} (
    sequence INTEGER PRIMARY KEY,
    record_json TEXT NOT NULL,
    producer_event_id TEXT UNIQUE
  )`);
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${VALIDATION_TABLE} (
    sequence INTEGER PRIMARY KEY,
    validation_family TEXT
  )`);
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${CONFIG_TABLE} (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    schema_json TEXT NOT NULL
  )`);
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${METRICS_TABLE} (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    input_bytes INTEGER NOT NULL,
    input_records INTEGER NOT NULL,
    decode_errors INTEGER NOT NULL,
    user_errors_json TEXT NOT NULL,
    ordering_violations INTEGER NOT NULL,
    backpressure_events INTEGER NOT NULL,
    lag_records INTEGER NOT NULL
  )`);
}

/** @param {DurableObjectState} ctx @param {object|undefined} schema @returns {Promise<void>} */
async function installOrCheckConfiguration(ctx, schema) {
  const schemaJson = schema === undefined ? 'null' : JSON.stringify(schema);
  const rows = await execute(ctx, `SELECT schema_json FROM ${CONFIG_TABLE} WHERE id = 1`);
  if (rows.length === 0) {
    const existingRecords = await execute(ctx, `SELECT sequence FROM ${TABLE} LIMIT 1`);
    if (existingRecords.length > 0 && schema !== undefined) {
      throw new Error('immutable Stream schema is unavailable for existing records; delete and recreate the Stream');
    }
    await execute(ctx, `INSERT INTO ${CONFIG_TABLE} (id, schema_json) VALUES (1, ?)`, schemaJson);
  } else if (String(rows[0].schema_json) !== schemaJson) {
    throw new Error('immutable Stream schema mismatch; delete and recreate the Stream');
  }
  const metrics = await execute(ctx, `SELECT id FROM ${METRICS_TABLE} WHERE id = 1`);
  if (metrics.length === 0) {
    await execute(
      ctx,
      `INSERT INTO ${METRICS_TABLE} (id, input_bytes, input_records, decode_errors, user_errors_json, ordering_violations, backpressure_events, lag_records) VALUES (1, 0, 0, 0, ?, 0, 0, 0)`,
      JSON.stringify(emptyUserErrors()),
    );
  }
}

/** @param {DurableObjectState} ctx @param {number} inputBytes @param {number} inputRecords @param {Record<string, number>} errorCounts @returns {Promise<void>} */
async function updateMetrics(ctx, inputBytes, inputRecords, errorCounts) {
  const current = await readMetrics(ctx);
  const userErrors = JSON.parse(current.user_errors_json);
  let decodeErrors = 0;
  for (const [family, count] of Object.entries(errorCounts)) {
    if (count === 0) continue;
    incrementUserError(userErrors, family);
    userErrors[family] += count - 1;
    decodeErrors += count;
  }
  await execute(
    ctx,
    `UPDATE ${METRICS_TABLE} SET input_bytes = input_bytes + ?, input_records = input_records + ?, decode_errors = decode_errors + ?, user_errors_json = ? WHERE id = 1`,
    inputBytes,
    inputRecords,
    decodeErrors,
    JSON.stringify(userErrors),
  );
}

/** @param {DurableObjectState} ctx @returns {Promise<object>} */
async function readMetrics(ctx) {
  const rows = await execute(ctx, `SELECT input_bytes, input_records, decode_errors, user_errors_json, ordering_violations, backpressure_events, lag_records FROM ${METRICS_TABLE} WHERE id = 1`);
  if (rows.length !== 1) throw new Error('Stream metrics row is missing');
  return rows[0];
}

/** @param {DurableObjectState} ctx @param {'ordering_violations'|'backpressure_events'|'lag_records'} column @param {number} amount @returns {Promise<void>} */
async function incrementExtension(ctx, column, amount) {
  if (!['ordering_violations', 'backpressure_events', 'lag_records'].includes(column)) throw new Error(`unknown Stream metric extension: ${column}`);
  await execute(ctx, `UPDATE ${METRICS_TABLE} SET ${column} = ${column} + ? WHERE id = 1`, amount);
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
  headers.set('access-control-allow-methods', 'GET, POST, OPTIONS');
  headers.set('access-control-allow-headers', 'content-type, authorization, x-verglas-producer-event-id');
  return new Response(response.body, { status: response.status, headers });
}
