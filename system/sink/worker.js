/**
 * Prebuilt exactly-once Iceberg Sink Worker and Durable Object.
 * The object owns only configuration and a Turso delivery ledger. Parquet file
 * creation and Iceberg commits are performed by the bound Catalog object.
 */

import { DurableObject } from 'cloudflare:workers';

export const BATCH_PATH = '/sink/batch';
export const STATUS_PATH = '/sink/status';
export const CATALOG_COMMIT_PATH = '/catalog/commit';
export const MAX_BATCH_ROWS = 10_000;
export const MAX_BATCH_BYTES = 8 * 1024 * 1024;
export const MAX_CATALOG_RESPONSE_BYTES = 64 * 1024;
export const MIN_ROLL_INTERVAL_SECONDS = 60;
export const MAX_ROLL_INTERVAL_SECONDS = 24 * 60 * 60;
export const MAX_ROLL_SIZE_BYTES = 512 * 1024 * 1024;

const CONFIG_TABLE = 'sink_config';
const LEDGER_TABLE = 'sink_ledger';
const SHA256_HEX = /^[a-f0-9]{64}$/u;
const NAME = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;
const COMPRESSION = new Set(['gzip', 'lz4', 'snappy', 'uncompressed', 'zstd']);
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder('utf-8', { fatal: true });
const BATCH_FIELDS = new Set([
  'batch_id',
  'pipeline_id',
  'sql_digest',
  'source',
  'sink',
  'first_sequence',
  'last_sequence',
  'records',
]);

/**
 * Routes only the internal Sink controls to the configured named object.
 * @param {Request} request
 * @param {Record<string, unknown>} env
 * @returns {Promise<Response>}
 */
async function fetch(request, env) {
  const url = new URL(request.url);
  const method = request.method.toUpperCase();
  const allowed = (method === 'POST' && url.pathname === BATCH_PATH)
    || (method === 'GET' && url.pathname === STATUS_PATH);
  if (!allowed) return new Response('not found', { status: 404 });

  const sinkId = requiredString(env.SINK_ID, 'SINK_ID');
  const namespace = env.SINK_DO;
  if (!namespace || typeof namespace.idFromName !== 'function' || typeof namespace.get !== 'function') {
    return new Response('Sink binding is not configured', { status: 500 });
  }

  const body = method === 'GET' ? undefined : new Uint8Array(await request.arrayBuffer());
  const target = new URL(`https://verglas.internal${url.pathname}${url.search}`);
  const internal = new Request(target, {
    method,
    headers: request.headers,
    ...(body === undefined ? {} : { body }),
  });
  const id = namespace.idFromName(sinkId);
  return namespace.get(id).fetch(internal);
}

/**
 * One serialized Sink object. Its immutable configuration and delivery ledger
 * are durable in the object's Turso SQL database; the Catalog is an external
 * idempotent commit authority.
 */
export class Sink extends DurableObject {
  #ready;
  #config;
  #initError;

  /** @param {DurableObjectState} ctx @param {Record<string, unknown>} env */
  constructor(ctx, env) {
    super(ctx, env);
    const preliminary = validateConfiguration(env);
    this.#ready = ctx.blockConcurrencyWhile(async () => {
      try {
        const config = await completeConfiguration(preliminary);
        await createTables(ctx);
        await installOrCheckConfiguration(ctx, config);
        this.#config = config;
      } catch (error) {
        this.#initError = error;
        throw error;
      }
    });
  }

  /**
   * Handles only internal batch delivery and status requests.
   * @param {Request} request
   * @returns {Promise<Response>}
   */
  async fetch(request) {
    await this.#ready;
    if (this.#initError) throw this.#initError;
    const url = new URL(request.url);
    const method = request.method.toUpperCase();
    if (method === 'POST' && url.pathname === BATCH_PATH) return this.#receiveBatch(request);
    if (method === 'GET' && url.pathname === STATUS_PATH) return this.#statusResponse();
    return new Response('not found', { status: 404 });
  }

  /**
   * Validates and delivers one Pipeline batch. The ledger insert is deliberately
   * after the Catalog confirmation, so a crash leaves a safe idempotent retry.
   * @param {Request} request
   * @returns {Promise<Response>}
   */
  async #receiveBatch(request) {
    let batch;
    try {
      batch = await parseBatchRequest(request, this.#config);
    } catch (error) {
      return errorResponse(error, error instanceof RequestError ? error.status : 400);
    }

    try {
      const existing = await loadLedgerReceipt(this.ctx, batch.batchId);
      if (existing) {
        if (existing.payload_digest !== batch.payloadDigest) {
          throw new RequestError('batch identity was reused with a different payload', 409);
        }
        return jsonResponse(JSON.parse(existing.receipt_json));
      }

      const catalogReceipt = await commitToCatalog(this.env, this.#config, batch);
      const receipt = {
        accepted: batch.records.length,
        batch_id: batch.batchId,
        file_id: batch.fileId,
        snapshot_id: catalogReceipt.snapshot_id,
      };
      await execute(
        this.ctx,
        `INSERT INTO ${LEDGER_TABLE} (batch_id, payload_digest, receipt_json) VALUES (?, ?, ?)`,
        batch.batchId,
        batch.payloadDigest,
        JSON.stringify(receipt),
      );
      return jsonResponse(receipt);
    } catch (error) {
      if (error instanceof RequestError) return errorResponse(error, error.status);
      if (error instanceof CatalogCommitError) return errorResponse(error, 502);
      return errorResponse(error, 503);
    }
  }

  /**
   * Returns non-secret configuration identity and the confirmed batch count.
   * @returns {Promise<Response>}
   */
  async #statusResponse() {
    const rows = await execute(this.ctx, `SELECT COUNT(*) AS confirmed_batches FROM ${LEDGER_TABLE}`);
    return jsonResponse({
      sink_id: this.#config.sinkId,
      sink_type: this.#config.sinkType,
      config_digest: this.#config.configDigest,
      confirmed_batches: Number(rows[0]?.confirmed_batches ?? 0),
    });
  }
}

export default { fetch };

/**
 * Marks malformed request input without exposing a stack or internal state.
 */
export class RequestError extends Error {
  /** @param {string} message @param {number} status */
  constructor(message, status = 400) {
    super(message);
    this.name = 'RequestError';
    this.status = status;
  }
}

/**
 * Marks a failed or invalid Catalog commit response. No ledger row is written.
 */
class CatalogCommitError extends Error {
  /** @param {string} message */
  constructor(message) {
    super(message);
    this.name = 'CatalogCommitError';
  }
}

/**
 * Validates immutable Sink vars before the Durable Object event gate starts.
 * @param {Record<string, unknown>} env
 * @returns {object}
 */
function validateConfiguration(env) {
  const sinkId = namedString(env.SINK_ID, 'SINK_ID');
  const sinkType = requiredString(env.SINK_TYPE, 'SINK_TYPE');
  if (sinkType !== 'iceberg') throw new Error('SINK_TYPE must be iceberg');
  const catalogBinding = bindingString(env.SINK_CATALOG_BINDING, 'SINK_CATALOG_BINDING');
  const catalogObject = namedString(env.SINK_CATALOG_OBJECT, 'SINK_CATALOG_OBJECT');
  const bucket = locationString(env.SINK_BUCKET, 'SINK_BUCKET');
  const namespace = locationString(env.SINK_NAMESPACE, 'SINK_NAMESPACE');
  const table = locationString(env.SINK_TABLE, 'SINK_TABLE');
  const compression = requiredString(env.SINK_COMPRESSION, 'SINK_COMPRESSION').toLowerCase();
  if (!COMPRESSION.has(compression)) {
    throw new Error(`SINK_COMPRESSION must be one of ${[...COMPRESSION].join(', ')}`);
  }
  return {
    sinkId,
    sinkType,
    catalogBinding,
    catalogObject,
    bucket,
    namespace,
    table,
    compression,
    rollIntervalSeconds: boundedInteger(
      env.SINK_ROLL_INTERVAL_SECONDS,
      'SINK_ROLL_INTERVAL_SECONDS',
      MIN_ROLL_INTERVAL_SECONDS,
      MAX_ROLL_INTERVAL_SECONDS,
    ),
    rollSizeBytes: boundedInteger(
      env.SINK_ROLL_SIZE_BYTES,
      'SINK_ROLL_SIZE_BYTES',
      1,
      MAX_ROLL_SIZE_BYTES,
    ),
  };
}

/**
 * Computes the immutable configuration digest after all values are normalized.
 * @param {object} preliminary
 * @returns {Promise<object>}
 */
async function completeConfiguration(preliminary) {
  const configJson = JSON.stringify({
    sink_id: preliminary.sinkId,
    sink_type: preliminary.sinkType,
    catalog_binding: preliminary.catalogBinding,
    catalog_object: preliminary.catalogObject,
    bucket: preliminary.bucket,
    namespace: preliminary.namespace,
    table: preliminary.table,
    compression: preliminary.compression,
    roll_interval_seconds: preliminary.rollIntervalSeconds,
    roll_size_bytes: preliminary.rollSizeBytes,
  });
  const configDigest = await digestHex(configJson);
  return { ...preliminary, configJson, configDigest };
}

/**
 * Creates the immutable configuration row and idempotency ledger tables.
 * @param {DurableObjectState} ctx
 * @returns {Promise<void>}
 */
async function createTables(ctx) {
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${CONFIG_TABLE} (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    sink_id TEXT NOT NULL,
    config_digest TEXT NOT NULL,
    config_json TEXT NOT NULL
  )`);
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${LEDGER_TABLE} (
    batch_id TEXT PRIMARY KEY,
    payload_digest TEXT NOT NULL,
    receipt_json TEXT NOT NULL
  )`);
}

/**
 * Installs configuration once and rejects any later digest change.
 * @param {DurableObjectState} ctx
 * @param {object} config
 * @returns {Promise<void>}
 */
async function installOrCheckConfiguration(ctx, config) {
  const rows = await execute(ctx, `SELECT sink_id, config_digest, config_json FROM ${CONFIG_TABLE} WHERE id = 1`);
  if (rows.length === 0) {
    await execute(
      ctx,
      `INSERT INTO ${CONFIG_TABLE} (id, sink_id, config_digest, config_json) VALUES (1, ?, ?, ?)`,
      config.sinkId,
      config.configDigest,
      config.configJson,
    );
    return;
  }
  const row = rows[0];
  if (String(row.config_digest) !== config.configDigest) {
    throw new Error(`immutable Sink configuration mismatch for ${config.sinkId}; delete and recreate the object`);
  }
  if (String(row.sink_id) !== config.sinkId || String(row.config_json) !== config.configJson) {
    throw new Error(`immutable Sink configuration mismatch for ${config.sinkId}; delete and recreate the object`);
  }
}

/**
 * Parses and validates the Pipeline batch envelope and computes its stable file
 * identity. The byte ceiling includes the complete received JSON body.
 * @param {Request} request
 * @param {object} config
 * @returns {Promise<object>}
 */
async function parseBatchRequest(request, config) {
  const contentType = request.headers.get('content-type');
  if (typeof contentType !== 'string' || !/^application\/json(?:\s*;|\s*$)/iu.test(contentType)) {
    throw new RequestError('content-type must be application/json');
  }
  const pipelineHeader = boundedHeader(request.headers.get('x-verglas-pipeline-id'), 'x-verglas-pipeline-id');
  const digestHeader = request.headers.get('x-verglas-sql-digest');
  if (digestHeader === null || !SHA256_HEX.test(digestHeader)) {
    throw new RequestError('x-verglas-sql-digest must be a lowercase SHA-256 hex digest');
  }
  const batchHeader = boundedHeader(request.headers.get('x-verglas-batch-id'), 'x-verglas-batch-id');
  const bytes = new Uint8Array(await request.arrayBuffer());
  if (bytes.byteLength > MAX_BATCH_BYTES) {
    throw new RequestError(`batch body exceeds the ${MAX_BATCH_BYTES}-byte ceiling`, 413);
  }

  let value;
  try {
    value = JSON.parse(textDecoder.decode(bytes));
  } catch (error) {
    throw new RequestError(`invalid batch JSON: ${error.message}`);
  }
  assertJsonValue(value, new WeakSet());
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new RequestError('batch body must be a JSON object');
  }
  for (const field of Object.keys(value)) {
    if (!BATCH_FIELDS.has(field)) throw new RequestError(`unknown batch field ${field}`);
  }
  for (const field of BATCH_FIELDS) {
    if (!Object.hasOwn(value, field)) throw new RequestError(`batch body is missing ${field}`);
  }

  const pipelineId = namedString(value.pipeline_id, 'pipeline_id');
  const sqlDigest = value.sql_digest;
  if (typeof sqlDigest !== 'string' || !SHA256_HEX.test(sqlDigest)) {
    throw new RequestError('sql_digest must be a lowercase SHA-256 hex digest');
  }
  const source = locationString(value.source, 'source');
  const sink = namedString(value.sink, 'sink');
  const firstSequence = positiveSequence(value.first_sequence, 'first_sequence');
  const lastSequence = positiveSequence(value.last_sequence, 'last_sequence');
  if (value.first_sequence !== firstSequence || value.last_sequence !== lastSequence) {
    throw new RequestError('sequence range values must be JSON numbers');
  }
  if (lastSequence < firstSequence) throw new RequestError('batch sequence range is reversed');
  if (!Array.isArray(value.records) || value.records.length < 1) {
    throw new RequestError('records must be a non-empty array');
  }
  if (value.records.length > MAX_BATCH_ROWS) {
    throw new RequestError(`records exceed the ${MAX_BATCH_ROWS}-row ceiling`, 413);
  }
  for (const record of value.records) {
    if (!record || typeof record !== 'object' || Array.isArray(record) || Object.getPrototypeOf(record) !== Object.prototype) {
      throw new RequestError('records must contain JSON objects');
    }
  }
  if (pipelineId !== pipelineHeader) throw new RequestError('pipeline identity does not match the request header');
  if (sqlDigest !== digestHeader) throw new RequestError('SQL digest does not match the request header');
  if (sink !== config.sinkId) throw new RequestError(`sink identity ${sink} does not match ${config.sinkId}`);

  const expectedBatchId = JSON.stringify([pipelineId, sqlDigest, firstSequence, lastSequence, sink]);
  if (value.batch_id !== expectedBatchId || batchHeader !== expectedBatchId) {
    throw new RequestError('batch_id is not the deterministic Pipeline identity');
  }
  const batchJson = JSON.stringify({
    batch_id: expectedBatchId,
    pipeline_id: pipelineId,
    sql_digest: sqlDigest,
    source,
    sink,
    first_sequence: firstSequence,
    last_sequence: lastSequence,
    records: value.records,
  });
  const payloadDigest = await digestHex(batchJson);
  const fileId = `verglas/${config.sinkId}/batch-${await digestHex(expectedBatchId)}.parquet`;
  return {
    batchId: expectedBatchId,
    payloadDigest,
    fileId,
    pipelineId,
    sqlDigest,
    source,
    sink,
    firstSequence,
    lastSequence,
    records: value.records,
  };
}

/**
 * Sends one deterministic commit request to the sole Iceberg authority.
 * @param {Record<string, unknown>} env
 * @param {object} config
 * @param {object} batch
 * @returns {Promise<object>}
 */
async function commitToCatalog(env, config, batch) {
  const payload = {
    batch_id: batch.batchId,
    file_id: batch.fileId,
    sink_id: config.sinkId,
    pipeline_id: batch.pipelineId,
    sql_digest: batch.sqlDigest,
    source: batch.source,
    first_sequence: batch.firstSequence,
    last_sequence: batch.lastSequence,
    bucket: config.bucket,
    namespace: config.namespace,
    table: config.table,
    format: 'parquet',
    compression: config.compression,
    roll_interval_seconds: config.rollIntervalSeconds,
    roll_size_bytes: config.rollSizeBytes,
    records: batch.records,
  };
  const request = new Request(`https://verglas.internal${CATALOG_COMMIT_PATH}`, {
    method: 'POST',
    headers: new Headers([
      ['content-type', 'application/json'],
      ['x-verglas-sink-id', config.sinkId],
      ['x-verglas-batch-id', batch.batchId],
      ['x-verglas-file-id', batch.fileId],
      ['x-verglas-pipeline-id', batch.pipelineId],
      ['x-verglas-sql-digest', batch.sqlDigest],
    ]),
    body: JSON.stringify(payload),
  });
  let response;
  try {
    response = await bindingFetch(env[config.catalogBinding], config.catalogObject, request);
  } catch (error) {
    throw new CatalogCommitError(`Catalog commit request failed: ${errorMessage(error)}`);
  }
  if (!response || response.status < 200 || response.status >= 300) {
    throw new CatalogCommitError(`Catalog did not confirm batch ${batch.batchId}: HTTP ${response?.status ?? 'unknown'}`);
  }
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (bytes.byteLength > MAX_CATALOG_RESPONSE_BYTES) {
    throw new CatalogCommitError('Catalog receipt exceeds its hard response ceiling');
  }
  let receipt;
  try {
    receipt = JSON.parse(textDecoder.decode(bytes));
  } catch (error) {
    throw new CatalogCommitError(`Catalog receipt is not valid JSON: ${error.message}`);
  }
  if (!receipt || typeof receipt !== 'object' || Array.isArray(receipt)) {
    throw new CatalogCommitError('Catalog receipt must be a JSON object');
  }
  if (receipt.committed !== true || receipt.batch_id !== batch.batchId || receipt.file_id !== batch.fileId) {
    throw new CatalogCommitError('Catalog receipt did not confirm the requested batch and file');
  }
  if (!Number.isSafeInteger(receipt.rows_committed) || receipt.rows_committed !== batch.records.length) {
    throw new CatalogCommitError('Catalog receipt has the wrong committed row count');
  }
  if (typeof receipt.snapshot_id !== 'string' || receipt.snapshot_id.trim() === '') {
    throw new CatalogCommitError('Catalog receipt is missing snapshot_id');
  }
  return receipt;
}

/**
 * Loads one previously confirmed receipt, if present.
 * @param {DurableObjectState} ctx
 * @param {string} batchId
 * @returns {Promise<object|undefined>}
 */
async function loadLedgerReceipt(ctx, batchId) {
  const rows = await execute(ctx, `SELECT payload_digest, receipt_json FROM ${LEDGER_TABLE} WHERE batch_id = ?`, batchId);
  return rows.length === 0 ? undefined : rows[0];
}

/**
 * Calls a direct service binding or a named Durable Object binding.
 * @param {unknown} binding
 * @param {string} objectName
 * @param {Request} request
 * @returns {Promise<Response>}
 */
async function bindingFetch(binding, objectName, request) {
  if (binding && typeof binding.fetch === 'function') return binding.fetch(request);
  if (binding && typeof binding.idFromName === 'function' && typeof binding.get === 'function') {
    const id = binding.idFromName(objectName);
    const stub = binding.get(id);
    if (!stub || typeof stub.fetch !== 'function') throw new Error(`binding ${objectName} did not return a fetch stub`);
    return stub.fetch(request);
  }
  throw new Error(`Catalog binding for ${objectName} is not configured`);
}

/**
 * Executes one statement against the object's Turso-backed SQL database.
 * @param {DurableObjectState} ctx
 * @param {string} statement
 * @param {...unknown} bindings
 * @returns {Promise<object[]>}
 */
async function execute(ctx, statement, ...bindings) {
  const result = await ctx.storage.sql.exec(statement, ...bindings);
  return result.toArray();
}

/**
 * Requires a non-empty trimmed string.
 * @param {unknown} value
 * @param {string} name
 * @returns {string}
 */
function requiredString(value, name) {
  if (typeof value !== 'string' || value.trim() === '') throw new Error(`${name} must be a non-empty string`);
  return value.trim();
}

/**
 * Validates a bounded resource identity.
 * @param {unknown} value
 * @param {string} name
 * @returns {string}
 */
function namedString(value, name) {
  const result = requiredString(value, name);
  if (!NAME.test(result)) throw new Error(`${name} must be an alphanumeric resource name of at most 128 characters`);
  return result;
}

/**
 * Validates a binding name without imposing a resource object's name grammar.
 * @param {unknown} value
 * @param {string} name
 * @returns {string}
 */
function bindingString(value, name) {
  const result = requiredString(value, name);
  if (!/^[A-Za-z_][A-Za-z0-9_$]{0,127}$/u.test(result)) throw new Error(`${name} is not a valid binding name`);
  return result;
}

/**
 * Validates a bucket, namespace, table, or source string and its wire size.
 * @param {unknown} value
 * @param {string} name
 * @returns {string}
 */
function locationString(value, name) {
  const result = requiredString(value, name);
  if (result.length > 255 || /[\u0000-\u001f\u007f]/u.test(result)) throw new Error(`${name} must be at most 255 printable characters`);
  return result;
}

/**
 * Parses an explicit integer configuration value within hard bounds.
 * @param {unknown} value
 * @param {string} name
 * @param {number} minimum
 * @param {number} maximum
 * @returns {number}
 */
function boundedInteger(value, name, minimum, maximum) {
  const number = typeof value === 'number'
    ? value
    : (typeof value === 'string' && /^\d+$/u.test(value.trim()) ? Number(value) : NaN);
  if (!Number.isSafeInteger(number) || number < minimum || number > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return number;
}

/**
 * Validates a required bounded request header.
 * @param {string|null} value
 * @param {string} name
 * @returns {string}
 */
function boundedHeader(value, name) {
  if (value === null || value.trim() === '' || value.length > 1024) throw new RequestError(`${name} is required and bounded`);
  return value;
}

/**
 * Parses a positive safe sequence number from JSON.
 * @param {unknown} value
 * @param {string} name
 * @returns {number}
 */
function positiveSequence(value, name) {
  const number = typeof value === 'number' ? value : (typeof value === 'string' && /^\d+$/u.test(value) ? Number(value) : NaN);
  if (!Number.isSafeInteger(number) || number < 1) throw new RequestError(`${name} must be a positive safe integer`);
  return number;
}

/**
 * Verifies that a value contains only finite JSON data.
 * @param {unknown} value
 * @param {WeakSet<object>} ancestors
 * @returns {void}
 */
function assertJsonValue(value, ancestors) {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return;
  if (typeof value === 'number') {
    if (Number.isFinite(value)) return;
    throw new RequestError('JSON numbers must be finite');
  }
  if (typeof value !== 'object') throw new RequestError(`unsupported JSON value ${typeof value}`);
  if (ancestors.has(value)) throw new RequestError('cyclic JSON value');
  ancestors.add(value);
  if (Array.isArray(value)) {
    for (const item of value) assertJsonValue(item, ancestors);
  } else {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) throw new RequestError('JSON objects must be plain objects');
    for (const key of Object.keys(value)) assertJsonValue(value[key], ancestors);
  }
  ancestors.delete(value);
}

/**
 * Hashes a string with Web Crypto for configuration, payload, and file identity.
 * @param {string} value
 * @returns {Promise<string>}
 */
async function digestHex(value) {
  if (!globalThis.crypto?.subtle) throw new Error('Web Crypto SHA-256 is required for Sink identities');
  const digest = new Uint8Array(await globalThis.crypto.subtle.digest('SHA-256', textEncoder.encode(value)));
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, '0')).join('');
}

/**
 * Converts an error into a stable user-facing message.
 * @param {unknown} error
 * @returns {string}
 */
function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Builds a JSON response without exposing implementation details.
 * @param {unknown} value
 * @param {number} [status]
 * @returns {Response}
 */
function jsonResponse(value, status = 200) {
  return Response.json(value, { status, headers: { 'content-type': 'application/json' } });
}

/**
 * Builds a bounded JSON error response.
 * @param {unknown} error
 * @param {number} status
 * @returns {Response}
 */
function errorResponse(error, status) {
  return jsonResponse({ error: errorMessage(error) }, status);
}
