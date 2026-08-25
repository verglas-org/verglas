/**
 * Prebuilt Iceberg Catalog Worker and Durable Object.
 * The object owns immutable deployment identity, REST namespace/table state,
 * and commit receipts. Only deterministic Iceberg publication crosses the
 * private runtime capability binding.
 */

import { DurableObject } from 'cloudflare:workers';

export const REST_CONFIG_PATH = '/v1/config';
export const REST_PREFIX = '/v1/';
export const CATALOG_COMMIT_PATH = '/catalog/commit';
export const CATALOG_STATUS_PATH = '/catalog/status';
export const MAX_COMMIT_BYTES = 8 * 1024 * 1024;
export const MAX_COMMIT_ROWS = 10_000;
export const MAX_AUTHORITY_RESPONSE_BYTES = 64 * 1024;
export const MIN_ROLL_INTERVAL_SECONDS = 60;
export const MAX_ROLL_INTERVAL_SECONDS = 24 * 60 * 60;
export const MAX_ROLL_SIZE_BYTES = 512 * 1024 * 1024;

const CONFIG_TABLE = 'catalog_config';
const LEDGER_TABLE = 'catalog_ledger';
const NAMESPACE_TABLE = 'catalog_namespaces';
const TABLE_TABLE = 'catalog_tables';
const SHA256_HEX = /^[a-f0-9]{64}$/u;
const NAME = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;
const LOCATION_MAX_LENGTH = 255;
const COMPRESSION = new Set(['gzip', 'lz4', 'snappy', 'uncompressed', 'zstd']);
const COMMIT_FIELDS = new Set([
  'batch_id',
  'file_id',
  'sink_id',
  'pipeline_id',
  'sql_digest',
  'source',
  'first_sequence',
  'last_sequence',
  'bucket',
  'namespace',
  'table',
  'format',
  'compression',
  'roll_interval_seconds',
  'roll_size_bytes',
  'records',
]);
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder('utf-8', { fatal: true });

/**
 * Routes only standard public Iceberg REST requests to the named Catalog DO.
 * Commit and status are deliberately absent from this public allowlist.
 * @param {Request} request
 * @param {Record<string, unknown>} env
 * @returns {Promise<Response>}
 */
async function fetch(request, env) {
  const url = new URL(request.url);
  const method = request.method.toUpperCase();
  if (!isPublicRestRequest(method, url.pathname)) return new Response('not found', { status: 404 });

  const catalogId = namedString(env.CATALOG_ID, 'CATALOG_ID');
  const namespace = env.CATALOG_DO;
  if (!namespace || typeof namespace.idFromName !== 'function' || typeof namespace.get !== 'function') {
    return new Response('Catalog binding is not configured', { status: 500 });
  }

  const body = method === 'GET' || method === 'HEAD' ? undefined : new Uint8Array(await request.arrayBuffer());
  const target = new URL(`https://verglas.internal${url.pathname}${url.search}`);
  const internal = new Request(target, {
    method,
    headers: new Headers(request.headers),
    ...(body === undefined ? {} : { body }),
  });
  const id = namespace.idFromName(catalogId);
  return namespace.get(id).fetch(internal);
}

/**
 * One serialized Catalog object. Its immutable configuration and delivery
 * ledger are durable in the object's Turso SQL database.
 */
export class Catalog extends DurableObject {
  #ready;
  #config;
  #initError;

  /**
   * Validates deployment configuration before admitting an object event.
   * @param {DurableObjectState} ctx
   * @param {Record<string, unknown>} env
   */
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
   * Handles the internal commit/status routes and the REST routes reached by
   * the public Worker. No other object path is callable.
   * @param {Request} request
   * @returns {Promise<Response>}
   */
  async fetch(request) {
    await this.#ready;
    if (this.#initError) throw this.#initError;
    const url = new URL(request.url);
    const method = request.method.toUpperCase();
    if (method === 'POST' && url.pathname === CATALOG_COMMIT_PATH) return this.#receiveCommit(request);
    if (method === 'GET' && url.pathname === CATALOG_STATUS_PATH) return this.#statusResponse();
    if (isPublicRestRequest(method, url.pathname)) return this.#handleRest(request);
    return new Response('not found', { status: 404 });
  }

  /**
   * Validates one frozen Sink envelope, resolves a durable replay, or performs
   * one idempotent authority call before inserting the receipt.
   * @param {Request} request
   * @returns {Promise<Response>}
   */
  async #receiveCommit(request) {
    let commit;
    try {
      commit = await parseCommitRequest(request, this.#config);
    } catch (error) {
      return errorResponse(error, error instanceof RequestError ? error.status : 400);
    }

    try {
      const existing = await loadLedgerEntry(this.ctx, commit.batchId);
      if (existing) {
        if (String(existing.payload_digest) !== commit.payloadDigest) {
          throw new RequestError('batch identity was reused with a different payload', 409);
        }
        return jsonResponse(JSON.parse(String(existing.receipt_json)));
      }

      const receipt = await commitToAuthority(this.env, this.#config, commit);
      await execute(
        this.ctx,
        `INSERT INTO ${LEDGER_TABLE} (batch_id, payload_digest, file_id, snapshot_id, rows_committed, receipt_json) VALUES (?, ?, ?, ?, ?, ?)`,
        commit.batchId,
        commit.payloadDigest,
        commit.fileId,
        receipt.snapshot_id,
        receipt.rows_committed,
        JSON.stringify(receipt),
      );
      return jsonResponse(receipt);
    } catch (error) {
      if (error instanceof RequestError) return errorResponse(error, error.status);
      if (error instanceof AuthorityCommitError) return errorResponse(error, 502);
      return errorResponse(error, 503);
    }
  }

  /**
   * Serves the bounded namespace and table registry from this object's Turso database.
   * Iceberg metadata publication remains an internal runtime capability.
   * @param {Request} request
   * @returns {Promise<Response>}
   */
  async #handleRest(request) {
    try {
      const url = new URL(request.url);
      const method = request.method.toUpperCase();
      if (method === 'GET' && url.pathname === REST_CONFIG_PATH) {
        return jsonResponse({ defaults: { warehouse: this.#config.warehouse } });
      }
      if (url.pathname === '/v1/namespaces' && method === 'POST') {
        const body = await readRestJson(request);
        const namespace = namespaceName(body.namespace);
        const properties = plainObject(body.properties ?? {}, 'properties');
        await execute(this.ctx, `INSERT INTO ${NAMESPACE_TABLE} (name, properties_json) VALUES (?, ?)`, namespace, canonicalJson(properties));
        return jsonResponse({ namespace: [namespace], properties });
      }
      const tableMatch = /^\/v1\/namespaces\/([^/]+)\/tables\/([^/]+)$/u.exec(url.pathname);
      if (tableMatch && (method === 'GET' || method === 'HEAD')) {
        const namespace = decodeURIComponent(tableMatch[1]);
        const name = decodeURIComponent(tableMatch[2]);
        const rows = await execute(this.ctx, `SELECT metadata_json FROM ${TABLE_TABLE} WHERE namespace = ? AND name = ?`, namespace, name);
        if (!rows[0]) return new Response('not found', { status: 404 });
        const response = jsonResponse({ metadata: JSON.parse(String(rows[0].metadata_json)) });
        return method === 'HEAD' ? new Response(null, { status: response.status, headers: response.headers }) : response;
      }
      const tablesMatch = /^\/v1\/namespaces\/([^/]+)\/tables$/u.exec(url.pathname);
      if (tablesMatch && method === 'POST') {
        const namespace = decodeURIComponent(tablesMatch[1]);
        const body = await readRestJson(request);
        const name = namedString(body.name, 'table name');
        const schema = plainObject(body.schema, 'schema');
        const namespaces = await execute(this.ctx, `SELECT name FROM ${NAMESPACE_TABLE} WHERE name = ?`, namespace);
        if (!namespaces[0]) throw new RequestError('namespace does not exist', 404);
        const metadata = { name, namespace: [namespace], schema };
        await execute(this.ctx, `INSERT INTO ${TABLE_TABLE} (namespace, name, metadata_json) VALUES (?, ?, ?)`, namespace, name, canonicalJson(metadata));
        return jsonResponse({ metadata });
      }
      return new Response('not found', { status: 404 });
    } catch (error) {
      return errorResponse(error, error instanceof RequestError ? error.status : 409);
    }
  }

  /**
   * Returns only non-secret deployment identity and confirmed receipt count.
   * @returns {Promise<Response>}
   */
  async #statusResponse() {
    const rows = await execute(this.ctx, `SELECT COUNT(*) AS confirmed_batches FROM ${LEDGER_TABLE}`);
    return jsonResponse({
      catalog_id: this.#config.catalogId,
      warehouse: this.#config.warehouse,
      config_digest: this.#config.configDigest,
      confirmed_batches: Number(rows[0]?.confirmed_batches ?? 0),
    });
  }
}

export default { fetch };

/** Reads one bounded REST JSON object. */
async function readRestJson(request) {
  const bytes = new Uint8Array(await request.arrayBuffer());
  if (bytes.byteLength > MAX_AUTHORITY_RESPONSE_BYTES) throw new RequestError('REST request is too large', 413);
  try {
    return plainObject(JSON.parse(textDecoder.decode(bytes)), 'request body');
  } catch (error) {
    if (error instanceof RequestError) throw error;
    throw new RequestError('request body must be valid JSON');
  }
}

/** Validates the one-segment namespace form supported by this product. */
function namespaceName(value) {
  if (!Array.isArray(value) || value.length !== 1) throw new RequestError('namespace must contain one segment');
  return namedString(value[0], 'namespace');
}

/** Validates a plain JSON object. */
function plainObject(value, field) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new RequestError(`${field} must be an object`);
  return value;
}

/**
 * Marks malformed input with a stable status and message.
 */
export class RequestError extends Error {
  /**
   * Creates a request validation failure.
   * @param {string} message
   * @param {number} status
   */
  constructor(message, status = 400) {
    super(message);
    this.name = 'RequestError';
    this.status = status;
  }
}

/**
 * Marks an authority call or receipt that cannot confirm a commit.
 */
class AuthorityCommitError extends Error {
  /**
   * Creates an authority failure.
   * @param {string} message
   */
  constructor(message) {
    super(message);
    this.name = 'AuthorityCommitError';
  }
}

/**
 * Validates all immutable Catalog deployment fields before the event gate.
 * @param {Record<string, unknown>} env
 * @returns {object}
 */
function validateConfiguration(env) {
  const catalogId = namedString(env.CATALOG_ID, 'CATALOG_ID');
  const warehouse = locationString(env.CATALOG_WAREHOUSE, 'CATALOG_WAREHOUSE');
  const bucket = locationString(env.CATALOG_BUCKET, 'CATALOG_BUCKET');
  const namespace = locationString(env.CATALOG_NAMESPACE, 'CATALOG_NAMESPACE');
  const table = locationString(env.CATALOG_TABLE, 'CATALOG_TABLE');
  const sinkId = namedString(env.CATALOG_SINK_ID, 'CATALOG_SINK_ID');
  return {
    catalogId,
    warehouse,
    bucket,
    namespace,
    table,
    sinkId,
  };
}

/**
 * Computes the immutable configuration JSON and digest after normalization.
 * @param {object} preliminary
 * @returns {Promise<object>}
 */
async function completeConfiguration(preliminary) {
  const configJson = canonicalJson({
    catalog_id: preliminary.catalogId,
    warehouse: preliminary.warehouse,
    bucket: preliminary.bucket,
    namespace: preliminary.namespace,
    table: preliminary.table,
    sink_id: preliminary.sinkId,
  });
  const configDigest = await digestHex(configJson);
  return { ...preliminary, configJson, configDigest };
}

/**
 * Creates the immutable configuration and commit ledger tables.
 * @param {DurableObjectState} ctx
 * @returns {Promise<void>}
 */
async function createTables(ctx) {
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${CONFIG_TABLE} (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    catalog_id TEXT NOT NULL,
    config_digest TEXT NOT NULL,
    config_json TEXT NOT NULL
  )`);
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${LEDGER_TABLE} (
    batch_id TEXT PRIMARY KEY,
    payload_digest TEXT NOT NULL,
    file_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    rows_committed INTEGER NOT NULL,
    receipt_json TEXT NOT NULL
  )`);
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${NAMESPACE_TABLE} (
    name TEXT PRIMARY KEY,
    properties_json TEXT NOT NULL
  )`);
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${TABLE_TABLE} (
    namespace TEXT NOT NULL,
    name TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    PRIMARY KEY (namespace, name)
  )`);
}

/**
 * Installs configuration once and rejects any later digest or JSON change.
 * @param {DurableObjectState} ctx
 * @param {object} config
 * @returns {Promise<void>}
 */
async function installOrCheckConfiguration(ctx, config) {
  const rows = await execute(ctx, `SELECT catalog_id, config_digest, config_json FROM ${CONFIG_TABLE} WHERE id = 1`);
  if (rows.length === 0) {
    await execute(
      ctx,
      `INSERT INTO ${CONFIG_TABLE} (id, catalog_id, config_digest, config_json) VALUES (1, ?, ?, ?)`,
      config.catalogId,
      config.configDigest,
      config.configJson,
    );
    return;
  }
  const row = rows[0];
  if (String(row.catalog_id) !== config.catalogId
      || String(row.config_digest) !== config.configDigest
      || String(row.config_json) !== config.configJson) {
    throw new Error(`immutable Catalog configuration mismatch for ${config.catalogId}; delete and recreate the object`);
  }
}

/**
 * Parses the exact Sink commit envelope, validates immutable ownership, and
 * computes a canonical digest used by the delivery ledger.
 * @param {Request} request
 * @param {object} config
 * @returns {Promise<object>}
 */
async function parseCommitRequest(request, config) {
  const contentType = request.headers.get('content-type');
  if (typeof contentType !== 'string' || !/^application\/json(?:\s*;|\s*$)/iu.test(contentType)) {
    throw new RequestError('content-type must be application/json');
  }

  const sinkHeader = boundedHeader(request.headers.get('x-verglas-sink-id'), 'x-verglas-sink-id');
  const batchHeader = boundedHeader(request.headers.get('x-verglas-batch-id'), 'x-verglas-batch-id');
  const fileHeader = boundedHeader(request.headers.get('x-verglas-file-id'), 'x-verglas-file-id');
  const pipelineHeader = boundedHeader(request.headers.get('x-verglas-pipeline-id'), 'x-verglas-pipeline-id');
  const digestHeader = request.headers.get('x-verglas-sql-digest');
  if (digestHeader === null || !SHA256_HEX.test(digestHeader)) {
    throw new RequestError('x-verglas-sql-digest must be a lowercase SHA-256 hex digest');
  }

  const bytes = new Uint8Array(await request.arrayBuffer());
  if (bytes.byteLength > MAX_COMMIT_BYTES) {
    throw new RequestError(`commit body exceeds the ${MAX_COMMIT_BYTES}-byte ceiling`, 413);
  }

  let value;
  try {
    value = JSON.parse(textDecoder.decode(bytes));
  } catch (error) {
    throw new RequestError(`invalid commit JSON: ${errorMessage(error)}`);
  }
  assertJsonValue(value, new WeakSet());
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new RequestError('commit body must be a JSON object');
  }
  for (const field of Object.keys(value)) {
    if (!COMMIT_FIELDS.has(field)) throw new RequestError(`unknown commit field ${field}`);
  }
  for (const field of COMMIT_FIELDS) {
    if (!Object.hasOwn(value, field)) throw new RequestError(`commit body is missing ${field}`);
  }

  const sinkId = namedString(value.sink_id, 'sink_id');
  const pipelineId = namedString(value.pipeline_id, 'pipeline_id');
  const sqlDigest = value.sql_digest;
  if (typeof sqlDigest !== 'string' || !SHA256_HEX.test(sqlDigest)) {
    throw new RequestError('sql_digest must be a lowercase SHA-256 hex digest');
  }
  const source = locationString(value.source, 'source');
  const bucket = locationString(value.bucket, 'bucket');
  const namespace = locationString(value.namespace, 'namespace');
  const table = locationString(value.table, 'table');
  const firstSequence = positiveSequence(value.first_sequence, 'first_sequence');
  const lastSequence = positiveSequence(value.last_sequence, 'last_sequence');
  if (value.first_sequence !== firstSequence || value.last_sequence !== lastSequence) {
    throw new RequestError('sequence range values must be JSON numbers');
  }
  if (lastSequence < firstSequence) throw new RequestError('batch sequence range is reversed');
  if (!Array.isArray(value.records) || value.records.length < 1) {
    throw new RequestError('records must be a non-empty array');
  }
  if (value.records.length > MAX_COMMIT_ROWS) {
    throw new RequestError(`records exceed the ${MAX_COMMIT_ROWS}-row ceiling`, 413);
  }
  for (const record of value.records) {
    if (!record || typeof record !== 'object' || Array.isArray(record) || Object.getPrototypeOf(record) !== Object.prototype) {
      throw new RequestError('records must contain JSON objects');
    }
  }

  if (sinkId !== sinkHeader || pipelineId !== pipelineHeader || sqlDigest !== digestHeader) {
    throw new RequestError('commit identity does not match its request headers');
  }
  if (value.batch_id !== batchHeader || value.file_id !== fileHeader) {
    throw new RequestError('batch and file identities do not match their request headers');
  }
  if (sinkId !== config.sinkId) throw new RequestError(`sink identity ${sinkId} does not match ${config.sinkId}`);
  if (bucket !== config.bucket) throw new RequestError(`bucket ${bucket} does not match immutable Catalog configuration`);
  if (namespace !== config.namespace) throw new RequestError(`namespace ${namespace} does not match immutable Catalog configuration`);
  if (table !== config.table) throw new RequestError(`table ${table} does not match immutable Catalog configuration`);
  if (value.format !== 'parquet') throw new RequestError('format must be parquet');
  if (typeof value.compression !== 'string' || !COMPRESSION.has(value.compression)) {
    throw new RequestError('compression is not supported');
  }
  const rollIntervalSeconds = boundedInteger(value.roll_interval_seconds, 'roll_interval_seconds', MIN_ROLL_INTERVAL_SECONDS, MAX_ROLL_INTERVAL_SECONDS);
  const rollSizeBytes = boundedInteger(value.roll_size_bytes, 'roll_size_bytes', 1, MAX_ROLL_SIZE_BYTES);

  const expectedBatchId = JSON.stringify([pipelineId, sqlDigest, firstSequence, lastSequence, sinkId]);
  if (value.batch_id !== expectedBatchId) throw new RequestError('batch_id is not the deterministic Pipeline identity');
  const expectedFileId = `verglas/${config.sinkId}/batch-${await digestHex(expectedBatchId)}.parquet`;
  if (value.file_id !== expectedFileId) throw new RequestError('file_id is not the deterministic batch file identity');

  const payload = {
    batch_id: expectedBatchId,
    file_id: expectedFileId,
    sink_id: sinkId,
    pipeline_id: pipelineId,
    sql_digest: sqlDigest,
    source,
    first_sequence: firstSequence,
    last_sequence: lastSequence,
    bucket,
    namespace,
    table,
    format: 'parquet',
    compression: value.compression,
    roll_interval_seconds: rollIntervalSeconds,
    roll_size_bytes: rollSizeBytes,
    records: value.records,
  };
  const canonicalPayload = canonicalJson(payload);
  return {
    batchId: expectedBatchId,
    fileId: expectedFileId,
    pipelineId,
    sqlDigest,
    payloadDigest: await digestHex(canonicalPayload),
    canonicalPayload,
  };
}

/**
 * Calls the sole injected authority with a deterministic internal request and
 * accepts only a receipt matching the requested identity and row count.
 * @param {Record<string, unknown>} env
 * @param {object} config
 * @param {object} commit
 * @returns {Promise<object>}
 */
async function commitToAuthority(env, config, commit) {
  const headers = new Headers([
    ['content-type', 'application/json'],
    ['x-verglas-sink-id', config.sinkId],
    ['x-verglas-batch-id', commit.batchId],
    ['x-verglas-file-id', commit.fileId],
    ['x-verglas-pipeline-id', commit.pipelineId],
    ['x-verglas-sql-digest', commit.sqlDigest],
  ]);
  const request = new Request(`https://verglas.internal${CATALOG_COMMIT_PATH}`, {
    method: 'POST',
    headers,
    body: commit.canonicalPayload,
  });

  let response;
  try {
    const capability = env.ICEBERG_COMMIT;
    if (!capability || typeof capability.fetch !== 'function') throw new Error('ICEBERG_COMMIT is not configured');
    response = await capability.fetch(request);
  } catch (error) {
    throw new AuthorityCommitError(`Catalog authority request failed: ${errorMessage(error)}`);
  }
  if (!response || response.status < 200 || response.status >= 300) {
    throw new AuthorityCommitError(`Catalog authority did not confirm batch ${commit.batchId}: HTTP ${response?.status ?? 'unknown'}`);
  }
  const receiptBytes = new Uint8Array(await response.arrayBuffer());
  if (receiptBytes.byteLength > MAX_AUTHORITY_RESPONSE_BYTES) {
    throw new AuthorityCommitError('Catalog authority receipt exceeds its hard response ceiling');
  }
  let receipt;
  try {
    receipt = JSON.parse(textDecoder.decode(receiptBytes));
  } catch (error) {
    throw new AuthorityCommitError(`Catalog authority receipt is not valid JSON: ${errorMessage(error)}`);
  }
  if (!receipt || typeof receipt !== 'object' || Array.isArray(receipt)) {
    throw new AuthorityCommitError('Catalog authority receipt must be a JSON object');
  }
  if (receipt.committed !== true || receipt.batch_id !== commit.batchId || receipt.file_id !== commit.fileId) {
    throw new AuthorityCommitError('Catalog authority receipt did not confirm the requested batch and file');
  }
  if (!Number.isSafeInteger(receipt.rows_committed) || receipt.rows_committed !== countRows(commit.canonicalPayload)) {
    throw new AuthorityCommitError('Catalog authority receipt has the wrong committed row count');
  }
  if (typeof receipt.snapshot_id !== 'string' || receipt.snapshot_id.trim() === '') {
    throw new AuthorityCommitError('Catalog authority receipt is missing snapshot_id');
  }
  return receipt;
}

/**
 * Reads the records count from the already validated canonical payload.
 * @param {string} canonicalPayload
 * @returns {number}
 */
function countRows(canonicalPayload) {
  const payload = JSON.parse(canonicalPayload);
  return payload.records.length;
}

/**
 * Loads one durable ledger entry by its idempotency key.
 * @param {DurableObjectState} ctx
 * @param {string} batchId
 * @returns {Promise<object|undefined>}
 */
async function loadLedgerEntry(ctx, batchId) {
  const rows = await execute(
    ctx,
    `SELECT payload_digest, file_id, snapshot_id, rows_committed, receipt_json FROM ${LEDGER_TABLE} WHERE batch_id = ?`,
    batchId,
  );
  return rows.length === 0 ? undefined : rows[0];
}

/**
 * Determines whether a method/path pair is a standard public Iceberg REST
 * endpoint. Internal Catalog controls intentionally do not match this set.
 * @param {string} method
 * @param {string} pathname
 * @returns {boolean}
 */
export function isPublicRestRequest(method, pathname) {
  if (pathname === REST_CONFIG_PATH) return method === 'GET';
  const rawSegments = pathname.split('/');
  if (rawSegments[0] !== '' || rawSegments.slice(1).some((segment) => segment.length === 0)) return false;
  const segments = rawSegments.slice(1);
  if (segments.length < 2 || segments[0] !== 'v1' || segments[1] !== 'namespaces') return false;
  if (segments.length === 2) return method === 'GET' || method === 'POST';
  if (segments.length === 3) return method === 'GET' || method === 'DELETE';
  if (segments.length === 4 && segments[3] === 'properties') return method === 'POST';
  if (segments.length === 4 && segments[3] === 'tables') return method === 'GET' || method === 'POST';
  if (segments.length === 4 && segments[3] === 'views') return method === 'GET' || method === 'POST';
  if (segments.length === 4 && segments[3] === 'register') return method === 'POST';
  if (segments.length === 5 && (segments[3] === 'tables' || segments[3] === 'views')) {
    return method === 'GET' || method === 'HEAD' || method === 'DELETE' || method === 'POST';
  }
  if (segments.length === 6 && (segments[3] === 'tables' || segments[3] === 'views') && segments[5] === 'rename') {
    return method === 'POST';
  }
  return false;
}

/**
 * Executes one SQL statement against the object's Turso-backed database.
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
 * Requires a non-empty bounded resource name.
 * @param {unknown} value
 * @param {string} name
 * @returns {string}
 */
function namedString(value, name) {
  if (typeof value !== 'string' || value.trim() === '' || !NAME.test(value.trim())) {
    throw new Error(`${name} must be an alphanumeric resource name of at most 128 characters`);
  }
  return value.trim();
}

/**
 * Requires a printable, bounded location or source identity.
 * @param {unknown} value
 * @param {string} name
 * @returns {string}
 */
function locationString(value, name) {
  if (typeof value !== 'string' || value.trim() === '' || value.length > LOCATION_MAX_LENGTH
      || /[\u0000-\u001f\u007f]/u.test(value)) {
    throw new Error(`${name} must be at most ${LOCATION_MAX_LENGTH} printable characters`);
  }
  return value.trim();
}

/**
 * Requires a positive safe integer inside a hard policy bound.
 * @param {unknown} value
 * @param {string} name
 * @param {number} minimum
 * @param {number} maximum
 * @returns {number}
 */
function boundedInteger(value, name, minimum, maximum) {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new RequestError(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return value;
}

/**
 * Requires a bounded non-empty request header.
 * @param {string|null} value
 * @param {string} name
 * @returns {string}
 */
function boundedHeader(value, name) {
  if (value === null || value.trim() === '' || value.length > 1024) {
    throw new RequestError(`${name} is required and bounded`);
  }
  return value;
}

/**
 * Parses one positive sequence value from a JSON number.
 * @param {unknown} value
 * @param {string} name
 * @returns {number}
 */
function positiveSequence(value, name) {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new RequestError(`${name} must be a positive safe integer`);
  }
  return value;
}

/**
 * Verifies that parsed input contains only finite JSON data.
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
 * Serializes JSON with recursively sorted object keys for stable payload and
 * configuration identity. Arrays preserve their documented order.
 * @param {unknown} value
 * @returns {string}
 */
function canonicalJson(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map((item) => canonicalJson(item)).join(',')}]`;
  const keys = Object.keys(value).sort();
  return `{${keys.map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`;
}

/**
 * Hashes UTF-8 text with Web Crypto SHA-256.
 * @param {string} value
 * @returns {Promise<string>}
 */
async function digestHex(value) {
  if (!globalThis.crypto?.subtle) throw new Error('Web Crypto SHA-256 is required for Catalog identities');
  const digest = new Uint8Array(await globalThis.crypto.subtle.digest('SHA-256', textEncoder.encode(value)));
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, '0')).join('');
}

/**
 * Converts unknown errors to bounded plain text.
 * @param {unknown} error
 * @returns {string}
 */
function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Creates a JSON response with an explicit content type.
 * @param {unknown} value
 * @param {number} [status]
 * @returns {Response}
 */
function jsonResponse(value, status = 200) {
  return Response.json(value, { status, headers: { 'content-type': 'application/json' } });
}

/**
 * Creates a stable JSON error response without a stack trace.
 * @param {unknown} error
 * @param {number} status
 * @returns {Response}
 */
function errorResponse(error, status) {
  return jsonResponse({ error: errorMessage(error) }, status);
}
