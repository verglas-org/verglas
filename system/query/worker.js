/**
 * Prebuilt Turso-backed Query materialization and its stateless routing Worker.
 * A Query consumes Pipeline batches directly and exposes only declared bounded endpoints.
 */

import { DurableObject } from 'cloudflare:workers';

const MAX_REQUEST_BYTES = 8 * 1024 * 1024;
const MAX_RESPONSE_BYTES = 1024 * 1024;
const MAX_RECORDS = 10_000;
const MAX_ENDPOINT_LIMIT = 1000;
const encoder = new TextEncoder();

/** Routes the optional public surface to the one configured Query identity. */
async function fetch(request, env) {
  if (env.QUERY_AUTH_TOKEN !== undefined && request.headers.get('authorization') !== `Bearer ${env.QUERY_AUTH_TOKEN}`) {
    return jsonResponse({ error: 'unauthorized' }, 401);
  }
  if (typeof env.QUERY_NAME !== 'string' || !env.QUERY_DO?.idFromName) return jsonResponse({ error: 'Query binding is not configured' }, 500);
  return env.QUERY_DO.get(env.QUERY_DO.idFromName(env.QUERY_NAME)).fetch(request);
}

/** One serialized named materialization stored durably in Turso. */
export class Query extends DurableObject {
  /** Validates and installs the immutable query definition before admitting events. */
  constructor(ctx, env) {
    super(ctx, env);
    this.definition = validateDefinition(env.QUERY_NAME, env.QUERY_DEFINITION);
    this.ready = ctx.blockConcurrencyWhile(async () => {
      await createTables(ctx);
      await installOrCheckConfiguration(ctx, this.definition);
      await createEndpointIndexes(ctx, this.definition);
    });
  }

  /** Dispatches Pipeline ingestion and the two private Query routes. */
  async fetch(request) {
    await this.ready;
    if (request.method.toUpperCase() !== 'POST') return jsonResponse({ error: 'method not allowed' }, 405);
    try {
      const path = new URL(request.url).pathname;
      if (path === '/sink/batch') return await this.ingest(request);
      if (path === '/query/run') return await this.run(request);
      if (path === '/query/describe') return await this.describe();
      return jsonResponse({ error: 'not found' }, 404);
    } catch (error) {
      if (error instanceof FatalQueryError) throw error.cause;
      const status = error instanceof QueryError ? error.status : 400;
      return jsonResponse({ error: stableError(error) }, status);
    }
  }

  /** Validates and materializes one idempotent Pipeline batch. */
  async ingest(request) {
    const { batch, updates } = validateBatch(await requestJson(request), this.definition);
    const payloadDigest = await digest(stableJson(batch));
    const previous = await execute(this.ctx, 'SELECT payload_digest, receipt_json FROM query_batch_receipts WHERE batch_id = ?', batch.batch_id);
    if (previous.length > 0) {
      if (previous[0].payload_digest !== payloadDigest) throw new QueryError(409, 'batch_id was replayed with different content');
      return jsonResponse(JSON.parse(previous[0].receipt_json));
    }
    const watermarkRows = await execute(this.ctx, 'SELECT last_sequence FROM query_source_watermarks WHERE source = ?', batch.source);
    const watermark = watermarkRows[0]?.last_sequence ?? 0;
    if (batch.first_sequence !== watermark + 1) throw new QueryError(409, `batch sequence must begin at ${watermark + 1}`);

    try {
      for (const update of updates) {
        const rows = await execute(this.ctx, 'SELECT measures_json FROM query_view_rows WHERE view_name = ? AND group_key = ?', update.view, update.groupKey);
        const measures = rows.length === 0 ? initialMeasures(update.viewDefinition) : JSON.parse(rows[0].measures_json);
        mergeMeasures(measures, update.measures, update.viewDefinition);
        await execute(this.ctx, `INSERT INTO query_view_rows (view_name, group_key, dimensions_json, measures_json)
          VALUES (?, ?, ?, ?) ON CONFLICT(view_name, group_key) DO UPDATE SET measures_json = excluded.measures_json`,
        update.view, update.groupKey, JSON.stringify(update.dimensions), JSON.stringify(measures));
      }
      await execute(this.ctx, `INSERT INTO query_source_watermarks (source, last_sequence) VALUES (?, ?)
        ON CONFLICT(source) DO UPDATE SET last_sequence = excluded.last_sequence`, batch.source, batch.last_sequence);
      const receipt = { batch_id: batch.batch_id, source: batch.source, last_sequence: batch.last_sequence, records: batch.records.length };
      await execute(this.ctx, 'INSERT INTO query_batch_receipts (batch_id, payload_digest, receipt_json) VALUES (?, ?, ?)', batch.batch_id, payloadDigest, JSON.stringify(receipt));
      return jsonResponse(receipt);
    } catch (error) {
      throw new FatalQueryError(error);
    }
  }

  /** Executes one declared endpoint with typed equality parameters and fixed limits. */
  async run(request) {
    const payload = await requestJson(request);
    exactKeys(payload, new Set(['endpoint', 'params']), 'query request');
    const endpoint = this.definition.endpoints.find((item) => item.name === payload.endpoint);
    if (!endpoint) throw new QueryError(404, 'unknown Query endpoint');
    const params = validateParams(payload.params, endpoint);
    const where = ['view_name = ?'];
    const bindings = [endpoint.view];
    for (const param of endpoint.params) {
      if (params[param.name] !== undefined) {
        where.push('json_extract(dimensions_json, ?) = ?');
        bindings.push(jsonPath(param.dimension), params[param.name]);
      }
    }
    const view = this.definition.views.find((item) => item.name === endpoint.view);
    const dimensionNames = new Set(view.dimensions.map((item) => item.name));
    const order = endpoint.order_by.map((item) => `${dimensionNames.has(item.field) ? 'json_extract(dimensions_json' : 'json_extract(measures_json'}, '${jsonPath(item.field)}') ${item.direction.toUpperCase()}`).join(', ');
    bindings.push(endpoint.limit);
    const rows = await execute(this.ctx, `SELECT dimensions_json, measures_json FROM query_view_rows INDEXED BY query_endpoint_${endpoint.name} WHERE ${where.join(' AND ')}${order ? ` ORDER BY ${order}` : ''} LIMIT ?`, ...bindings);
    const watermarks = await readWatermarks(this.ctx);
    return jsonResponse({ endpoint: endpoint.name, rows: rows.map((row) => ({ ...JSON.parse(row.dimensions_json), ...JSON.parse(row.measures_json) })), watermarks });
  }

  /** Describes the immutable definition and current source watermarks. */
  async describe() {
    return jsonResponse({ name: this.definition.name, sources: this.definition.sources, views: this.definition.views, endpoints: this.definition.endpoints, watermarks: await readWatermarks(this.ctx) });
  }
}

/** Creates the durable tables used by every Query object. */
async function createTables(ctx) {
  await execute(ctx, 'CREATE TABLE IF NOT EXISTS query_config (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), query_name TEXT NOT NULL, config_digest TEXT NOT NULL, config_json TEXT NOT NULL)');
  await execute(ctx, 'CREATE TABLE IF NOT EXISTS query_batch_receipts (batch_id TEXT PRIMARY KEY, payload_digest TEXT NOT NULL, receipt_json TEXT NOT NULL)');
  await execute(ctx, 'CREATE TABLE IF NOT EXISTS query_source_watermarks (source TEXT PRIMARY KEY, last_sequence INTEGER NOT NULL)');
  await execute(ctx, 'CREATE TABLE IF NOT EXISTS query_view_rows (view_name TEXT NOT NULL, group_key TEXT NOT NULL, dimensions_json TEXT NOT NULL, measures_json TEXT NOT NULL, PRIMARY KEY(view_name, group_key))');
}

/** Persists the first configuration and rejects later identity or definition changes. */
async function installOrCheckConfiguration(ctx, definition) {
  const json = stableJson(definition);
  const configDigest = await digest(json);
  const rows = await execute(ctx, 'SELECT query_name, config_digest FROM query_config WHERE singleton = 1');
  if (rows.length === 0) {
    await execute(ctx, 'INSERT INTO query_config (singleton, query_name, config_digest, config_json) VALUES (1, ?, ?, ?)', definition.name, configDigest, json);
  } else if (rows[0].query_name !== definition.name || rows[0].config_digest !== configDigest) {
    throw new Error('Query configuration is immutable');
  }
}

/** Installs expression indexes that match declared endpoint filter patterns. */
async function createEndpointIndexes(ctx, definition) {
  for (const endpoint of definition.endpoints) {
    const expressions = endpoint.params.map((param) => `json_extract(dimensions_json, '${jsonPath(param.dimension)}')`);
    await execute(ctx, `CREATE INDEX IF NOT EXISTS query_endpoint_${endpoint.name} ON query_view_rows(view_name${expressions.length ? `, ${expressions.join(', ')}` : ''}, group_key)`);
  }
}

/** Validates and normalizes one immutable Query definition. */
function validateDefinition(name, raw) {
  identifier(name, 'QUERY_NAME');
  if (!plainObject(raw)) throw new Error('QUERY_DEFINITION must be an object');
  exactKeys(raw, new Set(['sources', 'views', 'endpoints']), 'QUERY_DEFINITION');
  const sources = array(raw.sources, 'sources').map((source) => {
    exactKeys(source, new Set(['name']), 'source'); identifier(source.name, 'source name'); return { name: source.name };
  });
  unique(sources.map((item) => item.name), 'source');
  const sourceNames = new Set(sources.map((item) => item.name));
  const views = array(raw.views, 'views').map((view) => validateView(view, sourceNames));
  unique(views.map((item) => item.name), 'view');
  const viewMap = new Map(views.map((item) => [item.name, item]));
  const endpoints = array(raw.endpoints, 'endpoints').map((endpoint) => validateEndpoint(endpoint, viewMap));
  unique(endpoints.map((item) => item.name), 'endpoint');
  return { name, sources, views, endpoints };
}

/** Validates one grouped aggregate view. */
function validateView(view, sources) {
  if (!plainObject(view)) throw new Error('view must be an object');
  exactKeys(view, new Set(['name', 'source', 'dimensions', 'measures']), 'view');
  identifier(view.name, 'view name'); identifier(view.source, 'view source');
  if (!sources.has(view.source)) throw new Error(`unknown view source: ${view.source}`);
  const dimensions = array(view.dimensions, 'dimensions').map((item) => namedField(item, 'dimension'));
  const measures = array(view.measures, 'measures').map((item) => {
    if (!plainObject(item)) throw new Error('measure must be an object');
    exactKeys(item, new Set(['name', 'op', 'field']), 'measure'); identifier(item.name, 'measure name');
    if (!['count', 'sum', 'min', 'max'].includes(item.op)) throw new Error(`unsupported measure operation: ${item.op}`);
    if (item.op !== 'count') identifier(item.field, 'measure field');
    if (item.op === 'count' && item.field !== undefined) throw new Error('count measure cannot declare a field');
    return { name: item.name, op: item.op, ...(item.field === undefined ? {} : { field: item.field }) };
  });
  if (dimensions.length === 0 || measures.length === 0) throw new Error('views require dimensions and measures');
  unique([...dimensions, ...measures].map((item) => item.name), 'view field');
  return { name: view.name, source: view.source, dimensions, measures };
}

/** Validates one named input field mapping. */
function namedField(item, label) {
  if (!plainObject(item)) throw new Error(`${label} must be an object`);
  exactKeys(item, new Set(['name', 'field']), label); identifier(item.name, `${label} name`); identifier(item.field, `${label} field`);
  return { name: item.name, field: item.field };
}

/** Validates one bounded public endpoint. */
function validateEndpoint(endpoint, views) {
  if (!plainObject(endpoint)) throw new Error('endpoint must be an object');
  exactKeys(endpoint, new Set(['name', 'view', 'params', 'order_by', 'limit']), 'endpoint');
  identifier(endpoint.name, 'endpoint name'); identifier(endpoint.view, 'endpoint view');
  const view = views.get(endpoint.view); if (!view) throw new Error(`unknown endpoint view: ${endpoint.view}`);
  const dimensions = new Set(view.dimensions.map((item) => item.name));
  const fields = new Set([...dimensions, ...view.measures.map((item) => item.name)]);
  const params = array(endpoint.params ?? [], 'endpoint params').map((param) => {
    if (!plainObject(param)) throw new Error('endpoint param must be an object');
    exactKeys(param, new Set(['name', 'type', 'dimension', 'required']), 'endpoint param');
    identifier(param.name, 'parameter name'); identifier(param.dimension, 'parameter dimension');
    if (!dimensions.has(param.dimension)) throw new Error(`unknown parameter dimension: ${param.dimension}`);
    if (!['string', 'number', 'boolean'].includes(param.type)) throw new Error(`unsupported parameter type: ${param.type}`);
    if (param.required !== undefined && typeof param.required !== 'boolean') throw new Error('parameter required must be boolean');
    return { name: param.name, type: param.type, dimension: param.dimension, required: param.required === true };
  });
  unique(params.map((item) => item.name), 'endpoint parameter');
  const order_by = array(endpoint.order_by ?? [], 'order_by').map((item) => {
    if (!plainObject(item)) throw new Error('order_by item must be an object'); exactKeys(item, new Set(['field', 'direction']), 'order_by item');
    identifier(item.field, 'order field'); if (!fields.has(item.field)) throw new Error(`unknown order field: ${item.field}`);
    if (!['asc', 'desc'].includes(item.direction)) throw new Error('order direction must be asc or desc'); return { field: item.field, direction: item.direction };
  });
  if (!Number.isInteger(endpoint.limit) || endpoint.limit < 1 || endpoint.limit > MAX_ENDPOINT_LIMIT) throw new Error(`endpoint limit must be between 1 and ${MAX_ENDPOINT_LIMIT}`);
  return { name: endpoint.name, view: endpoint.view, params, order_by, limit: endpoint.limit };
}

/** Validates the standard append-only Pipeline batch envelope and all records. */
function validateBatch(batch, definition) {
  if (!plainObject(batch)) throw new Error('Pipeline batch must be an object');
  exactKeys(batch, new Set(['batch_id', 'pipeline_id', 'sql_digest', 'source', 'sink', 'first_sequence', 'last_sequence', 'records']), 'Pipeline batch');
  for (const field of ['batch_id', 'pipeline_id', 'sql_digest', 'source', 'sink']) if (typeof batch[field] !== 'string' || batch[field].length === 0) throw new Error(`${field} must be a non-empty string`);
  if (!/^[a-f0-9]{64}$/u.test(batch.sql_digest)) throw new Error('sql_digest must be a SHA-256 digest');
  if (batch.sink !== definition.name) throw new Error('batch sink does not match Query identity');
  if (!definition.sources.some((item) => item.name === batch.source)) throw new Error('batch source is not declared');
  if (!Number.isSafeInteger(batch.first_sequence) || !Number.isSafeInteger(batch.last_sequence) || batch.first_sequence < 1 || batch.last_sequence < batch.first_sequence) throw new Error('batch sequence range is invalid');
  const expectedId = JSON.stringify([batch.pipeline_id, batch.sql_digest, batch.first_sequence, batch.last_sequence, batch.sink]);
  if (batch.batch_id !== expectedId) throw new Error('batch_id does not match the deterministic Pipeline identity');
  if (!Array.isArray(batch.records) || batch.records.length > MAX_RECORDS) throw new Error(`records must be an array of at most ${MAX_RECORDS}`);
  for (const record of batch.records) if (!plainObject(record)) throw new Error('each Pipeline record must be an object');
  return { batch, updates: materializeUpdates(batch, definition) };
}

/** Builds validated group updates before any durable write occurs. */
function materializeUpdates(batch, definition) {
  const updates = new Map();
  for (const view of definition.views.filter((item) => item.source === batch.source)) {
    for (const record of batch.records) {
      const dimensions = {};
      for (const dimension of view.dimensions) {
        const value = record[dimension.field];
        if (!['string', 'number', 'boolean'].includes(typeof value) || (typeof value === 'number' && !Number.isFinite(value))) throw new Error(`dimension ${dimension.field} must be a JSON primitive`);
        dimensions[dimension.name] = value;
      }
      for (const measure of view.measures) if (measure.op !== 'count' && (typeof record[measure.field] !== 'number' || !Number.isFinite(record[measure.field]))) throw new Error(`measure ${measure.field} must be a finite number`);
      const groupKey = stableJson(Object.values(dimensions));
      const key = `${view.name}\0${groupKey}`;
      let update = updates.get(key);
      if (update === undefined) {
        update = { view: view.name, viewDefinition: view, dimensions, groupKey, measures: initialMeasures(view) };
        updates.set(key, update);
      }
      applyMeasures(update.measures, view, record);
    }
  }
  return [...updates.values()];
}

/** Creates zero-state for all declared aggregate measures. */
function initialMeasures(view) { return Object.fromEntries(view.measures.map((item) => [item.name, item.op === 'count' || item.op === 'sum' ? 0 : null])); }

/** Applies one record to an aggregate state. */
function applyMeasures(state, view, record) {
  for (const measure of view.measures) {
    const value = measure.op === 'count' ? undefined : record[measure.field];
    if (measure.op === 'count') state[measure.name] += 1;
    else if (measure.op === 'sum') state[measure.name] += value;
    else if (measure.op === 'min') state[measure.name] = state[measure.name] === null ? value : Math.min(state[measure.name], value);
    else state[measure.name] = state[measure.name] === null ? value : Math.max(state[measure.name], value);
  }
}

/** Merges one batch-local aggregate delta into durable aggregate state. */
function mergeMeasures(state, delta, view) {
  for (const measure of view.measures) {
    if (measure.op === 'count' || measure.op === 'sum') state[measure.name] += delta[measure.name];
    else if (measure.op === 'min') state[measure.name] = state[measure.name] === null ? delta[measure.name] : Math.min(state[measure.name], delta[measure.name]);
    else state[measure.name] = state[measure.name] === null ? delta[measure.name] : Math.max(state[measure.name], delta[measure.name]);
  }
}

/** Validates exact endpoint parameters. */
function validateParams(raw, endpoint) {
  if (!plainObject(raw)) throw new Error('Query params must be an object');
  const declared = new Map(endpoint.params.map((item) => [item.name, item]));
  for (const key of Object.keys(raw)) if (!declared.has(key)) throw new Error(`unknown Query parameter: ${key}`);
  for (const param of endpoint.params) {
    if (param.required && raw[param.name] === undefined) throw new Error(`missing Query parameter: ${param.name}`);
    if (raw[param.name] !== undefined && typeof raw[param.name] !== param.type) throw new Error(`Query parameter ${param.name} must be ${param.type}`);
    if (typeof raw[param.name] === 'number' && !Number.isFinite(raw[param.name])) throw new Error(`Query parameter ${param.name} must be finite`);
  }
  return raw;
}

/** Reads all source watermarks in stable source order. */
async function readWatermarks(ctx) { return Object.fromEntries((await execute(ctx, 'SELECT source, last_sequence FROM query_source_watermarks ORDER BY source')).map((row) => [row.source, row.last_sequence])); }

/** Parses one bounded JSON request. */
async function requestJson(request) {
  const bytes = new Uint8Array(await request.arrayBuffer());
  if (bytes.byteLength > MAX_REQUEST_BYTES) throw new Error(`Query request exceeds ${MAX_REQUEST_BYTES} bytes`);
  const value = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
  if (!plainObject(value)) throw new Error('Query request must be a JSON object'); return value;
}

/** Executes parameterized Durable Object SQL. */
async function execute(ctx, statement, ...bindings) { return (await ctx.storage.sql.exec(statement, ...bindings)).toArray(); }

/** Returns a bounded JSON response. */
function jsonResponse(value, status = 200) {
  const body = JSON.stringify(value); if (encoder.encode(body).byteLength > MAX_RESPONSE_BYTES) return Response.json({ error: `Query response exceeds ${MAX_RESPONSE_BYTES} bytes` }, { status: 400 });
  return new Response(body, { status, headers: { 'content-type': 'application/json' } });
}

/** Stable public error with an explicit HTTP status where needed. */
class QueryError extends Error { constructor(status, message) { super(message); this.status = status; } }

/** Marks a post-mutation failure so the host rolls back the entire DO event. */
class FatalQueryError extends Error { constructor(cause) { super('Query materialization failed'); this.cause = cause; } }

/** Converts thrown values to bounded public messages. */
function stableError(error) { const message = error instanceof Error ? error.message : String(error); return message.length <= 512 ? message : `${message.slice(0, 509)}...`; }

/** Requires an identifier that is safe for generated expression-index names. */
function identifier(value, label) { if (typeof value !== 'string' || !/^[A-Za-z_][A-Za-z0-9_]{0,63}$/u.test(value)) throw new Error(`${label} must be a safe identifier`); }

/** Rejects unknown keys. */
function exactKeys(value, allowed, label) { if (!plainObject(value)) throw new Error(`${label} must be an object`); for (const key of Object.keys(value)) if (!allowed.has(key)) throw new Error(`unknown ${label} key: ${key}`); }

/** Requires an array. */
function array(value, label) { if (!Array.isArray(value)) throw new Error(`${label} must be an array`); return value; }

/** Requires unique names. */
function unique(values, label) { if (new Set(values).size !== values.length) throw new Error(`duplicate ${label} name`); }

/** Returns whether a value is a plain JSON object. */
function plainObject(value) { return value !== null && typeof value === 'object' && !Array.isArray(value); }

/** Returns a safe JSON path for one validated identifier. */
function jsonPath(name) { return `$.${name}`; }

/** Canonicalizes JSON objects for durable identities. */
function stableJson(value) { if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`; if (plainObject(value)) return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(',')}}`; return JSON.stringify(value); }

/** Computes a lowercase SHA-256 digest. */
async function digest(value) { const bytes = new Uint8Array(await crypto.subtle.digest('SHA-256', encoder.encode(value))); return [...bytes].map((byte) => byte.toString(16).padStart(2, '0')).join(''); }

export default { fetch };
