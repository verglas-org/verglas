/**
 * Prebuilt Cloudflare-shaped Vectorize Worker and Durable Object.
 * Each named object owns immutable index configuration, F32 vectors, metadata
 * declarations, and mutation receipts in its serialized Turso transaction.
 */

import { DurableObject } from 'cloudflare:workers';

export const INSERT_PATH = '/vectorize/insert';
export const UPSERT_PATH = '/vectorize/upsert';
export const QUERY_PATH = '/vectorize/query';
export const QUERY_BY_ID_PATH = '/vectorize/query-by-id';
export const GET_BY_IDS_PATH = '/vectorize/get-by-ids';
export const DELETE_BY_IDS_PATH = '/vectorize/delete-by-ids';
export const DESCRIBE_PATH = '/vectorize/describe';
export const METADATA_CREATE_PATH = '/vectorize/metadata-index/create';
export const METADATA_LIST_PATH = '/vectorize/metadata-index/list';
export const METADATA_DELETE_PATH = '/vectorize/metadata-index/delete';
export const MAX_EXACT_SCAN_ROWS = 10_000;

const CONFIG_TABLE = 'vectorize_config';
const VECTOR_TABLE = 'vectorize_vectors';
const MUTATION_TABLE = 'vectorize_mutations';
const METADATA_INDEX_TABLE = 'vectorize_metadata_indexes';
const MAX_BATCH = 1000;
const MAX_METADATA_BYTES = 10 * 1024;
const MAX_NAMESPACES = 1000;
const MAX_METADATA_INDEXES = 10;
const encoder = new TextEncoder();

/** Routes an optional HTTP endpoint to the configured named Vectorize object. */
async function fetch(request, env) {
  const failure = authorize(request, env);
  if (failure) return failure;
  const indexName = env.VECTORIZE_INDEX_NAME;
  const namespace = env.VECTORIZE_DO;
  if (typeof indexName !== 'string' || indexName.trim() === '' || !namespace
      || typeof namespace.idFromName !== 'function' || typeof namespace.get !== 'function') {
    return jsonResponse({ error: 'Vectorize binding is not configured' }, 500);
  }
  const id = namespace.idFromName(indexName);
  return namespace.get(id).fetch(request);
}

/** One serialized Turso-backed Vectorize index. */
export class Vectorize extends DurableObject {
  /** Validates immutable configuration and creates the product schema. */
  constructor(ctx, env) {
    super(ctx, env);
    this.#config = validateConfiguration(env);
    this.#ready = ctx.blockConcurrencyWhile(async () => {
      await createTables(ctx, this.#config);
      await installOrCheckConfiguration(ctx, this.#config);
    });
  }

  #config;
  #ready;

  /** Dispatches the private binding and resource-management routes. */
  async fetch(request) {
    await this.#ready;
    const url = new URL(request.url);
    if (request.method.toUpperCase() !== 'POST') return jsonResponse({ error: 'method not allowed' }, 405);
    try {
      if (url.pathname === INSERT_PATH) return await this.#mutate(request, 'insert');
      if (url.pathname === UPSERT_PATH) return await this.#mutate(request, 'upsert');
      if (url.pathname === QUERY_PATH) return await this.#query(request);
      if (url.pathname === QUERY_BY_ID_PATH) return await this.#queryById(request);
      if (url.pathname === GET_BY_IDS_PATH) return await this.#getByIds(request);
      if (url.pathname === DELETE_BY_IDS_PATH) return await this.#deleteByIds(request);
      if (url.pathname === DESCRIBE_PATH) return await this.#describe();
      if (url.pathname === METADATA_CREATE_PATH) return await this.#createMetadataIndex(request);
      if (url.pathname === METADATA_LIST_PATH) return await this.#listMetadataIndexes();
      if (url.pathname === METADATA_DELETE_PATH) return await this.#deleteMetadataIndex(request);
      return jsonResponse({ error: 'not found' }, 404);
    } catch (error) {
      return jsonResponse({ error: error.message }, 400);
    }
  }

  /** Inserts or fully replaces one validated vector batch. */
  async #mutate(request, operation) {
    const payload = await requestJson(request);
    const vectors = validateVectors(payload.vectors, this.#config.dimensions);
    await validateNamespaceBudget(this.ctx, vectors);
    const mutationId = await stableMutationId(operation, payload);
    if (await mutationExists(this.ctx, mutationId)) return jsonResponse({ mutationId });
    for (const vector of vectors) {
      const valuesJson = JSON.stringify(vector.values);
      const metadataJson = vector.metadata === undefined ? null : JSON.stringify(vector.metadata);
      if (operation === 'insert') {
        await execute(
          this.ctx,
          `INSERT INTO ${VECTOR_TABLE} (external_id, embedding, namespace, metadata_json, mutation_id)
           VALUES (?, vector32(?), ?, ?, ?) ON CONFLICT(external_id) DO NOTHING`,
          vector.id,
          valuesJson,
          vector.namespace ?? null,
          metadataJson,
          mutationId,
        );
      } else {
        await execute(
          this.ctx,
          `INSERT INTO ${VECTOR_TABLE} (external_id, embedding, namespace, metadata_json, mutation_id)
           VALUES (?, vector32(?), ?, ?, ?)
           ON CONFLICT(external_id) DO UPDATE SET embedding = excluded.embedding,
             namespace = excluded.namespace, metadata_json = excluded.metadata_json,
             mutation_id = excluded.mutation_id`,
          vector.id,
          valuesJson,
          vector.namespace ?? null,
          metadataJson,
          mutationId,
        );
      }
    }
    await recordMutation(this.ctx, mutationId, operation);
    return jsonResponse({ mutationId });
  }

  /** Executes a bounded nearest-neighbor query over native Turso vectors. */
  async #query(request) {
    const payload = await requestJson(request);
    const vector = validateValues(payload.vector, this.#config.dimensions);
    return jsonResponse(await queryVector(this.ctx, this.#config, vector, payload));
  }

  /** Resolves one stored vector and executes the same bounded query path. */
  async #queryById(request) {
    const payload = await requestJson(request);
    const id = validateId(payload.id);
    const rows = await execute(
      this.ctx,
      `SELECT vector_extract(embedding) AS embedding_json FROM ${VECTOR_TABLE} WHERE external_id = ?`,
      id,
    );
    if (rows.length !== 1) throw new Error(`Vectorize vector does not exist: ${id}`);
    return jsonResponse(await queryVector(this.ctx, this.#config, JSON.parse(rows[0].embedding_json), payload));
  }

  /** Returns complete stored vectors in caller id order. */
  async #getByIds(request) {
    const payload = await requestJson(request);
    const ids = validateIds(payload.ids);
    const placeholders = ids.map(() => '?').join(',');
    const rows = await execute(
      this.ctx,
      `SELECT external_id AS id, vector_extract(embedding) AS embedding_json,
              namespace, metadata_json FROM ${VECTOR_TABLE}
       WHERE external_id IN (${placeholders})`,
      ...ids,
    );
    const byId = new Map(rows.map((row) => [row.id, storedVector(row)]));
    return jsonResponse(ids.flatMap((id) => byId.has(id) ? [byId.get(id)] : []));
  }

  /** Deletes the requested ids and records one durable mutation receipt. */
  async #deleteByIds(request) {
    const payload = await requestJson(request);
    const ids = validateIds(payload.ids);
    const mutationId = await stableMutationId('delete', { ids });
    if (await mutationExists(this.ctx, mutationId)) return jsonResponse({ mutationId });
    const placeholders = ids.map(() => '?').join(',');
    await execute(this.ctx, `DELETE FROM ${VECTOR_TABLE} WHERE external_id IN (${placeholders})`, ...ids);
    await recordMutation(this.ctx, mutationId, 'delete');
    return jsonResponse({ mutationId });
  }

  /** Describes immutable configuration and the latest durable mutation. */
  async #describe() {
    const count = await execute(this.ctx, `SELECT COUNT(*) AS count FROM ${VECTOR_TABLE}`);
    const mutation = await execute(
      this.ctx,
      `SELECT mutation_id FROM ${MUTATION_TABLE} ORDER BY sequence DESC LIMIT 1`,
    );
    return jsonResponse({
      dimensions: this.#config.dimensions,
      metric: this.#config.metric,
      vectorCount: Number(count[0].count),
      ...(mutation.length === 0 ? {} : { processedUpToMutation: mutation[0].mutation_id }),
    });
  }

  /** Declares one filterable metadata property without rewriting prior vectors. */
  async #createMetadataIndex(request) {
    const payload = await requestJson(request);
    const propertyName = validateMetadataProperty(payload.propertyName);
    const indexType = validateMetadataType(payload.indexType);
    const existing = await execute(
      this.ctx,
      `SELECT index_type FROM ${METADATA_INDEX_TABLE} WHERE property_name = ?`,
      propertyName,
    );
    if (existing.length > 0 && existing[0].index_type !== indexType) {
      throw new Error('Vectorize metadata index configuration is immutable');
    }
    const count = await execute(this.ctx, `SELECT COUNT(*) AS count FROM ${METADATA_INDEX_TABLE}`);
    if (existing.length === 0 && Number(count[0].count) >= MAX_METADATA_INDEXES) {
      throw new Error(`Vectorize supports at most ${MAX_METADATA_INDEXES} metadata indexes`);
    }
    const mutationId = await stableMutationId('metadata-index-create', { propertyName, indexType });
    await execute(
      this.ctx,
      `INSERT INTO ${METADATA_INDEX_TABLE} (property_name, index_type)
       VALUES (?, ?) ON CONFLICT(property_name) DO NOTHING`,
      propertyName,
      indexType,
    );
    if (!(await mutationExists(this.ctx, mutationId))) {
      await recordMutation(this.ctx, mutationId, 'metadata-index-create');
    }
    return jsonResponse({ mutationId });
  }

  /** Lists declared metadata indexes in deterministic property order. */
  async #listMetadataIndexes() {
    const rows = await execute(
      this.ctx,
      `SELECT property_name, index_type FROM ${METADATA_INDEX_TABLE} ORDER BY property_name`,
    );
    return jsonResponse({
      metadataIndexes: rows.map((row) => ({ propertyName: row.property_name, indexType: row.index_type })),
    });
  }

  /** Deletes one metadata index declaration idempotently. */
  async #deleteMetadataIndex(request) {
    const payload = await requestJson(request);
    const propertyName = validateMetadataProperty(payload.propertyName);
    const mutationId = await stableMutationId('metadata-index-delete', { propertyName });
    if (!(await mutationExists(this.ctx, mutationId))) {
      await execute(this.ctx, `DELETE FROM ${METADATA_INDEX_TABLE} WHERE property_name = ?`, propertyName);
      await recordMutation(this.ctx, mutationId, 'metadata-index-delete');
    }
    return jsonResponse({ mutationId });
  }
}

/** Validates the creation-only index configuration. */
function validateConfiguration(env) {
  const indexName = env.VECTORIZE_INDEX_NAME;
  const dimensions = Number(env.VECTORIZE_DIMENSIONS);
  const metric = env.VECTORIZE_METRIC;
  if (typeof indexName !== 'string' || indexName.trim() === '') throw new Error('VECTORIZE_INDEX_NAME is required');
  if (!Number.isInteger(dimensions) || dimensions < 1 || dimensions > 1536) {
    throw new Error('VECTORIZE_DIMENSIONS must be an integer between 1 and 1536');
  }
  if (!['cosine', 'euclidean', 'dot-product'].includes(metric)) {
    throw new Error('VECTORIZE_METRIC must be cosine, euclidean, or dot-product');
  }
  return { indexName, dimensions, metric };
}

/** Creates tables whose vector affinity includes the immutable dimensions. */
async function createTables(ctx, config) {
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${CONFIG_TABLE} (
    id INTEGER PRIMARY KEY CHECK (id = 1), index_name TEXT NOT NULL,
    dimensions INTEGER NOT NULL, metric TEXT NOT NULL)`);
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${VECTOR_TABLE} (
    rowid INTEGER PRIMARY KEY AUTOINCREMENT, external_id TEXT NOT NULL UNIQUE,
    embedding F32_BLOB(${config.dimensions}) NOT NULL, namespace TEXT,
    metadata_json TEXT, mutation_id TEXT NOT NULL)`);
  await execute(ctx, `CREATE INDEX IF NOT EXISTS vectorize_namespace_idx ON ${VECTOR_TABLE}(namespace)`);
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${MUTATION_TABLE} (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT, mutation_id TEXT NOT NULL UNIQUE,
    operation TEXT NOT NULL)`);
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${METADATA_INDEX_TABLE} (
    property_name TEXT PRIMARY KEY, index_type TEXT NOT NULL)`);
}

/** Installs first activation configuration or rejects a changed activation. */
async function installOrCheckConfiguration(ctx, config) {
  const rows = await execute(ctx, `SELECT index_name, dimensions, metric FROM ${CONFIG_TABLE} WHERE id = 1`);
  if (rows.length === 0) {
    await execute(
      ctx,
      `INSERT INTO ${CONFIG_TABLE} (id, index_name, dimensions, metric) VALUES (1, ?, ?, ?)`,
      config.indexName,
      config.dimensions,
      config.metric,
    );
    return;
  }
  const stored = rows[0];
  if (stored.index_name !== config.indexName || Number(stored.dimensions) !== config.dimensions
      || stored.metric !== config.metric) {
    throw new Error('Vectorize index configuration is immutable');
  }
}

/** Executes one bounded exact Turso vector query with pre-filtering. */
async function queryVector(ctx, config, vector, payload) {
  const options = validateQueryOptions(payload);
  const filter = await compileFilter(ctx, options.filter);
  const where = [];
  const whereBindings = [];
  if (options.namespace !== undefined) {
    where.push('namespace = ?');
    whereBindings.push(options.namespace);
  }
  if (filter.sql !== '') {
    where.push(filter.sql);
    whereBindings.push(...filter.bindings);
  }
  const whereSql = where.length === 0 ? '' : `WHERE ${where.join(' AND ')}`;
  const eligible = await execute(
    ctx,
    `SELECT COUNT(*) AS count FROM ${VECTOR_TABLE} ${whereSql}`,
    ...whereBindings,
  );
  if (Number(eligible[0].count) > MAX_EXACT_SCAN_ROWS) {
    throw new Error(`Vectorize exact-search ceiling is ${MAX_EXACT_SCAN_ROWS} eligible vectors`);
  }
  const distanceFunction = {
    cosine: 'vector_distance_cos',
    euclidean: 'vector_distance_l2',
    'dot-product': 'vector_distance_dot',
  }[config.metric];
  const rows = await execute(
    ctx,
    `SELECT external_id AS id, namespace, metadata_json,
            vector_extract(embedding) AS embedding_json,
            ${distanceFunction}(embedding, vector32(?)) AS distance
     FROM ${VECTOR_TABLE} ${whereSql}
     ORDER BY distance ASC, external_id ASC LIMIT ?`,
    JSON.stringify(vector),
    ...whereBindings,
    options.topK,
  );
  const indexedProperties = options.returnMetadata === 'indexed'
    ? await metadataIndexNames(ctx)
    : undefined;
  const matches = rows.map((row) => {
    const match = {
      id: row.id,
      score: config.metric === 'cosine'
        ? Math.max(-1, Math.min(1, 1 - Number(row.distance)))
        : Number(row.distance),
    };
    if (row.namespace !== null && row.namespace !== undefined) match.namespace = row.namespace;
    if (options.returnValues) match.values = JSON.parse(row.embedding_json);
    if (options.returnMetadata !== 'none' && row.metadata_json !== null && row.metadata_json !== undefined) {
      const metadata = JSON.parse(row.metadata_json);
      match.metadata = indexedProperties === undefined
        ? metadata
        : Object.fromEntries(Object.entries(metadata).filter(([key]) => indexedProperties.has(key)));
    }
    return match;
  });
  return { count: matches.length, matches };
}

/** Validates query options and current topK ceilings. */
function validateQueryOptions(payload) {
  const topK = payload.topK === undefined ? 5 : Number(payload.topK);
  const returnValues = payload.returnValues === true;
  const returnMetadata = payload.returnMetadata === undefined ? 'none' : payload.returnMetadata;
  if (!Number.isInteger(topK) || topK < 1 || topK > 100) throw new Error('topK must be an integer between 1 and 100');
  if (!['none', 'indexed', 'all'].includes(returnMetadata)) throw new Error('returnMetadata must be none, indexed, or all');
  if ((returnValues || returnMetadata === 'all') && topK > 50) {
    throw new Error('topK must not exceed 50 when values or all metadata are returned');
  }
  const namespace = payload.namespace === undefined ? undefined : validateNamespace(payload.namespace);
  return { topK, returnValues, returnMetadata, namespace, filter: payload.filter };
}

/** Compiles supported metadata operators to parameterized JSON SQL. */
async function compileFilter(ctx, filter) {
  if (filter === undefined) return { sql: '', bindings: [] };
  if (!filter || typeof filter !== 'object' || Array.isArray(filter)) throw new Error('Vectorize filter must be an object');
  const declarations = await metadataIndexes(ctx);
  const clauses = [];
  const bindings = [];
  for (const [property, expression] of Object.entries(filter)) {
    validateMetadataProperty(property);
    if (!declarations.has(property)) throw new Error(`metadata property is not indexed: ${property}`);
    const path = `$."${property}"`;
    const operations = expression && typeof expression === 'object' && !Array.isArray(expression)
      ? Object.entries(expression)
      : [['$eq', expression]];
    if (operations.length !== 1) throw new Error('each Vectorize metadata filter property requires one operator');
    const [operator, value] = operations[0];
    const sqlOperator = { $eq: '=', $ne: '!=', $lt: '<', $lte: '<=', $gt: '>', $gte: '>=' }[operator];
    if (sqlOperator !== undefined) {
      clauses.push(`json_extract(metadata_json, ?) ${sqlOperator} ?`);
      bindings.push(path, scalarFilterValue(value));
      continue;
    }
    if (operator === '$in' || operator === '$nin') {
      if (!Array.isArray(value) || value.length === 0 || value.length > 100) {
        throw new Error(`${operator} requires between 1 and 100 scalar values`);
      }
      clauses.push(`json_extract(metadata_json, ?) ${operator === '$in' ? 'IN' : 'NOT IN'} (${value.map(() => '?').join(',')})`);
      bindings.push(path, ...value.map(scalarFilterValue));
      continue;
    }
    throw new Error(`unknown Vectorize metadata filter operator: ${operator}`);
  }
  return { sql: clauses.join(' AND '), bindings };
}

/** Returns one scalar supported by metadata comparison bindings. */
function scalarFilterValue(value) {
  if (typeof value === 'string' || typeof value === 'boolean' || (typeof value === 'number' && Number.isFinite(value))) return value;
  throw new Error('Vectorize metadata filter values must be finite scalar values');
}

/** Validates an entire vector batch before issuing its first SQL mutation. */
function validateVectors(vectors, dimensions) {
  if (!Array.isArray(vectors) || vectors.length < 1 || vectors.length > MAX_BATCH) {
    throw new Error(`vectors must contain between 1 and ${MAX_BATCH} entries`);
  }
  return vectors.map((vector) => {
    if (!vector || typeof vector !== 'object' || Array.isArray(vector)) throw new Error('each vector must be an object');
    const result = { id: validateId(vector.id), values: validateValues(vector.values, dimensions) };
    if (vector.namespace !== undefined) result.namespace = validateNamespace(vector.namespace);
    if (vector.metadata !== undefined) {
      assertJsonValue(vector.metadata, new WeakSet());
      if (encoder.encode(JSON.stringify(vector.metadata)).byteLength > MAX_METADATA_BYTES) {
        throw new Error(`Vectorize metadata exceeds ${MAX_METADATA_BYTES} bytes`);
      }
      result.metadata = vector.metadata;
    }
    return result;
  });
}

/** Validates one vector against immutable dimensions. */
function validateValues(values, dimensions) {
  if (!Array.isArray(values) || values.length !== dimensions
      || values.some((value) => typeof value !== 'number' || !Number.isFinite(value))) {
    throw new Error(`Vectorize values must contain exactly ${dimensions} finite numbers`);
  }
  return values.map(Math.fround);
}

/** Validates one external id. */
function validateId(id) {
  if (typeof id !== 'string' || id.length === 0 || encoder.encode(id).byteLength > 64) {
    throw new Error('Vectorize id must be a non-empty string of at most 64 bytes');
  }
  return id;
}

/** Validates a bounded id list. */
function validateIds(ids) {
  if (!Array.isArray(ids) || ids.length < 1 || ids.length > 1000) {
    throw new Error('ids must contain between 1 and 1000 identifiers');
  }
  return ids.map(validateId);
}

/** Validates one namespace. */
function validateNamespace(namespace) {
  if (typeof namespace !== 'string' || namespace.length === 0 || encoder.encode(namespace).byteLength > 64) {
    throw new Error('Vectorize namespace must be a non-empty string of at most 64 bytes');
  }
  return namespace;
}

/** Enforces the namespace ceiling before writing a batch. */
async function validateNamespaceBudget(ctx, vectors) {
  const incoming = new Set(vectors.flatMap((vector) => vector.namespace === undefined ? [] : [vector.namespace]));
  if (incoming.size === 0) return;
  const existing = await execute(ctx, `SELECT DISTINCT namespace FROM ${VECTOR_TABLE} WHERE namespace IS NOT NULL`);
  const namespaces = new Set(existing.map((row) => row.namespace));
  for (const namespace of incoming) namespaces.add(namespace);
  if (namespaces.size > MAX_NAMESPACES) throw new Error(`Vectorize supports at most ${MAX_NAMESPACES} namespaces`);
}

/** Validates Cloudflare metadata property naming constraints. */
function validateMetadataProperty(property) {
  if (typeof property !== 'string' || property.length === 0 || property.includes('.')
      || property.includes('"') || property.startsWith('$') || encoder.encode(property).byteLength > 64) {
    throw new Error('invalid Vectorize metadata property name');
  }
  return property;
}

/** Validates one metadata index type. */
function validateMetadataType(type) {
  if (!['string', 'number', 'boolean'].includes(type)) throw new Error('metadata index type must be string, number, or boolean');
  return type;
}

/** Returns durable metadata index declarations. */
async function metadataIndexes(ctx) {
  const rows = await execute(ctx, `SELECT property_name, index_type FROM ${METADATA_INDEX_TABLE}`);
  return new Map(rows.map((row) => [row.property_name, row.index_type]));
}

/** Returns the names of all durable metadata index declarations. */
async function metadataIndexNames(ctx) {
  return new Set((await metadataIndexes(ctx)).keys());
}

/** Converts one stored SQL row to the Cloudflare getByIds shape. */
function storedVector(row) {
  return {
    id: row.id,
    values: JSON.parse(row.embedding_json),
    ...(row.namespace === null || row.namespace === undefined ? {} : { namespace: row.namespace }),
    ...(row.metadata_json === null || row.metadata_json === undefined ? {} : { metadata: JSON.parse(row.metadata_json) }),
  };
}

/** Computes a replay-stable UUID-shaped mutation identity from canonical input. */
async function stableMutationId(operation, payload) {
  const bytes = encoder.encode(JSON.stringify(canonicalJson([operation, payload])));
  const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', bytes));
  const hex = Array.from(digest, (value) => value.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
}

/** Sorts object keys so semantically identical retries keep one receipt id. */
function canonicalJson(value) {
  if (Array.isArray(value)) return value.map(canonicalJson);
  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.keys(value).sort().map((key) => [key, canonicalJson(value[key])]),
    );
  }
  return value;
}

/** Checks whether the same deterministic mutation already committed. */
async function mutationExists(ctx, mutationId) {
  const rows = await execute(ctx, `SELECT 1 AS present FROM ${MUTATION_TABLE} WHERE mutation_id = ?`, mutationId);
  return rows.length > 0;
}

/** Records one mutation after all of its state changes have been staged. */
async function recordMutation(ctx, mutationId, operation) {
  await execute(
    ctx,
    `INSERT INTO ${MUTATION_TABLE} (mutation_id, operation) VALUES (?, ?)
     ON CONFLICT(mutation_id) DO NOTHING`,
    mutationId,
    operation,
  );
}

/** Parses one JSON object request body. */
async function requestJson(request) {
  const bytes = new Uint8Array(await request.arrayBuffer());
  if (bytes.byteLength > 12 * 1024 * 1024) throw new Error('Vectorize request exceeds 12 MiB');
  const value = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Vectorize request body must be a JSON object');
  return value;
}

/** Rejects JSON values that would be silently rewritten. */
function assertJsonValue(value, ancestors) {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return;
  if (typeof value === 'number') {
    if (Number.isFinite(value)) return;
    throw new Error('metadata numbers must be finite');
  }
  if (!value || typeof value !== 'object' || ancestors.has(value)) throw new Error('metadata must be acyclic JSON');
  ancestors.add(value);
  if (Array.isArray(value)) {
    for (const entry of value) assertJsonValue(entry, ancestors);
  } else {
    for (const [key, entry] of Object.entries(value)) {
      validateMetadataProperty(key);
      assertJsonValue(entry, ancestors);
    }
  }
  ancestors.delete(value);
}

/** Executes parameterized Durable Object SQL and materializes bounded rows. */
async function execute(ctx, statement, ...bindings) {
  const cursor = await ctx.storage.sql.exec(statement, ...bindings);
  return cursor.toArray();
}

/** Produces a JSON response. */
function jsonResponse(value, status = 200) {
  return Response.json(value, { status });
}

/** Applies optional bearer authentication to the HTTP Worker endpoint. */
function authorize(request, env) {
  if (env.VECTORIZE_AUTH_TOKEN === undefined) return undefined;
  if (typeof env.VECTORIZE_AUTH_TOKEN !== 'string' || env.VECTORIZE_AUTH_TOKEN.length === 0) {
    return jsonResponse({ error: 'Vectorize authentication is misconfigured' }, 500);
  }
  return request.headers.get('authorization') === `Bearer ${env.VECTORIZE_AUTH_TOKEN}`
    ? undefined
    : jsonResponse({ error: 'unauthorized' }, 401);
}

export default { fetch };
