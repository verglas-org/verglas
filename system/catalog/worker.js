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
export const MAX_RUNTIME_RESPONSE_BYTES = 8 * 1024 * 1024;
export const MAX_REST_REQUEST_BYTES = 8 * 1024 * 1024;
export const MIN_ROLL_INTERVAL_SECONDS = 60;
export const MAX_ROLL_INTERVAL_SECONDS = 24 * 60 * 60;
export const MAX_ROLL_SIZE_BYTES = 512 * 1024 * 1024;

const CONFIG_TABLE = 'catalog_config';
const LEDGER_TABLE = 'catalog_ledger';
const REST_LEDGER_TABLE = 'catalog_rest_commits';
const NAMESPACE_TABLE = 'catalog_namespaces';
const TABLE_TABLE = 'catalog_tables';
const SHA256_HEX = /^[a-f0-9]{64}$/u;
const NAME = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;
const LOCATION_MAX_LENGTH = 4096;
const NAMESPACE_SEPARATOR = '\u001f';
const ICEBERG_REST_ENDPOINTS = Object.freeze([
  'GET /v1/config',
  'GET /v1/{prefix}/namespaces',
  'POST /v1/{prefix}/namespaces',
  'GET /v1/{prefix}/namespaces/{namespace}',
  'DELETE /v1/{prefix}/namespaces/{namespace}',
  'POST /v1/{prefix}/namespaces/{namespace}/properties',
  'GET /v1/{prefix}/namespaces/{namespace}/tables',
  'POST /v1/{prefix}/namespaces/{namespace}/tables',
  'POST /v1/{prefix}/namespaces/{namespace}/register',
  'GET /v1/{prefix}/namespaces/{namespace}/tables/{table}',
  'HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}',
  'POST /v1/{prefix}/namespaces/{namespace}/tables/{table}',
  'DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}',
  'POST /v1/{prefix}/tables/rename',
]);
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
  const headers = new Headers(request.headers);
  const forwardedHost = headers.get('x-forwarded-host');
  headers.set('x-verglas-public-origin', forwardedHost ? `https://${forwardedHost}` : url.origin);
  const internal = new Request(target, {
    method,
    headers,
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
   * one idempotent runtime proposal before inserting the SQLite receipt.
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

      const tableRows = await execute(
        this.ctx,
        `SELECT metadata_location FROM ${TABLE_TABLE} WHERE namespace = ? AND name = ?`,
        this.#config.namespace,
        this.#config.table,
      );
      const currentMetadataLocation = tableRows[0]?.metadata_location === undefined
        || tableRows[0]?.metadata_location === null
        ? null
        : String(tableRows[0].metadata_location);
      const runtimeProposal = await requestSinkProposal(
        this.env,
        this.#config,
        commit,
        currentMetadataLocation,
      );
      const receipt = {
        committed: true,
        batch_id: runtimeProposal.batch_id,
        file_id: runtimeProposal.file_id,
        snapshot_id: runtimeProposal.snapshot_id,
        metadata_location: runtimeProposal.metadata_location,
        rows_committed: runtimeProposal.rows_committed,
      };
      await persistCatalogCommit(this.ctx, this.#config, commit, runtimeProposal, receipt);
      return jsonResponse(receipt);
    } catch (error) {
      if (error instanceof RequestError) return errorResponse(error, error.status);
      if (error instanceof RuntimeProposalError) return errorResponse(error, 502);
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
        return jsonResponse({
          defaults: { warehouse: this.#config.warehouse },
          overrides: s3ClientOverrides(new URL(request.headers.get('x-verglas-public-origin') ?? url.origin)),
          endpoints: ICEBERG_REST_ENDPOINTS,
        });
      }
      if (url.pathname === '/v1/tables/rename' && method === 'POST') {
        return await this.#renameTable(request);
      }
      if (url.pathname === '/v1/namespaces') {
        if (method === 'GET') return await this.#listNamespaces(url);
        if (method === 'POST') return await this.#createNamespace(request);
      }
      const propertiesMatch = /^\/v1\/namespaces\/([^/]+)\/properties$/u.exec(url.pathname);
      if (propertiesMatch && method === 'POST') {
        return await this.#updateNamespaceProperties(propertiesMatch[1], request);
      }
      const namespaceMatch = /^\/v1\/namespaces\/([^/]+)$/u.exec(url.pathname);
      if (namespaceMatch) {
        if (method === 'GET') return await this.#loadNamespace(namespaceMatch[1]);
        if (method === 'DELETE') return await this.#dropNamespace(namespaceMatch[1]);
      }
      const registerMatch = /^\/v1\/namespaces\/([^/]+)\/register$/u.exec(url.pathname);
      if (registerMatch && method === 'POST') {
        return await this.#registerTable(registerMatch[1], request);
      }
      const tableMatch = /^\/v1\/namespaces\/([^/]+)\/tables\/([^/]+)$/u.exec(url.pathname);
      if (tableMatch) {
        if (method === 'POST') return await this.#commitTable(tableMatch[1], tableMatch[2], request);
        if (method === 'DELETE') return await this.#dropTable(tableMatch[1], tableMatch[2], url);
        if (method === 'GET' || method === 'HEAD') {
          const namespace = namespaceFromPath(tableMatch[1]);
          const name = tableNameFromPath(tableMatch[2]);
          const rows = await execute(this.ctx, `SELECT metadata_location, metadata_json FROM ${TABLE_TABLE} WHERE namespace = ? AND name = ?`, namespace.key, name);
          if (!rows[0]) throw new RequestError('table does not exist', 404, 'NoSuchTableException');
          const response = jsonResponse({
            'metadata-location': rows[0].metadata_location === null ? null : String(rows[0].metadata_location),
            metadata: JSON.parse(String(rows[0].metadata_json)),
            config: {},
          });
          return method === 'HEAD' ? new Response(null, { status: 204, headers: response.headers }) : response;
        }
      }
      const tablesMatch = /^\/v1\/namespaces\/([^/]+)\/tables$/u.exec(url.pathname);
      if (tablesMatch) {
        if (method === 'POST') return await this.#createTable(tablesMatch[1], request);
        if (method === 'GET') return await this.#listTables(tablesMatch[1]);
      }
      throw new RequestError('endpoint is not supported', 406, 'UnsupportedOperationException');
    } catch (error) {
      return icebergErrorResponse(error);
    }
  }

  /** Lists direct child namespaces, optionally under the standard multipart parent. */
  async #listNamespaces(url) {
    const parentValue = url.searchParams.get('parent');
    const parent = parentValue === null ? [] : namespaceFromPath(parentValue).segments;
    const rows = await execute(this.ctx, `SELECT name FROM ${NAMESPACE_TABLE} ORDER BY name`);
    const namespaces = rows
      .map((row) => namespaceSegmentsFromKey(String(row.name)))
      .filter((segments) => segments.length === parent.length + 1
        && parent.every((segment, index) => segments[index] === segment));
    return jsonResponse({ namespaces });
  }

  /** Creates one standard multipart Iceberg namespace in SQLite. */
  async #createNamespace(request) {
    const body = await readRestJson(request);
    const namespace = namespaceValue(body.namespace);
    const properties = stringMap(body.properties ?? {}, 'properties');
    const existing = await execute(this.ctx, `SELECT name FROM ${NAMESPACE_TABLE} WHERE name = ?`, namespace.key);
    if (existing[0]) throw new RequestError('namespace already exists', 409, 'AlreadyExistsException');
    await execute(this.ctx, `INSERT INTO ${NAMESPACE_TABLE} (name, properties_json) VALUES (?, ?)`, namespace.key, canonicalJson(properties));
    return jsonResponse({ namespace: namespace.segments, properties });
  }

  /** Loads one namespace and its string properties from SQLite. */
  async #loadNamespace(encodedNamespace) {
    const namespace = namespaceFromPath(encodedNamespace);
    const rows = await execute(this.ctx, `SELECT properties_json FROM ${NAMESPACE_TABLE} WHERE name = ?`, namespace.key);
    if (!rows[0]) throw new RequestError('namespace does not exist', 404, 'NoSuchNamespaceException');
    return jsonResponse({ namespace: namespace.segments, properties: JSON.parse(String(rows[0].properties_json)) });
  }

  /** Applies the Iceberg namespace property update contract in one serialized event. */
  async #updateNamespaceProperties(encodedNamespace, request) {
    const namespace = namespaceFromPath(encodedNamespace);
    const rows = await execute(this.ctx, `SELECT properties_json FROM ${NAMESPACE_TABLE} WHERE name = ?`, namespace.key);
    if (!rows[0]) throw new RequestError('namespace does not exist', 404, 'NoSuchNamespaceException');
    const body = await readRestJson(request);
    const removals = uniqueStringArray(body.removals ?? [], 'removals');
    const updates = stringMap(body.updates ?? {}, 'updates');
    const properties = JSON.parse(String(rows[0].properties_json));
    const removed = [];
    const missing = [];
    for (const key of removals) {
      if (Object.hasOwn(properties, key)) {
        delete properties[key];
        removed.push(key);
      } else {
        missing.push(key);
      }
    }
    for (const [key, value] of Object.entries(updates)) properties[key] = value;
    await execute(this.ctx, `UPDATE ${NAMESPACE_TABLE} SET properties_json = ? WHERE name = ?`, canonicalJson(properties), namespace.key);
    return jsonResponse({ removed, updated: Object.keys(updates), missing });
  }

  /** Publishes initial Iceberg metadata through the host and stores its pointer in SQLite. */
  async #createTable(encodedNamespace, request) {
    const namespace = namespaceFromPath(encodedNamespace);
    const namespaces = await execute(this.ctx, `SELECT name FROM ${NAMESPACE_TABLE} WHERE name = ?`, namespace.key);
    if (!namespaces[0]) throw new RequestError('namespace does not exist', 404, 'NoSuchNamespaceException');
    const body = await readRestJson(request);
    const name = icebergIdentifierPart(body.name, 'table name');
    plainObject(body.schema, 'schema');
    if (body['stage-create'] === true) {
      throw new RequestError('staged create is not supported', 406, 'UnsupportedOperationException');
    }
    const existing = await execute(this.ctx, `SELECT name FROM ${TABLE_TABLE} WHERE namespace = ? AND name = ?`, namespace.key, name);
    if (existing[0]) throw new RequestError('table already exists', 409, 'AlreadyExistsException');
    const publication = await callIcebergCapability(this.env, {
      operation: 'create-table',
      warehouse: this.#config.warehouse,
      namespace: namespace.segments,
      request: body,
    });
    validateTablePublication(publication);
    await execute(
      this.ctx,
      `INSERT INTO ${TABLE_TABLE} (namespace, name, metadata_location, metadata_json) VALUES (?, ?, ?, ?)`,
      namespace.key,
      name,
      publication['metadata-location'],
      canonicalJson(publication.metadata),
    );
    return jsonResponse({
      'metadata-location': publication['metadata-location'],
      metadata: publication.metadata,
      config: {},
    });
  }

  /** Lists all table identifiers in one namespace from SQLite. */
  async #listTables(encodedNamespace) {
    const namespace = namespaceFromPath(encodedNamespace);
    const namespaces = await execute(this.ctx, `SELECT name FROM ${NAMESPACE_TABLE} WHERE name = ?`, namespace.key);
    if (!namespaces[0]) throw new RequestError('namespace does not exist', 404, 'NoSuchNamespaceException');
    const rows = await execute(this.ctx, `SELECT name FROM ${TABLE_TABLE} WHERE namespace = ? ORDER BY name`, namespace.key);
    return jsonResponse({ identifiers: rows.map((row) => ({ namespace: namespace.segments, name: String(row.name) })) });
  }

  /** Registers existing immutable metadata as a new SQLite table head. */
  async #registerTable(encodedNamespace, request) {
    const namespace = namespaceFromPath(encodedNamespace);
    const namespaces = await execute(this.ctx, `SELECT name FROM ${NAMESPACE_TABLE} WHERE name = ?`, namespace.key);
    if (!namespaces[0]) throw new RequestError('namespace does not exist', 404, 'NoSuchNamespaceException');
    const body = await readRestJson(request);
    const name = icebergIdentifierPart(body.name, 'table name');
    const metadataLocation = locationString(body['metadata-location'], 'metadata-location');
    const existing = await execute(this.ctx, `SELECT name FROM ${TABLE_TABLE} WHERE namespace = ? AND name = ?`, namespace.key, name);
    if (existing[0] && body.overwrite !== true) throw new RequestError('table already exists', 409, 'AlreadyExistsException');
    const publication = await callIcebergCapability(this.env, {
      operation: 'register-table',
      metadata_location: metadataLocation,
    });
    validateTablePublication(publication);
    await execute(
      this.ctx,
      `INSERT INTO ${TABLE_TABLE} (namespace, name, metadata_location, metadata_json) VALUES (?, ?, ?, ?)
       ON CONFLICT(namespace, name) DO UPDATE SET metadata_location = excluded.metadata_location, metadata_json = excluded.metadata_json`,
      namespace.key,
      name,
      publication['metadata-location'],
      canonicalJson(publication.metadata),
    );
    return jsonResponse({
      'metadata-location': publication['metadata-location'],
      metadata: publication.metadata,
      config: {},
    });
  }

  /** Applies a standard Iceberg table commit and advances only the SQLite head. */
  async #commitTable(encodedNamespace, encodedName, request) {
    const namespace = namespaceFromPath(encodedNamespace);
    const name = tableNameFromPath(encodedName);
    const rows = await execute(
      this.ctx,
      `SELECT metadata_location FROM ${TABLE_TABLE} WHERE namespace = ? AND name = ?`,
      namespace.key,
      name,
    );
    if (!rows[0]) throw new RequestError('table does not exist', 404, 'NoSuchTableException');
    const document = await readRestDocument(request);
    const body = document.value;
    const identifier = plainObject(body.identifier, 'identifier');
    const requestedNamespace = namespaceValue(identifier.namespace);
    if (requestedNamespace.key !== namespace.key || identifier.name !== name) {
      throw new RequestError('table identifier does not match the request path');
    }
    if (!Array.isArray(body.requirements) || !Array.isArray(body.updates)) {
      throw new RequestError('requirements and updates must be arrays');
    }
    const idempotencyKey = request.headers.get('idempotency-key');
    const requestDigest = await digestHex(document.text);
    if (idempotencyKey !== null) {
      if (!/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(idempotencyKey)) {
        throw new RequestError('Idempotency-Key must be a UUIDv7');
      }
      const prior = await execute(
        this.ctx,
        `SELECT request_digest, response_json FROM ${REST_LEDGER_TABLE} WHERE idempotency_key = ?`,
        idempotencyKey,
      );
      if (prior[0]) {
        if (String(prior[0].request_digest) !== requestDigest) {
          throw new RequestError('Idempotency-Key was reused for a different request', 409, 'CommitFailedException');
        }
        return jsonResponse(JSON.parse(String(prior[0].response_json)));
      }
    }
    const currentMetadataLocation = String(rows[0].metadata_location);
    const publication = await callIcebergCapability(this.env, {
      operation: 'commit-table',
      current_metadata_location: currentMetadataLocation,
      request_json: document.text,
    });
    validateTablePublication(publication);
    await execute(
      this.ctx,
      `UPDATE ${TABLE_TABLE} SET metadata_location = ?, metadata_json = ? WHERE namespace = ? AND name = ? AND metadata_location = ?`,
      publication['metadata-location'],
      canonicalJson(publication.metadata),
      namespace.key,
      name,
      currentMetadataLocation,
    );
    const response = {
      'metadata-location': publication['metadata-location'],
      metadata: publication.metadata,
      config: {},
    };
    if (idempotencyKey !== null) {
      await execute(
        this.ctx,
        `INSERT INTO ${REST_LEDGER_TABLE} (idempotency_key, request_digest, response_json) VALUES (?, ?, ?)`,
        idempotencyKey,
        requestDigest,
        canonicalJson(response),
      );
    }
    return jsonResponse(response);
  }

  /** Removes a table pointer without deleting immutable customer objects. */
  async #dropTable(encodedNamespace, encodedName, url) {
    if (url.searchParams.get('purgeRequested') === 'true') {
      throw new RequestError('purging immutable table objects is not supported', 406, 'UnsupportedOperationException');
    }
    const namespace = namespaceFromPath(encodedNamespace);
    const name = tableNameFromPath(encodedName);
    const rows = await execute(this.ctx, `SELECT name FROM ${TABLE_TABLE} WHERE namespace = ? AND name = ?`, namespace.key, name);
    if (!rows[0]) throw new RequestError('table does not exist', 404, 'NoSuchTableException');
    await execute(this.ctx, `DELETE FROM ${TABLE_TABLE} WHERE namespace = ? AND name = ?`, namespace.key, name);
    return new Response(null, { status: 204 });
  }

  /** Atomically moves one table identifier while retaining its immutable metadata pointer. */
  async #renameTable(request) {
    const body = await readRestJson(request);
    const source = tableIdentifier(body.source, 'source');
    const destination = tableIdentifier(body.destination, 'destination');
    const destinationNamespace = await execute(this.ctx, `SELECT name FROM ${NAMESPACE_TABLE} WHERE name = ?`, destination.namespace.key);
    if (!destinationNamespace[0]) throw new RequestError('destination namespace does not exist', 404, 'NoSuchNamespaceException');
    const sourceRows = await execute(this.ctx, `SELECT name FROM ${TABLE_TABLE} WHERE namespace = ? AND name = ?`, source.namespace.key, source.name);
    if (!sourceRows[0]) throw new RequestError('source table does not exist', 404, 'NoSuchTableException');
    const destinationRows = await execute(this.ctx, `SELECT name FROM ${TABLE_TABLE} WHERE namespace = ? AND name = ?`, destination.namespace.key, destination.name);
    if (destinationRows[0]) throw new RequestError('destination table already exists', 409, 'AlreadyExistsException');
    await execute(
      this.ctx,
      `UPDATE ${TABLE_TABLE} SET namespace = ?, name = ? WHERE namespace = ? AND name = ?`,
      destination.namespace.key,
      destination.name,
      source.namespace.key,
      source.name,
    );
    return new Response(null, { status: 204 });
  }

  /** Drops an empty namespace from SQLite. */
  async #dropNamespace(encodedNamespace) {
    const namespace = namespaceFromPath(encodedNamespace);
    const rows = await execute(this.ctx, `SELECT name FROM ${NAMESPACE_TABLE} WHERE name = ?`, namespace.key);
    if (!rows[0]) throw new RequestError('namespace does not exist', 404, 'NoSuchNamespaceException');
    const tables = await execute(this.ctx, `SELECT name FROM ${TABLE_TABLE} WHERE namespace = ? LIMIT 1`, namespace.key);
    if (tables[0]) throw new RequestError('namespace is not empty', 409, 'NamespaceNotEmptyException');
    await execute(this.ctx, `DELETE FROM ${NAMESPACE_TABLE} WHERE name = ?`, namespace.key);
    return new Response(null, { status: 204 });
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

/**
 * Advertises the S3 endpoint exposed beside this Catalog Worker. The endpoint
 * is derived from the request host so the component bytes remain identical in
 * every deployment.
 * @param {URL} catalogUrl
 * @returns {Record<string, string>}
 */
function s3ClientOverrides(catalogUrl) {
  const publicHost = catalogUrl.hostname.includes('.catalog.');
  const host = publicHost
    ? catalogUrl.hostname.replace('.catalog.', '.s3.')
    : `${catalogUrl.hostname}:8443`;
  return {
    's3.endpoint': `${catalogUrl.protocol}//${host}`,
    's3.path-style-access': 'true',
    's3.region': 'auto',
  };
}

export default { fetch };

/** Reads one bounded REST JSON object while retaining exact integer lexemes. */
async function readRestDocument(request) {
  const bytes = new Uint8Array(await request.arrayBuffer());
  if (bytes.byteLength > MAX_REST_REQUEST_BYTES) throw new RequestError('REST request is too large', 413);
  try {
    const text = textDecoder.decode(bytes);
    return { value: plainObject(JSON.parse(text), 'request body'), text };
  } catch (error) {
    if (error instanceof RequestError) throw error;
    throw new RequestError('request body must be valid JSON');
  }
}

/** Reads one bounded REST JSON object when raw numeric lexemes are not forwarded. */
async function readRestJson(request) {
  return (await readRestDocument(request)).value;
}

/** Validates a standard non-empty multipart namespace value. */
function namespaceValue(value) {
  if (!Array.isArray(value) || value.length === 0) throw new RequestError('namespace must be a non-empty array');
  const segments = value.map((segment) => icebergIdentifierPart(segment, 'namespace segment'));
  return { segments, key: segments.join(NAMESPACE_SEPARATOR) };
}

/** Decodes the Iceberg unit-separator namespace path representation. */
function namespaceFromPath(value) {
  let decoded;
  try {
    decoded = decodeURIComponent(value);
  } catch {
    throw new RequestError('namespace path is not valid percent encoding');
  }
  return namespaceValue(decoded.split(NAMESPACE_SEPARATOR));
}

/** Decodes and validates one percent-encoded Iceberg table name. */
function tableNameFromPath(value) {
  try {
    return icebergIdentifierPart(decodeURIComponent(value), 'table name');
  } catch (error) {
    if (error instanceof RequestError) throw error;
    throw new RequestError('table path is not valid percent encoding');
  }
}

/** Validates a standard Iceberg table identifier object. */
function tableIdentifier(value, field) {
  const object = plainObject(value, field);
  return {
    namespace: namespaceValue(object.namespace),
    name: icebergIdentifierPart(object.name, `${field} table name`),
  };
}

/** Validates one Iceberg identifier component without imposing SQL-name syntax. */
function icebergIdentifierPart(value, field) {
  if (typeof value !== 'string' || value.length === 0 || value.length > 255
      || value.includes(NAMESPACE_SEPARATOR) || /[\u0000-\u001e\u007f]/u.test(value)) {
    throw new RequestError(`${field} must be a non-empty bounded identifier`);
  }
  return value;
}

/** Splits one canonical SQLite namespace key. */
function namespaceSegmentsFromKey(key) {
  return key.split(NAMESPACE_SEPARATOR);
}

/** Validates a plain JSON object. */
function plainObject(value, field) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new RequestError(`${field} must be an object`);
  return value;
}

/** Validates an Iceberg string-to-string property map. */
function stringMap(value, field) {
  const object = plainObject(value, field);
  for (const [key, item] of Object.entries(object)) {
    if (typeof item !== 'string') throw new RequestError(`${field}.${key} must be a string`);
  }
  return object;
}

/** Validates one duplicate-free array of strings. */
function uniqueStringArray(value, field) {
  if (!Array.isArray(value) || value.some((item) => typeof item !== 'string')) {
    throw new RequestError(`${field} must be an array of strings`);
  }
  const unique = [...new Set(value)];
  if (unique.length !== value.length) throw new RequestError(`${field} must not contain duplicates`);
  return unique;
}

/**
 * Marks malformed input with a stable status and message.
 */
export class RequestError extends Error {
  /**
   * Creates a request validation failure.
   * @param {string} message
   * @param {number} status
   * @param {string} type
   */
  constructor(message, status = 400, type = 'BadRequestException') {
    super(message);
    this.name = 'RequestError';
    this.status = status;
    this.type = type;
  }
}

/**
 * Marks a runtime proposal that cannot prove its immutable writes.
 */
class RuntimeProposalError extends Error {
  /**
   * Creates a runtime proposal failure.
   * @param {string} message
   */
  constructor(message) {
    super(message);
    this.name = 'RuntimeProposalError';
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
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${REST_LEDGER_TABLE} (
    idempotency_key TEXT PRIMARY KEY,
    request_digest TEXT NOT NULL,
    response_json TEXT NOT NULL
  )`);
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${NAMESPACE_TABLE} (
    name TEXT PRIMARY KEY,
    properties_json TEXT NOT NULL
  )`);
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${TABLE_TABLE} (
    namespace TEXT NOT NULL,
    name TEXT NOT NULL,
    metadata_location TEXT,
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

/** Calls the host capability for one bounded Iceberg metadata publication. */
async function callIcebergCapability(env, payload) {
  const capability = env.ICEBERG_COMMIT;
  if (!capability || typeof capability.fetch !== 'function') {
    throw new RequestError('Iceberg publication capability is unavailable', 503, 'ServiceUnavailableException');
  }
  let response;
  try {
    response = await capability.fetch(new Request(`https://verglas.internal${CATALOG_COMMIT_PATH}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: canonicalJson(payload),
    }));
  } catch (error) {
    throw new RequestError(`Iceberg publication failed: ${errorMessage(error)}`, 503, 'ServiceUnavailableException');
  }
  if (!response) throw new RequestError('Iceberg publication returned no response', 503, 'ServiceUnavailableException');
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (bytes.byteLength > MAX_RUNTIME_RESPONSE_BYTES) {
    throw new RequestError('Iceberg publication response is too large', 503, 'ServiceUnavailableException');
  }
  let decoded;
  try {
    decoded = plainObject(JSON.parse(textDecoder.decode(bytes)), 'Iceberg publication response');
  } catch (error) {
    if (response.status >= 200 && response.status < 300) {
      throw new RequestError('Iceberg publication response is not valid JSON', 503, 'ServiceUnavailableException');
    }
    decoded = {};
  }
  if (response.status < 200 || response.status >= 300) {
    const message = typeof decoded.error?.message === 'string'
      ? decoded.error.message
      : `Iceberg publication failed with HTTP ${response.status}`;
    if (response.status === 400) throw new RequestError(message, 400, 'BadRequestException');
    if (response.status === 409) throw new RequestError(message, 409, 'CommitFailedException');
    throw new RequestError(message, 503, 'ServiceUnavailableException');
  }
  return decoded;
}

/** Validates host proposal fields before SQLite installs the Catalog head. */
function validateTablePublication(publication) {
  if (typeof publication['metadata-location'] !== 'string' || publication['metadata-location'].trim() === '') {
    throw new RequestError('Iceberg publication response is missing metadata-location', 503, 'ServiceUnavailableException');
  }
  const metadata = plainObject(publication.metadata, 'Iceberg publication metadata');
  if (!Number.isInteger(metadata['format-version']) || typeof metadata['table-uuid'] !== 'string') {
    throw new RequestError('Iceberg publication response contains invalid table metadata', 503, 'ServiceUnavailableException');
  }
}

/**
 * Requests immutable Sink files from the sole runtime capability and accepts
 * only a proposal matching the requested identity and row count.
 * @param {Record<string, unknown>} env
 * @param {object} config
 * @param {object} commit
 * @param {string|null} currentMetadataLocation
 * @returns {Promise<object>}
 */
async function requestSinkProposal(env, config, commit, currentMetadataLocation) {
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
    body: canonicalJson({
      operation: 'commit-sink-batch',
      current_metadata_location: currentMetadataLocation,
      request: JSON.parse(commit.canonicalPayload),
    }),
  });

  let response;
  try {
    const capability = env.ICEBERG_COMMIT;
    if (!capability || typeof capability.fetch !== 'function') throw new Error('ICEBERG_COMMIT is not configured');
    response = await capability.fetch(request);
  } catch (error) {
    throw new RuntimeProposalError(`Runtime proposal request failed: ${errorMessage(error)}`);
  }
  if (!response || response.status < 200 || response.status >= 300) {
    throw new RuntimeProposalError(`Runtime proposal did not confirm batch ${commit.batchId}: HTTP ${response?.status ?? 'unknown'}`);
  }
  const receiptBytes = new Uint8Array(await response.arrayBuffer());
  if (receiptBytes.byteLength > MAX_RUNTIME_RESPONSE_BYTES) {
    throw new RuntimeProposalError('Runtime proposal receipt exceeds its hard response ceiling');
  }
  let receipt;
  try {
    receipt = JSON.parse(textDecoder.decode(receiptBytes));
  } catch (error) {
    throw new RuntimeProposalError(`Runtime proposal receipt is not valid JSON: ${errorMessage(error)}`);
  }
  if (!receipt || typeof receipt !== 'object' || Array.isArray(receipt)) {
    throw new RuntimeProposalError('Runtime proposal receipt must be a JSON object');
  }
  if (receipt.committed !== true || receipt.batch_id !== commit.batchId || receipt.file_id !== commit.fileId) {
    throw new RuntimeProposalError('Runtime proposal receipt did not confirm the requested batch and file');
  }
  if (!Number.isSafeInteger(receipt.rows_committed) || receipt.rows_committed !== countRows(commit.canonicalPayload)) {
    throw new RuntimeProposalError('Runtime proposal receipt has the wrong committed row count');
  }
  if (typeof receipt.snapshot_id !== 'string' || receipt.snapshot_id.trim() === '') {
    throw new RuntimeProposalError('Runtime proposal receipt is missing snapshot_id');
  }
  if (typeof receipt.metadata_location !== 'string' || receipt.metadata_location.trim() === '') {
    throw new RuntimeProposalError('Runtime proposal receipt is missing metadata_location');
  }
  if (!receipt.metadata || typeof receipt.metadata !== 'object' || Array.isArray(receipt.metadata)) {
    throw new RuntimeProposalError('Runtime proposal receipt is missing Iceberg table metadata');
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

/** Installs the proposed table pointer and Sink receipt in the host-owned event transaction. */
async function persistCatalogCommit(ctx, config, commit, runtimeProposal, receipt) {
  await execute(
    ctx,
    `INSERT OR IGNORE INTO ${NAMESPACE_TABLE} (name, properties_json) VALUES (?, '{}')`,
    config.namespace,
  );
  await execute(
    ctx,
    `INSERT INTO ${TABLE_TABLE} (namespace, name, metadata_location, metadata_json) VALUES (?, ?, ?, ?)
     ON CONFLICT(namespace, name) DO UPDATE SET metadata_location = excluded.metadata_location, metadata_json = excluded.metadata_json`,
    config.namespace,
    config.table,
    runtimeProposal.metadata_location,
    canonicalJson(runtimeProposal.metadata),
  );
  await execute(
    ctx,
    `INSERT INTO ${LEDGER_TABLE} (batch_id, payload_digest, file_id, snapshot_id, rows_committed, receipt_json) VALUES (?, ?, ?, ?, ?, ?)`,
    commit.batchId,
    commit.payloadDigest,
    commit.fileId,
    receipt.snapshot_id,
    receipt.rows_committed,
    JSON.stringify(receipt),
  );
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
  if (segments.length < 2 || segments[0] !== 'v1') return false;
  if (segments.length === 3 && segments[1] === 'tables' && segments[2] === 'rename') {
    return method === 'POST';
  }
  if (segments[1] !== 'namespaces') return false;
  if (segments.length === 2) return method === 'GET' || method === 'POST';
  if (segments.length === 3) return method === 'GET' || method === 'DELETE';
  if (segments.length === 4 && segments[3] === 'properties') return method === 'POST';
  if (segments.length === 4 && segments[3] === 'tables') return method === 'GET' || method === 'POST';
  if (segments.length === 4 && segments[3] === 'register') return method === 'POST';
  if (segments.length === 5 && segments[3] === 'tables') {
    return method === 'GET' || method === 'HEAD' || method === 'DELETE' || method === 'POST';
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

/** Creates the standard Iceberg REST error envelope. */
function icebergErrorResponse(error) {
  const status = error instanceof RequestError ? error.status : 500;
  const type = error instanceof RequestError ? error.type : 'InternalServerError';
  return jsonResponse({ error: { message: errorMessage(error), type, code: status } }, status);
}
