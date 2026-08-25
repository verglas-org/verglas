/**
 * Prebuilt stateless SQL Pipeline Worker and Durable Object.
 * The object owns only its cursor, pending batches, retry state, and immutable
 * configuration. Stream and Sink are reached through ordinary named bindings.
 */

import { DurableObject } from 'cloudflare:workers';

export const PROCESS_PATH = '/pipeline/process-now';
export const STATUS_PATH = '/pipeline/status';
export const STREAM_READ_PATH = '/stream/read';
export const SINK_BATCH_PATH = '/sink/batch';
export const MAX_STREAM_READ = 1000;
export const MAX_BATCH_ROWS = 10_000;
export const MAX_BATCH_BYTES = 8 * 1024 * 1024;
export const MAX_BATCH_SECONDS = 24 * 60 * 60;
export const MAX_UNNEST_ELEMENTS = 10_000;
export const MAX_READ_RESPONSE_BYTES = 16 * 1024 * 1024;

const CONFIG_TABLE = 'pipeline_config';
const CURSOR_TABLE = 'pipeline_cursor';
const BATCH_TABLE = 'pipeline_batch';
const RETRY_BASE_MILLISECONDS = 1_000;
const RETRY_MAX_MILLISECONDS = 60_000;
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder('utf-8', { fatal: true });
const SQL_KEYWORDS = new Set([
  'INSERT', 'INTO', 'SELECT', 'FROM', 'WHERE', 'AS', 'AND', 'OR', 'NOT', 'NULL',
  'TRUE', 'FALSE', 'IS', 'LIKE', 'JOIN', 'GROUP', 'BY', 'ORDER', 'HAVING', 'LIMIT',
  'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP', 'WITH', 'RECURSIVE', 'UNION', 'OVER', 'DISTINCT',
]);
const SCALAR_FUNCTIONS = new Set([
  'UPPER', 'LOWER', 'LENGTH', 'TRIM', 'ABS', 'ROUND', 'COALESCE', 'NULLIF', 'CONCAT',
]);

/**
 * Routes the two internal controls to the named Pipeline object. No tenant
 * route is exposed by this Worker.
 * @param {Request} request
 * @param {Record<string, unknown>} env
 * @returns {Promise<Response>}
 */
async function fetch(request, env) {
  const url = new URL(request.url);
  const method = request.method.toUpperCase();
  const allowed = (method === 'POST' && url.pathname === PROCESS_PATH)
    || (method === 'GET' && url.pathname === STATUS_PATH);
  if (!allowed) return new Response('not found', { status: 404 });

  const namespace = env.PIPELINE_DO;
  const pipelineId = requiredString(env.PIPELINE_ID, 'PIPELINE_ID');
  if (!namespace || typeof namespace.idFromName !== 'function' || typeof namespace.get !== 'function') {
    return new Response('Pipeline binding is not configured', { status: 500 });
  }
  const id = namespace.idFromName(pipelineId);
  const target = new URL(`https://verglas.internal${url.pathname}${url.search}`);
  const body = method === 'GET' ? undefined : new Uint8Array(await request.arrayBuffer());
  const internal = new Request(target, {
    method,
    headers: request.headers,
    ...(body === undefined ? {} : { body }),
  });
  return namespace.get(id).fetch(internal);
}

/**
 * One serialized Pipeline object. Its SQL and cursor are durable in the
 * object's SQL database; external delivery is deliberately outside that DB.
 */
export class Pipeline extends DurableObject {
  #ready;
  #config;

  /** @param {DurableObjectState} ctx @param {Record<string, unknown>} env */
  #initError;

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
   * Handles only internal processing and status controls.
   * @param {Request} request
   * @returns {Promise<Response>}
   */
  async fetch(request) {
    await this.#ready;
    if (this.#initError) throw this.#initError;
    const url = new URL(request.url);
    const method = request.method.toUpperCase();
    if (method === 'POST' && url.pathname === PROCESS_PATH) return this.#processNow();
    if (method === 'GET' && url.pathname === STATUS_PATH) return this.#statusResponse();
    return new Response('not found', { status: 404 });
  }

  /**
   * Runs a due rolling batch. A failed delivery remains pending and is retried
   * with bounded exponential delay; it is never silently discarded.
   * @returns {Promise<void>}
   */
  async alarm() {
    await this.#ready;
    if (this.#initError) throw this.#initError;
    try {
      await this.#processOnce(true);
    } catch (error) {
      await this.#scheduleRetry(error);
    }
  }

  /**
   * Forces one bounded read and flushes the resulting batch immediately.
   * @returns {Promise<Response>}
   */
  async #processNow() {
    try {
      const status = await this.#processOnce(true);
      return jsonResponse(status);
    } catch (error) {
      await this.#scheduleRetry(error);
      return jsonResponse({ error: errorMessage(error) }, 503);
    }
  }

  /**
   * Collects one contiguous range, persists it before delivery, and advances
   * the cursor only after every targeted sink has acknowledged its batch.
   * @param {boolean} forceFlush
   * @returns {Promise<object>}
   */
  async #processOnce(forceFlush) {
    const pending = await loadPending(this.ctx);
    if (pending) {
      if (!forceFlush && Date.now() < pending.flush_at) return this.#status();
      await this.#deliverPending(pending);
      return this.#status();
    }

    const cursor = await loadCursor(this.ctx);
    const source = await readSource(this.env, this.#config, cursor);
    if (source.records.length === 0) {
      await this.ctx.storage.deleteAlarm();
      return this.#status();
    }

    const assembled = assembleBatch(this.#config, source.records);
    const pendingBatch = {
      first_sequence: assembled.firstSequence,
      last_sequence: assembled.lastSequence,
      next_after: assembled.nextAfter,
      flush_at: Date.now() + this.#config.batchMaxSeconds * 1000,
      retry_count: 0,
      sink_batches: assembled.sinkBatches,
    };
    await savePending(this.ctx, pendingBatch);
    await this.ctx.storage.setAlarm(pendingBatch.flush_at);

    // A fully filtered range has no external confirmation to await. Persisting
    // and then advancing still makes a crash harmless because the same range
    // can be reconstructed from its unchanged cursor.
    if (pendingBatch.sink_batches.length === 0 || forceFlush || assembled.flushNow) {
      await this.#deliverPending(pendingBatch);
    }
    return this.#status();
  }

  /**
   * Sends every sink batch, then advances and removes the pending batch.
   * @param {object} pending
   * @returns {Promise<void>}
   */
  async #deliverPending(pending) {
    try {
      for (const batch of pending.sink_batches) {
        await sendSink(this.env, this.#config, batch);
      }
    } catch (error) {
      const retryCount = pending.retry_count + 1;
      await execute(this.ctx, `UPDATE ${BATCH_TABLE} SET retry_count = ? WHERE id = 1`, retryCount);
      await this.ctx.storage.setAlarm(Date.now() + retryDelay(retryCount));
      throw error;
    }

    // This ordering is the crash invariant: no cursor mutation occurs before
    // all sink acknowledgements. A crash after the update only causes a sink
    // idempotency retry before the pending row is removed.
    await execute(this.ctx, `UPDATE ${CURSOR_TABLE} SET next_sequence = ? WHERE id = 1`, pending.next_after);
    await execute(this.ctx, `DELETE FROM ${BATCH_TABLE} WHERE id = 1`);
    await this.ctx.storage.deleteAlarm();
  }

  /**
   * Returns the durable cursor and pending-delivery status without exposing
   * records or binding credentials.
   * @returns {Promise<Response>}
   */
  async #statusResponse() {
    return jsonResponse(await this.#status());
  }

  /** @returns {Promise<object>} */
  async #status() {
    const cursor = await loadCursor(this.ctx);
    const pending = await loadPending(this.ctx);
    return {
      pipeline_id: this.#config.pipelineId,
      sql_digest: this.#config.sqlDigest,
      cursor,
      pending: pending !== undefined,
      retry_count: pending?.retry_count ?? 0,
    };
  }

  /**
   * Schedules a retry for a source or transform failure when no batch row
   * exists yet. Sink failures already set their retry alarm in delivery.
   * @param {unknown} error
   * @returns {Promise<void>}
   */
  async #scheduleRetry(error) {
    const pending = await loadPending(this.ctx);
    if (pending) return;
    await this.ctx.storage.setAlarm(Date.now() + RETRY_BASE_MILLISECONDS);
    void error;
  }
}

export default { fetch };

/**
 * Parses and validates the deliberately small Pipeline SQL target. The parser
 * accepts one or more INSERT INTO sink SELECT statements with stateless CTEs,
 * derived tables, projection, filters, and one top-level UNNEST per SELECT; it
 * rejects stateful or unknown syntax before serving.
 * @param {string} sql
 * @returns {Array<object>}
 */
export function parsePipelineSql(sql) {
  if (typeof sql !== 'string' || sql.trim() === '') throw new Error('PIPELINE_SQL must be a non-empty string');
  const statements = splitStatements(sql).map((statement, index) => parseStatement(statement, index));
  if (statements.length === 0) throw new Error('PIPELINE_SQL must contain at least one statement');
  return statements;
}

/**
 * Validates all synchronous configuration before the Durable Object event gate
 * is installed. This makes malformed SQL a constructor-time hard error.
 * @param {Record<string, unknown>} env
 * @returns {object}
 */
function validateConfiguration(env) {
  const pipelineId = requiredString(env.PIPELINE_ID, 'PIPELINE_ID');
  const sql = requiredString(env.PIPELINE_SQL, 'PIPELINE_SQL').trim();
  const sourceBinding = requiredString(env.PIPELINE_SOURCE_BINDING, 'PIPELINE_SOURCE_BINDING');
  const sourceName = requiredString(env.PIPELINE_SOURCE_NAME, 'PIPELINE_SOURCE_NAME');
  const statements = parsePipelineSql(sql);
  for (const statement of statements) validateStatementRelations(statement, sourceName);
  const sinkBindings = parseSinkBindings(env.PIPELINE_SINK_BINDINGS);
  for (const statement of statements) {
    if (!Object.hasOwn(sinkBindings, statement.sinkName)) {
      throw new Error(`SQL sink ${statement.sinkName} has no PIPELINE_SINK_BINDINGS entry`);
    }
  }
  return {
    pipelineId,
    sql,
    sourceBinding,
    sourceName,
    sinkBindings,
    statements,
    batchMaxRows: boundedInteger(env.PIPELINE_BATCH_MAX_ROWS, 'PIPELINE_BATCH_MAX_ROWS', 1, MAX_BATCH_ROWS),
    batchMaxBytes: boundedInteger(env.PIPELINE_BATCH_MAX_BYTES, 'PIPELINE_BATCH_MAX_BYTES', 1, MAX_BATCH_BYTES),
    batchMaxSeconds: boundedInteger(env.PIPELINE_BATCH_MAX_SECONDS, 'PIPELINE_BATCH_MAX_SECONDS', 1, MAX_BATCH_SECONDS),
  };
}

/** @param {object} preliminary @returns {Promise<object>} */
async function completeConfiguration(preliminary) {
  const sqlDigest = await digestHex(preliminary.sql);
  return {
    ...preliminary,
    sqlDigest,
    configJson: JSON.stringify({
      pipeline_id: preliminary.pipelineId,
      sql: preliminary.sql,
      sql_digest: sqlDigest,
      source_binding: preliminary.sourceBinding,
      source_name: preliminary.sourceName,
      sink_bindings: preliminary.sinkBindings,
      batch_max_rows: preliminary.batchMaxRows,
      batch_max_bytes: preliminary.batchMaxBytes,
      batch_max_seconds: preliminary.batchMaxSeconds,
    }),
  };
}

/** @param {DurableObjectState} ctx @returns {Promise<void>} */
async function createTables(ctx) {
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${CONFIG_TABLE} (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    pipeline_id TEXT NOT NULL,
    sql_digest TEXT NOT NULL,
    config_json TEXT NOT NULL
  )`);
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${CURSOR_TABLE} (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    next_sequence INTEGER NOT NULL
  )`);
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${BATCH_TABLE} (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    first_sequence INTEGER NOT NULL,
    last_sequence INTEGER NOT NULL,
    next_after INTEGER NOT NULL,
    flush_at INTEGER NOT NULL,
    retry_count INTEGER NOT NULL,
    batch_json TEXT NOT NULL
  )`);
}

/** @param {DurableObjectState} ctx @param {object} config @returns {Promise<void>} */
async function installOrCheckConfiguration(ctx, config) {
  const rows = await execute(ctx, `SELECT pipeline_id, sql_digest, config_json FROM ${CONFIG_TABLE} WHERE id = 1`);
  if (rows.length === 0) {
    await execute(ctx, `INSERT INTO ${CONFIG_TABLE} (id, pipeline_id, sql_digest, config_json) VALUES (1, ?, ?, ?)`, config.pipelineId, config.sqlDigest, config.configJson);
    await execute(ctx, `INSERT INTO ${CURSOR_TABLE} (id, next_sequence) VALUES (1, 0)`);
    return;
  }
  const row = rows[0];
  if (String(row.sql_digest) !== config.sqlDigest) {
    throw new Error(`immutable SQL mismatch for Pipeline ${config.pipelineId}: existing digest ${row.sql_digest}, configured ${config.sqlDigest}`);
  }
  if (String(row.pipeline_id) !== config.pipelineId || String(row.config_json) !== config.configJson) {
    throw new Error(`immutable Pipeline configuration mismatch for ${config.pipelineId}; delete and recreate the object`);
  }
  const cursor = await execute(ctx, `SELECT id FROM ${CURSOR_TABLE} WHERE id = 1`);
  if (cursor.length === 0) await execute(ctx, `INSERT INTO ${CURSOR_TABLE} (id, next_sequence) VALUES (1, 0)`);
}

/** @param {DurableObjectState} ctx @returns {Promise<number>} */
async function loadCursor(ctx) {
  const rows = await execute(ctx, `SELECT next_sequence FROM ${CURSOR_TABLE} WHERE id = 1`);
  if (rows.length !== 1) throw new Error('Pipeline cursor row is missing');
  return safeSequence(rows[0].next_sequence, 'stored cursor');
}

/** @param {DurableObjectState} ctx @returns {Promise<object|undefined>} */
async function loadPending(ctx) {
  const rows = await execute(ctx, `SELECT first_sequence, last_sequence, next_after, flush_at, retry_count, batch_json FROM ${BATCH_TABLE} WHERE id = 1`);
  if (rows.length === 0) return undefined;
  if (rows.length !== 1) throw new Error('Pipeline has multiple pending batch rows');
  let batch;
  try {
    batch = JSON.parse(String(rows[0].batch_json));
  } catch (error) {
    throw new Error(`Pipeline pending batch JSON is invalid: ${error.message}`);
  }
  if (!batch || !Array.isArray(batch.sink_batches)) throw new Error('Pipeline pending batch shape is invalid');
  return {
    ...batch,
    first_sequence: safeSequence(rows[0].first_sequence, 'pending first sequence'),
    last_sequence: safeSequence(rows[0].last_sequence, 'pending last sequence'),
    next_after: safeSequence(rows[0].next_after, 'pending next sequence'),
    flush_at: safeSequence(rows[0].flush_at, 'pending flush time'),
    retry_count: boundedInteger(rows[0].retry_count, 'stored retry count', 0, Number.MAX_SAFE_INTEGER),
  };
}

/** @param {DurableObjectState} ctx @param {object} pending @returns {Promise<void>} */
async function savePending(ctx, pending) {
  await execute(ctx, `INSERT OR REPLACE INTO ${BATCH_TABLE} (id, first_sequence, last_sequence, next_after, flush_at, retry_count, batch_json) VALUES (1, ?, ?, ?, ?, ?, ?)`,
    pending.first_sequence,
    pending.last_sequence,
    pending.next_after,
    pending.flush_at,
    pending.retry_count,
    JSON.stringify({ sink_batches: pending.sink_batches }),
  );
}

/**
 * Reads the Stream's exclusive bounded range through a named binding. A
 * namespace binding and a direct service-style fetch binding are both ordinary
 * Worker/DO protocol shapes; neither creates an alternate transport.
 * @param {Record<string, unknown>} env
 * @param {object} config
 * @param {number} after
 * @returns {Promise<{records:Array<object>, nextAfter:number}>}
 */
async function readSource(env, config, after) {
  const limit = Math.min(config.batchMaxRows, MAX_STREAM_READ);
  const uri = `https://verglas.internal${STREAM_READ_PATH}?after=${after}&limit=${limit}`;
  const response = await bindingFetch(env[config.sourceBinding], config.sourceName, new Request(uri, { method: 'GET' }));
  const value = await responseJson(response, 'Stream read');
  if (!value || !Array.isArray(value.records)) throw new Error('Stream read response must contain records[]');
  const records = [];
  let expected = after + 1;
  for (const item of value.records) {
    if (!item || typeof item !== 'object' || Array.isArray(item)) throw new Error('Stream read record envelope must be an object');
    const sequence = safeSequence(item.sequence, 'Stream sequence');
    if (sequence !== expected) throw new Error(`Stream read is not contiguous at sequence ${sequence}; expected ${expected}`);
    if (!Object.hasOwn(item, 'record')) throw new Error('Stream read record envelope is missing record');
    assertJsonValue(item.record, new WeakSet());
    records.push({ sequence, record: item.record });
    expected += 1;
  }
  const nextAfter = value.next_after === undefined
    ? (records.length === 0 ? after : records.at(-1).sequence)
    : safeSequence(value.next_after, 'Stream next_after');
  if (nextAfter !== (records.length === 0 ? after : records.at(-1).sequence)) {
    throw new Error('Stream read next_after does not match its last sequence');
  }
  return { records, nextAfter };
}

/**
 * Converts one bounded source range into per-sink deterministic batches. The
 * byte ceiling is checked against the exact Sink envelope before a source row
 * joins, and output expansion is capped before it can allocate an unbounded
 * result set.
 * @param {object} config
 * @param {Array<{sequence:number,record:unknown}>} sourceRecords
 * @returns {{firstSequence:number,lastSequence:number,nextAfter:number,sinkBatches:Array<object>,flushNow:boolean}}
 */
function assembleBatch(config, sourceRecords) {
  const outputs = new Map();
  const consumed = [];
  let flushNow = false;
  for (const source of sourceRecords) {
    const candidate = new Map(outputs);
    let hasOutput = false;
    for (const statement of config.statements) {
      const results = transformRecord(statement, source.record, config.batchMaxRows, config.batchMaxBytes);
      if (results.length === 0) continue;
      hasOutput = true;
      const current = candidate.get(statement.sinkName) ?? { records: [], sequences: [] };
      const next = {
        records: current.records.concat(results),
        sequences: current.sequences.concat(results.map(() => source.sequence)),
      };
      if (next.records.length > config.batchMaxRows) {
        if (consumed.length === 0) {
          throw new Error(`UNNEST expansion exceeds PIPELINE_BATCH_MAX_ROWS (${config.batchMaxRows})`);
        }
        flushNow = true;
        break;
      }
      const envelope = makeSinkBatch(config, statement.sinkName, next);
      if (jsonByteLength(envelope) > config.batchMaxBytes) {
        if (consumed.length === 0) {
          throw new Error(`one transformed record exceeds PIPELINE_BATCH_MAX_BYTES (${config.batchMaxBytes})`);
        }
        flushNow = true;
        break;
      }
      candidate.set(statement.sinkName, next);
    }
    if (flushNow) break;
    outputs.clear();
    for (const [sinkName, value] of candidate) outputs.set(sinkName, value);
    consumed.push(source);
    if (hasOutput && [...outputs].some(([sinkName, value]) => jsonByteLength(makeSinkBatch(config, sinkName, value)) >= config.batchMaxBytes)) {
      flushNow = true;
    }
    if (consumed.length >= config.batchMaxRows) {
      flushNow = true;
    }
    if (flushNow) break;
  }
  if (consumed.length === 0) throw new Error('Pipeline could not assemble a source record within its hard batch ceiling');
  const firstSequence = consumed[0].sequence;
  const lastSequence = consumed.at(-1).sequence;
  const sinkBatches = [];
  for (const [sinkName, value] of outputs) sinkBatches.push(makeSinkBatch(config, sinkName, value));
  return {
    firstSequence,
    lastSequence,
    nextAfter: lastSequence,
    sinkBatches,
    flushNow,
  };
}

/**
 * Evaluates one statement against one Stream record. CTE and derived-table
 * rows remain ordinary JSON objects, so no state or second storage authority is
 * introduced by the relational syntax.
 * @param {object} statement
 * @param {unknown} record
 * @param {number} rowBudget
 * @param {number} byteBudget
 * @returns {Array<object>}
 */
function transformRecord(statement, record, rowBudget, byteBudget) {
  const cteRows = new Map();
  for (const query of statement.withQueries) {
    if (cteRows.has(query.name)) throw new Error(`duplicate CTE name ${query.name}`);
    cteRows.set(query.name, evaluateSelect(query.select, record, cteRows, rowBudget, byteBudget));
  }
  return evaluateSelect(statement.select, record, cteRows, rowBudget, byteBudget);
}

/**
 * Evaluates one SELECT relation against one source record. Every relation is
 * either the configured Stream, a prior CTE, or a derived SELECT; joins are
 * absent from the AST by construction.
 * @param {object} select
 * @param {unknown} sourceRecord
 * @param {Map<string,Array<object>>} cteRows
 * @param {number} rowBudget
 * @param {number} byteBudget
 * @returns {Array<object>}
 */
function evaluateSelect(select, sourceRecord, cteRows, rowBudget, byteBudget) {
  const localCtes = new Map(cteRows);
  for (const query of select.withQueries) {
    if (localCtes.has(query.name)) throw new Error(`duplicate CTE name ${query.name}`);
    localCtes.set(query.name, evaluateSelect(query.select, sourceRecord, localCtes, rowBudget, byteBudget));
  }
  const relationRows = evaluateRelation(select.from, sourceRecord, localCtes, rowBudget, byteBudget);
  const result = [];
  let resultBytes = 0;
  for (const relation of relationRows) {
    const scope = { record: relation.record, sourceAlias: relation.alias, unnestValues: new Map() };
    if (select.where && !truthy(evaluateExpression(select.where, scope))) continue;
    const expansion = select.unnest
      ? evaluateUnnest(select.unnest, scope, rowBudget)
      : [undefined];
    for (const element of expansion) {
      const rowScope = element === undefined
        ? scope
        : { ...scope, unnestValues: new Map([[select.unnest.alias, element]]) };
      const row = projectRow(select.projections, rowScope, element);
      resultBytes += jsonByteLength(row);
      if (resultBytes > byteBudget) throw new Error(`SELECT expansion exceeds PIPELINE_BATCH_MAX_BYTES (${byteBudget})`);
      result.push(row);
      if (result.length > rowBudget) throw new Error(`SELECT expansion exceeds PIPELINE_BATCH_MAX_ROWS (${rowBudget})`);
    }
  }
  return result;
}

/** @param {object} from @param {unknown} sourceRecord @param {Map<string,Array<object>>} cteRows @param {number} rowBudget @param {number} byteBudget @returns {Array<object>} */
function evaluateRelation(from, sourceRecord, cteRows, rowBudget, byteBudget) {
  if (from.kind === 'stream') return [{ record: sourceRecord, alias: from.alias }];
  if (from.kind === 'cte') {
    const rows = cteRows.get(from.name);
    if (!rows) throw new Error(`CTE ${from.name} is not available`);
    if (rows.length > rowBudget) throw new Error(`CTE ${from.name} exceeds PIPELINE_BATCH_MAX_ROWS (${rowBudget})`);
    return rows.map((record) => ({ record, alias: from.alias }));
  }
  if (from.kind === 'subquery') {
    return evaluateSelect(from.select, sourceRecord, cteRows, rowBudget, byteBudget)
      .map((record) => ({ record, alias: from.alias }));
  }
  throw new Error(`unknown relation kind ${from.kind}`);
}

/** @param {object} unnest @param {object} scope @param {number} rowBudget @returns {Array<unknown>} */
function evaluateUnnest(unnest, scope, rowBudget) {
  const value = evaluateExpression(unnest.expression, scope);
  if (!Array.isArray(value)) throw new Error('UNNEST requires a JSON array value');
  if (value.length > MAX_UNNEST_ELEMENTS || value.length > rowBudget) {
    throw new Error(`UNNEST expansion exceeds the hard limit of ${Math.min(MAX_UNNEST_ELEMENTS, rowBudget)} elements`);
  }
  assertJsonValue(value, new WeakSet());
  return value;
}

/** @param {Array<object>} projections @param {object} scope @param {unknown} element @returns {object} */
function projectRow(projections, scope, element) {
  if (projections.length === 1 && projections[0].kind === 'star') {
    if (scope.record && typeof scope.record === 'object' && !Array.isArray(scope.record)) return cloneJson(scope.record);
    return { value: cloneJson(scope.record) };
  }
  const result = {};
  for (const projection of projections) {
    const value = projection.expression.kind === 'unnest'
      ? element
      : evaluateExpression(projection.expression, scope);
    result[projection.name] = value === undefined ? null : cloneJson(value);
  }
  assertJsonValue(result, new WeakSet());
  return result;
}

/** @param {object} config @param {string} sinkName @param {{records:Array<object>,sequences:Array<number>}} value @returns {object} */
function makeSinkBatch(config, sinkName, value) {
  const first = value.sequences[0];
  const last = value.sequences.at(-1);
  const batchId = JSON.stringify([config.pipelineId, config.sqlDigest, first, last, sinkName]);
  return {
    batch_id: batchId,
    pipeline_id: config.pipelineId,
    sql_digest: config.sqlDigest,
    source: config.sourceName,
    sink: sinkName,
    first_sequence: first,
    last_sequence: last,
    records: value.records,
  };
}

/** @param {Record<string, unknown>} env @param {object} config @param {object} batch @returns {Promise<void>} */
async function sendSink(env, config, batch) {
  const bindingName = config.sinkBindings[batch.sink];
  const headers = new Headers([
    ['content-type', 'application/json'],
    ['x-verglas-pipeline-id', config.pipelineId],
    ['x-verglas-sql-digest', config.sqlDigest],
    ['x-verglas-batch-id', batch.batch_id],
  ]);
  const request = new Request(`https://verglas.internal${SINK_BATCH_PATH}`, {
    method: 'POST',
    headers,
    body: JSON.stringify(batch),
  });
  const response = await bindingFetch(env[bindingName], batch.sink, request);
  if (!response || response.status < 200 || response.status >= 300) {
    throw new Error(`Sink ${batch.sink} did not confirm batch ${batch.batch_id}: HTTP ${response?.status ?? 'unknown'}`);
  }
}

/** @param {unknown} binding @param {string} objectName @param {Request} request @returns {Promise<Response>} */
async function bindingFetch(binding, objectName, request) {
  if (binding && typeof binding.fetch === 'function') return binding.fetch(request);
  if (binding && typeof binding.idFromName === 'function' && typeof binding.get === 'function') {
    const id = binding.idFromName(objectName);
    const stub = binding.get(id);
    if (!stub || typeof stub.fetch !== 'function') throw new Error(`binding ${objectName} did not return a fetch stub`);
    return stub.fetch(request);
  }
  throw new Error(`binding for ${objectName} is not configured`);
}

/** @param {Response} response @param {string} operation @returns {Promise<any>} */
async function responseJson(response, operation) {
  if (!response || !Number.isInteger(Number(response.status))) throw new Error(`${operation} returned an invalid response`);
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (bytes.byteLength > MAX_READ_RESPONSE_BYTES) throw new Error(`${operation} response exceeds the hard memory ceiling`);
  let value;
  try {
    value = JSON.parse(textDecoder.decode(bytes));
  } catch (error) {
    throw new Error(`${operation} returned invalid JSON: ${error.message}`);
  }
  if (response.status < 200 || response.status >= 300) throw new Error(`${operation} failed: HTTP ${response.status}`);
  return value;
}

/** @param {DurableObjectState} ctx @param {string} statement @param {...unknown} bindings @returns {Promise<Array<object>>} */
async function execute(ctx, statement, ...bindings) {
  const cursor = await ctx.storage.sql.exec(statement, ...bindings);
  return cursor.toArray();
}

/** @param {unknown} value @param {string} name @returns {string} */
function requiredString(value, name) {
  if (typeof value !== 'string' || value.trim() === '') throw new Error(`${name} must be a non-empty string`);
  return value.trim();
}

/** @param {unknown} value @param {string} name @param {number} minimum @param {number} maximum @returns {number} */
function boundedInteger(value, name, minimum, maximum) {
  const number = typeof value === 'number' ? value : (typeof value === 'string' && /^\d+$/u.test(value.trim()) ? Number(value) : NaN);
  if (!Number.isSafeInteger(number) || number < minimum || number > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return number;
}

/** @param {unknown} value @returns {Record<string,string>} */
function parseSinkBindings(value) {
  let raw = value;
  if (typeof raw === 'string') {
    try { raw = JSON.parse(raw); } catch (error) { throw new Error(`PIPELINE_SINK_BINDINGS must be a JSON object: ${error.message}`); }
  }
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) throw new Error('PIPELINE_SINK_BINDINGS must be an object mapping sink names to binding names');
  const result = {};
  for (const [sinkName, binding] of Object.entries(raw)) {
    if (!/^[A-Za-z_][A-Za-z0-9_$]*$/u.test(sinkName)) throw new Error(`invalid sink name ${sinkName}`);
    result[sinkName] = requiredString(binding, `PIPELINE_SINK_BINDINGS.${sinkName}`);
  }
  if (Object.keys(result).length === 0) throw new Error('PIPELINE_SINK_BINDINGS must contain at least one sink');
  return Object.fromEntries(Object.keys(result).sort().map((sinkName) => [sinkName, result[sinkName]]));
}

/** @param {unknown} value @returns {number} */
function safeSequence(value, label) {
  const number = typeof value === 'number' ? value : (typeof value === 'string' && /^\d+$/u.test(value) ? Number(value) : NaN);
  if (!Number.isSafeInteger(number) || number < 0) throw new Error(`${label} must be a non-negative safe integer`);
  return number;
}

/** @param {number} retryCount @returns {number} */
function retryDelay(retryCount) {
  return Math.min(RETRY_MAX_MILLISECONDS, RETRY_BASE_MILLISECONDS * (2 ** Math.min(retryCount - 1, 6)));
}

/** @param {unknown} value @returns {boolean} */
function truthy(value) {
  return value !== null && value !== undefined && value !== false && value !== 0 && value !== '';
}

/** @param {unknown} value @returns {unknown} */
function cloneJson(value) {
  if (value === undefined) return null;
  return JSON.parse(JSON.stringify(value));
}

/** @param {unknown} value @param {WeakSet<object>} ancestors */
function assertJsonValue(value, ancestors) {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return;
  if (typeof value === 'number') {
    if (Number.isFinite(value)) return;
    throw new TypeError('JSON values must contain finite numbers');
  }
  if (typeof value !== 'object') throw new TypeError(`unsupported JSON value ${typeof value}`);
  if (ancestors.has(value)) throw new TypeError('cyclic JSON value');
  ancestors.add(value);
  if (Array.isArray(value)) {
    for (const item of value) assertJsonValue(item, ancestors);
  } else {
    for (const key of Object.keys(value)) assertJsonValue(value[key], ancestors);
  }
  ancestors.delete(value);
}

/** @param {unknown} value @returns {number} */
function jsonByteLength(value) {
  assertJsonValue(value, new WeakSet());
  return textEncoder.encode(JSON.stringify(value)).byteLength;
}

/** @param {unknown} value @returns {Promise<string>} */
async function digestHex(value) {
  if (!globalThis.crypto || !globalThis.crypto.subtle) throw new Error('Web Crypto SHA-256 is required for Pipeline SQL digests');
  const digest = new Uint8Array(await globalThis.crypto.subtle.digest('SHA-256', textEncoder.encode(value)));
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, '0')).join('');
}

/** @param {string} sql @returns {string[]} */
function splitStatements(sql) {
  const statements = [];
  let start = 0;
  let quote = false;
  let depth = 0;
  for (let index = 0; index < sql.length; index += 1) {
    const character = sql[index];
    if (quote) {
      if (character === "'" && sql[index + 1] === "'") {
        index += 1;
      } else if (character === "'") {
        quote = false;
      }
    } else if (character === "'") {
      quote = true;
    } else if (character === '(') {
      depth += 1;
    } else if (character === ')') {
      depth -= 1;
      if (depth < 0) throw new Error('SQL has an unmatched closing parenthesis');
    } else if (character === ';' && depth === 0) {
      if (sql.slice(start, index).trim() !== '') statements.push(sql.slice(start, index).trim());
      start = index + 1;
    }
  }
  if (quote) throw new Error('SQL has an unterminated string literal');
  if (depth !== 0) throw new Error('SQL has an unmatched opening parenthesis');
  if (sql.slice(start).trim() !== '') statements.push(sql.slice(start).trim());
  return statements;
}

/** @param {string} source @param {number} index @returns {object} */
function parseStatement(source, index) {
  const tokens = tokenize(source);
  const parser = new SqlParser(tokens, source);
  parser.expectKeyword('INSERT');
  parser.expectKeyword('INTO');
  const sinkName = parser.expectIdentifier('sink name');
  const withQueries = parser.parseWithClause();
  const select = parser.parseSelect();
  if (parser.peek()) parser.unsupported(`unexpected ${parser.peek().value} after statement ${index + 1}`);
  const directRelation = select.from.kind === 'relation' ? select.from : undefined;
  return {
    sinkName,
    withQueries,
    select,
    ...(directRelation === undefined ? {} : { sourceName: directRelation.name, sourceAlias: directRelation.alias }),
  };
}

/** @param {string} source @returns {Array<object>} */
function tokenize(source) {
  const tokens = [];
  for (let index = 0; index < source.length;) {
    const character = source[index];
    if (/\s/u.test(character)) {
      index += 1;
      continue;
    }
    if (character === "'") {
      let value = '';
      index += 1;
      let closed = false;
      while (index < source.length) {
        if (source[index] === "'" && source[index + 1] === "'") {
          value += "'";
          index += 2;
        } else if (source[index] === "'") {
          index += 1;
          closed = true;
          break;
        } else {
          value += source[index];
          index += 1;
        }
      }
      if (!closed) throw new Error('SQL has an unterminated string literal');
      tokens.push({ kind: 'literal', value, literal: true });
      continue;
    }
    if (character === '"') throw new Error('double-quoted identifiers are not supported by Pipeline SQL');
    const number = source.slice(index).match(/^\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/u);
    if (number) {
      const value = Number(number[0]);
      if (!Number.isFinite(value)) throw new Error('SQL numeric literal is not finite');
      tokens.push({ kind: 'literal', value });
      index += number[0].length;
      continue;
    }
    const identifier = source.slice(index).match(/^[A-Za-z_][A-Za-z0-9_$]*/u);
    if (identifier) {
      const value = identifier[0];
      const upper = value.toUpperCase();
      const keyword = SQL_KEYWORDS.has(upper) ? upper : undefined;
      tokens.push({ kind: 'identifier', value, keyword });
      index += value.length;
      continue;
    }
    const two = source.slice(index, index + 2);
    if (['<=', '>=', '<>', '!=', '||', '&&'].includes(two)) {
      tokens.push({ kind: 'operator', value: two });
      index += 2;
      continue;
    }
    if ('(),.*+-/%=<>[]'.includes(character)) {
      tokens.push({ kind: 'operator', value: character });
      index += 1;
      continue;
    }
    throw new Error(`unsupported SQL character ${character}`);
  }
  return tokens;
}

class SqlParser {
  /** @param {Array<object>} tokens @param {string} source */
  constructor(tokens, source) {
    this.tokens = tokens;
    this.source = source;
    this.position = 0;
  }

  /** @returns {object|undefined} */
  peek() { return this.tokens[this.position]; }

  /** @returns {object} */
  next() {
    const token = this.peek();
    if (!token) throw new Error('unexpected end of SQL');
    this.position += 1;
    return token;
  }

  /** @param {string} keyword @returns {void} */
  expectKeyword(keyword) {
    const token = this.next();
    if (token.kind !== 'identifier' || token.keyword !== keyword) this.unsupported(`expected ${keyword}, received ${token.value}`);
  }

  /** @param {string} keyword @returns {boolean} */
  matchKeyword(keyword) {
    const token = this.peek();
    if (token?.kind === 'identifier' && token.keyword === keyword) {
      this.position += 1;
      return true;
    }
    return false;
  }

  /** @param {string} description @returns {string} */
  expectIdentifier(description) {
    const token = this.next();
    if (token.kind !== 'identifier' || token.keyword) this.unsupported(`expected ${description}, received ${token.value}`);
    return token.value;
  }

  /** @returns {Array<object>} */
  parseWithClause() {
    if (!this.matchKeyword('WITH')) return [];
    if (this.matchKeyword('RECURSIVE')) this.unsupported('recursive CTEs are not supported');
    const queries = [];
    while (true) {
      const name = this.expectIdentifier('CTE name');
      this.expectKeyword('AS');
      if (this.next().value !== '(') this.unsupported('CTE body must be parenthesized');
      const nestedWith = this.parseWithClause();
      const select = this.parseSelect();
      if (this.next().value !== ')') this.unsupported('CTE body is missing its closing parenthesis');
      select.withQueries = nestedWith;
      queries.push({ name, select });
      if (!this.matchValue(',')) break;
    }
    return queries;
  }

  /** @returns {Array<object>} */
  parseProjectionList() {
    const projections = [];
    if (this.peek()?.value === '*') {
      this.next();
      if (!(this.peek()?.kind === 'identifier' && this.peek().keyword === 'FROM')) {
        this.unsupported('SELECT * cannot contain additional expressions');
      }
      return [{ kind: 'star' }];
    }
    while (true) {
      if (this.peek()?.value === '*') this.unsupported('SELECT * cannot be combined with projections');
      const expression = this.parseExpression();
      let name;
      let explicitAlias = false;
      if (this.matchKeyword('AS')) {
        name = this.expectIdentifier('projection alias');
        explicitAlias = true;
      } else if (this.peek()?.kind === 'identifier' && !this.peek().keyword) {
        name = this.next().value;
      } else {
        name = projectionName(expression, projections.length);
      }
      if (expression.kind === 'unnest' && !explicitAlias) this.unsupported('UNNEST requires an explicit AS alias');
      if (projections.some((projection) => projection.name === name)) throw new Error(`projection alias ${name} is not unique`);
      projections.push({ kind: 'expression', expression, name });
      if (this.peek()?.value !== ',') break;
      this.next();
    }
    return projections;
  }

  /** @returns {object} */
  parseSelect() {
    const withQueries = this.parseWithClause();
    this.expectKeyword('SELECT');
    const projections = this.parseProjectionList();
    this.expectKeyword('FROM');
    const from = this.parseRelation();
    let where;
    if (this.matchKeyword('WHERE')) where = this.parseExpression();
    const unnestProjections = projections.filter((projection) => projection.expression?.kind === 'unnest');
    const projectionUnnestCount = projections.reduce((count, projection) => count + countUnnest(projection.expression), 0);
    if (projectionUnnestCount > 1) this.unsupported('only one UNNEST expression is allowed per SELECT');
    const nestedUnnest = projections.some((projection) => projection.expression && countUnnest(projection.expression) > 0 && projection.expression.kind !== 'unnest');
    if (nestedUnnest || countUnnest(where) > 0) this.unsupported('UNNEST must be a top-level SELECT expression');
    const unnest = unnestProjections.length === 0 ? undefined : {
      alias: unnestProjections[0].name,
      expression: unnestProjections[0].expression.expression,
    };
    return { withQueries, projections, from, where, unnest };
  }

  /** @returns {object} */
  parseRelation() {
    if (this.matchValue('(')) {
      const select = this.parseSelect();
      if (!this.matchValue(')')) this.unsupported('derived table is missing its closing parenthesis');
      const alias = this.parseAlias('derived table alias', '__derived');
      return { kind: 'subquery', select, alias };
    }
    const name = this.expectIdentifier('relation name');
    const alias = this.parseAlias('relation alias', name);
    return { kind: 'relation', name, alias };
  }

  /** @param {string} description @param {string|undefined} defaultName @returns {string} */
  parseAlias(description, defaultName) {
    if (this.matchKeyword('AS')) return this.expectIdentifier(description);
    if (this.peek()?.kind === 'identifier' && !this.peek().keyword) return this.next().value;
    if (defaultName !== undefined) return defaultName;
    this.unsupported(`${description} is required`);
  }

  /** @param {string} value @returns {boolean} */
  matchValue(value) {
    if (this.peek()?.value === value) {
      this.position += 1;
      return true;
    }
    return false;
  }

  /** @param {number} minimum @returns {object} */
  parseExpression(minimum = 0) {
    let left = this.parsePrefix();
    while (true) {
      const operator = this.binaryOperator(this.peek());
      if (!operator || operator.precedence < minimum) break;
      this.next();
      let right;
      if (operator.operator === 'IS NULL' || operator.operator === 'IS NOT NULL') {
        if (operator.operator === 'IS NOT NULL') this.expectKeyword('NOT');
        this.expectKeyword('NULL');
        right = { kind: 'literal', value: null };
      } else {
        right = this.parseExpression(operator.precedence + 1);
      }
      left = { kind: 'binary', operator: operator.operator, left, right };
    }
    return left;
  }

  /** @returns {object} */
  parsePrefix() {
    const token = this.next();
    if (token.kind === 'literal') return { kind: 'literal', value: token.value };
    if (token.kind === 'identifier' && token.keyword === 'NULL') return { kind: 'literal', value: null };
    if (token.kind === 'identifier' && token.keyword === 'TRUE') return { kind: 'literal', value: true };
    if (token.kind === 'identifier' && token.keyword === 'FALSE') return { kind: 'literal', value: false };
    if (token.kind === 'identifier' && token.keyword === 'NOT') return { kind: 'unary', operator: 'NOT', value: this.parseExpression(3) };
    if (token.value === '+' || token.value === '-') return { kind: 'unary', operator: token.value, value: this.parseExpression(7) };
    if (token.value === '(') {
      const expression = this.parseExpression();
      if (this.next().value !== ')') this.unsupported('expected closing parenthesis');
      return expression;
    }
    if (token.value === '[') {
      const items = [];
      if (!this.matchValue(']')) {
        while (true) {
          items.push(this.parseExpression());
          if (this.matchValue(']')) break;
          if (!this.matchValue(',')) this.unsupported('array literal expects comma-separated values');
        }
      }
      return { kind: 'array', items };
    }
    if (token.kind === 'identifier') {
      if (token.keyword) this.unsupported(`keyword ${token.keyword} cannot start an expression`);
      const path = [token.value];
      while (this.peek()?.value === '.') {
        this.next();
        path.push(this.expectIdentifier('field name'));
      }
      if (this.peek()?.value === '(') {
        this.next();
        const args = [];
        if (this.peek()?.value !== ')') {
          while (true) {
            args.push(this.parseExpression());
            if (this.peek()?.value !== ',') break;
            this.next();
          }
        }
        if (this.next().value !== ')') this.unsupported('expected closing function parenthesis');
        const functionName = path.join('.').toUpperCase();
        if (path.length !== 1) this.unsupported('qualified function names are not supported');
        if (functionName === 'UNNEST') {
          if (args.length !== 1) this.unsupported('UNNEST requires exactly one array expression');
          return { kind: 'unnest', expression: args[0] };
        }
        if (['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'ARRAY_AGG', 'OVER'].includes(functionName)) this.unsupported(`aggregate or window function ${functionName} is not supported`);
        if (!SCALAR_FUNCTIONS.has(functionName)) this.unsupported(`scalar function ${functionName} is not supported`);
        return { kind: 'function', name: functionName, args };
      }
      return { kind: 'path', path };
    }
    this.unsupported(`unexpected expression token ${token.value}`);
  }

  /** @param {object|undefined} token @returns {{operator:string,precedence:number}|undefined} */
  binaryOperator(token) {
    if (!token) return undefined;
    if (token.kind === 'identifier' && token.keyword === 'OR') return { operator: 'OR', precedence: 1 };
    if (token.kind === 'identifier' && token.keyword === 'AND') return { operator: 'AND', precedence: 2 };
    if (token.kind === 'identifier' && token.keyword === 'IS') {
      const next = this.tokens[this.position + 1];
      const afterNext = this.tokens[this.position + 2];
      if (next?.kind === 'identifier' && next.keyword === 'NULL') return { operator: 'IS NULL', precedence: 3 };
      if (next?.kind === 'identifier' && next.keyword === 'NOT' && afterNext?.kind === 'identifier' && afterNext.keyword === 'NULL') {
        return { operator: 'IS NOT NULL', precedence: 3 };
      }
      this.unsupported('IS only supports NULL checks');
    }
    if (token.kind === 'identifier' && token.keyword === 'LIKE') return { operator: 'LIKE', precedence: 3 };
    if (token.value === '=' || token.value === '!=' || token.value === '<>' || token.value === '<' || token.value === '<=' || token.value === '>' || token.value === '>=') return { operator: token.value, precedence: 3 };
    if (token.value === '||') return { operator: '||', precedence: 4 };
    if (token.value === '+' || token.value === '-') return { operator: token.value, precedence: 5 };
    if (token.value === '*' || token.value === '/' || token.value === '%') return { operator: token.value, precedence: 6 };
    return undefined;
  }

  /** @param {string} message @returns {never} */
  unsupported(message) { throw new Error(`unsupported Pipeline SQL: ${message}`); }
}

/** @param {object} expression @returns {number} */
function countUnnest(expression) {
  if (!expression) return 0;
  if (expression.kind === 'unnest') return 1 + countUnnest(expression.expression);
  if (expression.kind === 'array') return expression.items.reduce((count, item) => count + countUnnest(item), 0);
  if (expression.kind === 'function') return expression.args.reduce((count, item) => count + countUnnest(item), 0);
  if (expression.kind === 'unary') return countUnnest(expression.value);
  if (expression.kind === 'binary') return countUnnest(expression.left) + countUnnest(expression.right);
  return 0;
}

/** @param {object} statement @param {string} sourceName @returns {void} */
function validateStatementRelations(statement, sourceName) {
  const available = new Set();
  for (const query of statement.withQueries) {
    if (query.name === sourceName || available.has(query.name)) throw new Error(`duplicate or conflicting CTE name ${query.name}`);
    validateSelectRelations(query.select, sourceName, available);
    available.add(query.name);
  }
  validateSelectRelations(statement.select, sourceName, available);
}

/** @param {object} select @param {string} sourceName @param {Set<string>} outerCtes @returns {void} */
function validateSelectRelations(select, sourceName, outerCtes) {
  const available = new Set(outerCtes);
  for (const query of select.withQueries) {
    if (query.name === sourceName || available.has(query.name)) throw new Error(`duplicate or conflicting CTE name ${query.name}`);
    validateSelectRelations(query.select, sourceName, available);
    available.add(query.name);
  }
  classifyRelation(select.from, sourceName, available);
  if (countUnnest(select.where) > 0) throw new Error('UNNEST is only supported as a top-level SELECT projection');
  if (select.from.kind === 'subquery') validateSelectRelations(select.from.select, sourceName, available);
}

/** @param {object} relation @param {string} sourceName @param {Set<string>} available @returns {void} */
function classifyRelation(relation, sourceName, available) {
  if (relation.kind === 'subquery') return;
  if (relation.name === sourceName) {
    relation.kind = 'stream';
    return;
  }
  if (available.has(relation.name)) {
    relation.kind = 'cte';
    return;
  }
  throw new Error(`relation ${relation.name} is not the configured Stream or a prior CTE`);
}

/** @param {object} expression @param {number} index @returns {string} */
function projectionName(expression, index) {
  if (expression.kind === 'path') return expression.path.at(-1);
  return `column_${index + 1}`;
}

/** @param {object} expression @param {{record:unknown,sourceAlias:string,unnestValues:Map<string,unknown>}} scope @returns {unknown} */
function evaluateExpression(expression, scope) {
  if (expression.kind === 'literal') return expression.value;
  if (expression.kind === 'array') return expression.items.map((item) => cloneJson(evaluateExpression(item, scope)));
  if (expression.kind === 'unnest') throw new Error('UNNEST may only be evaluated as a SELECT projection');
  if (expression.kind === 'path') {
    const aliasValue = scope.unnestValues?.get(expression.path[0]);
    const path = aliasValue !== undefined
      ? expression.path.slice(1)
      : expression.path[0] === scope.sourceAlias ? expression.path.slice(1) : expression.path;
    let value = aliasValue === undefined ? scope.record : aliasValue;
    for (const key of path) {
      if (!value || typeof value !== 'object' || !Object.hasOwn(value, key)) return null;
      value = value[key];
    }
    return value;
  }
  if (expression.kind === 'unary') {
    const value = evaluateExpression(expression.value, scope);
    if (expression.operator === 'NOT') return !truthy(value);
    if (expression.operator === '+') return numeric(value, 'unary +');
    return -numeric(value, 'unary -');
  }
  if (expression.kind === 'binary') return evaluateBinary(expression.operator, evaluateExpression(expression.left, scope), evaluateExpression(expression.right, scope));
  if (expression.kind === 'function') return evaluateFunction(expression.name, expression.args.map((arg) => evaluateExpression(arg, scope)));
  throw new Error(`unknown expression node ${expression.kind}`);
}

/** @param {string} operator @param {unknown} left @param {unknown} right @returns {unknown} */
function evaluateBinary(operator, left, right) {
  if (operator === 'AND') return truthy(left) && truthy(right);
  if (operator === 'OR') return truthy(left) || truthy(right);
  if (operator === 'IS NULL') return left === null || left === undefined;
  if (operator === 'IS NOT NULL') return left !== null && left !== undefined;
  if (operator === 'LIKE') {
    if (left === null || right === null) return false;
    const pattern = String(right).replace(/[.+^${}()|[\]\\]/gu, '\\$&').replace(/%/gu, '.*').replace(/_/gu, '.');
    return new RegExp(`^${pattern}$`, 'u').test(String(left));
  }
  if (left === null || right === null || left === undefined || right === undefined) return false;
  if (operator === '||') return String(left) + String(right);
  if (['+', '-', '*', '/', '%'].includes(operator)) {
    const leftNumber = numeric(left, operator);
    const rightNumber = numeric(right, operator);
    const result = operator === '+' ? leftNumber + rightNumber
      : operator === '-' ? leftNumber - rightNumber
        : operator === '*' ? leftNumber * rightNumber
          : operator === '/' ? leftNumber / rightNumber
            : leftNumber % rightNumber;
    if (!Number.isFinite(result)) throw new Error(`Pipeline SQL ${operator} expression is not finite`);
    return result;
  }
  if (operator === '=' || operator === '!=' || operator === '<>') return operator === '=' ? left === right : left !== right;
  if (operator === '<') return left < right;
  if (operator === '<=') return left <= right;
  if (operator === '>') return left > right;
  if (operator === '>=') return left >= right;
  throw new Error(`unsupported Pipeline SQL operator ${operator}`);
}

/** @param {string} name @param {unknown[]} args @returns {unknown} */
function evaluateFunction(name, args) {
  if (['UPPER', 'LOWER', 'LENGTH', 'TRIM', 'ABS', 'ROUND', 'COALESCE', 'NULLIF', 'CONCAT'].includes(name) === false) throw new Error(`unsupported Pipeline SQL scalar function ${name}`);
  if (name === 'COALESCE') return args.find((value) => value !== null && value !== undefined) ?? null;
  if (name === 'NULLIF') return args.length === 2 && args[0] === args[1] ? null : (args[0] ?? null);
  if (name === 'CONCAT') return args.map((value) => value === null || value === undefined ? '' : String(value)).join('');
  if (name === 'ROUND' && args.length === 2) {
    if (args[0] === null || args[0] === undefined || !Number.isSafeInteger(args[1])) return null;
    const scale = 10 ** args[1];
    const rounded = Math.round(numeric(args[0], name) * scale) / scale;
    if (!Number.isFinite(rounded)) throw new Error('Pipeline SQL ROUND result is not finite');
    return rounded;
  }
  if (args.length !== 1 || args[0] === null || args[0] === undefined) return null;
  if (name === 'UPPER') return String(args[0]).toUpperCase();
  if (name === 'LOWER') return String(args[0]).toLowerCase();
  if (name === 'LENGTH') return String(args[0]).length;
  if (name === 'TRIM') return String(args[0]).trim();
  if (name === 'ABS') return Math.abs(numeric(args[0], name));
  if (name === 'ROUND') {
    if (args.length === 1) return Math.round(numeric(args[0], name));
    if (args.length === 2 && Number.isSafeInteger(args[1])) {
      const scale = 10 ** args[1];
      const rounded = Math.round(numeric(args[0], name) * scale) / scale;
      if (!Number.isFinite(rounded)) throw new Error('Pipeline SQL ROUND result is not finite');
      return rounded;
    }
    throw new Error('Pipeline SQL ROUND accepts one value and an optional integer scale');
  }
  return Math.round(numeric(args[0], name));
}

/** @param {unknown} value @param {string} operation @returns {number} */
function numeric(value, operation) {
  if (typeof value !== 'number' || !Number.isFinite(value)) throw new Error(`Pipeline SQL ${operation} requires finite numeric values`);
  return value;
}

/** @param {unknown} error @returns {string} */
function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

/** @param {unknown} value @param {number} status @returns {Response} */
function jsonResponse(value, status = 200) {
  return Response.json(value, { status });
}
