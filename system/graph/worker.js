/**
 * Prebuilt bounded property-graph Worker and Durable Object.
 * Each named graph owns relational node and edge rows, bidirectional adjacency
 * indexes, property-index declarations, and mutation receipts in one Turso DB.
 */

import { DurableObject } from 'cloudflare:workers';

export const UPSERT_NODES_PATH = '/graph/upsert-nodes';
export const UPSERT_EDGES_PATH = '/graph/upsert-edges';
export const GET_NODES_PATH = '/graph/get-nodes';
export const GET_EDGES_PATH = '/graph/get-edges';
export const DELETE_NODES_PATH = '/graph/delete-nodes';
export const DELETE_EDGES_PATH = '/graph/delete-edges';
export const NEIGHBORS_PATH = '/graph/neighbors';
export const SHORTEST_PATH = '/graph/shortest-path';
export const DESCRIBE_PATH = '/graph/describe';
export const PROPERTY_INDEX_CREATE_PATH = '/graph/property-index/create';
export const PROPERTY_INDEX_LIST_PATH = '/graph/property-index/list';
export const PROPERTY_INDEX_DELETE_PATH = '/graph/property-index/delete';

export const MAX_DEPTH = 8;
export const MAX_FRONTIER = 1000;
export const MAX_VISITED_NODES = 10_000;
export const MAX_SCANNED_EDGES = 50_000;

const CONFIG_TABLE = 'graph_config';
const NODE_TABLE = 'graph_nodes';
const EDGE_TABLE = 'graph_edges';
const MUTATION_TABLE = 'graph_mutations';
const PROPERTY_INDEX_TABLE = 'graph_property_indexes';
const MAX_BATCH = 1000;
const MAX_PROPERTIES_BYTES = 10 * 1024;
const MAX_PROPERTY_INDEXES = 20;
const MAX_REQUEST_BYTES = 12 * 1024 * 1024;
const MAX_RESPONSE_BYTES = 4 * 1024 * 1024;
const encoder = new TextEncoder();

/** Routes an optional authenticated HTTP surface to the configured graph object. */
async function fetch(request, env) {
  const failure = authorize(request, env);
  if (failure) return failure;
  const graphName = env.GRAPH_NAME;
  const namespace = env.GRAPH_DO;
  if (typeof graphName !== 'string' || graphName.trim() === '' || !namespace
      || typeof namespace.idFromName !== 'function' || typeof namespace.get !== 'function') {
    return jsonResponse({ error: 'Graph binding is not configured' }, 500);
  }
  return namespace.get(namespace.idFromName(graphName)).fetch(request);
}

/** One serialized Turso-backed named property graph. */
export class Graph extends DurableObject {
  /** Creates the schema and checks immutable graph identity before activation. */
  constructor(ctx, env) {
    super(ctx, env);
    this.#name = validateGraphName(env.GRAPH_NAME);
    this.#ready = ctx.blockConcurrencyWhile(async () => {
      await createTables(ctx);
      await installOrCheckConfiguration(ctx, this.#name);
    });
  }

  #name;
  #ready;

  /** Dispatches private binding and operator property-index routes. */
  async fetch(request) {
    await this.#ready;
    const url = new URL(request.url);
    if (request.method.toUpperCase() !== 'POST') return jsonResponse({ error: 'method not allowed' }, 405);
    try {
      if (url.pathname === UPSERT_NODES_PATH) return await this.#upsertNodes(request);
      if (url.pathname === UPSERT_EDGES_PATH) return await this.#upsertEdges(request);
      if (url.pathname === GET_NODES_PATH) return await this.#getNodes(request);
      if (url.pathname === GET_EDGES_PATH) return await this.#getEdges(request);
      if (url.pathname === DELETE_NODES_PATH) return await this.#deleteNodes(request);
      if (url.pathname === DELETE_EDGES_PATH) return await this.#deleteEdges(request);
      if (url.pathname === NEIGHBORS_PATH) return await this.#neighbors(request);
      if (url.pathname === SHORTEST_PATH) return await this.#shortestPath(request);
      if (url.pathname === DESCRIBE_PATH) return await this.#describe();
      if (url.pathname === PROPERTY_INDEX_CREATE_PATH) return await this.#createPropertyIndex(request);
      if (url.pathname === PROPERTY_INDEX_LIST_PATH) return await this.#listPropertyIndexes();
      if (url.pathname === PROPERTY_INDEX_DELETE_PATH) return await this.#deletePropertyIndex(request);
      return jsonResponse({ error: 'not found' }, 404);
    } catch (error) {
      return jsonResponse({ error: stableError(error) }, 400);
    }
  }

  /** Fully replaces a validated node batch and records one deterministic receipt. */
  async #upsertNodes(request) {
    const payload = await requestJson(request);
    rejectUnknownKeys(payload, new Set(['nodes']), 'upsert-nodes request');
    const nodes = validateNodes(payload.nodes);
    const mutationId = await stableMutationId('upsert-nodes', { nodes });
    if (await mutationExists(this.ctx, mutationId)) return jsonResponse({ mutationId });
    for (const node of nodes) {
      await execute(
        this.ctx,
        `INSERT INTO ${NODE_TABLE} (external_id, kind, properties_json, mutation_id)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(external_id) DO UPDATE SET kind = excluded.kind,
           properties_json = excluded.properties_json, mutation_id = excluded.mutation_id`,
        node.id,
        node.kind,
        node.properties === undefined ? null : JSON.stringify(node.properties),
        mutationId,
      );
    }
    await recordMutation(this.ctx, mutationId, 'upsert-nodes');
    return jsonResponse({ mutationId });
  }

  /** Fully replaces an edge batch after proving every endpoint exists. */
  async #upsertEdges(request) {
    const payload = await requestJson(request);
    rejectUnknownKeys(payload, new Set(['edges']), 'upsert-edges request');
    const edges = validateEdges(payload.edges);
    await requireEndpoints(this.ctx, edges);
    const mutationId = await stableMutationId('upsert-edges', { edges });
    if (await mutationExists(this.ctx, mutationId)) return jsonResponse({ mutationId });
    for (const edge of edges) {
      await execute(
        this.ctx,
        `INSERT INTO ${EDGE_TABLE}
           (external_id, from_id, to_id, kind, weight, properties_json, mutation_id)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(external_id) DO UPDATE SET from_id = excluded.from_id,
           to_id = excluded.to_id, kind = excluded.kind, weight = excluded.weight,
           properties_json = excluded.properties_json, mutation_id = excluded.mutation_id`,
        edge.id,
        edge.from,
        edge.to,
        edge.kind,
        edge.weight ?? null,
        edge.properties === undefined ? null : JSON.stringify(edge.properties),
        mutationId,
      );
    }
    await recordMutation(this.ctx, mutationId, 'upsert-edges');
    return jsonResponse({ mutationId });
  }

  /** Returns requested nodes in caller id order. */
  async #getNodes(request) {
    const payload = await requestJson(request);
    rejectUnknownKeys(payload, new Set(['ids']), 'get-nodes request');
    return jsonResponse(await getNodes(this.ctx, validateIds(payload.ids)));
  }

  /** Returns requested edges in caller id order. */
  async #getEdges(request) {
    const payload = await requestJson(request);
    rejectUnknownKeys(payload, new Set(['ids']), 'get-edges request');
    return jsonResponse(await getEdges(this.ctx, validateIds(payload.ids)));
  }

  /** Deletes nodes and all incident edges in one event transaction. */
  async #deleteNodes(request) {
    const payload = await requestJson(request);
    rejectUnknownKeys(payload, new Set(['ids']), 'delete-nodes request');
    const ids = validateIds(payload.ids);
    const mutationId = await stableMutationId('delete-nodes', { ids });
    if (await mutationExists(this.ctx, mutationId)) return jsonResponse({ mutationId });
    const placeholders = ids.map(() => '?').join(',');
    await execute(
      this.ctx,
      `DELETE FROM ${EDGE_TABLE} WHERE from_id IN (${placeholders}) OR to_id IN (${placeholders})`,
      ...ids,
      ...ids,
    );
    await execute(this.ctx, `DELETE FROM ${NODE_TABLE} WHERE external_id IN (${placeholders})`, ...ids);
    await recordMutation(this.ctx, mutationId, 'delete-nodes');
    return jsonResponse({ mutationId });
  }

  /** Deletes requested edges and records one deterministic receipt. */
  async #deleteEdges(request) {
    const payload = await requestJson(request);
    rejectUnknownKeys(payload, new Set(['ids']), 'delete-edges request');
    const ids = validateIds(payload.ids);
    const mutationId = await stableMutationId('delete-edges', { ids });
    if (await mutationExists(this.ctx, mutationId)) return jsonResponse({ mutationId });
    const placeholders = ids.map(() => '?').join(',');
    await execute(this.ctx, `DELETE FROM ${EDGE_TABLE} WHERE external_id IN (${placeholders})`, ...ids);
    await recordMutation(this.ctx, mutationId, 'delete-edges');
    return jsonResponse({ mutationId });
  }

  /** Returns one bounded deterministic breadth-first neighborhood. */
  async #neighbors(request) {
    const payload = await requestJson(request);
    const id = validateId(payload.id);
    const options = validateTraversalOptions(payload, false);
    await requireNodes(this.ctx, [id]);
    const visited = new Set([id]);
    const nodeIds = [];
    const edgeIds = [];
    let frontier = [id];
    let scanned = 0;
    let depthReached = 0;
    let limited = false;
    for (let depth = 1; depth <= options.depth && frontier.length > 0 && !limited; depth += 1) {
      const result = await expandFrontier(this.ctx, frontier, options, scanned);
      scanned += result.scanned;
      const next = [];
      for (const row of result.rows) {
        if (visited.has(row.next_id)) continue;
        visited.add(row.next_id);
        assertVisitedBudget(visited);
        next.push(row.next_id);
        nodeIds.push(row.next_id);
        edgeIds.push(row.id);
        if (nodeIds.length >= options.limit) {
          limited = true;
          break;
        }
      }
      frontier = [...new Set(next)].sort();
      if (frontier.length > MAX_FRONTIER) throw new Error(`Graph frontier exceeds ${MAX_FRONTIER} nodes`);
      depthReached = depth;
    }
    return jsonResponse({
      nodes: options.returnNodes ? await getNodes(this.ctx, nodeIds) : [],
      edges: options.returnEdges ? await getEdges(this.ctx, edgeIds) : [],
      depthReached,
    });
  }

  /** Returns one deterministic bounded unweighted shortest path. */
  async #shortestPath(request) {
    const payload = await requestJson(request);
    const from = validateId(payload.from);
    const to = validateId(payload.to);
    const options = validateTraversalOptions(payload, true);
    await requireNodes(this.ctx, [...new Set([from, to])]);
    if (from === to) {
      return jsonResponse({ found: true, nodes: await getNodes(this.ctx, [from]), edges: [], hops: 0 });
    }
    const visited = new Set([from]);
    const predecessors = new Map();
    let frontier = [from];
    let scanned = 0;
    let found = false;
    for (let depth = 1; depth <= options.maxDepth && frontier.length > 0 && !found; depth += 1) {
      const result = await expandFrontier(this.ctx, frontier, options, scanned);
      scanned += result.scanned;
      const next = [];
      for (const row of result.rows) {
        if (visited.has(row.next_id)) continue;
        visited.add(row.next_id);
        assertVisitedBudget(visited);
        predecessors.set(row.next_id, { node: row.source_id, edge: row.id });
        next.push(row.next_id);
        if (row.next_id === to) {
          found = true;
          break;
        }
      }
      frontier = [...new Set(next)].sort();
      if (frontier.length > MAX_FRONTIER) throw new Error(`Graph frontier exceeds ${MAX_FRONTIER} nodes`);
    }
    if (!found) return jsonResponse({ found: false, nodes: [], edges: [], hops: 0 });
    const nodeIds = [to];
    const edgeIds = [];
    let current = to;
    while (current !== from) {
      const predecessor = predecessors.get(current);
      if (!predecessor) throw new Error('Graph path reconstruction failed');
      edgeIds.push(predecessor.edge);
      current = predecessor.node;
      nodeIds.push(current);
    }
    nodeIds.reverse();
    edgeIds.reverse();
    return jsonResponse({
      found: true,
      nodes: await getNodes(this.ctx, nodeIds),
      edges: await getEdges(this.ctx, edgeIds),
      hops: edgeIds.length,
    });
  }

  /** Describes immutable identity, graph size, and latest mutation. */
  async #describe() {
    const nodes = await execute(this.ctx, `SELECT COUNT(*) AS count FROM ${NODE_TABLE}`);
    const edges = await execute(this.ctx, `SELECT COUNT(*) AS count FROM ${EDGE_TABLE}`);
    const mutation = await execute(
      this.ctx,
      `SELECT mutation_id FROM ${MUTATION_TABLE} ORDER BY sequence DESC LIMIT 1`,
    );
    return jsonResponse({
      name: this.#name,
      nodes: Number(nodes[0].count),
      edges: Number(edges[0].count),
      ...(mutation.length === 0 ? {} : { mutationId: mutation[0].mutation_id }),
    });
  }

  /** Declares one typed property and creates its matching Turso expression index. */
  async #createPropertyIndex(request) {
    const payload = await requestJson(request);
    rejectUnknownKeys(payload, new Set(['scope', 'propertyName', 'indexType']), 'property-index create request');
    const scope = validateScope(payload.scope);
    const propertyName = validatePropertyName(payload.propertyName);
    const indexType = validateIndexType(payload.indexType);
    const declarations = await propertyIndexes(this.ctx);
    const key = `${scope}:${propertyName}`;
    if (declarations.has(key)) {
      if (declarations.get(key) !== indexType) throw new Error('Graph property index configuration is immutable');
      return jsonResponse({ scope, propertyName, indexType });
    }
    if (declarations.size >= MAX_PROPERTY_INDEXES) {
      throw new Error(`Graph supports at most ${MAX_PROPERTY_INDEXES} property indexes`);
    }
    await execute(
      this.ctx,
      `INSERT INTO ${PROPERTY_INDEX_TABLE} (scope, property_name, index_type) VALUES (?, ?, ?)`,
      scope,
      propertyName,
      indexType,
    );
    await execute(this.ctx, propertyIndexCreateSql(scope, propertyName));
    return jsonResponse({ scope, propertyName, indexType });
  }

  /** Lists durable property-index declarations in stable order. */
  async #listPropertyIndexes() {
    const rows = await execute(
      this.ctx,
      `SELECT scope, property_name, index_type FROM ${PROPERTY_INDEX_TABLE}
       ORDER BY scope, property_name`,
    );
    return jsonResponse({
      propertyIndexes: rows.map((row) => ({
        scope: row.scope,
        propertyName: row.property_name,
        indexType: row.index_type,
      })),
    });
  }

  /** Deletes one declaration and its matching Turso expression index. */
  async #deletePropertyIndex(request) {
    const payload = await requestJson(request);
    rejectUnknownKeys(payload, new Set(['scope', 'propertyName']), 'property-index delete request');
    const scope = validateScope(payload.scope);
    const propertyName = validatePropertyName(payload.propertyName);
    await execute(this.ctx, `DROP INDEX IF EXISTS ${propertyIndexSqlName(scope, propertyName)}`);
    await execute(
      this.ctx,
      `DELETE FROM ${PROPERTY_INDEX_TABLE} WHERE scope = ? AND property_name = ?`,
      scope,
      propertyName,
    );
    return jsonResponse({ scope, propertyName });
  }
}

/** Creates authoritative graph tables and covering adjacency indexes. */
async function createTables(ctx) {
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${CONFIG_TABLE} (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1), graph_name TEXT NOT NULL)`);
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${NODE_TABLE} (
    external_id TEXT PRIMARY KEY, kind TEXT NOT NULL, properties_json TEXT,
    mutation_id TEXT NOT NULL)`);
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${EDGE_TABLE} (
    external_id TEXT PRIMARY KEY, from_id TEXT NOT NULL, to_id TEXT NOT NULL,
    kind TEXT NOT NULL, weight REAL, properties_json TEXT, mutation_id TEXT NOT NULL)`);
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${MUTATION_TABLE} (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT, mutation_id TEXT NOT NULL UNIQUE,
    operation TEXT NOT NULL)`);
  await execute(ctx, `CREATE TABLE IF NOT EXISTS ${PROPERTY_INDEX_TABLE} (
    scope TEXT NOT NULL, property_name TEXT NOT NULL, index_type TEXT NOT NULL,
    PRIMARY KEY(scope, property_name))`);
  await execute(ctx, `CREATE INDEX IF NOT EXISTS graph_nodes_kind ON ${NODE_TABLE}(kind, external_id)`);
  await execute(ctx, `CREATE INDEX IF NOT EXISTS graph_edges_out ON ${EDGE_TABLE}(from_id, kind, to_id, external_id)`);
  await execute(ctx, `CREATE INDEX IF NOT EXISTS graph_edges_in ON ${EDGE_TABLE}(to_id, kind, from_id, external_id)`);
}

/** Installs immutable graph identity or rejects a changed activation. */
async function installOrCheckConfiguration(ctx, graphName) {
  const rows = await execute(ctx, `SELECT graph_name FROM ${CONFIG_TABLE} WHERE singleton = 1`);
  if (rows.length === 0) {
    await execute(ctx, `INSERT INTO ${CONFIG_TABLE} (singleton, graph_name) VALUES (1, ?)`, graphName);
    return;
  }
  if (rows[0].graph_name !== graphName) throw new Error('Graph configuration is immutable');
}

/** Expands one frontier through indexed directed adjacency seeks. */
async function expandFrontier(ctx, frontier, options, alreadyScanned) {
  if (frontier.length > MAX_FRONTIER) throw new Error(`Graph frontier exceeds ${MAX_FRONTIER} nodes`);
  const directions = options.direction === 'both' ? ['out', 'in'] : [options.direction];
  const rows = [];
  let scanned = 0;
  for (const direction of directions) {
    const remaining = MAX_SCANNED_EDGES - alreadyScanned - scanned;
    if (remaining <= 0) throw new Error(`Graph traversal exceeds ${MAX_SCANNED_EDGES} scanned edges`);
    const result = await adjacencyRows(ctx, frontier, options, direction, remaining + 1);
    scanned += result.length;
    if (alreadyScanned + scanned > MAX_SCANNED_EDGES) {
      throw new Error(`Graph traversal exceeds ${MAX_SCANNED_EDGES} scanned edges`);
    }
    rows.push(...result);
  }
  rows.sort(compareAdjacencyRows);
  const unique = [];
  const seen = new Set();
  for (const row of rows) {
    const key = `${row.id}\0${row.source_id}\0${row.next_id}`;
    if (!seen.has(key)) {
      seen.add(key);
      unique.push(row);
    }
  }
  return { rows: unique, scanned };
}

/** Executes one outbound or inbound indexed adjacency query. */
async function adjacencyRows(ctx, frontier, options, direction, limit) {
  const frontierPlaceholders = frontier.map(() => '?').join(',');
  const sourceColumn = direction === 'out' ? 'e.from_id' : 'e.to_id';
  const nextColumn = direction === 'out' ? 'e.to_id' : 'e.from_id';
  const nodeJoin = direction === 'out' ? 'e.to_id' : 'e.from_id';
  const where = [`${sourceColumn} IN (${frontierPlaceholders})`];
  const bindings = [...frontier];
  if (options.edgeKinds !== undefined) {
    where.push(`e.kind IN (${options.edgeKinds.map(() => '?').join(',')})`);
    bindings.push(...options.edgeKinds);
  }
  const edgeFilter = await compilePropertyFilter(ctx, 'edge', options.edgeFilter, 'e');
  const nodeFilter = await compilePropertyFilter(ctx, 'node', options.nodeFilter, 'n');
  if (edgeFilter.sql !== '') where.push(edgeFilter.sql);
  if (nodeFilter.sql !== '') where.push(nodeFilter.sql);
  bindings.push(...edgeFilter.bindings, ...nodeFilter.bindings, limit);
  return execute(
    ctx,
    `SELECT e.external_id AS id, e.from_id, e.to_id, e.kind, e.weight,
            e.properties_json, ${sourceColumn} AS source_id, ${nextColumn} AS next_id
     FROM ${EDGE_TABLE} e JOIN ${NODE_TABLE} n ON n.external_id = ${nodeJoin}
     WHERE ${where.join(' AND ')}
     ORDER BY ${sourceColumn}, e.kind, ${nextColumn}, e.external_id LIMIT ?`,
    ...bindings,
  );
}

/** Provides total ordering for rows returned by both-direction traversal. */
function compareAdjacencyRows(left, right) {
  return compareText(left.source_id, right.source_id)
    || compareText(left.kind, right.kind)
    || compareText(left.next_id, right.next_id)
    || compareText(left.id, right.id);
}

/** Compares identifiers by code unit without depending on the host locale. */
function compareText(left, right) {
  const leftText = String(left);
  const rightText = String(right);
  if (leftText < rightText) return -1;
  if (leftText > rightText) return 1;
  return 0;
}

/** Compiles declared property filters with validated SQL fragments and bound values. */
async function compilePropertyFilter(ctx, scope, filter, alias) {
  if (filter === undefined) return { sql: '', bindings: [] };
  if (!filter || typeof filter !== 'object' || Array.isArray(filter)) throw new Error('Graph property filter must be an object');
  const declarations = await propertyIndexes(ctx);
  const clauses = [];
  const bindings = [];
  for (const [property, expression] of Object.entries(filter)) {
    validatePropertyName(property);
    const indexType = declarations.get(`${scope}:${property}`);
    if (indexType === undefined) throw new Error(`${scope} property is not indexed: ${property}`);
    const path = `$."${property}"`;
    const valueSql = `json_extract(${alias}.properties_json, '${path}')`;
    clauses.push(propertyTypeSql(alias, path, indexType));
    const operations = expression && typeof expression === 'object' && !Array.isArray(expression)
      ? Object.entries(expression)
      : [['$eq', expression]];
    if (operations.length !== 1) throw new Error('each Graph property filter requires one operator');
    const [operator, value] = operations[0];
    const sqlOperator = { $eq: '=', $ne: '!=', $lt: '<', $lte: '<=', $gt: '>', $gte: '>=' }[operator];
    if (sqlOperator !== undefined) {
      clauses.push(`${valueSql} ${sqlOperator} ?`);
      bindings.push(typedFilterValue(value, indexType));
      continue;
    }
    if (operator === '$in' || operator === '$nin') {
      if (!Array.isArray(value) || value.length < 1 || value.length > 100) {
        throw new Error(`${operator} requires between 1 and 100 property values`);
      }
      clauses.push(`${valueSql} ${operator === '$in' ? 'IN' : 'NOT IN'} (${value.map(() => '?').join(',')})`);
      bindings.push(...value.map((entry) => typedFilterValue(entry, indexType)));
      continue;
    }
    throw new Error(`unknown Graph property filter operator: ${operator}`);
  }
  return { sql: clauses.join(' AND '), bindings };
}

/** Restricts indexed JSON comparisons to the declared property type. */
function propertyTypeSql(alias, path, indexType) {
  const expression = `json_type(${alias}.properties_json, '${path}')`;
  if (indexType === 'string') return `${expression} = 'text'`;
  if (indexType === 'number') return `${expression} IN ('integer','real')`;
  return `${expression} IN ('true','false')`;
}

/** Validates one filter value against its declared property type. */
function typedFilterValue(value, indexType) {
  if (indexType === 'string' && typeof value === 'string') return value;
  if (indexType === 'number' && typeof value === 'number' && Number.isFinite(value)) return value;
  if (indexType === 'boolean' && typeof value === 'boolean') return value;
  throw new Error(`Graph filter value must match declared ${indexType} property type`);
}

/** Returns requested nodes in input order. */
async function getNodes(ctx, ids) {
  if (ids.length === 0) return [];
  const unique = [...new Set(ids)];
  const rows = await execute(
    ctx,
    `SELECT external_id AS id, kind, properties_json FROM ${NODE_TABLE}
     WHERE external_id IN (${unique.map(() => '?').join(',')})`,
    ...unique,
  );
  const byId = new Map(rows.map((row) => [row.id, storedNode(row)]));
  return ids.flatMap((id) => byId.has(id) ? [byId.get(id)] : []);
}

/** Returns requested edges in input order. */
async function getEdges(ctx, ids) {
  if (ids.length === 0) return [];
  const unique = [...new Set(ids)];
  const rows = await execute(
    ctx,
    `SELECT external_id AS id, from_id, to_id, kind, weight, properties_json
     FROM ${EDGE_TABLE} WHERE external_id IN (${unique.map(() => '?').join(',')})`,
    ...unique,
  );
  const byId = new Map(rows.map((row) => [row.id, storedEdge(row)]));
  return ids.flatMap((id) => byId.has(id) ? [byId.get(id)] : []);
}

/** Converts one node SQL row to the public record shape. */
function storedNode(row) {
  return {
    id: row.id,
    kind: row.kind,
    ...(row.properties_json === null || row.properties_json === undefined
      ? {}
      : { properties: JSON.parse(row.properties_json) }),
  };
}

/** Converts one edge SQL row to the public record shape. */
function storedEdge(row) {
  return {
    id: row.id,
    from: row.from_id,
    to: row.to_id,
    kind: row.kind,
    ...(row.weight === null || row.weight === undefined ? {} : { weight: Number(row.weight) }),
    ...(row.properties_json === null || row.properties_json === undefined
      ? {}
      : { properties: JSON.parse(row.properties_json) }),
  };
}

/** Proves every referenced endpoint exists before the first edge write. */
async function requireEndpoints(ctx, edges) {
  await requireNodes(ctx, [...new Set(edges.flatMap((edge) => [edge.from, edge.to]))]);
}

/** Rejects a missing node from one bounded identifier set. */
async function requireNodes(ctx, ids) {
  const placeholders = ids.map(() => '?').join(',');
  const rows = await execute(
    ctx,
    `SELECT external_id AS id FROM ${NODE_TABLE} WHERE external_id IN (${placeholders})`,
    ...ids,
  );
  const present = new Set(rows.map((row) => row.id));
  const missing = ids.find((id) => !present.has(id));
  if (missing !== undefined) throw new Error(`Graph node does not exist: ${missing}`);
}

/** Validates traversal options after rejecting operation-specific unknown fields. */
function validateTraversalOptions(payload, shortest) {
  const allowed = new Set(shortest
    ? ['from', 'to', 'direction', 'edgeKinds', 'maxDepth', 'nodeFilter', 'edgeFilter']
    : ['id', 'direction', 'edgeKinds', 'depth', 'limit', 'returnNodes', 'returnEdges', 'nodeFilter', 'edgeFilter']);
  rejectUnknownKeys(payload, allowed, shortest ? 'shortest-path request' : 'neighbors request');
  const direction = payload.direction === undefined ? 'out' : payload.direction;
  if (!['out', 'in', 'both'].includes(direction)) throw new Error('Graph direction must be out, in, or both');
  let edgeKinds;
  if (payload.edgeKinds !== undefined) {
    if (!Array.isArray(payload.edgeKinds) || payload.edgeKinds.length < 1 || payload.edgeKinds.length > 20) {
      throw new Error('Graph edgeKinds must contain between 1 and 20 kinds');
    }
    edgeKinds = payload.edgeKinds.map(validateKind);
  }
  const depthName = shortest ? 'maxDepth' : 'depth';
  const depth = payload[depthName] === undefined ? (shortest ? MAX_DEPTH : 1) : Number(payload[depthName]);
  if (!Number.isInteger(depth) || depth < 1 || depth > MAX_DEPTH) {
    throw new Error(`Graph ${depthName} must be an integer between 1 and ${MAX_DEPTH}`);
  }
  const options = {
    direction,
    edgeKinds,
    nodeFilter: payload.nodeFilter,
    edgeFilter: payload.edgeFilter,
    ...(shortest ? { maxDepth: depth } : { depth }),
  };
  if (!shortest) {
    const limit = payload.limit === undefined ? 100 : Number(payload.limit);
    if (!Number.isInteger(limit) || limit < 1 || limit > 1000) {
      throw new Error('Graph limit must be an integer between 1 and 1000');
    }
    options.limit = limit;
    options.returnNodes = payload.returnNodes !== false;
    options.returnEdges = payload.returnEdges === true;
  }
  return options;
}

/** Validates a non-empty node batch before issuing SQL writes. */
function validateNodes(nodes) {
  if (!Array.isArray(nodes) || nodes.length < 1 || nodes.length > MAX_BATCH) {
    throw new Error(`nodes must contain between 1 and ${MAX_BATCH} entries`);
  }
  return nodes.map((node) => {
    if (!node || typeof node !== 'object' || Array.isArray(node)) throw new Error('each node must be an object');
    rejectUnknownKeys(node, new Set(['id', 'kind', 'properties']), 'node');
    const result = { id: validateId(node.id), kind: validateKind(node.kind) };
    if (node.properties !== undefined) result.properties = validateProperties(node.properties);
    return result;
  });
}

/** Validates a non-empty edge batch before issuing SQL writes. */
function validateEdges(edges) {
  if (!Array.isArray(edges) || edges.length < 1 || edges.length > MAX_BATCH) {
    throw new Error(`edges must contain between 1 and ${MAX_BATCH} entries`);
  }
  return edges.map((edge) => {
    if (!edge || typeof edge !== 'object' || Array.isArray(edge)) throw new Error('each edge must be an object');
    rejectUnknownKeys(edge, new Set(['id', 'from', 'to', 'kind', 'weight', 'properties']), 'edge');
    const result = {
      id: validateId(edge.id),
      from: validateId(edge.from),
      to: validateId(edge.to),
      kind: validateKind(edge.kind),
    };
    if (edge.weight !== undefined) {
      if (typeof edge.weight !== 'number' || !Number.isFinite(edge.weight)) throw new Error('Graph edge weight must be finite');
      result.weight = edge.weight;
    }
    if (edge.properties !== undefined) result.properties = validateProperties(edge.properties);
    return result;
  });
}

/** Validates one optional property object and its encoded size. */
function validateProperties(properties) {
  if (!properties || typeof properties !== 'object' || Array.isArray(properties)) {
    throw new Error('Graph properties must be a JSON object');
  }
  assertJsonValue(properties, new WeakSet());
  if (encoder.encode(JSON.stringify(properties)).byteLength > MAX_PROPERTIES_BYTES) {
    throw new Error(`Graph properties exceed ${MAX_PROPERTIES_BYTES} bytes`);
  }
  return properties;
}

/** Recursively validates finite acyclic JSON properties. */
function assertJsonValue(value, ancestors) {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return;
  if (typeof value === 'number') {
    if (Number.isFinite(value)) return;
    throw new Error('Graph property numbers must be finite');
  }
  if (!value || typeof value !== 'object' || ancestors.has(value)) throw new Error('Graph properties must be acyclic JSON');
  ancestors.add(value);
  for (const entry of Array.isArray(value) ? value : Object.values(value)) assertJsonValue(entry, ancestors);
  ancestors.delete(value);
}

/** Validates one graph identifier. */
function validateId(id) {
  if (typeof id !== 'string' || id.length === 0 || encoder.encode(id).byteLength > 64) {
    throw new Error('Graph id must be a non-empty string of at most 64 bytes');
  }
  return id;
}

/** Validates one bounded id list. */
function validateIds(ids) {
  if (!Array.isArray(ids) || ids.length < 1 || ids.length > MAX_BATCH) {
    throw new Error(`ids must contain between 1 and ${MAX_BATCH} identifiers`);
  }
  return ids.map(validateId);
}

/** Validates a node or edge kind. */
function validateKind(kind) {
  if (typeof kind !== 'string' || kind.length === 0 || encoder.encode(kind).byteLength > 64) {
    throw new Error('Graph kind must be a non-empty string of at most 64 bytes');
  }
  return kind;
}

/** Validates immutable graph identity. */
function validateGraphName(name) {
  if (typeof name !== 'string' || name.trim() === '' || encoder.encode(name).byteLength > 128) {
    throw new Error('GRAPH_NAME must be a non-empty string of at most 128 bytes');
  }
  return name;
}

/** Validates one property-index scope. */
function validateScope(scope) {
  if (!['node', 'edge'].includes(scope)) throw new Error('Graph property-index scope must be node or edge');
  return scope;
}

/** Restricts property names before embedding them in index SQL. */
function validatePropertyName(property) {
  if (typeof property !== 'string' || !/^[A-Za-z_][A-Za-z0-9_]{0,63}$/u.test(property)) {
    throw new Error('Graph indexed property name must be an ASCII identifier of at most 64 characters');
  }
  return property;
}

/** Validates a declared property-index scalar type. */
function validateIndexType(indexType) {
  if (!['string', 'number', 'boolean'].includes(indexType)) {
    throw new Error('Graph property index type must be string, number, or boolean');
  }
  return indexType;
}

/** Returns durable property-index declarations keyed by scope and name. */
async function propertyIndexes(ctx) {
  const rows = await execute(
    ctx,
    `SELECT scope, property_name, index_type FROM ${PROPERTY_INDEX_TABLE}`,
  );
  return new Map(rows.map((row) => [`${row.scope}:${row.property_name}`, row.index_type]));
}

/** Builds safe expression-index DDL from a validated property identifier. */
function propertyIndexCreateSql(scope, propertyName) {
  const table = scope === 'node' ? NODE_TABLE : EDGE_TABLE;
  return `CREATE INDEX IF NOT EXISTS ${propertyIndexSqlName(scope, propertyName)}
          ON ${table}(json_extract(properties_json, '$."${propertyName}"'))`;
}

/** Returns a safe SQL index name after property validation. */
function propertyIndexSqlName(scope, propertyName) {
  return `graph_${scope}_property_${validatePropertyName(propertyName)}`;
}

/** Enforces the total visited-node ceiling. */
function assertVisitedBudget(visited) {
  if (visited.size > MAX_VISITED_NODES) throw new Error(`Graph traversal exceeds ${MAX_VISITED_NODES} visited nodes`);
}

/** Computes a replay-stable UUID-shaped mutation identity. */
async function stableMutationId(operation, payload) {
  const bytes = encoder.encode(JSON.stringify(canonicalJson([operation, payload])));
  const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', bytes));
  const hex = Array.from(digest, (value) => value.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
}

/** Sorts object keys recursively for stable retry identity. */
function canonicalJson(value) {
  if (Array.isArray(value)) return value.map(canonicalJson);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map((key) => [key, canonicalJson(value[key])]));
  }
  return value;
}

/** Checks whether the same mutation already committed. */
async function mutationExists(ctx, mutationId) {
  return (await execute(ctx, `SELECT 1 AS present FROM ${MUTATION_TABLE} WHERE mutation_id = ?`, mutationId)).length > 0;
}

/** Records one mutation after all state changes have been staged. */
async function recordMutation(ctx, mutationId, operation) {
  await execute(
    ctx,
    `INSERT INTO ${MUTATION_TABLE} (mutation_id, operation) VALUES (?, ?)`,
    mutationId,
    operation,
  );
}

/** Parses one bounded JSON request object. */
async function requestJson(request) {
  const bytes = new Uint8Array(await request.arrayBuffer());
  if (bytes.byteLength > MAX_REQUEST_BYTES) throw new Error(`Graph request exceeds ${MAX_REQUEST_BYTES} bytes`);
  const value = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Graph request body must be a JSON object');
  return value;
}

/** Rejects unknown keys from one exact operation shape. */
function rejectUnknownKeys(object, allowed, label) {
  for (const key of Object.keys(object)) {
    if (!allowed.has(key)) throw new Error(`unknown Graph ${label} key: ${key}`);
  }
}

/** Executes parameterized Durable Object SQL and materializes bounded rows. */
async function execute(ctx, statement, ...bindings) {
  return (await ctx.storage.sql.exec(statement, ...bindings)).toArray();
}

/** Produces one response after enforcing the output-byte ceiling. */
function jsonResponse(value, status = 200) {
  const body = JSON.stringify(value);
  if (encoder.encode(body).byteLength > MAX_RESPONSE_BYTES) {
    return Response.json({ error: `Graph response exceeds ${MAX_RESPONSE_BYTES} bytes` }, { status: 400 });
  }
  return new Response(body, { status, headers: { 'content-type': 'application/json' } });
}

/** Converts arbitrary thrown values to stable bounded public errors. */
function stableError(error) {
  const message = error instanceof Error ? error.message : String(error);
  return message.length <= 512 ? message : `${message.slice(0, 509)}...`;
}

/** Applies optional bearer authentication to the HTTP management surface. */
function authorize(request, env) {
  if (env.GRAPH_AUTH_TOKEN === undefined) return undefined;
  if (typeof env.GRAPH_AUTH_TOKEN !== 'string' || env.GRAPH_AUTH_TOKEN.length === 0) {
    return jsonResponse({ error: 'Graph authentication is misconfigured' }, 500);
  }
  return request.headers.get('authorization') === `Bearer ${env.GRAPH_AUTH_TOKEN}`
    ? undefined
    : jsonResponse({ error: 'unauthorized' }, 401);
}

export default { fetch };
