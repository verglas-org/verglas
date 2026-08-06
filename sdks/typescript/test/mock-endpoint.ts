// An in-memory Verglas endpoint for tests: a real node:http server implementing
// the SDK's read/commit contract. node:http is used ONLY here in tests — the SDK
// itself uses global fetch and stays Worker/Node neutral.
//
// Model: every committed row gets a monotonic sequence number. The watermark is
// the highest sequence committed, as a string; a snapshot id is `snap-<n>`.
// `delta(since)` returns rows whose sequence is greater than `since`.

import { createServer, type IncomingMessage, type Server } from "node:http";
import type { AddressInfo, Socket } from "node:net";
import { createHash } from "node:crypto";
import { tableFromIPC } from "apache-arrow";

const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

function plainArrowValue(value: any): any {
  if (value && typeof value.toArray === "function") {
    return Array.from(value.toArray(), plainArrowValue);
  }
  if (Array.isArray(value)) return value.map(plainArrowValue);
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, plainArrowValue(item)]));
  }
  return value;
}

/** Encodes a text payload as a single unmasked server websocket frame. */
function encodeWsText(payload: string): Buffer {
  const body = Buffer.from(payload, "utf8");
  const len = body.length;
  let header: Buffer;
  if (len < 126) header = Buffer.from([0x81, len]);
  else if (len < 65536) header = Buffer.from([0x81, 126, (len >> 8) & 0xff, len & 0xff]);
  else {
    header = Buffer.alloc(10);
    header[0] = 0x81;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([header, body]);
}

interface StoredRow {
  seq: number;
  row: Record<string, unknown>;
}

class MockTable {
  rows: StoredRow[] = [];
  seq = 0;
  snapshotN = 0;
  /** idempotencyKey -> the response it first produced. */
  idem = new Map<string, { snapshotId: string; rowsCommitted: number; watermark: string }>();

  get watermark(): string {
    return String(this.seq);
  }
  get snapshotId(): string {
    return `snap-${this.snapshotN}`;
  }

  commit(rows: Record<string, unknown>[], idempotencyKey?: string) {
    if (idempotencyKey && this.idem.has(idempotencyKey)) {
      return { ...this.idem.get(idempotencyKey)!, idempotent: true };
    }
    for (const row of rows) this.rows.push({ seq: ++this.seq, row });
    this.snapshotN++;
    const result = { snapshotId: this.snapshotId, rowsCommitted: rows.length, watermark: this.watermark };
    if (idempotencyKey) this.idem.set(idempotencyKey, result);
    return { ...result, idempotent: false };
  }

  delta(since: number, limit?: number) {
    const newer = this.rows.filter((r) => r.seq > since);
    const page = limit ? newer.slice(0, limit) : newer;
    const watermark = page.length ? String(page[page.length - 1].seq) : String(since);
    return { rows: page.map((r) => r.row), watermark, snapshotId: this.snapshotId };
  }

  scan(limit?: number, cursor?: string) {
    const start = cursor ? Number(cursor) : 0;
    const slice = limit ? this.rows.slice(start, start + limit) : this.rows.slice(start);
    const end = start + slice.length;
    const nextCursor = end < this.rows.length ? String(end) : undefined;
    return { rows: slice.map((r) => r.row), watermark: this.watermark, snapshotId: this.snapshotId, nextCursor };
  }

  // --- vector indexes: exact search stands in for Vamana only after an index
  // attachment has been declared.
  indexes = new Map<string, { metric: string; idField: string }>();

  private liveVectors(field: string, idField: string): { id: number; vec: number[] }[] {
    const latest = new Map<number, number[] | null>();
    for (const { row } of this.rows) {
      const id = Number((row as Record<string, unknown>)[idField]);
      if (Number.isNaN(id)) continue;
      const v = (row as Record<string, unknown>)[field];
      latest.set(id, Array.isArray(v) ? (v as number[]) : null);
    }
    return [...latest.entries()]
      .filter(([, v]) => v !== null)
      .map(([id, v]) => ({ id, vec: v as number[] }));
  }

  declareIndex(field: string, metric: string, idField: string, name: string) {
    this.indexes.set(field, { metric, idField });
    const live = this.liveVectors(field, idField);
    return {
      target: `tbl:${name}`,
      field,
      metric,
      reflectedSnapshot: this.snapshotN,
      fullBuild: true,
      inserts: live.length,
      deletes: 0,
      consolidated: false,
      liveCount: live.length,
      tombstones: 0,
      blobLocation: `s3://warehouse/${name}/metadata/verglas-vamana-${this.snapshotN}.puffin`,
      blobBytes: 128 + live.length * 8,
    };
  }

  searchIndex(field: string, vector: number[], k: number, idField: string) {
    const declared = this.indexes.get(field);
    if (!declared) return undefined;
    const idCol = declared.idField ?? idField;
    const metric = declared.metric;
    const dist = (a: number[], b: number[]) => {
      if (metric === "cosine") {
        let dot = 0, na = 0, nb = 0;
        for (let i = 0; i < a.length; i++) { dot += a[i] * b[i]; na += a[i] * a[i]; nb += b[i] * b[i]; }
        return 1 - dot / (Math.sqrt(na) * Math.sqrt(nb) || 1);
      }
      let s = 0;
      for (let i = 0; i < a.length; i++) s += (a[i] - b[i]) ** 2;
      return Math.sqrt(s);
    };
    const neighbors = this.liveVectors(field, idCol)
      .map(({ id, vec }) => ({ id, distance: dist(vector, vec) }))
      .sort((x, y) => x.distance - y.distance)
      .slice(0, k);
    return { source: "index" as const, neighbors };
  }

  listIndexes(name: string) {
    return {
      indexes: [...this.indexes.entries()].map(([field, meta]) => ({
        target: `tbl:${name}`,
        field,
        metric: meta.metric,
        reflectedSnapshot: this.snapshotN,
        liveCount: this.liveVectors(field, meta.idField).length,
      })),
    };
  }
}

/** An in-memory queue mirroring the platform segment log: ordered records with
 *  stable positions and per-group watermarks, at-least-once. */
class MockQueue {
  records: Record<string, unknown>[] = [];
  groups = new Map<string, number>();

  enqueue(rows: Record<string, unknown>[]) {
    for (const row of rows) this.records.push(row);
    return { enqueued: rows.length, endPosition: this.records.length };
  }

  watermark(group: string): number {
    return this.groups.get(group) ?? 0;
  }

  poll(group: string, max?: number) {
    const from = this.watermark(group);
    const all = this.records
      .map((row, position) => ({ position, row }))
      .filter((r) => r.position >= from);
    const records = max ? all.slice(0, max) : all;
    return { records, watermark: from };
  }

  ack(group: string, position: number) {
    // Monotone: a regressing ack is ignored.
    if (position > this.watermark(group)) this.groups.set(group, position);
    return { watermark: this.watermark(group) };
  }
}

/** An in-memory graph mirroring the server's `/v1/graphs/...` routes: two node
 *  and edge lists plus an "index built" flag, with a scan-vs-index traversal
 *  that returns the same answers either way (the turn-off contract). */
class MockGraph {
  nodes: Record<string, unknown>[] = [];
  edges: Record<string, unknown>[] = [];
  snapshotN = 0;
  indexed = false;

  insertNodes(nodes: Record<string, unknown>[]) {
    for (const n of nodes) this.nodes.push(n);
    this.snapshotN++;
    return { snapshotId: this.snapshotN, count: nodes.length };
  }

  insertEdges(edges: Record<string, unknown>[]) {
    for (const e of edges) this.edges.push(e);
    this.snapshotN++;
    // A new edge snapshot invalidates any prior index binding.
    this.indexed = false;
    return { snapshotId: this.snapshotN, count: edges.length };
  }

  buildIndex() {
    const nodeIds = new Set<string>();
    for (const e of this.edges) {
      nodeIds.add(String(e.srcId));
      nodeIds.add(String(e.dstId));
    }
    this.indexed = true;
    return {
      built: this.edges.length > 0,
      snapshotId: this.snapshotN,
      nodeCount: nodeIds.size,
      edgeCount: this.edges.length,
      blobPath: "mock://index.puffin",
      blobBytes: 64,
      mode: "full",
    };
  }

  private backend(): string {
    return this.indexed ? "index" : "scan";
  }

  /** Direct out/in/both neighbors of `start` honoring a predicate filter. */
  private steps(start: string, direction: string, predicate?: string) {
    const out: { nodeId: string; edge: Record<string, unknown> }[] = [];
    for (const e of this.edges) {
      if (predicate && e.predicate !== predicate) continue;
      if ((direction === "out" || direction === "both") && e.srcId === start)
        out.push({ nodeId: String(e.dstId), edge: e });
      if ((direction === "in" || direction === "both") && e.dstId === start)
        out.push({ nodeId: String(e.srcId), edge: e });
    }
    return out;
  }

  query(body: Record<string, unknown>) {
    const op = String(body.op);
    const start = String(body.start);
    const direction = String(body.direction ?? "out");
    const filter = (body.filter ?? {}) as { predicate?: string };
    const base = { op, backend: this.backend(), snapshotId: this.snapshotN };

    if (op === "neighbors") {
      const neighbors = this.steps(start, direction, filter.predicate).map((s) => ({
        nodeId: s.nodeId,
        predicate: String(s.edge.predicate),
        confidence: Number(s.edge.confidence ?? 1),
        edgeId: String(s.edge.edgeId ?? ""),
        provenance: String(s.edge.provenance ?? ""),
        direction,
      }));
      return { ...base, neighbors };
    }
    if (op === "kHop") {
      const k = Number(body.k ?? 0);
      const reached = new Map<string, number>();
      let frontier = [start];
      for (let hop = 1; hop <= k; hop++) {
        const next: string[] = [];
        for (const node of frontier) {
          for (const s of this.steps(node, direction, filter.predicate)) {
            if (s.nodeId !== start && !reached.has(s.nodeId)) {
              reached.set(s.nodeId, hop);
              next.push(s.nodeId);
            }
          }
        }
        frontier = next;
      }
      return {
        ...base,
        reached: [...reached.entries()]
          .map(([nodeId, hops]) => ({ nodeId, hops, pathConfidence: 1 }))
          .sort((a, b) => a.hops - b.hops || a.nodeId.localeCompare(b.nodeId)),
      };
    }
    if (op === "paths") {
      const dst = String(body.dst);
      const maxHops = Number(body.maxHops ?? 0);
      // Breadth-first shortest path.
      const pred = new Map<string, string>();
      const seen = new Set<string>([start]);
      let frontier = [start];
      for (let hop = 0; hop < maxHops && !seen.has(dst); hop++) {
        const next: string[] = [];
        for (const node of frontier) {
          for (const s of this.steps(node, direction, filter.predicate)) {
            if (!seen.has(s.nodeId)) {
              seen.add(s.nodeId);
              pred.set(s.nodeId, node);
              next.push(s.nodeId);
            }
          }
        }
        frontier = next;
      }
      if (!seen.has(dst)) return { ...base, paths: [] };
      const nodes = [dst];
      let cur = dst;
      while (cur !== start) {
        cur = pred.get(cur)!;
        nodes.unshift(cur);
      }
      return { ...base, paths: [{ nodes, edges: [], confidence: 1 }] };
    }
    return base;
  }
}

export interface MockEndpoint {
  url: string;
  token: string;
  /** Access (or create) a table's in-memory state, e.g. to assert on it. */
  tableState(name: string): MockTable;
  /** Access (or create) a graph's in-memory state, e.g. to assert on it. */
  graphState(namespace: string): MockGraph;
  /** The single durable watermark cell (mirrors the control plane's per-deployment row). */
  watermark(): string | null;
  /** Requests seen, for assertions. */
  requests: { method: string; path: string; body?: unknown }[];
  /** Pushes a `change` frame to every attached change-feed socket. */
  pushChange(change: { seq: number; table: string; snapshotId?: string; committedAt?: string }): void;
  /** Resolves once at least `n` change-feed sockets have attached. */
  waitForFeed(n: number): Promise<void>;
  close(): Promise<void>;
}

/** Starts the mock endpoint and resolves once it is listening. */
export async function startMockEndpoint(token = "test-token"): Promise<MockEndpoint> {
  const tables = new Map<string, MockTable>();
  const definitions = new Map<string, { schema: unknown[]; partitions: unknown[] }>();
  const namespaces = new Set<string>();
  const queues = new Map<string, MockQueue>();
  const graphs = new Map<string, MockGraph>();
  const queueState = (name: string): MockQueue => {
    let q = queues.get(name);
    if (!q) queues.set(name, (q = new MockQueue()));
    return q;
  };
  const graphState = (namespace: string): MockGraph => {
    let g = graphs.get(namespace);
    if (!g) graphs.set(namespace, (g = new MockGraph()));
    return g;
  };
  const requests: MockEndpoint["requests"] = [];
  // A single durable watermark cell, as the control plane keeps one row per
  // deployment (the presented token identifies the deployment).
  let storedWatermark: string | null = null;
  const tableState = (name: string): MockTable => {
    let t = tables.get(name);
    if (!t) tables.set(name, (t = new MockTable()));
    return t;
  };

  const server: Server = createServer((req, res) => {
    const send = (code: number, body: unknown) => {
      res.writeHead(code, { "content-type": "application/json" });
      res.end(JSON.stringify(body));
    };

    if (req.headers.authorization !== `Bearer ${token}`) return send(401, { error: "bad token" });

    const url = new URL(req.url ?? "/", "http://localhost");

    if (url.pathname === "/admin/access" && req.method === "GET") {
      return send(200, {
        catalog_uri: `http://${req.headers.host}/catalog`,
        warehouse: "test",
      });
    }
    if (url.pathname === "/catalog/v1/config" && req.method === "GET") {
      return send(200, {overrides: {prefix: "test-prefix"}});
    }
    const catalogTable = url.pathname.match(
      /^\/catalog\/v1\/test-prefix\/namespaces\/([^/]+)\/tables\/([^/]+)$/,
    );
    if (catalogTable && req.method === "GET") {
      const namespace = decodeURIComponent(catalogTable[1]).replaceAll("\u001f", ".");
      const name = `${namespace}.${decodeURIComponent(catalogTable[2])}`;
      const definition = definitions.get(name);
      if (!definition) return send(404, {error: "missing table"});
      const fields = (definition.schema as Array<{name: string; type: string; nullable?: boolean}>)
        .map((field, index) => ({
          id: index + 1,
          name: field.name,
          required: !field.nullable,
          type: field.type === "int64" ? "long" : field.type === "date32" ? "date"
            : field.type === "utf8" ? "string" : field.type.replace(/^decimal128/, "decimal"),
        }));
      const ids = new Map(fields.map(field => [field.name, field.id]));
      return send(200, {metadata: {
        "current-schema-id": 0,
        schemas: [{"schema-id": 0, fields}],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, fields:
          (definition.partitions as Array<{source: string; transform: string}>).map(
            (partition, index) => ({
              "source-id": ids.get(partition.source),
              "field-id": 1000 + index,
              name: `${partition.source}_${partition.transform}`,
              transform: partition.transform,
            }),
          )}],
      }});
    }
    const catalogNamespaces = url.pathname === "/catalog/v1/test-prefix/namespaces";
    if (catalogNamespaces && req.method === "POST") {
      let raw = "";
      req.on("data", chunk => raw += chunk);
      req.on("end", () => {
        const body = JSON.parse(raw);
        const namespace = body.namespace.join(".");
        requests.push({method: "POST", path: url.pathname, body});
        if (namespaces.has(namespace)) return send(409, {error: "exists"});
        namespaces.add(namespace);
        send(200, {});
      });
      return;
    }
    const catalogCreate = url.pathname.match(
      /^\/catalog\/v1\/test-prefix\/namespaces\/([^/]+)\/tables$/,
    );
    if (catalogCreate && req.method === "POST") {
      let raw = "";
      req.on("data", chunk => raw += chunk);
      req.on("end", () => {
        const body = JSON.parse(raw);
        const namespace = decodeURIComponent(catalogCreate[1]).replaceAll("\u001f", ".");
        const name = `${namespace}.${body.name}`;
        requests.push({method: "POST", path: url.pathname, body});
        if (definitions.has(name)) return send(409, {error: "exists"});
        const fields = body.schema.fields.map((field: any) => ({
          name: field.name,
          type: field.type === "long" ? "int64" : field.type === "date" ? "date32"
            : field.type === "string" ? "utf8" : String(field.type).replace(/^decimal/, "decimal128"),
          nullable: !field.required,
        }));
        const names = new Map(body.schema.fields.map((field: any) => [field.id, field.name]));
        const partitions = body["partition-spec"].fields.map((field: any) => ({
          source: names.get(field["source-id"]),
          transform: field.transform,
        }));
        definitions.set(name, {schema: fields, partitions});
        tableState(name);
        send(200, {});
      });
      return;
    }

    if (url.pathname === "/v1/query" && req.method === "POST") {
      let raw = "";
      req.on("data", (c) => (raw += c));
      req.on("end", () => {
        const body = raw ? JSON.parse(raw) : {};
        requests.push({ method: "POST", path: url.pathname, body });
        send(200, { columns: ["id"], rows: [{ id: 1 }], row_count: 1 });
      });
      return;
    }

    const write = url.pathname.match(/^\/v1\/write\/([^/]+)$/);
    if (write && req.method === "POST") {
      const name = decodeURIComponent(write[1]);
      const chunks: Buffer[] = [];
      req.on("data", chunk => chunks.push(Buffer.from(chunk)));
      req.on("end", () => {
        const arrow = tableFromIPC(Buffer.concat(chunks));
        const rows = arrow.toArray().map(row =>
          plainArrowValue(row.toJSON()) as Record<string, unknown>,
        );
        requests.push({method: "POST", path: url.pathname, body: {rows}});
        send(200, tableState(name).commit(
          rows,
          typeof req.headers["idempotency-key"] === "string"
            ? req.headers["idempotency-key"]
            : undefined,
        ));
      });
      return;
    }

    // GET/PUT /v1/watermark — the deployment's durable cross-run watermark cell.
    if (url.pathname === "/v1/watermark") {
      if (req.method === "GET") {
        requests.push({ method: "GET", path: url.pathname });
        return send(200, { watermark: storedWatermark });
      }
      if (req.method === "PUT") {
        let raw = "";
        req.on("data", (c) => (raw += c));
        req.on("end", () => {
          const body = raw ? JSON.parse(raw) : {};
          requests.push({ method: "PUT", path: url.pathname, body });
          storedWatermark = typeof body.watermark === "string" ? body.watermark : storedWatermark;
          send(200, { watermark: storedWatermark });
        });
        return;
      }
      return send(405, { error: "method not allowed" });
    }

    // POST /v1/tables/:name — create a table from an explicit schema + partition
    // spec. Records the definition and echoes the column names.
    const create = url.pathname.match(/^\/v1\/tables\/([^/]+)$/);
    if (create && req.method === "POST") {
      const name = decodeURIComponent(create[1]);
      let raw = "";
      req.on("data", (c) => (raw += c));
      req.on("end", () => {
        const body = raw ? JSON.parse(raw) : {};
        requests.push({ method: "POST", path: url.pathname, body });
        tableState(name);
        definitions.set(name, { schema: body.schema ?? [], partitions: body.partitions ?? [] });
        const columns = (body.schema ?? []).map((c: { name: string }) => c.name);
        send(200, { table: name, columns });
      });
      return;
    }

    const definition = url.pathname.match(/^\/v1\/tables\/([^/]+)\/definition$/);
    if (definition && req.method === "GET") {
      const name = decodeURIComponent(definition[1]);
      requests.push({ method: "GET", path: url.pathname });
      const body = definitions.get(name);
      return body ? send(200, body) : send(404, { error: "missing table" });
    }

    // POST/GET /v1/tables/:name/indexes — declare or list vector indexes.
    const idxRoot = url.pathname.match(/^\/v1\/tables\/([^/]+)\/indexes$/);
    if (idxRoot) {
      const name = decodeURIComponent(idxRoot[1]);
      const table = tableState(name);
      if (req.method === "GET") {
        requests.push({ method: "GET", path: url.pathname });
        return send(200, table.listIndexes(name));
      }
      if (req.method === "POST") {
        let raw = "";
        req.on("data", (c) => (raw += c));
        req.on("end", () => {
          const body = raw ? JSON.parse(raw) : {};
          requests.push({ method: "POST", path: url.pathname, body });
          send(
            200,
            table.declareIndex(body.field, body.metric ?? "cosine", body.idField ?? "id", name),
          );
        });
        return;
      }
      return send(405, { error: "method not allowed" });
    }

    // POST /v1/tables/:name/indexes/:field/search — indexed ANN search.
    const idxSearch = url.pathname.match(/^\/v1\/tables\/([^/]+)\/indexes\/([^/]+)\/search$/);
    if (idxSearch && req.method === "POST") {
      const name = decodeURIComponent(idxSearch[1]);
      const field = decodeURIComponent(idxSearch[2]);
      const table = tableState(name);
      let raw = "";
      req.on("data", (c) => (raw += c));
      req.on("end", () => {
        const body = raw ? JSON.parse(raw) : {};
        requests.push({ method: "POST", path: url.pathname, body });
        const result = table.searchIndex(field, body.vector ?? [], body.k ?? 10, "id");
        if (!result) return send(404, { error: "no Vamana index attachment" });
        send(200, result);
      });
      return;
    }

    // /v1/graphs/:ns[/:action] — the graph verb family.
    const graphAction = url.pathname.match(/^\/v1\/graphs\/([^/]+)\/(nodes|edges|index|query)$/);
    if (graphAction && req.method === "POST") {
      const graph = graphState(decodeURIComponent(graphAction[1]));
      const action = graphAction[2];
      let raw = "";
      req.on("data", (c) => (raw += c));
      req.on("end", () => {
        const body = raw ? JSON.parse(raw) : {};
        requests.push({ method: "POST", path: url.pathname, body });
        if (action === "nodes") return send(200, graph.insertNodes(body.nodes ?? []));
        if (action === "edges") return send(200, graph.insertEdges(body.edges ?? []));
        if (action === "index") return send(200, graph.buildIndex());
        return send(200, graph.query(body));
      });
      return;
    }
    const graphRoot = url.pathname.match(/^\/v1\/graphs\/([^/]+)$/);
    if (graphRoot) {
      const graph = graphState(decodeURIComponent(graphRoot[1]));
      const ns = decodeURIComponent(graphRoot[1]);
      if (req.method === "POST") {
        let raw = "";
        req.on("data", (c) => (raw += c));
        req.on("end", () => {
          requests.push({ method: "POST", path: url.pathname });
          send(200, {
            namespace: ns,
            nodesTable: `${ns}.nodes`,
            edgesTable: `${ns}.edges`,
          });
        });
        return;
      }
      if (req.method === "GET") {
        requests.push({ method: "GET", path: url.pathname });
        return send(200, {
          namespace: ns,
          nodesTable: `${ns}.nodes`,
          edgesTable: `${ns}.edges`,
          nodeCount: graph.nodes.length,
          edgeCount: graph.edges.length,
          indexed: graph.indexed,
          snapshotId: graph.snapshotN,
        });
      }
      return send(405, { error: "method not allowed" });
    }

    // /v1/queues/:name/:action — the queue output type.
    const q = url.pathname.match(/^\/v1\/queues\/([^/]+)\/(enqueue|poll|ack)$/);
    if (q) {
      const queue = queueState(decodeURIComponent(q[1]));
      const action = q[2];
      if (req.method === "GET" && action === "poll") {
        requests.push({ method: "GET", path: url.pathname });
        const group = url.searchParams.get("group") ?? "";
        const max = url.searchParams.get("max");
        return send(200, queue.poll(group, max ? Number(max) : undefined));
      }
      if (req.method === "POST" && (action === "enqueue" || action === "ack")) {
        let raw = "";
        req.on("data", (c) => (raw += c));
        req.on("end", () => {
          const body = raw ? JSON.parse(raw) : {};
          requests.push({ method: "POST", path: url.pathname, body });
          if (action === "enqueue") return send(200, queue.enqueue(body.rows ?? []));
          return send(200, queue.ack(body.group ?? "", Number(body.position ?? 0)));
        });
        return;
      }
      return send(405, { error: "method not allowed" });
    }

    // /v1/tables/:name/:action  (name is a single URL-encoded segment)
    const m = url.pathname.match(/^\/v1\/tables\/([^/]+)\/(snapshot|rows|delta|commit)$/);
    if (!m) return send(404, { error: `no route for ${url.pathname}` });
    const name = decodeURIComponent(m[1]);
    const action = m[2];
    const table = tableState(name);

    if (req.method === "GET") {
      requests.push({ method: "GET", path: url.pathname });
      if (action === "snapshot") {
        return send(200, { snapshotId: table.snapshotId, watermark: table.watermark, recordCount: table.rows.length });
      }
      if (action === "rows") {
        const limit = url.searchParams.get("limit");
        return send(200, table.scan(limit ? Number(limit) : undefined, url.searchParams.get("cursor") ?? undefined));
      }
      if (action === "delta") {
        const since = Number(url.searchParams.get("since") ?? "0");
        const limit = url.searchParams.get("limit");
        return send(200, table.delta(since, limit ? Number(limit) : undefined));
      }
    }

    if (req.method === "POST" && action === "commit") {
      let raw = "";
      req.on("data", (c) => (raw += c));
      req.on("end", () => {
        const body = raw ? JSON.parse(raw) : {};
        requests.push({ method: "POST", path: url.pathname, body });
        const result = table.commit(body.rows ?? [], body.idempotencyKey);
        send(200, result);
      });
      return;
    }

    return send(405, { error: "method not allowed" });
  });

  // The catalog change feed lives at the same origin as the HTTP contract, so a
  // client's `follow`/`followRows` connects here. A minimal RFC 6455 endpoint:
  // handshake, send `hello`, discard client frames, and let a test push changes.
  const feedSockets: Socket[] = [];
  let feedWaiters: (() => void)[] = [];
  server.on("upgrade", (req: IncomingMessage, socket: Socket) => {
    if (req.url !== "/v1/catalog/feed" || req.headers.authorization !== `Bearer ${token}`) {
      socket.write("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n");
      socket.destroy();
      return;
    }
    const accept = createHash("sha1")
      .update((req.headers["sec-websocket-key"] ?? "") + WS_GUID)
      .digest("base64");
    socket.write(
      "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
        `Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
    );
    socket.on("data", () => void 0); // discard client subscribe frames
    socket.on("error", () => void 0);
    socket.on("close", () => {
      const i = feedSockets.indexOf(socket);
      if (i >= 0) feedSockets.splice(i, 1);
    });
    socket.write(encodeWsText(JSON.stringify({ type: "hello", cursor: 0 })));
    feedSockets.push(socket);
    const waiters = feedWaiters;
    feedWaiters = [];
    for (const w of waiters) w();
  });

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;

  return {
    url: `http://127.0.0.1:${port}`,
    token,
    tableState,
    graphState,
    watermark: () => storedWatermark,
    requests,
    pushChange: (change) => {
      const frame = encodeWsText(
        JSON.stringify({
          type: "change",
          seq: change.seq,
          table: change.table,
          snapshot_id: change.snapshotId ?? `snap-${change.seq}`,
          committed_at: change.committedAt ?? new Date().toISOString(),
        }),
      );
      for (const s of feedSockets) s.write(frame);
    },
    waitForFeed: (n: number): Promise<void> => {
      if (feedSockets.length >= n) return Promise.resolve();
      return new Promise<void>((resolve) => {
        const check = () => {
          if (feedSockets.length >= n) resolve();
          else feedWaiters.push(check);
        };
        feedWaiters.push(check);
      });
    },
    close: () =>
      new Promise<void>((resolve) => {
        for (const s of feedSockets) s.destroy();
        server.close(() => resolve());
      }),
  };
}
