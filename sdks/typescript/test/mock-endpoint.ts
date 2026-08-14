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

const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

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

}

/** In-memory approximation of exclusive PostgreSQL queue leases. */
class MockQueue {
  records: {id: string; topic: string; payload: Record<string, unknown>}[] = [];
  claims = new Map<string, Set<number>>();

  enqueue(messages: {id: string; topic: string; payload: Record<string, unknown>}[]) {
    const positions: number[] = [];
    for (const message of messages) {
      positions.push(this.records.length);
      this.records.push(message);
    }
    return { positions };
  }

  poll(group: string, owner: string, topics: string[], max: number) {
    const claims = this.claims.get(group) ?? new Set<number>();
    this.claims.set(group, claims);
    const deliveries = this.records
      .map((message, position) => ({position, message}))
      .filter(({position, message}) => !claims.has(position) && topics.includes(message.topic))
      .slice(0, max)
      .map(({position, message}) => {
        claims.add(position);
        return {
          position,
          topic: message.topic,
          payload: message.payload,
          receipt: {position, owner, generation: 1},
          expiresAt: "2026-08-10T00:00:30Z",
        };
      });
    return {deliveries};
  }

  ack(_group: string, _receipt: {position: number; owner: string; generation: number}) {
    return undefined;
  }
}


export interface MockEndpoint {
  url: string;
  token: string;
  /** Access (or create) a table's in-memory state, e.g. to assert on it. */
  tableState(name: string): MockTable;
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
  const queues = new Map<string, MockQueue>();
  const queueState = (name: string): MockQueue => {
    let queue = queues.get(name);
    if (!queue) queues.set(name, (queue = new MockQueue()));
    return queue;
  };
  const requests: MockEndpoint["requests"] = [];
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

    if (/^\/v1\/databases\/[^/]+\/query$/.test(url.pathname) && req.method === "POST") {
      let raw = "";
      req.on("data", (c) => (raw += c));
      req.on("end", () => {
        const body = raw ? JSON.parse(raw) : {};
        requests.push({ method: "POST", path: url.pathname, body });
        send(200, { columns: ["id"], rows: [{ id: 1 }], row_count: 1 });
      });
      return;
    }

    if (url.pathname === "/admin/access" && req.method === "GET") {
      requests.push({ method: "GET", path: url.pathname });
      const { port } = server.address() as AddressInfo;
      return send(200, {
        catalog_uri: `http://127.0.0.1:${port}`,
        query_uri: `http://127.0.0.1:${port}`,
        s3_endpoint: "http://127.0.0.1:8333",
      });
    }


    // Iceberg REST catalog (same origin in tests via /admin/access.catalog_uri).
    if (url.pathname === "/v1/namespaces" && req.method === "POST") {
      let raw = "";
      req.on("data", (c) => (raw += c));
      req.on("end", () => {
        requests.push({ method: "POST", path: url.pathname, body: raw ? JSON.parse(raw) : {} });
        send(200, { namespace: [] });
      });
      return;
    }

    const catalogTable = url.pathname.match(/^\/v1\/namespaces\/([^/]+)\/tables(?:\/([^/]+))?$/);
    if (catalogTable) {
      const ns = decodeURIComponent(catalogTable[1]).split("\u001f");
      const tableName = catalogTable[2] ? decodeURIComponent(catalogTable[2]) : undefined;
      const dotted = tableName ? `${ns.join(".")}.${tableName}` : ns.join(".");
      if (req.method === "GET" && tableName) {
        requests.push({ method: "GET", path: url.pathname });
        const def = definitions.get(dotted);
        if (!def) return send(404, { error: "NoSuchTableException" });
        return send(200, {
          metadata: {
            schemas: [
              {
                fields: (def.schema as Array<{ name: string; type: string; nullable?: boolean }>).map(
                  (column, index) => ({
                    id: index + 1,
                    name: column.name,
                    required: column.nullable === false,
                    type: column.type === "int64" ? "long" : column.type === "utf8" ? "string" : column.type,
                  }),
                ),
              },
            ],
            "partition-specs": [
              {
                fields: ((def.partitions as Array<{ source: string; transform: string }>) ?? []).map(
                  (partition, index) => ({
                    "source-id":
                      (def.schema as Array<{ name: string }>).findIndex((c) => c.name === partition.source) + 1,
                    "field-id": 1000 + index,
                    transform: partition.transform,
                  }),
                ),
              },
            ],
          },
        });
      }
      if (req.method === "POST" && !tableName) {
        let raw = "";
        req.on("data", (c) => (raw += c));
        req.on("end", () => {
          const body = raw ? JSON.parse(raw) : {};
          requests.push({ method: "POST", path: url.pathname, body });
          const name = `${ns.join(".")}.${body.name}`;
          const schema = (body.schema?.fields ?? []).map(
            (field: { name: string; type: string; required?: boolean }) => ({
              name: field.name,
              type: field.type === "long" ? "int64" : field.type === "string" ? "utf8" : field.type,
              nullable: field.required !== true,
            }),
          );
          const partitions = (body["partition-spec"]?.fields ?? []).map(
            (field: { "source-id": number; transform: string }) => ({
              source: schema[field["source-id"] - 1]?.name ?? String(field["source-id"]),
              transform: field.transform,
            }),
          );
          definitions.set(name, { schema, partitions });
          tableState(name);
          send(200, { metadata: {} });
        });
        return;
      }
    }

    // POST /v1/ingest/:name — JSONL append used by the TypeScript SDK.
    const ingest = url.pathname.match(/^\/v1\/ingest\/([^/]+)$/);
    if (ingest && req.method === "POST") {
      const name = decodeURIComponent(ingest[1]);
      let raw = "";
      req.on("data", (c) => (raw += c));
      req.on("end", () => {
        const rows = raw
          .split("\n")
          .map((line) => line.trim())
          .filter(Boolean)
          .map((line) => JSON.parse(line));
        const key = req.headers["idempotency-key"];
        requests.push({
          method: "POST",
          path: url.pathname,
          body: { rows, mode: url.searchParams.get("mode"), format: url.searchParams.get("format") },
        });
        send(200, tableState(name).commit(rows, typeof key === "string" ? key : undefined));
      });
      return;
    }


    // /v1/queues/:name/:action — the queue output type.
    const q = url.pathname.match(/^\/v1\/queues\/([^/]+)\/(enqueue|poll|ack)$/);
    if (q) {
      const queue = queueState(decodeURIComponent(q[1]));
      const action = q[2];
      if (req.method === "POST") {
        let raw = "";
        req.on("data", (c) => (raw += c));
        req.on("end", () => {
          const body = raw ? JSON.parse(raw) : {};
          requests.push({ method: "POST", path: url.pathname, body });
          if (action === "enqueue") return send(200, queue.enqueue(body.messages ?? []));
          if (action === "poll") return send(200, queue.poll(body.group ?? "", body.owner ?? "", body.topics ?? [], Number(body.max ?? 256)));
          queue.ack(body.group ?? "", body.receipt);
          res.writeHead(204);
          return res.end();
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
