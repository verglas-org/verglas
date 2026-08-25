// An in-memory Verglas endpoint for tests: a real node:http server implementing
// the SDK's read/commit contract. node:http is used ONLY here in tests — the SDK
// itself uses global fetch and stays Worker/Node neutral.
//
// Model: every committed row gets a monotonic sequence number. The watermark is
// the highest sequence committed, as a string; a snapshot id is `snap-<n>`.
// `delta(since)` returns rows whose sequence is greater than `since`.

import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";

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



export interface MockEndpoint {
  url: string;
  token: string;
  /** Access (or create) a table's in-memory state, e.g. to assert on it. */
  tableState(name: string): MockTable;
  /** Requests seen, for assertions. */
  requests: { method: string; path: string; body?: unknown }[];
  close(): Promise<void>;
}

/** Starts the mock endpoint and resolves once it is listening. */
export async function startMockEndpoint(token = "test-token"): Promise<MockEndpoint> {
  const tables = new Map<string, MockTable>();
  const definitions = new Map<string, { schema: unknown[]; partitions: unknown[] }>();
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

  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;

  return {
    url: `http://127.0.0.1:${port}`,
    token,
    tableState,
    requests,
    close: () =>
      new Promise<void>((resolve) => {
        server.close(() => resolve());
      }),
  };
}
