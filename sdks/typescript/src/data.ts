// The /v0 data client: the ONE append-ingest shape (NDJSON to /v0/events) for
// table writes, vector writes, and log shipping alike, plus SQL through
// /v0/sql. This is the SDK speaking the Tinybird-compatible /v0 surface
// natively — a deliberate break from the older per-kind write shapes.

import { makeTransport, type Transport } from "./http";

const DEFAULT_TIMEOUT_MS = 30_000;

/** Options for `createDataClient`. */
export interface DataClientOptions {
  /** Base URL of the /v0 API (e.g. https://api.verglas.dev). */
  baseUrl: string;
  /** A Tinybird-style workspace bearer token. */
  token: string;
  /** Optional fetch implementation for non-browser runtimes and tests. */
  fetch?: typeof fetch;
  /** Request timeout in milliseconds. */
  timeoutMs?: number;
}

/** One ingest acknowledgement, as returned by POST /v0/events. */
export interface IngestResult {
  successful_rows: number;
  quarantined_rows: number;
}

/** The /v0 data client: append-ingest (one shape, every kind of row) plus SQL. */
export interface DataClient {
  /** Appends rows to a datasource as NDJSON via POST /v0/events?name=<datasource>. */
  appendEvents(datasource: string, rows: object[]): Promise<IngestResult>;
  /** Same append shape as `appendEvents` — a table write is just rows on a datasource. */
  tableWrite(datasource: string, rows: object[]): Promise<IngestResult>;
  /** Same append shape as `appendEvents` — a vector write is just rows on a datasource. */
  vectorWrite(datasource: string, rows: object[]): Promise<IngestResult>;
  /** Runs a query through POST /v0/sql. */
  sql<T = unknown>(query: string): Promise<T>;
}

/** Encodes rows as NDJSON: one compact JSON object per line, trailing newline. */
function ndjson(rows: object[]): string {
  return rows.map((row) => JSON.stringify(row)).join("\n") + (rows.length ? "\n" : "");
}

async function appendEvents(transport: Transport, datasource: string, rows: object[]): Promise<IngestResult> {
  const resp = await transport.requestRaw("POST", "/v0/events", {
    query: { name: datasource },
    body: ndjson(rows),
    headers: { "content-type": "application/x-ndjson" },
  });
  return (await resp.json()) as IngestResult;
}

/** Opens a /v0 data client bound to one endpoint + workspace token. */
export function createDataClient(options: DataClientOptions): DataClient {
  if (!options.baseUrl) throw new Error("createDataClient: baseUrl is required");
  if (!options.token) throw new Error("createDataClient: token is required");
  const fetchImpl = options.fetch ?? globalThis.fetch;
  if (typeof fetchImpl !== "function") {
    throw new Error("createDataClient: no global fetch; pass DataClientOptions.fetch");
  }
  const transport = makeTransport(
    options.baseUrl.replace(/\/+$/, ""),
    options.token,
    fetchImpl,
    options.timeoutMs ?? DEFAULT_TIMEOUT_MS,
  );

  return {
    appendEvents: (datasource, rows) => appendEvents(transport, datasource, rows),
    tableWrite: (datasource, rows) => appendEvents(transport, datasource, rows),
    vectorWrite: (datasource, rows) => appendEvents(transport, datasource, rows),
    sql: <T>(query: string) => transport.request<T>("POST", "/v0/sql", { body: { q: query } }),
  };
}
