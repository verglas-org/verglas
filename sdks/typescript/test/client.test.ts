import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { connect, VerglasClient, VerglasHttpError } from "../src/index";
import { startMockEndpoint, type MockEndpoint } from "./mock-endpoint";

let endpoint: MockEndpoint;
let client: VerglasClient;

beforeEach(async () => {
  endpoint = await startMockEndpoint();
  client = connect({ endpoint: endpoint.url, token: endpoint.token });
});
afterEach(() => endpoint.close());

describe("connect", () => {
  it("requires endpoint and token", () => {
    expect(() => connect({ endpoint: "", token: "t" })).toThrow(/endpoint is required/);
    expect(() => connect({ endpoint: "http://x", token: "" })).toThrow(/token is required/);
  });

  it("rejects a bad token at request time", async () => {
    const bad = connect({ endpoint: endpoint.url, token: "wrong" });
    await expect(bad.table("t").snapshot()).rejects.toBeInstanceOf(VerglasHttpError);
  });
});

describe("createTable contract", () => {
  it("creates a table through the Iceberg REST catalog", async () => {
    const res = await client.createTable("rlean.custom_points", {
      schema: [
        { name: "value", type: "decimal128(38,18)", nullable: false },
        { name: "day", type: "date32", nullable: false },
        { name: "symbol_sid", type: "int64", nullable: true },
      ],
      partitions: [
        { source: "value", transform: "identity" },
        { source: "day", transform: "month" },
      ],
    });
    expect(res.table).toBe("rlean.custom_points");
    expect(res.columns).toEqual(["value", "day", "symbol_sid"]);

    const post = endpoint.requests.find(
      (r) => r.method === "POST" && r.path === "/v1/namespaces/rlean/tables",
    );
    expect(post).toBeTruthy();
    expect(post!.body).toMatchObject({ name: "custom_points", "stage-create": false });
  });

  it("requires a name and a non-empty schema", async () => {
    await expect(client.createTable("", { schema: [{ name: "a", type: "int64" }] })).rejects.toThrow(
      /name is required/,
    );
    await expect(client.createTable("t", { schema: [] })).rejects.toThrow(/schema is required/);
  });
});


describe("append -> ingest contract", () => {
  it("POSTs JSONL rows to /v1/ingest/{name} and surfaces the response", async () => {
    const res = await client.table("demo.job_runs").append([{ a: 1 }, { a: 2 }]);
    expect(res.rowsCommitted).toBe(2);
    expect(res.snapshotId).toMatch(/^snap-/);
    expect(res.idempotent).toBe(false);

    const ingest = endpoint.requests.find(
      (r) => r.method === "POST" && r.path === "/v1/ingest/demo.job_runs",
    );
    expect(ingest?.body).toEqual({
      rows: [{ a: 1 }, { a: 2 }],
      mode: "append",
      format: "jsonl",
    });
  });

  it("is idempotent under a repeated idempotency key", async () => {
    const first = await client.table("t").append([{ a: 1 }], { idempotencyKey: "k1" });
    const second = await client.table("t").append([{ a: 1 }], { idempotencyKey: "k1" });
    expect(first.idempotent).toBe(false);
    expect(second.idempotent).toBe(true);
    expect(second.snapshotId).toBe(first.snapshotId);
    expect(endpoint.tableState("t").rows).toHaveLength(1);
  });
});


describe("scan", () => {
  it("reads the current snapshot and pages via cursor", async () => {
    const t = client.table<{ n: number }>("t");
    await t.append([{ n: 0 }, { n: 1 }, { n: 2 }, { n: 3 }]);

    const p1 = await t.scan({ limit: 2 });
    expect(p1.rows).toEqual([{ n: 0 }, { n: 1 }]);
    expect(p1.nextCursor).toBeDefined();

    const p2 = await t.scan({ limit: 2, cursor: p1.nextCursor });
    expect(p2.rows).toEqual([{ n: 2 }, { n: 3 }]);
    expect(p2.nextCursor).toBeUndefined();
  });
});

describe("delta", () => {
  it("returns only rows committed after the watermark", async () => {
    const t = client.table<{ n: number }>("t");
    const a = await t.append([{ n: 1 }]);
    await t.append([{ n: 2 }, { n: 3 }]);

    const d = await t.delta(a.watermark);
    expect(d.rows).toEqual([{ n: 2 }, { n: 3 }]);

    const empty = await t.delta(d.watermark);
    expect(empty.rows).toEqual([]);
    expect(empty.watermark).toBe(d.watermark);
  });
});
