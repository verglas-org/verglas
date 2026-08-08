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
  it("POSTs an explicit schema + partition spec to /v1/tables/{name}", async () => {
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
      (r) => r.method === "POST" && r.path === "/v1/tables/rlean.custom_points",
    );
    expect(post).toBeTruthy();
    expect(post!.body).toEqual({
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
  });

  it("requires a name and a non-empty schema", () => {
    expect(() => client.createTable("", { schema: [{ name: "a", type: "int64" }] })).toThrow(
      /name is required/,
    );
    expect(() => client.createTable("t", { schema: [] })).toThrow(/schema is required/);
  });
});

describe("shared SDK parity contract", () => {
  it("ensures exact table definitions and executes SQL through the shared routes", async () => {
    const definition = { schema: [{ name: "id", type: "int64", nullable: false }] };
    expect(await client.ensureTable("sdk.events", definition)).toBe("created");
    expect(await client.ensureTable("sdk.events", definition)).toBe("existing");
    await expect(
      client.ensureTable("sdk.events", {
        schema: [{ name: "id", type: "utf8", nullable: false }],
      }),
    ).rejects.toThrow(/definition mismatch/);

    const result = await client.query("select 1 as id");
    expect(result).toEqual({ columns: ["id"], rows: [{ id: 1 }], row_count: 1 });
  });
});

describe("append -> commit contract", () => {
  it("POSTs rows to /v1/tables/{name}/commit and surfaces the response", async () => {
    const res = await client.table("cloud.job_runs").append([{ a: 1 }, { a: 2 }]);
    expect(res.rowsCommitted).toBe(2);
    expect(res.snapshotId).toMatch(/^snap-/);
    expect(res.idempotent).toBe(false);

    const commit = endpoint.requests.find((r) => r.method === "POST");
    expect(commit?.path).toBe("/v1/tables/cloud.job_runs/commit");
    expect(commit?.body).toEqual({ rows: [{ a: 1 }, { a: 2 }] });
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

describe("followRows", () => {
  /** Waits until `pred` holds, polling briefly. */
  const until = async (pred: () => boolean, ms = 2000): Promise<void> => {
    const start = Date.now();
    while (!pred()) {
      if (Date.now() - start > ms) throw new Error("until: timed out");
      await new Promise((r) => setTimeout(r, 5));
    }
  };

  it("requires a non-empty table name", () => {
    expect(() => client.followRows("", () => {})).toThrow(/table name is required/);
  });

  it("delta-reads and delivers newly committed rows on each change, then stops", async () => {
    const t = client.table<{ n: number }>("t");
    await t.append([{ n: 0 }]); // pre-existing; the follow starts after this

    const seen: number[][] = [];
    const sub = client.followRows<{ n: number }>("t", (rows) => {
      seen.push(rows.map((r) => r.n));
    });

    await endpoint.waitForFeed(1);
    // Commit, then let the change feed wake a bounded delta read.
    await t.append([{ n: 1 }, { n: 2 }]);
    endpoint.pushChange({ seq: 1, table: "t" });
    await until(() => seen.flat().length >= 2);
    await t.append([{ n: 3 }]);
    endpoint.pushChange({ seq: 2, table: "t" });
    await until(() => seen.flat().length >= 3);

    sub.close();
    const flat = seen.flat();
    expect(flat).toEqual([1, 2, 3]); // 0 excluded (pre-existing), order preserved
  });

  it("can start from a supplied watermark", async () => {
    const t = client.table<{ n: number }>("t");
    const first = await t.append([{ n: 1 }]);
    await t.append([{ n: 2 }]);

    const seen: number[] = [];
    const sub = client.followRows<{ n: number }>(
      "t",
      (rows) => {
        seen.push(...rows.map((r) => r.n));
      },
      { fromWatermark: first.watermark },
    );
    await endpoint.waitForFeed(1);
    endpoint.pushChange({ seq: 1, table: "t" });
    await until(() => seen.length >= 1);
    sub.close();

    expect(seen).toEqual([2]); // everything committed after `first`
  });

  it("routes handler errors to onError and keeps following", async () => {
    const t = client.table<{ n: number }>("t");
    const errors: unknown[] = [];
    let calls = 0;
    const sub = client.followRows("t", () => {
      calls++;
      if (calls === 1) throw new Error("boom");
    }, { onError: (e) => errors.push(e) });

    await endpoint.waitForFeed(1);
    await t.append([{ n: 1 }]);
    endpoint.pushChange({ seq: 1, table: "t" });
    await until(() => calls >= 1);
    await t.append([{ n: 2 }]);
    endpoint.pushChange({ seq: 2, table: "t" });
    await until(() => calls >= 2);
    sub.close();

    expect(errors).toHaveLength(1);
    expect(calls).toBeGreaterThanOrEqual(2); // survived the first throw
  });
});
