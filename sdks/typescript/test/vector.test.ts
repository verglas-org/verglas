import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { connect, VerglasClient } from "../src/index";
import { startMockEndpoint, type MockEndpoint } from "./mock-endpoint";

let endpoint: MockEndpoint;
let client: VerglasClient;

beforeEach(async () => {
  endpoint = await startMockEndpoint();
  client = connect({ endpoint: endpoint.url, token: endpoint.token });
});
afterEach(() => endpoint.close());

// A tiny 2-D corpus where the nearest neighbor of each query is obvious.
const corpus = [
  { id: 1, embedding: [0.0, 0.0] },
  { id: 2, embedding: [1.0, 0.0] },
  { id: 3, embedding: [0.0, 1.0] },
  { id: 4, embedding: [1.0, 1.0] },
];

describe("table vector index handle", () => {
  it("addIndex POSTs to /v1/tables/{t}/indexes with field + metric and returns the report", async () => {
    const t = client.table("docs");
    await t.append(corpus);
    const report = await t.addIndex("embedding", { metric: "l2" });
    expect(report.field).toBe("embedding");
    expect(report.metric).toBe("l2");
    expect(report.fullBuild).toBe(true);
    expect(report.liveCount).toBe(4);

    const post = endpoint.requests.find(
      (r) => r.method === "POST" && r.path === "/v1/tables/docs/indexes",
    );
    expect(post).toBeTruthy();
    expect((post!.body as { field: string }).field).toBe("embedding");
    expect((post!.body as { metric: string }).metric).toBe("l2");
  });

  it("searchIndex returns nearest neighbors from the index", async () => {
    const t = client.table("docs");
    await t.append(corpus);
    await t.addIndex("embedding", { metric: "l2" });

    const res = await t.searchIndex("embedding", [0.9, 0.1], { k: 2 });
    expect(res.source).toBe("index");
    expect(res.neighbors.length).toBe(2);
    // Nearest to (0.9, 0.1) is id 2 at (1,0).
    expect(res.neighbors[0].id).toBe(2);
    // Distances are sorted nearest-first.
    expect(res.neighbors[0].distance).toBeLessThanOrEqual(res.neighbors[1].distance);
  });

  it("searchIndex falls back to brute force when no index is declared (turn-off path)", async () => {
    const t = client.table("docs");
    await t.append(corpus);
    // No addIndex call.
    const res = await t.searchIndex("embedding", [0.1, 0.9], { k: 1 });
    expect(res.source).toBe("bruteForce");
    // Nearest to (0.1, 0.9) is id 3 at (0,1).
    expect(res.neighbors[0].id).toBe(3);
  });

  it("listIndexes returns the declared indexes", async () => {
    const t = client.table("docs");
    await t.append(corpus);
    await t.addIndex("embedding", { metric: "cosine" });
    const indexes = await t.listIndexes();
    expect(indexes.length).toBe(1);
    expect(indexes[0].field).toBe("embedding");
    expect(indexes[0].metric).toBe("cosine");
    expect(indexes[0].liveCount).toBe(4);
  });
});
