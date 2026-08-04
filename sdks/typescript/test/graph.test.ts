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

describe("graph handle", () => {
  it("requires a namespace", () => {
    expect(() => client.graph("")).toThrow(/namespace is required/);
  });

  it("create POSTs to /v1/graphs/{ns} and returns the two backing tables", async () => {
    const res = await client.graph("kg").create();
    expect(res).toEqual({ namespace: "kg", nodesTable: "kg.nodes", edgesTable: "kg.edges" });
    const post = endpoint.requests.find((r) => r.method === "POST" && r.path === "/v1/graphs/kg");
    expect(post).toBeTruthy();
  });

  it("insertNodes and insertEdges POST the batch and return the snapshot + count", async () => {
    const g = client.graph("kg");
    await g.create();

    const nodes = await g.insertNodes([{ id: "a" }, { id: "b" }, { id: "c" }, { id: "d" }]);
    expect(nodes.count).toBe(4);
    expect(typeof nodes.snapshotId).toBe("number");

    const edges = await g.insertEdges([
      { srcId: "a", predicate: "knows", dstId: "b", provenance: "m1" },
      { srcId: "b", predicate: "knows", dstId: "c", provenance: "m2" },
      { srcId: "c", predicate: "knows", dstId: "d", provenance: "m3" },
      { srcId: "a", predicate: "met", dstId: "c", confidence: 0.5, provenance: "m4" },
    ]);
    expect(edges.count).toBe(4);

    const state = endpoint.graphState("kg");
    expect(state.nodes.length).toBe(4);
    expect(state.edges.length).toBe(4);
  });

  it("bad token surfaces a VerglasHttpError", async () => {
    const bad = connect({ endpoint: endpoint.url, token: "wrong" });
    await expect(bad.graph("kg").create()).rejects.toBeInstanceOf(VerglasHttpError);
  });
});

/** The most recent graph-query request the mock recorded. */
function lastQuery(endpoint: MockEndpoint): { method: string; path: string; body?: unknown } {
  const queries = endpoint.requests.filter(
    (r) => r.method === "POST" && r.path === "/v1/graphs/kg/query",
  );
  const post = queries[queries.length - 1];
  expect(post).toBeTruthy();
  return post;
}

/** Seeds the small a→b→c→d (knows) + a→c (met) graph. */
async function seed(client: VerglasClient) {
  const g = client.graph("kg");
  await g.create();
  await g.insertNodes([{ id: "a" }, { id: "b" }, { id: "c" }, { id: "d" }]);
  await g.insertEdges([
    { srcId: "a", predicate: "knows", dstId: "b", provenance: "m1" },
    { srcId: "b", predicate: "knows", dstId: "c", provenance: "m2" },
    { srcId: "c", predicate: "knows", dstId: "d", provenance: "m3" },
    { srcId: "a", predicate: "met", dstId: "c", confidence: 0.5, provenance: "m4" },
  ]);
  return g;
}

describe("graph traversal", () => {
  it("neighbors returns the direct out-neighbors", async () => {
    const g = await seed(client);
    const neighbors = await g.neighbors("a", { direction: "out" });
    const ids = neighbors.map((n) => n.nodeId).sort();
    expect(ids).toEqual(["b", "c"]);
  });

  it("neighbors honors a predicate filter", async () => {
    const g = await seed(client);
    const neighbors = await g.neighbors("a", { direction: "out", predicate: "met" });
    expect(neighbors.map((n) => n.nodeId)).toEqual(["c"]);
    // The filter travelled on the wire.
    const post = lastQuery(endpoint);
    expect((post.body as { filter?: { predicate?: string } }).filter?.predicate).toBe("met");
  });

  it("kHop reaches every node within the hop bound", async () => {
    const g = await seed(client);
    const reached = await g.kHop("a", { hops: 2, direction: "out" });
    expect(reached.map((r) => r.nodeId).sort()).toEqual(["b", "c", "d"]);
    // --hops is sent as k on the wire.
    const post = lastQuery(endpoint);
    expect((post.body as { k?: number }).k).toBe(2);
  });

  it("paths returns the shortest path", async () => {
    const g = await seed(client);
    const paths = await g.paths("a", "d", { maxHops: 3, direction: "out" });
    expect(paths.length).toBe(1);
    expect(paths[0].nodes).toEqual(["a", "c", "d"]);
  });
});

describe("graph index and turn-off equivalence", () => {
  it("buildIndex reports the graph size and mode", async () => {
    const g = await seed(client);
    const report = await g.buildIndex();
    expect(report.built).toBe(true);
    expect(report.edgeCount).toBe(4);
    expect(report.mode).toBe("full");
  });

  it("kHop returns the same result with and without an index (turn-off)", async () => {
    const g = await seed(client);

    // No index yet: served by scan.
    const before = await g.kHop("a", { hops: 3, direction: "out" });
    let show = await g.show();
    expect(show.indexed).toBe(false);

    // Build the index, then the same query is served by it.
    await g.buildIndex();
    show = await g.show();
    expect(show.indexed).toBe(true);
    const after = await g.kHop("a", { hops: 3, direction: "out" });

    expect(after).toEqual(before);
  });

  it("show reports counts and index state", async () => {
    const g = await seed(client);
    const show = await g.show();
    expect(show.nodesTable).toBe("kg.nodes");
    expect(show.edgesTable).toBe("kg.edges");
    expect(show.nodeCount).toBe(4);
    expect(show.edgeCount).toBe(4);
    expect(show.indexed).toBe(false);
  });
});
