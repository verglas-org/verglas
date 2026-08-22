import { describe, expect, it } from "vitest";
import { Graph, graphFromEnv, VerglasGraphsClient } from "../src/index";

interface Sent {
  path: string;
  body: unknown;
}

/** Captures the operation path and decoded body of every signed request. */
function captureFetch(sent: Sent[]): typeof fetch {
  return (async (input: URL | RequestInfo, init?: RequestInit) => {
    sent.push({
      path: new URL(String(input)).pathname,
      body: JSON.parse(String(init?.body ?? "{}")),
    });
    return new Response("{}", { status: 200, headers: { "content-type": "application/json" } });
  }) as typeof fetch;
}

function graphOf(sent: Sent[]): Graph {
  const credentials = { accessKeyId: "key", secretAccessKey: "secret" };
  return new Graph(
    new VerglasGraphsClient("http://example.test", credentials, captureFetch(sent)),
    "rime-run",
  );
}

describe("Graph handle", () => {
  it("creates the graph it is bound to", async () => {
    const sent: Sent[] = [];
    await graphOf(sent).create();
    expect(sent[0]?.path).toBe("/CreateGraph");
    expect(sent[0]?.body).toEqual({ graphName: "rime-run" });
  });

  it("writes nodes through AddNodes, carrying the run namespace as a property", async () => {
    const sent: Sent[] = [];
    await graphOf(sent).insertNodes([
      { id: "rime/run/candidate/1", labels: ["RimeCandidate"], namespace: "run", properties: { score: 0.5 } },
    ]);
    expect(sent[0]?.path).toBe("/AddNodes");
    expect(sent[0]?.body).toEqual({
      graphName: "rime-run",
      nodes: [{
        id: "rime/run/candidate/1",
        labels: ["RimeCandidate"],
        properties: { score: 0.5, namespace: "run" },
      }],
    });
  });

  it("maps srcId and dstId onto the service's endpoint field names", async () => {
    const sent: Sent[] = [];
    await graphOf(sent).insertEdges([
      { srcId: "a", predicate: "derived_from", dstId: "b", provenance: "rime:run", namespace: "run" },
    ]);
    expect(sent[0]?.path).toBe("/AddEdges");
    expect(sent[0]?.body).toEqual({
      graphName: "rime-run",
      edges: [{
        sourceId: "a",
        targetId: "b",
        predicate: "derived_from",
        provenance: "rime:run",
        properties: { namespace: "run" },
      }],
    });
  });

  it("omits absent optional members rather than sending undefined", async () => {
    const sent: Sent[] = [];
    await graphOf(sent).insertNodes([{ id: "bare" }]);
    expect(sent[0]?.body).toEqual({ graphName: "rime-run", nodes: [{ id: "bare" }] });
  });
});

describe("graphFromEnv", () => {
  const credentials = {
    VERGLAS_ACCESS_KEY_ID: "key",
    VERGLAS_SECRET_ACCESS_KEY: "secret",
  };

  it("prefers the graph endpoint over the general one", async () => {
    const sent: Sent[] = [];
    const graph = graphFromEnv("g", {
      env: {
        ...credentials,
        VERGLAS_GRAPH_ENDPOINT: "http://graph.test",
        VERGLAS_ENDPOINT: "http://catalog.test",
      },
      fetch: captureFetch(sent),
    });
    await graph.create();
    expect(sent[0]?.path).toBe("/CreateGraph");
  });

  it("accepts the AWS credential names", () => {
    expect(() => graphFromEnv("g", {
      env: {
        VERGLAS_ENDPOINT: "http://example.test",
        AWS_ACCESS_KEY_ID: "key",
        AWS_SECRET_ACCESS_KEY: "secret",
      },
    })).not.toThrow();
  });

  it("names every accepted variable when configuration is missing", () => {
    expect(() => graphFromEnv("g", { env: credentials }))
      .toThrow(/VERGLAS_GRAPH_ENDPOINT or VERGLAS_S3_ENDPOINT or VERGLAS_ENDPOINT/);
    expect(() => graphFromEnv("g", { env: { VERGLAS_ENDPOINT: "http://example.test" } }))
      .toThrow(/VERGLAS_ACCESS_KEY_ID or AWS_ACCESS_KEY_ID/);
    expect(() => graphFromEnv("g", {
      env: { VERGLAS_ENDPOINT: "http://example.test", VERGLAS_ACCESS_KEY_ID: "key" },
    })).toThrow(/VERGLAS_SECRET_ACCESS_KEY or AWS_SECRET_ACCESS_KEY/);
  });

  it("treats a blank variable as unset", () => {
    expect(() => graphFromEnv("g", { env: { ...credentials, VERGLAS_ENDPOINT: "   " } }))
      .toThrow(/graph endpoint/);
  });
});
