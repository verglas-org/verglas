import { describe, expect, it } from "vitest";
import { S3VectorsClient, VerglasGraphsClient } from "../src/index";

// Reconstructing the signed request through fetch makes query ordering observable.
function captureFetch(requests: URL[]): typeof fetch {
  return (async (input: URL | RequestInfo) => {
    requests.push(new URL(String(input)));
    return new Response("{}", { status: 200, headers: { "content-type": "application/json" } });
  }) as typeof fetch;
}

describe("native semantic clients", () => {
  it("exports every checked-in REST-JSON operation", () => {
    expect(Object.getOwnPropertyNames(S3VectorsClient.prototype).sort()).toEqual(expect.arrayContaining([
      "createIndex", "createVectorBucket", "deleteIndex", "deleteVectorBucket",
      "deleteVectorBucketPolicy", "deleteVectors", "getIndex", "getVectorBucket",
      "getVectorBucketPolicy", "getVectors", "listIndexes", "listTagsForResource",
      "listVectorBuckets", "listVectors", "putVectorBucketPolicy", "putVectors",
      "queryVectors", "tagResource", "untagResource",
    ]));
    expect(Object.getOwnPropertyNames(VerglasGraphsClient.prototype).sort()).toEqual(expect.arrayContaining([
      "addEdges", "addNodes", "buildIndex", "createGraph", "deleteGraph", "describeGraph",
      "listGraphs", "retrieveNeighbors", "searchKHop", "searchNeighborhood", "searchPaths",
      "searchPrecedents",
    ]));
  });

  it("uses the cache listener SigV4 service names", () => {
    const credentials = { accessKeyId: "key", secretAccessKey: "secret" };
    expect(new S3VectorsClient("http://example.test", credentials).signingName()).toBe("s3vectors");
    expect(new VerglasGraphsClient("http://example.test", credentials).signingName()).toBe("verglasgraphs");
  });

  it("sends repeated tag keys as an empty-body request with AWS-encoded query ordering", async () => {
    const requests: URL[] = [];
    const client = new S3VectorsClient("http://example.test", { accessKeyId: "key", secretAccessKey: "secret" }, captureFetch(requests));
    await client.untagResource({ resourceArn: "arn:aws:s3vectors:bucket-a", tagKeys: ["z x", "a-b", "a/b"] });
    expect(requests[0].search).toBe("?tagKeys=z%20x&tagKeys=a-b&tagKeys=a%2Fb");
  });
});
