// Native SigV4 clients for the cache listener semantic REST-JSON APIs.

/** One JSON object accepted or returned by a checked-in REST-JSON operation. */
export type SemanticDocument = Record<string, unknown>;
import type * as Model from "./semantic-types";

/** Credentials used to derive an AWS SigV4 request signature. */
export interface SigV4Credentials {
  /** Cache-node credential identifier. */
  accessKeyId: string;
  /** Secret used only by the local signer. */
  secretAccessKey: string;
  /** AWS-style scope region. Defaults to us-east-1. */
  region?: string;
}

/** A fetch-capable direct cache-listener client. */
class SemanticClient {
  /** Creates a service-bound semantic client. */
  constructor(
    private readonly endpoint: string,
    private readonly credentials: SigV4Credentials,
    private readonly service: "s3vectors" | "verglasgraphs",
    private readonly fetchImpl: typeof fetch = fetch,
  ) {}

  /** Signs and sends one exact model operation. */
  async call<T>(method: "GET" | "POST" | "DELETE", path: string, input: unknown = {}): Promise<T> {
    const url = new URL(path, this.endpoint.replace(/\/+$/, "") + "/");
    const body = method === "GET" || method === "DELETE" ? "" : JSON.stringify(input);
    const timestamp = amzDate(new Date());
    const date = timestamp.slice(0, 8);
    const hash = await sha256(body);
    const signed = "host;x-amz-content-sha256;x-amz-date";
    const canonical = method + "\n" + url.pathname + "\n" + canonicalQuery(url.searchParams) + "\nhost:" + url.host + "\nx-amz-content-sha256:" + hash + "\nx-amz-date:" + timestamp + "\n\n" + signed + "\n" + hash;
    const scope = date + "/" + (this.credentials.region ?? "us-east-1") + "/" + this.service + "/aws4_request";
    const text = "AWS4-HMAC-SHA256\n" + timestamp + "\n" + scope + "\n" + await sha256(canonical);
    const signature = await sign(this.credentials.secretAccessKey, date, this.credentials.region ?? "us-east-1", this.service, text);
    const response = await this.fetchImpl(url, {
      method,
      headers: {
        authorization: "AWS4-HMAC-SHA256 Credential=" + this.credentials.accessKeyId + "/" + scope + ", SignedHeaders=" + signed + ", Signature=" + signature,
        "content-type": "application/json",
        "x-amz-content-sha256": hash,
        "x-amz-date": timestamp,
      },
      ...(body ? { body } : {}),
    });
    if (!response.ok) throw new Error("semantic request failed: HTTP " + response.status + " — " + await response.text());
    return (await response.json()) as T;
  }
}

/** Direct client for the exact AWS S3 Vectors REST-JSON service model. */
export class S3VectorsClient {
  private readonly client: SemanticClient;
  /** Creates a client that signs with the s3vectors service name. */
  constructor(endpoint: string, credentials: SigV4Credentials, fetchImpl?: typeof fetch) { this.client = new SemanticClient(endpoint, credentials, "s3vectors", fetchImpl); }
  /** Required SigV4 signing name. */
  signingName(): "s3vectors" { return "s3vectors"; }
  /** Calls CreateIndex. */
  createIndex(input: Model.CreateIndexInput): Promise<Model.CreateIndexOutput> { return this.client.call("POST", "/CreateIndex", input) as Promise<Model.CreateIndexOutput>; }
  /** Calls CreateVectorBucket. */
  createVectorBucket(input: Model.CreateVectorBucketInput): Promise<Model.CreateVectorBucketOutput> { return this.client.call("POST", "/CreateVectorBucket", input) as Promise<Model.CreateVectorBucketOutput>; }
  /** Calls DeleteIndex. */
  deleteIndex(input: Model.DeleteIndexInput): Promise<Model.DeleteIndexOutput> { return this.client.call("POST", "/DeleteIndex", input) as Promise<Model.DeleteIndexOutput>; }
  /** Calls DeleteVectorBucket. */
  deleteVectorBucket(input: Model.DeleteVectorBucketInput): Promise<Model.DeleteVectorBucketOutput> { return this.client.call("POST", "/DeleteVectorBucket", input) as Promise<Model.DeleteVectorBucketOutput>; }
  /** Calls DeleteVectorBucketPolicy. */
  deleteVectorBucketPolicy(input: Model.DeleteVectorBucketPolicyInput): Promise<Model.DeleteVectorBucketPolicyOutput> { return this.client.call("POST", "/DeleteVectorBucketPolicy", input) as Promise<Model.DeleteVectorBucketPolicyOutput>; }
  /** Calls DeleteVectors. */
  deleteVectors(input: Model.DeleteVectorsInput): Promise<Model.DeleteVectorsOutput> { return this.client.call("POST", "/DeleteVectors", input) as Promise<Model.DeleteVectorsOutput>; }
  /** Calls GetIndex. */
  getIndex(input: Model.GetIndexInput): Promise<Model.GetIndexOutput> { return this.client.call("POST", "/GetIndex", input) as Promise<Model.GetIndexOutput>; }
  /** Calls GetVectorBucket. */
  getVectorBucket(input: Model.GetVectorBucketInput): Promise<Model.GetVectorBucketOutput> { return this.client.call("POST", "/GetVectorBucket", input) as Promise<Model.GetVectorBucketOutput>; }
  /** Calls GetVectorBucketPolicy. */
  getVectorBucketPolicy(input: Model.GetVectorBucketPolicyInput): Promise<Model.GetVectorBucketPolicyOutput> { return this.client.call("POST", "/GetVectorBucketPolicy", input) as Promise<Model.GetVectorBucketPolicyOutput>; }
  /** Calls GetVectors. */
  getVectors(input: Model.GetVectorsInput): Promise<Model.GetVectorsOutput> { return this.client.call("POST", "/GetVectors", input) as Promise<Model.GetVectorsOutput>; }
  /** Calls ListIndexes. */
  listIndexes(input: Model.ListIndexesInput): Promise<Model.ListIndexesOutput> { return this.client.call("POST", "/ListIndexes", input) as Promise<Model.ListIndexesOutput>; }
  /** Calls ListTagsForResource. */
  listTagsForResource(input: Model.ListTagsForResourceInput): Promise<Model.ListTagsForResourceOutput> { return this.client.call("GET", "/tags/" + encodeURIComponent(input.resourceArn), input) as Promise<Model.ListTagsForResourceOutput>; }
  /** Calls ListVectorBuckets. */
  listVectorBuckets(input: Model.ListVectorBucketsInput): Promise<Model.ListVectorBucketsOutput> { return this.client.call("POST", "/ListVectorBuckets", input) as Promise<Model.ListVectorBucketsOutput>; }
  /** Calls ListVectors. */
  listVectors(input: Model.ListVectorsInput): Promise<Model.ListVectorsOutput> { return this.client.call("POST", "/ListVectors", input) as Promise<Model.ListVectorsOutput>; }
  /** Calls PutVectorBucketPolicy. */
  putVectorBucketPolicy(input: Model.PutVectorBucketPolicyInput): Promise<Model.PutVectorBucketPolicyOutput> { return this.client.call("POST", "/PutVectorBucketPolicy", input) as Promise<Model.PutVectorBucketPolicyOutput>; }
  /** Calls PutVectors. */
  putVectors(input: Model.PutVectorsInput): Promise<Model.PutVectorsOutput> { return this.client.call("POST", "/PutVectors", input) as Promise<Model.PutVectorsOutput>; }
  /** Calls QueryVectors. */
  queryVectors(input: Model.QueryVectorsInput): Promise<Model.QueryVectorsOutput> { return this.client.call("POST", "/QueryVectors", input) as Promise<Model.QueryVectorsOutput>; }
  /** Calls TagResource. */
  tagResource(input: Model.TagResourceInput): Promise<Model.TagResourceOutput> { return this.client.call("POST", "/tags/" + encodeURIComponent(input.resourceArn), input) as Promise<Model.TagResourceOutput>; }
  /** Calls UntagResource. */
  untagResource(input: Model.UntagResourceInput): Promise<Model.UntagResourceOutput> { return this.client.call("DELETE", "/tags/" + encodeURIComponent(input.resourceArn) + "?" + input.tagKeys.map((key) => "tagKeys=" + encodeURIComponent(key)).join("&"), input) as Promise<Model.UntagResourceOutput>; }
}

/** Direct client for the exact Verglas Graph REST-JSON service model. */
export class VerglasGraphsClient {
  private readonly client: SemanticClient;
  /** Creates a client that signs with the verglasgraphs service name. */
  constructor(endpoint: string, credentials: SigV4Credentials, fetchImpl?: typeof fetch) { this.client = new SemanticClient(endpoint, credentials, "verglasgraphs", fetchImpl); }
  /** Required SigV4 signing name. */
  signingName(): "verglasgraphs" { return "verglasgraphs"; }
  /** Calls BuildIndex. */
  buildIndex(input: Model.BuildIndexInput): Promise<Model.BuildIndexOutput> { return this.client.call("POST", "/BuildIndex", input) as Promise<Model.BuildIndexOutput>; }
  /** Calls CreateGraph. */
  createGraph(input: Model.CreateGraphInput): Promise<Model.CreateGraphOutput> { return this.client.call("POST", "/CreateGraph", input) as Promise<Model.CreateGraphOutput>; }
  /** Calls DeleteGraph. */
  deleteGraph(input: Model.DeleteGraphInput): Promise<Model.DeleteGraphOutput> { return this.client.call("POST", "/DeleteGraph", input) as Promise<Model.DeleteGraphOutput>; }
  /** Calls DescribeGraph. */
  describeGraph(input: Model.DescribeGraphInput): Promise<Model.DescribeGraphOutput> { return this.client.call("POST", "/DescribeGraph", input) as Promise<Model.DescribeGraphOutput>; }
  /** Calls RetrieveNeighbors. */
  retrieveNeighbors(input: Model.RetrieveNeighborsInput): Promise<Model.RetrieveNeighborsOutput> { return this.client.call("POST", "/RetrieveNeighbors", input) as Promise<Model.RetrieveNeighborsOutput>; }
  /** Calls ListGraphs. */
  listGraphs(input: Model.ListGraphsInput): Promise<Model.ListGraphsOutput> { return this.client.call("POST", "/ListGraphs", input) as Promise<Model.ListGraphsOutput>; }
  /** Calls AddEdges. */
  addEdges(input: Model.AddEdgesInput): Promise<Model.AddEdgesOutput> { return this.client.call("POST", "/AddEdges", input) as Promise<Model.AddEdgesOutput>; }
  /** Calls AddNodes. */
  addNodes(input: Model.AddNodesInput): Promise<Model.AddNodesOutput> { return this.client.call("POST", "/AddNodes", input) as Promise<Model.AddNodesOutput>; }
  /** Calls SearchKHop. */
  searchKHop(input: Model.SearchKHopInput): Promise<Model.SearchKHopOutput> { return this.client.call("POST", "/SearchKHop", input) as Promise<Model.SearchKHopOutput>; }
  /** Calls SearchNeighborhood. */
  searchNeighborhood(input: Model.SearchNeighborhoodInput): Promise<Model.SearchNeighborhoodOutput> { return this.client.call("POST", "/SearchNeighborhood", input) as Promise<Model.SearchNeighborhoodOutput>; }
  /** Calls SearchPaths. */
  searchPaths(input: Model.SearchPathsInput): Promise<Model.SearchPathsOutput> { return this.client.call("POST", "/SearchPaths", input) as Promise<Model.SearchPathsOutput>; }
  /** Calls SearchPrecedents. */
  searchPrecedents(input: Model.SearchPrecedentsInput): Promise<Model.SearchPrecedentsOutput> { return this.client.call("POST", "/SearchPrecedents", input) as Promise<Model.SearchPrecedentsOutput>; }
}

/** Formats the UTC timestamp AWS SigV4 places in requests. */
function amzDate(now: Date): string {
  const two = (value: number) => value.toString().padStart(2, "0");
  return String(now.getUTCFullYear()) + two(now.getUTCMonth() + 1) + two(now.getUTCDate()) + "T" + two(now.getUTCHours()) + two(now.getUTCMinutes()) + two(now.getUTCSeconds()) + "Z";
}

/** Returns a hexadecimal SHA-256 digest through browser-compatible Web Crypto. */
async function sha256(value: string): Promise<string> { return hex(new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value)))); }

/** Derives the SigV4 signing key and signs one string-to-sign. */
async function sign(secret: string, date: string, region: string, service: string, value: string): Promise<string> {
  const dateKey = await hmac(new TextEncoder().encode("AWS4" + secret), date);
  const regionKey = await hmac(dateKey, region);
  const serviceKey = await hmac(regionKey, service);
  return hex(await hmac(await hmac(serviceKey, "aws4_request"), value));
}

/** Computes one browser-compatible HMAC-SHA256 digest. */
async function hmac(key: Uint8Array, value: string): Promise<Uint8Array> {
  const imported = await crypto.subtle.importKey("raw", key as BufferSource, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return new Uint8Array(await crypto.subtle.sign("HMAC", imported, new TextEncoder().encode(value)));
}

/** Renders a byte sequence as lowercase hexadecimal. */
function hex(bytes: Uint8Array): string { return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join(""); }

/** AWS-encodes a decoded URI/query component without escaping AWS-unreserved bytes. */
function awsEncode(value: string): string {
  return Array.from(new TextEncoder().encode(value), (byte) =>
    (byte >= 0x41 && byte <= 0x5a) || (byte >= 0x61 && byte <= 0x7a) || (byte >= 0x30 && byte <= 0x39) || byte === 0x2d || byte === 0x5f || byte === 0x2e || byte === 0x7e
      ? String.fromCharCode(byte)
      : "%" + byte.toString(16).toUpperCase().padStart(2, "0"),
  ).join("");
}

/** Canonicalizes decoded query fields by encoded AWS byte ordering. */
function canonicalQuery(params: URLSearchParams): string {
  return Array.from(params.entries(), ([key, value]) => [awsEncode(key), awsEncode(value)] as const)
    .sort(([leftKey, leftValue], [rightKey, rightValue]) =>
      leftKey < rightKey ? -1 : leftKey > rightKey ? 1 : leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0,
    )
    .map(([key, value]) => key + "=" + value)
    .join("&");
}
