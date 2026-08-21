/**
 * Graph handle bound to one graph name, exposing the node and edge writes the
 * RIME state store drives (`create`, `insertNodes`, `insertEdges`) over the
 * generated Verglas Graph operations.
 *
 * RIME names an edge's endpoints `srcId`/`dstId` and carries a per-run
 * `namespace` on both nodes and edges; the service model names the endpoints
 * `sourceId`/`targetId` and carries no namespace field. This handle performs
 * that translation in one place so callers keep RIME's vocabulary and the wire
 * keeps the service's.
 */

import type * as Model from "./semantic-types.js";
import { VerglasGraphsClient } from "./semantic.js";

/**
 * Environment names the graph endpoint is read from, in precedence order.
 * Graph and S3 Vectors are served by the cache node's S3 listener, which is a
 * different endpoint from the bearer/catalog service, so a graph-specific name
 * wins over the general one.
 */
const ENDPOINT_VARS = [
  "VERGLAS_GRAPH_ENDPOINT",
  "VERGLAS_S3_ENDPOINT",
  "VERGLAS_ENDPOINT",
] as const;

/** Environment names the SigV4 access key id is read from, in precedence order. */
const ACCESS_KEY_VARS = ["VERGLAS_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"] as const;

/** Environment names the SigV4 secret key is read from, in precedence order. */
const SECRET_KEY_VARS = ["VERGLAS_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY"] as const;

/** Options for constructing a graph client from the environment. */
export interface GraphFromEnvOptions {
  /** Variable source; defaults to the process environment. */
  env?: Record<string, string | undefined>;
  /** Fetch implementation the client sends through. */
  fetch?: typeof fetch;
}

/** First variable in `names` carrying a non-blank value. */
function readEnv(
  env: Record<string, string | undefined>,
  names: readonly string[],
): string | undefined {
  for (const name of names) {
    const value = env[name];
    if (value !== undefined && value.trim() !== "") return value.trim();
  }
  return undefined;
}

/** The variable's value, or a failure naming every accepted variable. */
function requireEnv(
  env: Record<string, string | undefined>,
  names: readonly string[],
  subject: string,
): string {
  const value = readEnv(env, names);
  if (value === undefined) {
    throw new Error(`Set ${names.join(" or ")} to the Verglas ${subject}.`);
  }
  return value;
}

/**
 * Build a graph handle from environment configuration.
 * @param graphName - the graph every call targets.
 * @param options - variable source and fetch implementation.
 * @returns a handle bound to that graph.
 * @throws {Error} when the endpoint or either credential variable is unset.
 */
export function graphFromEnv(graphName: string, options: GraphFromEnvOptions = {}): Graph {
  const env = options.env ?? process.env;
  const endpoint = requireEnv(env, ENDPOINT_VARS, "graph endpoint");
  const credentials = {
    accessKeyId: requireEnv(env, ACCESS_KEY_VARS, "access key id"),
    secretAccessKey: requireEnv(env, SECRET_KEY_VARS, "secret access key"),
    ...(readEnv(env, ["VERGLAS_REGION", "AWS_REGION"]) === undefined
      ? {}
      : { region: readEnv(env, ["VERGLAS_REGION", "AWS_REGION"]) as string }),
  };
  return new Graph(new VerglasGraphsClient(endpoint, credentials, options.fetch), graphName);
}

/** One node as RIME emits it. */
export interface GraphNodeInput {
  /** Stable node identity, already namespaced by the caller. */
  id: string;
  /** Type labels applied to the node. */
  labels?: string[];
  /** Run scope the node belongs to; recorded as a property on the wire. */
  namespace?: string;
  /** Business properties carried with the node. */
  properties?: Record<string, unknown>;
}

/** One edge as RIME emits it. */
export interface GraphEdgeInput {
  /** Source node identity. */
  srcId: string;
  /** Relationship name. */
  predicate: string;
  /** Target node identity. */
  dstId: string;
  /** Origin recorded for the assertion. */
  provenance: string;
  /** Run scope the edge belongs to; recorded as a property on the wire. */
  namespace?: string;
  /** Business properties carried with the edge. */
  properties?: Record<string, unknown>;
}

/** Namespace is a RIME concept, so it travels as an ordinary property. */
function withNamespace(
  properties: Record<string, unknown> | undefined,
  namespace: string | undefined,
): Record<string, unknown> | undefined {
  if (namespace === undefined) return properties;
  return { ...(properties ?? {}), namespace };
}

/** Writes for one named graph. */
export class Graph {
  /**
   * Binds a handle to one graph.
   * @param client - the Verglas Graph client the writes travel through.
   * @param graphName - the graph every call targets.
   */
  constructor(
    private readonly client: VerglasGraphsClient,
    private readonly graphName: string,
  ) {}

  /**
   * Create the graph this handle is bound to.
   * @returns the created graph's description.
   */
  create(): Promise<Model.CreateGraphOutput> {
    return this.client.createGraph({ graphName: this.graphName });
  }

  /**
   * Insert nodes, translating RIME's namespace into a node property.
   * @param nodes - the nodes to write.
   * @returns the snapshot the write produced.
   */
  insertNodes(nodes: readonly GraphNodeInput[]): Promise<Model.AddNodesOutput> {
    return this.client.addNodes({
      graphName: this.graphName,
      nodes: nodes.map((node) => ({
        id: node.id,
        ...(node.labels === undefined ? {} : { labels: node.labels }),
        ...(withNamespace(node.properties, node.namespace) === undefined
          ? {}
          : { properties: withNamespace(node.properties, node.namespace) }),
      })),
    });
  }

  /**
   * Insert edges, mapping `srcId`/`dstId` onto the service's
   * `sourceId`/`targetId` and translating the namespace into a property.
   * @param edges - the edges to write.
   * @returns the snapshot the write produced.
   */
  insertEdges(edges: readonly GraphEdgeInput[]): Promise<Model.AddEdgesOutput> {
    return this.client.addEdges({
      graphName: this.graphName,
      edges: edges.map((edge) => ({
        sourceId: edge.srcId,
        targetId: edge.dstId,
        predicate: edge.predicate,
        provenance: edge.provenance,
        ...(withNamespace(edge.properties, edge.namespace) === undefined
          ? {}
          : { properties: withNamespace(edge.properties, edge.namespace) }),
      })),
    });
  }
}
