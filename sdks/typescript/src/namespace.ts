//! Reflected Integration namespace discovery and invocation for the TypeScript SDK.

import type { Transport } from "./http";
import type {
  NamespaceBindings,
  NamespaceCall,
  NamespaceManifest,
  NamespaceRegistry,
} from "./types";

/** Owns reflection caching and creates the lazy namespace proxy exposed by one client. */
export class NamespaceRuntime<Namespaces extends NamespaceRegistry> {
  readonly namespace: NamespaceBindings<Namespaces>;
  readonly #transport: Transport;
  readonly #manifests = new Map<string, Promise<NamespaceManifest>>();

  /** Binds namespace operations to the same authenticated transport as the data-plane client. */
  constructor(transport: Transport) {
    this.#transport = transport;
    this.namespace = this.#proxy([]) as NamespaceBindings<Namespaces>;
  }

  /** Lists every reflected namespace and seeds the per-namespace manifest cache. */
  async reflect(): Promise<NamespaceManifest[]> {
    const manifests = await this.#transport.request<NamespaceManifest[]>("GET", "/v1/namespaces");
    for (const manifest of manifests) this.#manifests.set(manifest.namespace, Promise.resolve(manifest));
    return manifests;
  }

  /** Loads one namespace manifest once for validation and agent inspection. */
  manifest(namespace: string): Promise<NamespaceManifest> {
    if (!namespace) return Promise.reject(new Error("namespace is required"));
    let pending = this.#manifests.get(namespace);
    if (!pending) {
      pending = this.#transport.request<NamespaceManifest>(
        "GET",
        `/v1/namespaces/${encodeURIComponent(namespace)}`,
      );
      this.#manifests.set(namespace, pending);
    }
    return pending;
  }

  /** Builds an indefinitely nestable property path whose function call starts an invocation. */
  #proxy(path: string[]): unknown {
    const target = () => undefined;
    return new Proxy(target, {
      get: (_target, property) => {
        if (property === "then") return undefined;
        if (typeof property !== "string") return undefined;
        return this.#proxy([...path, property]);
      },
      apply: (_target, _thisArg, args: unknown[]) => {
        if (path.length < 2) {
          throw new Error("a namespace invocation requires a namespace and method");
        }
        if (args.length > 1) throw new Error("namespace methods accept at most one input value");
        const [namespace, ...segments] = path;
        return this.#call(namespace, segments.join("."), args[0]);
      },
    });
  }

  /** Creates one lazy call which resolves reflection before unary or streaming execution. */
  #call<Result>(namespace: string, method: string, input: unknown): NamespaceCall<Result> {
    const checkedMethod = async () => {
      const manifest = await this.manifest(namespace);
      const definition = manifest.methods[method];
      if (!definition) throw new Error(`namespace ${namespace} does not declare method ${method}`);
      return definition;
    };
    const unary = async (): Promise<Result> => {
      const definition = await checkedMethod();
      if (definition.mode === "stream") {
        throw new Error(`namespace method ${namespace}.${method} is a stream and must be iterated`);
      }
      return await this.#transport.request<Result>(
        "POST",
        this.#invocationPath(namespace, method),
        {body: input},
      );
    };
    const stream = async function* (runtime: NamespaceRuntime<Namespaces>): AsyncGenerator<Result> {
      const definition = await checkedMethod();
      if (definition.mode !== "stream") {
        throw new Error(`namespace method ${namespace}.${method} is not a stream`);
      }
      const response = await runtime.#transport.requestRaw(
        "POST",
        runtime.#invocationPath(namespace, method),
        {
          body: input === undefined ? undefined : JSON.stringify(input),
          headers: {
            accept: "application/x-ndjson",
            ...(input === undefined ? {} : {"content-type": "application/json"}),
          },
        },
      );
      yield* decodeNdjson<Result>(response);
    };
    return {
      then: (fulfilled, rejected) => unary().then(fulfilled, rejected),
      [Symbol.asyncIterator]: () => stream(this),
    };
  }

  /** Produces the stable gateway route for one reflected method. */
  #invocationPath(namespace: string, method: string): string {
    return `/v1/namespaces/${encodeURIComponent(namespace)}/invoke/${encodeURIComponent(method)}`;
  }
}

/** Decodes an NDJSON response incrementally without buffering an unbounded stream. */
async function* decodeNdjson<Result>(response: Response): AsyncGenerator<Result> {
  if (!response.body) throw new Error("namespace stream returned no response body");
  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  let buffered = "";
  try {
    while (true) {
      const {done, value} = await reader.read();
      if (done) break;
      buffered += value;
      let newline = buffered.indexOf("\n");
      while (newline >= 0) {
        const line = buffered.slice(0, newline).trim();
        buffered = buffered.slice(newline + 1);
        if (line) yield JSON.parse(line) as Result;
        newline = buffered.indexOf("\n");
      }
    }
    const finalLine = buffered.trim();
    if (finalLine) yield JSON.parse(finalLine) as Result;
  } finally {
    reader.releaseLock();
  }
}
