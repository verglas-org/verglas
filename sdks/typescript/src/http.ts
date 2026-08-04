// Minimal fetch wrapper shared by the client. No Node APIs: uses the global
// (or injected) `fetch`, `AbortController`, and `AbortSignal`, all present in
// edge/serverless runtimes and Node 18+.

/** An error carrying the endpoint's HTTP status and body for diagnosis. */
export class VerglasHttpError extends Error {
  constructor(
    readonly status: number,
    readonly method: string,
    readonly path: string,
    readonly body: string,
  ) {
    super(`Verglas ${method} ${path} failed: HTTP ${status}${body ? ` — ${body}` : ""}`);
    this.name = "VerglasHttpError";
  }
}

/** Everything the transport needs, resolved once at `connect` time. */
export interface Transport {
  request<T>(
    method: "GET" | "POST" | "PUT",
    path: string,
    opts?: { query?: Record<string, string | number | undefined>; body?: unknown; headers?: Record<string, string> },
  ): Promise<T>;
}

/** Joins the endpoint base and a path, tolerating a trailing slash on either. */
function joinUrl(endpoint: string, path: string): string {
  const base = endpoint.replace(/\/+$/, "");
  const rel = path.replace(/^\/+/, "");
  return `${base}/${rel}`;
}

/** Builds the transport bound to one endpoint + token. */
export function makeTransport(
  endpoint: string,
  token: string,
  fetchImpl: typeof fetch,
  timeoutMs: number,
): Transport {
  return {
    async request<T>(
      method: "GET" | "POST" | "PUT",
      path: string,
      opts?: { query?: Record<string, string | number | undefined>; body?: unknown; headers?: Record<string, string> },
    ): Promise<T> {
      const url = new URL(joinUrl(endpoint, path));
      for (const [k, v] of Object.entries(opts?.query ?? {})) {
        if (v !== undefined) url.searchParams.set(k, String(v));
      }

      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const resp = await fetchImpl(url.toString(), {
          method,
          headers: {
            authorization: `Bearer ${token}`,
            accept: "application/json",
            ...(opts?.body !== undefined ? { "content-type": "application/json" } : {}),
            ...opts?.headers,
          },
          body: opts?.body !== undefined ? JSON.stringify(opts.body) : undefined,
          signal: controller.signal,
        });
        if (!resp.ok) {
          const text = await resp.text().catch(() => "");
          throw new VerglasHttpError(resp.status, method, url.pathname, text);
        }
        // 204 or empty body: return undefined cast to T.
        const text = await resp.text();
        return (text ? JSON.parse(text) : undefined) as T;
      } finally {
        clearTimeout(timer);
      }
    },
  };
}
