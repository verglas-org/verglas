type ApplicationPreviewEnv = {
  VERGLAS_CONTAINER_RUNTIME_URL?: string;
};

/** Proxies one public Application path without forwarding Workshop identity credentials. */
export async function proxyApplicationPreview(
  request: Request,
  env: ApplicationPreviewEnv,
  fetcher: typeof fetch = fetch,
): Promise<Response> {
  const endpoint = env.VERGLAS_CONTAINER_RUNTIME_URL?.trim();
  if (!endpoint) return new Response("Application previews are unavailable.", {status: 503});

  const incoming = new URL(request.url);
  const target = new URL(incoming.pathname + incoming.search, endpoint.replace(/\/+$/, "") + "/");
  const headers = new Headers(request.headers);
  headers.delete("authorization");
  headers.delete("cookie");
  headers.delete("host");
  headers.delete("cf-access-jwt-assertion");

  return await fetcher(target, {
    method: request.method,
    headers,
    body: request.method === "GET" || request.method === "HEAD" ? undefined : request.body,
    redirect: "manual",
  });
}
