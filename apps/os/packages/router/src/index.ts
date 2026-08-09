// The public origin of a Verglas OS instance. Routes API traffic to the workshop
// backend and serves the workshop frontend for everything else.

export interface Env {
  WORKSHOP_BACKEND: Fetcher;
  // Present in production (wrangler.jsonc assets stanza); absent in dev.
  ASSETS?: Fetcher;
}

function backendFetch(env: Env, req: Request): Promise<Response> {
  if (!env.WORKSHOP_BACKEND) {
    return Promise.resolve(new Response(
      "WORKSHOP_BACKEND service binding is missing. Restart via `pnpm run-local` / `pnpm dev-server` so wrangler.dev.jsonc is regenerated.",
      { status: 503, headers: { "content-type": "text/plain; charset=utf-8" } },
    ));
  }
  return env.WORKSHOP_BACKEND.fetch(req);
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);

    if (url.pathname === "/api" || url.pathname.startsWith("/api/") ||
        url.pathname === "/blueprint-screenshot" ||
        url.pathname.startsWith("/blueprint-screenshot/") ||
        url.pathname === "/application-screenshot" ||
        url.pathname.startsWith("/application-screenshot/")) {
      return backendFetch(env, req);
    }

    if (env.ASSETS) {
      return env.ASSETS.fetch(req);
    }

    // Dev only: with no assets binding here, everything else goes to the backend.
    return backendFetch(env, req);
  },
} satisfies ExportedHandler<Env>;
