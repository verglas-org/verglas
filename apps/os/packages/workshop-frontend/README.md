# Verglas OS Workshop Frontend

Single-page app for the Verglas OS Workshop UI. Built with React, Kumo, and Vite.
Brand palette matches Verglas Cloud (`IBM Plex`, cool dark surfaces, `#3d9cf0`
primary).

## Development

```sh
pnpm dev        # start dev server on http://localhost:3000
pnpm build      # type-check and build for production
pnpm preview    # preview production build locally
```

From the monorepo root, `pnpm dev-server` + `pnpm dev-client` (or
`pnpm run-local`) is the usual full-stack loop.

## Authentication modes

The frontend supports two authentication modes, selected at build time.

### Password mode (default)

Users log in with an email address and password. Account creation is available via `/signup` when an administrator enables sign-ups.
This is the default — no extra configuration needed.

### Cloudflare Access mode

When the backend is deployed behind [Cloudflare Access](https://developers.cloudflare.com/cloudflare-one/applications/), Access handles identity before the user ever reaches the app. In this mode:

- The password-based login page and signup page are disabled.
- On load, the app authenticates automatically using the CF Access session that Access has already established (via `authenticateFromCfAccess()` on the server).

To build in CF Access mode, set `VITE_CF_ACCESS_MODE=true`:

```sh
VITE_CF_ACCESS_MODE=true pnpm build
```

Or add it to a `.env` file for persistent local configuration:

```sh
# .env.local
VITE_CF_ACCESS_MODE=true
```
