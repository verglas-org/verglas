# Running Workspaces as a public, multi-user service

The Workshop supports built-in username/password accounts, Cloudflare Access, or sign-in through
Google, GitHub, and Cloudflare gatekeepers. Each user configures a direct model API token or an
native model-runtime adapter; the deployment does not proxy inference through Cloudflare AI Gateway.

Sign-in is provided by **authentication gatekeepers**: each auth-capable gatekeeper (Google, GitHub,
Cloudflare) uses its single OAuth app both to authenticate the user (by verified email) and to
connect the account's capabilities. There's no single switch — the pieces turn on independently:

| Configure | Effect |
| --- | --- |
| `AUTH_GATEKEEPERS=cloudflare,google,github` | Allowlists which connected gatekeepers may be used to sign in. Each shows a "Continue with …" button alongside username/password. |
| Each gatekeeper's OAuth credentials (on the gatekeeper Worker) | Required for that gatekeeper to actually authenticate. In dev, seeded from `GOOGLE_*` / `GITHUB_*` / `CLOUDFLARE_OAUTH_*` shell vars (see `run-dev-server.js`). |
| `DISABLE_PASSWORD_AUTH=true` | Hides username/password, leaving gatekeeper sign-in only (ignored unless `AUTH_GATEKEEPERS` is non-empty, to avoid lockout). |

The primary account key is always the user's **verified email**: signing in with any allowlisted
gatekeeper that yields the same verified email maps to the same account.

For local development, set the required variables in a root `.dev.vars` file (gitignored,
`KEY=VALUE` per line); `pnpm run dev-server` loads it automatically. A minimal example:

```
PUBLIC_BASE_URL=http://localhost:8787
AUTH_GATEKEEPERS=cloudflare,google,github

# Each gatekeeper's OAuth app (client id/secret). In dev these seed the gatekeeper Workers:
GITHUB_CLIENT_ID=...
GITHUB_CLIENT_SECRET=...
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
CLOUDFLARE_OAUTH_CLIENT_ID=...
CLOUDFLARE_OAUTH_CLIENT_SECRET=...
```

Each gatekeeper's OAuth app must be registered with that gatekeeper's redirect URI (replace the host
with `PUBLIC_BASE_URL`):

- GitHub: `${PUBLIC_BASE_URL}/gatekeeper/github/oauth`
- Google: `${PUBLIC_BASE_URL}/gatekeeper/google/oauth`
- Cloudflare: `${PUBLIC_BASE_URL}/gatekeeper/cloudflare/oauth`

See [OAuth sign-in](oauth-signin.md) and [model runtimes](model-runtimes.md).
