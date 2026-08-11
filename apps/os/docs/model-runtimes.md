# Pi subscription model runtime

Verglas OS uses Pi for subscription-backed Codex, Claude, and GitHub Copilot models. The
Workshop owns the Pi agent loop; `scripts/pi-model-runtime.mjs` owns provider OAuth, credential
refresh, and native model streaming. No coding-agent CLI or OpenAI-compatible translation layer
is involved.

## Local deployment

`pnpm run-local` starts the Pi model service alongside Wrangler. The service:

- registers Pi's `openai-codex`, `anthropic`, and `github-copilot` providers;
- runs Pi-owned OAuth flows and persists refreshed credentials per user scope;
- exposes the provider's live model catalog; and
- streams Pi assistant events over the native `pi-messages` protocol.

The frontend never receives the deployment bearer token or provider credentials. The backend
sends a stable `X-Verglas-Credential-Scope` on every management and inference request so one
user cannot read or refresh another user's credentials.

Direct OpenAI and Anthropic API keys remain separate Workshop model credentials. They do not
enter the Pi subscription credential store.

## Service contract

- `GET /v1/runtimes` reports Pi provider login status.
- `GET /v1/runtimes/{id}/models` returns the provider-owned catalog.
- `POST /v1/runtimes/{id}/login` starts Pi OAuth.
- `POST /v1/login-sessions/{id}` advances or polls OAuth.
- `DELETE /v1/login-sessions/{id}` cancels OAuth.
- `POST /v1/runtimes/{id}/verify` performs a bounded native inference.
- `POST /messages` streams Pi assistant-message events.

Every route requires the deployment bearer token. Every route except the process health check
also requires a credential scope. The local service binds to loopback by default; Compose exposes
only the Workshop origin.

Cloud deployments provide the same Pi service contract from their model-runtime container.
