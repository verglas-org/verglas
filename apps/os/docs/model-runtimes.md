# Native model runtimes

Verglas OS can use Codex, Claude Code, and Cursor with the credentials already owned by each
installed CLI. OpenClaw is not installed, launched, or called by this integration.

## Local open-source deployment

`pnpm run-local` starts a loopback-only model-runtime adapter alongside Wrangler. The adapter:

- detects `codex`, `claude`, and `cursor-agent` on the user's machine;
- asks each CLI for its current subscription login state;
- launches the CLI's own browser login command when the user selects **Continue with
  subscription**;
- invokes the selected CLI for model turns without copying its subscription credential into the
  Workshop database; and
- accepts requests only with an ephemeral backend-only bearer token generated at startup.

The Workshop frontend never receives that adapter token. The backend receives
`LOCAL_MODEL_RUNTIME_URL` and `LOCAL_MODEL_RUNTIME_TOKEN` from `run-dev-server.js` and calls the
adapter over loopback.

The three branded connection flows map directly to native CLIs:

| UI choice | Subscription runtime | API-key behavior |
| --- | --- | --- |
| Codex | `codex` and ChatGPT sign-in | Saved as a normal OpenAI model credential |
| Claude Code | `claude` and Claude subscription sign-in | Saved as a normal Anthropic model credential |
| Cursor | `cursor-agent` and Cursor sign-in | Passed only to the local Cursor invocation |

API keys remain user model credentials in Workshop storage. They are not written into global CLI
configuration. Subscription credentials remain wherever the vendor CLI normally stores them.

## Runtime contract

The local adapter exposes a deliberately narrow HTTP API:

- `GET /v1/runtimes` reports installation and login status.
- `POST /v1/runtimes/{id}/login` launches the vendor CLI login.
- `POST /v1/login-sessions/{id}` polls a running login.
- `DELETE /v1/login-sessions/{id}` cancels a login process.
- `POST /v1/chat/completions` runs a model turn through the selected CLI.

`/v1/chat/completions` is an OpenAI-compatible façade. Subscription CLIs are not native
tool-calling APIs, so the adapter asks each CLI for a structured assistant message
(`content` + `tool_calls`) via vendor output-schema support where it exists (Codex/Claude),
or via prompt alone (Cursor), then forwards that as a normal chat completion. That bridge is
a local-dev compromise, not a product requirement for cloud model providers.

Every route requires the deployment-owned bearer token. The server binds to `127.0.0.1` by
default. Do not expose it as public ingress.

## Cloud deployment

The frontend and Workshop RPC API remain unchanged in the cloud. A cloud deployment supplies the
same adapter contract from the account's model-runtime container instead of spawning it on the
dashboard host. Container placement and lifecycle belong to the cloud runtime control plane; the
Workshop does not call the Verglas scheduler for each model turn.

This is separate from Application Vessels and Source workers: those use the Verglas admin /
scheduler / container-runtime APIs described in [architecture.md](architecture.md). Model turns
only need this narrow chat-completions adapter.
