//! Backend resolution for the pure-client CLI — the one place that decides
//! which server a command targets.
//!
//! The `verglas` CLI is a PURE CLIENT: it is never required to be co-located
//! with a server. Every data-plane and registry verb (`table`, `workers`)
//! targets a **server endpoint** — the global
//! `--server-endpoint` / `VERGLAS_ENDPOINT` value (default
//! `http://127.0.0.1:8334`). It may be localhost OR a remote self-hosted
//! server. WHICH server owns a declaration is a property of the endpoint, not
//! of where the CLI runs.
//!
//! `drain` is local by definition: it drains the server addressed by the
//! configured endpoint. Running a local/edge server is for cache and read
//! latency, never a requirement to use the platform.
//!
//! Because of that principle, the server-unreachable error ([`ServerError`])
//! names the remote-endpoint option so a command never hangs or fails
//! cryptically when nothing is listening.

use verglas_sdk::server::{ServerClient, ServerError};

/// Resolves a [`ServerClient`] for `endpoint`, the backend every data-plane and
/// registry verb speaks to. The endpoint may be the default localhost server or
/// any remote one; the client attempts no connection until its first call and
/// then fails fast (bounded connect timeout) with a clear, remote-aware error
/// when nothing is listening — it never silently assumes a local server.
pub fn server(endpoint: &str, token: Option<&str>) -> Result<ServerClient, ServerError> {
    ServerClient::new_with_token(endpoint, token)
}
