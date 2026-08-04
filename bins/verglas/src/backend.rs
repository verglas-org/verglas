//! Backend resolution for the pure-client CLI — the one place that decides
//! which backend a command targets.
//!
//! The `verglas` CLI is a PURE CLIENT: it is never required to be co-located
//! with a daemon. Every command targets one of two backends, resolved here:
//!
//! - a **daemon endpoint** — the global `--daemon-endpoint` / `VERGLAS_ENDPOINT`
//!   value (default `http://127.0.0.1:8334`). It may be localhost OR a remote
//!   daemon: a cloud node, or another host's daemon (the case a `data_sidecar`
//!   box points at so it can declare workers without running its own daemon).
//!   The data-plane verbs (`table`, `query`, `graph`) and the registry verbs
//!   (`workers`, `index`, `tables`) all target it through [`daemon`]. WHICH
//!   daemon owns a declaration is a property of the endpoint, not of where the
//!   CLI runs — a remote endpoint means the remote daemon owns and executes it.
//!
//! - the **control plane** — resolved from a stored `verglas login` via
//!   [`control_plane`]. It backs `login` itself and the cloud enrichment of
//!   `workers list`/`show` and `status`.
//!
//! The service-lifecycle verbs (`init`, `start`, `stop`, `restart`, `status`,
//! `logs`, `dev`, `drain`) are the only ones that are local by definition: they
//! install, run, or drain the daemon on THIS machine (the CLI is local-only for
//! cluster/service ops). They are about DEPLOYING a daemon, not about using the
//! platform — running a local/edge daemon is for cache and read latency, never
//! a requirement.
//!
//! Because of that principle, the daemon-unreachable error ([`DaemonError`])
//! names the remote-endpoint option, and [`control_plane`] names `verglas login`
//! when no key is stored, so a command never hangs or fails cryptically when its
//! backend is not reachable.

use crate::controlplane::{ControlPlaneClient, ControlPlaneError};
use verglas_sdk::daemon::{DaemonClient, DaemonError};

/// Resolves a [`DaemonClient`] for `endpoint`, the backend every data-plane and
/// registry verb speaks to. The endpoint may be the default localhost daemon or
/// any remote one; the client attempts no connection until its first call and
/// then fails fast (bounded connect timeout) with a clear, remote-aware error
/// when nothing is listening — it never silently assumes a local daemon.
pub fn daemon(endpoint: &str) -> Result<DaemonClient, DaemonError> {
    DaemonClient::new(endpoint)
}

/// Resolves the control-plane client from the stored login, or
/// [`ControlPlaneError::NotLoggedIn`] (rendered as "run `verglas login`") when
/// no key is stored.
///
/// Commands that only ENRICH their output with cloud data treat `NotLoggedIn` as
/// "there are simply no cloud items" and stay local-only (they match the variant
/// and carry on); commands that REQUIRE the control plane surface the error so
/// the operator is told to log in rather than seeing a cryptic failure.
pub fn control_plane() -> Result<ControlPlaneClient, ControlPlaneError> {
    ControlPlaneClient::from_stored()
}
