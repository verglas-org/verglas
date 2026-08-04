//! TLS termination for the S3 endpoint (issue #11).
//!
//! Production puts an L4 NLB (TCP passthrough) in front of the servers, so TLS
//! terminates here, at the server — the NLB never sees plaintext and SigV4's
//! signed `Host` header survives intact. This module loads the configured PEM
//! cert/key into an [`axum_server::tls_rustls::RustlsConfig`] and reloads them on
//! SIGHUP. A reload swaps the certificate for *new* handshakes only; connections
//! already established keep their session, so a production cert rotation drops no
//! requests.

use std::path::PathBuf;

use axum_server::tls_rustls::RustlsConfig;
use verglas_core::config::Tls;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Installs the process-default rustls crypto provider (aws-lc-rs) so
/// `RustlsConfig`'s internal `ServerConfig::builder()` has a provider to build
/// with. Both aws-lc-rs and ring are in the dependency graph, so rustls will not
/// pick one implicitly — the default must be set explicitly, once. Idempotent:
/// a call after a provider is already installed is ignored.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

/// Loads the cert chain and private key from the configured PEM files into a
/// reloadable rustls config. The paths are validated at config load, so a
/// failure here is a genuine load error (unreadable or malformed PEM), which the
/// caller surfaces as a startup failure — nothing binds on a bad cert.
pub async fn load_config(tls: &Tls) -> std::io::Result<RustlsConfig> {
    RustlsConfig::from_pem_file(&tls.cert_path, &tls.key_path).await
}

/// Spawns a background task that reloads the cert/key from disk into `config`
/// each time the process receives SIGHUP. The reload is atomic and affects only
/// new handshakes, so an operator rotates a production cert by rewriting the PEM
/// files and sending `kill -HUP` — live connections are never dropped. A failed
/// reload (e.g. a half-written file) is logged and the previous cert stays in
/// force, so a botched rotation degrades to "old cert still served", never to a
/// dead listener.
///
/// SIGHUP has no portable Windows analogue, so on non-Unix platforms this is a
/// no-op that logs once: TLS certificates load at startup and a rotation there
/// means restarting the server. Everything else about serving TLS is unchanged.
#[cfg(unix)]
pub fn spawn_reload_on_sighup(config: RustlsConfig, cert_path: PathBuf, key_path: PathBuf) {
    tokio::spawn(async move {
        let mut hangup = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!(
                    "verglas-server {VERSION} could not install the SIGHUP handler for TLS reload: {error}"
                );
                return;
            }
        };
        while hangup.recv().await.is_some() {
            match config.reload_from_pem_file(&cert_path, &key_path).await {
                Ok(()) => eprintln!(
                    "verglas-server {VERSION} reloaded TLS certificate from {} on SIGHUP (live connections kept)",
                    cert_path.display()
                ),
                Err(error) => eprintln!(
                    "verglas-server {VERSION} TLS certificate reload failed ({error}); keeping the current certificate"
                ),
            }
        }
    });
}

/// Non-Unix stub: there is no SIGHUP, so live cert reload is unavailable. The
/// server still serves TLS with the certificate loaded at startup; rotating it
/// means restarting the server. Logged once so the operator knows the reload
/// signal is inert here.
#[cfg(not(unix))]
pub fn spawn_reload_on_sighup(_config: RustlsConfig, cert_path: PathBuf, _key_path: PathBuf) {
    eprintln!(
        "verglas-server {VERSION} live TLS certificate reload (SIGHUP) is not available on this platform; \
         restart the server to load a new certificate ({})",
        cert_path.display()
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use verglas_core::config::Tls;

    /// Writes a fresh self-signed cert/key PEM pair into `dir` and returns the
    /// [`Tls`] pointing at them.
    fn self_signed(dir: &std::path::Path) -> Tls {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("generate self-signed cert");
        let cert_path = dir.join("cert.pem");
        let key_path = dir.join("key.pem");
        std::fs::write(&cert_path, cert.cert.pem()).expect("write cert");
        std::fs::write(&key_path, cert.key_pair.serialize_pem()).expect("write key");
        Tls {
            cert_path,
            key_path,
        }
    }

    /// Installing the crypto provider is safe to call more than once: the second
    /// call (a provider already installed) is ignored, never a panic. The server
    /// relies on this being idempotent per process.
    #[test]
    fn install_crypto_provider_is_idempotent() {
        install_crypto_provider();
        install_crypto_provider();
    }

    /// A valid PEM cert/key pair loads into a `RustlsConfig` — the happy path the
    /// TLS listener takes on startup.
    #[tokio::test]
    async fn load_config_reads_a_valid_pem_pair() {
        install_crypto_provider();
        let dir = tempfile::tempdir().expect("temp dir");
        let tls = self_signed(dir.path());
        load_config(&tls).await.expect("valid PEM pair loads");
    }

    /// A missing key file is a load error, which the caller surfaces as a startup
    /// failure — nothing binds on a bad cert.
    #[tokio::test]
    async fn load_config_errors_on_a_missing_file() {
        install_crypto_provider();
        let dir = tempfile::tempdir().expect("temp dir");
        let mut tls = self_signed(dir.path());
        tls.key_path = dir.path().join("does-not-exist.pem");
        load_config(&tls).await.expect_err("missing key must error");
    }
}
