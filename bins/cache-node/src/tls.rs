//! TLS for the optional public S3 passthrough listener (#R6-A).
//!
//! This module owns exactly two things: parsing/validating the
//! `VERGLAS_S3_TLS_*` env trio into a bound, ready-to-accept listener
//! ([`from_env`]), and running that listener's per-connection accept loop
//! ([`serve`]). The S3 request routing itself is unchanged — the same axum
//! router the plaintext listener serves is handed in here unmodified.
//!
//! ## Trust model
//!
//! Certificates arrive as env vars from the control plane, the same way the
//! SigV4 keypair does ([`crate::resolve_auth`]) — never a mounted file this
//! process reads on its own.
//!
//! - **Platform CA**: one root, shared by every deployment, that this
//!   process neither presents nor validates against. It exists so a caller
//!   (the CLI, a customer's S3 SDK) can pin a single CA once and reach any
//!   tenant's cache node. This process is a pure TLS *server* on this
//!   listener: it never requests or checks a client certificate.
//! - **Per-deployment leaf**: the certificate this listener actually
//!   presents, scoped to `<deployment_id>.fly.dev` and signed by the
//!   platform CA. It proves *transport* identity only — SigV4 over the
//!   decrypted request is still what proves the caller's identity, exactly
//!   as it does on the plaintext listener today. TLS here removes the
//!   fly-proxy TLS-termination tax from the hot path; it does not change who
//!   is trusted to write data.
//! - **6PN stays plaintext**: `VERGLAS_S3_ADDR`, the original listener, is
//!   not replaced. Fly's private 6PN network (catalog, inter-cache peers,
//!   gateway health probes) already reaches this process inside the
//!   platform's own network boundary, so wrapping that traffic in TLS would
//!   add cost without adding trust. Only the public listener this module
//!   builds is behind a hostile network.
//!
//! Fly's edge is configured as raw TCP passthrough on the public port (see
//! crossplane R6-C) — this module's handshake is the only TLS termination on
//! that path, not a second hop behind one fly-proxy already terminated.
//!
//! ## Env trio
//!
//! `VERGLAS_S3_TLS_ADDR`, `VERGLAS_S3_TLS_CERT_PEM`, `VERGLAS_S3_TLS_KEY_PEM`
//! must be set together or not at all:
//! - All three unset → [`from_env`] returns `Ok(None)`: byte-identical
//!   behavior to before this module existed.
//! - All three set → the same axum S3 router serves over rustls on
//!   `VERGLAS_S3_TLS_ADDR`, beside the plaintext listener.
//! - Anything else (one or two set, unparsable PEM, an unbindable address)
//!   → [`TlsConfigError`], surfaced by the caller as a boot-time failure.
//!   There is no partial-trio fallback to plaintext-only serving.

use std::io;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use hyper_util::service::TowerToHyperService;
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_util::task::TaskTracker;
use tower::ServiceExt as _;

/// Env var naming the TLS listener address (`host:port`, e.g. `[::]:8443`).
/// Same shape as `VERGLAS_S3_ADDR`.
pub const ADDR_VAR: &str = "VERGLAS_S3_TLS_ADDR";
/// Env var holding the PEM-encoded leaf certificate this listener presents
/// (and any intermediates, concatenated after it).
pub const CERT_VAR: &str = "VERGLAS_S3_TLS_CERT_PEM";
/// Env var holding the PEM-encoded private key matching [`CERT_VAR`]'s leaf.
pub const KEY_VAR: &str = "VERGLAS_S3_TLS_KEY_PEM";

/// A misconfiguration of the `VERGLAS_S3_TLS_*` env trio. Every variant is a
/// boot-time failure: the process must not start with a half-built TLS
/// listener, and must not silently keep serving plaintext-only for an
/// operator who asked for TLS.
#[derive(Debug, thiserror::Error)]
pub enum TlsConfigError {
    /// One or two of the three env vars are set, not all three.
    #[error(
        "partial VERGLAS_S3_TLS_* configuration: {present} set, {missing} not set — \
         VERGLAS_S3_TLS_ADDR, VERGLAS_S3_TLS_CERT_PEM, and VERGLAS_S3_TLS_KEY_PEM \
         must be set together or not at all"
    )]
    PartialTrio {
        /// The env vars that were set, joined with ", ".
        present: String,
        /// The env vars that were not set, joined with ", ".
        missing: String,
    },
    /// `VERGLAS_S3_TLS_CERT_PEM` did not decode as one or more PEM
    /// certificates.
    #[error("{CERT_VAR} does not contain a valid PEM certificate chain")]
    InvalidCertificatePem,
    /// `VERGLAS_S3_TLS_KEY_PEM` did not decode as a PEM private key (PKCS#8,
    /// PKCS#1, and SEC1 are all accepted).
    #[error("{KEY_VAR} does not contain a valid PEM private key")]
    InvalidPrivateKeyPem,
    /// rustls rejected the certificate/key pair itself — e.g. the key does
    /// not match the leaf, or the chain uses an algorithm this process's
    /// crypto provider does not support.
    #[error("rustls rejected the {CERT_VAR}/{KEY_VAR} pair: {0}")]
    InvalidServerConfig(#[source] rustls::Error),
    /// The listener could not bind `VERGLAS_S3_TLS_ADDR`.
    #[error("cannot bind {ADDR_VAR}=`{addr}`: {source}")]
    Bind {
        /// The address that failed to bind.
        addr: String,
        /// The underlying OS error.
        #[source]
        source: io::Error,
    },
}

/// The bound, ready-to-accept TLS S3 listener: a raw TCP listener plus the
/// rustls acceptor built from the env trio. Returned by [`from_env`]; handed
/// to [`serve`] once the rest of boot (recovery, health-gating) is done, the
/// same ordering the plaintext listener already follows.
pub struct TlsListener {
    tcp: TcpListener,
    acceptor: TlsAcceptor,
}

impl TlsListener {
    /// The address the listener actually bound (resolves `:0` to the chosen
    /// ephemeral port), for the boot log line.
    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.tcp.local_addr()
    }
}

// `tokio_rustls::TlsAcceptor` does not implement `Debug`, so this is
// hand-written rather than derived; it exists only so tests can
// `expect_err`/`unwrap_err` an `Option<TlsListener>` or `Result<TlsListener, _>`.
impl std::fmt::Debug for TlsListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsListener")
            .field("tcp", &self.tcp)
            .finish_non_exhaustive()
    }
}

/// Reads the `VERGLAS_S3_TLS_*` env trio and, if all three are present,
/// parses and binds them into a [`TlsListener`]. See the module docs for the
/// all-or-none contract.
pub async fn from_env() -> Result<Option<TlsListener>, TlsConfigError> {
    let addr = std::env::var(ADDR_VAR).ok();
    let cert_pem = std::env::var(CERT_VAR).ok();
    let key_pem = std::env::var(KEY_VAR).ok();

    let (addr, cert_pem, key_pem) = match (addr, cert_pem, key_pem) {
        (None, None, None) => return Ok(None),
        (Some(addr), Some(cert_pem), Some(key_pem)) => (addr, cert_pem, key_pem),
        (addr, cert_pem, key_pem) => {
            return Err(partial_trio_error(
                addr.is_some(),
                cert_pem.is_some(),
                key_pem.is_some(),
            ));
        }
    };

    let certs = parse_certificate_chain(&cert_pem)?;
    let key = parse_private_key(&key_pem)?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(TlsConfigError::InvalidServerConfig)?;

    let tcp = TcpListener::bind(&addr)
        .await
        .map_err(|source| TlsConfigError::Bind {
            addr: addr.clone(),
            source,
        })?;

    Ok(Some(TlsListener {
        tcp,
        acceptor: TlsAcceptor::from(Arc::new(config)),
    }))
}

/// Builds the `PartialTrio` error naming exactly which of the three vars are
/// set and which are missing.
fn partial_trio_error(has_addr: bool, has_cert: bool, has_key: bool) -> TlsConfigError {
    let flags = [
        (has_addr, ADDR_VAR),
        (has_cert, CERT_VAR),
        (has_key, KEY_VAR),
    ];
    let join = |want_set: bool| -> String {
        flags
            .iter()
            .filter(|(set, _)| *set == want_set)
            .map(|(_, name)| *name)
            .collect::<Vec<_>>()
            .join(", ")
    };
    TlsConfigError::PartialTrio {
        present: join(true),
        missing: join(false),
    }
}

/// Parses one or more concatenated PEM certificate blocks (leaf first, then
/// any intermediates) into the DER chain `rustls::ServerConfig` wants.
fn parse_certificate_chain(pem: &str) -> Result<Vec<CertificateDer<'static>>, TlsConfigError> {
    let mut reader = io::Cursor::new(pem.as_bytes());
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| TlsConfigError::InvalidCertificatePem)?;
    if certs.is_empty() {
        return Err(TlsConfigError::InvalidCertificatePem);
    }
    Ok(certs)
}

/// Parses a single PEM private key (PKCS#8, PKCS#1, or SEC1) into the DER
/// form `rustls::ServerConfig` wants.
fn parse_private_key(pem: &str) -> Result<PrivateKeyDer<'static>, TlsConfigError> {
    let mut reader = io::Cursor::new(pem.as_bytes());
    rustls_pemfile::private_key(&mut reader)
        .map_err(|_| TlsConfigError::InvalidPrivateKeyPem)?
        .ok_or(TlsConfigError::InvalidPrivateKeyPem)
}

/// Serves `app` on an already-bound TLS listener until `shutdown` resolves,
/// then drains in-flight connections before returning — the same observable
/// contract as `axum::serve(..).with_graceful_shutdown(..)` on the plaintext
/// listener (see `serve.rs`).
///
/// This is hand-rolled rather than routed through `axum::serve`'s `Listener`
/// trait because that trait's `accept()` would have to perform the TLS
/// handshake itself, serializing every handshake behind the next plain TCP
/// accept. Here the handshake happens inside the per-connection spawned
/// task, so accepting connection N+1 never waits on connection N's
/// handshake.
pub async fn serve(
    listener: TlsListener,
    app: Router,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> io::Result<()> {
    let TlsListener { tcp, acceptor } = listener;
    let tracker = TaskTracker::new();
    let mut shutdown = std::pin::pin!(shutdown);

    loop {
        let (stream, _peer_addr) = tokio::select! {
            biased;
            () = &mut shutdown => break,
            accepted = tcp.accept() => match accepted {
                Ok(pair) => {
                    // Nagle off before the TLS handshake: SigV4 request/response
                    // exchanges are small and latency-bound.
                    let _ = pair.0.set_nodelay(true);
                    pair
                }
                Err(error) => {
                    // Matches axum's own accept-error handling (transient:
                    // log and keep the listener alive) rather than tearing
                    // the process down over one bad accept.
                    tracing::warn!(%error, "TLS S3 listener: TCP accept error");
                    continue;
                }
            },
        };

        let acceptor = acceptor.clone();
        // Router is Arc-backed internally; cloning per connection is the
        // same cost axum's own per-connection `make_service.call` pays.
        let app = app.clone();
        tracker.spawn(async move {
            let tls_stream = match acceptor.accept(stream).await {
                Ok(stream) => stream,
                Err(error) => {
                    tracing::debug!(%error, "TLS S3 listener: handshake failed");
                    return;
                }
            };

            // Adapts the axum `Router` (a `Service<Request<axum::body::Body>>`)
            // into `Service<Request<Incoming>>`, the shape hyper-util's
            // connection builder drives — the same adaptor axum::serve
            // applies internally for the plaintext listener.
            let tower_service =
                app.map_request(|request: hyper::Request<Incoming>| request.map(Body::new));
            let hyper_service = TowerToHyperService::new(tower_service);
            let io = TokioIo::new(tls_stream);

            if let Err(error) = ConnBuilder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(io, hyper_service)
                .await
            {
                tracing::debug!(%error, "TLS S3 listener: connection error");
            }
        });
    }

    tracker.close();
    tracker.wait().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::routing::get;
    use rcgen::{CertificateParams, Issuer, KeyPair};
    use rustls::RootCertStore;
    use rustls::pki_types::ServerName;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// One generated platform CA plus one leaf signed by it, as PEM strings,
    /// mirroring the shape the control plane hands this process (leaf cert +
    /// key via env; the CA is only ever used by the *client* side of these
    /// tests, matching how this process never validates against its own
    /// CA — see the trust-model doc comment).
    struct TestChain {
        ca_pem: String,
        leaf_cert_pem: String,
        leaf_key_pem: String,
    }

    /// Builds a self-signed CA and a leaf certificate for `dns_name` signed
    /// by it. Not required to be byte-stable (unlike the real R6-B
    /// deterministic certs) — these exist only for this process's own tests.
    fn generate_test_chain(dns_name: &str) -> TestChain {
        let ca_key = KeyPair::generate().expect("generate CA key");
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_cert = ca_params.self_signed(&ca_key).expect("self-sign CA");

        let leaf_key = KeyPair::generate().expect("generate leaf key");
        let leaf_params = CertificateParams::new(vec![dns_name.to_owned()]).expect("leaf params");
        let issuer = Issuer::new(ca_params, &ca_key);
        let leaf_cert = leaf_params
            .signed_by(&leaf_key, &issuer)
            .expect("sign leaf");

        TestChain {
            ca_pem: ca_cert.pem(),
            leaf_cert_pem: leaf_cert.pem(),
            leaf_key_pem: leaf_key.serialize_pem(),
        }
    }

    /// Holds the process-wide env-var lock for one test body and clears the
    /// three `VERGLAS_S3_TLS_*` vars when it ends (including on panic),
    /// before releasing that lock to the next waiting test.
    ///
    /// `std::env::set_var` mutates process-global state, and a Rust test
    /// binary runs tests on a thread pool by default, so without a lock two
    /// env-mutating tests in this module could interleave and each observe
    /// the other's partial trio. The lock must outlive the cleanup: holding
    /// the `MutexGuard` as a field (rather than a sibling value dropped
    /// separately) guarantees this `Drop` impl's var-clearing runs strictly
    /// before the field drops release the lock, so the next test's setup
    /// can never race this test's teardown.
    // The held guard is never read; it exists purely so its own drop (which
    // releases the lock) is sequenced after `EnvGuard::drop`'s var-clearing
    // by the struct-field drop order described above.
    #[allow(dead_code)]
    struct EnvGuard(std::sync::MutexGuard<'static, ()>);

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            // Safety: `self.0` (dropped after this fn returns) serializes
            // this against every other test in this module; no other code
            // in this process reads or writes these vars.
            unsafe {
                std::env::remove_var(ADDR_VAR);
                std::env::remove_var(CERT_VAR);
                std::env::remove_var(KEY_VAR);
            }
        }
    }

    /// Sets the env trio's three vars (or omits any given as `None`) for one
    /// test, guarded to run alongside the other tests in this module without
    /// interleaving.
    fn with_env_trio(
        addr: Option<&str>,
        cert_pem: Option<&str>,
        key_pem: Option<&str>,
    ) -> EnvGuard {
        // `main()` installs this once for the real process before `tls::from_env`
        // ever runs; the test binary never calls `main`, so each test does the
        // same one-time install rustls needs before building any client/server
        // config.
        static INSTALL_CRYPTO_PROVIDER: std::sync::Once = std::sync::Once::new();
        INSTALL_CRYPTO_PROVIDER.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });

        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let lock = match LOCK.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        // Safety: serialized by `lock` against every other test in this
        // module; no other code in this process reads or writes these vars.
        unsafe {
            match addr {
                Some(v) => std::env::set_var(ADDR_VAR, v),
                None => std::env::remove_var(ADDR_VAR),
            }
            match cert_pem {
                Some(v) => std::env::set_var(CERT_VAR, v),
                None => std::env::remove_var(CERT_VAR),
            }
            match key_pem {
                Some(v) => std::env::set_var(KEY_VAR, v),
                None => std::env::remove_var(KEY_VAR),
            }
        }
        EnvGuard(lock)
    }

    /// None of the three env vars set: `from_env` returns `Ok(None)` — the
    /// contract that a cache node started before this module existed keeps
    /// running byte-identically.
    #[tokio::test]
    async fn from_env_returns_none_when_the_trio_is_entirely_absent() {
        let _guard = with_env_trio(None, None, None);
        let result = from_env().await.expect("no error with nothing configured");
        assert!(result.is_none());
    }

    /// Any partial subset of the trio is a typed `PartialTrio` error naming
    /// which vars were present and which were missing, not a silent
    /// fallback to plaintext-only serving.
    #[tokio::test]
    async fn from_env_rejects_addr_only() {
        let _guard = with_env_trio(Some("127.0.0.1:0"), None, None);
        let error = from_env().await.expect_err("partial trio must fail");
        match error {
            TlsConfigError::PartialTrio { present, missing } => {
                assert_eq!(present, ADDR_VAR);
                assert!(missing.contains(CERT_VAR));
                assert!(missing.contains(KEY_VAR));
            }
            other => panic!("expected PartialTrio, got {other:?}"),
        }
    }

    /// Same partial-trio contract with two of three set.
    #[tokio::test]
    async fn from_env_rejects_addr_and_cert_without_key() {
        let chain = generate_test_chain("deployment.fly.dev");
        let _guard = with_env_trio(Some("127.0.0.1:0"), Some(&chain.leaf_cert_pem), None);
        let error = from_env().await.expect_err("partial trio must fail");
        match error {
            TlsConfigError::PartialTrio { present, missing } => {
                assert!(present.contains(ADDR_VAR));
                assert!(present.contains(CERT_VAR));
                assert_eq!(missing, KEY_VAR);
            }
            other => panic!("expected PartialTrio, got {other:?}"),
        }
    }

    /// A cert PEM that is not valid PEM at all is `InvalidCertificatePem`,
    /// not a panic and not a fallback.
    #[tokio::test]
    async fn from_env_rejects_unparsable_certificate_pem() {
        let chain = generate_test_chain("deployment.fly.dev");
        let _guard = with_env_trio(
            Some("127.0.0.1:0"),
            Some("not a pem certificate"),
            Some(&chain.leaf_key_pem),
        );
        let error = from_env().await.expect_err("bad cert PEM must fail");
        assert!(matches!(error, TlsConfigError::InvalidCertificatePem));
    }

    /// A key PEM that is not valid PEM at all is `InvalidPrivateKeyPem`.
    #[tokio::test]
    async fn from_env_rejects_unparsable_key_pem() {
        let chain = generate_test_chain("deployment.fly.dev");
        let _guard = with_env_trio(
            Some("127.0.0.1:0"),
            Some(&chain.leaf_cert_pem),
            Some("not a pem key"),
        );
        let error = from_env().await.expect_err("bad key PEM must fail");
        assert!(matches!(error, TlsConfigError::InvalidPrivateKeyPem));
    }

    /// A syntactically valid cert PEM paired with a key that does not match
    /// it (a second, unrelated leaf's key) is rejected by rustls itself when
    /// building the `ServerConfig` — this is exactly the "unparsable"
    /// boot-failure case the contract calls out beyond bad PEM syntax.
    #[tokio::test]
    async fn from_env_rejects_a_cert_key_mismatch() {
        let chain_a = generate_test_chain("deployment-a.fly.dev");
        let chain_b = generate_test_chain("deployment-b.fly.dev");
        let _guard = with_env_trio(
            Some("127.0.0.1:0"),
            Some(&chain_a.leaf_cert_pem),
            Some(&chain_b.leaf_key_pem),
        );
        let error = from_env().await.expect_err("mismatched cert/key must fail");
        assert!(matches!(error, TlsConfigError::InvalidServerConfig(_)));
    }

    /// A fully valid trio naming an address that cannot be bound (already
    /// held by another listener on this process) fails with `Bind`, not a
    /// silent retry or a plaintext-only fallback.
    #[tokio::test]
    async fn from_env_reports_bind_failure_on_an_unavailable_address() {
        let chain = generate_test_chain("deployment.fly.dev");
        let held = TcpListener::bind("127.0.0.1:0").await.expect("hold a port");
        let held_addr = held.local_addr().expect("addr").to_string();

        let _guard = with_env_trio(
            Some(&held_addr),
            Some(&chain.leaf_cert_pem),
            Some(&chain.leaf_key_pem),
        );
        let error = from_env()
            .await
            .expect_err("bind onto a held port must fail");
        assert!(matches!(error, TlsConfigError::Bind { .. }));
        drop(held);
    }

    /// The end-to-end contract: a fully valid trio on `127.0.0.1:0` binds, a
    /// real rustls client (trusting the generated test CA) completes a TLS
    /// handshake against it, and a plain HTTP request over that TLS
    /// connection reaches the mounted axum router and gets its response —
    /// proving the S3 router is reachable over TLS, not just that a listener
    /// exists.
    #[tokio::test]
    async fn tls_listener_serves_a_real_handshake_and_reaches_the_router() {
        let chain = generate_test_chain("localhost");
        let _guard = with_env_trio(
            Some("127.0.0.1:0"),
            Some(&chain.leaf_cert_pem),
            Some(&chain.leaf_key_pem),
        );

        let listener = from_env()
            .await
            .expect("trio parses")
            .expect("trio present");
        let addr = listener.local_addr().expect("bound addr");

        let app = Router::new().route("/probe", get(|| async { "tls-ok" }));
        let serve_task = tokio::spawn(serve(listener, app, std::future::pending()));

        // A real rustls client, trusting only the generated test CA — the
        // same trust boundary a CLI or SDK pinning the platform CA would
        // use, per the module's trust-model doc comment.
        let mut roots = RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut io::Cursor::new(chain.ca_pem.as_bytes())) {
            roots.add(cert.expect("valid CA PEM")).expect("trust CA");
        }
        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));

        let tcp = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect to TLS listener");
        let server_name = ServerName::try_from("localhost")
            .expect("valid DNS name")
            .to_owned();
        let mut tls = connector
            .connect(server_name, tcp)
            .await
            .expect("TLS handshake against the test CA succeeds");

        let request = b"GET /probe HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
        tls.write_all(request).await.expect("write request");
        tls.flush().await.expect("flush request");

        let mut response = Vec::new();
        tls.read_to_end(&mut response)
            .await
            .expect("read response over TLS");
        let response = String::from_utf8(response).expect("utf8 response");
        assert!(
            response.starts_with("HTTP/1.1 200"),
            "expected 200 from the routed handler, got: {response}"
        );
        assert!(
            response.ends_with("tls-ok"),
            "expected the router's body in the response: {response}"
        );

        serve_task.abort();
    }

    /// A malformed key with an otherwise-valid certificate never reaches
    /// `TcpListener::bind` at all — parsing fails before any socket work,
    /// so a bad env trio can never accidentally win a race and start
    /// serving.
    #[tokio::test]
    async fn from_env_fails_before_binding_on_bad_key() {
        let chain = generate_test_chain("deployment.fly.dev");
        let mut corrupt_key = chain.leaf_key_pem.clone();
        // Corrupt one base64 line without touching the PEM header/footer, so
        // this exercises rustls-pemfile's decode failure, not a missing
        // header.
        if let Some(line_start) = corrupt_key.find('\n').map(|i| i + 1) {
            corrupt_key.insert(line_start, '!');
        }

        let _guard = with_env_trio(
            Some("127.0.0.1:0"),
            Some(&chain.leaf_cert_pem),
            Some(&corrupt_key),
        );
        let error = from_env().await.expect_err("corrupt key PEM must fail");
        assert!(matches!(error, TlsConfigError::InvalidPrivateKeyPem));
    }
}
