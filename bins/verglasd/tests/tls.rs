//! End-to-end TLS tests for the daemon's S3 endpoint (issue #11). The daemon is
//! configured with a self-signed cert/key and a base domain, then driven over
//! HTTPS: a signed GET returns the object over TLS both path-style and
//! virtual-hosted style, and a cert rotation under continuous load (rewrite the
//! PEM files, then SIGHUP) completes with zero failed requests.
//!
//! TLS termination happens at the daemon by design: production puts an L4 NLB
//! (TCP passthrough) in front, so the daemon is where plaintext first exists and
//! where SigV4's signed Host header must survive intact.

#![cfg(unix)]

use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use reqwest::header::{AUTHORIZATION, HOST, HeaderMap, HeaderValue};
use verglas_s3::{
    BackendStore, NoopInvalidation, PassthroughList, PassthroughRead, PassthroughWrite,
};

/// Bucket the origin holds and the daemon serves.
const BUCKET: &str = "e2e-lake";
/// The endpoint's configured base domain for virtual-hosted addressing.
const DOMAIN: &str = "s3.verglas.test";
/// Keypair the daemon's S3 client presents to the mock origin.
const ORIGIN_KEYS: (&str, &str) = ("origin-ak", "origin-secret");
/// Keypair engines present to the daemon's S3 endpoint.
const ENGINE_KEYS: (&str, &str) = ("engine-ak", "engine-secret");
/// Region used for SigV4 signing in this test.
const REGION: &str = "us-east-1";
/// SHA-256 of an empty request body.
const EMPTY_PAYLOAD_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Signs an HTTP request with the given static keypair, signing `host` as the
/// canonical Host (which the caller also sends as the `Host` header, so the
/// server's signature check matches whatever addressing style is under test).
fn sign(method: &str, url: &str, host: &str, access_key: &str, secret_key: &str) -> HeaderMap {
    let datetime = Utc::now();
    let full_path = url
        .strip_prefix("https://")
        .and_then(|rest| rest.find('/').map(|idx| &rest[idx..]))
        .unwrap_or("/");
    let (path, canonical_query) = match full_path.split_once('?') {
        Some((path, query)) => (path, query.to_owned()),
        None => (full_path, String::new()),
    };
    let amz_date = datetime.format("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = datetime.format("%Y%m%d").to_string();

    let mut header_pairs = vec![
        ("host", host),
        ("x-amz-date", amz_date.as_str()),
        ("x-amz-content-sha256", EMPTY_PAYLOAD_HASH),
    ];
    header_pairs.sort_by_key(|(name, _)| *name);

    let signed_headers = header_pairs
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers = header_pairs
        .iter()
        .map(|(name, value)| format!("{name}:{value}"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";

    let canonical_request = format!(
        "{method}\n{path}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{EMPTY_PAYLOAD_HASH}"
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{date_stamp}/{REGION}/s3/aws4_request\n{}",
        hex::encode(sha256(canonical_request.as_bytes()))
    );
    let signature = sigv4_signature(secret_key, &date_stamp, REGION, "s3", &string_to_sign);
    let credential = format!("{access_key}/{date_stamp}/{REGION}/s3/aws4_request");
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={credential}, SignedHeaders={signed_headers}, Signature={signature}"
    );

    let mut headers = HeaderMap::new();
    for (name, value) in header_pairs {
        headers.insert(
            reqwest::header::HeaderName::from_bytes(name.as_bytes()).expect("header name"),
            HeaderValue::from_str(value).expect("header value"),
        );
    }
    // Send the exact Host we signed, overriding the URL's IP authority so the
    // server sees the addressing-style host, not `127.0.0.1`.
    headers.insert(HOST, HeaderValue::from_str(host).expect("host header"));
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&authorization).expect("auth"),
    );
    headers
}

/// SHA-256 digest bytes.
fn sha256(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes).into()
}

/// SigV4 signing key derivation and signature.
fn sigv4_signature(
    secret_key: &str,
    date_stamp: &str,
    region: &str,
    service: &str,
    string_to_sign: &str,
) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }
    let k_date = hmac(
        format!("AWS4{secret_key}").as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, service.as_bytes());
    let k_signing = hmac(&k_service, b"aws4_request");
    hex::encode(hmac(&k_signing, string_to_sign.as_bytes()))
}

/// Deterministic payload; byte `i` is `i % 251`.
fn pattern(len: u64) -> Bytes {
    (0..len)
        .map(|i| (i % 251) as u8)
        .collect::<Vec<u8>>()
        .into()
}

/// Reserves an ephemeral port (bind-then-drop; the same tiny-race trade-off the
/// other daemon smoke tests make rather than parsing logs).
fn free_addr() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);
    addr
}

/// Generates a fresh self-signed cert/key PEM pair for the endpoint domain.
fn self_signed_pem() -> (String, String) {
    let cert = rcgen::generate_simple_self_signed(vec![
        DOMAIN.to_owned(),
        format!("*.{DOMAIN}"),
        "127.0.0.1".to_owned(),
        "localhost".to_owned(),
    ])
    .expect("generate self-signed cert");
    (cert.cert.pem(), cert.key_pair.serialize_pem())
}

/// Serves the mock S3 origin (in-memory store behind verglas-s3's own front-end,
/// validating the static origin keypair) and returns its URL.
async fn spawn_origin(objects: &[(&str, Bytes)]) -> String {
    let store = Arc::new(InMemory::new());
    for (key, bytes) in objects {
        store
            .put(&Path::from(*key), PutPayload::from(bytes.clone()))
            .await
            .expect("seed origin object");
    }
    let app = verglas_s3::router(
        PassthroughRead::new(BackendStore::single(BUCKET, store.clone())),
        PassthroughWrite::new(BackendStore::single(BUCKET, store.clone())),
        Arc::new(PassthroughList::new(BackendStore::single(BUCKET, store))),
        Arc::new(NoopInvalidation),
        Some((ORIGIN_KEYS.0.to_owned(), ORIGIN_KEYS.1.to_owned())),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind origin");
    let addr = listener.local_addr().expect("origin addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve origin");
    });
    format!("http://{addr}")
}

/// Spawns `verglasd` with a TLS + base-domain config pointed at `origin_url`.
/// Returns the child, the `https://127.0.0.1:port` base URL, and the cert/key
/// file paths (so a test can rotate them).
/// Drains a spawned daemon's stderr on a background thread into a shared buffer.
/// The daemon logs one line per request (#61), so a test that piped stderr and
/// only read it at the end would fill the OS pipe and — with a blocking writer —
/// stall the daemon; draining continuously keeps the pipe empty. (The daemon's
/// own log writer is non-blocking, so it would shed lines rather than stall, but
/// the test still drains so its assertions see the full stderr.)
struct StderrTap {
    /// Accumulated stderr bytes.
    buffer: Arc<Mutex<Vec<u8>>>,
    /// The reader thread, joined when the daemon stops.
    reader: JoinHandle<()>,
}

impl StderrTap {
    /// Starts draining `stderr` immediately.
    fn spawn(mut stderr: std::process::ChildStderr) -> Self {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let sink = buffer.clone();
        let reader = std::thread::spawn(move || {
            let mut chunk = [0u8; 4096];
            loop {
                match stderr.read(&mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => sink
                        .lock()
                        .expect("stderr buffer")
                        .extend_from_slice(&chunk[..n]),
                }
            }
        });
        StderrTap { buffer, reader }
    }

    /// Joins the reader and returns everything read from stderr.
    fn finish(self) -> String {
        let _ = self.reader.join();
        let bytes = self.buffer.lock().expect("stderr buffer").clone();
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

fn spawn_tls_daemon(
    origin_url: &str,
    tag: &str,
) -> (
    Child,
    String,
    std::path::PathBuf,
    std::path::PathBuf,
    StderrTap,
) {
    let scratch = std::env::temp_dir().join(format!("verglasd-tls-{tag}-{}", std::process::id()));
    let cache_dir = scratch.join("cache");
    std::fs::create_dir_all(&cache_dir).expect("create cache dir");

    let (cert_pem, key_pem) = self_signed_pem();
    let cert_path = scratch.join("cert.pem");
    let key_path = scratch.join("key.pem");
    std::fs::write(&cert_path, cert_pem).expect("write cert");
    std::fs::write(&key_path, key_pem).expect("write key");

    let creds_path = scratch.join("endpoint-credentials");
    std::fs::write(
        &creds_path,
        format!(
            "[default]\naws_access_key_id = {}\naws_secret_access_key = {}\n",
            ENGINE_KEYS.0, ENGINE_KEYS.1
        ),
    )
    .expect("write endpoint credentials");
    let config_path = scratch.join("verglas.toml");
    std::fs::write(
        &config_path,
        format!(
            "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n\n\
             [listen]\ndomain = \"{DOMAIN}\"\n\n\
             [listen.tls]\ncert_path = \"{}\"\nkey_path = \"{}\"\n\n\
             [auth]\ncredentials_file = \"{}\"\n\n\
             [backend]\nbucket = \"{BUCKET}\"\n",
            cache_dir.display(),
            cert_path.display(),
            key_path.display(),
            creds_path.display(),
        ),
    )
    .expect("write config");

    let addr = free_addr();
    let mut daemon = Command::new(env!("CARGO_BIN_EXE_verglasd"))
        .arg("--config")
        .arg(&config_path)
        .env("VERGLAS_ADMIN_ADDR", "127.0.0.1:0")
        .env("VERGLAS_S3_ADDR", addr.to_string())
        .env("AWS_ACCESS_KEY_ID", ORIGIN_KEYS.0)
        .env("AWS_SECRET_ACCESS_KEY", ORIGIN_KEYS.1)
        .env("AWS_REGION", REGION)
        .env("AWS_ENDPOINT", origin_url)
        .env("AWS_ALLOW_HTTP", "true")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn verglasd");
    let tap = StderrTap::spawn(daemon.stderr.take().expect("piped stderr"));
    (daemon, format!("https://{addr}"), cert_path, key_path, tap)
}

/// A client that accepts the daemon's self-signed cert (verification off is the
/// point: this is the `--no-verify` path the issue names, exercised in-process).
fn tls_client() -> reqwest::Client {
    reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .expect("build tls client")
}

/// Stops a spawned daemon, returning the stderr its tap drained.
fn stop_daemon(mut daemon: Child, tap: StderrTap) -> String {
    let _ = daemon.kill();
    let _ = daemon.wait();
    tap.finish()
}

/// Polls the TLS endpoint until a signed path-style GET of `key` succeeds.
async fn wait_for_ready(client: &reqwest::Client, base: &str, key: &str) {
    let url = format!("{base}/{BUCKET}/{key}");
    for _ in 0..100 {
        if let Ok(response) = client
            .get(&url)
            .headers(sign("GET", &url, DOMAIN, ENGINE_KEYS.0, ENGINE_KEYS.1))
            .send()
            .await
            && response.status().is_success()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("daemon TLS endpoint did not become ready at {base}");
}

#[tokio::test]
async fn daemon_serves_reads_over_tls_both_addressing_styles() {
    let body = pattern(50_000);
    let origin_url = spawn_origin(&[("warehouse/part-0.parquet", body.clone())]).await;
    let (daemon, base, _cert, _key, tap) = spawn_tls_daemon(&origin_url, "reads");
    let client = tls_client();
    wait_for_ready(&client, &base, "warehouse/part-0.parquet").await;

    // Path-style over TLS: Host is the bare domain, bucket in the path.
    let url = format!("{base}/{BUCKET}/warehouse/part-0.parquet");
    let response = client
        .get(&url)
        .headers(sign("GET", &url, DOMAIN, ENGINE_KEYS.0, ENGINE_KEYS.1))
        .send()
        .await
        .expect("path-style TLS GET");
    assert_eq!(response.status(), 200, "path-style GET over TLS");
    assert_eq!(response.bytes().await.expect("body"), body);

    // Virtual-hosted style over TLS: bucket is a Host subdomain, key is the path.
    let vhost = format!("{BUCKET}.{DOMAIN}");
    let vurl = format!("{base}/warehouse/part-0.parquet");
    let response = client
        .get(&vurl)
        .headers(sign("GET", &vurl, &vhost, ENGINE_KEYS.0, ENGINE_KEYS.1))
        .send()
        .await
        .expect("virtual-hosted TLS GET");
    assert_eq!(response.status(), 200, "virtual-hosted GET over TLS");
    assert_eq!(
        response.bytes().await.expect("vhost body"),
        body,
        "virtual-hosted resolves the same object as path-style"
    );

    let stderr = stop_daemon(daemon, tap);
    assert!(
        stderr.contains("https://"),
        "expected an https S3 boot log, stderr: {stderr}"
    );
}

#[tokio::test]
async fn cert_rotation_under_load_drops_no_requests() {
    let body = pattern(20_000);
    let origin_url = spawn_origin(&[("warehouse/part-0.parquet", body.clone())]).await;
    let (daemon, base, cert_path, key_path, tap) = spawn_tls_daemon(&origin_url, "rotate");
    let pid = daemon.id();
    let client = tls_client();
    wait_for_ready(&client, &base, "warehouse/part-0.parquet").await;

    let url = Arc::new(format!("{base}/{BUCKET}/warehouse/part-0.parquet"));
    let failures = Arc::new(AtomicUsize::new(0));
    let successes = Arc::new(AtomicUsize::new(0));

    // Flood the endpoint from several workers, each doing a run of sequential
    // signed GETs, so requests are in flight across the rotation.
    let mut workers = Vec::new();
    for _ in 0..8 {
        let client = client.clone();
        let url = url.clone();
        let failures = failures.clone();
        let successes = successes.clone();
        workers.push(tokio::spawn(async move {
            for _ in 0..40 {
                let headers = sign("GET", &url, DOMAIN, ENGINE_KEYS.0, ENGINE_KEYS.1);
                match client.get(url.as_str()).headers(headers).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        // Drain the body so the connection is cleanly reusable.
                        let _ = resp.bytes().await;
                        successes.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }));
    }

    // Midway through the flood, rotate the cert on disk and SIGHUP the daemon.
    tokio::time::sleep(Duration::from_millis(40)).await;
    let (cert_pem, key_pem) = self_signed_pem();
    std::fs::write(&cert_path, cert_pem).expect("rewrite cert");
    std::fs::write(&key_path, key_pem).expect("rewrite key");
    let killed = Command::new("kill")
        .arg("-HUP")
        .arg(pid.to_string())
        .status()
        .expect("send SIGHUP");
    assert!(killed.success(), "SIGHUP must be delivered");

    for worker in workers {
        worker.await.expect("worker join");
    }

    let failed = failures.load(Ordering::Relaxed);
    let ok = successes.load(Ordering::Relaxed);
    let stderr = stop_daemon(daemon, tap);
    assert_eq!(
        failed, 0,
        "cert rotation under load must drop zero requests ({ok} ok, {failed} failed); stderr: {stderr}"
    );
    assert_eq!(ok, 8 * 40, "every request must have succeeded");
    assert!(
        stderr.contains("reloaded TLS certificate"),
        "daemon must log the SIGHUP-driven cert reload; stderr: {stderr}"
    );
}
