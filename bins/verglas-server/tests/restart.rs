//! End-to-end restart-recovery tests for the server (issue #16): warm the
//! cache through the real `verglas-server` binary, `kill -9` it mid-life, restart it
//! over the *same* `cache.dir`, and prove the previously cached object serves
//! from the recovered NVMe tier with zero backend block re-fills — while the
//! health endpoint gates serving until recovery completes.
//!
//! The harness mirrors `s3_smoke.rs`: an in-process mock S3 origin behind
//! verglas-s3's own front-end, a spawned server signing SigV4 against it, and
//! the client signing SigV4 against the server. The one difference that matters
//! here is the server's `cache.dir` is a fixed path passed in, so the second
//! server re-opens the first server's on-disk cache.

use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use verglas_core::admin::StatsInfo;
use verglas_s3::{
    BackendStore, NoopInvalidation, PassthroughList, PassthroughRead, PassthroughWrite,
};

/// Bucket the origin holds and the server serves.
const BUCKET: &str = "restart-lake";
/// Keypair the server's S3 client presents to the mock origin.
const ORIGIN_KEYS: (&str, &str) = ("origin-ak", "origin-secret");
/// Keypair engines present to the server's S3 endpoint.
const ENGINE_KEYS: (&str, &str) = ("engine-ak", "engine-secret");
/// Region used for SigV4 signing in this test.
const REGION: &str = "us-east-1";
/// SHA-256 of an empty request body.
const EMPTY_PAYLOAD_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
/// One mebibyte.
const MIB: usize = 1024 * 1024;

/// Signs an HTTP request with the given static keypair (SigV4, header auth).
fn sign_with_keys(
    method: &str,
    url: &str,
    access_key: &str,
    secret_key: &str,
    extra: &[(&str, &str)],
) -> HeaderMap {
    let datetime = Utc::now();
    let host = url
        .strip_prefix("http://")
        .and_then(|rest| rest.split('/').next())
        .expect("host");
    let full_path = url
        .strip_prefix("http://")
        .and_then(|rest| rest.find('/').map(|idx| &rest[idx..]))
        .unwrap_or("/");
    let (path, canonical_query) = match full_path.split_once('?') {
        Some((path, query)) => (path, canonical_query_string(query)),
        None => (full_path, String::new()),
    };
    let amz_date = datetime.format("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = datetime.format("%Y%m%d").to_string();

    let mut header_pairs = vec![
        ("host", host),
        ("x-amz-date", amz_date.as_str()),
        ("x-amz-content-sha256", EMPTY_PAYLOAD_HASH),
    ];
    for (name, value) in extra {
        if let Some(existing) = header_pairs.iter_mut().find(|(n, _)| *n == *name) {
            existing.1 = value;
        } else {
            header_pairs.push((name, value));
        }
    }
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
    let payload_hash = header_pairs
        .iter()
        .find(|(name, _)| *name == "x-amz-content-sha256")
        .map(|(_, value)| *value)
        .unwrap_or(EMPTY_PAYLOAD_HASH);

    let canonical_request = format!(
        "{method}\n{path}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
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
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&authorization).expect("auth"),
    );
    headers
}

/// Signs a request against the server with the engine keypair.
fn sign_request(method: &str, url: &str, extra: &[(&str, &str)]) -> HeaderMap {
    sign_with_keys(method, url, ENGINE_KEYS.0, ENGINE_KEYS.1, extra)
}

/// Builds the canonical query string for SigV4 from a raw query.
fn canonical_query_string(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut params: Vec<(String, String)> = query
        .split('&')
        .filter(|segment| !segment.is_empty())
        .map(|pair| {
            if let Some((key, value)) = pair.split_once('=') {
                (key.to_owned(), value.to_owned())
            } else {
                (pair.to_owned(), String::new())
            }
        })
        .collect();
    params.sort_by(|left, right| left.0.cmp(&right.0));
    params
        .iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                urlencoding::encode(key),
                urlencoding::encode(value)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
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
fn pattern(len: usize) -> Bytes {
    (0..len)
        .map(|i| (i % 251) as u8)
        .collect::<Vec<u8>>()
        .into()
}

/// Reserves an ephemeral port (bind-then-drop).
fn free_addr() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);
    addr
}

/// Serves the mock S3 origin (in-memory store behind verglas-s3's front-end).
async fn spawn_origin(objects: &[(&str, Bytes)]) -> String {
    let store = Arc::new(InMemory::new());
    for (key, bytes) in objects {
        store
            .put(&Path::from(*key), PutPayload::from(bytes.clone()))
            .await
            .expect("seed origin object");
    }
    let app = verglas_s3::router(
        "managed-lakehouse",
        PassthroughRead::new(BackendStore::single(
            "managed-lakehouse",
            BUCKET,
            store.clone(),
        )),
        PassthroughWrite::new(BackendStore::single(
            "managed-lakehouse",
            BUCKET,
            store.clone(),
        )),
        Arc::new(PassthroughList::new(BackendStore::single(
            "managed-lakehouse",
            BUCKET,
            store,
        ))),
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

/// A spawned server and the URLs the test drives it through.
struct Server {
    child: Child,
    s3_base: String,
    admin: String,
}

/// Spawns `verglas-server` over a *fixed* `cache_dir` pointed at the mock origin, so a
/// later server over the same directory re-opens the first server's on-disk
/// cache (the whole point of #16). DRAM is at the engine floor (a two-block
/// memory tier) so the working set spills to and is served from NVMe.
fn spawn_server(origin_url: &str, cache_dir: &std::path::Path) -> Server {
    std::fs::create_dir_all(cache_dir).expect("create cache dir");
    let parent = cache_dir.parent().expect("parent");
    // The endpoint keypair lives in an AWS-format credentials file the config
    // names, never inline in config.toml. It sits beside the config so the
    // restarted server over the same directory reads the same keypair.
    let creds_path = parent.join("endpoint-credentials");
    std::fs::write(
        &creds_path,
        "[default]\naws_access_key_id = engine-ak\naws_secret_access_key = engine-secret\n",
    )
    .expect("write endpoint credentials");
    let config_path = parent.join("verglas.toml");
    std::fs::write(
        &config_path,
        format!(
            "[cache]\ndir = \"{}\"\ncapacity_bytes = \"256MB\"\ndram_bytes = \"80MB\"\ndata_block_bytes = \"8MB\"\n\n\
             [auth]\ncredentials_file = \"{}\"\n\n\
             [backend]\nbucket = \"{BUCKET}\"\n",
            cache_dir.display(),
            creds_path.display()
        ),
    )
    .expect("write config");

    let s3_addr = free_addr();
    let admin_addr = free_addr();
    let child = Command::new(env!("CARGO_BIN_EXE_verglas-server"))
        .arg("--config")
        .arg(&config_path)
        .env("VERGLAS_ADMIN_ADDR", admin_addr.to_string())
        .env("VERGLAS_S3_ADDR", s3_addr.to_string())
        .env("AWS_ACCESS_KEY_ID", ORIGIN_KEYS.0)
        .env("AWS_SECRET_ACCESS_KEY", ORIGIN_KEYS.1)
        .env("AWS_REGION", "us-east-1")
        .env("AWS_ENDPOINT", origin_url)
        .env("AWS_ALLOW_HTTP", "true")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn verglas-server");
    Server {
        child,
        s3_base: format!("http://{s3_addr}"),
        admin: format!("http://{admin_addr}"),
    }
}

/// Polls `/admin/healthz` until it reports ready (`200 ok`). This is the
/// serve-gating signal: while disk recovery runs the endpoint answers `503
/// starting`, so `is_success()` returning is itself proof recovery finished.
async fn wait_until_ready(client: &reqwest::Client, admin: &str) {
    for _ in 0..100 {
        if let Ok(response) = client.get(format!("{admin}/admin/healthz")).send().await
            && response.status().is_success()
        {
            let info: verglas_core::admin::HealthzInfo = response.json().await.expect("healthz");
            assert_eq!(info.status, "ok", "ready server must report ok");
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server did not become ready at {admin}");
}

/// Reads the server's live read-path counters.
async fn stats(client: &reqwest::Client, admin: &str) -> StatsInfo {
    client
        .get(format!("{admin}/admin/stats"))
        .send()
        .await
        .expect("stats request")
        .json()
        .await
        .expect("stats json")
}

/// Signs and issues a full GET through the server, returning the body.
async fn get_object(client: &reqwest::Client, url: &str) -> Bytes {
    let response = client
        .get(url)
        .headers(sign_request("GET", url, &[]))
        .send()
        .await
        .expect("GET");
    assert_eq!(response.status(), 200, "GET {url}");
    response.bytes().await.expect("body")
}

/// The end-to-end acceptance criterion (#16): populate the cache, `kill -9` the
/// server, restart it over the same directory, and prove the previously cached
/// object serves from the recovered NVMe tier with zero backend block re-fills
/// — after one metadata round trip (a HEAD) to re-resolve the DRAM-only mapping.
/// Health gating is proven too: the restarted server reports `ok` only once
/// recovery has completed.
#[tokio::test]
async fn kill_dash_nine_restart_serves_from_nvme_with_zero_refills() {
    let object = "warehouse/part-0.parquet";
    let body = pattern(24 * MIB); // three 8 MiB blocks
    // Filler objects give us a way to evict the target fully from the two-block
    // memory tier, so a subsequent full re-read serves *every* block from disk —
    // the deterministic proof that all three blocks are durable on NVMe before
    // the crash (write-through to NVMe is asynchronous, so "some" is not enough).
    let fillers: Vec<(String, Bytes)> = (0..4)
        .map(|i| (format!("warehouse/filler-{i}.parquet"), pattern(8 * MIB)))
        .collect();
    let mut seeded: Vec<(&str, Bytes)> = vec![(object, body.clone())];
    seeded.extend(fillers.iter().map(|(n, b)| (n.as_str(), b.clone())));
    let origin_url = spawn_origin(&seeded).await;

    let scratch =
        std::env::temp_dir().join(format!("verglas-server-restart-{}", std::process::id()));
    let cache_dir = scratch.join("cache");
    let client = reqwest::Client::new();
    let url = format!("http://HOST/{BUCKET}/{object}");

    // --- First server: warm the object onto the NVMe tier. ---
    let mut first = spawn_server(&origin_url, &cache_dir);
    wait_until_ready(&client, &first.admin).await;
    let host1 = first
        .s3_base
        .strip_prefix("http://")
        .expect("http url")
        .to_owned();
    let url1 = url.replace("HOST", &host1);

    assert_eq!(get_object(&client, &url1).await, body, "warm read");

    // Confirm all three blocks are durable on NVMe: evict the target from the
    // memory tier by reading the fillers, then read the target in full and
    // require that read to be served entirely from disk (three disk hits).
    let mut all_on_disk = false;
    for _ in 0..50 {
        for (name, fbody) in &fillers {
            let furl = format!("http://{host1}/{BUCKET}/{name}");
            assert_eq!(get_object(&client, &furl).await, *fbody, "warm filler");
        }
        let before = stats(&client, &first.admin).await.counters.disk_hits;
        assert_eq!(get_object(&client, &url1).await, body, "re-read target");
        let delta = stats(&client, &first.admin).await.counters.disk_hits - before;
        if delta == 3 {
            all_on_disk = true;
            break;
        }
    }
    assert!(
        all_on_disk,
        "all three blocks must be durable on NVMe before the crash"
    );

    // --- kill -9: std's Child::kill sends SIGKILL on Unix — an unclean crash,
    // no graceful flush. Only what already reached NVMe can survive. ---
    first.child.kill().expect("SIGKILL the server");
    let _ = first.child.wait();

    // --- Second server over the SAME cache dir: recovery must serve the warm
    // cache. Fresh process ⇒ fresh counters, so post-restart fills start at 0. ---
    let mut second = spawn_server(&origin_url, &cache_dir);
    wait_until_ready(&client, &second.admin).await;
    let host2 = second
        .s3_base
        .strip_prefix("http://")
        .expect("http url")
        .to_owned();
    let url2 = url.replace("HOST", &host2);

    // The DRAM-only mapping did not survive: a HEAD re-resolves it (one metadata
    // round trip, no block bytes). Model the engine's HEAD-then-GET access.
    let head = client
        .head(&url2)
        .headers(sign_request("HEAD", &url2, &[]))
        .send()
        .await
        .expect("HEAD");
    assert_eq!(head.status(), 200, "HEAD re-resolves the mapping");

    // The GET now serves every block from the recovered NVMe tier: byte
    // identical, and — the criterion — zero backend block fills.
    assert_eq!(
        get_object(&client, &url2).await,
        body,
        "post-restart read must be byte-identical to the origin"
    );
    let after = stats(&client, &second.admin).await;
    assert_eq!(
        after.counters.backend_fills, 0,
        "recovered blocks must be reused with zero backend block re-fills, got {}",
        after.counters.backend_fills
    );
    assert!(
        after.counters.disk_hits > 0,
        "the object must be served from the recovered NVMe tier, got {} disk hits",
        after.counters.disk_hits
    );
    assert_eq!(
        after.counters.backend_heads, 1,
        "exactly one metadata round trip re-resolved the DRAM-only mapping"
    );

    second.child.kill().expect("stop server");
    let _ = second.child.wait();
    let _ = std::fs::remove_dir_all(&scratch);
}

/// Metadata that remains in the DRAM front must still survive an unclean
/// server restart. The single object is smaller than the metadata DRAM budget,
/// so recovery can only succeed when insertion also writes it to NVMe.
#[tokio::test]
async fn kill_dash_nine_restart_recovers_metadata_that_never_evicted_from_dram() {
    let object = "warehouse/metadata/v1.metadata.json";
    let body = pattern(256 * 1024);
    let origin_url = spawn_origin(&[(object, body.clone())]).await;
    let scratch = std::env::temp_dir().join(format!(
        "verglas-server-meta-restart-{}",
        std::process::id()
    ));
    let cache_dir = scratch.join("cache");
    let client = reqwest::Client::new();

    let mut first = spawn_server(&origin_url, &cache_dir);
    wait_until_ready(&client, &first.admin).await;
    let host1 = first.s3_base.strip_prefix("http://").expect("http url");
    let url1 = format!("http://{host1}/{BUCKET}/{object}");
    assert_eq!(get_object(&client, &url1).await, body, "warm metadata");
    tokio::time::sleep(Duration::from_millis(500)).await;
    first.child.kill().expect("SIGKILL first server");
    let _ = first.child.wait();

    let mut second = spawn_server(&origin_url, &cache_dir);
    wait_until_ready(&client, &second.admin).await;
    let host2 = second.s3_base.strip_prefix("http://").expect("http url");
    let url2 = format!("http://{host2}/{BUCKET}/{object}");
    let head = client
        .head(&url2)
        .headers(sign_request("HEAD", &url2, &[]))
        .send()
        .await
        .expect("HEAD after restart");
    assert_eq!(head.status(), 200, "mapping re-resolves after restart");
    assert_eq!(get_object(&client, &url2).await, body, "recovered metadata");

    let counters = stats(&client, &second.admin).await.counters;
    assert_eq!(
        counters.meta_hits, 1,
        "metadata that never left DRAM must recover from NVMe"
    );
    assert_eq!(
        counters.meta_misses, 0,
        "recovered metadata must not refill from the origin"
    );

    second.child.kill().expect("stop second server");
    let _ = second.child.wait();
    std::fs::remove_dir_all(&scratch).ok();
}
