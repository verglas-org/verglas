//! End-to-end smoke tests for the server's S3 endpoint: spawns the real
//! `verglas-server` binary with a temp config, points its backend at a mock S3
//! origin served in-process (verglas-s3's own front-end over an in-memory
//! store — the passthrough signs real SigV4 requests against it), and hits
//! the server with SigV4-signed HTTP. Covers reads (issue #6: bytes, headers,
//! error XML) and writes (issue #9: put/copy/delete/multipart passed through
//! durably to the origin).

use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use verglas_s3::{
    BackendStore, NoopInvalidation, PassthroughList, PassthroughRead, PassthroughWrite,
};

/// Bucket the origin holds and the server serves.
const BUCKET: &str = "e2e-lake";
/// Keypair the server's S3 client presents to the mock origin.
const ORIGIN_KEYS: (&str, &str) = ("origin-ak", "origin-secret");
/// Keypair engines present to the server's S3 endpoint.
const ENGINE_KEYS: (&str, &str) = ("engine-ak", "engine-secret");
/// Region used for SigV4 signing in this test.
const REGION: &str = "us-east-1";

/// SHA-256 of an empty request body.
const EMPTY_PAYLOAD_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Signs an HTTP request with the given static keypair.
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

/// Signs an HTTP request against the server with the engine keypair.
fn sign_request(method: &str, url: &str, extra: &[(&str, &str)]) -> HeaderMap {
    sign_with_keys(method, url, ENGINE_KEYS.0, ENGINE_KEYS.1, extra)
}

/// Signs an HTTP request against the mock origin with the origin keypair.
fn sign_origin(method: &str, url: &str, extra: &[(&str, &str)]) -> HeaderMap {
    sign_with_keys(method, url, ORIGIN_KEYS.0, ORIGIN_KEYS.1, extra)
}

/// Builds the canonical query string for SigV4 from a raw query (no leading `?`).
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
fn pattern(len: u64) -> Bytes {
    (0..len)
        .map(|i| (i % 251) as u8)
        .collect::<Vec<u8>>()
        .into()
}

/// Reserves an ephemeral port. Bind-then-drop, same trade-off as the admin
/// API tests: a tiny race window in exchange for not parsing server logs.
fn free_port() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);
    addr
}

/// Serves the mock S3 origin (in-memory store behind verglas-s3's own
/// front-end, validating the static origin keypair) and returns its URL.
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

/// Serves a mock origin over a bucket GLOB set (#235): the origin front-end
/// serves any bucket matching `glob`, all backed by one in-memory store seeded
/// with `(key, bytes)` pairs. Used to prove the server serves a glob-matched
/// bucket end-to-end. One bucket's objects at a time (all buckets share the
/// store), which is all this test needs.
async fn spawn_origin_glob(glob: &str, objects: &[(&str, Bytes)]) -> String {
    let store = Arc::new(InMemory::new());
    for (key, bytes) in objects {
        store
            .put(&Path::from(*key), PutPayload::from(bytes.clone()))
            .await
            .expect("seed origin object");
    }
    let make = || {
        BackendStore::with_glob_factory(
            "managed-lakehouse",
            None,
            vec![glob.to_owned()],
            64,
            Default::default(),
            Default::default(),
            {
                let store = store.clone();
                move |_bucket| Ok(store.clone() as Arc<dyn verglas_s3::MultipartObjectStore>)
            },
        )
    };
    let app = verglas_s3::router(
        "managed-lakehouse",
        PassthroughRead::new(make()),
        PassthroughWrite::new(make()),
        Arc::new(PassthroughList::new(make())),
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

/// Spawns `verglas-server` with a temp config whose `[backend]` body is `backend_toml`
/// (so a test can name a single bucket or `bucket_globs`), pointed at the mock
/// origin via the AWS env overrides the standard credential chain honors.
fn spawn_server_with_backend(origin_url: &str, tag: &str, backend_toml: &str) -> (Child, String) {
    let scratch = std::env::temp_dir().join(format!(
        "verglas-server-s3-smoke-{tag}-{}",
        std::process::id()
    ));
    let cache_dir = scratch.join("cache");
    std::fs::create_dir_all(&cache_dir).expect("create cache dir");
    let creds_path = scratch.join("endpoint-credentials");
    std::fs::write(
        &creds_path,
        "[default]\naws_access_key_id = engine-ak\naws_secret_access_key = engine-secret\n",
    )
    .expect("write endpoint credentials");
    let config_path = scratch.join("verglas.toml");
    std::fs::write(
        &config_path,
        format!(
            "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n\n\
             [auth]\ncredentials_file = \"{}\"\n\n\
             [backend]\n{backend_toml}\n",
            cache_dir.display(),
            creds_path.display()
        ),
    )
    .expect("write config");

    let s3_addr = free_port();
    let server = Command::new(env!("CARGO_BIN_EXE_verglas-server"))
        .arg("--config")
        .arg(&config_path)
        .env("VERGLAS_ADMIN_ADDR", "127.0.0.1:0")
        .env("VERGLAS_S3_ADDR", s3_addr.to_string())
        .env("AWS_ACCESS_KEY_ID", ORIGIN_KEYS.0)
        .env("AWS_SECRET_ACCESS_KEY", ORIGIN_KEYS.1)
        .env("AWS_REGION", "us-east-1")
        .env("AWS_ENDPOINT", origin_url)
        .env("AWS_ALLOW_HTTP", "true")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn verglas-server");
    (server, format!("http://{s3_addr}"))
}

/// Spawns `verglas-server` with a temp config whose backend points at the mock
/// origin via the AWS env overrides the standard credential chain honors.
/// `tag` keeps each test's scratch dir distinct: the cache engine owns its
/// `cache.dir` exclusively, so two servers must never share one.
fn spawn_server(origin_url: &str, tag: &str) -> (Child, String) {
    let scratch = std::env::temp_dir().join(format!(
        "verglas-server-s3-smoke-{tag}-{}",
        std::process::id()
    ));
    let cache_dir = scratch.join("cache");
    std::fs::create_dir_all(&cache_dir).expect("create cache dir");
    let creds_path = scratch.join("endpoint-credentials");
    std::fs::write(
        &creds_path,
        "[default]\naws_access_key_id = engine-ak\naws_secret_access_key = engine-secret\n",
    )
    .expect("write endpoint credentials");
    let config_path = scratch.join("verglas.toml");
    std::fs::write(
        &config_path,
        format!(
            "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n\n\
             [auth]\ncredentials_file = \"{}\"\n\n\
             [backend]\nbucket = \"{BUCKET}\"\n",
            cache_dir.display(),
            creds_path.display()
        ),
    )
    .expect("write config");

    let s3_addr = free_port();
    let server = Command::new(env!("CARGO_BIN_EXE_verglas-server"))
        .arg("--config")
        .arg(&config_path)
        .env("VERGLAS_ADMIN_ADDR", "127.0.0.1:0")
        .env("VERGLAS_S3_ADDR", s3_addr.to_string())
        .env("AWS_ACCESS_KEY_ID", ORIGIN_KEYS.0)
        .env("AWS_SECRET_ACCESS_KEY", ORIGIN_KEYS.1)
        .env("AWS_REGION", "us-east-1")
        .env("AWS_ENDPOINT", origin_url)
        .env("AWS_ALLOW_HTTP", "true")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn verglas-server");
    (server, format!("http://{s3_addr}"))
}

/// Stops a spawned server, returning its captured stderr.
fn stop_server(mut server: Child) -> String {
    let _ = server.kill();
    let output = server.wait_with_output().expect("reap server");
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// Polls the server's S3 endpoint until a signed GET succeeds.
async fn wait_for_ready(client: &reqwest::Client, url: &str) {
    for _ in 0..100 {
        if let Ok(response) = client
            .get(url)
            .headers(sign_request("GET", url, &[]))
            .send()
            .await
            && response.status().is_success()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server S3 endpoint did not become ready at {url}");
}

#[tokio::test]
async fn server_serves_reads_through_to_origin() {
    let body = pattern(100_000);
    let origin_url = spawn_origin(&[("warehouse/part-0.parquet", body.clone())]).await;
    let (server, base) = spawn_server(&origin_url, "reads");
    let client = reqwest::Client::new();
    let url = format!("{base}/{BUCKET}/warehouse/part-0.parquet");
    wait_for_ready(&client, &url).await;

    // Full GET: byte-identical body, S3-shaped headers.
    let response = client
        .get(&url)
        .headers(sign_request("GET", &url, &[]))
        .send()
        .await
        .expect("GET");
    assert_eq!(response.status(), 200);
    assert_eq!(
        response
            .headers()
            .get("content-length")
            .expect("content-length"),
        &body.len().to_string()
    );
    assert_eq!(
        response
            .headers()
            .get("accept-ranges")
            .expect("accept-ranges"),
        "bytes"
    );
    let etag = response
        .headers()
        .get("etag")
        .expect("etag")
        .to_str()
        .expect("ascii")
        .to_owned();
    assert!(
        etag.starts_with('"') && etag.ends_with('"'),
        "unquoted ETag {etag}"
    );
    assert!(response.headers().contains_key("last-modified"));
    assert_eq!(response.bytes().await.expect("body"), body);

    // Ranged GET: 206 with correct Content-Range and slice.
    let response = client
        .get(&url)
        .headers(sign_request("GET", &url, &[("range", "bytes=1000-1999")]))
        .send()
        .await
        .expect("ranged GET");
    assert_eq!(response.status(), 206);
    assert_eq!(
        response
            .headers()
            .get("content-range")
            .expect("content-range"),
        "bytes 1000-1999/100000"
    );
    assert_eq!(
        response.bytes().await.expect("body"),
        body.slice(1000..2000)
    );

    // HEAD: metadata parity with GET.
    let response = client
        .head(&url)
        .headers(sign_request("HEAD", &url, &[]))
        .send()
        .await
        .expect("HEAD");
    assert_eq!(response.status(), 200);
    assert_eq!(
        response
            .headers()
            .get("etag")
            .expect("etag")
            .to_str()
            .expect("ascii"),
        etag
    );
    assert_eq!(
        response
            .headers()
            .get("content-length")
            .expect("content-length"),
        &body.len().to_string()
    );

    // Missing key travels origin 404 -> passthrough -> S3 error XML.
    let missing_url = format!("{base}/{BUCKET}/warehouse/nope");
    let response = client
        .get(&missing_url)
        .headers(sign_request("GET", &missing_url, &[]))
        .send()
        .await
        .expect("GET missing");
    assert_eq!(response.status(), 404);
    let text = response.text().await.expect("body");
    assert!(text.contains("<Code>NoSuchKey</Code>"), "got: {text}");

    // Bucket-set serving (#235): this config names a single `backend.bucket`,
    // so it serves exactly that bucket. A request to a bucket outside the set is
    // rejected at the server with S3 NoSuchBucket (404), before the origin.
    let other_bucket_url = format!("{base}/another-lake/warehouse/part-0.parquet");
    let response = client
        .get(&other_bucket_url)
        .headers(sign_request("GET", &other_bucket_url, &[]))
        .send()
        .await
        .expect("GET second bucket");
    assert_eq!(
        response.status(),
        404,
        "an unconfigured bucket must be rejected"
    );
    let text = response.text().await.expect("body");
    assert!(text.contains("<Code>NoSuchBucket</Code>"), "got: {text}");

    let stderr = stop_server(server);
    assert!(
        stderr.contains("serving S3 on"),
        "expected the one-line S3 boot log, stderr: {stderr}"
    );
}

#[tokio::test]
async fn server_serves_a_glob_matched_bucket_and_rejects_non_matches() {
    // Bucket-set serving via a glob (#235): the server is configured with
    // `bucket_globs = ["*--table-s3"]` (the AWS S3 Tables shape) and no single
    // bucket. A read for a bucket MATCHING the glob is served end-to-end (its
    // origin client built lazily on first request), returning the seeded bytes.
    // A read for a bucket OUTSIDE the set is rejected at the server with
    // NoSuchBucket, before the origin is consulted.
    let body = pattern(4_096);
    let origin_url = spawn_origin_glob("*--table-s3", &[("data/rows.parquet", body.clone())]).await;
    let (server, base) =
        spawn_server_with_backend(&origin_url, "glob", "bucket_globs = [\"*--table-s3\"]");
    let client = reqwest::Client::new();

    // A glob-matching bucket serves the object through to the origin.
    let matched = format!("{base}/909ef3f6-a7df--table-s3/data/rows.parquet");
    wait_for_ready(&client, &matched).await;
    let response = client
        .get(&matched)
        .headers(sign_request("GET", &matched, &[]))
        .send()
        .await
        .expect("GET glob-matched bucket");
    assert_eq!(
        response.status(),
        200,
        "a bucket matching the glob must be served"
    );
    assert_eq!(response.bytes().await.expect("body"), body);

    // A bucket outside the set is NoSuchBucket, before the origin.
    let unmatched = format!("{base}/plain-bucket/data/rows.parquet");
    let response = client
        .get(&unmatched)
        .headers(sign_request("GET", &unmatched, &[]))
        .send()
        .await
        .expect("GET non-matching bucket");
    assert_eq!(response.status(), 404);
    let text = response.text().await.expect("body");
    assert!(
        text.contains("<Code>NoSuchBucket</Code>"),
        "a bucket outside the glob set must be NoSuchBucket, got: {text}"
    );

    stop_server(server);
}

/// Extracts the text of the first `<tag>...</tag>` in an XML body.
fn xml_tag(body: &str, tag: &str) -> String {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body
        .find(&open)
        .unwrap_or_else(|| panic!("no <{tag}> in: {body}"))
        + open.len();
    let end = start + body[start..].find(&close).expect("closing tag");
    body[start..end].to_owned()
}

#[tokio::test]
async fn server_passes_writes_through_to_origin() {
    // The origin starts with one seeded object so the readiness probe from
    // the read smoke test can be reused unchanged.
    let origin_url = spawn_origin(&[("seed.bin", pattern(10))]).await;
    let (server, base) = spawn_server(&origin_url, "writes");
    let client = reqwest::Client::new();
    wait_for_ready(&client, &format!("{base}/{BUCKET}/seed.bin")).await;

    // PUT through the server: small (one atomic origin PUT) and large
    // (crosses the server's 8 MiB threshold, streaming to the origin as a
    // multipart upload via the signed S3 client).
    for (key, size) in [
        ("writes/small.bin", 100_000u64),
        ("writes/large.bin", 20 * 1024 * 1024),
    ] {
        let body = pattern(size);
        let url = format!("{base}/{BUCKET}/{key}");
        let payload_hash = hex::encode(sha256(body.as_ref()));
        let response = client
            .put(&url)
            .headers(sign_request(
                "PUT",
                &url,
                &[("x-amz-content-sha256", &payload_hash)],
            ))
            .body(body.clone())
            .send()
            .await
            .expect("PUT through server");
        assert_eq!(response.status(), 200, "PUT {key}");
        assert!(
            response.headers().contains_key("etag"),
            "PUT ack must carry the backend ETag"
        );

        // Readable back through the server...
        let response = client
            .get(&url)
            .headers(sign_request("GET", &url, &[]))
            .send()
            .await
            .expect("GET through server");
        assert_eq!(response.status(), 200);
        assert_eq!(response.bytes().await.expect("body"), body, "{key}");

        // ...and durable at the origin itself — Verglas is never the only
        // copy of any byte.
        let origin_get = format!("{origin_url}/{BUCKET}/{key}");
        let response = client
            .get(&origin_get)
            .headers(sign_origin("GET", &origin_get, &[]))
            .send()
            .await
            .expect("GET direct from origin");
        assert_eq!(response.status(), 200, "{key} must be durable at origin");
        assert_eq!(
            response.bytes().await.expect("body"),
            body,
            "{key} at origin"
        );
    }

    // CopyObject through the server.
    let copy_url = format!("{base}/{BUCKET}/writes/copy.bin");
    let response = client
        .put(&copy_url)
        .headers(sign_request(
            "PUT",
            &copy_url,
            &[
                ("x-amz-copy-source", &format!("{BUCKET}/writes/small.bin")),
                ("x-amz-content-sha256", EMPTY_PAYLOAD_HASH),
            ],
        ))
        .send()
        .await
        .expect("COPY through server");
    assert_eq!(response.status(), 200);
    let origin_copy = format!("{origin_url}/{BUCKET}/writes/copy.bin");
    let response = client
        .get(&origin_copy)
        .headers(sign_origin("GET", &origin_copy, &[]))
        .send()
        .await
        .expect("GET copy from origin");
    assert_eq!(response.status(), 200);
    assert_eq!(response.bytes().await.expect("body"), pattern(100_000));

    // DeleteObject through the server: gone from the origin, NoSuchKey back
    // through the server.
    let delete_url = format!("{base}/{BUCKET}/writes/small.bin");
    let response = client
        .delete(&delete_url)
        .headers(sign_request("DELETE", &delete_url, &[]))
        .send()
        .await
        .expect("DELETE through server");
    assert_eq!(response.status(), 204);
    let response = client
        .get(&delete_url)
        .headers(sign_request("GET", &delete_url, &[]))
        .send()
        .await
        .expect("GET after delete");
    assert_eq!(response.status(), 404);
    let text = response.text().await.expect("body");
    assert!(text.contains("<Code>NoSuchKey</Code>"), "got: {text}");

    // Client-driven multipart lifecycle through the server: the origin's
    // upload ID round-trips through server and client untouched.
    let url = format!("{base}/{BUCKET}/writes/multipart.bin");
    let create_url = format!("{url}?uploads");
    let response = client
        .post(&create_url)
        .headers(sign_request("POST", &create_url, &[]))
        .send()
        .await
        .expect("CreateMultipartUpload through server");
    assert_eq!(response.status(), 200);
    let upload_id = xml_tag(&response.text().await.expect("body"), "UploadId");

    let part1 = pattern(6 * 1024 * 1024);
    let part2 = pattern(1_234_567);
    let mut etags = Vec::new();
    for (number, part) in [(1, part1.clone()), (2, part2.clone())] {
        let part_url = format!("{url}?partNumber={number}&uploadId={upload_id}");
        let payload_hash = hex::encode(sha256(part.as_ref()));
        let response = client
            .put(&part_url)
            .headers(sign_request(
                "PUT",
                &part_url,
                &[("x-amz-content-sha256", &payload_hash)],
            ))
            .body(part)
            .send()
            .await
            .expect("UploadPart through server");
        assert_eq!(response.status(), 200, "part {number}");
        etags.push(
            response
                .headers()
                .get("etag")
                .expect("part ETag")
                .to_str()
                .expect("ascii")
                .to_owned(),
        );
    }
    let complete_body = format!(
        "<CompleteMultipartUpload>\
           <Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part>\
           <Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part>\
         </CompleteMultipartUpload>",
        etags[0], etags[1]
    );
    let complete_url = format!("{url}?uploadId={upload_id}");
    let complete_hash = hex::encode(sha256(complete_body.as_bytes()));
    let response = client
        .post(&complete_url)
        .header("content-type", "application/xml")
        .headers(sign_request(
            "POST",
            &complete_url,
            &[
                ("content-type", "application/xml"),
                ("x-amz-content-sha256", &complete_hash),
            ],
        ))
        .body(complete_body)
        .send()
        .await
        .expect("CompleteMultipartUpload through server");
    assert_eq!(response.status(), 200);

    let mut expected = part1.to_vec();
    expected.extend_from_slice(&part2);
    let origin_multipart = format!("{origin_url}/{BUCKET}/writes/multipart.bin");
    let response = client
        .get(&origin_multipart)
        .headers(sign_origin("GET", &origin_multipart, &[]))
        .send()
        .await
        .expect("GET assembled object from origin");
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.bytes().await.expect("body").as_ref(),
        &expected[..],
        "assembled object at origin differs"
    );

    // A bogus upload ID travels server -> origin 404 -> NoSuchUpload XML.
    let bogus_url = format!("{url}?partNumber=1&uploadId=bogus-id");
    let bogus_hash = hex::encode(sha256(pattern(10).as_ref()));
    let response = client
        .put(&bogus_url)
        .headers(sign_request(
            "PUT",
            &bogus_url,
            &[("x-amz-content-sha256", &bogus_hash)],
        ))
        .body(pattern(10))
        .send()
        .await
        .expect("UploadPart with bogus ID");
    assert_eq!(response.status(), 404);
    let text = response.text().await.expect("body");
    assert!(text.contains("<Code>NoSuchUpload</Code>"), "got: {text}");

    stop_server(server);
}

#[tokio::test]
async fn server_lists_through_to_origin_with_pagination_and_delimiter() {
    // Seed enough keys under one prefix to force a second page at max-keys=2,
    // plus a nested "directory" to exercise delimiter roll-up.
    let origin_url = spawn_origin(&[
        ("warehouse/data/a.parquet", pattern(10)),
        ("warehouse/data/b.parquet", pattern(10)),
        ("warehouse/data/c.parquet", pattern(10)),
        ("warehouse/meta/v1.json", pattern(10)),
    ])
    .await;
    let (server, base) = spawn_server(&origin_url, "list");
    let client = reqwest::Client::new();
    let ready_url = format!("{base}/{BUCKET}/warehouse/data/a.parquet");
    wait_for_ready(&client, &ready_url).await;

    // Paginated recursive walk under the prefix: follow continuation tokens and
    // collect every key. Must reproduce exactly the four seeded keys' data
    // objects under warehouse/data + warehouse/meta.
    let mut seen = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let query = match &token {
            Some(t) => format!(
                "list-type=2&max-keys=2&prefix=warehouse/&continuation-token={}",
                urlencoding::encode(t)
            ),
            None => "list-type=2&max-keys=2&prefix=warehouse/".to_owned(),
        };
        let url = format!("{base}/{BUCKET}?{query}");
        let response = client
            .get(&url)
            .headers(sign_request("GET", &url, &[]))
            .send()
            .await
            .expect("ListObjectsV2 through server");
        let status = response.status();
        let xml = response.text().await.expect("body");
        assert_eq!(
            status, 200,
            "LIST must be 200 through the server; body: {xml}"
        );
        for key in xml
            .split("<Key>")
            .skip(1)
            .filter_map(|s| s.split("</Key>").next())
        {
            seen.push(key.to_owned());
        }
        match xml
            .split("<NextContinuationToken>")
            .nth(1)
            .and_then(|s| s.split("</NextContinuationToken>").next())
        {
            Some(next) => token = Some(next.to_owned()),
            None => break,
        }
    }
    seen.sort();
    assert_eq!(
        seen,
        vec![
            "warehouse/data/a.parquet",
            "warehouse/data/b.parquet",
            "warehouse/data/c.parquet",
            "warehouse/meta/v1.json",
        ],
        "paginated LIST through server must return every key exactly once"
    );

    // Delimiter listing at the warehouse/ level rolls up the two directories.
    let url = format!("{base}/{BUCKET}?list-type=2&prefix=warehouse/&delimiter=/");
    let response = client
        .get(&url)
        .headers(sign_request("GET", &url, &[]))
        .send()
        .await
        .expect("delimited ListObjectsV2 through server");
    assert_eq!(response.status(), 200);
    let xml = response.text().await.expect("body");
    assert!(
        xml.contains("<Prefix>warehouse/data/</Prefix>"),
        "got: {xml}"
    );
    assert!(
        xml.contains("<Prefix>warehouse/meta/</Prefix>"),
        "got: {xml}"
    );

    stop_server(server);
}
