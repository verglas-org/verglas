//! The hosted Iceberg catalog served from inside a ring node.
//!
//! The cloud topology pins one stateless catalog to every ring node, so the
//! catalog runs in this process and reaches consensus through an in-process
//! transport rather than issuing HTTP requests to the node's own ingress.
//! This exercises that whole path end to end: a real Iceberg REST request,
//! authorized by a real ES256 credential, resolved through the node's own
//! consensus plane.
//!
//! Hermetic by construction. The caller credential is minted here against a
//! generated P-256 key, so no control plane and no network are involved,
//! and namespace operations are pure catalog records — they never reach
//! object storage.

use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine as _;

/// Kills the node on drop so a failing assertion cannot leak the child.
struct NodeGuard(Child);

impl Drop for NodeGuard {
    /// Terminates and reaps the spawned node.
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// One generated signing key plus the JWKS a verifier needs for it.
struct CallerKey {
    /// PKCS#8 PEM used to sign credentials.
    signing_pem: String,
    /// Single-key JWKS in the form the authorizer parses.
    jwks: String,
    /// Key id carried in every minted credential's header.
    kid: String,
}

/// Generates a P-256 key and publishes it as a one-key JWKS.
///
/// The authorizer pins ES256, so the fixture must produce a real EC key
/// rather than a symmetric one. `public_key_raw` is the SEC1 uncompressed
/// point (`0x04 || x || y`), which is exactly the two coordinates a JWK
/// carries.
fn caller_key() -> CallerKey {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("generate P-256");
    let raw = key.public_key_raw();
    assert_eq!(raw.len(), 65, "uncompressed SEC1 point");
    let encoder = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let kid = "verglas-test-key".to_owned();
    let jwks = serde_json::json!({
        "keys": [{
            "kty": "EC",
            "crv": "P-256",
            "alg": "ES256",
            "use": "sig",
            "kid": kid,
            "x": encoder.encode(&raw[1..33]),
            "y": encoder.encode(&raw[33..65]),
        }]
    })
    .to_string();
    CallerKey {
        signing_pem: key.serialize_pem(),
        jwks,
        kid,
    }
}

/// Mints one caller credential granting every action on every resource.
///
/// `resource: "/*"` with `action: "admin"` is the broadest grant the scope
/// matcher accepts; the subject under test is the catalog path, not the
/// permission algebra.
fn caller_token(key: &CallerKey, issuer: &str, tenant: &str) -> String {
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs()
        + 3600;
    let claims = serde_json::json!({
        "iss": issuer,
        "aud": "catalog",
        "sub": "verglas-test-principal",
        "jti": "verglas-test-token",
        "exp": expiry,
        "tenant_id": tenant,
        "scope": [{ "resource": "/*", "action": "admin" }],
    });
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::ES256);
    header.kid = Some(key.kid.clone());
    jsonwebtoken::encode(
        &header,
        &claims,
        &jsonwebtoken::EncodingKey::from_ec_pem(key.signing_pem.as_bytes())
            .expect("EC signing key"),
    )
    .expect("mint caller credential")
}

/// Reserves one free localhost port.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("reserve port")
        .local_addr()
        .expect("reserved address")
        .port()
}

/// A four-node ring whose first node also serves the hosted catalog.
struct EmbeddedCatalog {
    _nodes: Vec<NodeGuard>,
    _root: tempfile::TempDir,
    catalog_port: u16,
    stderr: Arc<Mutex<Vec<String>>>,
}

/// Starts the production-sized ring and mounts the hosted catalog inside its
/// first node. Four nodes at `k=2/m=2/w=3` match the safekeeper tests.
fn start(key: &CallerKey, issuer: &str, tenant: &str, warehouse: &str) -> EmbeddedCatalog {
    start_with_nodes(key, issuer, tenant, warehouse, 4)
}

/// Starts a ring with an explicit voter count for topology-specific catalog tests.
fn start_with_nodes(
    key: &CallerKey,
    issuer: &str,
    tenant: &str,
    warehouse: &str,
    node_count: usize,
) -> EmbeddedCatalog {
    let root = tempfile::TempDir::new().expect("test root");
    let ring_ports: Vec<u16> = (0..node_count).map(|_| free_port()).collect();
    let peers = ring_ports
        .iter()
        .enumerate()
        .map(|(index, port)| format!("node-{index}=127.0.0.1:{port}"))
        .collect::<Vec<_>>()
        .join(",");
    let catalog_port = free_port();
    let stderr = Arc::new(Mutex::new(Vec::new()));
    let mut nodes = Vec::with_capacity(node_count);

    for (index, ring_port) in ring_ports.iter().enumerate() {
        let node = root.path().join(format!("node-{index}"));
        std::fs::create_dir_all(&node).expect("node dir");
        let credentials = node.join("credentials");
        std::fs::write(
            &credentials,
            "[default]\naws_access_key_id = test\naws_secret_access_key = testsecret\n",
        )
        .expect("credentials");

        let (s3_port, admin_port) = (free_port(), free_port());
        // Every process must declare the mode it runs. Each node gets its own
        // catalog listener, while the test client exercises the first one.
        let node_catalog_port = if index == 0 {
            catalog_port
        } else {
            free_port()
        };
        let profile = serde_json::json!({
            "bucket": "catalog-test",
            "region": "us-east-1",
            "endpoint": format!("http://127.0.0.1:{s3_port}"),
            "path-style-access": true,
            "sts-enabled": false,
        })
        .to_string();
        let catalog_section = format!(
            "[catalog_server]\nport = {node_catalog_port}\ntenant = \"{tenant}\"\n\
             warehouse = \"{warehouse}\"\nmanaged_s3_profile = {profile}\n\
             authz_issuer = \"{issuer}\"\nauthz_jwks = {jwks}\n\
             authz_tenant_id = \"{tenant}\"\n\n",
            profile = serde_json::Value::String(profile),
            jwks = serde_json::Value::String(key.jwks.clone()),
        );

        let config = node.join("config.toml");
        std::fs::write(
            &config,
            format!(
                "[listen]\ns3_port = {s3_port}\nadmin_port = {admin_port}\n\n\
                 [cache]\ndir = \"{node_dir}\"\ncapacity_bytes = \"64MB\"\ndram_bytes = \"80MB\"\n\n\
                 {catalog_section}\
                 [backend]\nbucket = \"wal-test\"\nbucket_globs = [\"catalog-test\"]\n\
                 endpoint = \"http://127.0.0.1:9\"\nallow_http = true\nregion = \"us-east-1\"\n\
                 credentials_file = \"{creds}\"\n\n\
                 [catalog_archive]\nbucket = \"catalog-test\"\nprefix = \"_verglas/test-catalog\"\n\n\
                 [auth]\ncredentials_file = \"{creds}\"\n",
                node_dir = node.display(),
                creds = credentials.display(),
            ),
        )
        .expect("config");

        let mut child = Command::new(env!("CARGO_BIN_EXE_verglas-cache-node"))
            .arg("--config")
            .arg(&config)
            .env("VERGLAS_DEV_ALLOW_MISSING_ORIGIN", "1")
            .env("VERGLAS_CATALOG", "on")
            .env("VERGLAS_NODE_ID", format!("node-{index}"))
            .env("VERGLAS_RING_PEERS", &peers)
            .env("VERGLAS_SAFEKEEPER_EC_K", "2")
            .env("VERGLAS_SAFEKEEPER_EC_M", "2")
            .env("VERGLAS_SAFEKEEPER_EC_W", "3")
            .env("VERGLAS_RING_ADDR", format!("127.0.0.1:{ring_port}"))
            .env("VERGLAS_BLOCK_ADDR", format!("127.0.0.1:{}", free_port()))
            .env(
                "VERGLAS_SAFEKEEPER_ADDR",
                format!("127.0.0.1:{}", free_port()),
            )
            // The catalog signs its own metadata IO with these; no request in
            // this test reaches object storage, but the deployment requires
            // them.
            .env("AWS_ACCESS_KEY_ID", "test")
            .env("AWS_SECRET_ACCESS_KEY", "testsecret")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn cache node");
        let pipe = child.stderr.take().expect("piped stderr");
        let sink = Arc::clone(&stderr);
        thread::spawn(move || {
            for line in BufReader::new(pipe).lines().map_while(Result::ok) {
                if let Ok(mut lines) = sink.lock() {
                    lines.push(format!("node-{index}: {line}"));
                }
            }
        });
        nodes.push(NodeGuard(child));
    }

    EmbeddedCatalog {
        _nodes: nodes,
        _root: root,
        catalog_port,
        stderr,
    }
}

impl EmbeddedCatalog {
    /// Returns the node's captured diagnostics.
    fn diagnostics(&self) -> String {
        self.stderr
            .lock()
            .map(|lines| lines.join("\n"))
            .unwrap_or_default()
    }

    /// Waits until the catalog port answers, or reports what the node logged.
    async fn wait_ready(&self, client: &reqwest::Client) {
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            if client
                .get(format!("http://127.0.0.1:{}/health", self.catalog_port))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!(
            "hosted catalog never became ready.\nnode diagnostics:\n{}",
            self.diagnostics()
        );
    }
}

/// A ring node must serve real Iceberg REST traffic from its own process.
///
/// This is the co-located packaging end to end: the request is authorized by
/// a genuine ES256 credential, routed through the embedded catalog, and
/// resolved by the node's own consensus plane over the in-process transport —
/// no second process and no HTTP hop back into this node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ring_node_serves_its_own_iceberg_catalog() {
    const ISSUER: &str = "https://verglas.test/issuer";
    const TENANT: &str = "tenant-local";
    const WAREHOUSE: &str = "lite";

    let key = caller_key();
    let node = start(&key, ISSUER, TENANT, WAREHOUSE);
    let token = caller_token(&key, ISSUER, TENANT);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("http client");
    node.wait_ready(&client).await;

    let base = format!("http://127.0.0.1:{}/catalog/v1", node.catalog_port);

    // Creating and listing a namespace is a pure catalog record: it proves the
    // consensus round trip without depending on object storage.
    let created = client
        .post(format!("{base}/{WAREHOUSE}/namespaces"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "namespace": ["bench"], "properties": {} }))
        .send()
        .await
        .expect("create namespace request");
    assert!(
        created.status().is_success(),
        "namespace create failed with {}: {}\nnode diagnostics:\n{}",
        created.status(),
        created.text().await.unwrap_or_default(),
        node.diagnostics()
    );

    let listed = client
        .get(format!("{base}/{WAREHOUSE}/namespaces"))
        .bearer_auth(&token)
        .send()
        .await
        .expect("list namespaces request");
    assert!(
        listed.status().is_success(),
        "namespace list failed with {}: {}",
        listed.status(),
        listed.text().await.unwrap_or_default()
    );
    let body: serde_json::Value = listed.json().await.expect("namespace list body");
    let namespaces = body["namespaces"]
        .as_array()
        .expect("namespaces array")
        .iter()
        .filter_map(|entry| entry.as_array())
        .filter_map(|parts| parts.first().and_then(serde_json::Value::as_str))
        .collect::<Vec<_>>();
    assert!(
        namespaces.contains(&"bench"),
        "the committed namespace must be readable back through consensus, got {namespaces:?}"
    );
}

/// A one-voter hosted catalog keeps catalog consensus independent from Neon
/// safekeeper startup. The consensus plane creates its payload store lazily, so
/// inline catalog records do not require distributed erasure-coding geometry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_solo_node_serves_its_catalog_through_one_voter_consensus() {
    const ISSUER: &str = "https://verglas.test/issuer";
    const TENANT: &str = "tenant-solo";
    const WAREHOUSE: &str = "solo";

    let key = caller_key();
    let node = start_with_nodes(&key, ISSUER, TENANT, WAREHOUSE, 1);
    let token = caller_token(&key, ISSUER, TENANT);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("http client");
    node.wait_ready(&client).await;

    let response = client
        .post(format!(
            "http://127.0.0.1:{}/catalog/v1/{WAREHOUSE}/namespaces",
            node.catalog_port
        ))
        .bearer_auth(token)
        .json(&serde_json::json!({ "namespace": ["solo"], "properties": {} }))
        .send()
        .await
        .expect("solo namespace request");
    assert!(
        response.status().is_success(),
        "one-voter catalog request failed with {}: {}\nnode diagnostics:\n{}",
        response.status(),
        response.text().await.unwrap_or_default(),
        node.diagnostics()
    );
}

/// An unsigned caller must not reach the embedded catalog.
///
/// The catalog is mounted inside the ring node, so a missing bearer must fail
/// at the same authorization boundary a standalone deployment enforces rather
/// than inheriting the node's own trust.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_embedded_catalog_still_refuses_an_unauthenticated_caller() {
    const ISSUER: &str = "https://verglas.test/issuer";
    const TENANT: &str = "tenant-local";
    const WAREHOUSE: &str = "lite";

    let key = caller_key();
    let node = start(&key, ISSUER, TENANT, WAREHOUSE);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("http client");
    node.wait_ready(&client).await;

    let response = client
        .get(format!(
            "http://127.0.0.1:{}/catalog/v1/{WAREHOUSE}/namespaces",
            node.catalog_port
        ))
        .send()
        .await
        .expect("unauthenticated request");
    assert!(
        !response.status().is_success(),
        "an unauthenticated caller must be refused, got {}",
        response.status()
    );
}

/// The node mounts the hosted Iceberg surface and nothing else.
///
/// `new_v1_hosted_router` carries config, namespaces, tables, and views. It
/// deliberately omits Catalog's management API — warehouses, projects, roles,
/// users, and permissions — because this deployment serves one warehouse from
/// config and delegates authorization to an external decision service. This
/// pins that boundary so the documented surface cannot drift from the served
/// one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_node_serves_the_hosted_iceberg_surface_and_no_management_api() {
    const ISSUER: &str = "https://verglas.test/issuer";
    const TENANT: &str = "tenant-local";
    const WAREHOUSE: &str = "lite";

    let key = caller_key();
    let node = start(&key, ISSUER, TENANT, WAREHOUSE);
    let token = caller_token(&key, ISSUER, TENANT);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("http client");
    node.wait_ready(&client).await;
    let root = format!("http://127.0.0.1:{}", node.catalog_port);

    // An authorized management call must not reach a handler. Anything
    // successful means the surface is mounted after all.
    for path in [
        "/management/v1/warehouse",
        "/management/v1/project",
        "/management/v1/role",
        "/management/v1/user",
        "/management/v1/permissions",
        "/management/v1/info",
        "/management/v1/whoami",
    ] {
        let response = client
            .get(format!("{root}{path}"))
            .bearer_auth(&token)
            .send()
            .await
            .expect("management probe");
        assert!(
            !response.status().is_success(),
            "{path} answered {} — the management API must not be mounted",
            response.status()
        );
    }

    // The Iceberg surface this deployment does serve.
    let config = client
        .get(format!("{root}/catalog/v1/config?warehouse={WAREHOUSE}"))
        .bearer_auth(&token)
        .send()
        .await
        .expect("config request");
    assert!(
        config.status().is_success(),
        "the Iceberg config route must be served, got {}:\n{}",
        config.status(),
        node.diagnostics()
    );

    let namespaces = client
        .get(format!("{root}/catalog/v1/{WAREHOUSE}/namespaces"))
        .bearer_auth(&token)
        .send()
        .await
        .expect("namespaces request");
    assert!(
        namespaces.status().is_success(),
        "the Iceberg namespaces route must be served, got {}",
        namespaces.status()
    );
}

/// SQL is served by the catalog, under the same version prefix and behind the
/// same bearer.
///
/// It reads customer table data, so it must not answer a caller without the
/// credential the catalog already verifies. Mounting it into the catalog's own
/// router is what gives it that check; this proves the wiring.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_is_served_beside_the_catalog_and_requires_its_bearer() {
    const ISSUER: &str = "https://verglas.test/issuer";
    const TENANT: &str = "tenant-local";
    const WAREHOUSE: &str = "lite";

    let key = caller_key();
    let node = start(&key, ISSUER, TENANT, WAREHOUSE);
    let token = caller_token(&key, ISSUER, TENANT);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("http client");
    node.wait_ready(&client).await;
    let query = format!("http://127.0.0.1:{}/catalog/v1/query", node.catalog_port);

    let anonymous = client
        .post(&query)
        .json(&serde_json::json!({ "sql": "SELECT 1" }))
        .send()
        .await
        .expect("unauthenticated query");
    assert!(
        !anonymous.status().is_success(),
        "SQL must not serve an unauthenticated caller, got {}",
        anonymous.status()
    );

    let authorized = client
        .post(&query)
        .bearer_auth(&token)
        .json(&serde_json::json!({ "sql": "SELECT 1 AS n" }))
        .send()
        .await
        .expect("authorized query");
    assert!(
        authorized.status().is_success(),
        "an authorized SQL request must be served, got {}:\n{}",
        authorized.status(),
        node.diagnostics()
    );
    let body: serde_json::Value = authorized.json().await.expect("query result");
    assert_eq!(body["rows"], 1, "one row expected, got {body}");
    assert_eq!(body["data"][0]["n"], 1);
}
