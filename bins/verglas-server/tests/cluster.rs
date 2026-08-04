//! Integration tests for the server's cluster wiring (#27/#28).
//!
//! Three checks map to the issues' acceptance criteria:
//! - a key the ring routes to a *remote* node still backend-fills locally and
//!   returns the correct bytes (peer FETCH is #29; until then a remote owner is
//!   never an error, just a local fill — slow is acceptable, wrong is never);
//! - a single-node server (no `[cluster]`) serves no membership route — the
//!   turn-off path is unchanged;
//! - a 3-server localhost pod with a shared seed converges so every server's
//!   `GET /admin/members` reports the same 3-member view.

use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStoreExt, PutPayload};

use verglas_cache::HybridCacheEngine;
use verglas_cluster::{LiveRing, WeightedMember};
use verglas_core::CacheKey;
use verglas_core::admin::MembersInfo;
use verglas_core::config::{ByteSize, Cache as CacheConfig};
use verglas_core::node::NodeId;
use verglas_core::peer::NoopPeerFetch;
use verglas_core::ring::Ring;
use verglas_s3::{BackendStore, ObjectRead, PassthroughRead, ReadRange};

/// A deterministic payload: byte `i` is `(i + seed) % 251`.
fn pattern(seed: u64, len: usize) -> Bytes {
    (0..len)
        .map(|i| ((i as u64 + seed) % 251) as u8)
        .collect::<Vec<u8>>()
        .into()
}

/// The standing invariant, exercised through the *real* live ring: a key the
/// ring assigns to a remote node, with the no-op peer transport (#29 not yet
/// built), degrades to a correct local backend fill — never an error, never
/// wrong bytes, and the peer counter stays zero.
#[tokio::test(flavor = "multi_thread")]
async fn remote_owned_key_backend_fills_locally_never_errors() {
    let local = NodeId::new("local");
    let remote = NodeId::new("remote");
    // A two-member live ring; some keys land on `remote`.
    let ring = LiveRing::new(vec![
        WeightedMember::new(local.clone(), 1),
        WeightedMember::new(remote.clone(), 1),
    ])
    .expect("non-empty ring");

    // Find a key the ring routes to the remote node — the interesting case.
    let name = (0..10_000)
        .map(|i| format!("obj-{i}.bin"))
        .find(|n| {
            ring.owner(&CacheKey {
                bucket: "bkt".to_owned(),
                key: n.clone(),
            }) == remote
        })
        .expect("some key is remote-owned");
    let key = CacheKey {
        bucket: "bkt".to_owned(),
        key: name.clone(),
    };

    let body = pattern(7, 3 * 1024 * 1024);
    let store = std::sync::Arc::new(InMemory::new());
    store
        .put(
            &ObjPath::from(name.as_str()),
            PutPayload::from(body.clone()),
        )
        .await
        .expect("seed object");

    let dir = tempfile::tempdir().expect("temp dir");
    let cache = CacheConfig {
        dir: dir.path().to_path_buf(),
        capacity_bytes: ByteSize(64 * 1024 * 1024),
        dram_bytes: ByteSize(128 * 1024 * 1024),
        admission: Default::default(),
        ..Default::default()
    };
    let engine = HybridCacheEngine::new(
        PassthroughRead::new(BackendStore::single("bkt", store)),
        NoopPeerFetch,
        ring,
        local,
        &cache,
    )
    .await
    .expect("build engine");

    // Read the remote-owned key: it must succeed with the exact origin bytes.
    let got = engine.get(&key, ReadRange::Full).await.expect("read ok");
    let mut buf = BytesMut::new();
    let mut stream = got.body;
    while let Some(chunk) = stream.try_next().await.expect("stream chunk") {
        buf.extend_from_slice(&chunk);
    }
    assert_eq!(buf.freeze(), body, "remote-owned key served wrong bytes");

    let counters = engine.counters().snapshot();
    assert!(
        counters.backend_fills > 0,
        "remote-owned key must fill from the backend locally"
    );
    assert_eq!(
        counters.peer_hits, 0,
        "no peer transport exists yet (#29); peer hits must be zero"
    );
}

// --- subprocess helpers ------------------------------------------------------

/// Reserves a free localhost port number (binds then drops), reused for the
/// server's gossip/admin sockets. A small race window remains, acceptable for
/// tests.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);
    port
}

/// A spawned server plus its admin endpoint and scratch dir (kept alive so it
/// is not deleted while the server runs).
struct Server {
    child: Child,
    endpoint: String,
    _dir: tempfile::TempDir,
}

impl Drop for Server {
    /// Kills and reaps the server when the handle drops.
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Writes a `[cluster]` config and spawns `verglas-server` for one pod node.
fn spawn_cluster_server(node_id: &str, gossip_port: u16, seed_port: u16) -> Server {
    let admin_addr = format!("127.0.0.1:{}", free_port());
    let dir = tempfile::tempdir().expect("temp dir");
    let toml = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\ndram_bytes = \"80MB\"\n\n\
         [backend]\nbucket = \"bkt\"\n\n\
         [cluster]\npod_id = \"itest-pod\"\nnode_id = \"{node_id}\"\n\
         gossip_addr = \"127.0.0.1:{gossip_port}\"\nseeds = [\"127.0.0.1:{seed_port}\"]\n\
         suspicion_secs = 2\n",
        dir.path().display()
    );
    let cfg = dir.path().join("verglas.toml");
    std::fs::File::create(&cfg)
        .expect("create config")
        .write_all(toml.as_bytes())
        .expect("write config");

    let child = Command::new(env!("CARGO_BIN_EXE_verglas-server"))
        .arg("--config")
        .arg(&cfg)
        .env("VERGLAS_ADMIN_ADDR", &admin_addr)
        .env("VERGLAS_S3_ADDR", "127.0.0.1:0")
        // The test bucket has no reachable origin; skip the #233 startup probe.
        .env("VERGLAS_DEV_ALLOW_MISSING_ORIGIN", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn verglas-server");

    Server {
        child,
        endpoint: format!("http://{admin_addr}"),
        _dir: dir,
    }
}

/// Fetches `GET /admin/members` from a server, returning `None` until it is up.
fn fetch_members(endpoint: &str) -> Option<MembersInfo> {
    reqwest::blocking::get(format!("{endpoint}/admin/members"))
        .ok()
        .filter(|r| r.status().is_success())
        .and_then(|r| r.json::<MembersInfo>().ok())
}

/// A single-node server (no `[cluster]`) serves no membership route — the
/// turn-off path: nothing about serving depends on cluster state.
#[test]
fn single_node_server_omits_the_members_route() {
    let admin_addr = format!("127.0.0.1:{}", free_port());
    let mut child = Command::new(env!("CARGO_BIN_EXE_verglas-server"))
        .env("VERGLAS_ADMIN_ADDR", &admin_addr)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn verglas-server");
    let endpoint = format!("http://{admin_addr}");

    // Wait for the admin API, then confirm /admin/members is 404 while healthz
    // is 200 (the server is up; it simply has no membership to report).
    let mut healthy = false;
    for _ in 0..50 {
        if let Ok(r) = reqwest::blocking::get(format!("{endpoint}/admin/healthz"))
            && r.status().is_success()
        {
            healthy = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(healthy, "single-node admin API never came up");

    let status = reqwest::blocking::get(format!("{endpoint}/admin/members"))
        .expect("request")
        .status();
    assert_eq!(
        status.as_u16(),
        404,
        "single-node server must have no members route"
    );

    let _ = child.kill();
    let _ = child.wait();
}

/// Three servers on localhost sharing one seed converge so every server's
/// `/admin/members` reports the same 3-member view.
#[test]
fn three_server_pod_converges_on_identical_members() {
    let seed_port = free_port();
    let gb = free_port();
    let gc = free_port();

    let a = spawn_cluster_server("node-a", seed_port, seed_port);
    let b = spawn_cluster_server("node-b", gb, seed_port);
    let c = spawn_cluster_server("node-c", gc, seed_port);
    let servers = [&a, &b, &c];

    // Poll until all three report exactly three members (or the deadline).
    let start = std::time::Instant::now();
    let mut converged = false;
    while start.elapsed() < Duration::from_secs(30) {
        if servers
            .iter()
            .all(|d| fetch_members(&d.endpoint).is_some_and(|m| m.members.len() == 3))
        {
            converged = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    assert!(
        converged,
        "the pod did not converge on 3 members within 30s"
    );

    // Every server sees the same three node ids and the same pod.
    let sorted_ids = |m: &MembersInfo| {
        let mut v: Vec<String> = m.members.iter().map(|x| x.node_id.clone()).collect();
        v.sort();
        v
    };
    let view_a = fetch_members(&a.endpoint).expect("a members");
    let view_b = fetch_members(&b.endpoint).expect("b members");
    let view_c = fetch_members(&c.endpoint).expect("c members");

    let expected = vec![
        "node-a".to_owned(),
        "node-b".to_owned(),
        "node-c".to_owned(),
    ];
    assert_eq!(sorted_ids(&view_a), expected);
    assert_eq!(sorted_ids(&view_a), sorted_ids(&view_b));
    assert_eq!(sorted_ids(&view_b), sorted_ids(&view_c));
    assert_eq!(view_a.pod_id, "itest-pod");

    // Published metadata is present: every member advertises its capacity
    // (its ring weight) and an active state.
    for member in &view_a.members {
        assert_eq!(member.capacity_bytes, 64 * 1024 * 1024);
        assert_eq!(member.state, "active");
    }

    // Handles drop here, killing the servers.
    let _ = (a, b, c);
}
