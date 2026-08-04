//! Server-instance reporting: the privacy invariant and the register+heartbeat
//! wire behavior.
//!
//! The privacy invariant is release-blocking: a server with no [control_plane]
//! config must make NO network call to any control plane. We assert that by
//! constructing the reporter from configs and checking that `from_config`
//! returns None (no reporter ⇒ no task ⇒ no request) unless the machine looks
//! logged in (a [control_plane] URL AND a control-plane token file).
//!
//! The positive path drives the real reporter against a local mock control
//! plane (an axum server on an ephemeral port, exactly the idiom the CLI
//! control-plane tests use) and asserts a register and a heartbeat arrive with
//! the right body — the heartbeat carrying the metrics snapshot.

use std::net::TcpListener;
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{Value, json};

use verglas_core::admin::{CacheConfigInfo, CountersInfo, StatsInfo, WarmingInfo};
use verglas_core::config::Config;
use verglas_server::admin::StatsSlot;
use verglas_server::node_report::{NodeIdentity, NodeReporter};

/// Captured request bodies the mock control plane saw.
#[derive(Clone, Default)]
struct Captured {
    register: Arc<Mutex<Option<Value>>>,
    heartbeat: Arc<Mutex<Option<Value>>>,
}

struct MockControlPlane {
    url: String,
    captured: Captured,
}

async fn register(State(cap): State<Captured>, Json(body): Json<Value>) -> Json<Value> {
    *cap.register.lock().expect("lock") = Some(body.clone());
    // Echo a node-shaped row back (the reporter ignores the body on success).
    Json(json!({ "node_id": body.get("node_id"), "active": true }))
}

async fn heartbeat(State(cap): State<Captured>, Json(body): Json<Value>) -> Json<Value> {
    *cap.heartbeat.lock().expect("lock") = Some(body.clone());
    Json(json!({ "node_id": body.get("node_id"), "active": true }))
}

fn spawn_control_plane() -> MockControlPlane {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.set_nonblocking(true).expect("nonblocking");
    let addr = listener.local_addr().expect("addr");
    let captured = Captured::default();
    let server_captured = captured.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async move {
            let app = Router::new()
                .route("/v1/nodes/register", post(register))
                .route("/v1/nodes/heartbeat", post(heartbeat))
                .with_state(server_captured);
            let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
            axum::serve(listener, app).await.expect("serve");
        });
    });
    MockControlPlane {
        url: format!("http://{addr}"),
        captured,
    }
}

/// A server config from a minimal TOML, with an optional [control_plane] URL.
fn config_with_control_plane(url: Option<&str>) -> Config {
    let dir = std::env::temp_dir().join(format!("verglas-nr-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("mkdir");
    let mut toml = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n\n[backend]\nbucket = \"b\"\nendpoint = \"https://s3\"\nregion = \"us-east-1\"\n",
        dir.display()
    );
    if let Some(u) = url {
        toml.push_str(&format!("\n[control_plane]\nurl = \"{u}\"\n"));
    }
    Config::from_toml_str(&toml).expect("parses")
}

fn sample_identity() -> NodeIdentity {
    NodeIdentity {
        node_id: "node-under-test".to_owned(),
        name: "host-under-test".to_owned(),
        version: "9.9.9".to_owned(),
        os: "linux".to_owned(),
        arch: "x86_64".to_owned(),
    }
}

/// A StatsInfo fixture the heartbeat maps into its metrics snapshot.
fn sample_stats() -> StatsInfo {
    let counters = CountersInfo {
        dram_hits: 700,
        dram_misses: 100,
        disk_hits: 200,
        disk_misses: 100,
        peer_hits: 0,
        peer_misses: 0,
        peer_errors: 0,
        peer_served_blocks: 0,
        peer_served_bytes: 0,
        dram_bytes_served: 1000,
        disk_bytes_served: 2000,
        peer_bytes_served: 0,
        backend_bytes_served: 500,
        backend_fills: 42,
        backend_fill_bytes: 0,
        backend_heads: 0,
        non_cacheable_passthroughs: 0,
        meta_hits: 100,
        meta_misses: 0,
        meta_bytes_served: 50,
        retired_bytes_pending: 0,
        retired_bytes_reclaimed: 0,
        retired_files_reclaimed: 0,
    };
    StatsInfo {
        cache: CacheConfigInfo {
            dir: "/cache".to_owned(),
            capacity_bytes: 20 * 1024 * 1024 * 1024,
            dram_bytes: 1024 * 1024 * 1024,
        },
        counters,
        dram_usage_bytes: 512 * 1024 * 1024,
        dram_live_bytes: 512 * 1024 * 1024,
        dram_reclaimable_bytes: 0,
        warming: Some(WarmingInfo {
            tables_completed: 3,
            ..Default::default()
        }),
        writeback: None,
    }
}

#[test]
fn no_control_plane_config_means_no_reporter_and_no_network() {
    // The release-blocking privacy invariant: without [control_plane], the server
    // never constructs a reporter, so it can never make a control-plane request.
    let config = config_with_control_plane(None);
    assert!(
        NodeReporter::from_config(&config, None).is_none(),
        "a server with no [control_plane] config must not build a reporter"
    );
}

#[test]
fn control_plane_url_without_a_token_file_still_makes_no_reporter() {
    // A machine that has NOT run `verglas login` has a URL but no token file.
    // It must not phone home either. Point HOME at an empty tempdir so no token
    // file exists.
    let home = tempfile::tempdir().expect("tempdir");
    let config = config_with_control_plane(Some("https://api.example.invalid"));
    temp_env_home(home.path(), || {
        assert!(
            NodeReporter::from_config(&config, None).is_none(),
            "a URL without a stored token must not build a reporter"
        );
    });
}

#[test]
fn logged_in_machine_registers_and_heartbeats_with_metrics() {
    let cp = spawn_control_plane();

    // A logged-in machine: a token file under HOME and a [control_plane] URL.
    let home = tempfile::tempdir().expect("tempdir");
    let creds = home.path().join(".verglas/credentials");
    std::fs::create_dir_all(&creds).expect("mkdir creds");
    std::fs::write(creds.join("control-plane-token"), "vgk_test_token\n").expect("write token");

    let stats: StatsSlot = Arc::new(OnceLock::new());
    let fixture = sample_stats();
    let _ = stats.set(Arc::new(move || fixture.clone()));

    let config = config_with_control_plane(Some(&cp.url));

    // Gating: with a [control_plane] URL AND a token file present, from_config
    // must yield a reporter (this is what a logged-in machine looks like).
    let built = temp_env_home(home.path(), || {
        NodeReporter::from_config(&config, Some(stats.clone()))
    });
    assert!(built.is_some(), "a logged-in machine must build a reporter");

    // Drive a reporter with a FIXED identity so the wire-body assertions are
    // deterministic (from_config would stamp this host's real cluster id).
    let reporter = NodeReporter::new(
        &cp.url,
        "vgk_test_token",
        sample_identity(),
        Some(stats.clone()),
    )
    .expect("reporter builds");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        reporter.register().await.expect("register succeeds");
        reporter.heartbeat().await.expect("heartbeat succeeds");
    });

    // The register body carried the node identity.
    let reg = cp
        .captured
        .register
        .lock()
        .expect("lock")
        .clone()
        .expect("a register arrived");
    assert_eq!(
        reg.get("node_id").and_then(Value::as_str),
        Some("node-under-test")
    );
    assert_eq!(
        reg.get("name").and_then(Value::as_str),
        Some("host-under-test")
    );
    assert_eq!(reg.get("version").and_then(Value::as_str), Some("9.9.9"));
    assert_eq!(reg.get("os").and_then(Value::as_str), Some("linux"));
    assert_eq!(reg.get("arch").and_then(Value::as_str), Some("x86_64"));

    // The heartbeat body carried the node id and a metrics snapshot from stats.
    let beat = cp
        .captured
        .heartbeat
        .lock()
        .expect("lock")
        .clone()
        .expect("a heartbeat arrived");
    assert_eq!(
        beat.get("node_id").and_then(Value::as_str),
        Some("node-under-test")
    );
    let metrics = beat.get("metrics").expect("heartbeat carries metrics");
    // 1000 hits (700+200+0+100) over 1200 lookups.
    assert_eq!(
        metrics.get("cache_hits").and_then(Value::as_u64),
        Some(1000)
    );
    assert_eq!(
        metrics.get("cache_misses").and_then(Value::as_u64),
        Some(200)
    );
    assert_eq!(
        metrics.get("nvme_capacity_bytes").and_then(Value::as_u64),
        Some(20 * 1024 * 1024 * 1024)
    );
    assert_eq!(
        metrics.get("dram_used_bytes").and_then(Value::as_u64),
        Some(512 * 1024 * 1024)
    );
    assert_eq!(
        metrics.get("backend_fills").and_then(Value::as_u64),
        Some(42)
    );
    assert_eq!(
        metrics.get("tables_warmed").and_then(Value::as_u64),
        Some(3)
    );
    let hit_rate = metrics
        .get("hit_rate")
        .and_then(Value::as_f64)
        .expect("hit_rate");
    assert!(
        (hit_rate - 1000.0 / 1200.0).abs() < 1e-9,
        "hit_rate = {hit_rate}"
    );
}

/// Runs `f` with HOME set to `home`, restoring the previous value after. The
/// node_report tests read the token file under $HOME; a serialized override keeps
/// them hermetic without touching the developer's real ~/.verglas.
fn temp_env_home<T>(home: &std::path::Path, f: impl FnOnce() -> T) -> T {
    // These HOME-dependent tests run in the same binary; a process-wide mutex
    // serializes the override so they do not race on the env var.
    static LOCK: Mutex<()> = Mutex::new(());
    let _guard = LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let prev = std::env::var_os("HOME");
    // SAFETY: single-threaded within the guard; no other thread reads HOME here.
    unsafe { std::env::set_var("HOME", home) };
    let out = f();
    match prev {
        Some(v) => unsafe { std::env::set_var("HOME", v) },
        None => unsafe { std::env::remove_var("HOME") },
    }
    out
}
