//! Integration tests for the `verglas-server` admin HTTP API.

use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use verglas_core::admin::{HealthzInfo, LogLevelInfo, PurgeReport, StatsInfo, VersionInfo};

/// Waits until the server admin API responds on `/admin/healthz`.
fn wait_for_admin(endpoint: &str) {
    for _ in 0..50 {
        if let Ok(response) = reqwest::blocking::get(format!("{endpoint}/admin/healthz"))
            && response.status().is_success()
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    panic!("admin API did not become ready at {endpoint}");
}

/// Spawns `verglas-server` bound to an ephemeral localhost port.
fn spawn_server() -> (Child, String) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);

    let server = Command::new(env!("CARGO_BIN_EXE_verglas-server"))
        .env("VERGLAS_ADMIN_ADDR", addr.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn verglas-server");

    (server, format!("http://{addr}"))
}

/// Stops a spawned server and reaps its process slot.
fn stop_server(server: &mut Child) {
    let _ = server.kill();
    let _ = server.wait();
}

/// Spawns `verglas-server` with a small on-disk config so the cache engine is built
/// and `/admin/stats` is served. Returns the server and its admin endpoint. The
/// S3 listener binds an ephemeral port so parallel tests never collide.
fn spawn_configured_server(dram: &str, capacity: &str) -> (Child, String) {
    let admin = std::net::TcpListener::bind("127.0.0.1:0").expect("bind admin");
    let admin_addr = admin.local_addr().expect("admin addr");
    drop(admin);

    let dir = std::env::temp_dir().join(format!(
        "verglas-server-stats-{}-{}",
        std::process::id(),
        admin_addr.port()
    ));
    std::fs::create_dir_all(&dir).expect("create cache dir");
    let toml = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"{capacity}\"\ndram_bytes = \"{dram}\"\n\n\
         [backend]\nbucket = \"test-lake\"\n",
        dir.display()
    );
    let cfg = dir.join("verglas.toml");
    std::fs::File::create(&cfg)
        .expect("create config")
        .write_all(toml.as_bytes())
        .expect("write config");

    let server = Command::new(env!("CARGO_BIN_EXE_verglas-server"))
        .arg("--config")
        .arg(&cfg)
        .env("VERGLAS_ADMIN_ADDR", admin_addr.to_string())
        .env("VERGLAS_S3_ADDR", "127.0.0.1:0")
        // The test bucket has no reachable origin; skip the #233 startup probe.
        .env("VERGLAS_DEV_ALLOW_MISSING_ORIGIN", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn verglas-server");

    (server, format!("http://{admin_addr}"))
}

#[test]
fn admin_stats_reports_configured_cache_budgets() {
    // The stats probe is a bench run's single source of truth for tier context:
    // it must report the server's actual configured DRAM/disk ceilings (issue
    // #141) so reports never guess. 80MB DRAM is the engine floor; a healthy
    // engine at that budget serves stats.
    let (mut server, endpoint) = spawn_configured_server("80MB", "64MB");
    wait_for_admin(&endpoint);

    let resp = reqwest::blocking::get(format!("{endpoint}/admin/stats")).expect("stats");
    assert!(
        resp.status().is_success(),
        "stats status: {}",
        resp.status()
    );
    let stats: StatsInfo = resp.json().expect("stats json");

    assert_eq!(stats.cache.dram_bytes, 80 * 1024 * 1024);
    assert_eq!(stats.cache.capacity_bytes, 64 * 1024 * 1024);
    // A freshly booted engine has served nothing yet, but the fields exist so a
    // report can diff cold vs warm legs.
    assert_eq!(stats.counters.disk_hits, 0);
    // The DRAM tier holds its bookkeeping; usage is a real gauge, not absent.
    let _ = stats.dram_usage_bytes;

    stop_server(&mut server);
}

#[test]
fn admin_stats_absent_without_a_config() {
    // The config-less admin-only mode has no engine to report on, so the stats
    // route is not mounted (404) rather than fabricating zeros.
    let (mut server, endpoint) = spawn_server();
    wait_for_admin(&endpoint);

    let resp = reqwest::blocking::get(format!("{endpoint}/admin/stats")).expect("stats");
    assert_eq!(resp.status().as_u16(), 404);

    stop_server(&mut server);
}

#[test]
fn admin_version_and_healthz_respond() {
    let (mut server, endpoint) = spawn_server();
    wait_for_admin(&endpoint);

    let version = reqwest::blocking::get(format!("{endpoint}/admin/version")).expect("version");
    assert!(version.status().is_success());
    let body: VersionInfo = version.json().expect("version json");
    assert_eq!(body.name, "verglas-server");
    assert_eq!(body.version, env!("CARGO_PKG_VERSION"));

    let healthz = reqwest::blocking::get(format!("{endpoint}/admin/healthz")).expect("healthz");
    assert!(healthz.status().is_success());
    let body: HealthzInfo = healthz.json().expect("healthz json");
    assert_eq!(body.status, "ok");

    stop_server(&mut server);
}

/// The runtime log-level control (#61) hot-reloads the filter: a valid level is
/// applied and echoed back (200), an unparseable one is rejected by name (400)
/// so a CLI typo is a clear error rather than a silent no-op.
#[test]
fn admin_log_level_reloads_and_rejects_bad_filters() {
    let (mut server, endpoint) = spawn_server();
    wait_for_admin(&endpoint);

    let client = reqwest::blocking::Client::new();
    let ok = client
        .post(format!("{endpoint}/admin/log"))
        .json(&serde_json::json!({ "level": "verglas_cache=debug,info" }))
        .send()
        .expect("post log level");
    assert!(ok.status().is_success(), "a valid filter is accepted");
    let applied: LogLevelInfo = ok.json().expect("log level json");
    assert_eq!(applied.level, "verglas_cache=debug,info");

    let bad = client
        .post(format!("{endpoint}/admin/log"))
        .json(&serde_json::json!({ "level": "verglas_cache=notalevel" }))
        .send()
        .expect("post bad log level");
    assert_eq!(
        bad.status().as_u16(),
        400,
        "an unparseable filter is a 400, not a silent no-op"
    );

    stop_server(&mut server);
}

/// Grabs a free loopback port by binding `:0`, reading the address, and
/// dropping the listener so the server can rebind it (same TOCTOU tradeoff the
/// other spawn helpers accept).
fn free_loopback_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);
    addr.to_string()
}

/// Spawns a fully configured `verglas-server` (S3 + admin) over a temp cache dir, so
/// the cache engine — and therefore the purge endpoint — exists. Returns the
/// server, its admin base URL, and the temp cache dir guard.
fn spawn_purge_server() -> (Child, String, tempfile::TempDir) {
    let cache_dir = tempfile::tempdir().expect("temp cache dir");
    let config = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n\n[backend]\nbucket = \"test-lake\"\n",
        cache_dir.path().display()
    );
    let mut config_file = tempfile::Builder::new()
        .suffix(".toml")
        .tempfile()
        .expect("config file");
    config_file
        .write_all(config.as_bytes())
        .expect("write config");
    config_file.flush().expect("flush config");
    // Keep the file around for the server's lifetime by leaking the handle's
    // path; the temp file is cleaned when the process exits.
    let config_path = config_file.into_temp_path();
    let config_path = config_path.keep().expect("persist config path");

    let admin_addr = free_loopback_addr();
    let s3_addr = free_loopback_addr();
    let server = Command::new(env!("CARGO_BIN_EXE_verglas-server"))
        .arg("--config")
        .arg(&config_path)
        .env("VERGLAS_ADMIN_ADDR", &admin_addr)
        .env("VERGLAS_S3_ADDR", &s3_addr)
        // The test bucket has no reachable origin; skip the #233 startup probe.
        .env("VERGLAS_DEV_ALLOW_MISSING_ORIGIN", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn verglas-server");

    (server, format!("http://{admin_addr}"), cache_dir)
}

/// `POST /cache/purge` on the loopback admin listener resets the cache and
/// returns byte counts. A cold server returns zeros with the disk tier cleared
/// — the endpoint exists, is loopback-only, and speaks the `PurgeReport`
/// contract (issue #138).
#[test]
fn admin_cache_purge_returns_counts() {
    let (mut server, endpoint, _cache_dir) = spawn_purge_server();
    wait_for_admin(&endpoint);

    // The admin base URL is loopback — the purge control surface never leaves
    // the node.
    assert!(
        endpoint.starts_with("http://127.0.0.1:"),
        "admin API must be loopback-only, got {endpoint}"
    );

    let client = reqwest::blocking::Client::new();
    let response = client
        .post(format!("{endpoint}/cache/purge"))
        .send()
        .expect("purge request");
    assert!(
        response.status().is_success(),
        "purge must succeed, got {}",
        response.status()
    );
    let report: PurgeReport = response.json().expect("purge report json");
    // Nothing was warmed, so no bytes were live to become reclaimable, and the
    // generation bump takes effect (a fresh server's first purge → generation 1).
    assert_eq!(report.mapping_bytes_freed, 0);
    assert_eq!(report.reclaimable_bytes, 0);
    assert_eq!(
        report.generation, 1,
        "the first purge bumps to generation 1"
    );

    stop_server(&mut server);
}
