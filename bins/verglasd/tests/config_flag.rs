//! Integration tests for `verglasd --config <path>`: a bad config exits
//! non-zero naming the field, and a good config prints the summary and boots.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Writes a config document to a unique scratch file and returns its path.
fn write_config(tag: &str, contents: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("verglasd-config-{}-{tag}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    let path = dir.join("verglas.toml");
    let mut file = std::fs::File::create(&path).expect("create config file");
    file.write_all(contents.as_bytes())
        .expect("write config file");
    path
}

/// Reserves an ephemeral loopback address, then releases it so a catalog
/// request to that address fails promptly with connection refused.
fn unavailable_loopback_address() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback address");
    listener.local_addr().expect("reserved address")
}

/// Reads daemon stderr until `needle` appears or the bounded wait expires.
/// Returns every line observed so a failed assertion remains actionable.
fn wait_for_stderr_line(
    receiver: &mpsc::Receiver<String>,
    needle: &str,
    timeout: Duration,
) -> Vec<String> {
    let deadline = Instant::now() + timeout;
    let mut lines = Vec::new();
    loop {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return lines;
        };
        match receiver.recv_timeout(remaining) {
            Ok(line) => {
                let found = line.contains(needle);
                lines.push(line);
                if found {
                    return lines;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout | mpsc::RecvTimeoutError::Disconnected) => {
                return lines;
            }
        }
    }
}

#[test]
fn bad_config_exits_nonzero_naming_field() {
    // A zero concurrency ceiling is meaningless (it would deadlock every fill);
    // validation must reject it by name and the daemon must exit non-zero.
    let path = write_config("bad", "[backend]\nmax_concurrent_requests = 0\n");
    let out = Command::new(env!("CARGO_BIN_EXE_verglasd"))
        .arg("--config")
        .arg(&path)
        .output()
        .expect("binary runs");
    assert!(!out.status.success(), "bad config must exit non-zero");
    let stderr = String::from_utf8(out.stderr).expect("utf8");
    assert!(
        stderr.contains("backend.max_concurrent_requests"),
        "stderr should name the field, got: {stderr}"
    );
}

#[test]
fn dram_below_engine_floor_exits_nonzero_at_startup() {
    // `verglas dev --dram` can request a DRAM budget below the engine floor
    // (~80 MiB). The engine validates the budget before any listener binds, so
    // the daemon must exit non-zero naming the dram budget rather than boot
    // half-alive (issue #141).
    let cache_dir = std::env::temp_dir().join(format!("verglasd-floor-{}", std::process::id()));
    std::fs::create_dir_all(&cache_dir).expect("create cache dir");
    let toml_text = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\ndram_bytes = \"1MB\"\n\n[backend]\nbucket = \"test-lake\"\n",
        cache_dir.display()
    );
    let path = write_config("dram-floor", &toml_text);
    let out = Command::new(env!("CARGO_BIN_EXE_verglasd"))
        .arg("--config")
        .arg(&path)
        .env("VERGLAS_ADMIN_ADDR", "127.0.0.1:0")
        .env("VERGLAS_S3_ADDR", "127.0.0.1:0")
        // Reach the dram-floor check: skip the #233 startup probe for a bucket
        // with no reachable origin.
        .env("VERGLAS_DEV_ALLOW_MISSING_ORIGIN", "1")
        .output()
        .expect("binary runs");
    assert!(
        !out.status.success(),
        "a sub-floor dram budget must exit non-zero"
    );
    let stderr = String::from_utf8(out.stderr).expect("utf8");
    assert!(
        stderr.contains("cache.dram_bytes"),
        "stderr should name the dram budget, got: {stderr}"
    );
}

#[test]
fn good_config_prints_summary_and_generated_keys_and_boots() {
    let cache_dir = std::env::temp_dir().join(format!("verglasd-cache-{}", std::process::id()));
    std::fs::create_dir_all(&cache_dir).expect("create cache dir");
    let toml_text = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n\n[backend]\nbucket = \"test-lake\"\n",
        cache_dir.display()
    );
    let path = write_config("good", &toml_text);
    // Port 0 lets the OS pick a free admin port so parallel tests never
    // collide on the config's default admin_port.
    let mut daemon = Command::new(env!("CARGO_BIN_EXE_verglasd"))
        .arg("--config")
        .arg(&path)
        .env("VERGLAS_ADMIN_ADDR", "127.0.0.1:0")
        // The test bucket has no reachable origin; skip the #233 startup probe.
        .env("VERGLAS_DEV_ALLOW_MISSING_ORIGIN", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn verglasd");

    let stdout = daemon.stdout.take().expect("piped stdout");
    let mut reader = BufReader::new(stdout);
    let mut summary = String::new();
    reader.read_line(&mut summary).expect("read summary line");
    let mut keys = String::new();
    reader
        .read_line(&mut keys)
        .expect("read generated-keys line");

    let _ = daemon.kill();
    let _ = daemon.wait();

    assert!(
        summary.contains("config ok:") && summary.contains("backend=test-lake"),
        "expected one-line summary, got: {summary}"
    );
    assert!(
        keys.contains("generated access keys"),
        "expected generated keypair, got: {keys}"
    );
}

#[test]
fn unavailable_catalog_is_reported_to_stderr() {
    // The watcher already records a connectivity failure with `tracing::warn!`.
    // This subprocess proof requires that the daemon make that event visible to
    // an operator, rather than silently warming zero tables (#241).
    let scratch = tempfile::tempdir().expect("create scratch dir");
    let cache_dir = scratch.path().join("cache");
    std::fs::create_dir_all(&cache_dir).expect("create cache dir");
    let catalog_addr = unavailable_loopback_address();
    let toml_text = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\ndram_bytes = \"80MB\"\n\n[backend]\nbucket = \"test-lake\"\n\n[catalog]\nuri = \"http://{catalog_addr}\"\npoll_interval_secs = 1\n",
        cache_dir.display()
    );
    let config_path = scratch.path().join("verglas.toml");
    std::fs::write(&config_path, toml_text).expect("write config");

    let mut daemon = Command::new(env!("CARGO_BIN_EXE_verglasd"))
        .arg("--config")
        .arg(&config_path)
        .env("RUST_LOG", "info")
        .env("VERGLAS_ADMIN_ADDR", "127.0.0.1:0")
        .env("VERGLAS_S3_ADDR", "127.0.0.1:0")
        // The test bucket has no reachable origin; skip the #233 startup probe.
        .env("VERGLAS_DEV_ALLOW_MISSING_ORIGIN", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn verglasd");
    let stderr = daemon.stderr.take().expect("piped stderr");
    let (sender, receiver) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines() {
            let Ok(line) = line else {
                return;
            };
            if sender.send(line).is_err() {
                return;
            }
        }
    });

    let lines = wait_for_stderr_line(
        &receiver,
        "catalog feed seeding poll failed; keeping last-known state",
        Duration::from_secs(10),
    );
    let _ = daemon.kill();
    let _ = daemon.wait();
    reader.join().expect("stderr reader exits");

    assert!(
        lines
            .iter()
            .any(|line| line.contains("catalog feed seeding poll failed; keeping last-known state")),
        "catalog connectivity failure must be visible on stderr, got:\n{}",
        lines.join("\n")
    );
}
