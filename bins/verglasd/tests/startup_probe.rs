//! End-to-end proof of the #233 startup probe: a daemon configured with a real
//! backend bucket it cannot reach must exit non-zero with a clear error, not
//! come up healthy over an empty store. The mirror case — the explicit dev/test
//! opt-out — is covered by the other daemon integration tests, which point at an
//! unreachable placeholder bucket and set `VERGLAS_DEV_ALLOW_MISSING_ORIGIN`.

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Reserves an ephemeral port and immediately drops the listener, so a request
/// to it is refused at once — a deterministic "origin unreachable".
fn refused_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);
    port
}

/// A daemon pointed at a bucket whose origin refuses every connection must fail
/// the startup probe: it exits non-zero and names the bucket and the credential
/// chain it tried, rather than serving an empty store.
#[test]
fn unreachable_backend_fails_startup_with_a_clear_error() {
    let scratch = std::env::temp_dir().join(format!(
        "verglasd-probe-itest-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let cache_dir = scratch.join("cache");
    std::fs::create_dir_all(&cache_dir).expect("create cache dir");
    let backend_creds = scratch.join("backend-credentials");
    std::fs::write(
        &backend_creds,
        "[default]\naws_access_key_id = AK\naws_secret_access_key = secret\n",
    )
    .expect("write backend creds");

    // A closed loopback port: the probe HEAD is refused immediately. Retries are
    // disabled so the probe fails fast rather than spending its backoff budget.
    let endpoint = format!("http://127.0.0.1:{}", refused_port());
    let config_path = scratch.join("verglas.toml");
    std::fs::write(
        &config_path,
        format!(
            "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n\n\
             [backend]\nbucket = \"lake\"\nendpoint = \"{endpoint}\"\nregion = \"us-east-1\"\n\
             allow_http = true\ncredentials_file = \"{}\"\n\n\
             [backend.retry]\nmax_retries = 0\ninitial_backoff_ms = 1\nmax_backoff_ms = 1\nbudget_ms = 50\n",
            cache_dir.display(),
            backend_creds.display()
        ),
    )
    .expect("write config");

    let mut daemon = Command::new(env!("CARGO_BIN_EXE_verglasd"))
        .arg("--config")
        .arg(&config_path)
        .env("VERGLAS_ADMIN_ADDR", "127.0.0.1:0")
        .env("VERGLAS_S3_ADDR", "127.0.0.1:0")
        // No VERGLAS_DEV_ALLOW_MISSING_ORIGIN: the probe must run and fail.
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn verglasd");

    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = daemon.try_wait().expect("try_wait") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = daemon.kill();
            panic!("daemon did not exit; it must fail startup on an unreachable backend");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(
        !status.success(),
        "an unreachable configured backend must exit non-zero"
    );

    let mut stderr = String::new();
    daemon
        .stderr
        .take()
        .expect("piped stderr")
        .read_to_string(&mut stderr)
        .expect("read stderr");
    assert!(
        stderr.contains("lake"),
        "stderr names the unreachable bucket, got: {stderr}"
    );
    assert!(
        stderr.contains("credentials-file"),
        "stderr names the credential chain tried, got: {stderr}"
    );
}
