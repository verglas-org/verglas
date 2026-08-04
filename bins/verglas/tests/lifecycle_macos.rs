//! Live acceptance test for the macOS host lifecycle (#216).
//!
//! Installs a real LaunchAgent through `verglas init`, starts it, checks the
//! service is running and the daemon reports healthy, then stops it and removes
//! it by the documented manual steps (README, "Removing Verglas") — asserting
//! `launchctl print` no longer lists the job and the plist is gone. Gated to
//! macOS: on Linux CI this file compiles to nothing (the systemd path is
//! covered by unit tests and manual verification).
//!
//! The test uses a throwaway cache dir under a temp path and the default port
//! pair shifted to an unlikely value so it does not collide with a developer's
//! own running install. It leaves no LaunchAgent behind: teardown runs even on a
//! failed assertion.

#![cfg(target_os = "macos")]

use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

/// Path to the `verglas` CLI binary under test. `cargo test` builds `verglasd`
/// alongside it, which `verglas init` locates next to itself.
fn verglas_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_verglas"))
}

/// The launchd service label the CLI installs. Kept in sync with the const in
/// `service::launchd`; the test addresses the same job.
const SERVICE_LABEL: &str = "org.verglas.verglas";

/// The GUI-domain service target `launchctl print` addresses.
fn service_target() -> String {
    // SAFETY: getuid only reads the caller's uid and cannot fail.
    let uid = unsafe { libc::getuid() };
    format!("gui/{uid}/{SERVICE_LABEL}")
}

/// Whether `launchctl print` currently lists the job (it is bootstrapped).
fn launchctl_lists_job() -> bool {
    Command::new("launchctl")
        .args(["print", &service_target()])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// An isolated HOME for this test process. `verglas` reads and writes its
/// config, credentials, and LaunchAgent plist under `$HOME`, so pointing HOME at
/// a throwaway directory keeps the test off the developer's real `~/.verglas`
/// (it neither collides with an existing config nor purges it on teardown). The
/// launchd job label is still global per-user, which `launchctl_lists_job`
/// covers.
fn test_home() -> PathBuf {
    std::env::temp_dir().join(format!("verglas-lifecycle-home-{}", std::process::id()))
}

/// Runs a `verglas` subcommand against the isolated test HOME.
fn run_verglas(args: &[&str]) -> std::process::Output {
    let home = test_home();
    std::fs::create_dir_all(&home).expect("create test home");
    Command::new(verglas_bin())
        .args(args)
        .env("HOME", &home)
        .output()
        .expect("verglas runs")
}

/// Best-effort teardown using the documented manual removal steps (README,
/// "Removing Verglas"): boot the job out, remove the plist, then delete the
/// test HOME and cache dir, so a failed run never leaves a LaunchAgent,
/// config, or scratch dir behind.
fn teardown(cache_dir: &std::path::Path) {
    let uid = unsafe { libc::getuid() };
    let plist = test_home()
        .join("Library/LaunchAgents")
        .join(format!("{SERVICE_LABEL}.plist"));
    let _ = Command::new("launchctl")
        .args([
            "bootout",
            &format!("gui/{uid}"),
            &plist.display().to_string(),
        ])
        .output();
    let _ = std::fs::remove_file(&plist);
    let _ = std::fs::remove_dir_all(cache_dir);
    let _ = std::fs::remove_dir_all(test_home());
}

#[test]
fn install_start_status_stop_remove_a_launchagent() {
    // Only run when no real install is present, so the test never clobbers a
    // developer's own running service.
    if launchctl_lists_job() {
        eprintln!("skipping: a verglas LaunchAgent is already loaded on this machine");
        return;
    }

    let cache_dir =
        std::env::temp_dir().join(format!("verglas-lifecycle-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&cache_dir);
    let cache = cache_dir.display().to_string();

    // A blank init (no --endpoint/--region) writes a scaffold whose required
    // trio is not complete, so `start` must refuse naming the missing field
    // before touching the service manager (#221 item 2).
    let blank_init = run_verglas(&["init", "--cache-dir", &cache, "--port", "18333"]);
    assert!(
        blank_init.status.success(),
        "blank init failed: {}",
        String::from_utf8_lossy(&blank_init.stderr)
    );
    let blank_start = run_verglas(&["start"]);
    assert!(
        !blank_start.status.success(),
        "start on a blank config must fail"
    );
    let blank_err = String::from_utf8_lossy(&blank_start.stderr);
    assert!(
        blank_err.contains("backend.bucket") && blank_err.contains("config.toml"),
        "blank start must name the missing field and file: {blank_err}"
    );

    // init --force fills the required trio into the scaffold. A throwaway port
    // pair well away from 8333 keeps the test off a developer's own dev endpoint.
    let init = run_verglas(&[
        "init",
        "--force",
        "--bucket",
        "s3://verglas-lifecycle-test",
        "--endpoint",
        "https://s3.us-east-1.amazonaws.com",
        "--region",
        "us-east-1",
        "--cache-dir",
        &cache,
        "--port",
        "18333",
    ]);
    assert!(
        init.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&init.stderr)
    );

    let result = std::panic::catch_unwind(|| {
        // The plist and the credentials scaffold exist.
        let plist = test_home()
            .join("Library/LaunchAgents")
            .join(format!("{SERVICE_LABEL}.plist"));
        assert!(plist.exists(), "init must write the plist");
        let creds = test_home().join(".verglas/credentials/default");
        assert!(creds.exists(), "init must scaffold the credentials file");

        // start bootstraps the LaunchAgent; the daemon comes up and reports ready
        // (start blocks until /admin/healthz is ready). The daemon does not touch
        // the backend at startup, so an unreachable test bucket still boots.
        let start = run_verglas(&["start"]);
        assert!(
            start.status.success(),
            "start failed: {}",
            String::from_utf8_lossy(&start.stderr)
        );

        // launchctl now lists the job.
        assert!(launchctl_lists_job(), "launchctl must list the started job");

        // status reports running + healthy, and names the running daemon's
        // version (#288 moved the daemon version out of `verglas version` into
        // `verglas status`, where it appears when the daemon is reachable).
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut saw_running = false;
        while Instant::now() < deadline {
            let status = run_verglas(&["status"]);
            let out = String::from_utf8_lossy(&status.stdout);
            if out.contains("running") && out.contains("ok") {
                assert!(
                    out.contains("verglasd") && out.contains(env!("CARGO_PKG_VERSION")),
                    "status must name the reachable daemon version: {out}"
                );
                saw_running = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(300));
        }
        assert!(saw_running, "status must report running + ok");

        // stop boots the job out; launchctl no longer lists it, but the plist
        // (and cache) remain — the service is installed but stopped.
        let stop = run_verglas(&["stop"]);
        assert!(stop.status.success(), "stop failed");
        // Give launchd a moment to tear the job down.
        std::thread::sleep(Duration::from_millis(500));
        assert!(
            !launchctl_lists_job(),
            "launchctl must not list a stopped job"
        );
        assert!(plist.exists(), "stop must leave the plist installed");

        // Removal is manual (#288): the README's "Removing Verglas" steps —
        // boot the job out and remove the plist — must fully remove the
        // service while leaving the cache dir untouched.
        let uid = unsafe { libc::getuid() };
        let _ = Command::new("launchctl")
            .args([
                "bootout",
                &format!("gui/{uid}"),
                &plist.display().to_string(),
            ])
            .output();
        std::fs::remove_file(&plist).expect("remove the plist");
        std::thread::sleep(Duration::from_millis(300));
        assert!(!plist.exists(), "manual removal must remove the plist");
        assert!(
            !launchctl_lists_job(),
            "manual removal must leave no loaded job"
        );
        assert!(
            std::path::Path::new(&cache).exists(),
            "removal must never touch the cache dir"
        );
    });

    teardown(&cache_dir);
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
