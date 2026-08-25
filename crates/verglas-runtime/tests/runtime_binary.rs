//! Fail-closed process checks for the embedded-Turso `verglas-runtime` binary.
//!
//! These tests verify that retired remote credentials and obsolete durability
//! arguments are rejected before any process endpoint can be exposed.

use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

/// The retired remote URL and token-file arguments are rejected before bind.
#[test]
fn binary_rejects_retired_turso_arguments() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    for retired in ["--turso-url", "--turso-token-file"] {
        let output = Command::new(env!("CARGO_BIN_EXE_verglas-runtime"))
            .args([
                "--do-id",
                "agent-1",
                "--data-dir",
                directory.path().to_str().ok_or("invalid data path")?,
                retired,
                "retired-value",
            ])
            .output()?;
        assert!(!output.status.success(), "{retired} was accepted");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("unknown argument"),
            "{retired} did not identify the retired option"
        );
    }
    Ok(())
}

/// SIGINT exits successfully only after the embedded shutdown fence completes.
#[test]
fn binary_sigint_completes_embedded_shutdown_fence() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_verglas-runtime"))
        .args([
            "--do-id",
            "agent-signal",
            "--data-dir",
            directory.path().to_str().ok_or("invalid data path")?,
        ])
        .spawn()?;
    let database = directory.path().join("turso.db");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !database.exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(database.exists(), "runtime did not open embedded Turso");
    thread::sleep(Duration::from_millis(50));
    let pid = libc::pid_t::try_from(child.id())?;
    // SAFETY: the pid belongs to the live child retained directly above.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGINT) }, 0);
    let status = child.wait()?;
    assert!(status.success(), "runtime shutdown failed with {status}");
    Ok(())
}

/// Component options remain paired while DO identity and local data are enough otherwise.
#[test]
fn binary_requires_component_options_together() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let digest = "ababaabababaabababaabababaabababaabababaabababaabababaabababaaba";
    let cases = [
        ["--component-digest", digest],
        [
            "--component-dir",
            directory.path().to_str().ok_or("invalid component path")?,
        ],
    ];
    for case in cases {
        let mut arguments = vec![
            "--do-id",
            "agent-1",
            "--data-dir",
            directory.path().to_str().ok_or("invalid data path")?,
        ];
        arguments.extend(case);
        let output = Command::new(env!("CARGO_BIN_EXE_verglas-runtime"))
            .args(arguments)
            .output()?;
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("--component-digest and --component-dir must be supplied together")
        );
    }
    Ok(())
}

/// The removed replica/CAS argument surface is not accepted as compatibility.
#[test]
fn binary_rejects_removed_durability_arguments() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let status = Command::new(env!("CARGO_BIN_EXE_verglas-runtime"))
        .args([
            "--do-id",
            "agent-1",
            "--data-dir",
            directory.path().to_str().ok_or("invalid data path")?,
            "--replica-id",
            "1",
        ])
        .status()?;
    assert!(!status.success());
    Ok(())
}

/// Privileged Catalog configuration cannot be opened without a verified component endpoint.
#[test]
fn catalog_host_config_requires_component_event_socket() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let output = Command::new(env!("CARGO_BIN_EXE_verglas-runtime"))
        .args([
            "--do-id",
            "catalog-1",
            "--data-dir",
            directory.path().to_str().ok_or("invalid data path")?,
            "--catalog-host-config",
            directory
                .path()
                .join("catalog.json")
                .to_str()
                .ok_or("invalid config path")?,
        ])
        .output()?;
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("--catalog-host-config requires a verified component event socket")
    );
    Ok(())
}

/// The Catalog host configuration flag is parsed as a real startup input.
#[test]
fn binary_rejects_unreadable_catalog_host_config_before_socket_bind()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let event_socket = directory.path().join("events.sock");
    let digest = "ababaabababaabababaabababaabababaabababaabababaabababaabababaaba";
    let output = Command::new(env!("CARGO_BIN_EXE_verglas-runtime"))
        .args([
            "--do-id",
            "agent-1",
            "--data-dir",
            directory.path().to_str().ok_or("invalid data path")?,
            "--component-digest",
            digest,
            "--component-dir",
            directory.path().to_str().ok_or("invalid component path")?,
            "--event-socket",
            event_socket.to_str().ok_or("invalid event path")?,
            "--catalog-host-config",
            directory
                .path()
                .join("missing.json")
                .to_str()
                .ok_or("invalid config path")?,
        ])
        .output()?;
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("unknown argument `--catalog-host-config`"));
    assert!(stderr.contains("Read"));
    assert!(!event_socket.exists());
    Ok(())
}

/// Invalid Catalog JSON exits before the runtime can bind its event socket.
#[test]
fn binary_rejects_invalid_catalog_host_config_before_socket_bind()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let event_socket = directory.path().join("events.sock");
    let config_path = directory.path().join("catalog.json");
    std::fs::write(&config_path, b"{\"unknown\":true}")?;
    let output = Command::new(env!("CARGO_BIN_EXE_verglas-runtime"))
        .args([
            "--do-id",
            "agent-1",
            "--data-dir",
            directory.path().to_str().ok_or("invalid data path")?,
            "--component-digest",
            "ababaabababaabababaabababaabababaabababaabababaabababaabababaaba",
            "--component-dir",
            directory.path().to_str().ok_or("invalid component path")?,
            "--event-socket",
            event_socket.to_str().ok_or("invalid event path")?,
            "--catalog-host-config",
            config_path.to_str().ok_or("invalid config path")?,
        ])
        .output()?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Json"));
    assert!(!event_socket.exists());
    Ok(())
}
