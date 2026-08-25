//! Fail-closed process checks for the Turso-backed `verglas-runtime` binary.
//!
//! A real remote Turso service is intentionally not faked in this suite. These
//! tests verify that missing configuration and obsolete durability arguments are
//! rejected before any process endpoint can be exposed.

use std::process::{Command, Stdio};

/// Missing remote Turso configuration exits before any event socket bind.
#[test]
fn binary_requires_explicit_turso_configuration() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let event_socket = directory.path().join("events.sock");
    let status = Command::new(env!("CARGO_BIN_EXE_verglas-runtime"))
        .args([
            "--do-id",
            "agent-1",
            "--data-dir",
            directory.path().to_str().ok_or("invalid data path")?,
            "--event-socket",
            event_socket.to_str().ok_or("invalid event path")?,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    assert!(!status.success());
    assert!(!event_socket.exists());
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
            "--turso-url",
            "https://tenant.turso.io/catalog-1",
            "--turso-token-file",
            directory
                .path()
                .join("token")
                .to_str()
                .ok_or("invalid token path")?,
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
            "--turso-url",
            "https://tenant.turso.io/db-1",
            "--turso-token-file",
            directory
                .path()
                .join("token")
                .to_str()
                .ok_or("invalid token path")?,
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
            "--turso-url",
            "https://tenant.turso.io/db-1",
            "--turso-token-file",
            directory
                .path()
                .join("token")
                .to_str()
                .ok_or("invalid token path")?,
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
