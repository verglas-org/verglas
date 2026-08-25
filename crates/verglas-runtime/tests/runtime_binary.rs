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
