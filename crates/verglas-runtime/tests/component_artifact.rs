//! Acceptance tests for verified component loading before embedded Turso startup.
//!
//! The process tests exercise the same local database path used in production;
//! component validation still happens before any event endpoint bind.

use std::process::{Command, Stdio};

use verglas_do_wasm::ComponentDigest;

/// Builds a command with the production embedded Turso arguments.
fn command(directory: &tempfile::TempDir) -> Result<Command, Box<dyn std::error::Error>> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_verglas-runtime"));
    command
        .args(["--do-id", "artifact-test", "--data-dir"])
        .arg(directory.path().join("data"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    Ok(command)
}

/// A single component option is rejected before Turso startup or endpoint bind.
#[test]
fn component_options_must_be_supplied_as_a_pair() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let digest = ComponentDigest::compute(b"component");
    let status = command(&directory)?
        .args(["--component-digest"])
        .arg(digest.to_string())
        .status()?;
    assert!(!status.success());
    Ok(())
}

/// An event socket without a verified component is rejected before binding.
#[test]
fn event_socket_requires_component_arguments() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let event_socket = directory.path().join("events.sock");
    let status = command(&directory)?
        .args(["--event-socket"])
        .arg(&event_socket)
        .status()?;
    assert!(!status.success());
    assert!(!event_socket.exists());
    Ok(())
}

/// A corrupt digest-named artifact stops a worker before embedded startup.
#[test]
fn corrupt_component_exits_before_turso_startup() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let artifact_dir = directory.path().join("components");
    std::fs::create_dir_all(&artifact_dir)?;
    let digest = ComponentDigest::compute(b"the expected component");
    std::fs::write(
        artifact_dir.join(format!("{digest}.wasm")),
        b"corrupt component",
    )?;
    let status = command(&directory)?
        .args(["--component-digest"])
        .arg(digest.to_string())
        .args(["--component-dir"])
        .arg(&artifact_dir)
        .status()?;
    assert!(!status.success());
    Ok(())
}

/// A configured cache path that is not a directory fails before embedded startup.
#[test]
fn unusable_cwasm_cache_exits_before_turso_startup() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let artifact_dir = directory.path().join("components");
    let cache_path = directory.path().join("cache-file");
    std::fs::create_dir_all(&artifact_dir)?;
    let bytes = wat::parse_str("(component)")?;
    let digest = ComponentDigest::compute(&bytes);
    std::fs::write(artifact_dir.join(format!("{digest}.wasm")), bytes)?;
    std::fs::write(&cache_path, b"not a directory")?;
    let status = command(&directory)?
        .args(["--component-digest"])
        .arg(digest.to_string())
        .args(["--component-dir"])
        .arg(&artifact_dir)
        .args(["--cwasm-cache-dir"])
        .arg(&cache_path)
        .status()?;
    assert!(!status.success());
    Ok(())
}
