//! CLI contract tests for the standalone Gadget runtime service.

use std::process::Command;

#[test]
fn help_documents_local_multiplexing_and_cloud_targeting() {
    let output = Command::new(env!("CARGO_BIN_EXE_verglas-gadget-runtime"))
        .arg("--help")
        .output()
        .expect("run help");
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("help UTF-8");
    assert!(stdout.contains("--max-gadgets"));
    assert!(stdout.contains("--target-gadget"));
    assert!(stdout.contains("--host-script"));
}

#[test]
fn startup_requires_an_explicit_runtime_token() {
    let output = Command::new(env!("CARGO_BIN_EXE_verglas-gadget-runtime"))
        .env_remove("VERGLAS_GADGET_RUNTIME_TOKEN")
        .output()
        .expect("run without token");
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr UTF-8");
    assert!(stderr.contains("--runtime-token"));
}
