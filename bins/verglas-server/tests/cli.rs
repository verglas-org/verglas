//! Smoke tests for the `verglas-server` binary.

use std::process::Command;

#[test]
fn version_flag_prints_name_and_version() {
    let out = Command::new(env!("CARGO_BIN_EXE_verglas-server"))
        .arg("--version")
        .output()
        .expect("binary runs");
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    assert!(stdout.starts_with("verglas-server "));
}
