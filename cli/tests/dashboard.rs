//! Cloud json-render dashboard CLI contract.

use std::process::Command;

/// Runs the real CLI against an endpoint override.
fn run(endpoint: &str, args: &[&str]) -> (bool, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .env("VERGLAS_ENDPOINT", endpoint)
        .env("VERGLAS_TOKEN", "dashboard-token")
        .args(args)
        .output()
        .expect("run CLI");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn dashboard_rejects_oss_endpoints() {
    let (ok, _stdout, stderr) = run(
        "http://127.0.0.1:8334",
        &["dashboard", "list", "--tenant-id", "tenant-1"],
    );
    assert!(!ok, "OSS endpoint must be rejected");
    assert!(
        stderr.contains("Verglas Cloud") || stderr.contains("OSS"),
        "stderr must mention Cloud-only: {stderr}"
    );
}

#[test]
fn dashboard_create_requires_file() {
    let output = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .env("VERGLAS_ENDPOINT", "https://api.verglas.dev")
        .args(["dashboard", "create"])
        .output()
        .expect("run CLI");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--file") || stderr.contains("required"),
        "create must require --file: {stderr}"
    );
}

#[test]
fn dashboard_create_rejects_static_rows_locally() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bad.json");
    std::fs::write(
        &path,
        r#"{
          "spec_version": 1,
          "name": "bad",
          "title": "Bad",
          "sources": {
            "rows": { "type": "table", "table": "sales.orders", "follow": true }
          },
          "root": "page",
          "elements": {
            "page": {
              "type": "Table",
              "props": { "source": "rows", "rows": [{ "id": 1 }] },
              "children": []
            }
          }
        }"#,
    )
    .expect("write");
    let (ok, _stdout, stderr) = run(
        "https://api.verglas.dev",
        &[
            "dashboard",
            "create",
            "--file",
            path.to_str().expect("utf8"),
            "--tenant-id",
            "tenant-1",
        ],
    );
    assert!(!ok, "static rows must fail before HTTP");
    assert!(
        stderr.contains("static row") || stderr.contains("embeds"),
        "must reject static rows: {stderr}"
    );
    assert!(!stderr.contains("Rill"), "must not mention Rill: {stderr}");
}
