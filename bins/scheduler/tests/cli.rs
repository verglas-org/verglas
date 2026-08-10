//! Command-line contract tests for the standalone scheduler service.

use std::process::Command;

/// The service exposes only Postgres queue, execution, and listener settings.
#[test]
fn help_has_no_cloud_callback_or_polling_modes() {
    let output = Command::new(env!("CARGO_BIN_EXE_verglas-scheduler"))
        .arg("--help")
        .output()
        .expect("scheduler help");
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 help");
    assert!(stdout.contains("--database-url"));
    assert!(stdout.contains("--verglas-url"));
    assert!(stdout.contains("--verglas-token"));
    assert!(stdout.contains("--worker-endpoint"));
    assert!(stdout.contains("--listen"));
    assert!(!stdout.contains("--once"));
    assert!(!stdout.contains("--poll-ms"));
    assert!(!stdout.contains("--orchestrator-callback"));
    assert!(!stdout.contains("--catalog-url"));
}
