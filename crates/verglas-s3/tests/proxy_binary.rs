//! Command-line contract for the one-bucket Durable Object S3 proxy.

use std::process::Command;

/// The proxy exposes only explicit coordinates and credential files. It does
/// not accept source code, component paths, or compilation options.
#[test]
fn help_describes_one_preprovisioned_bucket() {
    let output = Command::new(env!("CARGO_BIN_EXE_verglas-s3-proxy"))
        .arg("--help")
        .output()
        .expect("run proxy help");
    let help = String::from_utf8(output.stderr).expect("UTF-8 help");
    assert!(help.contains("--public-bucket"));
    assert!(help.contains("--origin-bucket"));
    assert!(help.contains("--origin-credentials"));
    assert!(help.contains("--client-credentials"));
    assert!(!help.contains("compile"));
}
