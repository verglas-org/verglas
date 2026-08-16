//! `verglas logout` removes the connection profile and nothing else.

use std::fs;
use std::process::Command;

use tempfile::TempDir;

/// Logout strips `[connection]` and its credential files while preserving
/// every other config section, and is a no-op when no profile is stored.
#[test]
fn logout_removes_the_profile_and_preserves_other_sections() {
    let home = TempDir::new().expect("tempdir");
    let config = home.path().join("config.toml");
    let bearer = home.path().join("catalog-token");
    fs::write(&bearer, "cfat_secret").expect("bearer file");
    fs::write(
        &config,
        format!(
            "[catalog]\nuri = \"https://catalog.example\"\n\n[connection]\ncontrol_plane_url = \"https://api.verglas.dev\"\nbearer_file = \"{}\"\n",
            bearer.display()
        ),
    )
    .expect("config file");

    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .env("VERGLAS_CONFIG", &config)
        .arg("logout")
        .output()
        .expect("binary runs");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let text = fs::read_to_string(&config).expect("config remains");
    assert!(text.contains("[catalog]"), "catalog preserved: {text}");
    assert!(!text.contains("[connection]"), "connection removed: {text}");
    assert!(!bearer.exists(), "bearer credential file deleted");

    let again = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .env("VERGLAS_CONFIG", &config)
        .arg("logout")
        .output()
        .expect("binary runs");
    assert!(again.status.success());
    assert!(
        String::from_utf8_lossy(&again.stdout).contains("No connection profile"),
        "second logout is a clean no-op"
    );
}
