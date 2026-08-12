//! End-to-end contract tests for release-embedded agent skill installation.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use tempfile::TempDir;

/// Returns the checked-in RIME skill that the release binary must embed exactly.
fn source_skill() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../apps/os/packages/rime/skills/rime")
}

/// Returns one checked-in source artifact used by a native host integration.
fn source_artifact(path: &str) -> Vec<u8> {
    fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../apps/os/packages/rime")
            .join(path),
    )
    .expect("read source artifact")
}

/// Runs the CLI with every supported agent home redirected below a temporary root.
fn run_install(home: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(args)
        .env("HOME", home)
        .env("CODEX_HOME", home.join("codex"))
        .env("CLAUDE_CONFIG_DIR", home.join("claude"))
        .env("VERGLAS_CREDENTIALS_FILE", home.join("credentials.json"))
        .output()
        .expect("binary runs")
}

/// Recursively records relative file names and contents in deterministic order.
fn tree(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    fn visit(root: &Path, directory: &Path, files: &mut Vec<(PathBuf, Vec<u8>)>) {
        let mut entries = fs::read_dir(directory)
            .expect("read skill directory")
            .collect::<Result<Vec<_>, _>>()
            .expect("read skill entries");
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            if path.is_dir() {
                visit(root, &path, files);
            } else {
                files.push((
                    path.strip_prefix(root)
                        .expect("relative skill path")
                        .to_owned(),
                    fs::read(path).expect("read skill file"),
                ));
            }
        }
    }

    let mut files = Vec::new();
    visit(root, root, &mut files);
    files
}

#[test]
fn default_install_places_the_complete_rime_integration_in_every_agent() {
    let home = TempDir::new().expect("temporary home");
    fs::write(home.path().join("credentials.json"), b"not json")
        .expect("write deliberately invalid credentials");
    let output = run_install(home.path(), &["skill", "install", "rime"]);
    assert!(
        output.status.success(),
        "install failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let expected = tree(&source_skill());
    assert_eq!(tree(&home.path().join(".pi/agent/skills/rime")), expected);
    assert_eq!(tree(&home.path().join("codex/skills/rime")), expected);
    assert_eq!(
        fs::read(home.path().join(".pi/agent/extensions/rime.ts")).expect("Pi extension"),
        source_artifact("extensions/rime.ts")
    );
    assert_eq!(
        fs::read(home.path().join(".pi/agent/agents/rime-worker.md")).expect("Pi worker"),
        source_artifact("host/pi/rime-worker.md")
    );
    assert_eq!(
        fs::read(home.path().join("codex/agents/rime_worker.toml")).expect("Codex worker"),
        source_artifact("host/codex/rime_worker.toml")
    );
    let claude_plugin = home.path().join("claude/skills/rime");
    assert_eq!(
        fs::read(claude_plugin.join(".claude-plugin/plugin.json")).expect("Claude manifest"),
        source_artifact(".claude-plugin/plugin.json")
    );
    assert_eq!(
        tree(&claude_plugin.join("skills/rime")),
        tree(&source_skill())
    );
    assert_eq!(
        fs::read(claude_plugin.join("agents/rime-worker.md")).expect("Claude worker"),
        source_artifact("agents/rime-worker.md")
    );
    assert_eq!(
        fs::read(home.path().join("credentials.json")).expect("read credentials"),
        b"not json",
        "skill installation must neither read nor rewrite credentials"
    );
}

#[test]
fn one_target_installs_only_that_agents_skill() {
    let home = TempDir::new().expect("temporary home");
    let output = run_install(home.path(), &["skill", "install", "rime", "--target", "pi"]);
    assert!(output.status.success());
    assert_eq!(
        tree(&home.path().join(".pi/agent/skills/rime")),
        tree(&source_skill())
    );
    assert!(home.path().join(".pi/agent/extensions/rime.ts").exists());
    assert!(home.path().join(".pi/agent/agents/rime-worker.md").exists());
    assert!(!home.path().join("codex/skills/rime").exists());
    assert!(!home.path().join("claude/skills/rime").exists());
}

#[test]
fn reinstall_atomically_replaces_the_managed_directory() {
    let home = TempDir::new().expect("temporary home");
    let args = ["skill", "install", "rime", "--target", "codex"];
    assert!(run_install(home.path(), &args).status.success());
    let destination = home.path().join("codex/skills/rime");
    fs::write(destination.join("stale.txt"), b"stale").expect("write stale file");
    let unrelated = home.path().join("codex/agents/unrelated.toml");
    fs::write(&unrelated, b"keep me").expect("write unrelated agent");
    fs::write(
        home.path().join("codex/agents/rime_worker.toml"),
        b"stale worker",
    )
    .expect("write stale worker");

    let output = run_install(home.path(), &args);
    assert!(output.status.success());
    assert_eq!(tree(&destination), tree(&source_skill()));
    assert!(!destination.join("stale.txt").exists());
    assert_eq!(
        fs::read(unrelated).expect("unrelated agent survives"),
        b"keep me"
    );
    assert_eq!(
        fs::read(home.path().join("codex/agents/rime_worker.toml")).expect("updated worker"),
        source_artifact("host/codex/rime_worker.toml")
    );
}

#[test]
fn unsupported_skill_and_target_do_not_install_anything() {
    for args in [
        &["skill", "install", "memory"][..],
        &["skill", "install", "rime", "--target", "cursor"][..],
    ] {
        let home = TempDir::new().expect("temporary home");
        let output = run_install(home.path(), args);
        assert!(!output.status.success());
        assert!(!home.path().join(".pi").exists());
        assert!(!home.path().join("codex").exists());
        assert!(!home.path().join("claude").exists());
    }
}
