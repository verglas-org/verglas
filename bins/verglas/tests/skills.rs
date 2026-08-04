//! `verglas skills install` — the installer writes the expected assets, and the
//! generated session_start hook behaves against a mock MCP server (emits an
//! injection block on success; fails open — exit 0, empty — on any error).
//!
//! The endpoint + bearer are supplied via the VERGLAS_MCP_ENDPOINT /
//! VERGLAS_MCP_BEARER env overrides so the install never contacts a control
//! plane. HOME is redirected to a scratch dir so the harness surfaces
//! (~/.claude/...) land under the temp tree, not the developer's real home.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;

/// A throwaway temp dir under the target dir, removed on drop.
struct TempHome {
    path: PathBuf,
}

impl TempHome {
    fn new(tag: &str) -> TempHome {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("test setup")
            .as_nanos();
        path.push(format!(
            "verglas-skills-{tag}-{nanos}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("create temp home");
        TempHome { path }
    }
}

impl Drop for TempHome {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Runs `verglas skills install` with HOME + the MCP env overrides set.
fn run_install(
    home: &Path,
    base_dir: &Path,
    endpoint: &str,
    bearer: &str,
    harness: &str,
) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["skills", "install", "--harness", harness, "--base-dir"])
        .arg(base_dir)
        .env("HOME", home)
        .env("VERGLAS_MCP_ENDPOINT", endpoint)
        .env("VERGLAS_MCP_BEARER", bearer)
        .output()
        .expect("binary runs")
}

#[test]
fn install_writes_hooks_creds_skill_and_wiring() {
    let home = TempHome::new("write");
    let base = home.path.join(".verglas/agent");
    let out = run_install(
        &home.path,
        &base,
        "https://cognee-acme.verglas.dev/mcp",
        "vgcat_test_bearer",
        "claude",
    );
    assert!(
        out.status.success(),
        "install failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // The three hook scripts exist and reference the cred files.
    for name in ["session_start.sh", "prompt_recall.sh", "consolidate.sh"] {
        let p = base.join("hooks").join(name);
        assert!(p.exists(), "missing hook {name}");
        let body = std::fs::read_to_string(&p).expect("test setup");
        assert!(
            body.contains("mcp-endpoint"),
            "{name} must read the endpoint file"
        );
        assert!(
            body.contains("mcp-bearer"),
            "{name} must read the bearer file"
        );
        // No placeholder left un-rendered.
        assert!(
            !body.contains("__VERGLAS_MCP_ENDPOINT_FILE__"),
            "{name} placeholder unrendered"
        );
    }

    // The credential files hold the endpoint (0644) and bearer (0600).
    let endpoint_file = home.path.join(".verglas/credentials/mcp-endpoint");
    let bearer_file = home.path.join(".verglas/credentials/mcp-bearer");
    assert_eq!(
        std::fs::read_to_string(&endpoint_file).expect("test setup"),
        "https://cognee-acme.verglas.dev/mcp"
    );
    assert_eq!(
        std::fs::read_to_string(&bearer_file).expect("test setup"),
        "vgcat_test_bearer"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&bearer_file)
            .expect("test setup")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "bearer file must be 0600");
    }

    // The skill file landed in the Claude skills dir.
    let skill = home.path.join(".claude/skills/verglas/SKILL.md");
    assert!(skill.exists(), "SKILL.md missing");
    assert!(
        std::fs::read_to_string(&skill)
            .expect("test setup")
            .contains("recall")
    );

    // settings.json wired the five Claude hook events at the shipped scripts.
    let settings = home.path.join(".claude/settings.json");
    let raw = std::fs::read_to_string(&settings).expect("test setup");
    let v: serde_json::Value = serde_json::from_str(&raw).expect("test setup");
    let hooks = v
        .get("hooks")
        .and_then(|h| h.as_object())
        .expect("hooks object");
    for event in [
        "SessionStart",
        "UserPromptSubmit",
        "Stop",
        "SessionEnd",
        "PreCompact",
    ] {
        assert!(
            hooks.contains_key(event),
            "settings.json missing hook event {event}"
        );
    }
    assert!(
        raw.contains("session_start.sh"),
        "settings must reference session_start.sh"
    );
}

#[test]
fn install_is_idempotent_and_preserves_foreign_hooks() {
    let home = TempHome::new("idem");
    let base = home.path.join(".verglas/agent");
    // Seed a foreign Claude hook that must survive.
    let claude = home.path.join(".claude");
    std::fs::create_dir_all(&claude).expect("test setup");
    std::fs::write(
        claude.join("settings.json"),
        r#"{"hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"echo foreign"}]}]}}"#,
    )
    .expect("test setup");

    let a = run_install(
        &home.path,
        &base,
        "https://cognee-a.verglas.dev/mcp",
        "b1",
        "claude",
    );
    assert!(a.status.success());
    let b = run_install(
        &home.path,
        &base,
        "https://cognee-a.verglas.dev/mcp",
        "b1",
        "claude",
    );
    assert!(b.status.success());

    let raw = std::fs::read_to_string(claude.join("settings.json")).expect("test setup");
    let v: serde_json::Value = serde_json::from_str(&raw).expect("test setup");
    let start = v["hooks"]["SessionStart"].as_array().expect("test setup");
    // Foreign hook preserved; our hook present EXACTLY once (not duplicated by the
    // second install).
    assert_eq!(
        raw.matches("echo foreign").count(),
        1,
        "foreign hook must survive once"
    );
    assert_eq!(
        raw.matches("session_start.sh").count(),
        1,
        "our hook must appear once after re-install"
    );
    assert!(
        start.len() >= 2,
        "both the foreign and our SessionStart hook present"
    );
}

/// Whether curl and python3 are available (the hooks need both). Skips the
/// behavioral test cleanly when they are not.
fn have_hook_deps() -> bool {
    ["curl", "python3"].iter().all(|bin| {
        Command::new(bin)
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    })
}

#[test]
fn session_start_hook_injects_context_from_a_mock_mcp_server() {
    if !have_hook_deps() {
        eprintln!("skipping: curl/python3 not available");
        return;
    }
    let home = TempHome::new("mock");
    let base = home.path.join(".verglas/agent");
    // Install with placeholder creds; we drive the hook via env overrides below.
    run_install(
        &home.path,
        &base,
        "https://unused.example/mcp",
        "unused",
        "claude",
    );
    let hook = base.join("hooks/session_start.sh");

    // A one-shot mock MCP server returning a JSON-RPC session_context result.
    let listener = TcpListener::bind("127.0.0.1:0").expect("test setup");
    let port = listener.local_addr().expect("test setup").port();
    let server = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf); // consume the request (not parsed)
            let body = r#"{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"REMEMBERED CONTEXT"}]}}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.flush();
        }
    });

    let out = Command::new("bash")
        .arg(&hook)
        .arg("claude")
        .env(
            "VERGLAS_MCP_ENDPOINT",
            format!("http://127.0.0.1:{port}/mcp"),
        )
        .env("VERGLAS_MCP_BEARER", "test-bearer")
        .env("VERGLAS_MCP_TIMEOUT", "5")
        .output()
        .expect("hook runs");
    let _ = server.join();

    assert!(out.status.success(), "hook must exit 0");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("REMEMBERED CONTEXT"),
        "hook must inject the recalled block: {stdout}"
    );
    assert!(
        stdout.contains("additionalContext"),
        "hook must emit the SessionStart envelope: {stdout}"
    );
}

#[test]
fn session_start_hook_fails_open_when_the_server_is_down() {
    if !have_hook_deps() {
        eprintln!("skipping: curl/python3 not available");
        return;
    }
    let home = TempHome::new("failopen");
    let base = home.path.join(".verglas/agent");
    run_install(
        &home.path,
        &base,
        "https://unused.example/mcp",
        "unused",
        "claude",
    );
    let hook = base.join("hooks/session_start.sh");

    // Point at a port nothing is listening on -> curl fails -> hook must fail open.
    let out = Command::new("bash")
        .arg(&hook)
        .arg("claude")
        .env("VERGLAS_MCP_ENDPOINT", "http://127.0.0.1:9/mcp")
        .env("VERGLAS_MCP_BEARER", "test-bearer")
        .env("VERGLAS_MCP_TIMEOUT", "2")
        .output()
        .expect("hook runs");
    assert!(out.status.success(), "hook must exit 0 on failure");
    assert!(
        out.stdout.is_empty(),
        "hook must emit nothing on failure: {:?}",
        String::from_utf8_lossy(&out.stdout)
    );
}
