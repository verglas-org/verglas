//! `verglas skills install` — install the Verglas agent skill + lifecycle hooks
//! that speak to the TENANT'S memory MCP endpoint.
//!
//! Memory moved out of the daemon into a per-tenant cognee MCP container. This
//! installer wires an agent session to that memory over MCP:
//!   - three shared hook scripts under `~/.verglas/agent/hooks/` — session_start
//!     (inject `session_context`), prompt_recall (inject `recall`), and
//!     consolidate (post the session via `remember` at close);
//!   - the memory MCP endpoint + bearer, written to `~/.verglas/credentials/`
//!     (`mcp-endpoint` 0644, `mcp-bearer` 0600). The hooks read these two files.
//!     The endpoint + bearer are resolved from the control plane (`GET /v1/mcp`,
//!     the agent-hook discovery route) so the CLI never hardcodes the volatile
//!     memory-container name; env overrides win for tests/dev;
//!   - the skill file, plus the per-harness hook wiring (Claude settings.json,
//!     Codex/Cursor hooks.json), merged ADDITIVELY after a timestamped backup.
//!
//! The install is idempotent: re-running rewrites the assets and re-registers the
//! same hook entries (foreign entries are never touched). Nothing here is secret
//! on stdout — the bearer lives only in the mode-0600 file.

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use crate::cli::SkillsInstallArgs;
use crate::controlplane::ControlPlaneClient;

// Embedded assets — shipped in the binary so install writes them with no download.
const SKILL_MD: &str = include_str!("skill_assets/SKILL.md");
const HOOK_SESSION_START_SH: &str = include_str!("skill_assets/hooks/session_start.sh");
const HOOK_PROMPT_RECALL_SH: &str = include_str!("skill_assets/hooks/prompt_recall.sh");
const HOOK_CONSOLIDATE_SH: &str = include_str!("skill_assets/hooks/consolidate.sh");

/// Which harnesses to install the skill + hook wiring for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Harness {
    Claude,
    Codex,
    Cursor,
}

/// The agent-hook discovery route on the control plane: the tenant's memory MCP
/// ingress + endpoint bearer.
const AGENT_MCP_PATH: &str = "/v1/mcp";
/// Env override for the memory MCP endpoint URL (tests/dev; wins over discovery).
const ENDPOINT_ENV: &str = "VERGLAS_MCP_ENDPOINT";
/// Env override for the memory MCP bearer (tests/dev; wins over discovery).
const BEARER_ENV: &str = "VERGLAS_MCP_BEARER";

/// The resolved paths and configuration for one install.
struct Install {
    home: PathBuf,
    hooks: PathBuf,         // ~/.verglas/agent/hooks
    endpoint_file: PathBuf, // ~/.verglas/credentials/mcp-endpoint
    bearer_file: PathBuf,   // ~/.verglas/credentials/mcp-bearer
    endpoint: String,
    bearer: String,
    portal_url: Option<String>,
}

/// Runs `verglas skills install`.
pub async fn run(args: SkillsInstallArgs, json: bool) -> Result<(), Box<dyn Error>> {
    let harnesses = parse_harnesses(&args.harness)?;
    let (endpoint, bearer, portal_url) = resolve_endpoint(&args).await?;
    let install = Install::resolve(&args, endpoint, bearer, portal_url)?;

    install.write_shared()?;
    let mut done = Vec::new();
    for h in &harnesses {
        install.install_harness(*h)?;
        done.push(format!("{h:?}").to_lowercase());
    }

    if json {
        println!(
            "{}",
            serde_json::json!({
                "installed_harnesses": done,
                "hooks_dir": install.hooks.display().to_string(),
                "endpoint_file": install.endpoint_file.display().to_string(),
                "bearer_file": install.bearer_file.display().to_string(),
                "memory_mcp": install.endpoint,
            })
        );
    } else {
        println!("Verglas skill installed for: {}", done.join(", "));
        println!("  hooks:    {}", install.hooks.display());
        println!("  memory:   {}", install.endpoint);
        if let Some(p) = &install.portal_url {
            println!("  portal:   {p}");
        }
        println!("  endpoint: {} (0644)", install.endpoint_file.display());
        println!("  bearer:   {} (0600)", install.bearer_file.display());
        println!("Re-run `verglas skills install` any time; it is idempotent.");
    }
    Ok(())
}

/// Parses the `--harness` value into a set.
fn parse_harnesses(s: &str) -> Result<Vec<Harness>, Box<dyn Error>> {
    Ok(match s {
        "all" => vec![Harness::Claude, Harness::Codex, Harness::Cursor],
        "claude" => vec![Harness::Claude],
        "codex" => vec![Harness::Codex],
        "cursor" => vec![Harness::Cursor],
        other => {
            return Err(format!("unknown harness '{other}' (want claude|codex|cursor|all)").into());
        }
    })
}

/// Resolves the memory MCP endpoint + bearer (+ optional portal URL). Order:
/// the `--endpoint` flag or `VERGLAS_MCP_ENDPOINT`/`VERGLAS_MCP_BEARER` env
/// (tests/dev) win; otherwise the control-plane discovery route is called with
/// the stored login key. The bearer never comes from a CLI flag (never a secret
/// on the command line).
async fn resolve_endpoint(
    args: &SkillsInstallArgs,
) -> Result<(String, String, Option<String>), Box<dyn Error>> {
    let env_endpoint = args
        .endpoint
        .clone()
        .or_else(|| std::env::var(ENDPOINT_ENV).ok())
        .filter(|s| !s.trim().is_empty());
    let env_bearer = std::env::var(BEARER_ENV)
        .ok()
        .filter(|s| !s.trim().is_empty());
    if let (Some(endpoint), Some(bearer)) = (&env_endpoint, &env_bearer) {
        return Ok((endpoint.trim().to_owned(), bearer.trim().to_owned(), None));
    }

    // Discover from the control plane (requires a prior `verglas login`).
    let client = ControlPlaneClient::from_stored()?;
    let value = client.get_value(AGENT_MCP_PATH).await.map_err(|e| {
        format!("could not reach the memory MCP discovery route ({e}); run `verglas login` first")
    })?;
    let endpoint = env_endpoint
        .or_else(|| {
            value
                .get("endpoint")
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        })
        .ok_or("the control plane did not return a memory MCP endpoint")?;
    let bearer = value
        .get("bearer")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .ok_or("the control plane did not return a memory MCP bearer")?;
    let portal_url = value
        .get("portal_url")
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    Ok((endpoint, bearer, portal_url))
}

impl Install {
    /// Resolves the install paths, honoring `--base-dir` (the agent base, default
    /// `~/.verglas/agent`) so tests can point it at a scratch dir.
    fn resolve(
        args: &SkillsInstallArgs,
        endpoint: String,
        bearer: String,
        portal_url: Option<String>,
    ) -> Result<Install, Box<dyn Error>> {
        let home = PathBuf::from(std::env::var("HOME")?);
        let base = args
            .base_dir
            .clone()
            .unwrap_or_else(|| home.join(".verglas/agent"));
        let hooks = base.join("hooks");
        let cred_dir = home.join(".verglas/credentials");
        Ok(Install {
            home,
            hooks,
            endpoint_file: cred_dir.join("mcp-endpoint"),
            bearer_file: cred_dir.join("mcp-bearer"),
            endpoint,
            bearer,
            portal_url,
        })
    }

    /// Writes the shared assets: the credential files the hooks read, then the
    /// three hook scripts (the cred-file paths baked in; env overrides win).
    fn write_shared(&self) -> Result<(), Box<dyn Error>> {
        fs::create_dir_all(&self.hooks)?;
        if let Some(parent) = self.endpoint_file.parent() {
            fs::create_dir_all(parent)?;
        }
        // The endpoint is not secret (0644); the bearer is (0600).
        write_mode(&self.endpoint_file, &self.endpoint, 0o644)?;
        write_mode(&self.bearer_file, &self.bearer, 0o600)?;

        for (name, body) in [
            ("session_start.sh", HOOK_SESSION_START_SH),
            ("prompt_recall.sh", HOOK_PROMPT_RECALL_SH),
            ("consolidate.sh", HOOK_CONSOLIDATE_SH),
        ] {
            let rendered = body
                .replace(
                    "__VERGLAS_MCP_ENDPOINT_FILE__",
                    &self.endpoint_file.display().to_string(),
                )
                .replace(
                    "__VERGLAS_MCP_BEARER_FILE__",
                    &self.bearer_file.display().to_string(),
                );
            write_exec(&self.hooks.join(name), &rendered)?;
        }
        Ok(())
    }

    /// Installs the skill file + hook wiring for one harness.
    fn install_harness(&self, h: Harness) -> Result<(), Box<dyn Error>> {
        match h {
            Harness::Claude => {
                self.write_skill(&self.home.join(".claude/skills/verglas"))?;
                self.merge_json_hooks(
                    &self.home.join(".claude/settings.json"),
                    &self.claude_hook_commands(),
                    false,
                )
            }
            Harness::Codex => {
                self.write_skill(&self.home.join(".codex/skills/verglas"))?;
                self.merge_json_hooks(
                    &self.home.join(".codex/hooks.json"),
                    &self.codex_hook_commands(),
                    false,
                )
            }
            Harness::Cursor => {
                self.write_skill(&self.home.join(".cursor/skills/verglas"))?;
                self.merge_cursor_hooks(
                    &self.home.join(".cursor/hooks.json"),
                    &self.cursor_hook_commands(),
                )
            }
        }
    }

    /// Writes SKILL.md into a harness skill directory.
    fn write_skill(&self, dir: &Path) -> Result<(), Box<dyn Error>> {
        fs::create_dir_all(dir)?;
        fs::write(dir.join("SKILL.md"), SKILL_MD)?;
        Ok(())
    }

    /// The Claude Code hook commands: session-start injection, per-prompt recall,
    /// and consolidation at every session-close event.
    fn claude_hook_commands(&self) -> Vec<(&'static str, String)> {
        let h = |name: &str| format!("bash {}", self.hooks.join(name).display());
        vec![
            ("SessionStart", format!("{} claude", h("session_start.sh"))),
            (
                "UserPromptSubmit",
                format!("{} claude", h("prompt_recall.sh")),
            ),
            ("Stop", format!("{} claude", h("consolidate.sh"))),
            ("SessionEnd", format!("{} claude", h("consolidate.sh"))),
            ("PreCompact", format!("{} claude", h("consolidate.sh"))),
        ]
    }

    /// The Codex hook commands (Claude-style event names; no SessionEnd).
    fn codex_hook_commands(&self) -> Vec<(&'static str, String)> {
        let h = |name: &str| format!("bash {}", self.hooks.join(name).display());
        vec![
            ("SessionStart", format!("{} codex", h("session_start.sh"))),
            (
                "UserPromptSubmit",
                format!("{} codex", h("prompt_recall.sh")),
            ),
            ("Stop", format!("{} codex", h("consolidate.sh"))),
            ("PreCompact", format!("{} codex", h("consolidate.sh"))),
        ]
    }

    /// The Cursor hook commands (lower-camel event names, flat entry shape).
    fn cursor_hook_commands(&self) -> Vec<(&'static str, String)> {
        let h = |name: &str| format!("bash {}", self.hooks.join(name).display());
        vec![
            ("sessionStart", format!("{} cursor", h("session_start.sh"))),
            (
                "beforeSubmitPrompt",
                format!("{} cursor", h("prompt_recall.sh")),
            ),
            ("stop", format!("{} cursor", h("consolidate.sh"))),
            ("sessionEnd", format!("{} cursor", h("consolidate.sh"))),
        ]
    }

    /// Merges Verglas hooks into a Claude-format JSON hooks file (Claude's
    /// settings.json, Codex's hooks.json — both use `hooks.{event}[]`). The file
    /// is backed up first, our prior entries pruned, foreign entries preserved,
    /// and the current ones written (idempotent). `nested` is unused today; both
    /// targets share the same shape.
    fn merge_json_hooks(
        &self,
        path: &Path,
        commands: &[(&'static str, String)],
        _nested: bool,
    ) -> Result<(), Box<dyn Error>> {
        fs::create_dir_all(path.parent().expect("hooks file has a parent"))?;
        let mut root = read_json_with_backup(path)?;
        if !root.is_object() {
            root = serde_json::json!({});
        }
        let obj = root.as_object_mut().expect("root is an object");
        let hooks = obj.entry("hooks").or_insert_with(|| serde_json::json!({}));
        let hooks = hooks.as_object_mut().ok_or("hooks is not an object")?;
        merge_command_hooks(hooks, commands);
        fs::write(path, serde_json::to_string_pretty(&root)? + "\n")?;
        Ok(())
    }

    /// Merges Verglas hooks into a Cursor hooks.json (flat, versioned schema).
    fn merge_cursor_hooks(
        &self,
        path: &Path,
        commands: &[(&'static str, String)],
    ) -> Result<(), Box<dyn Error>> {
        fs::create_dir_all(path.parent().expect("hooks file has a parent"))?;
        let mut root = read_json_with_backup(path)?;
        merge_cursor_hooks(&mut root, commands);
        fs::write(path, serde_json::to_string_pretty(&root)? + "\n")?;
        Ok(())
    }
}

/// Reads a JSON file into a value, backing it up (timestamped) first when it
/// exists. A missing or unparseable file yields an empty object.
fn read_json_with_backup(path: &Path) -> Result<serde_json::Value, Box<dyn Error>> {
    if !path.exists() {
        return Ok(serde_json::json!({}));
    }
    let raw = fs::read_to_string(path)?;
    let backup = path.with_file_name(format!(
        "{}.verglas-bak-{}",
        path.file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default(),
        chrono_stamp(),
    ));
    fs::write(&backup, &raw)?;
    Ok(serde_json::from_str(&raw).unwrap_or_else(|_| serde_json::json!({})))
}

/// True if a hook command belongs to Verglas — it invokes one of our scripts. A
/// re-install prunes its own entries by this test before writing the current
/// ones, so a changed path never leaves a stale duplicate. Foreign hooks never match.
fn is_our_hook_command(command: &str) -> bool {
    ["/consolidate.sh", "/session_start.sh", "/prompt_recall.sh"]
        .iter()
        .any(|script| command.contains(script))
}

/// Merges Verglas command hooks into a Claude-format `hooks` object
/// (`{event: [{hooks: [{type:"command", command}]}]}`). Prunes our prior entries
/// first (idempotent, follows a path change), then appends the current ones.
/// Foreign entries and foreign event arrays are preserved untouched.
fn merge_command_hooks(
    hooks: &mut serde_json::Map<String, serde_json::Value>,
    commands: &[(&'static str, String)],
) {
    for entry in hooks.values_mut() {
        if let Some(arr) = entry.as_array_mut() {
            arr.retain(|group| {
                !group
                    .get("hooks")
                    .and_then(|h| h.as_array())
                    .map(|inner| {
                        inner.iter().any(|c| {
                            c.get("command")
                                .and_then(|v| v.as_str())
                                .map(is_our_hook_command)
                                .unwrap_or(false)
                        })
                    })
                    .unwrap_or(false)
            });
        }
    }
    for (event, command) in commands {
        let arr = hooks
            .entry((*event).to_owned())
            .or_insert_with(|| serde_json::json!([]));
        if let Some(arr) = arr.as_array_mut() {
            let inner = serde_json::json!({ "type": "command", "command": command });
            arr.push(serde_json::json!({ "hooks": [inner] }));
        }
    }
    hooks.retain(|_k, v| v.as_array().map(|a| !a.is_empty()).unwrap_or(true));
}

/// Merges Verglas command hooks into a Cursor hooks.json value
/// (`{version: 1, hooks: {event: [{command}]}}`). Ensures `version`, prunes our
/// prior entries, appends the current ones, preserves foreign entries.
fn merge_cursor_hooks(root: &mut serde_json::Value, commands: &[(&'static str, String)]) {
    if !root.is_object() {
        *root = serde_json::json!({});
    }
    let obj = root.as_object_mut().expect("root is an object");
    obj.entry("version").or_insert_with(|| serde_json::json!(1));
    let hooks = obj.entry("hooks").or_insert_with(|| serde_json::json!({}));
    if !hooks.is_object() {
        *hooks = serde_json::json!({});
    }
    let hooks = hooks.as_object_mut().expect("hooks is an object");
    for entry in hooks.values_mut() {
        if let Some(arr) = entry.as_array_mut() {
            arr.retain(|c| {
                !c.get("command")
                    .and_then(|v| v.as_str())
                    .map(is_our_hook_command)
                    .unwrap_or(false)
            });
        }
    }
    for (event, command) in commands {
        let arr = hooks
            .entry((*event).to_owned())
            .or_insert_with(|| serde_json::json!([]));
        if let Some(arr) = arr.as_array_mut() {
            arr.push(serde_json::json!({ "command": command }));
        }
    }
    hooks.retain(|_k, v| v.as_array().map(|a| !a.is_empty()).unwrap_or(true));
}

/// Writes `content` to `path` and marks it executable (0755).
fn write_exec(path: &Path, content: &str) -> std::io::Result<()> {
    fs::write(path, content)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms)?;
    }
    Ok(())
}

/// Writes `content` to `path` at the given unix mode (mode ignored off unix), so
/// the bearer file is never briefly world-readable and a re-install re-tightens it.
fn write_mode(path: &Path, content: &str, mode: u32) -> std::io::Result<()> {
    fs::write(path, content)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    }
    #[cfg(not(unix))]
    let _ = mode;
    Ok(())
}

/// A filesystem-safe timestamp for a backup suffix (epoch seconds; no crate).
fn chrono_stamp() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
        .to_string()
}
