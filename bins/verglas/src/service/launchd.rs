//! The macOS launchd implementation of [`ServiceManager`] (#216).
//!
//! Per-user scope writes a LaunchAgent plist under `~/Library/LaunchAgents`
//! (no sudo). System scope writes a LaunchDaemon under `/Library/LaunchDaemons`
//! (needs root). We render the plist ourselves and drive `launchctl`
//! bootstrap / bootout / kickstart / print. The plist points StandardOutPath and
//! StandardErrorPath at log files under the state dir so `verglas logs` can tail
//! them.

use std::path::{Path, PathBuf};
use std::process::Command;

use super::{Scope, ServiceError, ServiceManager, ServiceSpec, ServiceState, check_tool_output};

/// The launchd service label. Reverse-DNS by convention; the plist filename is
/// `<label>.plist` and `launchctl` addresses the service by it.
pub const SERVICE_LABEL: &str = "org.verglas.verglas";

/// The macOS service manager. Holds the scope so every operation targets the
/// right domain (`gui/<uid>` for a LaunchAgent, `system` for a LaunchDaemon).
#[derive(Debug, Clone)]
pub struct LaunchdManager {
    /// The install scope this manager operates in.
    scope: Scope,
}

impl LaunchdManager {
    /// Builds a manager for `scope`.
    pub fn new(scope: Scope) -> Self {
        Self { scope }
    }

    /// The plist path for this scope: `~/Library/LaunchAgents/<label>.plist` for
    /// a user agent, `/Library/LaunchDaemons/<label>.plist` for a system daemon.
    pub fn plist_path(&self) -> Result<PathBuf, ServiceError> {
        let dir = match self.scope {
            Scope::User => home_dir()?.join("Library").join("LaunchAgents"),
            Scope::System => PathBuf::from("/Library/LaunchDaemons"),
        };
        Ok(dir.join(format!("{SERVICE_LABEL}.plist")))
    }

    /// The `launchctl` domain target for this scope. A LaunchAgent lives in the
    /// caller's GUI domain (`gui/<uid>`); a LaunchDaemon lives in `system`.
    fn domain_target(&self) -> Result<String, ServiceError> {
        match self.scope {
            // SAFETY: getuid only reads the caller's uid and cannot fail.
            Scope::User => Ok(format!("gui/{}", unsafe { libc::getuid() })),
            Scope::System => Ok("system".to_owned()),
        }
    }

    /// The fully-qualified service target `launchctl kickstart`/`print` address:
    /// `<domain>/<label>`.
    fn service_target(&self) -> Result<String, ServiceError> {
        Ok(format!("{}/{SERVICE_LABEL}", self.domain_target()?))
    }

    /// Whether the job is currently bootstrapped into its domain, decided by a
    /// successful `launchctl print`. Used by `start` to choose bootstrap (not
    /// loaded) versus kickstart (loaded).
    fn is_loaded(&self) -> Result<bool, ServiceError> {
        let target = self.service_target()?;
        let output = Command::new("launchctl")
            .args(["print", &target])
            .output()
            .map_err(|source| ServiceError::ToolSpawn {
                tool: "launchctl",
                source,
            })?;
        Ok(output.status.success())
    }

    /// Runs `launchctl` with `args`, capturing output, mapping a spawn failure
    /// and a non-zero exit to a [`ServiceError`] naming `verb`.
    fn launchctl(&self, verb: &'static str, args: &[&str]) -> Result<(), ServiceError> {
        let output = Command::new("launchctl")
            .args(args)
            .output()
            .map_err(|source| ServiceError::ToolSpawn {
                tool: "launchctl",
                source,
            })?;
        check_tool_output("launchctl", verb, &output)
    }
}

impl ServiceManager for LaunchdManager {
    /// Renders the plist and writes it under the LaunchAgents/LaunchDaemons
    /// directory. Installing does not start the service — that is `verglas
    /// start`. A prior loaded instance is booted out first so a re-run installs a
    /// clean, stopped service. Idempotent: overwriting the plist and booting out
    /// an absent service are both fine.
    fn install(&self, spec: &ServiceSpec) -> Result<(), ServiceError> {
        let plist = render_plist(SERVICE_LABEL, spec);
        let path = self.plist_path()?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| ServiceError::File {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        // Boot out any prior instance so the new plist is not shadowed by a
        // stale loaded job; the "not loaded" failure is expected on a fresh
        // install and ignored.
        let domain = self.domain_target()?;
        let _ = self.launchctl("bootout", &["bootout", &domain, path_str(&path)?]);
        std::fs::write(&path, plist).map_err(|source| ServiceError::File {
            path: path.clone(),
            source,
        })?;
        Ok(())
    }

    /// Bootstraps the plist into the domain. With `RunAtLoad` the daemon starts
    /// immediately; `KeepAlive` then restarts it on a crash. Idempotent: an
    /// already-loaded service is kickstarted instead so `start` on a running
    /// service is not an error.
    fn start(&self) -> Result<(), ServiceError> {
        let path = self.plist_path()?;
        let domain = self.domain_target()?;
        // Already loaded? Kickstart it (re)spawns the process without a
        // double-bootstrap error. Otherwise bootstrap loads and starts it.
        if self.state()? == ServiceState::NotInstalled || !self.is_loaded()? {
            self.launchctl("bootstrap", &["bootstrap", &domain, path_str(&path)?])
        } else {
            let target = self.service_target()?;
            self.launchctl("kickstart", &["kickstart", &target])
        }
    }

    /// Boots the service out of its domain, stopping the process. The plist stays
    /// on disk so the service remains installed and can be started again.
    /// Tolerates an already-stopped service.
    fn stop(&self) -> Result<(), ServiceError> {
        let path = self.plist_path()?;
        let domain = self.domain_target()?;
        let _ = self.launchctl("bootout", &["bootout", &domain, path_str(&path)?]);
        Ok(())
    }

    /// Reports the OS service state. The plist file's presence decides installed
    /// vs not-installed (a stopped service still has its plist on disk); when
    /// installed, `launchctl print` decides running (a live pid) vs stopped.
    fn state(&self) -> Result<ServiceState, ServiceError> {
        if !self.plist_path()?.exists() {
            return Ok(ServiceState::NotInstalled);
        }
        let target = self.service_target()?;
        let output = Command::new("launchctl")
            .args(["print", &target])
            .output()
            .map_err(|source| ServiceError::ToolSpawn {
                tool: "launchctl",
                source,
            })?;
        if !output.status.success() {
            // The plist exists but the job is not bootstrapped: installed but
            // stopped.
            return Ok(ServiceState::Stopped);
        }
        let text = String::from_utf8_lossy(&output.stdout);
        Ok(parse_print_state(&text))
    }

    /// Tails the plist's stdout/stderr log files. launchd has no per-service
    /// journal, so `verglas logs` follows the same files the plist redirects to.
    fn log_command(&self, follow: bool, lines: usize) -> Vec<String> {
        // Resolved by the caller from the spec; the command tails both the
        // stdout and stderr log so the operator sees everything the daemon
        // emitted, the way `verglas dev` interleaves them.
        let mut cmd = vec!["tail".to_owned()];
        if follow {
            cmd.push("-f".to_owned());
        }
        cmd.push("-n".to_owned());
        cmd.push(lines.to_string());
        cmd
    }
}

/// The user's home directory, from `$HOME`. The LaunchAgents path is rooted
/// here; failing to resolve it is an actionable error rather than a panic.
fn home_dir() -> Result<PathBuf, ServiceError> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| ServiceError::NeedsPrivilege("HOME is not set".to_owned()))
}

/// Renders a path as a `&str`, erroring on non-UTF-8 rather than lossily
/// mangling a path launchctl must receive verbatim.
fn path_str(path: &Path) -> Result<&str, ServiceError> {
    path.to_str().ok_or_else(|| ServiceError::File {
        path: path.to_path_buf(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, "path is not valid UTF-8"),
    })
}

/// Parses `launchctl print` output into a [`ServiceState`]. A printed record
/// carrying a live `pid = N` is running; a record without one (or with
/// `state = not running`) is stopped. Pure so it is unit-tested against captured
/// fixtures.
fn parse_print_state(text: &str) -> ServiceState {
    // launchctl print includes a `pid = <n>` line only while the job has a live
    // process. Its absence (the job is loaded but idle) means stopped.
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("pid = ")
            && rest.trim().parse::<u32>().is_ok()
        {
            return ServiceState::Running;
        }
    }
    ServiceState::Stopped
}

/// Escapes a string for an XML text node (plist values are XML): the five
/// predefined entities. launchd plists carry filesystem paths and env values, so
/// `&`, `<`, `>`, and quotes must be escaped or the plist fails to parse.
fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

/// Renders the LaunchAgent/LaunchDaemon plist for `spec` under `label`. Pure so
/// the exact XML is golden-tested.
///
/// The job runs `verglasd --config <path>` in the foreground (`RunAtLoad` true,
/// `KeepAlive` true so launchd restarts a crash), redirects stdout/stderr to the
/// state-dir log files `verglas logs` tails, sets the working directory to the
/// state dir, and carries the backend AWS credential-chain env in
/// `EnvironmentVariables` (a boot-started service inherits none of the shell that
/// ran `verglas init`). macOS has no cgroup budget knobs, so the memory/CPU/IO
/// ceilings in the spec are not rendered here — they are documented as
/// systemd-only.
pub fn render_plist(label: &str, spec: &ServiceSpec) -> String {
    let mut env_block = String::new();
    if !spec.environment.is_empty() {
        env_block.push_str("    <key>EnvironmentVariables</key>\n    <dict>\n");
        for (k, v) in &spec.environment {
            env_block.push_str(&format!(
                "        <key>{}</key>\n        <string>{}</string>\n",
                xml_escape(k),
                xml_escape(v),
            ));
        }
        env_block.push_str("    </dict>\n");
    }
    // The cache engine opens many region files; launchd's default open-files
    // ceiling (256) is far too low, so raise both the soft and hard limit or the
    // daemon fails to build its cache with "too many open files".
    let mut limit_block = String::new();
    if let Some(limit) = spec.open_files_limit {
        limit_block.push_str(&format!(
            "    <key>SoftResourceLimits</key>\n    <dict>\n        \
             <key>NumberOfFiles</key>\n        <integer>{limit}</integer>\n    </dict>\n    \
             <key>HardResourceLimits</key>\n    <dict>\n        \
             <key>NumberOfFiles</key>\n        <integer>{limit}</integer>\n    </dict>\n",
        ));
    }
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
<plist version=\"1.0\">\n\
<dict>\n\
    <key>Label</key>\n\
    <string>{label}</string>\n\
    <key>ProgramArguments</key>\n\
    <array>\n\
        <string>{program}</string>\n\
        <string>--config</string>\n\
        <string>{config}</string>\n\
    </array>\n\
    <key>RunAtLoad</key>\n\
    <true/>\n\
    <key>KeepAlive</key>\n\
    <true/>\n\
    <key>WorkingDirectory</key>\n\
    <string>{workdir}</string>\n\
    <key>StandardOutPath</key>\n\
    <string>{stdout}</string>\n\
    <key>StandardErrorPath</key>\n\
    <string>{stderr}</string>\n\
{limit_block}\
{env_block}\
</dict>\n\
</plist>\n",
        label = xml_escape(label),
        program = xml_escape(&spec.program.display().to_string()),
        config = xml_escape(&spec.config_path.display().to_string()),
        workdir = xml_escape(&spec.state_dir.display().to_string()),
        stdout = xml_escape(&spec.stdout_log.display().to_string()),
        stderr = xml_escape(&spec.stderr_log.display().to_string()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// A spec pointed at fixed paths with two env vars, so the rendered plist is
    /// deterministic and golden-testable.
    fn sample_spec() -> ServiceSpec {
        let mut env = BTreeMap::new();
        env.insert("AWS_ACCESS_KEY_ID".to_owned(), "AKIA<EXAMPLE>".to_owned());
        env.insert(
            "AWS_ENDPOINT".to_owned(),
            "https://ns.compat.objectstorage.example.com".to_owned(),
        );
        ServiceSpec {
            program: PathBuf::from("/usr/local/bin/verglasd"),
            config_path: PathBuf::from("/Users/op/.verglas/verglas.toml"),
            state_dir: PathBuf::from("/Users/op/.verglas"),
            stdout_log: PathBuf::from("/Users/op/.verglas/logs/verglasd.out.log"),
            stderr_log: PathBuf::from("/Users/op/.verglas/logs/verglasd.err.log"),
            environment: env,
            memory_max_bytes: Some(4 * 1024 * 1024 * 1024),
            cpu_weight: Some(100),
            io_weight: Some(100),
            open_files_limit: Some(65536),
        }
    }

    #[test]
    fn plist_raises_the_open_files_limit() {
        // launchd's default open-files ceiling is too low for the cache engine;
        // the plist must raise the soft and hard NumberOfFiles limit.
        let plist = render_plist(SERVICE_LABEL, &sample_spec());
        assert!(
            plist.contains("<key>SoftResourceLimits</key>"),
            "soft: {plist}"
        );
        assert!(
            plist.contains("<key>HardResourceLimits</key>"),
            "hard: {plist}"
        );
        assert!(
            plist.contains("<key>NumberOfFiles</key>"),
            "nofile: {plist}"
        );
        assert!(plist.contains("<integer>65536</integer>"), "value: {plist}");
    }

    #[test]
    fn plist_runs_verglasd_foreground_with_config_and_restart() {
        // The job must run `verglasd --config <path>` (foreground, never
        // self-daemonizing), start at load, and be kept alive so launchd
        // restarts a crash.
        let plist = render_plist(SERVICE_LABEL, &sample_spec());
        assert!(plist.contains("<string>/usr/local/bin/verglasd</string>"));
        assert!(plist.contains("<string>--config</string>"));
        assert!(plist.contains("<string>/Users/op/.verglas/verglas.toml</string>"));
        assert!(plist.contains("<key>RunAtLoad</key>"));
        assert!(plist.contains("<key>KeepAlive</key>"));
        assert!(plist.contains(&format!("<string>{SERVICE_LABEL}</string>")));
    }

    #[test]
    fn plist_points_logs_at_state_dir_files() {
        // StandardOutPath/StandardErrorPath must point at the state-dir log files
        // `verglas logs` tails.
        let plist = render_plist(SERVICE_LABEL, &sample_spec());
        assert!(plist.contains("<key>StandardOutPath</key>"));
        assert!(plist.contains("<string>/Users/op/.verglas/logs/verglasd.out.log</string>"));
        assert!(plist.contains("<key>StandardErrorPath</key>"));
        assert!(plist.contains("<string>/Users/op/.verglas/logs/verglasd.err.log</string>"));
        assert!(plist.contains("<key>WorkingDirectory</key>"));
    }

    #[test]
    fn plist_carries_backend_env_credentials() {
        // A boot-started service inherits nothing from the init shell, so the
        // AWS credential-chain env must be baked into EnvironmentVariables.
        let plist = render_plist(SERVICE_LABEL, &sample_spec());
        assert!(plist.contains("<key>EnvironmentVariables</key>"));
        assert!(plist.contains("<key>AWS_ACCESS_KEY_ID</key>"));
        assert!(plist.contains("<key>AWS_ENDPOINT</key>"));
        assert!(plist.contains("<string>https://ns.compat.objectstorage.example.com</string>"));
    }

    #[test]
    fn plist_escapes_xml_special_characters() {
        // A path or env value with `&` or `<` must be XML-escaped or the plist
        // fails to parse.
        let mut env = BTreeMap::new();
        env.insert("X".to_owned(), "a&b<c>".to_owned());
        let spec = ServiceSpec {
            program: PathBuf::from("/bin/verglasd"),
            config_path: PathBuf::from("/tmp/c.toml"),
            state_dir: PathBuf::from("/tmp"),
            stdout_log: PathBuf::from("/tmp/o.log"),
            stderr_log: PathBuf::from("/tmp/e.log"),
            environment: env,
            memory_max_bytes: None,
            cpu_weight: None,
            io_weight: None,
            open_files_limit: None,
        };
        let plist = render_plist(SERVICE_LABEL, &spec);
        assert!(plist.contains("a&amp;b&lt;c&gt;"), "escapes value: {plist}");
        assert!(!plist.contains("a&b<c>"));
    }

    #[test]
    fn plist_omits_env_block_when_no_environment() {
        // With no env vars the EnvironmentVariables dict is omitted entirely, not
        // rendered empty.
        let spec = ServiceSpec {
            program: PathBuf::from("/bin/verglasd"),
            config_path: PathBuf::from("/tmp/c.toml"),
            state_dir: PathBuf::from("/tmp"),
            stdout_log: PathBuf::from("/tmp/o.log"),
            stderr_log: PathBuf::from("/tmp/e.log"),
            environment: BTreeMap::new(),
            memory_max_bytes: None,
            cpu_weight: None,
            io_weight: None,
            open_files_limit: None,
        };
        let plist = render_plist(SERVICE_LABEL, &spec);
        assert!(!plist.contains("EnvironmentVariables"));
    }

    #[test]
    fn user_scope_targets_the_gui_domain_and_launchagents_path() {
        // A user install writes to ~/Library/LaunchAgents and addresses the GUI
        // domain; the system scope writes to /Library/LaunchDaemons.
        let user = LaunchdManager::new(Scope::User);
        let path = user.plist_path().expect("home resolves");
        assert!(
            path.to_string_lossy().contains("Library/LaunchAgents"),
            "user plist under LaunchAgents: {}",
            path.display()
        );
        assert!(user.domain_target().expect("uid").starts_with("gui/"));

        let system = LaunchdManager::new(Scope::System);
        let sys_path = system.plist_path().expect("system path");
        assert_eq!(
            sys_path,
            PathBuf::from(format!("/Library/LaunchDaemons/{SERVICE_LABEL}.plist"))
        );
        assert_eq!(system.domain_target().expect("system"), "system");
    }

    #[test]
    fn print_state_running_when_a_live_pid_is_present() {
        // A `launchctl print` record with a live pid means the job is running.
        let running =
            "org.verglas.verglas = {\n\tactive count = 1\n\tpid = 4242\n\tstate = running\n}\n";
        assert_eq!(parse_print_state(running), ServiceState::Running);
    }

    #[test]
    fn print_state_stopped_when_no_pid_line() {
        // A loaded-but-idle job has no pid line; that is stopped, not running.
        let stopped = "org.verglas.verglas = {\n\tactive count = 0\n\tstate = not running\n}\n";
        assert_eq!(parse_print_state(stopped), ServiceState::Stopped);
    }

    #[test]
    fn log_command_tails_and_optionally_follows() {
        let mgr = LaunchdManager::new(Scope::User);
        let follow = mgr.log_command(true, 50);
        assert_eq!(follow.first().map(String::as_str), Some("tail"));
        assert!(follow.contains(&"-f".to_owned()));
        assert!(follow.contains(&"50".to_owned()));
        let no_follow = mgr.log_command(false, 20);
        assert!(!no_follow.contains(&"-f".to_owned()));
    }
}
