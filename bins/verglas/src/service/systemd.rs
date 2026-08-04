//! The Linux systemd implementation of [`ServiceManager`] (#216).
//!
//! Per-user scope writes a unit under `~/.config/systemd/user` and drives
//! `systemctl --user` (no sudo). System scope writes under
//! `/etc/systemd/system` and drives system `systemctl` (needs root). We render
//! the unit ourselves. The cgroup budget knobs (`MemoryMax`, `CPUWeight`,
//! `IOWeight`) come from config so a colocated node stays a polite tenant.
//! `verglas logs` reads the unit's journal via `journalctl`.

use std::path::PathBuf;
use std::process::Command;

use super::{Scope, ServiceError, ServiceManager, ServiceSpec, ServiceState, check_tool_output};

/// The systemd unit name. `verglasd.service` under the user or system unit dir.
pub const UNIT_NAME: &str = "verglasd.service";

/// The Linux service manager. Holds the scope so every `systemctl` call carries
/// `--user` (user scope) or runs against the system manager.
#[derive(Debug, Clone)]
pub struct SystemdManager {
    /// The install scope this manager operates in.
    scope: Scope,
}

impl SystemdManager {
    /// Builds a manager for `scope`.
    pub fn new(scope: Scope) -> Self {
        Self { scope }
    }

    /// The unit-file path for this scope: `~/.config/systemd/user/verglasd.service`
    /// for a user unit, `/etc/systemd/system/verglasd.service` for a system unit.
    pub fn unit_path(&self) -> Result<PathBuf, ServiceError> {
        let dir = match self.scope {
            Scope::User => home_dir()?.join(".config").join("systemd").join("user"),
            Scope::System => PathBuf::from("/etc/systemd/system"),
        };
        Ok(dir.join(UNIT_NAME))
    }

    /// Whether this scope's `systemctl` calls carry `--user`.
    fn is_user(&self) -> bool {
        matches!(self.scope, Scope::User)
    }

    /// Runs `systemctl` (with `--user` in user scope) plus `args`, mapping a
    /// spawn failure or non-zero exit to a [`ServiceError`] naming `verb`.
    fn systemctl(&self, verb: &'static str, args: &[&str]) -> Result<(), ServiceError> {
        let output = self.systemctl_output(args)?;
        check_tool_output("systemctl", verb, &output)
    }

    /// Runs `systemctl` capturing its output, without interpreting the exit code
    /// (callers that read `systemctl show` want the output even on a non-zero
    /// exit).
    fn systemctl_output(&self, args: &[&str]) -> Result<std::process::Output, ServiceError> {
        let mut cmd = Command::new("systemctl");
        if self.is_user() {
            cmd.arg("--user");
        }
        cmd.args(args);
        cmd.output().map_err(|source| ServiceError::ToolSpawn {
            tool: "systemctl",
            source,
        })
    }
}

impl ServiceManager for SystemdManager {
    /// Renders the unit, writes it (creating the unit dir if needed), reloads the
    /// systemd manager, and enables the unit so it boot-starts. Idempotent: a
    /// re-run overwrites the unit and re-enables cleanly.
    fn install(&self, spec: &ServiceSpec) -> Result<(), ServiceError> {
        let unit = render_unit(spec, self.is_user());
        let path = self.unit_path()?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| ServiceError::File {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        std::fs::write(&path, unit).map_err(|source| ServiceError::File {
            path: path.clone(),
            source,
        })?;
        self.systemctl("daemon-reload", &["daemon-reload"])?;
        self.systemctl("enable", &["enable", UNIT_NAME])
    }

    /// Starts the unit. The unit is `Type=exec`, so `systemctl start` returns
    /// once the process has been executed; the caller (`verglas start`) then
    /// polls `/admin/healthz` to block until the cache is actually serving (the
    /// #16 gate). We poll rather than implement `sd_notify` so the same readiness
    /// path works on launchd, which has no notify protocol.
    fn start(&self) -> Result<(), ServiceError> {
        self.systemctl("start", &["start", UNIT_NAME])
    }

    /// Stops the unit.
    fn stop(&self) -> Result<(), ServiceError> {
        self.systemctl("stop", &["stop", UNIT_NAME])
    }

    /// Reports the OS service state from `systemctl show` `ActiveState` and
    /// `LoadState`: `active` is running, a loaded-but-inactive unit is stopped,
    /// and a not-found unit is not installed.
    fn state(&self) -> Result<ServiceState, ServiceError> {
        let output = self.systemctl_output(&[
            "show",
            UNIT_NAME,
            "--property=ActiveState",
            "--property=LoadState",
        ])?;
        let text = String::from_utf8_lossy(&output.stdout);
        Ok(parse_show_state(&text))
    }

    /// Follows the unit's journal via `journalctl` (`--user` in user scope).
    fn log_command(&self, follow: bool, lines: usize) -> Vec<String> {
        let mut cmd = vec!["journalctl".to_owned()];
        if self.is_user() {
            cmd.push("--user".to_owned());
        }
        cmd.push("-u".to_owned());
        cmd.push(UNIT_NAME.to_owned());
        cmd.push("-n".to_owned());
        cmd.push(lines.to_string());
        if follow {
            cmd.push("-f".to_owned());
        }
        cmd
    }
}

/// The user's home directory from `$HOME`.
fn home_dir() -> Result<PathBuf, ServiceError> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| ServiceError::NeedsPrivilege("HOME is not set".to_owned()))
}

/// Parses `systemctl show` `ActiveState`/`LoadState` output into a
/// [`ServiceState`]. Pure so it is unit-tested against captured fixtures.
///
/// `LoadState=not-found` means no unit is installed. Otherwise `ActiveState`
/// decides: `active`/`activating`/`reloading` are running, anything else
/// (inactive, failed, deactivating) is stopped.
fn parse_show_state(text: &str) -> ServiceState {
    let mut active = "";
    let mut load = "";
    for line in text.lines() {
        if let Some(v) = line.trim().strip_prefix("ActiveState=") {
            active = v.trim();
        } else if let Some(v) = line.trim().strip_prefix("LoadState=") {
            load = v.trim();
        }
    }
    if load == "not-found" || load.is_empty() {
        return ServiceState::NotInstalled;
    }
    match active {
        "active" | "activating" | "reloading" => ServiceState::Running,
        _ => ServiceState::Stopped,
    }
}

/// Renders the systemd unit for `spec`. Pure so the exact text is golden-tested.
///
/// The unit runs `verglasd --config <path>` in the foreground (`Type=exec`;
/// readiness is enforced by the caller polling `/admin/healthz`, not `sd_notify`,
/// so the same path works on launchd), restarts on failure, raises the
/// open-files limit for the cache engine, carries the backend AWS
/// credential-chain env, and sets the cgroup budget knobs (`MemoryMax`,
/// `CPUWeight`, `IOWeight`) from config so a colocated node is a provably polite
/// tenant. A user unit installs into `default.target`; a system unit into
/// `multi-user.target`.
pub fn render_unit(spec: &ServiceSpec, is_user: bool) -> String {
    let wanted_by = if is_user {
        "default.target"
    } else {
        "multi-user.target"
    };
    let mut resource = String::new();
    if let Some(mem) = spec.memory_max_bytes {
        resource.push_str(&format!("MemoryMax={mem}\n"));
    }
    if let Some(cpu) = spec.cpu_weight {
        resource.push_str(&format!("CPUWeight={cpu}\n"));
    }
    if let Some(io) = spec.io_weight {
        resource.push_str(&format!("IOWeight={io}\n"));
    }
    if let Some(nofile) = spec.open_files_limit {
        resource.push_str(&format!("LimitNOFILE={nofile}\n"));
    }
    let mut env_lines = String::new();
    for (k, v) in &spec.environment {
        // systemd Environment= takes a quoted value so spaces and specials are
        // safe; a literal `"` in a value is escaped.
        let escaped = v.replace('\\', "\\\\").replace('"', "\\\"");
        env_lines.push_str(&format!("Environment=\"{k}={escaped}\"\n"));
    }
    format!(
        "[Unit]\n\
         Description=Verglas cache daemon (verglasd)\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=exec\n\
         ExecStart={program} --config {config}\n\
         WorkingDirectory={workdir}\n\
         Restart=on-failure\n\
         RestartSec=2\n\
         {env_lines}\
         {resource}\
         \n\
         [Install]\n\
         WantedBy={wanted_by}\n",
        program = spec.program.display(),
        config = spec.config_path.display(),
        workdir = spec.state_dir.display(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// A spec with env and cgroup budgets set, so the unit renders every field.
    fn sample_spec() -> ServiceSpec {
        let mut env = BTreeMap::new();
        env.insert("AWS_ACCESS_KEY_ID".to_owned(), "AKIA<EXAMPLE>".to_owned());
        env.insert(
            "AWS_ENDPOINT".to_owned(),
            "https://ns.compat.example.com".to_owned(),
        );
        ServiceSpec {
            program: PathBuf::from("/usr/local/bin/verglasd"),
            config_path: PathBuf::from("/home/op/.verglas/verglas.toml"),
            state_dir: PathBuf::from("/home/op/.verglas"),
            stdout_log: PathBuf::from("/home/op/.verglas/logs/verglasd.out.log"),
            stderr_log: PathBuf::from("/home/op/.verglas/logs/verglasd.err.log"),
            environment: env,
            memory_max_bytes: Some(4 * 1024 * 1024 * 1024),
            cpu_weight: Some(100),
            io_weight: Some(120),
            open_files_limit: Some(65536),
        }
    }

    #[test]
    fn unit_runs_verglasd_foreground_with_exec_readiness() {
        // The unit runs `verglasd --config <path>` as Type=exec. Readiness is the
        // caller polling /admin/healthz (the #16 gate), not sd_notify, so the
        // same path works on launchd.
        let unit = render_unit(&sample_spec(), true);
        assert!(
            unit.contains(
                "ExecStart=/usr/local/bin/verglasd --config /home/op/.verglas/verglas.toml"
            )
        );
        assert!(unit.contains("Type=exec"));
        assert!(
            !unit.contains("Type=notify"),
            "must not claim notify: {unit}"
        );
        assert!(unit.contains("Restart=on-failure"));
    }

    #[test]
    fn unit_raises_the_open_files_limit() {
        // The cache engine opens many region files; the unit must raise
        // LimitNOFILE or the daemon fails to build its cache.
        let unit = render_unit(&sample_spec(), true);
        assert!(unit.contains("LimitNOFILE=65536"), "nofile: {unit}");
    }

    #[test]
    fn unit_sets_cgroup_budget_knobs_from_config() {
        // The whitepaper's colocated mode relies on the cgroup ceilings; the
        // unit must set MemoryMax/CPUWeight/IOWeight from config.
        let unit = render_unit(&sample_spec(), true);
        assert!(unit.contains("MemoryMax=4294967296"), "memory: {unit}");
        assert!(unit.contains("CPUWeight=100"), "cpu: {unit}");
        assert!(unit.contains("IOWeight=120"), "io: {unit}");
    }

    #[test]
    fn unit_carries_backend_env_credentials() {
        // A boot-started unit inherits no shell env, so the AWS credential-chain
        // env is baked into Environment= lines.
        let unit = render_unit(&sample_spec(), true);
        assert!(unit.contains("Environment=\"AWS_ACCESS_KEY_ID=AKIA<EXAMPLE>\""));
        assert!(unit.contains("Environment=\"AWS_ENDPOINT=https://ns.compat.example.com\""));
    }

    #[test]
    fn user_and_system_units_target_the_right_install_target() {
        // A user unit installs into default.target; a system unit into
        // multi-user.target.
        assert!(render_unit(&sample_spec(), true).contains("WantedBy=default.target"));
        assert!(render_unit(&sample_spec(), false).contains("WantedBy=multi-user.target"));
    }

    #[test]
    fn unit_omits_unset_cgroup_knobs() {
        // An install with no budgets configured renders no MemoryMax line rather
        // than a bogus `MemoryMax=` empty value.
        let mut spec = sample_spec();
        spec.memory_max_bytes = None;
        spec.cpu_weight = None;
        spec.io_weight = None;
        spec.open_files_limit = None;
        let unit = render_unit(&spec, true);
        assert!(!unit.contains("MemoryMax="));
        assert!(!unit.contains("CPUWeight="));
        assert!(!unit.contains("IOWeight="));
        assert!(!unit.contains("LimitNOFILE="));
    }

    #[test]
    fn user_scope_targets_the_user_unit_dir() {
        // A user install writes under ~/.config/systemd/user; system under
        // /etc/systemd/system.
        let user = SystemdManager::new(Scope::User);
        let path = user.unit_path().expect("home resolves");
        assert!(
            path.to_string_lossy().contains(".config/systemd/user"),
            "user unit path: {}",
            path.display()
        );
        let system = SystemdManager::new(Scope::System);
        assert_eq!(
            system.unit_path().expect("system"),
            PathBuf::from(format!("/etc/systemd/system/{UNIT_NAME}"))
        );
    }

    #[test]
    fn show_state_running_active_stopped_inactive_absent_notfound() {
        // ActiveState/LoadState map to the three coarse states.
        assert_eq!(
            parse_show_state("ActiveState=active\nLoadState=loaded\n"),
            ServiceState::Running
        );
        assert_eq!(
            parse_show_state("ActiveState=inactive\nLoadState=loaded\n"),
            ServiceState::Stopped
        );
        assert_eq!(
            parse_show_state("ActiveState=inactive\nLoadState=not-found\n"),
            ServiceState::NotInstalled
        );
        assert_eq!(
            parse_show_state("ActiveState=failed\nLoadState=loaded\n"),
            ServiceState::Stopped
        );
    }

    #[test]
    fn log_command_uses_journalctl_with_user_flag_in_user_scope() {
        let user = SystemdManager::new(Scope::User);
        let cmd = user.log_command(true, 100);
        assert_eq!(cmd.first().map(String::as_str), Some("journalctl"));
        assert!(cmd.contains(&"--user".to_owned()));
        assert!(cmd.contains(&"-u".to_owned()));
        assert!(cmd.contains(&UNIT_NAME.to_owned()));
        assert!(cmd.contains(&"-f".to_owned()));

        let system = SystemdManager::new(Scope::System);
        let sys_cmd = system.log_command(false, 100);
        assert!(!sys_cmd.contains(&"--user".to_owned()));
        assert!(!sys_cmd.contains(&"-f".to_owned()));
    }
}
