//! The internal per-OS service layer the lifecycle commands drive (#216).
//!
//! `verglas init/start/stop/restart/status/logs` install and run
//! `verglasd` as a managed OS service. Each platform is a thin implementation of
//! the [`ServiceManager`] trait behind a `#[cfg(target_os)]` gate: it renders a
//! text service definition (a launchd plist or a systemd unit) and shells out to
//! the platform's own control tool. We own this layer rather than depend on a
//! general cross-platform service crate.
//!
//! The daemon (`verglasd`) stays foreground. The OS service manager owns
//! backgrounding, restart, and boot-start. Nothing here self-daemonizes.

use std::collections::BTreeMap;
use std::path::PathBuf;

#[cfg(target_os = "macos")]
pub mod launchd;
#[cfg(target_os = "linux")]
pub mod systemd;
// Platforms without a native backend yet (Windows and anything that is neither
// macOS nor Linux) get a stub manager whose every operation returns a clear
// "not supported yet; run verglasd in the foreground" error.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub mod unsupported;

/// Which service scope an install targets. Per-user is the default: it needs no
/// sudo (a LaunchAgent in the user's home, or a `systemctl --user` unit). System
/// scope is the production-node choice: a LaunchDaemon or a system systemd unit,
/// installed under a root-owned path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// Per-user: no privilege needed. LaunchAgent / `systemctl --user`.
    User,
    /// System-wide: needs root. LaunchDaemon / system systemd unit.
    System,
}

impl Scope {
    /// Resolves the scope from the `--system` flag: set means [`Scope::System`],
    /// absent means the no-sudo default [`Scope::User`].
    pub fn from_system_flag(system: bool) -> Self {
        if system { Scope::System } else { Scope::User }
    }

    /// A short human label used in operator messages.
    pub fn label(self) -> &'static str {
        match self {
            Scope::User => "user",
            Scope::System => "system",
        }
    }
}

/// Everything the platform renderers need to write a service definition. Built
/// once by `verglas init` from the resolved config and passed to the platform
/// [`ServiceManager`]. Kept platform-neutral so the same spec drives launchd and
/// systemd.
#[derive(Debug, Clone)]
pub struct ServiceSpec {
    /// Absolute path to the `verglasd` binary the service runs.
    pub program: PathBuf,
    /// Absolute path to the daemon config file the service passes as `--config`.
    pub config_path: PathBuf,
    /// The cache/state directory. The service's log files live under it so
    /// `verglas logs` can find them, and it is the daemon's working directory.
    pub state_dir: PathBuf,
    /// Absolute path of the file the service's stdout is redirected to. Used
    /// by the launchd backend, which names explicit log files; the systemd
    /// backend logs to journald and reads these through `journalctl` instead,
    /// so the fields are unread on Linux.
    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    pub stdout_log: PathBuf,
    /// Absolute path of the file the service's stderr is redirected to. See
    /// [`ServiceSpec::stdout_log`] for the per-platform note.
    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    pub stderr_log: PathBuf,
    /// Environment variables baked into the service definition. These carry the
    /// AWS credential-chain env (`AWS_ACCESS_KEY_ID`, `AWS_ENDPOINT`, ...) the
    /// backend needs, since a boot-started service inherits none of the shell
    /// that ran `verglas init`. Ordered so the rendered file is deterministic.
    pub environment: BTreeMap<String, String>,
    /// Hard memory ceiling in bytes for the service cgroup (systemd `MemoryMax`),
    /// or `None` to leave it unset. macOS launchd has no equivalent knob, so the
    /// launchd renderer ignores this field — hence the allow, which keeps the
    /// field on the platform-neutral spec without a macOS dead-code warning.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub memory_max_bytes: Option<u64>,
    /// CPU weight for the service cgroup (systemd `CPUWeight`, 1..=10000), or
    /// `None` to leave it unset. Systemd-only; see `memory_max_bytes`.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub cpu_weight: Option<u32>,
    /// IO weight for the service cgroup (systemd `IOWeight`, 1..=10000), or
    /// `None` to leave it unset. Systemd-only; see `memory_max_bytes`.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub io_weight: Option<u32>,
    /// Open-files ceiling (`RLIMIT_NOFILE`) the service runs under. The cache
    /// engine (foyer) opens many region files, and a service inherits the
    /// supervisor's low default (256 under launchd), so the service definition
    /// must raise it or the daemon fails to build its cache with "too many open
    /// files". `None` leaves the platform default. Renders as
    /// `SoftResourceLimits`/`HardResourceLimits` `NumberOfFiles` on launchd and
    /// `LimitNOFILE` on systemd.
    pub open_files_limit: Option<u64>,
}

/// The combined lifecycle state `verglas status` reports: the OS service state
/// merged with the daemon's own health and cache-warmth self-report. Built by
/// [`aggregate_status`] from the two halves so the aggregation is unit-tested.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusReport {
    /// Whether the OS service manager considers the service loaded/running.
    pub service_state: ServiceState,
    /// The running daemon's version (name and release, e.g. `verglasd 0.1.0`)
    /// from `/admin/version`, `None` when the daemon could not be reached. The
    /// daemon version lives here since #288 removed the `verglas version`
    /// subcommand.
    pub version: Option<String>,
    /// The daemon's `/admin/healthz` self-report, `None` when it could not be
    /// reached (the service is stopped, or still binding).
    pub health: Option<String>,
    /// A one-line cache-warmth summary derived from `/admin/stats`, `None` when
    /// stats could not be read.
    pub warmth: Option<String>,
    /// The S3 endpoint URL the installed daemon serves on.
    pub endpoint: String,
}

/// What the OS service manager reports about the service. Deliberately coarse:
/// the fine-grained health comes from the daemon's own `/admin/healthz`, not the
/// supervisor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceState {
    /// The service is loaded and its process is running.
    Running,
    /// The service is installed but not currently running.
    Stopped,
    /// No service is installed for this scope.
    NotInstalled,
}

impl ServiceState {
    /// A short human label for the state line.
    pub fn label(self) -> &'static str {
        match self {
            ServiceState::Running => "running",
            ServiceState::Stopped => "stopped",
            ServiceState::NotInstalled => "not installed",
        }
    }
}

/// Errors any platform service operation can surface. Each renders one
/// actionable operator-facing line.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// The platform control tool (launchctl / systemctl) could not be run.
    #[error("failed to run {tool}: {source}")]
    ToolSpawn {
        /// The control tool that could not be spawned.
        tool: &'static str,
        /// The underlying spawn error.
        source: std::io::Error,
    },
    /// The platform control tool ran but returned a failure status.
    #[error("{tool} {verb} failed (exit {code}): {stderr}")]
    ToolFailed {
        /// The control tool that failed.
        tool: &'static str,
        /// The high-level operation attempted (e.g. `bootstrap`).
        verb: &'static str,
        /// The tool's exit code, or -1 if it was killed by a signal.
        code: i32,
        /// The tool's captured stderr, trimmed.
        stderr: String,
    },
    /// A file the service layer had to write or read failed.
    #[error("service file operation failed for {path}: {source}")]
    File {
        /// The path at fault.
        path: PathBuf,
        /// The underlying IO error.
        source: std::io::Error,
    },
    /// This scope needs privileges the caller does not have.
    #[error("{0}")]
    NeedsPrivilege(String),
    /// This platform has no OS service backend yet (e.g. Windows). The daemon
    /// still runs in the foreground; only the managed-service install is absent.
    /// Constructed only by the non-macOS/Linux stub backend, so it reads as dead
    /// code on the platforms that have a real backend.
    #[cfg_attr(any(target_os = "macos", target_os = "linux"), allow(dead_code))]
    #[error("{0}")]
    Unsupported(String),
}

/// The control surface every platform implements. `verglas init` installs;
/// start/stop/restart drive the running service; status reports its OS state;
/// logs locate the platform log. Removal is manual and documented (README,
/// "Removing Verglas") — there is no removal operation here (#288).
///
/// Implementations shell out to the platform tool and never self-daemonize.
pub trait ServiceManager {
    /// Writes the service definition and registers it with the OS supervisor,
    /// without starting it. Idempotent: re-running overwrites the definition and
    /// re-registers cleanly.
    fn install(&self, spec: &ServiceSpec) -> Result<(), ServiceError>;

    /// Starts the installed service. Idempotent: starting a running service is
    /// not an error.
    fn start(&self) -> Result<(), ServiceError>;

    /// Stops the running service. Idempotent: stopping a stopped service is not
    /// an error.
    fn stop(&self) -> Result<(), ServiceError>;

    /// Reports the OS service state (running / stopped / not installed).
    fn state(&self) -> Result<ServiceState, ServiceError>;

    /// Returns the platform log-tail command as a program and its arguments, so
    /// the caller can exec it (launchd: `tail -f` the plist's log files;
    /// systemd: `journalctl --user -f -u <unit>`).
    fn log_command(&self, follow: bool, lines: usize) -> Vec<String>;
}

/// Merges the OS service state with the daemon's health and cache-warmth
/// self-report into the single [`StatusReport`] `verglas status` prints. Pure so
/// the aggregation logic is unit-tested without a running daemon: the caller
/// fetches `/admin/healthz` and `/admin/stats` and passes what it got (or `None`
/// when the daemon could not be reached).
pub fn aggregate_status(
    service_state: ServiceState,
    endpoint: String,
    version: Option<String>,
    health: Option<String>,
    warmth: Option<String>,
) -> StatusReport {
    StatusReport {
        service_state,
        version,
        health,
        warmth,
        endpoint,
    }
}

/// Renders a one-line cache-warmth summary from the daemon's `/admin/stats`
/// DRAM occupancy and read-path hit counters. Pure so it is asserted on
/// directly. Reports the live DRAM working set and the DRAM/disk hit split — the
/// coarse "is the cache warm" signal an operator wants from `verglas status`.
pub fn render_warmth(stats: &verglas_core::admin::StatsInfo) -> String {
    let c = &stats.counters;
    let warm_hits = c.dram_hits + c.disk_hits;
    let total = warm_hits + c.dram_misses.max(c.disk_misses);
    let hit_pct = if total == 0 {
        0.0
    } else {
        (warm_hits as f64 / total as f64) * 100.0
    };
    format!(
        "dram {} live / {} ceiling, {} dram hits, {} disk hits ({hit_pct:.0}% warm)",
        human_bytes(stats.dram_live_bytes),
        human_bytes(stats.cache.dram_bytes),
        c.dram_hits,
        c.disk_hits,
    )
}

/// Formats a byte count as a short human string (`1.5GB`, `20MB`). Binary
/// multiples, matching the config's `ByteSize` parsing. Pure helper for the
/// warmth and status lines.
fn human_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;
    let (value, unit) = if bytes >= TB {
        (bytes as f64 / TB as f64, "TB")
    } else if bytes >= GB {
        (bytes as f64 / GB as f64, "GB")
    } else if bytes >= MB {
        (bytes as f64 / MB as f64, "MB")
    } else if bytes >= KB {
        (bytes as f64 / KB as f64, "KB")
    } else {
        return format!("{bytes}B");
    };
    // One decimal place, trimming a trailing `.0` for round values.
    let s = format!("{value:.1}");
    let s = s.strip_suffix(".0").unwrap_or(&s);
    format!("{s}{unit}")
}

/// Renders the human `verglas status` block from a [`StatusReport`]. Pure so the
/// output shape is golden-tested. Names the OS service state, the daemon health
/// (or that it is unreachable), the cache warmth, and the endpoint.
pub fn render_status(report: &StatusReport) -> String {
    let version = match &report.version {
        Some(v) => v.as_str(),
        None => "unreachable",
    };
    let health = match &report.health {
        Some(h) => h.as_str(),
        None => "unreachable",
    };
    let warmth = match &report.warmth {
        Some(w) => w.as_str(),
        None => "unavailable",
    };
    format!(
        "\n\
         \x20 Verglas service status\n\
         \x20 service:    {service}\n\
         \x20 daemon:     {version}\n\
         \x20 health:     {health}\n\
         \x20 cache:      {warmth}\n\
         \x20 endpoint:   {endpoint}\n\n",
        service = report.service_state.label(),
        endpoint = report.endpoint,
    )
}

/// Interprets a control tool's captured output into a [`ServiceError`] on
/// failure, or `Ok(())` on success. Shared by both platforms so the exit-code
/// and stderr handling is one place. A signal-killed tool reports code -1.
pub fn check_tool_output(
    tool: &'static str,
    verb: &'static str,
    output: &std::process::Output,
) -> Result<(), ServiceError> {
    if output.status.success() {
        return Ok(());
    }
    let code = output.status.code().unwrap_or(-1);
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    Err(ServiceError::ToolFailed {
        tool,
        verb,
        code,
        stderr,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use verglas_core::admin::{CacheConfigInfo, CountersInfo, StatsInfo};

    /// A stats payload with the given DRAM figures and hit/miss counters, other
    /// counters zero. Keeps the warmth tests focused.
    fn stats_with(
        dram_live: u64,
        dram_ceiling: u64,
        dram_hits: u64,
        disk_hits: u64,
        dram_misses: u64,
    ) -> StatsInfo {
        StatsInfo {
            cache: CacheConfigInfo {
                dir: "/cache".to_owned(),
                capacity_bytes: 20 * 1024 * 1024 * 1024,
                dram_bytes: dram_ceiling,
            },
            counters: CountersInfo {
                dram_hits,
                dram_misses,
                disk_hits,
                disk_misses: 0,
                peer_hits: 0,
                peer_misses: 0,
                peer_errors: 0,
                peer_served_blocks: 0,
                peer_served_bytes: 0,
                dram_bytes_served: 0,
                disk_bytes_served: 0,
                peer_bytes_served: 0,
                backend_bytes_served: 0,
                backend_fills: 0,
                backend_fill_bytes: 0,
                backend_heads: 0,
                non_cacheable_passthroughs: 0,
                meta_hits: 0,
                meta_misses: 0,
                meta_bytes_served: 0,
                retired_bytes_pending: 0,
                retired_bytes_reclaimed: 0,
                retired_files_reclaimed: 0,
            },
            dram_usage_bytes: dram_live,
            dram_live_bytes: dram_live,
            dram_reclaimable_bytes: 0,
            warming: None,
            writeback: None,
        }
    }

    #[test]
    fn scope_defaults_to_user_without_the_system_flag() {
        // Per-user is the no-sudo default; `--system` opts into the root scope.
        assert_eq!(Scope::from_system_flag(false), Scope::User);
        assert_eq!(Scope::from_system_flag(true), Scope::System);
    }

    #[test]
    fn warmth_reports_dram_working_set_and_hit_split() {
        // The warmth line an operator reads: live DRAM working set against the
        // ceiling, the DRAM/disk hit counts, and a coarse warm percentage.
        let stats = stats_with(512 * 1024 * 1024, 1024 * 1024 * 1024, 900, 90, 10);
        let line = render_warmth(&stats);
        assert!(line.contains("512MB"), "shows live working set: {line}");
        assert!(line.contains("1GB"), "shows the ceiling: {line}");
        assert!(line.contains("900 dram hits"), "shows dram hits: {line}");
        assert!(line.contains("90 disk hits"), "shows disk hits: {line}");
        // 990 warm hits of 1000 lookups is 99% warm.
        assert!(line.contains("99% warm"), "computes warm pct: {line}");
    }

    #[test]
    fn warmth_of_a_cold_daemon_reports_zero_percent() {
        // A freshly started daemon has served nothing: no divide-by-zero, 0% warm.
        let stats = stats_with(0, 1024 * 1024 * 1024, 0, 0, 0);
        let line = render_warmth(&stats);
        assert!(line.contains("0% warm"), "cold reads 0%: {line}");
    }

    #[test]
    fn status_aggregates_service_state_health_and_warmth() {
        // The combined report merges the OS service state with the daemon's own
        // health and warmth self-report and the endpoint.
        let report = aggregate_status(
            ServiceState::Running,
            "http://127.0.0.1:8333".to_owned(),
            Some("verglasd 0.1.0".to_owned()),
            Some("ok".to_owned()),
            Some("dram 512MB live".to_owned()),
        );
        assert_eq!(report.service_state, ServiceState::Running);
        assert_eq!(report.version.as_deref(), Some("verglasd 0.1.0"));
        assert_eq!(report.health.as_deref(), Some("ok"));
        assert_eq!(report.warmth.as_deref(), Some("dram 512MB live"));
        assert_eq!(report.endpoint, "http://127.0.0.1:8333");
    }

    #[test]
    fn status_render_names_running_health_warmth_and_endpoint() {
        // The human status block states every field an operator wants at a glance.
        let report = aggregate_status(
            ServiceState::Running,
            "http://127.0.0.1:8333".to_owned(),
            Some("verglasd 0.1.0".to_owned()),
            Some("ok".to_owned()),
            Some("dram 512MB live / 1GB ceiling".to_owned()),
        );
        let out = render_status(&report);
        assert!(out.contains("running"), "names the service state: {out}");
        assert!(out.contains("ok"), "names the health: {out}");
        assert!(out.contains("dram 512MB live"), "names the warmth: {out}");
        assert!(
            out.contains("http://127.0.0.1:8333"),
            "names the endpoint: {out}"
        );
    }

    #[test]
    fn status_render_names_the_daemon_version_when_reachable() {
        // The daemon version moved out of `verglas version` into `verglas status`
        // (#288): when the daemon answers `/admin/version`, its version appears in
        // the status block so an operator can see what is actually running.
        let report = aggregate_status(
            ServiceState::Running,
            "http://127.0.0.1:8333".to_owned(),
            Some("verglasd 0.1.0".to_owned()),
            Some("ok".to_owned()),
            Some("dram 512MB live / 1GB ceiling".to_owned()),
        );
        let out = render_status(&report);
        assert!(
            out.contains("verglasd 0.1.0"),
            "status must name the running daemon version: {out}"
        );
    }

    #[test]
    fn status_render_marks_the_daemon_version_unreachable_when_stopped() {
        // A stopped service cannot report a version: the daemon line reads
        // unreachable rather than fabricating one.
        let report = aggregate_status(
            ServiceState::Stopped,
            "http://127.0.0.1:8333".to_owned(),
            None,
            None,
            None,
        );
        let out = render_status(&report);
        assert!(
            out.contains("daemon:") && out.contains("unreachable"),
            "a stopped daemon version reads unreachable: {out}"
        );
    }

    #[test]
    fn status_render_marks_a_stopped_daemon_unreachable() {
        // A stopped service cannot be probed: health and warmth read as
        // unreachable/unavailable rather than fabricating an "ok".
        let report = aggregate_status(
            ServiceState::Stopped,
            "http://127.0.0.1:8333".to_owned(),
            None,
            None,
            None,
        );
        let out = render_status(&report);
        assert!(out.contains("stopped"), "names stopped: {out}");
        assert!(out.contains("unreachable"), "health unreachable: {out}");
        assert!(out.contains("unavailable"), "warmth unavailable: {out}");
    }

    #[test]
    fn check_tool_output_maps_a_nonzero_exit_to_a_named_error() {
        // A failing control tool surfaces its verb, exit code, and stderr in one
        // actionable error line.
        use std::os::unix::process::ExitStatusExt;
        let output = std::process::Output {
            status: std::process::ExitStatus::from_raw(256), // exit code 1
            stdout: Vec::new(),
            stderr: b"Bootstrap failed: 5: Input/output error".to_vec(),
        };
        let err = check_tool_output("launchctl", "bootstrap", &output)
            .expect_err("a nonzero exit must be an error");
        let msg = err.to_string();
        assert!(msg.contains("launchctl"), "names the tool: {msg}");
        assert!(msg.contains("bootstrap"), "names the verb: {msg}");
        assert!(msg.contains("Bootstrap failed"), "carries stderr: {msg}");
    }

    #[test]
    fn check_tool_output_passes_a_success() {
        use std::os::unix::process::ExitStatusExt;
        let output = std::process::Output {
            status: std::process::ExitStatus::from_raw(0),
            stdout: Vec::new(),
            stderr: Vec::new(),
        };
        assert!(check_tool_output("launchctl", "print", &output).is_ok());
    }

    #[test]
    fn human_bytes_uses_binary_multiples_and_trims_round_values() {
        assert_eq!(human_bytes(0), "0B");
        assert_eq!(human_bytes(1024), "1KB");
        assert_eq!(human_bytes(20 * 1024 * 1024 * 1024), "20GB");
        assert_eq!(human_bytes(1536 * 1024 * 1024), "1.5GB");
    }
}
