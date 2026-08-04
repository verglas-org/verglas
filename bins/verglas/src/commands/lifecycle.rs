//! `verglas init/start/stop/restart/status/logs` (#216, #221).
//!
//! The host-install path: install and run `verglasd` as a managed OS service.
//! `init` scaffolds `~/.verglas/config.toml` (every setting at its default, the
//! required bucket/endpoint/region filled from flags or left blank) and a
//! `~/.verglas/credentials/` directory (AWS-format, mode 0600), provisions the
//! cache/log dirs, generates the endpoint dev keys, and installs the OS service.
//! `start`/`restart` validate the required config before touching the service
//! manager, then drive it and block until `/admin/healthz` is ready. `status`
//! merges the service state with `/admin/healthz` and `/admin/stats`. `logs`
//! tails the platform log. Removal is deliberate and manual — the README's
//! "Removing Verglas" section documents the per-OS steps (#288); there is no
//! executable removal verb.
//!
//! Secrets never go in the config: the daemon reads endpoint/region from the
//! config and backend credentials from the credentials file, so a boot-started
//! service needs no AWS_* env. The daemon stays foreground; the OS service
//! manager backgrounds it. Nothing here self-daemonizes.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use verglas_core::admin::{HealthzInfo, StatsInfo};

use crate::commands::scaffold;
use crate::service::{self, Scope, ServiceManager, ServiceSpec, ServiceState, StatusReport};

/// The neutral region stamped into printed snippets, matching `verglas dev`. The
/// endpoint validates SigV4 against whatever region the client signs with.
const DEV_REGION: &str = "us-east-1";

/// The open-files ceiling (`RLIMIT_NOFILE`) the installed service runs under. The
/// cache engine (foyer) opens many region files; a service inherits the
/// supervisor's low default (256 under launchd), so it must be raised or the
/// daemon fails to build its cache with "too many open files". 65536 comfortably
/// covers a large NVMe cache's region files.
const OPEN_FILES_LIMIT: u64 = 65536;

/// Arguments for `verglas init`: scaffold `~/.verglas/config.toml`
/// and `~/.verglas/credentials/`, provision the cache dir, generate dev keys,
/// and install the OS service. With no flags it writes a complete blank scaffold;
/// the flags fill specific values into it.
#[derive(Debug, clap::Args)]
pub struct InitArgs {
    /// The primary bucket, `s3://<name>` form. Filled into `[backend] bucket`
    /// for the printed snippets and the `verglas status` round-trip. Omit to
    /// leave it blank in the scaffold for the operator to fill in.
    #[arg(long)]
    pub bucket: Option<String>,

    /// The object-store provider: `s3` (AWS/OCI/MinIO), `azure`, or `gcp`. Filled
    /// into `[backend] provider` and picks the credentials-file template format.
    /// Defaults to s3.
    #[arg(long, default_value = "s3")]
    pub provider: String,

    /// The origin S3 endpoint URL (OCI/MinIO). Filled into `[backend] endpoint`.
    /// Omit for AWS with the ambient credential chain, or to fill it in later.
    #[arg(long)]
    pub endpoint: Option<String>,

    /// The signing region. Filled into `[backend] region`. Omit for AWS with the
    /// ambient chain (defaults to `us-east-1`) or to fill it in later.
    #[arg(long)]
    pub region: Option<String>,

    /// Overwrite an existing `config.toml`. Without it, init refuses to clobber
    /// a config that is already there.
    #[arg(long)]
    pub force: bool,

    /// The cache/state directory. Defaults to the per-scope state dir. Point it
    /// at an NVMe path for real performance.
    #[arg(long)]
    pub cache_dir: Option<PathBuf>,

    /// On-disk cache size ceiling (`20GB`, `512MB`, or a byte count).
    #[arg(long, default_value = "20GB")]
    pub cache_size: String,

    /// In-memory (DRAM) cache size ceiling.
    #[arg(long, default_value = "1GB")]
    pub dram: String,

    /// Port for the local S3 endpoint (admin API binds the next port).
    #[arg(long, default_value_t = 8333)]
    pub port: u16,

    /// Install as a system service (needs root) rather than the per-user default.
    #[arg(long)]
    pub system: bool,

    /// Hard memory ceiling for the service cgroup (systemd only), e.g. `4GB`.
    #[arg(long)]
    pub memory_max: Option<String>,

    /// CPU weight for the service cgroup (systemd only, 1..=10000).
    #[arg(long)]
    pub cpu_weight: Option<u32>,

    /// IO weight for the service cgroup (systemd only, 1..=10000).
    #[arg(long)]
    pub io_weight: Option<u32>,
}

/// Shared flags for the scope-only verbs (start/stop/restart/status/logs).
#[derive(Debug, clap::Args)]
pub struct ScopeArgs {
    /// Operate on the system service rather than the per-user default.
    #[arg(long)]
    pub system: bool,
}

/// Arguments for `verglas logs`.
#[derive(Debug, clap::Args)]
pub struct LogsArgs {
    /// Operate on the system service rather than the per-user default.
    #[arg(long)]
    pub system: bool,

    /// Follow the log (like `tail -f`).
    #[arg(long, short = 'f')]
    pub follow: bool,

    /// Number of trailing lines to show.
    #[arg(long, short = 'n', default_value_t = 50)]
    pub lines: usize,
}

/// Lifecycle-command errors beyond the service-layer transport errors.
#[derive(Debug, thiserror::Error)]
pub enum LifecycleError {
    /// `--bucket` was not `s3://<name>` form.
    #[error("invalid --bucket `{0}`: expected s3://<bucket-name>")]
    BadBucket(String),
    /// `--provider` was not one of `s3`/`azure`/`gcp`.
    #[error("invalid --provider `{0}`: expected s3, azure, or gcp")]
    BadProvider(String),
    /// A required directory could not be created.
    #[error("cannot provision {0}: {1}")]
    Provision(PathBuf, std::io::Error),
    /// The generated config failed validation (a bad size, port collision, ...).
    #[error("generated config is invalid: {0}")]
    InvalidConfig(String),
    /// A size flag (`--cache-size`, `--dram`, `--memory-max`) was not a valid
    /// human byte string.
    #[error("invalid size `{0}`: expected e.g. `20GB`, `512MB`, or a byte count")]
    BadSize(String),
    /// The `verglasd` binary was not found next to `verglas`.
    #[error("{0}")]
    LocateDaemon(String),
    /// The service is not installed for this scope.
    #[error("no verglas {0} service is installed; run `verglas init` first")]
    NotInstalled(&'static str),
    /// A config file already exists and `--force` was not given.
    #[error(
        "config already exists at {0}; not overwriting it. Pass `verglas init --force` to replace it, or edit it directly."
    )]
    ConfigExists(PathBuf),
    /// The config could not be read (start/status/validate needs it).
    #[error("cannot read config at {0}: {1}. Run `verglas init` first.")]
    ConfigUnreadable(PathBuf, String),
    /// A required config value is unset. Names the field, the file, and how to
    /// set it, so `verglas start` never silently connects to nothing.
    #[error(
        "required config value `{field}` is not set in {file}.\n  Set it with `{init_flag}`, or edit {file} and set `{toml_key}`."
    )]
    MissingRequired {
        /// The dotted config field path (e.g. `backend.endpoint`).
        field: &'static str,
        /// The config file the field lives in.
        file: PathBuf,
        /// The `verglas init` flag that fills it.
        init_flag: &'static str,
        /// The TOML key to set by hand.
        toml_key: &'static str,
    },
}

/// The resolved paths a lifecycle command works with, derived from the scope
/// alone. Grouped so the derivation is one pure function that tests assert on.
///
/// The state dir is a well-known per-scope location holding the config, the keys,
/// and the logs — so `start`/`status`/`logs` find it without any
/// flag. The cache data dir is separate and may be pointed at NVMe with `init
/// --cache-dir`; it does not move the config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Paths {
    /// The state directory (holds the config, credentials, and logs). Well-known
    /// per scope so the scope-only verbs need no `--cache-dir`.
    pub state_dir: PathBuf,
    /// The daemon config file path (`<state>/config.toml`).
    pub config_path: PathBuf,
    /// The per-backend credentials directory (`<state>/credentials/`), holding
    /// AWS-format credential files (mode 0600). Secrets live here, never in the
    /// config.
    pub credentials_dir: PathBuf,
    /// The default backend credentials file the config references
    /// (`<state>/credentials/default`).
    pub credentials_file: PathBuf,
    /// The endpoint keypair file the config's `[auth]` references
    /// (`<state>/credentials/endpoint`). Holds the generated dev keys the S3
    /// endpoint accepts from engines (mode 0600); never in the config.
    pub endpoint_credentials_file: PathBuf,
    /// The stdout log file path.
    pub stdout_log: PathBuf,
    /// The stderr log file path.
    pub stderr_log: PathBuf,
}

/// Derives the well-known state dir and the config/credentials/log paths under
/// it for `scope`. Pure so the layout is unit-tested.
///
/// User scope roots at `~/.verglas`; system scope at `/var/lib/verglas`. The
/// config lives at `<state>/config.toml`, credentials under
/// `<state>/credentials/`, logs under `<state>/logs/`. The cache data dir is
/// resolved separately (see [`resolve_cache_dir`]) so a `--cache-dir` override
/// never moves the config the scope-only verbs look for.
pub fn resolve_paths(scope: Scope, home: &Path) -> Paths {
    let state_dir = match scope {
        Scope::User => home.join(".verglas"),
        Scope::System => PathBuf::from("/var/lib/verglas"),
    };
    let credentials_dir = state_dir.join("credentials");
    Paths {
        config_path: state_dir.join("config.toml"),
        credentials_file: credentials_dir.join("default"),
        endpoint_credentials_file: credentials_dir.join("endpoint"),
        credentials_dir,
        stdout_log: state_dir.join("logs").join("verglasd.out.log"),
        stderr_log: state_dir.join("logs").join("verglasd.err.log"),
        state_dir,
    }
}

/// Resolves the cache data directory: the explicit `--cache-dir` when given, else
/// the state dir (so the cache lives alongside the config by default). Pure.
pub fn resolve_cache_dir(state_dir: &Path, cache_dir: Option<&Path>) -> PathBuf {
    match cache_dir {
        Some(dir) => dir.to_path_buf(),
        None => state_dir.to_path_buf(),
    }
}

/// Validates a `--bucket s3://<name>` value and returns the bucket name. The
/// daemon config names the one bucket it serves, and init requires a concrete
/// backend so the snippets and round-trip are real. Pure.
pub fn parse_bucket(bucket: &str) -> Result<String, LifecycleError> {
    let name = bucket
        .strip_prefix("s3://")
        .ok_or_else(|| LifecycleError::BadBucket(bucket.to_owned()))?;
    let name = name.trim_end_matches('/');
    if name.is_empty() || name.contains('/') {
        return Err(LifecycleError::BadBucket(bucket.to_owned()));
    }
    Ok(name.to_owned())
}

/// Normalizes and validates a `--provider` value to the canonical lowercase
/// `s3`/`azure`/`gcp`. Pure so the mapping is unit-tested. The daemon config only
/// accepts these three, so a typo fails here with a clear message rather than at
/// service start.
pub fn parse_provider(provider: &str) -> Result<String, LifecycleError> {
    match provider.trim().to_ascii_lowercase().as_str() {
        "s3" => Ok("s3".to_owned()),
        "azure" => Ok("azure".to_owned()),
        "gcp" => Ok("gcp".to_owned()),
        _ => Err(LifecycleError::BadProvider(provider.to_owned())),
    }
}

/// Generates a dev access keypair, matching `verglas dev`'s shape. Not customer
/// credentials: these only authenticate engines to this host's daemon.
fn generate_dev_keys() -> (String, String) {
    use std::hash::{BuildHasher, RandomState};
    let r = || RandomState::new().hash_one(0u64);
    (
        format!("VG{:016X}", r()),
        format!("{:016x}{:016x}", r(), r()),
    )
}

/// Parses a human byte-size string via the core config parser, mapping the error
/// to [`LifecycleError::BadSize`]. Used to validate `--memory-max` up front.
fn parse_size(raw: &str) -> Result<u64, LifecycleError> {
    // The core ByteSize parser accepts the same forms as the config file; round
    // it through the config to reuse that logic exactly.
    let toml =
        format!("[cache]\ndir = \"/tmp\"\ncapacity_bytes = \"{raw}\"\ndram_bytes = \"80MB\"\n");
    match verglas_core::config::Config::from_toml_str(&toml) {
        Ok(config) => Ok(config.cache.capacity_bytes.0),
        Err(_) => Err(LifecycleError::BadSize(raw.to_owned())),
    }
}

/// Locates the `verglasd` binary next to this executable — the layout
/// `cargo install` and the release tarball use.
fn locate_verglasd() -> Result<PathBuf, LifecycleError> {
    let exe = std::env::current_exe()
        .map_err(|e| LifecycleError::LocateDaemon(format!("cannot resolve current exe: {e}")))?;
    let dir = exe
        .parent()
        .ok_or_else(|| LifecycleError::LocateDaemon("current exe has no parent".to_owned()))?;
    let candidate = dir.join(format!("verglasd{}", std::env::consts::EXE_SUFFIX));
    if candidate.exists() {
        Ok(candidate)
    } else {
        Err(LifecycleError::LocateDaemon(format!(
            "verglasd not found next to verglas at {}",
            candidate.display()
        )))
    }
}

/// The home directory from `$HOME`, defaulting to `.` so a headless system-scope
/// install (which does not use home) still resolves.
fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

/// Builds the platform service manager for `scope`. One `#[cfg]` switch; every
/// verb goes through the returned trait object.
fn manager(scope: Scope) -> Box<dyn ServiceManager> {
    #[cfg(target_os = "macos")]
    {
        Box::new(service::launchd::LaunchdManager::new(scope))
    }
    #[cfg(target_os = "linux")]
    {
        Box::new(service::systemd::SystemdManager::new(scope))
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        // No native supervisor backend on this platform yet (e.g. Windows): the
        // stub manager returns a clear "run verglasd in the foreground" error
        // from every operation rather than failing to compile.
        Box::new(service::unsupported::UnsupportedManager::new(scope))
    }
}

/// Renders the `verglas init` success banner: the endpoint, the resolved paths
/// (config, credentials, cache, logs), the dev keys, the next steps, and the
/// engine snippets. Pure so it is golden-tested. When the required trio is not
/// yet filled it says so and points at the config and credentials to edit.
pub fn render_init_banner(
    endpoint: &str,
    bucket: Option<&str>,
    paths: &Paths,
    cache_dir: &Path,
    scope: Scope,
    access_key_id: &str,
    secret_access_key: &str,
) -> String {
    let backend_line = match bucket {
        Some(b) => format!("s3://{b} (set backend.endpoint/region in the config)"),
        None => "(not set — fill in bucket/endpoint/region in the config)".to_owned(),
    };
    let next = if bucket.is_some() {
        "Fill in the backend credentials file, set backend.endpoint/region if needed, then:"
    } else {
        "Fill in bucket/endpoint/region in the config and the backend credentials file, then:"
    };
    format!(
        "\n\
         \x20 Verglas installed as a {scope} service\n\
         \x20 endpoint:   {endpoint}\n\
         \x20 backend:    {backend_line}\n\
         \x20 config:     {config}\n\
         \x20 backend creds:  {creds}  (mode 0600; fill in your backend keys)\n\
         \x20 endpoint creds: {endpoint_creds}  (mode 0600; the dev keypair was written here)\n\
         \x20 cache:      {cache} (persistent; Verglas never removes cache data)\n\
         \x20 logs:       {stdout}\n\
         \x20 dev keys:   access_key_id={access}\n\
         \x20             secret_access_key={secret}\n\
         \n\
         \x20 {next}\n\
         \x20 Start it:   verglas start{scope_flag}\n\
         \x20 Status:     verglas status{scope_flag}\n\
         \n\
         {snippets}\n",
        scope = scope.label(),
        config = paths.config_path.display(),
        creds = paths.credentials_file.display(),
        endpoint_creds = paths.endpoint_credentials_file.display(),
        cache = cache_dir.display(),
        stdout = paths.stdout_log.display(),
        access = access_key_id,
        secret = secret_access_key,
        scope_flag = if matches!(scope, Scope::System) {
            " --system"
        } else {
            ""
        },
        snippets = engine_snippets(endpoint, access_key_id, secret_access_key),
    )
}

/// The placeholder bucket in the printed snippets, matching `verglas dev`.
const BUCKET_PLACEHOLDER: &str = "your-bucket";

/// Renders the copy-pasteable engine snippets, identical in shape to
/// `verglas dev`'s: DuckDB `SET`s, a Polars/PyArrow `storage_options` dict, and
/// an aws-CLI invocation, all pointed at the one endpoint.
fn engine_snippets(endpoint: &str, access_key_id: &str, secret: &str) -> String {
    let bucket = BUCKET_PLACEHOLDER;
    let host_port = endpoint.strip_prefix("http://").unwrap_or(endpoint);
    format!(
        "  DuckDB:\n\
         \x20   INSTALL httpfs; LOAD httpfs;\n\
         \x20   SET s3_endpoint='{host_port}';\n\
         \x20   SET s3_url_style='path';\n\
         \x20   SET s3_use_ssl=false;\n\
         \x20   SET s3_region='{DEV_REGION}';\n\
         \x20   SET s3_access_key_id='{access_key_id}';\n\
         \x20   SET s3_secret_access_key='{secret}';\n\
         \n\
         \x20 aws CLI (reads and writes):\n\
         \x20   AWS_ACCESS_KEY_ID={access_key_id} \\\n\
         \x20   AWS_SECRET_ACCESS_KEY={secret} \\\n\
         \x20   aws --endpoint-url {endpoint} s3 ls s3://{bucket}/\n"
    )
}

/// Runs `verglas init`. Scaffolds a complete, commented
/// `~/.verglas/config.toml` (every setting at its default; the required
/// bucket/endpoint/region filled from flags or left blank) and a
/// `~/.verglas/credentials/` directory with a mode-0600 AWS-format template,
/// provisions the cache/log dirs, generates the endpoint dev keys, and installs
/// the OS service. Refuses to overwrite an existing config without `--force`.
///
/// Secrets never go in the config. Backend credentials live in the credentials
/// file the config references; a boot-started service reads endpoint/region from
/// the config and credentials from that file, so no AWS_* env is needed.
pub async fn init(args: InitArgs) -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope::from_system_flag(args.system);
    let paths = resolve_paths(scope, &home_dir());
    let cache_dir = resolve_cache_dir(&paths.state_dir, args.cache_dir.as_deref());

    // Refuse to clobber an existing config unless --force: the operator may have
    // edited it, and a silent overwrite would lose those edits (#221).
    if paths.config_path.exists() && !args.force {
        return Err(LifecycleError::ConfigExists(paths.config_path.clone()).into());
    }

    // The bucket flag accepts s3://<name>; store the bare name in the scaffold.
    let bucket = match &args.bucket {
        Some(raw) => Some(parse_bucket(raw)?),
        None => None,
    };

    // The provider gates the backend client and the credentials-file template.
    let provider = parse_provider(&args.provider)?;

    // Provision the state dir, the log dir, the credentials dir, and the cache
    // data dir. Verglas never removes any of them.
    for dir in [
        &paths.state_dir,
        &paths.state_dir.join("logs"),
        &paths.credentials_dir,
        &cache_dir,
    ] {
        std::fs::create_dir_all(dir).map_err(|e| LifecycleError::Provision(dir.clone(), e))?;
    }

    let s3_port = args.port;
    let admin_port = args
        .port
        .checked_add(1)
        .ok_or("port too high (admin port would overflow)")?;

    // Validate the size flags up front so a bad value fails before anything is
    // written, with an actionable message.
    let _ = parse_size(&args.cache_size)?;
    let _ = parse_size(&args.dram)?;

    let (access_key_id, secret_access_key) = generate_dev_keys();

    // Write the endpoint keypair to its own credentials file only when absent,
    // so a re-run never regenerates the keys engines are already using. Mode
    // 0600. The keypair never goes into the config; the config's [auth] names
    // this file.
    if !paths.endpoint_credentials_file.exists() {
        write_endpoint_credentials(
            &paths.endpoint_credentials_file,
            &access_key_id,
            &secret_access_key,
        )?;
    }

    // Generate the annotated config from the daemon's own schema, filling the
    // init flags. Secrets stay out: [auth] and [backend] name credentials files,
    // never inline keys.
    let overrides = verglas_core::config_template::Overrides {
        s3_port: Some(s3_port),
        admin_port: Some(admin_port),
        cache_dir: Some(cache_dir.display().to_string()),
        capacity_bytes: Some(args.cache_size.clone()),
        dram_bytes: Some(args.dram.clone()),
        provider: Some(provider.clone()),
        bucket: bucket.clone(),
        endpoint: args.endpoint.clone(),
        region: args.region.clone(),
        backend_credentials_file: Some(paths.credentials_file.display().to_string()),
        auth_credentials_file: Some(paths.endpoint_credentials_file.display().to_string()),
    };
    let config_toml = verglas_core::config_template::render(&overrides);

    // The generated config must parse against the daemon's schema — a template
    // bug is caught here, not at first service start. Required serving values
    // (bucket/endpoint/region) are intentionally blank in a fresh scaffold, so
    // they are NOT enforced here; `verglas start` and the daemon check them at
    // boot. (Running the full `validate()` here would reject a blank scaffold on
    // the now-required `backend.bucket`.)
    verglas_core::config::Config::from_toml_str(&config_toml)
        .map_err(|e| LifecycleError::InvalidConfig(e.to_string()))?;
    std::fs::write(&paths.config_path, &config_toml)
        .map_err(|e| LifecycleError::Provision(paths.config_path.clone(), e))?;

    // Write the backend credentials template only when the file is absent, so a
    // re-run never clobbers real keys the operator already filled in. The
    // template matches the chosen provider's format. Mode 0600.
    if !paths.credentials_file.exists() {
        write_credentials_template(&paths.credentials_file, &provider)?;
    }

    let memory_max_bytes = match &args.memory_max {
        Some(raw) => Some(parse_size(raw)?),
        None => None,
    };

    // The service definition carries no secrets and no AWS_* env: the daemon
    // reads endpoint/region from the config and credentials from the credentials
    // file (#221). Left empty so a boot-started service needs no shell env.
    let spec = ServiceSpec {
        program: locate_verglasd()?,
        config_path: paths.config_path.clone(),
        state_dir: paths.state_dir.clone(),
        stdout_log: paths.stdout_log.clone(),
        stderr_log: paths.stderr_log.clone(),
        environment: BTreeMap::new(),
        memory_max_bytes,
        cpu_weight: args.cpu_weight,
        io_weight: args.io_weight,
        open_files_limit: Some(OPEN_FILES_LIMIT),
    };
    manager(scope).install(&spec)?;

    let endpoint = format!("http://127.0.0.1:{s3_port}");
    print!(
        "{}",
        render_init_banner(
            &endpoint,
            bucket.as_deref(),
            &paths,
            &cache_dir,
            scope,
            &access_key_id,
            &secret_access_key,
        )
    );
    Ok(())
}

/// Writes the credentials-file template for `provider` at `path` mode 0600, so
/// backend secrets live under `~/.verglas/credentials/` in the provider's format
/// (AWS-INI for s3, key=value for azure/gcp), never in the config. The
/// 0600 mode is set at create time so the secret file is never briefly
/// world-readable.
fn write_credentials_template(path: &Path, provider: &str) -> Result<(), LifecycleError> {
    write_credentials_file(path, &scaffold::render_credentials_template(provider))
}

/// Writes the generated endpoint keypair at `path` in AWS format, mode 0600, so
/// the S3 endpoint reads the keys engines present from a file the config's
/// `[auth]` names — never from the config itself.
fn write_endpoint_credentials(
    path: &Path,
    access_key_id: &str,
    secret_access_key: &str,
) -> Result<(), LifecycleError> {
    write_credentials_file(
        path,
        &scaffold::render_endpoint_credentials(access_key_id, secret_access_key),
    )
}

/// Writes `contents` at `path` mode 0600, so a secrets file is never briefly
/// world-readable. Shared by the backend template and the endpoint keypair.
fn write_credentials_file(path: &Path, contents: &str) -> Result<(), LifecycleError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .map_err(|e| LifecycleError::Provision(path.to_path_buf(), e))?;
        file.write_all(contents.as_bytes())
            .map_err(|e| LifecycleError::Provision(path.to_path_buf(), e))?;
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, contents.as_bytes())
            .map_err(|e| LifecycleError::Provision(path.to_path_buf(), e))?;
    }
    Ok(())
}

/// Validates that the required backend config values are present before `start`
/// touches the service manager. A missing value is a named, actionable
/// error, not a silent default that connects to nothing. Pure so it is
/// unit-tested: it takes the loaded config, the config file path (for the
/// message), and an env reader.
///
/// A bucket set is required in the config: `backend.bucket` and/or
/// `backend.bucket_globs`; there is no env equivalent. `endpoint` and
/// `region` are required unless supplied by the AWS env
/// (`AWS_ENDPOINT`/`AWS_REGION`), so a production AWS deployment relying on the
/// ambient chain is not forced to set them.
pub fn validate_required_config<F>(
    config: &verglas_core::config::Config,
    config_path: &Path,
    env: &F,
) -> Result<(), LifecycleError>
where
    F: Fn(&str) -> Option<String>,
{
    let is_set = |v: &Option<String>| v.as_ref().is_some_and(|s| !s.is_empty());
    let env_set = |name: &str| env(name).is_some_and(|s| !s.is_empty());

    // A served bucket set is required: a single `bucket` or at least one glob.
    if !is_set(&config.backend.bucket) && config.backend.bucket_globs.is_empty() {
        return Err(LifecycleError::MissingRequired {
            field: "backend.bucket",
            file: config_path.to_path_buf(),
            init_flag: "verglas init --bucket s3://<name> --force",
            toml_key: "[backend] bucket (or bucket_globs)",
        });
    }
    if !is_set(&config.backend.endpoint) && !env_set("AWS_ENDPOINT") && !env_set("AWS_ENDPOINT_URL")
    {
        return Err(LifecycleError::MissingRequired {
            field: "backend.endpoint",
            file: config_path.to_path_buf(),
            init_flag: "verglas init --endpoint <url> --force",
            toml_key: "[backend] endpoint",
        });
    }
    if !is_set(&config.backend.region) && !env_set("AWS_REGION") && !env_set("AWS_DEFAULT_REGION") {
        return Err(LifecycleError::MissingRequired {
            field: "backend.region",
            file: config_path.to_path_buf(),
            init_flag: "verglas init --region <region> --force",
            toml_key: "[backend] region",
        });
    }
    Ok(())
}

/// Loads the installed config for `scope` and checks the required values, so
/// `start`/`restart` fail early with a named error rather than starting a daemon
/// that connects to nothing.
fn preflight_config(scope: Scope) -> Result<(), LifecycleError> {
    let paths = resolve_paths(scope, &home_dir());
    let config = verglas_core::config::Config::load(&paths.config_path)
        .map_err(|e| LifecycleError::ConfigUnreadable(paths.config_path.clone(), e.to_string()))?;
    validate_required_config(&config, &paths.config_path, &|name| {
        std::env::var(name).ok()
    })
}

/// Runs `verglas start`: checks the config has the required values, drives the
/// service manager, then polls `/admin/healthz` until the daemon reports ready so
/// `start` blocks until the cache is actually serving (the gate).
pub async fn start(args: ScopeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope::from_system_flag(args.system);
    let mgr = manager(scope);
    if mgr.state()? == ServiceState::NotInstalled {
        return Err(LifecycleError::NotInstalled(scope.label()).into());
    }
    // Validate the config before touching the service manager (#221).
    preflight_config(scope)?;
    mgr.start()?;
    let endpoint = admin_endpoint(scope)?;
    wait_until_ready(&endpoint).await?;
    println!("verglas: service started and reports ready ({endpoint})");
    Ok(())
}

/// Runs `verglas stop`: drives the service manager to stop the running service.
pub async fn stop(args: ScopeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope::from_system_flag(args.system);
    let mgr = manager(scope);
    if mgr.state()? == ServiceState::NotInstalled {
        return Err(LifecycleError::NotInstalled(scope.label()).into());
    }
    mgr.stop()?;
    println!("verglas: service stopped");
    Ok(())
}

/// Runs `verglas restart`: stop then start, re-blocking on readiness. Validates
/// the config before restarting so a blank config fails without bouncing a
/// working daemon.
pub async fn restart(args: ScopeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope::from_system_flag(args.system);
    let mgr = manager(scope);
    if mgr.state()? == ServiceState::NotInstalled {
        return Err(LifecycleError::NotInstalled(scope.label()).into());
    }
    preflight_config(scope)?;
    mgr.stop()?;
    mgr.start()?;
    let endpoint = admin_endpoint(scope)?;
    wait_until_ready(&endpoint).await?;
    println!(
        "verglas: service restarted and reports ready ({})",
        endpoint
    );
    Ok(())
}

/// Runs `verglas status`: merges the OS service state with the daemon's
/// `/admin/healthz` and `/admin/stats` self-report and prints the combined block.
pub async fn status(args: ScopeArgs, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope::from_system_flag(args.system);
    let mgr = manager(scope);
    let service_state = mgr.state()?;
    let admin = admin_endpoint(scope)?;
    let s3_endpoint = s3_endpoint(scope)?;

    // Probe the daemon only when the service is running; a stopped service has
    // no listener, so version/health/warmth read as unreachable rather than
    // hanging.
    let (version, health, warmth) = if service_state == ServiceState::Running {
        (
            fetch_version(&admin).await,
            fetch_health(&admin).await,
            fetch_warmth(&admin).await,
        )
    } else {
        (None, None, None)
    };

    let report = service::aggregate_status(service_state, s3_endpoint, version, health, warmth);
    // Best-effort control-plane view: when this machine is logged in, tell the
    // operator whether the control plane currently lists THIS node as active.
    // Not logged in (or any error) leaves the output unchanged.
    let node_cp = fetch_node_cp_status(scope).await;
    if json {
        print_status_json(&report, node_cp.as_ref())?;
    } else {
        print!("{}", service::render_status(&report));
        if let Some(node) = &node_cp {
            print!("{}", render_node_cp_status(node));
        }
    }
    Ok(())
}

/// This node's registration state in the control plane, for `verglas status`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeCpStatus {
    /// The node's resolved cluster id (what the daemon registers under).
    pub node_id: String,
    /// True when the control plane has a row for this node id.
    pub registered: bool,
    /// True when the control plane counts this node active (recent heartbeat);
    /// only meaningful when `registered`.
    pub active: bool,
}

/// Renders the one-line control-plane node status for the human status block.
/// Pure so it is unit-tested without a live control plane.
pub fn render_node_cp_status(node: &NodeCpStatus) -> String {
    let state = if !node.registered {
        "not yet registered with the control plane"
    } else if node.active {
        "active in the control plane"
    } else {
        "registered, inactive in the control plane"
    };
    format!("  node:       {} ({state})\n", node.node_id)
}

/// Fetches this node's control-plane registration state, best-effort. Returns
/// `None` when the machine is not logged in, the daemon config cannot be read,
/// or the control plane is unreachable — so a status run never fails or changes
/// its output on account of the control plane. Reuses the daemon's own
/// `resolve_cluster_id` for the node id, so the CLI and daemon agree on identity.
async fn fetch_node_cp_status(scope: Scope) -> Option<NodeCpStatus> {
    let client = crate::backend::control_plane().ok()?;
    let paths = resolve_paths(scope, &home_dir());
    let config = verglas_core::config::Config::load(&paths.config_path).ok()?;
    let node_id = config.resolve_cluster_id();
    let nodes = client.nodes().await.ok()?;
    let found = nodes.iter().find(|n| n.node_id == node_id);
    Some(NodeCpStatus {
        node_id,
        registered: found.is_some(),
        active: found.is_some_and(|n| n.active),
    })
}

/// Prints the status report as JSON for scripting. The control-plane node view
/// is added only when the machine is logged in and reachable (otherwise absent,
/// so the shape is unchanged for self-hosted installs).
fn print_status_json(
    report: &StatusReport,
    node_cp: Option<&NodeCpStatus>,
) -> Result<(), std::io::Error> {
    let mut value = serde_json::json!({
        "service": report.service_state.label(),
        "version": report.version,
        "health": report.health,
        "warmth": report.warmth,
        "endpoint": report.endpoint,
    });
    if let Some(node) = node_cp {
        value["node"] = serde_json::json!({
            "id": node.node_id,
            "registered": node.registered,
            "active": node.active,
        });
    }
    let encoded = serde_json::to_string_pretty(&value)?;
    writeln!(std::io::stdout(), "{encoded}")
}

/// Runs `verglas logs`: builds the platform log-tail command and execs it,
/// pointing launchd's `tail` at the plist's log files. On systemd the command is
/// a self-contained `journalctl` invocation.
pub async fn logs(args: LogsArgs) -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope::from_system_flag(args.system);
    let mgr = manager(scope);
    let mut cmd = mgr.log_command(args.follow, args.lines);
    if cmd.is_empty() {
        return Err("no log command available for this platform".into());
    }
    // launchd's tail command needs the log file paths appended (systemd's
    // journalctl is self-contained). Append them for the tail-based command.
    #[cfg(target_os = "macos")]
    {
        let paths = resolve_paths(scope, &home_dir());
        cmd.push(paths.stderr_log.display().to_string());
        cmd.push(paths.stdout_log.display().to_string());
    }
    let program = cmd.remove(0);
    let status = tokio::process::Command::new(&program)
        .args(&cmd)
        .status()
        .await?;
    if !status.success() {
        return Err(format!("{program} exited with {status}").into());
    }
    Ok(())
}

/// The admin API base URL for the installed service at `scope`: loopback on the
/// admin port derived from the config's S3 port + 1. Reads the config to learn
/// the port; falls back to the default init port pair when the config is absent.
fn admin_endpoint(scope: Scope) -> Result<String, LifecycleError> {
    let (_s3, admin) = resolve_ports(scope);
    Ok(format!("http://127.0.0.1:{admin}"))
}

/// The S3 endpoint URL for the installed service at `scope`.
fn s3_endpoint(scope: Scope) -> Result<String, LifecycleError> {
    let (s3, _admin) = resolve_ports(scope);
    Ok(format!("http://127.0.0.1:{s3}"))
}

/// Reads the installed config's ports, defaulting to `8333`/`8334` when it
/// cannot be read (a status/start before init, or a hand-removed config).
fn resolve_ports(scope: Scope) -> (u16, u16) {
    let paths = resolve_paths(scope, &home_dir());
    match verglas_core::config::Config::load(&paths.config_path) {
        Ok(config) => (config.listen.s3_port, config.listen.admin_port),
        Err(_) => (8333, 8334),
    }
}

/// Fetches the running daemon's version from `/admin/version` and renders it as
/// `<name> <version>` (e.g. `verglasd 0.1.0`) for the status block, `None` if
/// the daemon is unreachable. The daemon version lives in `verglas status`
/// since removed the `verglas version` subcommand.
async fn fetch_version(admin_endpoint: &str) -> Option<String> {
    let client = crate::admin_client::AdminClient::new(admin_endpoint).ok()?;
    let info = client.version().await.ok()?;
    Some(format!("{} {}", info.name, info.version))
}

/// Fetches the daemon's `/admin/healthz` status string, `None` if unreachable.
async fn fetch_health(admin_endpoint: &str) -> Option<String> {
    let url = format!("{admin_endpoint}{}", verglas_core::admin::HEALTHZ_PATH);
    let resp = reqwest::get(&url).await.ok()?;
    let info: HealthzInfo = resp.json().await.ok()?;
    Some(info.status)
}

/// Fetches `/admin/stats` and renders the one-line warmth summary, `None` if
/// unreachable.
async fn fetch_warmth(admin_endpoint: &str) -> Option<String> {
    let url = format!("{admin_endpoint}{}", verglas_core::admin::STATS_PATH);
    let resp = reqwest::get(&url).await.ok()?;
    let stats: StatsInfo = resp.json().await.ok()?;
    Some(service::render_warmth(&stats))
}

/// Polls `/admin/healthz` until the daemon reports ready, so `start` blocks until
/// the cache is actually serving. Fails after a bounded wait so a misconfigured
/// service does not hang the command forever.
async fn wait_until_ready(admin_endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("{admin_endpoint}{}", verglas_core::admin::HEALTHZ_PATH);
    for _ in 0..150 {
        if let Ok(resp) = reqwest::get(&url).await
            && resp.status().is_success()
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(format!("service did not report ready within 30s at {url}").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bucket_accepts_s3_uri_and_rejects_others() {
        assert_eq!(parse_bucket("s3://hyperglas").expect("ok"), "hyperglas");
        assert_eq!(parse_bucket("s3://hyperglas/").expect("ok"), "hyperglas");
        assert!(parse_bucket("hyperglas").is_err());
        assert!(parse_bucket("s3://").is_err());
        assert!(parse_bucket("s3://a/b").is_err());
    }

    /// A minimal loaded config with the given backend trio, using a real temp
    /// cache dir so it validates. Helper for the required-config tests.
    fn config_with(
        bucket: Option<&str>,
        endpoint: Option<&str>,
        region: Option<&str>,
    ) -> verglas_core::config::Config {
        let dir = std::env::temp_dir().join(format!(
            "verglas-req-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let mut toml = format!(
            "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n\n[backend]\n",
            dir.display()
        );
        if let Some(b) = bucket {
            toml.push_str(&format!("bucket = \"{b}\"\n"));
        }
        if let Some(e) = endpoint {
            toml.push_str(&format!("endpoint = \"{e}\"\n"));
        }
        if let Some(r) = region {
            toml.push_str(&format!("region = \"{r}\"\n"));
        }
        verglas_core::config::Config::from_toml_str(&toml).expect("parses")
    }

    #[test]
    fn resolve_paths_user_scope_roots_at_home_dotverglas() {
        // User scope roots the state dir at ~/.verglas with the config,
        // credentials, and logs under it (#221: config is config.toml).
        let paths = resolve_paths(Scope::User, Path::new("/home/op"));
        assert_eq!(paths.state_dir, PathBuf::from("/home/op/.verglas"));
        assert_eq!(
            paths.config_path,
            PathBuf::from("/home/op/.verglas/config.toml")
        );
        assert_eq!(
            paths.credentials_dir,
            PathBuf::from("/home/op/.verglas/credentials")
        );
        assert_eq!(
            paths.credentials_file,
            PathBuf::from("/home/op/.verglas/credentials/default")
        );
        assert_eq!(
            paths.endpoint_credentials_file,
            PathBuf::from("/home/op/.verglas/credentials/endpoint")
        );
        assert_eq!(
            paths.stdout_log,
            PathBuf::from("/home/op/.verglas/logs/verglasd.out.log")
        );
    }

    #[test]
    fn resolve_paths_system_scope_roots_at_var_lib() {
        let paths = resolve_paths(Scope::System, Path::new("/home/op"));
        assert_eq!(paths.state_dir, PathBuf::from("/var/lib/verglas"));
    }

    #[test]
    fn cache_dir_override_moves_data_but_not_the_config() {
        // `--cache-dir` points the cache data at NVMe, but the config and logs
        // stay at the well-known scope path so the scope-only verbs find them.
        let paths = resolve_paths(Scope::User, Path::new("/home/op"));
        assert_eq!(
            paths.config_path,
            PathBuf::from("/home/op/.verglas/config.toml")
        );
        let cache = resolve_cache_dir(&paths.state_dir, Some(Path::new("/nvme/verglas")));
        assert_eq!(cache, PathBuf::from("/nvme/verglas"));
        // Without an override the cache lives alongside the config.
        let default_cache = resolve_cache_dir(&paths.state_dir, None);
        assert_eq!(default_cache, PathBuf::from("/home/op/.verglas"));
    }

    #[test]
    fn start_refuses_when_a_required_value_is_unset_naming_field_file_and_how() {
        // #221 item 2: a missing required value fails with a message that names
        // the field, the file, and how to set it — no cryptic error.
        let config = config_with(None, None, None);
        let path = Path::new("/home/op/.verglas/config.toml");
        let no_env = |_: &str| None;
        let err =
            validate_required_config(&config, path, &no_env).expect_err("missing bucket must fail");
        let msg = err.to_string();
        assert!(msg.contains("backend.bucket"), "names the field: {msg}");
        assert!(msg.contains("config.toml"), "names the file: {msg}");
        assert!(msg.contains("verglas init --bucket"), "how to set: {msg}");
    }

    #[test]
    fn start_accepts_bucket_globs_without_a_single_bucket() {
        // #235: a config that names only `bucket_globs` (the S3 Tables case)
        // satisfies the bucket-set requirement, with endpoint/region supplied.
        let dir = std::env::temp_dir().join(format!(
            "verglas-globs-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let toml = format!(
            "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n\n[backend]\nbucket_globs = [\"*--table-s3\"]\n\
             endpoint = \"https://s3.us-west-2.amazonaws.com\"\nregion = \"us-west-2\"\n",
            dir.display()
        );
        let config = verglas_core::config::Config::from_toml_str(&toml).expect("parses");
        let path = Path::new("/home/op/.verglas/config.toml");
        validate_required_config(&config, path, &|_| None)
            .expect("a globs-only bucket set passes the bucket requirement");
    }

    #[test]
    fn start_refuses_when_endpoint_and_region_unset_and_no_env() {
        // With a bucket but no endpoint/region and no AWS env, endpoint is the
        // first missing field reported.
        let config = config_with(Some("hyperglas"), None, None);
        let path = Path::new("/home/op/.verglas/config.toml");
        let no_env = |_: &str| None;
        let err = validate_required_config(&config, path, &no_env).expect_err("missing endpoint");
        assert!(err.to_string().contains("backend.endpoint"), "{err}");
    }

    #[test]
    fn start_accepts_config_trio_or_env_fallback() {
        let path = Path::new("/home/op/.verglas/config.toml");
        // The full trio in config passes with no env.
        let config = config_with(Some("hyperglas"), Some("https://oci"), Some("us-ashburn-1"));
        validate_required_config(&config, path, &|_| None).expect("config trio passes");

        // A bucket in config plus endpoint/region from the AWS env passes too —
        // the production AWS/instance-role path is not forced to set them.
        let config = config_with(Some("hyperglas"), None, None);
        let env = |name: &str| match name {
            "AWS_ENDPOINT" => Some("https://s3.amazonaws.com".to_owned()),
            "AWS_REGION" => Some("us-east-1".to_owned()),
            _ => None,
        };
        validate_required_config(&config, path, &env).expect("env fallback passes");
    }

    #[test]
    fn init_banner_names_endpoint_paths_keys_and_snippets() {
        // The banner states the endpoint, the resolved config/cache/log paths,
        // the dev keys, the start hint, and the engine snippets — everything an
        // operator needs after init.
        let paths = resolve_paths(Scope::User, Path::new("/home/op"));
        let banner = render_init_banner(
            "http://127.0.0.1:8333",
            Some("hyperglas"),
            &paths,
            Path::new("/home/op/.verglas"),
            Scope::User,
            "VGKEY",
            "secret",
        );
        assert!(
            banner.contains("http://127.0.0.1:8333"),
            "endpoint: {banner}"
        );
        assert!(banner.contains("s3://hyperglas"), "backend: {banner}");
        assert!(
            banner.contains("/home/op/.verglas/config.toml"),
            "config: {banner}"
        );
        assert!(
            banner.contains("/home/op/.verglas/credentials/default"),
            "backend credentials: {banner}"
        );
        assert!(
            banner.contains("/home/op/.verglas/credentials/endpoint"),
            "endpoint credentials: {banner}"
        );
        assert!(
            banner.contains("Verglas never removes cache data"),
            "cache note: {banner}"
        );
        assert!(
            banner.contains("VGKEY") && banner.contains("secret"),
            "keys: {banner}"
        );
        assert!(banner.contains("verglas start"), "start hint: {banner}");
        // The snippets are embedded and match the dev shape.
        assert!(
            banner.contains("SET s3_endpoint='127.0.0.1:8333';"),
            "duckdb: {banner}"
        );
        assert!(
            banner.contains("aws --endpoint-url http://127.0.0.1:8333"),
            "aws cli: {banner}"
        );
    }

    #[test]
    fn init_banner_adds_system_flag_hint_in_system_scope() {
        // A system-scope install must hint `verglas start --system`, not the bare
        // form, so the operator drives the right scope.
        let paths = resolve_paths(Scope::System, Path::new("/home/op"));
        let banner = render_init_banner(
            "http://127.0.0.1:8333",
            Some("hyperglas"),
            &paths,
            Path::new("/home/op/.verglas"),
            Scope::System,
            "VGKEY",
            "secret",
        );
        assert!(
            banner.contains("verglas start --system"),
            "system hint: {banner}"
        );
        assert!(
            banner.contains("system service"),
            "names system scope: {banner}"
        );
    }

    #[test]
    fn parse_provider_normalizes_and_rejects_unknown() {
        assert_eq!(parse_provider("s3").expect("ok"), "s3");
        assert_eq!(parse_provider("AZURE").expect("ok"), "azure");
        assert_eq!(parse_provider(" Gcp ").expect("ok"), "gcp");
        assert!(parse_provider("dropbox").is_err());
    }

    #[test]
    fn parse_size_accepts_human_sizes_and_rejects_junk() {
        assert_eq!(parse_size("4GB").expect("ok"), 4 * 1024 * 1024 * 1024);
        assert!(parse_size("nonsense").is_err());
    }

    #[test]
    fn node_cp_status_line_reflects_registration_and_activity() {
        // Active: names the node id and says it is active in the control plane.
        let active = render_node_cp_status(&NodeCpStatus {
            node_id: "host-a".to_owned(),
            registered: true,
            active: true,
        });
        assert!(active.contains("host-a"), "names the node id: {active}");
        assert!(active.contains("active in the control plane"), "{active}");

        // Registered but stale.
        let stale = render_node_cp_status(&NodeCpStatus {
            node_id: "host-a".to_owned(),
            registered: true,
            active: false,
        });
        assert!(stale.contains("registered, inactive"), "{stale}");

        // Never registered.
        let missing = render_node_cp_status(&NodeCpStatus {
            node_id: "host-a".to_owned(),
            registered: false,
            active: false,
        });
        assert!(missing.contains("not yet registered"), "{missing}");
    }
}
