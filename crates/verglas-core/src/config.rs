//! The M1 server configuration: a small TOML schema with defaults, human-size
//! byte strings, and startup validation that names the exact field. Fields for
//! unbuilt features do not exist here — a config field lands in the same PR as
//! the feature it configures.

use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// Error loading or validating the configuration. Every variant renders one
/// actionable operator-facing line naming the file or field at fault.
#[derive(Debug)]
pub enum ConfigError {
    /// The config file could not be read.
    Read(PathBuf, std::io::Error),
    /// Not valid TOML for this schema; the TOML error carries the offending
    /// field name and line/column.
    Parse(Box<toml::de::Error>),
    /// A field parsed but fails a semantic check; names the field path.
    Invalid(&'static str, String),
}

impl fmt::Display for ConfigError {
    /// Renders the one-line operator-facing message.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Read(p, e) => write!(f, "cannot read config file {}: {e}", p.display()),
            ConfigError::Parse(e) => write!(f, "invalid config: {e}"),
            ConfigError::Invalid(field, p) => write!(f, "invalid config field `{field}`: {p}"),
        }
    }
}

impl std::error::Error for ConfigError {}

/// Root of the server configuration. Unknown fields anywhere are rejected at
/// parse time so a typo cannot silently become a default.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Ports the server binds and the S3 endpoint's TLS and addressing.
    #[serde(default)]
    pub listen: Listen,
    /// Structured-logging output format and default verbosity (#61).
    #[serde(default)]
    pub log: Log,
    /// Cache location, sizing, and the admission, warming, prefetch, and
    /// write-back policies.
    #[serde(default)]
    pub cache: Cache,
    /// Origin S3 store: endpoint, region, addressing, credentials, and the
    /// retry and circuit-breaker policies.
    #[serde(default)]
    pub backend: Backend,
    /// Credentials the S3 endpoint accepts from query engines. Unset generates
    /// an ephemeral keypair at startup and prints it once.
    pub auth: Option<Auth>,
    /// Iceberg REST catalog to track for table commits. Unset leaves Iceberg
    /// awareness off.
    pub catalog: Option<Catalog>,
    /// The standalone query role this server can dispatch `POST /v1/query` to
    /// instead of running it embedded (a `verglas-query` worker, launched on
    /// demand and killed after use). Unset keeps every query embedded —
    /// nothing about an existing deployment changes, and a dispatch failure
    /// for any reason still falls back to the embedded path. Everything the
    /// worker needs beyond this binary path (the cache S3 endpoint, the
    /// catalog URI, the signing keypair) the server derives from its own
    /// loopback surface and its own resolved auth — the same values the
    /// embedded path already uses.
    pub query_worker: Option<QueryWorker>,
    /// Isolated logical table writer launched by the server for Arrow commits.
    pub write_worker: Option<WriteWorker>,
    /// Cluster membership over gossip. Unset runs a single node.
    pub cluster: Option<Cluster>,
    /// The control plane the CLI authenticates against for cross-node and cloud
    /// deployment visibility. Written by `verglas login`; unset otherwise. The
    /// server does not use it, and the CLI's local commands never require it —
    /// only cloud verbs do.
    #[serde(default)]
    pub control_plane: Option<ControlPlane>,
}

/// The control plane the CLI talks to for the cross-node and cloud deployment
/// view. Its URL is recorded here by `verglas login`; the matching API key
/// lives in a mode-0600 credentials file, never in this config.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ControlPlane {
    /// Base URL of the control plane API (for example
    /// `https://api.verglas.dev`).
    pub url: String,
}

/// Ports the server binds: the S3 endpoint and the private admin API, plus the
/// S3 endpoint's TLS termination and virtual-hosted addressing (issue #11).
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct Listen {
    /// S3 endpoint port.
    pub s3_port: u16,
    /// Admin API port.
    pub admin_port: u16,
    /// Base domain for virtual-hosted-style addressing (bucket.domain).
    /// Requires wildcard DNS for the domain. Unset serves path-style
    /// addressing only.
    #[serde(default)]
    pub domain: Option<String>,
    /// TLS for the S3 endpoint. Unset serves plain HTTP. The server reloads the
    /// PEM files on SIGHUP without dropping connections.
    #[serde(default)]
    pub tls: Option<Tls>,
}

impl Default for Listen {
    /// The documented default: ports 8333 (S3) and 8334 (admin), plain HTTP,
    /// path-style addressing.
    fn default() -> Self {
        Listen {
            s3_port: 8333,
            admin_port: 8334,
            domain: None,
            tls: None,
        }
    }
}

/// Structured-logging output format and default verbosity (#61). The server
/// installs one `tracing` subscriber from this; the level is hot-reloadable via
/// the admin API and overridable at startup by `RUST_LOG`.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct Log {
    /// Output format. `json` emits one JSON object per line for log pipelines
    /// (jq / CloudWatch / Loki); `pretty` emits human-readable lines for local
    /// diagnostics.
    pub format: LogFormat,
    /// Default verbosity filter in `RUST_LOG` syntax (e.g. `info`, or
    /// `verglas_cache=debug,info`). `RUST_LOG` in the environment overrides it
    /// at startup; the admin API hot-reloads it at runtime.
    pub level: String,
}

impl Default for Log {
    /// JSON at `info`: machine-parseable by default, loud enough that a watcher
    /// or fill failure is visible without extra configuration (#241).
    fn default() -> Self {
        Log {
            format: LogFormat::Json,
            level: "info".to_owned(),
        }
    }
}

impl Log {
    /// Rejects an empty level; the string's filter syntax is validated by the
    /// subscriber when it is installed (the parser lives in `tracing`, not here).
    fn validate(&self) -> Result<(), ConfigError> {
        if self.level.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "log.level",
                "must be a non-empty RUST_LOG-style filter (e.g. `info`)".into(),
            ));
        }
        Ok(())
    }
}

/// Log output format (#61).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// One JSON object per line, for log pipelines.
    Json,
    /// Human-readable lines for local diagnostics.
    Pretty,
}

/// TLS termination for the S3 endpoint (issue #11). The server reads the leaf
/// certificate chain and private key from these PEM files and reloads them on
/// SIGHUP so a production cert rotation drops no connections.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    /// Path to the PEM certificate chain, leaf certificate first.
    pub cert_path: PathBuf,
    /// Path to the PEM private key.
    pub key_path: PathBuf,
}

impl Tls {
    /// Semantic checks: both PEM files exist and are readable regular files.
    /// The bytes are parsed when the listener binds; failing here first turns a
    /// typo'd path into an actionable startup error rather than a bind failure.
    fn validate(&self) -> Result<(), ConfigError> {
        for (field, path) in [
            ("listen.tls.cert_path", &self.cert_path),
            ("listen.tls.key_path", &self.key_path),
        ] {
            if !path.is_file() {
                return Err(ConfigError::Invalid(
                    field,
                    format!(
                        "{} does not exist or is not a readable file",
                        path.display()
                    ),
                ));
            }
        }
        Ok(())
    }
}

/// A naive base-domain check mirroring what the s3s host parser accepts: a
/// non-empty dotted name whose labels are ASCII alphanumeric or `-`, with an
/// optional `:port`. Rejects a scheme, a path, or whitespace early so a
/// malformed `listen.domain` is caught at startup, not at first request.
fn is_valid_base_domain(mut s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    if let Some((host, port)) = s.split_once(':') {
        if port.is_empty() || port.parse::<u16>().is_err() {
            return false;
        }
        s = host;
    }
    s.split('.').all(|label| {
        !label.is_empty()
            && label
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    })
}

/// Local cache directory and sizing. Byte fields accept human-size strings
/// like `"20GB"` (binary multiples) or plain integers.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct Cache {
    /// Directory for cached data. Must exist and be writable.
    pub dir: PathBuf,
    /// Max disk (NVMe) used for the cache.
    pub capacity_bytes: ByteSize,
    /// Max memory (DRAM) used for the cache.
    pub dram_bytes: ByteSize,
    /// Alignment and maximum fill size for data-cache blocks. Smaller blocks
    /// reduce Parquet range-read amplification; larger blocks reduce origin
    /// request count. Metadata uses its own store and is unaffected.
    #[serde(default = "default_data_block_bytes")]
    pub data_block_bytes: ByteSize,
    /// Fraction of the DRAM budget reserved for the metadata store, which holds
    /// table and file metadata isolated from data eviction. Range 0 to 1.
    #[serde(default = "default_meta_fraction")]
    pub meta_fraction: f64,
    /// Seconds a mutable object's key-to-ETag mapping is trusted before it is
    /// revalidated against the origin. Immutable data files are never
    /// revalidated. 0 revalidates on every read.
    #[serde(default = "default_mutable_mapping_ttl_secs")]
    pub mutable_mapping_ttl_secs: u64,
    /// Admission policy: keeps a large one-off scan from evicting the hot
    /// working set.
    #[serde(default)]
    pub admission: Admission,
    /// Metadata warming: reads a watched table's metadata into the cache before
    /// a query asks for it.
    #[serde(default)]
    pub warming: Warming,
    /// Prefetch: after a compaction commit, prefetches the rewritten files' hot
    /// chunks so the cache hit rate recovers without waiting for queries.
    #[serde(default)]
    pub prefetch: Prefetch,
    /// Erasure-coded write-back tier. Off by default; opted in per key prefix.
    #[serde(default)]
    pub writeback: Writeback,
}

/// Erasure-coded write-back configuration (#180).
///
/// When enabled, PUTs to an opted-in key prefix are acked once `w` Reed-Solomon
/// fragments (`k` data + `m` parity) are durable on `w` distinct live nodes,
/// with the object propagated to the origin in the background. When the live pod
/// cannot place `w` fragments the write degrades to synchronous write-through.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct Writeback {
    /// Enable the write-back tier. Off passes writes straight to the origin.
    pub enabled: bool,
    /// Data fragments per stripe.
    pub k: usize,
    /// Parity fragments per stripe (lost-fragment tolerance).
    pub m: usize,
    /// Fragment writes acknowledged before a write succeeds. Between k and k+m.
    pub w: usize,
    /// Milliseconds to wait for the write quorum before falling back to
    /// write-through.
    pub ack_deadline_ms: u64,
    /// How often the background scrubber walks stored write-back fragments to
    /// verify their checksums and re-encode any corrupt or missing ones, in
    /// seconds. Silent bit-rot fires no membership event, so node-loss repair
    /// never sees it; the scrubber must run often enough to repair a fault before
    /// enough independent faults accumulate to lose an object. Default 6 hours.
    #[serde(default = "default_scrub_interval_secs")]
    pub scrub_interval_secs: u64,
    /// Key prefixes that opt in to write-back. With the tier enabled, an empty
    /// list opts in every key at the default geometry.
    #[serde(default)]
    pub prefixes: Vec<WritebackPrefix>,
}

/// One opt-in key prefix, optionally overriding the default fragment geometry.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WritebackPrefix {
    /// Object keys starting with this prefix are eligible for write-back.
    pub prefix: String,
    /// Data fragments for this prefix. Defaults to the tier's k.
    pub k: Option<usize>,
    /// Parity fragments for this prefix. Defaults to the tier's m.
    pub m: Option<usize>,
    /// Acknowledgement quorum for this prefix. Defaults to the tier's w.
    pub w: Option<usize>,
}

impl Default for Writeback {
    /// Off, with the product-default geometry k=4, m=2, w=5 (acks tolerate one
    /// straggler and survive one post-ack node loss) and a 2 s ack deadline.
    /// There is no fragment sizing knob: fragments share the one NVMe budget
    /// (`cache.capacity_bytes`) with the block cache, first come first served
    /// (#223).
    fn default() -> Self {
        Writeback {
            enabled: false,
            k: 4,
            m: 2,
            w: 5,
            ack_deadline_ms: 2000,
            scrub_interval_secs: default_scrub_interval_secs(),
            prefixes: Vec::new(),
        }
    }
}

impl Writeback {
    /// Validates the write-back geometry. Rejects a geometry that can never
    /// reach quorum on any pod (`w > k+m`, more fragments than exist) or that
    /// cannot reconstruct from the acked set (`w < k`). Skipped entirely when
    /// disabled. Runs for the default geometry and every prefix override.
    fn validate(&self) -> Result<(), ConfigError> {
        if !self.enabled {
            return Ok(());
        }
        if self.ack_deadline_ms == 0 {
            return Err(ConfigError::Invalid(
                "cache.writeback.ack_deadline_ms",
                "must be at least 1".into(),
            ));
        }
        if self.scrub_interval_secs == 0 {
            return Err(ConfigError::Invalid(
                "cache.writeback.scrub_interval_secs",
                "must be at least 1".into(),
            ));
        }
        validate_geometry("cache.writeback", self.k, self.m, self.w)?;
        for prefix in &self.prefixes {
            let k = prefix.k.unwrap_or(self.k);
            let m = prefix.m.unwrap_or(self.m);
            let w = prefix.w.unwrap_or(self.w);
            validate_geometry("cache.writeback.prefixes", k, m, w)?;
        }
        Ok(())
    }
}

/// Validates one `(k, m, w)` erasure geometry, naming `field` on failure.
fn validate_geometry(field: &'static str, k: usize, m: usize, w: usize) -> Result<(), ConfigError> {
    if k == 0 {
        return Err(ConfigError::Invalid(field, "k must be at least 1".into()));
    }
    if m == 0 {
        return Err(ConfigError::Invalid(field, "m must be at least 1".into()));
    }
    if w < k {
        return Err(ConfigError::Invalid(
            field,
            format!("w={w} must be at least k={k} so the acked fragments reconstruct"),
        ));
    }
    if w > k + m {
        return Err(ConfigError::Invalid(
            field,
            format!(
                "w={w} exceeds k+m={}; the pod can never place {w} distinct fragments",
                k + m
            ),
        ));
    }
    Ok(())
}

/// The documented default metadata-store share: 5% of the cache.
fn default_meta_fraction() -> f64 {
    0.05
}

/// The documented default fragment scrub interval: 6 hours (#220). Short enough
/// to repair a bit-rot fault well before enough independent faults could
/// accumulate to cross `m` on one object; long enough that the periodic
/// re-encode is a negligible tenant on the NVMe and pod LAN.
fn default_scrub_interval_secs() -> u64 {
    6 * 60 * 60
}

/// The documented default mutable-mapping TTL: 5 seconds (#14).
fn default_mutable_mapping_ttl_secs() -> u64 {
    5
}

/// The measured default data-cache block size: 2 MiB.
fn default_data_block_bytes() -> ByteSize {
    ByteSize(DEFAULT_DATA_BLOCK_BYTES)
}

impl Default for Cache {
    /// The documented defaults: `/var/lib/verglas`, 20 GB disk, 1 GB DRAM,
    /// 2 MiB data blocks, and scan-resistant admission on.
    fn default() -> Self {
        Cache {
            dir: PathBuf::from("/var/lib/verglas"),
            capacity_bytes: ByteSize(20 * GB),
            dram_bytes: ByteSize(GB),
            data_block_bytes: default_data_block_bytes(),
            meta_fraction: default_meta_fraction(),
            mutable_mapping_ttl_secs: default_mutable_mapping_ttl_secs(),
            admission: Admission::default(),
            warming: Warming::default(),
            prefetch: Prefetch::default(),
            writeback: Writeback::default(),
        }
    }
}

impl Cache {
    /// Validates the data-block geometry shared by cache alignment, admission,
    /// and Foyer's data-store layout.
    fn validate(&self) -> Result<(), ConfigError> {
        let bytes = self.data_block_bytes.0;
        if !(MIN_DATA_BLOCK_BYTES..=MAX_DATA_BLOCK_BYTES).contains(&bytes)
            || !bytes.is_power_of_two()
        {
            return Err(ConfigError::Invalid(
                "cache.data_block_bytes",
                format!(
                    "must be a power of two from {MIN_DATA_BLOCK_BYTES} to {MAX_DATA_BLOCK_BYTES} bytes, got {bytes}"
                ),
            ));
        }
        Ok(())
    }
}

/// Snapshot-driven prefetch tuning (#51). On a compaction (`replace`) commit the
/// server prefetches the rewritten files' hot column chunks — scored against a
/// heat ledger fed from the request path — so the cache hit rate recovers before
/// queries re-earn it from the origin. One policy struct in `[cache]`; defaults
/// are conservative and polite.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct Prefetch {
    /// Prefetches data files after a compaction commit. Off leaves the cache to
    /// re-earn its hit rate as queries run.
    pub enabled: bool,
    /// Concurrent prefetch fetches.
    pub concurrency: usize,
    /// Bytes read from the end of a Parquet file to capture its footer.
    pub footer_read_bytes: ByteSize,
    /// Live reads a prefetch yields to before proceeding.
    pub organic_yield_k: usize,
    /// Maximum queued prefetch tasks. Beyond this, the lowest-priority tasks are
    /// dropped.
    pub max_queue: usize,
    /// Length of a heat-tracking epoch, in seconds.
    pub heat_epoch_secs: u64,
    /// Maximum objects tracked per table in the heat ledger.
    pub heat_table_cap: usize,
    /// Capacity of the channel carrying heat samples from the request path.
    pub heat_channel_capacity: usize,
}

impl Default for Prefetch {
    /// The documented defaults: on, 32 workers, a 64 KiB footer window, yield at
    /// 32 organic in-flight, 64k queue depth, 5-minute epochs, 64k entries/table,
    /// a 16k-deep sample channel.
    fn default() -> Self {
        Prefetch {
            enabled: true,
            concurrency: 32,
            footer_read_bytes: ByteSize(64 * 1024),
            organic_yield_k: 32,
            max_queue: 64 * 1024,
            heat_epoch_secs: 300,
            heat_table_cap: 64 * 1024,
            heat_channel_capacity: 16 * 1024,
        }
    }
}

impl Prefetch {
    /// Semantic checks: positive worker count, footer window covering the 8-byte
    /// Parquet trailer, and positive ledger/queue/channel sizes.
    fn validate(&self) -> Result<(), ConfigError> {
        if self.concurrency == 0 {
            return Err(ConfigError::Invalid(
                "cache.prefetch.concurrency",
                "must be at least 1".into(),
            ));
        }
        if self.footer_read_bytes.0 < 8 {
            return Err(ConfigError::Invalid(
                "cache.prefetch.footer_read_bytes",
                "must be at least 8 bytes (the Parquet trailer)".into(),
            ));
        }
        if self.organic_yield_k == 0 {
            return Err(ConfigError::Invalid(
                "cache.prefetch.organic_yield_k",
                "must be at least 1".into(),
            ));
        }
        if self.max_queue == 0 || self.heat_table_cap == 0 || self.heat_channel_capacity == 0 {
            return Err(ConfigError::Invalid(
                "cache.prefetch",
                "max_queue, heat_table_cap, and heat_channel_capacity must be positive".into(),
            ));
        }
        if self.heat_epoch_secs == 0 {
            return Err(ConfigError::Invalid(
                "cache.prefetch.heat_epoch_secs",
                "must be at least 1".into(),
            ));
        }
        Ok(())
    }
}

/// Eager metadata warming tuning (#168). Warming reads a watched table's
/// metadata (manifest list, manifests, Parquet footers) through the cache on
/// onboarding and on every commit, so cold planning walks hit the pinned store.
/// One policy struct in `[cache]`; defaults are conservative.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct Warming {
    /// Warms watched tables' metadata. Off leaves metadata to be filled by the
    /// first query that needs it.
    pub enabled: bool,
    /// Concurrent warming fetches.
    pub concurrency: usize,
    /// Bytes read from the end of a Parquet file to capture its footer.
    pub footer_read_bytes: ByteSize,
    /// Warming throughput limit, in bytes per second. 0 is unlimited.
    pub byte_budget_bytes_per_sec: ByteSize,
}

impl Default for Warming {
    /// The documented defaults: on, 64 in flight, a 64 KiB footer window, and a
    /// 256 MiB/s byte ceiling.
    fn default() -> Self {
        Warming {
            enabled: true,
            concurrency: 64,
            footer_read_bytes: ByteSize(64 * 1024),
            byte_budget_bytes_per_sec: ByteSize(256 * 1024 * 1024),
        }
    }
}

impl Warming {
    /// Semantic checks: concurrency must be positive and the footer window must
    /// cover at least the 8-byte Parquet trailer.
    fn validate(&self) -> Result<(), ConfigError> {
        if self.concurrency == 0 {
            return Err(ConfigError::Invalid(
                "cache.warming.concurrency",
                "must be at least 1".into(),
            ));
        }
        if self.footer_read_bytes.0 < 8 {
            return Err(ConfigError::Invalid(
                "cache.warming.footer_read_bytes",
                "must be at least 8 bytes (the Parquet trailer)".into(),
            ));
        }
        Ok(())
    }
}

/// Scan-resistant admission tuning (#15). A one-touch bulk read (a full-table
/// scan larger than the cache) must not evict the frequently-reused working
/// set, so once the cache has filled, a block is admitted only if a compact
/// frequency sketch shows it has been seen at least `frequency_threshold`
/// times. One policy struct in `[cache]`; defaults on. The metadata mapping
/// cache is deliberately *not* subject to this — mappings are tiny and
/// disproportionately valuable, so they are always admitted.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct Admission {
    /// Turns scan-resistant admission on. Off admits every fetched block
    /// unconditionally.
    pub enabled: bool,
    /// Accesses a block must reach before it is admitted while the cache is
    /// under pressure. 2 admits on the second sighting, so a one-touch scan
    /// never gets in. 1 admits everything.
    pub frequency_threshold: u32,
    /// Probability of admitting an eligible block while the cache is churning.
    /// Below 1, a cyclic scan larger than the cache is thinned so a stable
    /// resident set survives. Range above 0 up to 1; 1 admits every eligible
    /// block.
    pub churn_admit_probability: f64,
}

impl Default for Admission {
    /// The documented default: scan resistance on, admit on the second sight.
    fn default() -> Self {
        Admission {
            enabled: true,
            frequency_threshold: 2,
            churn_admit_probability: 1.0,
        }
    }
}

impl Admission {
    /// Semantic check: the frequency threshold must be at least one (zero would
    /// admit before a block is ever seen, which is meaningless).
    fn validate(&self) -> Result<(), ConfigError> {
        if self.frequency_threshold == 0 {
            return Err(ConfigError::Invalid(
                "cache.admission.frequency_threshold",
                "must be at least 1".into(),
            ));
        }
        // Probability is a fraction of a resident's slot: zero would admit
        // nothing under pressure (the cache could never refill after eviction),
        // above one is not a probability. NaN fails both comparisons, so the
        // `!(0 < p <= 1)` form rejects it too.
        if !(self.churn_admit_probability > 0.0 && self.churn_admit_probability <= 1.0) {
            return Err(ConfigError::Invalid(
                "cache.admission.churn_admit_probability",
                "must lie in (0, 1]".into(),
            ));
        }
        Ok(())
    }
}

/// The object-store provider whose bytes the backend serves. `s3` covers AWS,
/// OCI, and MinIO — they differ only by endpoint and region. `azure` is Azure
/// Blob Storage, `gcp` is Google Cloud Storage. The provider picks which
/// object-store client the backend builds and which credential format the
/// credentials file uses.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, schemars::JsonSchema, Default,
)]
#[serde(rename_all = "lowercase")]
pub enum BackendProvider {
    /// AWS S3 and any S3-compatible store (OCI, MinIO). The default.
    #[default]
    S3,
    /// Azure Blob Storage.
    Azure,
    /// Google Cloud Storage.
    Gcp,
}

/// Origin object-store settings. The server serves a configured SET of buckets
/// (#235): the single `bucket` for the common one-bucket case, plus
/// `bucket_globs` — glob patterns (`*` matches any run of characters) for
/// dynamic families like AWS S3 Tables' `*--table-s3` buckets. A request whose
/// bucket equals `bucket` or matches a glob is served; any other bucket returns
/// `NoSuchBucket`. At least one of the two must be set. One credential set (the
/// provider's chain, or `credentials_file`) covers the whole set. Carries the
/// provider, the request-concurrency ceiling, and the origin
/// endpoint/region/addressing/credentials overrides (#221).
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct Backend {
    /// The object-store provider: `s3` (AWS/OCI/MinIO), `azure`, or `gcp`.
    /// Picks the client the backend builds and the credentials-file format.
    #[serde(default)]
    pub provider: BackendProvider,
    /// Maximum in-flight requests to the origin bucket, so a miss storm cannot
    /// exhaust the origin's connection pool.
    pub max_concurrent_requests: usize,
    /// Retry and backoff policy for transient origin failures.
    pub retry: RetryPolicy,
    /// Circuit breaker: sheds load from an origin that keeps failing, while
    /// cache hits keep serving.
    pub breaker: BreakerPolicy,
    /// A single bucket or container this server serves, no scheme prefix. The
    /// common one-bucket case. At least one of `bucket` / `bucket_globs` is
    /// required; the served set is their union.
    #[serde(default)]
    pub bucket: Option<String>,
    /// Glob patterns naming a family of buckets this server serves. `*` matches
    /// any run of characters, so `"*--table-s3"` serves every AWS S3 Tables
    /// underlying bucket under one credential set. A request whose bucket matches
    /// any glob is served; anything outside the set returns NoSuchBucket.
    #[serde(default)]
    pub bucket_globs: Vec<String>,
    /// Origin S3 endpoint. Unset uses AWS.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Signing region. Unset defaults to us-east-1.
    #[serde(default)]
    pub region: Option<String>,
    /// Allow a plaintext http:// origin endpoint.
    #[serde(default)]
    pub allow_http: bool,
    /// Address buckets as virtual hosts (bucket.host) rather than path-style
    /// (host/bucket).
    #[serde(default)]
    pub virtual_hosted_style: bool,
    /// Origin credentials file (mode 0600), in the provider's format: AWS-INI for
    /// s3, a key=value file for azure, a service-account path for gcp. Unset uses
    /// the provider's ambient env chain. Secret keys never live in this config.
    #[serde(default)]
    pub credentials_file: Option<String>,
    /// Profile in the credentials file. Unset uses default. S3 only; azure and
    /// gcp credentials files carry a single credential.
    #[serde(default)]
    pub credentials_profile: Option<String>,
}

impl Default for Backend {
    /// The documented default: no bucket named, the standard concurrency
    /// ceiling, the default retry/breaker policies, and no endpoint/region/
    /// credentials override (the client falls back to the AWS env chain).
    fn default() -> Self {
        Backend {
            provider: BackendProvider::default(),
            max_concurrent_requests: default_max_concurrent_requests(),
            retry: RetryPolicy::default(),
            breaker: BreakerPolicy::default(),
            bucket: None,
            bucket_globs: Vec::new(),
            endpoint: None,
            region: None,
            allow_http: false,
            virtual_hosted_style: false,
            credentials_file: None,
            credentials_profile: None,
        }
    }
}

impl Backend {
    /// Whether this server serves `bucket`: it equals the single `bucket` or
    /// matches one of the `bucket_globs` (#235). The one gate every serving path
    /// consults so a request outside the configured set is rejected loudly.
    pub fn serves_bucket(&self, bucket: &str) -> bool {
        if self.bucket.as_deref() == Some(bucket) {
            return true;
        }
        self.bucket_globs
            .iter()
            .any(|pattern| crate::glob::matches(pattern, bucket))
    }

    /// A one-line description of the configured bucket set for the startup log:
    /// the single bucket and/or the glob list, or `<unset>` when neither is set
    /// (which fails validation before serving).
    pub fn describe_bucket_set(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if let Some(bucket) = self.bucket.as_deref().filter(|b| !b.is_empty()) {
            parts.push(bucket.to_owned());
        }
        for glob in &self.bucket_globs {
            parts.push(format!("glob:{glob}"));
        }
        if parts.is_empty() {
            "<unset>".to_owned()
        } else {
            parts.join(",")
        }
    }

    /// Semantic checks on the origin settings: at least one of `bucket` /
    /// `bucket_globs` is set (the server needs a bucket set to serve and refuses
    /// to start without one), no glob pattern is empty, a configured `http://`
    /// endpoint must set `allow_http`, and a configured endpoint must be a
    /// parseable `http`/`https` URL. The endpoint checks are provider-agnostic —
    /// the http gate applies to every provider. Credentials are not required
    /// here: the provider's env chain stays a valid fallback, so credential
    /// resolution happens at client build, not validate.
    fn validate(&self) -> Result<(), ConfigError> {
        let has_bucket = self.bucket.as_deref().is_some_and(|b| !b.is_empty());
        if !has_bucket && self.bucket_globs.is_empty() {
            return Err(ConfigError::Invalid(
                "backend.bucket",
                "at least one of backend.bucket or backend.bucket_globs is required: name the bucket(s) this server serves (no scheme prefix)"
                    .into(),
            ));
        }
        if self.bucket_globs.iter().any(|glob| glob.is_empty()) {
            return Err(ConfigError::Invalid(
                "backend.bucket_globs",
                "a bucket glob must be non-empty".into(),
            ));
        }
        if let Some(endpoint) = &self.endpoint {
            let scheme = endpoint.split_once("://").map(|(s, _)| s).unwrap_or("");
            match scheme {
                "https" => {}
                "http" => {
                    if !self.allow_http {
                        return Err(ConfigError::Invalid(
                            "backend.endpoint",
                            format!(
                                "`{endpoint}` uses http; set backend.allow_http = true to permit a plaintext endpoint"
                            ),
                        ));
                    }
                }
                other => {
                    return Err(ConfigError::Invalid(
                        "backend.endpoint",
                        format!("`{endpoint}` must be an http:// or https:// URL, not `{other}`"),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Default backend request-concurrency ceiling: high enough to saturate a
/// node's fill bandwidth, low enough to stay a polite tenant of the origin's
/// connection pool.
fn default_max_concurrent_requests() -> usize {
    64
}

/// How verglas-backend retries a transient origin failure: a bounded number of
/// jittered exponential-backoff attempts within a wall-clock budget. One policy
/// struct, not a scatter of knobs — the retry layer reads it whole.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct RetryPolicy {
    /// Retry attempts after the first try. 0 disables retry.
    pub max_retries: usize,
    /// Delay before the first retry, in milliseconds. Each subsequent delay
    /// grows exponentially with jitter, capped at max_backoff_ms.
    pub initial_backoff_ms: u64,
    /// Upper bound on a single backoff delay, in milliseconds.
    pub max_backoff_ms: u64,
    /// Total time budget for a request including retries, in milliseconds. Once
    /// elapsed, the last error is returned even if attempts remain.
    pub budget_ms: u64,
}

impl Default for RetryPolicy {
    /// The documented default: up to 3 retries, 100 ms→3 s jittered backoff,
    /// 10 s total budget.
    fn default() -> Self {
        RetryPolicy {
            max_retries: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 3_000,
            budget_ms: 10_000,
        }
    }
}

impl RetryPolicy {
    /// Semantic checks: a non-zero starting delay, a ceiling no smaller than
    /// it, and a non-zero budget. `max_retries` needs no check — zero is the
    /// legitimate "no retry" setting.
    fn validate(&self) -> Result<(), ConfigError> {
        if self.initial_backoff_ms == 0 {
            return Err(ConfigError::Invalid(
                "backend.retry.initial_backoff_ms",
                "must be at least 1".into(),
            ));
        }
        if self.max_backoff_ms < self.initial_backoff_ms {
            return Err(ConfigError::Invalid(
                "backend.retry.max_backoff_ms",
                format!("must be ≥ initial_backoff_ms ({})", self.initial_backoff_ms),
            ));
        }
        if self.budget_ms == 0 {
            return Err(ConfigError::Invalid(
                "backend.retry.budget_ms",
                "must be at least 1".into(),
            ));
        }
        Ok(())
    }
}

/// Per-bucket circuit-breaker tuning. The breaker watches a rolling window of
/// backend outcomes; when the failure rate over a large enough sample crosses
/// the threshold it opens, sheds load for a cooldown, then half-opens to probe
/// recovery. One policy struct feeds the whole state machine.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct BreakerPolicy {
    /// Failure fraction over the sample window that opens the breaker. Range
    /// above 0 up to 1.
    pub failure_rate: f64,
    /// Minimum requests in the window before the failure rate is trusted.
    pub min_samples: u32,
    /// Time the breaker stays open before it probes the origin again, in
    /// milliseconds.
    pub cooldown_ms: u64,
    /// Consecutive probe successes that close the breaker, and the cap on
    /// probes in flight while it is probing.
    pub half_open_max_probes: u32,
}

impl Default for BreakerPolicy {
    /// The documented default: trip at 50% failures over ≥20 samples, 5 s
    /// cooldown, 3 successful probes to recover.
    fn default() -> Self {
        BreakerPolicy {
            failure_rate: 0.5,
            min_samples: 20,
            cooldown_ms: 5_000,
            half_open_max_probes: 3,
        }
    }
}

impl BreakerPolicy {
    /// Semantic checks: a failure rate in (0,1], and non-zero sample floor,
    /// cooldown, and probe count (each zero would make the state machine
    /// degenerate — never trip, never recover, or divide by zero).
    fn validate(&self) -> Result<(), ConfigError> {
        if !(self.failure_rate > 0.0 && self.failure_rate <= 1.0) {
            return Err(ConfigError::Invalid(
                "backend.breaker.failure_rate",
                "must be in the range (0.0, 1.0]".into(),
            ));
        }
        if self.min_samples == 0 {
            return Err(ConfigError::Invalid(
                "backend.breaker.min_samples",
                "must be at least 1".into(),
            ));
        }
        if self.cooldown_ms == 0 {
            return Err(ConfigError::Invalid(
                "backend.breaker.cooldown_ms",
                "must be at least 1".into(),
            ));
        }
        if self.half_open_max_probes == 0 {
            return Err(ConfigError::Invalid(
                "backend.breaker.half_open_max_probes",
                "must be at least 1".into(),
            ));
        }
        Ok(())
    }
}

/// Static access key pair the S3 endpoint will accept from engines.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Auth {
    /// Endpoint keypair file (AWS format, mode 0600). The keypair is never
    /// stored in this config.
    pub credentials_file: String,
    /// Profile in the credentials file. Unset uses default.
    #[serde(default)]
    pub credentials_profile: Option<String>,
}

/// Iceberg REST catalog watcher settings (#47). Filters match against the
/// dotted `namespace.table` name with `*` wildcards; empty `include` means
/// every table, and `exclude` always wins over `include`.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Catalog {
    /// Base URI of the Iceberg REST catalog. Unset leaves Iceberg awareness off.
    pub uri: String,
    /// Seconds between catalog polls.
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,
    /// Tables to watch, as namespace.table globs. Empty watches everything.
    #[serde(default)]
    pub include: Vec<String>,
    /// Tables to skip, as namespace.table globs. A match here always wins over
    /// include.
    #[serde(default)]
    pub exclude: Vec<String>,
    /// Credentials file for catalog auth. In bearer mode it holds the bearer
    /// token; in SigV4 mode (see `sigv4_region`) it is an AWS-INI credentials
    /// file, the same format as `[backend]`. Keep it mode 0600; secrets never
    /// live in this config. Unset in SigV4 mode uses the ambient AWS chain.
    #[serde(default)]
    pub credentials_file: Option<String>,
    /// Profile in the AWS-INI `credentials_file` for SigV4 mode. Unset uses the
    /// default profile. Ignored in bearer mode (the file holds one token).
    #[serde(default)]
    pub credentials_profile: Option<String>,
    /// Inline bearer token sent on catalog requests. Prefer credentials_file;
    /// setting both is an error, and it is mutually exclusive with SigV4.
    #[serde(default)]
    pub bearer_token: Option<String>,
    /// SigV4 signing region for an AWS-backed catalog (S3 Tables / Glue), e.g.
    /// `us-west-2`. Set together with `sigv4_signing_name` to sign every catalog
    /// request; do not also set a bearer token.
    #[serde(default)]
    pub sigv4_region: Option<String>,
    /// SigV4 signing name (service) for an AWS-backed catalog, e.g. `s3tables`
    /// for S3 Tables or `glue` for the Glue Iceberg REST endpoint. Set together
    /// with `sigv4_region`.
    #[serde(default)]
    pub sigv4_signing_name: Option<String>,
    /// Warehouse identifier, required by multi-warehouse catalogs. For S3 Tables
    /// this is the table-bucket ARN passed to `/v1/config`.
    #[serde(default)]
    pub warehouse: Option<String>,
}

impl Catalog {
    /// Whether SigV4 signing is configured: both `sigv4_region` and
    /// `sigv4_signing_name` are set. In that case `credentials_file` is an
    /// AWS-INI file (not a bearer token) and no bearer token is resolved.
    pub fn sigv4_enabled(&self) -> bool {
        self.sigv4_region.is_some() && self.sigv4_signing_name.is_some()
    }

    /// Resolves the effective bearer token: none in SigV4 mode, else the trimmed
    /// contents of `credentials_file` when set, else the inline `bearer_token`,
    /// else none. Setting both file and inline is rejected — loud beats a silent
    /// precedence. Reading the file here (not at parse time) keeps
    /// `Config::from_toml_str` pure.
    pub fn resolve_bearer_token(&self) -> Result<Option<String>, ConfigError> {
        if self.sigv4_enabled() {
            return Ok(None);
        }
        match (&self.credentials_file, &self.bearer_token) {
            (Some(file), None) => {
                let text = std::fs::read_to_string(file).map_err(|e| {
                    ConfigError::Invalid(
                        "catalog.credentials_file",
                        format!("cannot read `{file}`: {e}"),
                    )
                })?;
                Ok(Some(text.trim().to_owned()))
            }
            (None, Some(token)) => Ok(Some(token.clone())),
            (None, None) => Ok(None),
            (Some(_), Some(_)) => Err(ConfigError::Invalid(
                "catalog.credentials_file",
                "set either catalog.credentials_file or catalog.bearer_token, not both".into(),
            )),
        }
    }

    /// Semantic checks: the URI is an http/https URL, the poll interval is
    /// positive, the SigV4 pair is set together and not alongside a bearer token,
    /// at most one bearer-token source is set, and a named credentials file
    /// exists as a readable regular file (its bytes are read when the watcher
    /// starts; failing here first turns a typo'd path into an actionable startup
    /// error).
    fn validate(&self) -> Result<(), ConfigError> {
        if !self.uri.starts_with("http://") && !self.uri.starts_with("https://") {
            return Err(ConfigError::Invalid(
                "catalog.uri",
                format!("`{}` must start with http:// or https://", self.uri),
            ));
        }
        if self.poll_interval_secs == 0 {
            return Err(ConfigError::Invalid(
                "catalog.poll_interval_secs",
                "poll interval must be at least 1 second".into(),
            ));
        }
        // SigV4 needs both a region and a signing name — one without the other is
        // a mistake. Name whichever is missing.
        match (&self.sigv4_region, &self.sigv4_signing_name) {
            (Some(_), None) => {
                return Err(ConfigError::Invalid(
                    "catalog.sigv4_signing_name",
                    "SigV4 needs both catalog.sigv4_region and catalog.sigv4_signing_name".into(),
                ));
            }
            (None, Some(_)) => {
                return Err(ConfigError::Invalid(
                    "catalog.sigv4_region",
                    "SigV4 needs both catalog.sigv4_region and catalog.sigv4_signing_name".into(),
                ));
            }
            _ => {}
        }
        // SigV4 and a bearer token are two different auth modes; setting both is
        // ambiguous. In SigV4 mode `credentials_file` is an AWS-INI file, not a
        // bearer token, so it is allowed alongside SigV4.
        if self.sigv4_enabled() && self.bearer_token.is_some() {
            return Err(ConfigError::Invalid(
                "catalog.sigv4_region",
                "SigV4 signing is mutually exclusive with catalog.bearer_token".into(),
            ));
        }
        // Bearer mode: at most one token source (a file or an inline token).
        if !self.sigv4_enabled() && self.credentials_file.is_some() && self.bearer_token.is_some() {
            return Err(ConfigError::Invalid(
                "catalog.credentials_file",
                "set either catalog.credentials_file or catalog.bearer_token, not both".into(),
            ));
        }
        if let Some(file) = &self.credentials_file
            && !Path::new(file).is_file()
        {
            return Err(ConfigError::Invalid(
                "catalog.credentials_file",
                format!("{file} does not exist or is not a readable file"),
            ));
        }
        Ok(())
    }
}

/// The documented default poll interval: 30 seconds.
fn default_poll_interval_secs() -> u64 {
    30
}

/// The standalone query worker a server can dispatch to (see [`Config::query_worker`]).
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryWorker {
    /// Path to the `verglas-query` binary this server spawns on demand.
    pub binary: String,
}

/// The standalone logical write worker a server dispatches Arrow commits to.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WriteWorker {
    /// Path to the `verglas-write` binary spawned on demand.
    pub binary: String,
}

impl WriteWorker {
    /// Validates the configured role binary before the first write request.
    fn validate(&self) -> Result<(), ConfigError> {
        if !Path::new(&self.binary).is_file() {
            return Err(ConfigError::Invalid(
                "write_worker.binary",
                format!("{} does not exist or is not a readable file", self.binary),
            ));
        }
        Ok(())
    }
}

impl QueryWorker {
    /// The binary exists as a readable file. Failing here first turns a
    /// typo'd path into an actionable startup error instead of a dispatch
    /// failure discovered on the first query (which falls back to the
    /// embedded path silently, so a broken path would otherwise go unnoticed).
    fn validate(&self) -> Result<(), ConfigError> {
        if !Path::new(&self.binary).is_file() {
            return Err(ConfigError::Invalid(
                "query_worker.binary",
                format!("{} does not exist or is not a readable file", self.binary),
            ));
        }
        Ok(())
    }
}

/// Gossip cluster membership and ring participation (#27/#28).
///
/// A node advertises its identity and capacity over `chitchat` gossip and
/// takes a weighted-rendezvous share of the pod's keyspace. The whole table is
/// optional: without it a server is a cluster-of-one. These fields land with
/// the feature — the server reads them only when gossip is wired up.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Cluster {
    /// Stable cluster identity used in node reports. Unset (or no `[cluster]`
    /// table at all) falls back to the machine hostname; the
    /// `VERGLAS_CLUSTER_ID` environment variable overrides both.
    #[serde(default)]
    pub id: Option<String>,
    /// Pod identity. Peers join only within the same pod.
    #[serde(default = "default_pod_id")]
    pub pod_id: String,
    /// Stable node identity. Unset derives one from the hostname.
    pub node_id: Option<String>,
    /// host:port this node binds for gossip.
    pub gossip_addr: String,
    /// host:port peers use to fetch blocks this node owns.
    pub advertise_addr: Option<String>,
    /// Gossip addresses to contact on join. Empty for a bootstrap node.
    #[serde(default)]
    pub seeds: Vec<String>,
    /// This node's keyspace share. Unset uses its cache size.
    pub weight: Option<u64>,
    /// Failure-detection window in seconds. A silent node is dropped from the
    /// ring within a small multiple of this.
    #[serde(default = "default_suspicion_secs")]
    pub suspicion_secs: u64,
    /// Shared secret peers present on every peer request. Unset accepts
    /// unauthenticated peer requests; set it on any untrusted network.
    pub secret: Option<String>,
    /// Connect timeout for a peer fetch, in milliseconds. A peer that does not
    /// answer in this window is skipped for an origin fill.
    #[serde(default = "default_peer_connect_timeout_ms")]
    pub peer_connect_timeout_ms: u64,
    /// Total timeout for a peer fetch, in milliseconds. Past it the fetch is
    /// abandoned and the read fills from the origin.
    #[serde(default = "default_peer_request_timeout_ms")]
    pub peer_request_timeout_ms: u64,
    /// Warm-from-peers window in seconds after joining a pod: an owned block
    /// missing locally is pulled from a peer rather than the origin. 0 disables
    /// it.
    #[serde(default = "default_warm_from_peers_secs")]
    pub warm_from_peers_secs: u64,
    /// Drain window in seconds: a draining node keeps serving as a donor for at
    /// most this long before exiting.
    #[serde(default = "default_drain_timeout_secs")]
    pub drain_timeout_secs: u64,
}

/// The documented default pod identity when `[cluster]` names none.
fn default_pod_id() -> String {
    "default".to_owned()
}

/// The documented default failure-detection window: 5 seconds.
fn default_suspicion_secs() -> u64 {
    5
}

/// The documented default peer-fetch connect budget: 5 ms.
fn default_peer_connect_timeout_ms() -> u64 {
    5
}

/// The documented default peer-fetch total budget: 50 ms.
fn default_peer_request_timeout_ms() -> u64 {
    50
}

/// The documented default warm-from-peers window: 5 minutes — long enough for a
/// steady workload to re-touch (and pull) its hot working set after a join.
fn default_warm_from_peers_secs() -> u64 {
    300
}

/// The documented default drain donor window: 10 minutes (the issue's example
/// `--timeout 10m`).
fn default_drain_timeout_secs() -> u64 {
    600
}

/// The environment variable that overrides the configured (or hostname-derived)
/// cluster id.
pub const CLUSTER_ID_ENV: &str = "VERGLAS_CLUSTER_ID";

/// The machine hostname via POSIX `gethostname`, trimmed at the first NUL, or
/// `None` when it cannot be read or is empty/non-UTF-8. The single-node default
/// for the cluster id when neither the env override nor `[cluster].id` is set.
#[cfg(unix)]
pub fn machine_hostname() -> Option<String> {
    let mut buf = vec![0u8; 256];
    // SAFETY: `buf` is a valid writable buffer of `buf.len()` bytes; gethostname
    // writes at most that many and NUL-terminates when it fits.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if rc != 0 {
        return None;
    }
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    buf.truncate(end);
    String::from_utf8(buf).ok().filter(|s| !s.trim().is_empty())
}

/// The machine hostname on non-Unix platforms. `libc::gethostname` is not
/// available there, so read Windows's `COMPUTERNAME` env var; other platforms
/// return `None` and the cluster id falls back to `"localhost"`.
#[cfg(not(unix))]
pub fn machine_hostname() -> Option<String> {
    std::env::var("COMPUTERNAME")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// Pure cluster-id resolution: the env override wins, then the configured id,
/// then the hostname, then a `"localhost"` fallback. A blank string at any level
/// is treated as unset so an empty override or config never wins.
fn resolve_cluster_id_from(
    env: Option<String>,
    configured: Option<String>,
    hostname: Option<String>,
) -> String {
    [env, configured, hostname]
        .into_iter()
        .flatten()
        .map(|s| s.trim().to_owned())
        .find(|s| !s.is_empty())
        .unwrap_or_else(|| "localhost".to_owned())
}

impl Cluster {
    /// Semantic checks: the gossip address and any advertised peer address
    /// parse as socket addresses, an explicit weight is non-zero, and the
    /// suspicion window is at least one second. Seed entries are *not* parsed
    /// — they may be DNS names resolved at runtime (#37).
    fn validate(&self) -> Result<(), ConfigError> {
        if self.gossip_addr.parse::<std::net::SocketAddr>().is_err() {
            return Err(ConfigError::Invalid(
                "cluster.gossip_addr",
                format!("`{}` is not a host:port socket address", self.gossip_addr),
            ));
        }
        if let Some(addr) = &self.advertise_addr
            && addr.parse::<std::net::SocketAddr>().is_err()
        {
            return Err(ConfigError::Invalid(
                "cluster.advertise_addr",
                format!("`{addr}` is not a host:port socket address"),
            ));
        }
        if self.weight == Some(0) {
            return Err(ConfigError::Invalid(
                "cluster.weight",
                "must be at least 1 (a zero-weight node would own no keyspace)".into(),
            ));
        }
        if self.suspicion_secs == 0 {
            return Err(ConfigError::Invalid(
                "cluster.suspicion_secs",
                "must be at least 1 second".into(),
            ));
        }
        if self.peer_connect_timeout_ms == 0 {
            return Err(ConfigError::Invalid(
                "cluster.peer_connect_timeout_ms",
                "must be at least 1 millisecond".into(),
            ));
        }
        if self.peer_request_timeout_ms == 0 {
            return Err(ConfigError::Invalid(
                "cluster.peer_request_timeout_ms",
                "must be at least 1 millisecond".into(),
            ));
        }
        Ok(())
    }
}

/// One binary gigabyte, the unit of the documented defaults.
const GB: u64 = 1024 * 1024 * 1024;

/// Smallest supported data-cache block: fine enough for Parquet page ranges
/// without making the Foyer index unbounded.
pub const MIN_DATA_BLOCK_BYTES: u64 = 1024 * 1024;

/// Largest supported data-cache block: the S3 range-request upper sweet spot.
pub const MAX_DATA_BLOCK_BYTES: u64 = 8 * 1024 * 1024;

/// Default data-cache block: measured against local Iceberg TPC-DS SF1000.
pub const DEFAULT_DATA_BLOCK_BYTES: u64 = 2 * 1024 * 1024;

/// A byte count deserialized from a human-size string (`"512MB"`, `"20GB"`;
/// suffixes B/KB/MB/GB/TB, binary multiples; a bare number means bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteSize(pub u64);

impl schemars::JsonSchema for ByteSize {
    /// Named so the reference page and JSON Schema call it `ByteSize`.
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ByteSize".into()
    }

    /// A `ByteSize` deserializes from a string, so its schema is a string: a
    /// byte count (`"21474836480"`) or a suffixed size (`"20GB"`).
    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "description": "A byte count (\"1048576\") or a binary-suffixed size (\"20GB\", \"512MB\").",
        })
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    /// Delegates to [`parse_bytes`], surfacing its message as the TOML error.
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let s = String::deserialize(de)?;
        parse_bytes(&s)
            .map(ByteSize)
            .map_err(serde::de::Error::custom)
    }
}

impl serde::Serialize for ByteSize {
    /// Serializes as the plain byte-count string (e.g. `"21474836480"`), the
    /// form the deserializer accepts, so a serialized config round-trips.
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&self.0.to_string())
    }
}

/// Parses `"20GB"`-style sizes; the error text lists the accepted suffixes.
fn parse_bytes(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let digits_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let unit = match s[digits_end..].trim().to_ascii_uppercase().as_str() {
        "" | "B" => 1,
        "KB" => 1024,
        "MB" => 1024 * 1024,
        "GB" => GB,
        "TB" => 1024 * GB,
        other => {
            return Err(format!(
                "unknown size suffix `{other}`; expected B/KB/MB/GB/TB"
            ));
        }
    };
    let n: u64 = s[..digits_end]
        .parse()
        .map_err(|_| format!("`{s}` is not a size like \"20GB\" or a byte count"))?;
    n.checked_mul(unit)
        .ok_or_else(|| format!("`{s}` overflows a byte count"))
}

impl Config {
    /// Reads, parses, and validates a config file — the one call `verglas-server`
    /// startup makes. Any error is ready to print and exit on.
    pub fn load(path: &Path) -> Result<Config, ConfigError> {
        let text =
            std::fs::read_to_string(path).map_err(|e| ConfigError::Read(path.to_owned(), e))?;
        let config = Config::from_toml_str(&text)?;
        config.validate()?;
        Ok(config)
    }

    /// Parses a TOML document without validating. Building block for
    /// [`Config::load`] and a hook for tests.
    pub fn from_toml_str(text: &str) -> Result<Config, ConfigError> {
        toml::from_str(text).map_err(|e| ConfigError::Parse(Box::new(e)))
    }

    /// Resolves the cluster id this server stamps on the vector-index registry
    /// rows it writes (and filters by on reboot): the `VERGLAS_CLUSTER_ID`
    /// environment variable if set, else the configured `[cluster].id`, else the
    /// machine hostname, else `"localhost"`. Resolved once at server start.
    pub fn resolve_cluster_id(&self) -> String {
        let env = std::env::var(CLUSTER_ID_ENV).ok();
        let configured = self.cluster.as_ref().and_then(|c| c.id.clone());
        resolve_cluster_id_from(env, configured, machine_hostname())
    }

    /// The complete M1 semantic checks: the backend names a bucket set (a
    /// `bucket` and/or `bucket_globs` — #235), the backend concurrency ceiling
    /// is at least one, the cache dir exists and is writable (probed with a real
    /// file create, startup only), and the ports are non-zero and distinct.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.backend.max_concurrent_requests == 0 {
            return Err(ConfigError::Invalid(
                "backend.max_concurrent_requests",
                "must be at least 1".into(),
            ));
        }
        self.log.validate()?;
        self.backend.retry.validate()?;
        self.backend.breaker.validate()?;
        self.backend.validate()?;
        self.cache.validate()?;
        self.cache.admission.validate()?;
        self.cache.warming.validate()?;
        self.cache.prefetch.validate()?;
        self.cache.writeback.validate()?;
        if !(self.cache.meta_fraction > 0.0 && self.cache.meta_fraction < 1.0) {
            return Err(ConfigError::Invalid(
                "cache.meta_fraction",
                format!(
                    "must be in the range (0.0, 1.0) exclusive, got {}",
                    self.cache.meta_fraction
                ),
            ));
        }
        if !self.cache.dir.is_dir() {
            return Err(ConfigError::Invalid(
                "cache.dir",
                format!(
                    "{} does not exist or is not a directory",
                    self.cache.dir.display()
                ),
            ));
        }
        let probe = self.cache.dir.join(".verglas-write-probe");
        std::fs::write(&probe, b"")
            .and_then(|()| std::fs::remove_file(&probe))
            .map_err(|e| {
                ConfigError::Invalid(
                    "cache.dir",
                    format!("cannot write to {}: {e}", self.cache.dir.display()),
                )
            })?;
        // Refuse a budget bigger than the disk (#96): a server that reserves more
        // NVMe than the cache filesystem can hold would run the disk out. Bytes
        // the cache already holds count toward the budget (#298) — on restart
        // they are what consumed the free space, and a config that booted cold
        // must still validate warm. Probed once, at startup, off any hot path.
        // An unprobeable filesystem (None) does not gate — wrong is worse than
        // slow.
        if let Some(free) = crate::disk::free_bytes(&self.cache.dir) {
            let owned = crate::disk::allocated_bytes(&self.cache.dir);
            let available = free.saturating_add(owned);
            if self.cache.capacity_bytes.0 > available {
                return Err(ConfigError::Invalid(
                    "cache.capacity_bytes",
                    format!(
                        "configured {} bytes exceeds the {} bytes available on the filesystem backing cache.dir ({} free plus {} already held by the cache under {}); lower cache.capacity_bytes or free disk space",
                        self.cache.capacity_bytes.0,
                        available,
                        free,
                        owned,
                        self.cache.dir.display()
                    ),
                ));
            }
        }
        if let Some(catalog) = &self.catalog {
            catalog.validate()?;
        }
        if let Some(query_worker) = &self.query_worker {
            query_worker.validate()?;
        }
        if let Some(write_worker) = &self.write_worker {
            write_worker.validate()?;
        }
        if let Some(cluster) = &self.cluster {
            cluster.validate()?;
        }
        if let Some(domain) = &self.listen.domain
            && !is_valid_base_domain(domain)
        {
            return Err(ConfigError::Invalid(
                "listen.domain",
                format!("`{domain}` is not a bare host domain (no scheme, path, or spaces)"),
            ));
        }
        if let Some(tls) = &self.listen.tls {
            tls.validate()?;
        }
        match (self.listen.s3_port, self.listen.admin_port) {
            (0, _) => Err(ConfigError::Invalid(
                "listen.s3_port",
                "port must be non-zero".into(),
            )),
            (_, 0) => Err(ConfigError::Invalid(
                "listen.admin_port",
                "port must be non-zero".into(),
            )),
            (s3, admin) if s3 == admin => Err(ConfigError::Invalid(
                "listen.admin_port",
                format!("conflicts with listen.s3_port (both {s3})"),
            )),
            _ => Ok(()),
        }
    }

    /// One-line startup summary so operators can eyeball what loaded.
    pub fn summary(&self) -> String {
        format!(
            "s3_port={} admin_port={} tls={} addressing={} cache={} (capacity={}B dram={}B block={}B) backend={}(max_concurrent={}) auth={} catalog={} cluster={} query_worker={} write_worker={}",
            self.listen.s3_port,
            self.listen.admin_port,
            if self.listen.tls.is_some() {
                "on"
            } else {
                "off"
            },
            match &self.listen.domain {
                Some(domain) => domain.as_str(),
                None => "path-style",
            },
            self.cache.dir.display(),
            self.cache.capacity_bytes.0,
            self.cache.dram_bytes.0,
            self.cache.data_block_bytes.0,
            self.backend.describe_bucket_set(),
            self.backend.max_concurrent_requests,
            if self.auth.is_some() {
                "configured"
            } else {
                "generated"
            },
            self.catalog.as_ref().map_or("off", |c| c.uri.as_str()),
            self.cluster.as_ref().map_or("off", |c| c.pod_id.as_str()),
            self.query_worker
                .as_ref()
                .map_or("off", |q| q.binary.as_str()),
            self.write_worker
                .as_ref()
                .map_or("off", |w| w.binary.as_str()),
        )
    }
}

/// The operator config file: `$VERGLAS_CONFIG` when set, else
/// `~/.verglas/config.toml`. `None` when neither resolves to a path.
pub fn user_config_path() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os("VERGLAS_CONFIG") {
        return Some(PathBuf::from(p));
    }
    std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".verglas/config.toml"))
}

#[cfg(test)]
mod cluster_id_tests {
    use super::{machine_hostname, resolve_cluster_id_from};

    /// With no env override and no configured id, resolution falls back to the
    /// machine hostname.
    #[test]
    fn defaults_to_hostname_when_unset() {
        let hostname = machine_hostname().expect("a POSIX host has a hostname");
        assert_eq!(
            resolve_cluster_id_from(None, None, Some(hostname.clone())),
            hostname
        );
    }

    /// The env override wins over both the configured id and the hostname.
    #[test]
    fn env_override_wins() {
        assert_eq!(
            resolve_cluster_id_from(
                Some("from-env".into()),
                Some("from-config".into()),
                Some("from-host".into()),
            ),
            "from-env"
        );
    }

    /// The configured id wins over the hostname when no env override is set.
    #[test]
    fn configured_id_beats_hostname() {
        assert_eq!(
            resolve_cluster_id_from(None, Some("from-config".into()), Some("from-host".into())),
            "from-config"
        );
    }

    /// A blank override or configured id is treated as unset; a last-resort
    /// `"localhost"` covers the (unlikely) no-hostname case.
    #[test]
    fn blanks_are_skipped_and_localhost_is_the_floor() {
        assert_eq!(
            resolve_cluster_id_from(Some("  ".into()), None, Some("from-host".into())),
            "from-host"
        );
        assert_eq!(resolve_cluster_id_from(None, None, None), "localhost");
    }
}
