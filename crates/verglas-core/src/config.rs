//! Configuration values shared by the origin backend and byte cache.
//!
//! This module contains only settings that configure retained origin access and
//! local Foyer caching. Network entrypoints and product deployment settings live
//! with their owning host.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Smallest supported data-cache block size.
pub const MIN_DATA_BLOCK_BYTES: u64 = 1024 * 1024;
/// Largest supported data-cache block size.
pub const MAX_DATA_BLOCK_BYTES: u64 = 8 * 1024 * 1024;
/// Default data-cache block size.
pub const DEFAULT_DATA_BLOCK_BYTES: u64 = 2 * 1024 * 1024;

/// A byte count deserialized from a human-size string such as `"512MB"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ByteSize(pub u64);

impl schemars::JsonSchema for ByteSize {
    /// Names the schema type used by generated configuration documentation.
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ByteSize".into()
    }

    /// Describes the accepted plain and suffixed string forms.
    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "description": "A byte count or a binary-suffixed size such as 20GB.",
        })
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    /// Parses a plain or binary-suffixed byte count.
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        parse_bytes(&value)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}

impl Serialize for ByteSize {
    /// Serializes a byte count in the plain string form accepted by the parser.
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

/// Parses a plain or binary-suffixed byte count.
fn parse_bytes(value: &str) -> Result<u64, String> {
    const GB: u64 = 1024 * 1024 * 1024;
    let value = value.trim();
    let digits_end = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let multiplier = match value[digits_end..].trim().to_ascii_uppercase().as_str() {
        "" | "B" => 1,
        "KB" => 1024,
        "MB" => 1024 * 1024,
        "GB" => GB,
        "TB" => 1024 * GB,
        suffix => {
            return Err(format!(
                "unknown size suffix `{suffix}`; expected B/KB/MB/GB/TB"
            ));
        }
    };
    let number = value[..digits_end]
        .parse::<u64>()
        .map_err(|_| format!("`{value}` is not a byte count"))?;
    number
        .checked_mul(multiplier)
        .ok_or_else(|| format!("`{value}` overflows a byte count"))
}

/// Local cache directory and hard DRAM/NVMe budgets.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct Cache {
    /// Directory used by Foyer's persistent storage.
    pub dir: PathBuf,
    /// Maximum persistent storage budget.
    pub capacity_bytes: ByteSize,
    /// Maximum DRAM budget.
    pub dram_bytes: ByteSize,
    /// Fixed-size origin block geometry.
    pub data_block_bytes: ByteSize,
    /// Scan-resistant admission policy.
    pub admission: Admission,
}

impl Default for Cache {
    /// Returns the local-cache defaults used by the runtime.
    fn default() -> Self {
        Self {
            dir: PathBuf::from("/var/lib/verglas"),
            capacity_bytes: ByteSize(20 * 1024 * 1024 * 1024),
            dram_bytes: ByteSize(1024 * 1024 * 1024),
            data_block_bytes: ByteSize(DEFAULT_DATA_BLOCK_BYTES),
            admission: Admission::default(),
        }
    }
}

impl Cache {
    /// Validates the block geometry before Foyer opens its device.
    pub fn validate(&self) -> Result<(), String> {
        let bytes = self.data_block_bytes.0;
        if !(MIN_DATA_BLOCK_BYTES..=MAX_DATA_BLOCK_BYTES).contains(&bytes)
            || !bytes.is_power_of_two()
        {
            return Err(format!(
                "cache.data_block_bytes must be a power of two from {MIN_DATA_BLOCK_BYTES} to {MAX_DATA_BLOCK_BYTES} bytes, got {bytes}"
            ));
        }
        Ok(())
    }
}

/// Scan-resistant admission tuning for origin blocks.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct Admission {
    /// Enables frequency-gated admission under persistent-cache pressure.
    pub enabled: bool,
    /// Minimum estimated frequency for a pressured candidate.
    pub frequency_threshold: u32,
    /// Fraction of qualifying candidates admitted during cyclic churn.
    pub churn_admit_probability: f64,
}

impl Default for Admission {
    /// Enables second-touch admission without churn thinning.
    fn default() -> Self {
        Self {
            enabled: true,
            frequency_threshold: 2,
            churn_admit_probability: 1.0,
        }
    }
}

/// Origin provider supported by the backend adapter.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, schemars::JsonSchema, Default,
)]
#[serde(rename_all = "lowercase")]
pub enum BackendProvider {
    /// AWS S3 and S3-compatible origins.
    #[default]
    S3,
    /// Azure Blob Storage.
    Azure,
    /// Google Cloud Storage.
    Gcp,
}

/// Origin object-store settings consumed by `verglas-backend`.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct Backend {
    /// Origin provider.
    pub provider: BackendProvider,
    /// Maximum concurrent origin requests.
    pub max_concurrent_requests: usize,
    /// Retry policy for transient origin failures.
    pub retry: RetryPolicy,
    /// Circuit-breaker policy for origin failures.
    pub breaker: BreakerPolicy,
    /// One explicitly served bucket.
    pub bucket: Option<String>,
    /// Glob patterns for served bucket families.
    pub bucket_globs: Vec<String>,
    /// Optional origin endpoint override.
    pub endpoint: Option<String>,
    /// Optional signing region.
    pub region: Option<String>,
    /// Allows plaintext origin endpoints.
    pub allow_http: bool,
    /// Uses virtual-hosted bucket addressing.
    pub virtual_hosted_style: bool,
    /// Optional provider credentials file.
    pub credentials_file: Option<String>,
    /// Optional credentials-file profile.
    pub credentials_profile: Option<String>,
}

impl Default for Backend {
    /// Returns the ambient-credential S3 defaults.
    fn default() -> Self {
        Self {
            provider: BackendProvider::default(),
            max_concurrent_requests: 64,
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

/// Matches a bucket glob where `*` consumes any run of characters.
fn glob_matches(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    let (mut pattern_index, mut text_index) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while text_index < text.len() {
        if pattern_index < pattern.len() && pattern[pattern_index] == '*' {
            star = Some((pattern_index + 1, text_index));
            pattern_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == text[text_index] {
            pattern_index += 1;
            text_index += 1;
        } else if let Some((after_star, consumed)) = star {
            star = Some((after_star, consumed + 1));
            pattern_index = after_star;
            text_index = consumed + 1;
        } else {
            return false;
        }
    }
    pattern[pattern_index..]
        .iter()
        .all(|character| *character == '*')
}

impl Backend {
    /// Returns whether `bucket` belongs to this backend's served set.
    pub fn serves_bucket(&self, bucket: &str) -> bool {
        self.bucket.as_deref() == Some(bucket)
            || self
                .bucket_globs
                .iter()
                .any(|pattern| glob_matches(pattern, bucket))
    }

    /// Describes the configured bucket set for logs and diagnostics.
    pub fn describe_bucket_set(&self) -> String {
        let mut parts = Vec::new();
        if let Some(bucket) = self.bucket.as_deref().filter(|bucket| !bucket.is_empty()) {
            parts.push(bucket.to_owned());
        }
        parts.extend(self.bucket_globs.iter().map(|glob| format!("glob:{glob}")));
        if parts.is_empty() {
            "<unset>".to_owned()
        } else {
            parts.join(",")
        }
    }
}

/// Retry and backoff settings for origin requests.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct RetryPolicy {
    /// Retry attempts after the first request.
    pub max_retries: usize,
    /// Initial retry delay in milliseconds.
    pub initial_backoff_ms: u64,
    /// Maximum retry delay in milliseconds.
    pub max_backoff_ms: u64,
    /// Total retry budget in milliseconds.
    pub budget_ms: u64,
}

impl Default for RetryPolicy {
    /// Returns bounded exponential backoff defaults.
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 3_000,
            budget_ms: 10_000,
        }
    }
}

/// Circuit-breaker settings for one origin.
#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct BreakerPolicy {
    /// Failure fraction that opens the breaker.
    pub failure_rate: f64,
    /// Minimum sample count before evaluating the fraction.
    pub min_samples: u32,
    /// Open cooldown in milliseconds.
    pub cooldown_ms: u64,
    /// Maximum concurrent half-open probes.
    pub half_open_max_probes: u32,
}

impl Default for BreakerPolicy {
    /// Returns conservative failure shedding defaults.
    fn default() -> Self {
        Self {
            failure_rate: 0.5,
            min_samples: 20,
            cooldown_ms: 5_000,
            half_open_max_probes: 3,
        }
    }
}
