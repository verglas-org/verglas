//! Generates the annotated `config.toml` scaffold from the [`Config`] schema.
//!
//! The config file is not hand-written. Every setting, its help text, and its
//! default come from the [`Config`] struct itself: field doc comments feed the
//! JSON Schema `schemars` derives, and this module walks that schema to emit
//! TOML with each description as a comment above its key. The struct is the one
//! source of truth, so the config file, the rustdoc, and the schema can never
//! drift apart.

use crate::config::Config;
use serde_json::Value;
use std::fmt::Write as _;

/// Values a config scaffold injects into the generated config, overriding the
/// struct defaults. Anything left `None` keeps the default. The required
/// backend trio (bucket/endpoint/region) stays commented out when its override
/// is `None`, so a blank scaffold names the fields without writing bogus values.
#[derive(Debug, Default)]
pub struct Overrides {
    /// `listen.s3_port`.
    pub s3_port: Option<u16>,
    /// `listen.admin_port`.
    pub admin_port: Option<u16>,
    /// `cache.dir`.
    pub cache_dir: Option<String>,
    /// `cache.capacity_bytes`.
    pub capacity_bytes: Option<String>,
    /// `cache.dram_bytes`.
    pub dram_bytes: Option<String>,
    /// `backend.provider` (`s3`/`azure`/`gcp`). None keeps the struct default s3.
    pub provider: Option<String>,
    /// `backend.bucket` (required; commented when absent).
    pub bucket: Option<String>,
    /// `backend.endpoint` (required; commented when absent).
    pub endpoint: Option<String>,
    /// `backend.region` (required; commented when absent).
    pub region: Option<String>,
    /// `backend.credentials_file`.
    pub backend_credentials_file: Option<String>,
    /// `auth.credentials_file`.
    pub auth_credentials_file: Option<String>,
}

/// Every dotted path [`Overrides::get`] can fill. Used to decide whether an
/// override activates an otherwise-optional section (e.g. an `auth.*` override
/// turns the commented `[auth]` example into a live section).
const OVERRIDE_PATHS: &[&str] = &[
    "listen.s3_port",
    "listen.admin_port",
    "cache.dir",
    "cache.capacity_bytes",
    "cache.dram_bytes",
    "backend.provider",
    "backend.bucket",
    "backend.endpoint",
    "backend.region",
    "backend.credentials_file",
    "auth.credentials_file",
];

impl Overrides {
    /// True when an override fills a field under `section`, so an optional
    /// section is written live rather than as a commented example.
    fn targets_section(&self, section: &str) -> bool {
        let prefix = format!("{section}.");
        OVERRIDE_PATHS
            .iter()
            .any(|p| p.starts_with(&prefix) && self.get(p).is_some())
    }

    /// Looks up an override for a dotted config path, as a TOML value.
    fn get(&self, path: &str) -> Option<Value> {
        let s = |o: &Option<String>| o.clone().map(Value::from);
        match path {
            "listen.s3_port" => self.s3_port.map(|v| Value::from(v as u64)),
            "listen.admin_port" => self.admin_port.map(|v| Value::from(v as u64)),
            "cache.dir" => s(&self.cache_dir),
            "cache.capacity_bytes" => s(&self.capacity_bytes),
            "cache.dram_bytes" => s(&self.dram_bytes),
            "backend.provider" => s(&self.provider),
            "backend.bucket" => s(&self.bucket),
            "backend.endpoint" => s(&self.endpoint),
            "backend.region" => s(&self.region),
            "backend.credentials_file" => s(&self.backend_credentials_file),
            "auth.credentials_file" => s(&self.auth_credentials_file),
            _ => None,
        }
    }
}

/// Top-level section order in the emitted file. Logical rather than alphabetical
/// (the schema is sorted): what you set first is listed first. A section missing
/// here would still be emitted, after these, so nothing is silently dropped.
const SECTION_ORDER: &[&str] = &[
    "listen", "log", "cache", "backend", "auth", "catalog", "cluster",
];

/// The settings the generated config actually shows: the ones an operator sets.
/// Everything else the server reads keeps its default and is left out of the
/// file — those are internal tuning knobs, not deployment settings, and dumping
/// them in the config only obscures the handful that matter. The struct still
/// defines and documents them; they are simply not surfaced here. A section is
/// shown only if it holds one of these, and a field only if it is listed.
const EXPOSED: &[&str] = &[
    // Ports the endpoints listen on.
    "listen.s3_port",
    "listen.admin_port",
    // Structured-logging output format and default verbosity (#61).
    "log.format",
    "log.level",
    // Cache location, size ceilings, and data-block geometry.
    "cache.dir",
    "cache.capacity_bytes",
    "cache.dram_bytes",
    "cache.data_block_bytes",
    // Erasure-coded write-back tier.
    "cache.writeback.enabled",
    "cache.writeback.k",
    "cache.writeback.m",
    "cache.writeback.w",
    "cache.writeback.ack_deadline_ms",
    // Origin connection and credentials.
    "backend.provider",
    "backend.endpoint",
    "backend.region",
    "backend.bucket",
    "backend.bucket_globs",
    "backend.credentials_file",
    "backend.allow_http",
    // Endpoint credentials the S3 API accepts from engines.
    "auth.credentials_file",
    // Iceberg REST catalog to watch (the bearer token lives in a file, not here).
    "catalog.uri",
    "catalog.credentials_file",
    "catalog.credentials_profile",
    "catalog.sigv4_region",
    "catalog.sigv4_signing_name",
    "catalog.include",
    "catalog.exclude",
    "catalog.warehouse",
    // Cluster network membership (the shared secret lives in a file, not here).
    "cluster.pod_id",
    "cluster.node_id",
    "cluster.gossip_addr",
    "cluster.advertise_addr",
    "cluster.seeds",
    "cluster.weight",
];

/// True when `path` is a leaf field the config surfaces.
fn is_exposed_leaf(path: &str) -> bool {
    EXPOSED.contains(&path)
}

/// True when any exposed field lives under `prefix` (a section or sub-table), so
/// the section is worth emitting at all.
fn section_has_exposed(prefix: &str) -> bool {
    let dotted = format!("{prefix}.");
    EXPOSED.iter().any(|p| p.starts_with(&dotted))
}

/// Renders the complete annotated `config.toml`, applying `overrides`.
pub fn render(overrides: &Overrides) -> String {
    let schema: Value = serde_json::to_value(schemars::schema_for!(Config))
        .expect("config schema serializes to JSON");
    let defs = schema.get("$defs").cloned().unwrap_or(Value::Null);
    let props = schema
        .get("properties")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    let mut out = String::new();

    // Sections in the fixed order first, then any not listed (kept for safety).
    let mut ordered: Vec<String> = SECTION_ORDER.iter().map(|s| s.to_string()).collect();
    for name in props.keys() {
        if !ordered.contains(name) {
            ordered.push(name.clone());
        }
    }

    for name in ordered {
        if !section_has_exposed(&name) {
            continue;
        }
        let Some(schema) = props.get(&name) else {
            continue;
        };
        let default = schema.get("default").cloned();
        render_section(&mut out, &name, schema, default.as_ref(), &defs, overrides);
    }
    // Each section opens with a blank line; drop the leading one so the file
    // starts at the first section, not a blank line.
    out.trim_start().to_string()
}

/// Renders one top-level section (`[name]`). A section backed by an object
/// schema with a default is written live; an optional section (`anyOf` with
/// `null`, no default) is written commented as an example to fill in.
fn render_section(
    out: &mut String,
    name: &str,
    schema: &Value,
    default: Option<&Value>,
    defs: &Value,
    overrides: &Overrides,
) {
    let commented = default.is_none() && !overrides.targets_section(name);
    let resolved = resolve(schema, defs);
    let Some(object) = object_properties(&resolved, defs) else {
        return;
    };
    out.push('\n');
    writeln!(out, "{}[{name}]", prefix(commented)).ok();
    render_object(out, name, &object, default, defs, overrides, commented);
}

/// Renders the fields of an object at TOML path `path`. Leaf fields are written
/// first (a `[sub.table]` header would otherwise capture them), then nested
/// tables recurse.
fn render_object(
    out: &mut String,
    path: &str,
    object: &serde_json::Map<String, Value>,
    default: Option<&Value>,
    defs: &Value,
    overrides: &Overrides,
    commented: bool,
) {
    let default_obj = default.and_then(Value::as_object);
    let mut tables: Vec<(&String, Value)> = Vec::new();
    let mut leaves: Vec<(&String, &Value)> = Vec::new();

    for (key, field_schema) in object {
        let resolved = resolve(field_schema, defs);
        let dotted = format!("{path}.{key}");

        if array_of_objects(&resolved, defs).is_some() {
            // Arrays of tables (e.g. per-prefix write-back overrides) are not
            // surfaced in the scaffold.
            continue;
        }
        if object_properties(&resolved, defs).is_some() {
            // A nested table; keep it only if it holds an exposed field, and
            // defer it so this object's own leaves come first.
            if section_has_exposed(&dotted) {
                tables.push((key, field_schema.clone()));
            }
            continue;
        }
        // Only the settings an operator sets are shown.
        if is_exposed_leaf(&dotted) {
            leaves.push((key, field_schema));
        }
    }

    // Emit leaves in the order they appear in EXPOSED (logical, not the schema's
    // alphabetical order) so related settings read top to bottom as authored.
    leaves.sort_by_key(|(key, _)| {
        let dotted = format!("{path}.{key}");
        EXPOSED
            .iter()
            .position(|p| *p == dotted)
            .unwrap_or(usize::MAX)
    });
    for (key, field_schema) in leaves {
        let dotted = format!("{path}.{key}");
        let field_default = default_obj.and_then(|o| o.get(key));
        let overridden = overrides.get(&dotted);
        let field_schema_default = field_schema.get("default");
        let value = overridden
            .as_ref()
            .or(field_default)
            .or(field_schema_default);
        write_description(out, field_schema, commented);
        match value {
            Some(v) if !v.is_null() => {
                writeln!(out, "{}{key} = {}", prefix(commented), toml_value(v)).ok();
            }
            // An unset field is left as `# key =` to fill in, never `= ""`.
            _ => {
                writeln!(out, "# {key} =").ok();
            }
        }
    }

    for (key, field_schema) in tables {
        let dotted = format!("{path}.{key}");
        let sub_default = default_obj.and_then(|o| o.get(key.as_str()));
        let sub_commented = commented || sub_default.map(Value::is_null).unwrap_or(true);
        out.push('\n');
        writeln!(out, "{}[{dotted}]", prefix(sub_commented)).ok();
        let resolved = resolve(&field_schema, defs);
        if let Some(object) = object_properties(&resolved, defs) {
            render_object(
                out,
                &dotted,
                &object,
                sub_default,
                defs,
                overrides,
                sub_commented,
            );
        }
    }
}

/// Writes a schema's `description` as `# ` comment lines. A commented field
/// keeps the same text; the `# key = ` line that follows carries the marker.
fn write_description(out: &mut String, schema: &Value, _commented: bool) {
    if let Some(desc) = schema.get("description").and_then(Value::as_str) {
        for line in desc.lines() {
            if line.is_empty() {
                out.push_str("#\n");
            } else {
                writeln!(out, "# {line}").ok();
            }
        }
    }
}

/// `"# "` for a commented line, `""` for a live one.
fn prefix(commented: bool) -> &'static str {
    if commented { "# " } else { "" }
}

/// Resolves a `$ref` to its definition; returns other schemas unchanged.
fn resolve(schema: &Value, defs: &Value) -> Value {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str)
        && let Some(name) = reference.strip_prefix("#/$defs/")
        && let Some(def) = defs.get(name)
    {
        return def.clone();
    }
    schema.clone()
}

/// If a schema is an object with named properties (a struct → TOML table),
/// returns those properties. A scalar newtype like `ByteSize` (schema `type:
/// string`) has no properties and returns `None`, so it stays a leaf.
fn object_properties(schema: &Value, defs: &Value) -> Option<serde_json::Map<String, Value>> {
    // Unwrap an `anyOf: [ref, null]` optional wrapper to its inner object.
    if let Some(inner) = any_of_inner(schema) {
        return object_properties(&resolve(&inner, defs), defs);
    }
    if schema.get("type").and_then(Value::as_str) == Some("object") {
        return schema.get("properties").and_then(Value::as_object).cloned();
    }
    None
}

/// If a schema is an array whose items are an object, returns the item object's
/// properties (used to render `[[array.of.tables]]` example blocks).
fn array_of_objects(schema: &Value, defs: &Value) -> Option<serde_json::Map<String, Value>> {
    if schema.get("type").and_then(Value::as_str) == Some("array") {
        let items = schema.get("items")?;
        return object_properties(&resolve(items, defs), defs);
    }
    None
}

/// For an `anyOf: [X, null]` optional, returns the non-null branch `X`.
fn any_of_inner(schema: &Value) -> Option<Value> {
    let arms = schema.get("anyOf")?.as_array()?;
    arms.iter()
        .find(|a| a.get("type").and_then(Value::as_str) != Some("null"))
        .cloned()
}

/// Renders a JSON value as a TOML scalar/array literal.
fn toml_value(value: &Value) -> String {
    match value {
        Value::String(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::Array(items) => {
            let parts: Vec<String> = items.iter().map(toml_value).collect();
            format!("[{}]", parts.join(", "))
        }
        Value::Null => "\"\"".to_string(),
        Value::Object(_) => "{}".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    /// A unique, existing scratch directory for a test's `cache.dir`. Each test
    /// needs its own: `Config::validate()` probes `cache.dir` by creating and
    /// deleting a fixed-name write-probe file, so tests sharing one directory
    /// (e.g. the bare temp dir) race on that probe when run in parallel.
    /// Tiny cache budget for tests. `Config::validate()` gates
    /// `cache.capacity_bytes` against the real free space of the filesystem
    /// backing `cache.dir`, so a test that validates must never claim the
    /// multi-GB scaffold default — a CI runner's disk can't guarantee it.
    const TEST_CAPACITY: &str = "64MB";

    fn scratch_dir(tag: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("verglas-cfgtmpl-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        dir
    }

    /// A blank scaffold (no overrides) PARSES — every default is present and the
    /// backend bucket fields stay commented — but does NOT validate: the server
    /// needs at least one of `backend.bucket` / `backend.bucket_globs`, so a
    /// scaffold with neither is refused at startup, the error naming
    /// `backend.bucket`. A scaffold WITH a bucket override validates. `cache.dir`
    /// is a real directory because `validate` probes it on disk.
    #[test]
    fn blank_scaffold_parses_but_requires_a_bucket_to_validate() {
        let dir = scratch_dir("blank");
        let blank = render(&Overrides {
            cache_dir: Some(dir.display().to_string()),
            ..Default::default()
        });
        let config = Config::from_toml_str(&blank).expect("blank scaffold parses");
        // The required backend trio has no override, so it is commented out and
        // no `[auth]`/`[catalog]`/`[cluster]` section is active.
        assert!(config.backend.bucket.is_none());
        assert!(config.auth.is_none());
        let err = config
            .validate()
            .expect_err("blank scaffold must not validate without a bucket");
        assert!(
            err.to_string().contains("backend.bucket"),
            "error must name the required bucket, got: {err}"
        );

        // With a bucket override, the scaffold validates.
        let filled = render(&Overrides {
            cache_dir: Some(dir.display().to_string()),
            capacity_bytes: Some(TEST_CAPACITY.into()),
            bucket: Some("my-bucket".into()),
            ..Default::default()
        });
        let config = Config::from_toml_str(&filled).expect("filled scaffold parses");
        config.validate().expect("scaffold with a bucket validates");
        assert_eq!(config.backend.bucket.as_deref(), Some("my-bucket"));
    }

    /// Init's overrides land in the file, and secrets never do: the config
    /// names credentials *files* under `[backend]` and `[auth]`, and the
    /// generated text carries no access-key or secret-key assignment.
    #[test]
    fn overrides_fill_values_and_no_secret_lands_in_config() {
        let dir = scratch_dir("overrides");
        let toml = render(&Overrides {
            cache_dir: Some(dir.display().to_string()),
            capacity_bytes: Some(TEST_CAPACITY.into()),
            dram_bytes: Some("1GB".into()),
            bucket: Some("hyperglas".into()),
            endpoint: Some("https://oci.example.com".into()),
            region: Some("us-ashburn-1".into()),
            backend_credentials_file: Some("/home/op/.verglas/credentials/default".into()),
            auth_credentials_file: Some("/home/op/.verglas/credentials/endpoint".into()),
            ..Default::default()
        });
        let config = Config::from_toml_str(&toml).expect("filled scaffold parses");
        config.validate().expect("filled scaffold validates");
        assert_eq!(config.backend.bucket.as_deref(), Some("hyperglas"));
        assert_eq!(config.backend.region.as_deref(), Some("us-ashburn-1"));
        assert_eq!(
            config.backend.endpoint.as_deref(),
            Some("https://oci.example.com")
        );
        let auth = config.auth.expect("auth section present");
        assert_eq!(
            auth.credentials_file,
            "/home/op/.verglas/credentials/endpoint"
        );
        // The secret never appears in the config, only the file that holds it.
        assert!(!toml.contains("secret_access_key ="));
        assert!(!toml.contains("access_key_id ="));
    }

    /// The config surfaces only operator settings. Internal tuning knobs keep
    /// their defaults and never appear; there is no empty `= ""` placeholder;
    /// no cluster shared secret is written.
    #[test]
    fn only_operator_settings_are_shown() {
        let toml = render(&Overrides {
            cache_dir: Some(scratch_dir("operator").display().to_string()),
            ..Default::default()
        });
        // Shown: the settings an operator sets.
        for shown in [
            "[cache]",
            "capacity_bytes",
            "dram_bytes",
            "data_block_bytes",
            "[cache.writeback]",
            "[backend]",
            "[cluster]",
        ] {
            assert!(toml.contains(shown), "expected `{shown}` in the config");
        }
        // Hidden: internal tuning knobs and their sections.
        for hidden in [
            "[cache.admission]",
            "churn_admit_probability",
            "frequency_threshold",
            "[cache.prefetch]",
            "heat_channel_capacity",
            "[cache.warming]",
            "[backend.retry]",
            "[backend.breaker]",
            "meta_fraction",
            "max_concurrent_requests",
            // The scrub interval is an internal tuning knob (#220): it keeps its
            // 6h default and never appears in the generated config.
            "scrub_interval_secs",
        ] {
            assert!(!toml.contains(hidden), "tuning knob `{hidden}` leaked in");
        }
        // No empty-string placeholders, and no cluster secret in the file.
        assert!(
            !toml.contains("= \"\""),
            "empty `= \"\"` placeholder present"
        );
        assert!(!toml.contains("secret ="), "a secret was written to config");
    }

    /// The backend provider is surfaced and defaults to `s3` in the scaffold, so
    /// an operator sees the field and the common case is pre-filled. A config with
    /// `provider = "azure"` or `"gcp"` parses and validates.
    #[test]
    fn provider_shows_defaults_to_s3_and_other_providers_parse() {
        use crate::config::BackendProvider;
        let dir = scratch_dir("provider");
        let toml = render(&Overrides {
            cache_dir: Some(dir.display().to_string()),
            capacity_bytes: Some(TEST_CAPACITY.into()),
            bucket: Some("my-bucket".into()),
            ..Default::default()
        });
        // The field is shown under [backend] and pre-filled to s3.
        assert!(
            toml.contains("provider = \"s3\""),
            "provider must show and default to s3, got:\n{toml}"
        );
        let config = Config::from_toml_str(&toml).expect("scaffold parses");
        assert_eq!(config.backend.provider, BackendProvider::S3);

        // Swapping the provider to azure/gcp still parses and validates (the
        // scaffold is a valid starting point for every provider).
        for (name, want) in [
            ("azure", BackendProvider::Azure),
            ("gcp", BackendProvider::Gcp),
        ] {
            let swapped = toml.replace("provider = \"s3\"", &format!("provider = \"{name}\""));
            let config = Config::from_toml_str(&swapped).expect("swapped provider parses");
            assert_eq!(config.backend.provider, want);
            config.validate().expect("swapped provider validates");
        }
    }

    /// The `[catalog]` section renders as a commented example (the whole section
    /// is optional). Only the operator-facing fields appear: uri, the credentials
    /// *file*, the SigV4 settings, include/exclude, and warehouse. The tuning
    /// knob `poll_interval_secs` and the inline `bearer_token` stay hidden, and no
    /// token is written to the config.
    #[test]
    fn catalog_section_renders_commented_without_tuning_or_inline_token() {
        let toml = render(&Overrides {
            cache_dir: Some(scratch_dir("catalog").display().to_string()),
            ..Default::default()
        });
        // The section header is present and commented (optional section).
        assert!(
            toml.contains("# [catalog]"),
            "catalog section must render as a commented example, got:\n{toml}"
        );
        // The operator-facing fields are named.
        for shown in [
            "Base URI of the Iceberg REST catalog",
            "Credentials file for catalog auth",
            "sigv4_region =",
            "sigv4_signing_name =",
            "include =",
            "exclude =",
            "warehouse =",
        ] {
            assert!(
                toml.contains(shown),
                "expected `{shown}` in the catalog section"
            );
        }
        // The tuning knob and the inline token never appear.
        for hidden in ["poll_interval_secs", "bearer_token"] {
            assert!(
                !toml.contains(hidden),
                "catalog knob `{hidden}` must stay hidden from the template"
            );
        }
    }

    /// A blank scaffold's commented `[catalog]` block becomes a live catalog when
    /// uncommented: it parses, and it validates once a bucket is present. The
    /// credentials-file path (not an inline token) is what an operator fills in.
    #[test]
    fn uncommented_catalog_parses_and_validates() {
        let dir = scratch_dir("catalog-live");
        // Point catalog.credentials_file at a real 0600-ish file so validate's
        // existence check passes; the token itself is never in the config.
        let token_file = dir.join("catalog-token");
        std::fs::write(&token_file, "sekrit\n").expect("write token file");
        let toml = format!(
            "[cache]\ndir = \"{dir}\"\ncapacity_bytes = \"{cap}\"\n\n\
             [backend]\nbucket = \"b\"\n\n\
             [catalog]\nuri = \"https://catalog.example.com\"\ncredentials_file = \"{token}\"\n\
             include = [\"db.*\"]\nexclude = [\"db.tmp_*\"]\n",
            dir = dir.display(),
            cap = TEST_CAPACITY,
            token = token_file.display(),
        );
        let config = Config::from_toml_str(&toml).expect("catalog config parses");
        let catalog = config.catalog.as_ref().expect("catalog section present");
        assert_eq!(catalog.uri, "https://catalog.example.com");
        assert_eq!(
            catalog.credentials_file.as_deref(),
            Some(token_file.display().to_string().as_str())
        );
        config.validate().expect("catalog config validates");
        // The token is resolved from the file, trimmed.
        assert_eq!(
            catalog.resolve_bearer_token().expect("resolves"),
            Some("sekrit".to_owned())
        );
    }

    /// Setting both a credentials file and an inline token is rejected — loud
    /// beats a silent precedence. Both `validate()` and `resolve_bearer_token()`
    /// refuse it, naming the field.
    #[test]
    fn catalog_rejects_both_token_sources() {
        let dir = scratch_dir("catalog-both");
        let toml = format!(
            "[cache]\ndir = \"{dir}\"\ncapacity_bytes = \"{cap}\"\n\n\
             [backend]\nbucket = \"b\"\n\n\
             [catalog]\nuri = \"https://c.example.com\"\n\
             credentials_file = \"/nope\"\nbearer_token = \"inline\"\n",
            dir = dir.display(),
            cap = TEST_CAPACITY,
        );
        let config = Config::from_toml_str(&toml).expect("parses");
        let err = config.validate().expect_err("both token sources must fail");
        assert!(
            err.to_string().contains("catalog.credentials_file"),
            "error must name the field, got: {err}"
        );
        let err = config
            .catalog
            .as_ref()
            .expect("catalog present")
            .resolve_bearer_token()
            .expect_err("resolve must reject both sources");
        assert!(err.to_string().contains("catalog.credentials_file"));
    }

    /// A named catalog credentials file that does not exist is rejected at
    /// validate, mirroring the TLS PEM-file existence check — a typo'd path is
    /// an actionable startup error, not a silent no-auth poll.
    #[test]
    fn catalog_missing_credentials_file_fails_validate() {
        let dir = scratch_dir("catalog-missing");
        let toml = format!(
            "[cache]\ndir = \"{dir}\"\ncapacity_bytes = \"{cap}\"\n\n\
             [backend]\nbucket = \"b\"\n\n\
             [catalog]\nuri = \"https://c.example.com\"\n\
             credentials_file = \"{dir}/does-not-exist\"\n",
            dir = dir.display(),
            cap = TEST_CAPACITY,
        );
        let config = Config::from_toml_str(&toml).expect("parses");
        let err = config
            .validate()
            .expect_err("missing credentials file must fail");
        assert!(
            err.to_string().contains("catalog.credentials_file"),
            "error must name the field, got: {err}"
        );
    }

    /// The generated text carries no issue-tracker references — the config is
    /// operator documentation, not a changelog.
    #[test]
    fn generated_config_has_no_issue_references() {
        let toml = render(&Overrides::default());
        for line in toml.lines() {
            assert!(
                !line.contains("(#") && !line.contains(" #1") && !line.contains(" #2"),
                "config comment references an issue number: {line}"
            );
        }
    }
}
