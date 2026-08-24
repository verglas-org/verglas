//! Strict parsing for the Wrangler manifest and Turso deployment contract.
//!
//! Durable-object namespaces and system pipeline bindings are separate maps. A
//! deployment carrying either requires an explicit Turso URL template and token
//! file; unknown fields and incomplete credentials fail before process launch.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};
use thiserror::Error;

/// One named Durable Object binding from the manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Binding {
    name: String,
    class_name: String,
}

impl Binding {
    /// Returns the binding name used in gateway URLs.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the guest class name selected by the build pipeline.
    pub fn class_name(&self) -> &str {
        &self.class_name
    }
}

/// One Wrangler `pipelines` binding targeting the prebuilt Stream deployment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PipelineBinding {
    binding: String,
    stream: String,
}

impl PipelineBinding {
    /// Returns the environment binding name exposed by the Worker shim.
    pub fn binding(&self) -> &str {
        &self.binding
    }

    /// Returns the Stream object identity used by `PipelineBinding.send`.
    pub fn stream(&self) -> &str {
        &self.stream
    }
}

/// One Turso URL/token mapping used by a deployment or one named binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TursoDeployment {
    url_template: String,
    token_file: PathBuf,
}

impl TursoDeployment {
    /// Creates one explicit Turso URL template and token-file template.
    pub fn new(
        url_template: impl Into<String>,
        token_file: impl Into<PathBuf>,
    ) -> Result<Self, ManifestError> {
        let url_template = url_template.into();
        let token_file = token_file.into();
        validate_turso_fields(&url_template, &token_file)?;
        Ok(Self {
            url_template,
            token_file,
        })
    }

    /// Resolves `{binding}` and `{do_id}` placeholders for one object.
    pub fn url(&self, binding: &str, do_id: &str) -> String {
        substitute(&self.url_template, binding, do_id)
    }

    /// Resolves `{binding}` and `{do_id}` in the token-file path.
    pub fn token_file(&self, binding: &str, do_id: &str) -> PathBuf {
        PathBuf::from(substitute(
            &self.token_file.to_string_lossy(),
            binding,
            do_id,
        ))
    }

    /// Returns the configured URL template without resolving placeholders.
    pub fn url_template(&self) -> &str {
        &self.url_template
    }

    /// Returns the configured token-file template without resolving placeholders.
    pub fn token_file_template(&self) -> &Path {
        &self.token_file
    }
}

/// Explicit Turso deployment defaults plus optional per-binding overrides.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TursoConfig {
    default: Option<TursoDeployment>,
    bindings: BTreeMap<String, TursoDeployment>,
}

impl TursoConfig {
    /// Resolves a named binding or the deployment default, failing closed if absent.
    pub fn for_binding(&self, binding: &str) -> Result<&TursoDeployment, ManifestError> {
        self.bindings
            .get(binding)
            .or(self.default.as_ref())
            .ok_or_else(|| ManifestError::MissingTursoDeployment {
                binding: binding.to_owned(),
            })
    }

    /// Returns the deployment-level mapping when one was declared.
    pub fn default(&self) -> Option<&TursoDeployment> {
        self.default.as_ref()
    }

    /// Returns all explicit per-binding mappings.
    pub fn bindings(&self) -> &BTreeMap<String, TursoDeployment> {
        &self.bindings
    }
}

/// One accepted Durable Object migration declaration from a Wrangler manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Migration {
    tag: String,
    new_classes: Vec<String>,
    new_sqlite_classes: Vec<String>,
}

impl Migration {
    /// Returns the migration tag used to order deployment changes.
    pub fn tag(&self) -> &str {
        &self.tag
    }

    /// Returns classes introduced with the default Durable Object storage kind.
    pub fn new_classes(&self) -> &[String] {
        &self.new_classes
    }

    /// Returns classes introduced with SQLite-backed Durable Object storage.
    pub fn new_sqlite_classes(&self) -> &[String] {
        &self.new_sqlite_classes
    }
}

/// The validated subset of a Wrangler-style deployment manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Manifest {
    name: String,
    main: String,
    compatibility_date: Option<String>,
    compatibility_flags: Vec<String>,
    bindings: Vec<Binding>,
    pipelines: Vec<PipelineBinding>,
    migrations: Vec<Migration>,
    vars: Map<String, Value>,
    component_digest: String,
    component_dir: PathBuf,
    cwasm_cache_dir: Option<PathBuf>,
    data_root: PathBuf,
    turso: Option<TursoConfig>,
}

impl Manifest {
    /// Parses one JSON or JSONC document without consulting the filesystem.
    pub fn parse(source: &str) -> Result<Self, ManifestError> {
        let source = strip_jsonc(source)?;
        let value = serde_json::from_str::<Value>(&source).map_err(|source| {
            ManifestError::InvalidJson {
                message: source.to_string(),
            }
        })?;
        Self::from_value(value)
    }

    /// Reads and parses a `.json` or `.jsonc` manifest file.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ManifestError> {
        let path = path.as_ref();
        let extension = path.extension().and_then(|value| value.to_str());
        if !matches!(extension, Some(value) if value.eq_ignore_ascii_case("json") || value.eq_ignore_ascii_case("jsonc"))
        {
            return Err(ManifestError::UnsupportedExtension {
                path: path.to_path_buf(),
            });
        }
        let source = std::fs::read_to_string(path).map_err(|source| ManifestError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Self::parse(&source)
    }

    /// Returns the deployment name from the manifest.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the build entry point, which the gateway does not execute.
    pub fn main(&self) -> &str {
        &self.main
    }

    /// Returns the optional Cloudflare compatibility date.
    pub fn compatibility_date(&self) -> Option<&str> {
        self.compatibility_date.as_deref()
    }

    /// Returns compatibility flags in manifest order.
    pub fn compatibility_flags(&self) -> &[String] {
        &self.compatibility_flags
    }

    /// Returns all declared Durable Object bindings in manifest order.
    pub fn bindings(&self) -> &[Binding] {
        &self.bindings
    }

    /// Returns all declared system pipeline bindings in manifest order.
    pub fn pipelines(&self) -> &[PipelineBinding] {
        &self.pipelines
    }

    /// Returns accepted migration declarations in manifest order.
    pub fn migrations(&self) -> &[Migration] {
        &self.migrations
    }

    /// Returns worker environment values exactly as declared by Wrangler.
    pub fn vars(&self) -> &Map<String, Value> {
        &self.vars
    }

    /// Looks up one binding in the durable-object namespace only.
    pub fn binding(&self, name: &str) -> Option<&Binding> {
        self.bindings.iter().find(|binding| binding.name == name)
    }

    /// Looks up one system pipeline binding without merging namespaces.
    pub fn pipeline(&self, name: &str) -> Option<&PipelineBinding> {
        self.pipelines
            .iter()
            .find(|pipeline| pipeline.binding == name)
    }

    /// Resolves the explicit Turso deployment for one DO or system binding.
    pub fn turso_for(&self, binding: &str) -> Result<&TursoDeployment, ManifestError> {
        self.turso
            .as_ref()
            .ok_or_else(|| ManifestError::MissingTursoDeployment {
                binding: binding.to_owned(),
            })?
            .for_binding(binding)
    }

    /// Returns the validated hexadecimal component digest.
    pub fn component_digest(&self) -> &str {
        &self.component_digest
    }

    /// Returns the directory containing immutable component artifacts.
    pub fn component_dir(&self) -> &Path {
        &self.component_dir
    }

    /// Returns the optional Wasmtime compiled component cache directory.
    pub fn cwasm_cache_dir(&self) -> Option<&Path> {
        self.cwasm_cache_dir.as_deref()
    }

    /// Returns the manifest's runtime data root.
    pub fn data_root(&self) -> &Path {
        &self.data_root
    }

    /// Builds a validated manifest from a parsed JSON value.
    fn from_value(value: Value) -> Result<Self, ManifestError> {
        let Value::Object(mut object) = value else {
            return Err(ManifestError::RootNotObject);
        };
        let allowed = [
            "name",
            "main",
            "compatibility_date",
            "compatibility_flags",
            "durable_objects",
            "pipelines",
            "migrations",
            "vars",
            "component_digest",
            "component_dir",
            "cwasm_cache_dir",
            "data_root",
            "turso",
        ]
        .into_iter()
        .collect::<HashSet<_>>();
        if let Some(key) = object
            .keys()
            .find(|key| !allowed.contains(key.as_str()))
            .cloned()
        {
            return Err(ManifestError::UnknownTopLevelKey { key });
        }

        let name = required_string(&mut object, "name")?;
        let main = required_string(&mut object, "main")?;
        let compatibility_date = object
            .remove("compatibility_date")
            .map(|value| parse_nonempty_string(value, "compatibility_date"))
            .transpose()?;
        let compatibility_flags = object
            .remove("compatibility_flags")
            .map(|value| parse_string_array(value, "compatibility_flags"))
            .transpose()?
            .unwrap_or_default();
        let durable_objects = required_object(&mut object, "durable_objects")?;
        let bindings = parse_durable_objects(durable_objects)?;
        let pipelines = object
            .remove("pipelines")
            .map(parse_pipelines)
            .transpose()?
            .unwrap_or_default();
        let migrations = object
            .remove("migrations")
            .map(parse_migrations)
            .transpose()?
            .unwrap_or_default();
        let vars = object
            .remove("vars")
            .map(parse_vars)
            .transpose()?
            .unwrap_or_default();
        let component_digest = required_string(&mut object, "component_digest")?;
        validate_digest(&component_digest)?;
        let component_dir = PathBuf::from(required_string(&mut object, "component_dir")?);
        let cwasm_cache_dir = object
            .remove("cwasm_cache_dir")
            .map(|value| parse_nonempty_string(value, "cwasm_cache_dir").map(PathBuf::from))
            .transpose()?;
        let data_root = PathBuf::from(required_string(&mut object, "data_root")?);
        let turso = object.remove("turso").map(parse_turso).transpose()?;
        if (!bindings.is_empty() || !pipelines.is_empty()) && turso.is_none() {
            return Err(ManifestError::MissingField { field: "turso" });
        }
        Ok(Self {
            name,
            main,
            compatibility_date,
            compatibility_flags,
            bindings,
            pipelines,
            migrations,
            vars,
            component_digest,
            component_dir,
            cwasm_cache_dir,
            data_root,
            turso,
        })
    }
}

/// A manifest document was malformed or violated the strict prototype schema.
#[derive(Debug, Error)]
pub enum ManifestError {
    /// Reading a manifest file failed.
    #[error("failed to read manifest {path}: {source}")]
    Io {
        /// Path that could not be read.
        path: PathBuf,
        /// Filesystem error returned by the operating system.
        #[source]
        source: std::io::Error,
    },
    /// The input path is not one of the supported manifest extensions.
    #[error("manifest {path} must have a .json or .jsonc extension")]
    UnsupportedExtension {
        /// Path whose extension was rejected.
        path: PathBuf,
    },
    /// JSON syntax or JSONC syntax after comment removal was invalid.
    #[error("manifest JSON is invalid: {message}")]
    InvalidJson {
        /// Parser-provided syntax detail.
        message: String,
    },
    /// A block comment was not terminated.
    #[error("manifest JSONC has an unterminated block comment")]
    UnterminatedComment,
    /// The document root must be a JSON object.
    #[error("manifest root must be a JSON object")]
    RootNotObject,
    /// A top-level key is outside the prototype manifest subset.
    #[error("unknown top-level manifest key: {key}")]
    UnknownTopLevelKey {
        /// Key that was not recognized.
        key: String,
    },
    /// A nested durable-object section contains an unknown key.
    #[error("unknown durable_objects manifest key: {key}")]
    UnknownDurableObjectsKey {
        /// Key that was not recognized.
        key: String,
    },
    /// A binding object contains an unknown key.
    #[error("unknown durable_objects.bindings key: {key}")]
    UnknownBindingKey {
        /// Key that was not recognized.
        key: String,
    },
    /// A pipeline object contains an unknown key.
    #[error("unknown pipelines manifest key: {key}")]
    UnknownPipelineKey {
        /// Key that was not recognized.
        key: String,
    },
    /// A Turso object contains an unknown key.
    #[error("unknown turso manifest key: {key}")]
    UnknownTursoKey {
        /// Key that was not recognized.
        key: String,
    },
    /// A Turso per-binding object contains an unknown key.
    #[error("unknown turso.bindings key: {key}")]
    UnknownTursoBindingKey {
        /// Key that was not recognized.
        key: String,
    },
    /// A required field was absent.
    #[error("manifest field {field} is required")]
    MissingField {
        /// Dotted field name that was absent.
        field: &'static str,
    },
    /// A field had a JSON type other than required.
    #[error("manifest field {field} must be a {expected}")]
    InvalidType {
        /// Dotted field name with the wrong type.
        field: &'static str,
        /// Human-readable expected JSON type.
        expected: &'static str,
    },
    /// A required string was empty.
    #[error("manifest field {field} must not be empty")]
    EmptyField {
        /// Field whose value was empty.
        field: &'static str,
    },
    /// The component identity was not exactly 32 bytes of hexadecimal text.
    #[error("component_digest must be exactly 64 hexadecimal characters: {value}")]
    InvalidComponentDigest {
        /// Rejected digest text.
        value: String,
    },
    /// A binding did not have a unique URL-visible name.
    #[error("duplicate Durable Object binding: {name}")]
    DuplicateBinding {
        /// Name repeated by more than one binding.
        name: String,
    },
    /// A pipeline did not have a unique environment binding name.
    #[error("duplicate pipeline binding: {name}")]
    DuplicatePipelineBinding {
        /// Name repeated by more than one pipeline.
        name: String,
    },
    /// A Turso mapping was missing for one binding.
    #[error("Turso deployment mapping is missing for binding {binding}")]
    MissingTursoDeployment {
        /// Binding that could not resolve a deployment.
        binding: String,
    },
    /// A Turso URL or token file template was invalid.
    #[error("invalid Turso deployment: {message}")]
    InvalidTursoDeployment {
        /// Stable validation detail.
        message: String,
    },
    /// A migration object contains an unsupported migration kind or key.
    #[error("unknown migrations manifest key: {key}")]
    UnknownMigrationKey {
        /// Key that was not recognized.
        key: String,
    },
}

/// Parses one nonempty string value for an optional manifest field.
fn parse_nonempty_string(value: Value, field: &'static str) -> Result<String, ManifestError> {
    let Value::String(value) = value else {
        return Err(ManifestError::InvalidType {
            field,
            expected: "string",
        });
    };
    if value.is_empty() {
        return Err(ManifestError::EmptyField { field });
    }
    Ok(value)
}

/// Parses an ordered array of nonempty strings.
fn parse_string_array(value: Value, field: &'static str) -> Result<Vec<String>, ManifestError> {
    let Value::Array(values) = value else {
        return Err(ManifestError::InvalidType {
            field,
            expected: "array",
        });
    };
    values
        .into_iter()
        .map(|value| parse_nonempty_string(value, field))
        .collect()
}

/// Parses the worker environment object without interpreting or dropping values.
fn parse_vars(value: Value) -> Result<Map<String, Value>, ManifestError> {
    let Value::Object(values) = value else {
        return Err(ManifestError::InvalidType {
            field: "vars",
            expected: "object",
        });
    };
    Ok(values)
}

/// Parses accepted migration entries and rejects unsupported migration kinds.
fn parse_migrations(value: Value) -> Result<Vec<Migration>, ManifestError> {
    let Value::Array(values) = value else {
        return Err(ManifestError::InvalidType {
            field: "migrations",
            expected: "array",
        });
    };
    values.into_iter().map(parse_migration).collect()
}

/// Parses one migration entry from Wrangler's accepted class-introduction forms.
fn parse_migration(value: Value) -> Result<Migration, ManifestError> {
    let Value::Object(mut object) = value else {
        return Err(ManifestError::InvalidType {
            field: "migrations[]",
            expected: "object",
        });
    };
    let allowed = ["tag", "new_classes", "new_sqlite_classes"]
        .into_iter()
        .collect::<HashSet<_>>();
    if let Some(key) = object
        .keys()
        .find(|key| !allowed.contains(key.as_str()))
        .cloned()
    {
        return Err(ManifestError::UnknownMigrationKey { key });
    }
    let tag = required_string(&mut object, "tag")?;
    let new_classes = object
        .remove("new_classes")
        .map(|value| parse_string_array(value, "migrations[].new_classes"))
        .transpose()?
        .unwrap_or_default();
    let new_sqlite_classes = object
        .remove("new_sqlite_classes")
        .map(|value| parse_string_array(value, "migrations[].new_sqlite_classes"))
        .transpose()?
        .unwrap_or_default();
    Ok(Migration {
        tag,
        new_classes,
        new_sqlite_classes,
    })
}

/// Parses the exact Cloudflare pipelines binding array.
fn parse_pipelines(value: Value) -> Result<Vec<PipelineBinding>, ManifestError> {
    let Value::Array(values) = value else {
        return Err(ManifestError::InvalidType {
            field: "pipelines",
            expected: "array",
        });
    };
    let mut pipelines = Vec::with_capacity(values.len());
    let mut names = HashSet::with_capacity(values.len());
    for value in values {
        let Value::Object(mut object) = value else {
            return Err(ManifestError::InvalidType {
                field: "pipelines[]",
                expected: "object",
            });
        };
        let allowed = ["binding", "stream"].into_iter().collect::<HashSet<_>>();
        if let Some(key) = object
            .keys()
            .find(|key| !allowed.contains(key.as_str()))
            .cloned()
        {
            return Err(ManifestError::UnknownPipelineKey { key });
        }
        let binding = required_string(&mut object, "binding")?;
        let stream = required_string(&mut object, "stream")?;
        if !names.insert(binding.clone()) {
            return Err(ManifestError::DuplicatePipelineBinding { name: binding });
        }
        pipelines.push(PipelineBinding { binding, stream });
    }
    Ok(pipelines)
}

/// Parses deployment-level and per-binding Turso mappings.
fn parse_turso(value: Value) -> Result<TursoConfig, ManifestError> {
    let Value::Object(mut object) = value else {
        return Err(ManifestError::InvalidType {
            field: "turso",
            expected: "object",
        });
    };
    let allowed = ["url_template", "token_file", "bindings"]
        .into_iter()
        .collect::<HashSet<_>>();
    if let Some(key) = object
        .keys()
        .find(|key| !allowed.contains(key.as_str()))
        .cloned()
    {
        return Err(ManifestError::UnknownTursoKey { key });
    }
    let default = match (object.remove("url_template"), object.remove("token_file")) {
        (None, None) => None,
        (Some(url), Some(token)) => Some(TursoDeployment::new(
            parse_nonempty_string(url, "turso.url_template")?,
            PathBuf::from(parse_nonempty_string(token, "turso.token_file")?),
        )?),
        _ => {
            return Err(ManifestError::InvalidTursoDeployment {
                message: "url_template and token_file must be supplied together".to_owned(),
            });
        }
    };
    let bindings = match object.remove("bindings") {
        None => BTreeMap::new(),
        Some(Value::Object(values)) => {
            let mut mappings = BTreeMap::new();
            for (name, value) in values {
                let Value::Object(mut mapping) = value else {
                    return Err(ManifestError::InvalidType {
                        field: "turso.bindings[]",
                        expected: "object",
                    });
                };
                let allowed = ["url_template", "token_file"]
                    .into_iter()
                    .collect::<HashSet<_>>();
                if let Some(key) = mapping
                    .keys()
                    .find(|key| !allowed.contains(key.as_str()))
                    .cloned()
                {
                    return Err(ManifestError::UnknownTursoBindingKey { key });
                }
                let url = required_string(&mut mapping, "url_template")?;
                let token = required_string(&mut mapping, "token_file")?;
                mappings.insert(name, TursoDeployment::new(url, PathBuf::from(token))?);
            }
            mappings
        }
        Some(_) => {
            return Err(ManifestError::InvalidType {
                field: "turso.bindings",
                expected: "object",
            });
        }
    };
    if default.is_none() && bindings.is_empty() {
        return Err(ManifestError::InvalidTursoDeployment {
            message: "at least one deployment mapping is required".to_owned(),
        });
    }
    Ok(TursoConfig { default, bindings })
}

/// Extracts a required nonempty string from an object.
fn required_string(
    object: &mut Map<String, Value>,
    field: &'static str,
) -> Result<String, ManifestError> {
    let value = object
        .remove(field)
        .ok_or(ManifestError::MissingField { field })?;
    let Value::String(value) = value else {
        return Err(ManifestError::InvalidType {
            field,
            expected: "string",
        });
    };
    if value.is_empty() {
        return Err(ManifestError::EmptyField { field });
    }
    Ok(value)
}

/// Extracts a required object from an object.
fn required_object(
    object: &mut Map<String, Value>,
    field: &'static str,
) -> Result<Map<String, Value>, ManifestError> {
    let value = object
        .remove(field)
        .ok_or(ManifestError::MissingField { field })?;
    let Value::Object(value) = value else {
        return Err(ManifestError::InvalidType {
            field,
            expected: "object",
        });
    };
    Ok(value)
}

/// Parses the only supported durable-object section.
fn parse_durable_objects(mut object: Map<String, Value>) -> Result<Vec<Binding>, ManifestError> {
    if let Some(key) = object
        .keys()
        .find(|key| key.as_str() != "bindings")
        .cloned()
    {
        return Err(ManifestError::UnknownDurableObjectsKey { key });
    }
    let value = object
        .remove("bindings")
        .ok_or(ManifestError::MissingField {
            field: "durable_objects.bindings",
        })?;
    let Value::Array(values) = value else {
        return Err(ManifestError::InvalidType {
            field: "durable_objects.bindings",
            expected: "array",
        });
    };
    let mut bindings = Vec::with_capacity(values.len());
    let mut names = HashSet::with_capacity(values.len());
    for value in values {
        let binding = parse_binding(value)?;
        if !names.insert(binding.name.clone()) {
            return Err(ManifestError::DuplicateBinding { name: binding.name });
        }
        bindings.push(binding);
    }
    Ok(bindings)
}

/// Parses one durable-object binding object.
fn parse_binding(value: Value) -> Result<Binding, ManifestError> {
    let Value::Object(mut object) = value else {
        return Err(ManifestError::InvalidType {
            field: "durable_objects.bindings[]",
            expected: "object",
        });
    };
    if let Some(key) = object
        .keys()
        .find(|key| key.as_str() != "name" && key.as_str() != "class_name")
        .cloned()
    {
        return Err(ManifestError::UnknownBindingKey { key });
    }
    let name = required_string(&mut object, "name")?;
    let class_name = required_string(&mut object, "class_name")?;
    Ok(Binding { name, class_name })
}

/// Validates a SHA-256 component identity without a runtime dependency.
fn validate_digest(value: &str) -> Result<(), ManifestError> {
    if value.len() != 64 || hex::decode(value).map_or(true, |bytes| bytes.len() != 32) {
        return Err(ManifestError::InvalidComponentDigest {
            value: value.to_owned(),
        });
    }
    Ok(())
}

/// Validates a nonempty URL and token-file template.
fn validate_turso_fields(url: &str, token_file: &Path) -> Result<(), ManifestError> {
    if url.is_empty() || url.chars().any(char::is_whitespace) {
        return Err(ManifestError::InvalidTursoDeployment {
            message: "url_template must be nonempty and contain no whitespace".to_owned(),
        });
    }
    if token_file.as_os_str().is_empty() {
        return Err(ManifestError::InvalidTursoDeployment {
            message: "token_file must be nonempty".to_owned(),
        });
    }
    Ok(())
}

/// Replaces only the two documented deployment placeholders.
fn substitute(template: &str, binding: &str, do_id: &str) -> String {
    template
        .replace("{binding}", binding)
        .replace("{do_id}", do_id)
}

/// Removes comments and trailing commas accepted by Wrangler's JSONC subset.
fn strip_jsonc(source: &str) -> Result<String, ManifestError> {
    let characters = source.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(source.len());
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;
    while let Some(character) = characters.get(index).copied() {
        if in_string {
            output.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
            }
            index += 1;
            continue;
        }
        if character == '"' {
            in_string = true;
            output.push(character);
            index += 1;
            continue;
        }
        if character == '/' && characters.get(index + 1) == Some(&'/') {
            index += 2;
            while let Some(next) = characters.get(index).copied() {
                index += 1;
                if next == '\n' {
                    output.push('\n');
                    break;
                }
            }
            continue;
        }
        if character == '/' && characters.get(index + 1) == Some(&'*') {
            index += 2;
            let mut terminated = false;
            while let Some(next) = characters.get(index).copied() {
                if next == '*' && characters.get(index + 1) == Some(&'/') {
                    index += 2;
                    terminated = true;
                    output.push(' ');
                    break;
                }
                if next == '\n' {
                    output.push('\n');
                }
                index += 1;
            }
            if !terminated {
                return Err(ManifestError::UnterminatedComment);
            }
            continue;
        }
        output.push(character);
        index += 1;
    }
    Ok(strip_trailing_commas(&output))
}

/// Removes commas immediately before a closing array or object delimiter.
fn strip_trailing_commas(source: &str) -> String {
    let characters = source.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(source.len());
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;
    while let Some(character) = characters.get(index).copied() {
        if in_string {
            output.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
            }
            index += 1;
            continue;
        }
        if character == '"' {
            in_string = true;
            output.push(character);
            index += 1;
            continue;
        }
        if character == ',' {
            let mut lookahead = index + 1;
            while matches!(characters.get(lookahead), Some(value) if value.is_whitespace()) {
                lookahead += 1;
            }
            if matches!(characters.get(lookahead), Some('}' | ']')) {
                index += 1;
                continue;
            }
        }
        output.push(character);
        index += 1;
    }
    output
}
