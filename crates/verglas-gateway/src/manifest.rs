//! Strict parsing for the prototype wrangler JSON and JSONC manifest subset.
//!
//! The parser keeps only gateway deployment metadata and rejects unknown top-level
//! keys so build-time configuration cannot silently alter runtime behavior.

use std::collections::HashSet;
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

/// The validated subset of a wrangler-style deployment manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Manifest {
    name: String,
    main: String,
    bindings: Vec<Binding>,
    component_digest: String,
    component_dir: PathBuf,
    data_root: PathBuf,
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

    /// Returns all declared Durable Object bindings in manifest order.
    pub fn bindings(&self) -> &[Binding] {
        &self.bindings
    }

    /// Looks up one binding by its URL-visible name.
    pub fn binding(&self, name: &str) -> Option<&Binding> {
        self.bindings.iter().find(|binding| binding.name == name)
    }

    /// Returns the validated hexadecimal component digest.
    pub fn component_digest(&self) -> &str {
        &self.component_digest
    }

    /// Returns the directory containing immutable component artifacts.
    pub fn component_dir(&self) -> &Path {
        &self.component_dir
    }

    /// Returns the manifest's prototype runtime data root.
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
            "durable_objects",
            "component_digest",
            "component_dir",
            "data_root",
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
        let durable_objects = required_object(&mut object, "durable_objects")?;
        let bindings = parse_durable_objects(durable_objects)?;
        let component_digest = required_string(&mut object, "component_digest")?;
        validate_digest(&component_digest)?;
        let component_dir = PathBuf::from(required_string(&mut object, "component_dir")?);
        let data_root = PathBuf::from(required_string(&mut object, "data_root")?);
        if component_dir.as_os_str().is_empty() {
            return Err(ManifestError::EmptyField {
                field: "component_dir",
            });
        }
        if data_root.as_os_str().is_empty() {
            return Err(ManifestError::EmptyField { field: "data_root" });
        }
        Ok(Self {
            name,
            main,
            bindings,
            component_digest,
            component_dir,
            data_root,
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
    /// A required field was absent.
    #[error("manifest field {field} is required")]
    MissingField {
        /// Dotted field name that was absent.
        field: &'static str,
    },
    /// A field had a JSON type other than a string or object as required.
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

/// Parses one binding object and keeps duplicate names impossible.
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

/// Validates a SHA-256 component identity without taking a dependency on the runtime crate.
fn validate_digest(value: &str) -> Result<(), ManifestError> {
    if value.len() != 64 || hex::decode(value).map_or(true, |bytes| bytes.len() != 32) {
        return Err(ManifestError::InvalidComponentDigest {
            value: value.to_owned(),
        });
    }
    Ok(())
}

/// Removes comments and trailing commas accepted by wrangler's JSONC subset.
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
