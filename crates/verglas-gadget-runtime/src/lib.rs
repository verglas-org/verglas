//! Supervises Gadget code bundles for local and cloud Verglas deployments.
//!
//! One local runtime may register several Gadgets. Supplying a target Gadget ID
//! constrains the same runtime to one identity for a cloud microVM deployment.

use std::collections::{BTreeMap, HashMap};
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

mod server;
mod supervisor;

pub use server::{DataPlaneConfig, RuntimeService};
pub use supervisor::{HostConfig, ProcessSupervisor, SupervisorError};

/// Maximum accepted bytes across one Gadget's source bundle.
pub const MAX_BUNDLE_BYTES: usize = 8 * 1024 * 1024;

/// Configuration that selects local multiplexing or one cloud target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    maximum_gadgets: usize,
    target_gadget: Option<String>,
}

impl RuntimeConfig {
    /// Configures a local runtime with an explicit hard Gadget ceiling.
    pub fn local(maximum_gadgets: usize) -> Self {
        Self {
            maximum_gadgets,
            target_gadget: None,
        }
    }

    /// Configures a cloud runtime that accepts exactly one Gadget identity.
    pub fn single(target_gadget: impl Into<String>) -> Self {
        Self {
            maximum_gadgets: 1,
            target_gadget: Some(target_gadget.into()),
        }
    }

    /// Returns the maximum number of simultaneously registered Gadgets.
    pub fn maximum_gadgets(&self) -> usize {
        self.maximum_gadgets
    }

    /// Returns the configured cloud target, if the runtime is constrained.
    pub fn target_gadget(&self) -> Option<&str> {
        self.target_gadget.as_deref()
    }
}

/// An immutable Gadget code revision supplied by Verglas OS.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GadgetBundle {
    /// Caller-selected immutable revision identifier.
    pub version: String,
    /// Server module exporting the `Gadget` class.
    pub server_module: String,
    /// Browser module served to the OS-owned sandboxed iframe.
    pub client_module: String,
    /// Additional relative source modules bundled with this revision.
    #[serde(default)]
    pub files: BTreeMap<String, String>,
}

impl GadgetBundle {
    /// Computes a stable content digest over the complete bundle.
    fn content_digest(&self) -> Result<String, RuntimeError> {
        let bytes = serde_json::to_vec(self)
            .map_err(|error| RuntimeError::BundleEncoding(error.to_string()))?;
        Ok(hex::encode(Sha256::digest(bytes)))
    }

    /// Rejects empty, unsafe, or unbounded bundle content before registration.
    fn validate(&self) -> Result<(), RuntimeError> {
        if self.version.is_empty() || self.version.len() > 256 {
            return Err(RuntimeError::InvalidVersion {
                version: self.version.clone(),
            });
        }
        if self.server_module.is_empty() {
            return Err(RuntimeError::MissingServerModule);
        }
        let mut bytes = self
            .server_module
            .len()
            .saturating_add(self.client_module.len());
        for (name, contents) in &self.files {
            validate_bundle_path(name)?;
            if matches!(
                name.as_str(),
                "server.js" | "client.js" | "cloudflare-workers.mjs"
            ) {
                return Err(RuntimeError::ReservedBundlePath { path: name.clone() });
            }
            bytes = bytes
                .saturating_add(name.len())
                .saturating_add(contents.len());
        }
        if bytes > MAX_BUNDLE_BYTES {
            return Err(RuntimeError::BundleTooLarge {
                bytes,
                maximum: MAX_BUNDLE_BYTES,
            });
        }
        Ok(())
    }
}

/// Public metadata for one selected Gadget revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GadgetRecord {
    /// Stable Gadget identity used in routes and KV namespace derivation.
    pub id: String,
    /// Selected immutable revision identifier.
    pub version: String,
    /// SHA-256 digest of the complete registered bundle.
    pub digest: String,
}

/// Result of idempotently selecting a Gadget revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterOutcome {
    /// A new Gadget identity was registered.
    Created {
        /// Digest of the selected bundle.
        digest: String,
    },
    /// The same version and bytes were already selected.
    Unchanged {
        /// Digest of the selected bundle.
        digest: String,
    },
    /// A new immutable version replaced the prior selected version.
    Replaced {
        /// Version that was selected before this request.
        previous_version: String,
        /// Digest of the newly selected bundle.
        digest: String,
    },
}

/// Runtime registration and policy failures.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RuntimeError {
    /// A configured runtime ceiling was zero.
    #[error("maximum Gadgets must be greater than zero")]
    InvalidCapacity,
    /// The HTTP service was configured without an authentication secret.
    #[error("runtime token must not be empty")]
    EmptyRuntimeToken,
    /// The trusted data-plane proxy was configured without an endpoint or credential.
    #[error("data-plane endpoint, token, and capability base URL must not be empty")]
    EmptyDataPlaneConfig,
    /// A Gadget identity cannot be used safely in a route or directory.
    #[error("invalid Gadget id `{id}`")]
    InvalidGadgetId {
        /// Rejected identity.
        id: String,
    },
    /// A cloud runtime received a different Gadget identity.
    #[error("runtime targets Gadget `{expected}`, not `{actual}`")]
    TargetMismatch {
        /// Configured cloud target.
        expected: String,
        /// Rejected request identity.
        actual: String,
    },
    /// A local runtime reached its explicit Gadget ceiling.
    #[error("runtime capacity is {maximum} Gadgets")]
    Capacity {
        /// Configured hard ceiling.
        maximum: usize,
    },
    /// The same immutable version was submitted with different bytes.
    #[error("Gadget `{id}` version `{version}` already has different content")]
    RevisionConflict {
        /// Gadget whose revision conflicted.
        id: String,
        /// Immutable revision identifier.
        version: String,
    },
    /// A bundled module escaped or ambiguously addressed its bundle root.
    #[error("invalid bundle path `{path}`")]
    InvalidBundlePath {
        /// Rejected relative path.
        path: String,
    },
    /// A caller attempted to replace a runtime-owned module.
    #[error("bundle path `{path}` is reserved by the Gadget runtime")]
    ReservedBundlePath {
        /// Rejected runtime-owned path.
        path: String,
    },
    /// The caller omitted executable server code.
    #[error("serverModule must not be empty")]
    MissingServerModule,
    /// The caller supplied an unusable revision identifier.
    #[error("invalid Gadget version `{version}`")]
    InvalidVersion {
        /// Rejected version.
        version: String,
    },
    /// The bundle exceeded the hard request ceiling.
    #[error("bundle contains {bytes} bytes; maximum is {maximum}")]
    BundleTooLarge {
        /// Observed source bytes.
        bytes: usize,
        /// Hard source-byte ceiling.
        maximum: usize,
    },
    /// Canonical JSON encoding failed unexpectedly.
    #[error("bundle encoding: {0}")]
    BundleEncoding(String),
}

/// In-memory selected-revision registry owned by the runtime process.
///
/// Authoritative code remains in Verglas OS. A runtime restart intentionally
/// starts empty and the control plane re-registers desired revisions.
pub struct RuntimeCatalog {
    config: RuntimeConfig,
    entries: HashMap<String, CatalogEntry>,
}

/// Private bundle content paired with its public selected-revision metadata.
struct CatalogEntry {
    record: GadgetRecord,
    bundle: GadgetBundle,
}

impl RuntimeCatalog {
    /// Creates an empty registry after validating its deployment policy.
    pub fn new(config: RuntimeConfig) -> Result<Self, RuntimeError> {
        if config.maximum_gadgets == 0 {
            return Err(RuntimeError::InvalidCapacity);
        }
        if let Some(target) = config.target_gadget.as_deref() {
            validate_gadget_id(target)?;
        }
        Ok(Self {
            config,
            entries: HashMap::new(),
        })
    }

    /// Selects one immutable bundle idempotently for a Gadget identity.
    pub fn register(
        &mut self,
        id: &str,
        bundle: GadgetBundle,
    ) -> Result<RegisterOutcome, RuntimeError> {
        validate_gadget_id(id)?;
        self.authorize_target(id)?;
        bundle.validate()?;
        let digest = bundle.content_digest()?;

        if let Some(existing) = self.entries.get(id) {
            if existing.record.version == bundle.version {
                return if existing.record.digest == digest {
                    Ok(RegisterOutcome::Unchanged { digest })
                } else {
                    Err(RuntimeError::RevisionConflict {
                        id: id.to_owned(),
                        version: bundle.version,
                    })
                };
            }
            let previous_version = existing.record.version.clone();
            self.entries.insert(
                id.to_owned(),
                CatalogEntry {
                    record: GadgetRecord {
                        id: id.to_owned(),
                        version: bundle.version.clone(),
                        digest: digest.clone(),
                    },
                    bundle,
                },
            );
            return Ok(RegisterOutcome::Replaced {
                previous_version,
                digest,
            });
        }

        if self.entries.len() >= self.config.maximum_gadgets {
            return Err(RuntimeError::Capacity {
                maximum: self.config.maximum_gadgets,
            });
        }
        self.entries.insert(
            id.to_owned(),
            CatalogEntry {
                record: GadgetRecord {
                    id: id.to_owned(),
                    version: bundle.version.clone(),
                    digest: digest.clone(),
                },
                bundle,
            },
        );
        Ok(RegisterOutcome::Created { digest })
    }

    /// Returns sorted public metadata for every selected Gadget.
    pub fn list(&self) -> Vec<&GadgetRecord> {
        let mut records = self
            .entries
            .values()
            .map(|entry| &entry.record)
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.id.cmp(&right.id));
        records
    }

    /// Returns public metadata for one Gadget identity.
    pub fn get(&self, id: &str) -> Option<&GadgetRecord> {
        self.entries.get(id).map(|entry| &entry.record)
    }

    /// Returns the selected bundle for execution or browser delivery.
    pub fn bundle(&self, id: &str) -> Option<&GadgetBundle> {
        self.entries.get(id).map(|entry| &entry.bundle)
    }

    /// Removes a selected Gadget revision and reports whether it existed.
    pub fn remove(&mut self, id: &str) -> Result<bool, RuntimeError> {
        validate_gadget_id(id)?;
        self.authorize_target(id)?;
        Ok(self.entries.remove(id).is_some())
    }

    /// Enforces the optional cloud target before any registry lookup.
    fn authorize_target(&self, id: &str) -> Result<(), RuntimeError> {
        if let Some(expected) = self.config.target_gadget.as_deref()
            && expected != id
        {
            return Err(RuntimeError::TargetMismatch {
                expected: expected.to_owned(),
                actual: id.to_owned(),
            });
        }
        Ok(())
    }
}

/// Validates the conservative identity grammar used across runtime surfaces.
fn validate_gadget_id(id: &str) -> Result<(), RuntimeError> {
    let valid = !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && id.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
        && id.as_bytes().last().is_some_and(u8::is_ascii_alphanumeric);
    if valid {
        Ok(())
    } else {
        Err(RuntimeError::InvalidGadgetId { id: id.to_owned() })
    }
}

/// Validates that a bundled module stays below its isolated bundle root.
fn validate_bundle_path(path: &str) -> Result<(), RuntimeError> {
    let path_ref = Path::new(path);
    let valid = !path.is_empty()
        && path.len() <= 512
        && !path_ref.is_absolute()
        && path_ref
            .components()
            .all(|component| matches!(component, Component::Normal(_)));
    if valid {
        Ok(())
    } else {
        Err(RuntimeError::InvalidBundlePath {
            path: path.to_owned(),
        })
    }
}
