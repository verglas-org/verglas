//! Content-addressed storage and verification for compiled Worker components.
//!
//! Component bytes are identified by SHA-256 and are verified after every
//! filesystem read; a managed-bucket store can implement the same trait later.
//! The optional compiled cache adds the Wasmtime engine compatibility key to
//! that identity and never treats another engine's precompiled bytes as a hit.

use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use thiserror::Error;
use wasmtime::component::Component;
use wasmtime::{Engine, Precompiled};

/// SHA-256 identity of one compiled component artifact.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ComponentDigest([u8; 32]);

impl ComponentDigest {
    /// Creates a digest from its raw 32-byte SHA-256 representation.
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Computes the SHA-256 identity of component bytes.
    pub fn compute(bytes: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let digest = hasher.finalize();
        let mut raw = [0_u8; 32];
        raw.copy_from_slice(&digest);
        Self::new(raw)
    }

    /// Parses exactly 32 bytes of hexadecimal digest text.
    pub fn from_hex(value: &str) -> Result<Self, ArtifactError> {
        if value.len() != 64 {
            return Err(ArtifactError::InvalidLength {
                actual: value.len(),
            });
        }
        let mut bytes = [0_u8; 32];
        hex::decode_to_slice(value.as_bytes(), &mut bytes[..]).map_err(|_| {
            ArtifactError::InvalidHex {
                value: value.to_owned(),
            }
        })?;
        Ok(Self::new(bytes))
    }

    /// Returns the raw digest bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Verifies that bytes have exactly this component identity.
    pub fn verify(&self, bytes: &[u8]) -> Result<(), ArtifactError> {
        let actual = Self::compute(bytes);
        if actual == *self {
            return Ok(());
        }
        Err(ArtifactError::DigestMismatch {
            expected: *self,
            actual,
        })
    }
}

impl fmt::Display for ComponentDigest {
    /// Formats the digest as lower-case hexadecimal text.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

impl FromStr for ComponentDigest {
    type Err = ArtifactError;

    /// Parses a component digest from lower- or upper-case hexadecimal text.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::from_hex(value)
    }
}

/// Errors from digest parsing, verification, and artifact reads.
#[derive(Debug, Error)]
pub enum ArtifactError {
    /// Reports a digest string with the wrong number of characters.
    #[error("component digest must contain 64 hexadecimal characters, got {actual}")]
    InvalidLength {
        /// The rejected string's character count.
        actual: usize,
    },
    /// Reports a digest string containing a non-hexadecimal byte.
    #[error("component digest is not valid hexadecimal: {value}")]
    InvalidHex {
        /// The rejected digest text.
        value: String,
    },
    /// Reports bytes that do not match the requested content identity.
    #[error("component digest mismatch: expected {expected}, got {actual}")]
    DigestMismatch {
        /// The identity the caller demanded.
        expected: ComponentDigest,
        /// The identity of the bytes actually read.
        actual: ComponentDigest,
    },
    /// Reports a filesystem failure while reading an artifact.
    #[error("failed to read component artifact {path}: {source}")]
    Io {
        /// The path that failed to read.
        path: PathBuf,
        /// The underlying filesystem error.
        #[source]
        source: io::Error,
    },
    /// Reports a filesystem failure while using the compiled component cache.
    #[error("failed to access compiled component cache {path}: {source}")]
    CacheIo {
        /// The cache path that failed.
        path: PathBuf,
        /// The underlying filesystem error.
        #[source]
        source: io::Error,
    },
    /// Reports a Wasmtime compilation or deserialization failure.
    #[error("compiled component cache {operation} failed: {source}")]
    Wasmtime {
        /// The cache operation that failed.
        operation: &'static str,
        /// The Wasmtime failure.
        #[source]
        source: wasmtime::Error,
    },
    /// Reports bytes that do not have Wasmtime's compiled-component marker.
    #[error("compiled component cache entry {path} is not a Wasmtime component")]
    InvalidCacheEntry {
        /// The invalid cache path.
        path: PathBuf,
    },
}

/// Asynchronous source of verified component bytes by content identity.
///
/// A managed-bucket implementation will plug into this seam later without
/// changing the digest-named artifact contract or its verification rule.
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    /// Fetches and verifies the bytes identified by `digest`.
    async fn fetch(&self, digest: ComponentDigest) -> Result<Vec<u8>, ArtifactError>;
}

/// Filesystem artifact store using one digest-named WASM file per component.
#[derive(Clone, Debug)]
pub struct DirArtifactStore {
    /// Directory containing `<hex-digest>.wasm` component files.
    root: PathBuf,
}

impl DirArtifactStore {
    /// Creates a store rooted at the directory containing component files.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Returns the only filesystem path accepted for one component digest.
    fn path_for(&self, digest: ComponentDigest) -> PathBuf {
        self.root.join(format!("{digest}.wasm"))
    }
}

#[async_trait]
impl ArtifactStore for DirArtifactStore {
    /// Reads the digest-named file and fails closed if its bytes differ.
    async fn fetch(&self, digest: ComponentDigest) -> Result<Vec<u8>, ArtifactError> {
        let path = self.path_for(digest);
        let bytes = std::fs::read(&path).map_err(|source| ArtifactError::Io {
            path: path.clone(),
            source,
        })?;
        digest.verify(&bytes)?;
        Ok(bytes)
    }
}

/// Directory cache for precompiled component bytes.
///
/// Each filename is `{sha256-digest}-{engine-key}.cwasm`. `engine-key` is the
/// lower-case hexadecimal `u64` produced by hashing Wasmtime's
/// `Engine::precompile_compatibility_hash()` with Rust's `DefaultHasher`. A
/// caller must provide a directory it exclusively owns; that ownership is the
/// safety boundary for deserializing entries previously written by this cache.
#[derive(Clone, Debug)]
pub struct CwasmCache {
    /// Directory containing engine-keyed compiled component entries.
    root: PathBuf,
}

impl CwasmCache {
    /// Creates a compiled-component cache rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Compiles or deserializes one digest- and engine-keyed component.
    ///
    /// A provided cache directory is part of the startup contract: directory,
    /// read, write, marker, and deserialization failures are returned instead
    /// of silently recompiling without the cache. Entries for another engine
    /// key are ignored because they have different filenames.
    pub fn load_or_compile(
        &self,
        engine: &Engine,
        digest: ComponentDigest,
        bytes: &[u8],
    ) -> Result<Component, ArtifactError> {
        digest.verify(bytes)?;
        std::fs::create_dir_all(&self.root).map_err(|source| ArtifactError::CacheIo {
            path: self.root.clone(),
            source,
        })?;
        let path = self.path_for(engine, digest);
        match std::fs::read(&path) {
            Ok(serialized) => self.deserialize(engine, &path, serialized),
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                let serialized = engine.precompile_component(bytes).map_err(|source| {
                    ArtifactError::Wasmtime {
                        operation: "precompile",
                        source,
                    }
                })?;
                std::fs::write(&path, &serialized).map_err(|source| ArtifactError::CacheIo {
                    path: path.clone(),
                    source,
                })?;
                self.deserialize(engine, &path, serialized)
            }
            Err(source) => Err(ArtifactError::CacheIo { path, source }),
        }
    }

    /// Returns the cache entry path for one source digest and engine key.
    fn path_for(&self, engine: &Engine, digest: ComponentDigest) -> PathBuf {
        let mut hasher = DefaultHasher::new();
        engine.precompile_compatibility_hash().hash(&mut hasher);
        self.root
            .join(format!("{digest}-{:016x}.cwasm", hasher.finish()))
    }

    /// Deserializes one cache entry after checking its compiled-component marker.
    fn deserialize(
        &self,
        engine: &Engine,
        path: &Path,
        serialized: Vec<u8>,
    ) -> Result<Component, ArtifactError> {
        if Engine::detect_precompiled(&serialized) != Some(Precompiled::Component) {
            return Err(ArtifactError::InvalidCacheEntry {
                path: path.to_path_buf(),
            });
        }
        // SAFETY: `path` is the exact digest plus compatibility-key entry under
        // the caller-owned cache directory. Only bytes returned by this same
        // engine's `precompile_component` are written there, and the key keeps
        // incompatible engine configurations in separate files. Callers must
        // not allow another process to mutate the configured cache directory.
        unsafe { Component::deserialize(engine, serialized) }.map_err(|source| {
            ArtifactError::Wasmtime {
                operation: "deserialize",
                source,
            }
        })
    }
}
