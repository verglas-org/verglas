//! Owner-only local persistence for CLI access tokens.
//!
//! The credentials file is intentionally separate from server configuration.
//! It stores bearer values only after the access service has returned them once,
//! and all writes create an owner-only file on platforms with POSIX permissions.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// One locally retained bearer credential and its non-secret token identifier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredToken {
    /// Plaintext bearer value retained because the service cannot return it again.
    pub token: String,
    /// Stable service identifier used to clear the credential after revocation.
    pub token_id: String,
}

/// Serialized credential inventory keyed by normalized access-service endpoint.
#[derive(Debug, Default, Serialize, Deserialize)]
struct CredentialsFile {
    /// Tokens available to the current OS user.
    tokens: BTreeMap<String, StoredToken>,
}

/// Failures while reading or writing local bearer credentials.
#[derive(Debug, Error)]
pub enum CredentialsError {
    /// No configuration directory can be derived from the environment.
    #[error("cannot find a home directory for Verglas credentials")]
    MissingConfigDirectory,
    /// Local credentials could not be read or written safely.
    #[error("credentials file {path}: {source}")]
    Io {
        /// File that caused the I/O failure.
        path: PathBuf,
        /// Underlying I/O failure.
        source: std::io::Error,
    },
    /// Existing plaintext credentials are readable by another local user.
    #[error("credentials file {path} must have owner-only permissions (0600)")]
    InsecurePermissions {
        /// File whose permissions expose a bearer token.
        path: PathBuf,
    },
    /// Existing local credentials are not a valid credential inventory.
    #[error("credentials file {path} is invalid JSON: {source}")]
    Decode {
        /// File that contained invalid JSON.
        path: PathBuf,
        /// Underlying JSON decoding failure.
        source: serde_json::Error,
    },
    /// Credentials could not be encoded before writing.
    #[error("could not encode local credentials: {0}")]
    Encode(serde_json::Error),
}

/// Resolves one explicit or platform-default credentials file path.
pub fn credentials_path(explicit: Option<&Path>) -> Result<PathBuf, CredentialsError> {
    if let Some(path) = explicit {
        return Ok(path.to_owned());
    }
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .ok_or(CredentialsError::MissingConfigDirectory)?;
    Ok(base.join("verglas").join("credentials.json"))
}

/// Loads the token stored for one access-service endpoint, if it exists.
pub fn load_token(path: &Path, endpoint: &str) -> Result<Option<StoredToken>, CredentialsError> {
    let credentials = load(path)?;
    Ok(credentials
        .tokens
        .get(&normalized_endpoint(endpoint))
        .cloned())
}

/// Stores or replaces one access-service token with owner-only file permissions.
pub fn save_token(path: &Path, endpoint: &str, token: StoredToken) -> Result<(), CredentialsError> {
    let mut credentials = load(path)?;
    credentials
        .tokens
        .insert(normalized_endpoint(endpoint), token);
    save(path, &credentials)
}

/// Removes an exact local token record after its service-side revocation succeeds.
pub fn remove_token(path: &Path, endpoint: &str, token_id: &str) -> Result<(), CredentialsError> {
    let mut credentials = load(path)?;
    let key = normalized_endpoint(endpoint);
    if credentials
        .tokens
        .get(&key)
        .is_some_and(|stored| stored.token_id == token_id)
    {
        credentials.tokens.remove(&key);
        save(path, &credentials)?;
    }
    Ok(())
}

/// Reads a credential inventory, treating a missing file as empty.
fn load(path: &Path) -> Result<CredentialsFile, CredentialsError> {
    match fs::read(path) {
        Ok(bytes) => {
            ensure_private_file(path)?;
            serde_json::from_slice(&bytes).map_err(|source| CredentialsError::Decode {
                path: path.to_owned(),
                source,
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(CredentialsFile::default())
        }
        Err(source) => Err(CredentialsError::Io {
            path: path.to_owned(),
            source,
        }),
    }
}

/// Rejects a local bearer file that another POSIX user can read or modify.
fn ensure_private_file(path: &Path) -> Result<(), CredentialsError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let permissions = fs::metadata(path)
            .map_err(|source| CredentialsError::Io {
                path: path.to_owned(),
                source,
            })?
            .permissions()
            .mode();
        if permissions & 0o077 != 0 {
            return Err(CredentialsError::InsecurePermissions {
                path: path.to_owned(),
            });
        }
    }
    Ok(())
}

/// Writes the complete credential inventory through an owner-only temporary file.
fn save(path: &Path, credentials: &CredentialsFile) -> Result<(), CredentialsError> {
    let parent = path.parent().ok_or_else(|| CredentialsError::Io {
        path: path.to_owned(),
        source: std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "credentials path has no parent",
        ),
    })?;
    fs::create_dir_all(parent).map_err(|source| CredentialsError::Io {
        path: parent.to_owned(),
        source,
    })?;
    set_private_directory_permissions(parent)?;
    let encoded = serde_json::to_vec_pretty(credentials).map_err(CredentialsError::Encode)?;
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut file = private_file(&temporary)?;
    file.write_all(&encoded)
        .map_err(|source| CredentialsError::Io {
            path: temporary.clone(),
            source,
        })?;
    file.write_all(b"\n")
        .map_err(|source| CredentialsError::Io {
            path: temporary.clone(),
            source,
        })?;
    file.sync_all().map_err(|source| CredentialsError::Io {
        path: temporary.clone(),
        source,
    })?;
    fs::rename(&temporary, path).map_err(|source| CredentialsError::Io {
        path: path.to_owned(),
        source,
    })?;
    set_private_file_permissions(path)?;
    Ok(())
}

/// Opens a replacement file with owner-only permissions from its first byte.
fn private_file(path: &Path) -> Result<fs::File, CredentialsError> {
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    options.open(path).map_err(|source| CredentialsError::Io {
        path: path.to_owned(),
        source,
    })
}

/// Restricts an existing directory to its owner where the platform supports it.
fn set_private_directory_permissions(path: &Path) -> Result<(), CredentialsError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|source| {
            CredentialsError::Io {
                path: path.to_owned(),
                source,
            }
        })?;
    }
    Ok(())
}

/// Restricts an existing credential file to its owner where the platform supports it.
fn set_private_file_permissions(path: &Path) -> Result<(), CredentialsError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|source| {
            CredentialsError::Io {
                path: path.to_owned(),
                source,
            }
        })?;
    }
    Ok(())
}

/// Canonicalizes URL spelling so a trailing slash does not create a second profile.
fn normalized_endpoint(endpoint: &str) -> String {
    endpoint.trim_end_matches('/').to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Stores and retrieves the exact token by normalized endpoint.
    #[test]
    fn token_round_trip_normalizes_trailing_slash() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("credentials.json");
        save_token(
            &path,
            "http://localhost:8345/",
            StoredToken {
                token: "secret".to_owned(),
                token_id: "token-1".to_owned(),
            },
        )
        .expect("save");
        assert_eq!(
            load_token(&path, "http://localhost:8345")
                .expect("load")
                .expect("token")
                .token,
            "secret"
        );
    }
}
