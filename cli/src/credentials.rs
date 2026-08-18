//! Owner-only local read access to a legacy CLI bearer-credential file.
//!
//! The credentials file is intentionally separate from server configuration
//! and from the connection profile `verglas login` writes
//! (`crate::connection_profile`). `VERGLAS_TOKEN` and this file are the two
//! ways `Cli::resolved_token` finds a bearer for commands that call Verglas
//! Cloud directly (workers, dashboards, secrets). Nothing in this CLI writes
//! the file; an operator populates it out of band when they want a token
//! resolved without setting the environment variable each time.

use std::collections::BTreeMap;
use std::fs;
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

/// Failures while reading local bearer credentials.
#[derive(Debug, Error)]
pub enum CredentialsError {
    /// No configuration directory can be derived from the environment.
    #[error("cannot find a home directory for Verglas credentials")]
    MissingConfigDirectory,
    /// Local credentials could not be read safely.
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

/// Canonicalizes URL spelling so a trailing slash does not create a second profile.
fn normalized_endpoint(endpoint: &str) -> String {
    endpoint.trim_end_matches('/').to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn write_owner_only(path: &Path, contents: &str) {
        use std::os::unix::fs::PermissionsExt as _;
        fs::write(path, contents).expect("write credentials");
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("chmod");
    }

    /// Reads back the token stored under a normalized endpoint, tolerating a
    /// trailing slash on the lookup.
    #[cfg(unix)]
    #[test]
    fn load_token_normalizes_a_trailing_slash() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("credentials.json");
        write_owner_only(
            &path,
            r#"{"tokens":{"http://localhost:8345":{"token":"secret","token_id":"token-1"}}}"#,
        );
        assert_eq!(
            load_token(&path, "http://localhost:8345/")
                .expect("load")
                .expect("token")
                .token,
            "secret"
        );
    }

    /// A missing credentials file resolves to no stored token, not an error.
    #[test]
    fn load_token_tolerates_a_missing_file() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("credentials.json");
        assert!(
            load_token(&path, "http://localhost:8345")
                .expect("missing file is not an error")
                .is_none()
        );
    }

    /// A group- or world-readable credentials file is rejected rather than trusted.
    #[cfg(unix)]
    #[test]
    fn load_token_rejects_insecure_permissions() {
        use std::os::unix::fs::PermissionsExt as _;
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("credentials.json");
        fs::write(&path, r#"{"tokens":{}}"#).expect("write credentials");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("chmod");
        assert!(matches!(
            load_token(&path, "http://localhost:8345"),
            Err(CredentialsError::InsecurePermissions { .. })
        ));
    }
}
