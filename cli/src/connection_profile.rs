//! Shared Verglas connection profile.
//!
//! This is deliberately a client-side contract: `login` exchanges an API key
//! for endpoint-scoped credentials, writes a secret-free TOML profile, and
//! integrations resolve the profile through `verglas connection --json`.  The
//! profile never contains secret material; it only references owner-only files.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

const DEFAULT_REGION: &str = "us-east-1";

#[derive(Debug, Error)]
pub enum ConnectionProfileError {
    #[error("cannot locate a home directory for Verglas settings")]
    NoConfigDirectory,
    #[error("not signed in; run `verglas login`")]
    MissingProfile(String),
    #[error("cannot read {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("cannot parse {path}: {source}")]
    Toml {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("connection configuration at {0} must be a TOML table")]
    InvalidConfig(PathBuf),
    #[error("cannot restrict secret file {path} to owner-only permissions: {source}")]
    SecureSecret {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("connection profile is incomplete: {0}")]
    Incomplete(&'static str),
    #[error("invalid control-plane URL `{0}")]
    InvalidUrl(String),
    #[error("control-plane request failed: {0}")]
    Request(reqwest::Error),
    #[error("control plane returned HTTP {status}: {body}")]
    Provision { status: u16, body: String },
    #[error("could not decode provision response: {0}")]
    Decode(reqwest::Error),
}

#[derive(Debug, Clone, Serialize)]
pub struct ResolvedConnection {
    /// Self-hosted server endpoint (`VERGLAS_ENDPOINT`) or a legacy profile's
    /// stored value. Cloud profiles no longer carry one: there is no hosted
    /// query service.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_uri: Option<String>,
    pub semantic_uri: String,
    pub region: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bearer_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_key_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_access_key: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ConfigFile {
    connection: Option<Profile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Profile {
    #[serde(default)]
    control_plane_url: Option<String>,
    #[serde(default)]
    query_uri: Option<String>,
    #[serde(default)]
    semantic_uri: Option<String>,
    #[serde(default)]
    catalog_uri: Option<String>,
    #[serde(default)]
    ca_file: Option<String>,
    #[serde(default)]
    region: Option<String>,
    #[serde(default)]
    credentials_file: Option<String>,
    #[serde(default)]
    bearer_file: Option<String>,
    /// The deployment (tenant) id provisioning reported; addresses the control
    /// plane's tenant-scoped routes (`verglas lakehouse`).
    #[serde(default)]
    tenant_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ProvisionResponse {
    s3_url: String,
    catalog_url: String,
    s3_access_key_id: String,
    s3_secret_access_key: String,
    catalog_token: String,
    #[serde(default)]
    ca_certificate: Option<String>,
    /// The durable, first-party tenant machine token (server-side 30-day
    /// sliding renewal). Present on the WorkOS device-login exchange; the
    /// `--api-key` exchange keeps writing the caller's own long-lived key
    /// instead, so this is `None` there.
    #[serde(default)]
    api_token: Option<String>,
    /// Present on control planes that report the deployment id; recorded in
    /// the profile so tenant-scoped commands need no discovery round trip.
    #[serde(default)]
    tenant_id: Option<String>,
}

/// Authenticates to a public compatible control-plane contract and persists one
/// profile. No server behavior is implemented here.
pub async fn login(url: &str, api_key: &str) -> Result<(), ConnectionProfileError> {
    let provision = exchange(url, api_key).await?;
    persist(url, Some(api_key), &provision)
}

/// Exchanges a WorkOS device-login access token for a connection profile.
/// The WorkOS bearer itself is spent here and discarded; the durable local
/// credential written to `~/.verglas/credentials/control-plane-token` is the
/// response's own `api_token`, not the WorkOS token.
pub async fn login_with_bearer(url: &str, bearer: &str) -> Result<(), ConnectionProfileError> {
    let provision = exchange(url, bearer).await?;
    persist(url, None, &provision)
}

/// Presents `bearer` as the `Authorization` header against `POST <url>/v1/provision`.
/// Identical wire contract whether `bearer` is a long-lived API key or a
/// single-use browser-login code.
async fn exchange(url: &str, bearer: &str) -> Result<ProvisionResponse, ConnectionProfileError> {
    let base = normalized_url(url)?;
    let response = reqwest::Client::new()
        .post(base.join("v1/provision").expect("validated base URL"))
        .bearer_auth(bearer)
        .send()
        .await
        .map_err(ConnectionProfileError::Request)?;
    let status = response.status();
    if !status.is_success() {
        return Err(ConnectionProfileError::Provision {
            status: status.as_u16(),
            body: response.text().await.unwrap_or_default(),
        });
    }
    response
        .json::<ProvisionResponse>()
        .await
        .map_err(ConnectionProfileError::Decode)
}

/// Resolves environment overrides first, then the profile and its referenced
/// credential files. Local profiles may omit a bearer token entirely.
pub fn resolve_from_environment(
    env: &std::collections::HashMap<String, String>,
) -> Result<ResolvedConnection, ConnectionProfileError> {
    let profile = match read_profile() {
        Ok(profile) => Some(profile),
        Err(ConnectionProfileError::MissingProfile(_)) if has_complete_environment(env) => None,
        Err(error) => return Err(error),
    };
    let value = |name: &str| {
        env.get(name)
            .map(String::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
    };
    let profile_value = |f: fn(&Profile) -> &Option<String>| {
        profile.as_ref().and_then(|profile| f(profile).clone())
    };
    let query_uri = value("VERGLAS_ENDPOINT").or_else(|| profile_value(|p| &p.query_uri));
    let semantic_uri = value("VERGLAS_SEMANTIC_ENDPOINT")
        .or_else(|| value("VERGLAS_S3_ENDPOINT"))
        .or_else(|| profile_value(|p| &p.semantic_uri))
        .or_else(|| query_uri.clone())
        .ok_or(ConnectionProfileError::Incomplete(
            "semantic_uri / VERGLAS_S3_ENDPOINT",
        ))?;
    let region = value("VERGLAS_REGION")
        .or_else(|| value("AWS_REGION"))
        .or_else(|| value("AWS_DEFAULT_REGION"))
        .or_else(|| profile_value(|p| &p.region))
        .unwrap_or_else(|| DEFAULT_REGION.to_owned());
    let credentials_path = profile
        .as_ref()
        .and_then(|p| p.credentials_file.as_deref())
        .map(expand_home)
        .transpose()?;
    let credentials = credentials_path
        .as_deref()
        .map(read_aws_credentials)
        .transpose()?;
    let bearer_file = profile
        .as_ref()
        .and_then(|p| p.bearer_file.as_deref())
        .map(expand_home)
        .transpose()?;
    let stored_bearer = bearer_file.as_deref().map(read_secret).transpose()?;
    let bearer_token = value("VERGLAS_TOKEN").or(stored_bearer);
    let access_key_id = value("VERGLAS_ACCESS_KEY_ID")
        .or_else(|| value("VERGLAS_S3_ACCESS_KEY_ID"))
        .or_else(|| value("AWS_ACCESS_KEY_ID"))
        .or_else(|| credentials.as_ref().map(|pair| pair.0.clone()));
    let secret_access_key = value("VERGLAS_SECRET_ACCESS_KEY")
        .or_else(|| value("VERGLAS_S3_SECRET_ACCESS_KEY"))
        .or_else(|| value("AWS_SECRET_ACCESS_KEY"))
        .or_else(|| credentials.as_ref().map(|pair| pair.1.clone()));
    Ok(ResolvedConnection {
        query_uri,
        semantic_uri,
        region,
        catalog_uri: profile_value(|p| &p.catalog_uri),
        ca_file: profile_value(|p| &p.ca_file),
        bearer_token,
        access_key_id,
        secret_access_key,
    })
}

pub fn environment() -> std::collections::HashMap<String, String> {
    std::env::vars().collect()
}

fn has_complete_environment(env: &std::collections::HashMap<String, String>) -> bool {
    env.get("VERGLAS_ENDPOINT")
        .is_some_and(|value| !value.trim().is_empty())
        && (env
            .get("VERGLAS_ACCESS_KEY_ID")
            .or_else(|| env.get("VERGLAS_S3_ACCESS_KEY_ID"))
            .or_else(|| env.get("AWS_ACCESS_KEY_ID"))
            .is_some())
        && (env
            .get("VERGLAS_SECRET_ACCESS_KEY")
            .or_else(|| env.get("VERGLAS_S3_SECRET_ACCESS_KEY"))
            .or_else(|| env.get("AWS_SECRET_ACCESS_KEY"))
            .is_some())
}

fn persist(
    url: &str,
    api_key: Option<&str>,
    provision: &ProvisionResponse,
) -> Result<(), ConnectionProfileError> {
    let config_path = profile_path()?;
    let root = config_path
        .parent()
        .map(Path::to_owned)
        .ok_or(ConnectionProfileError::NoConfigDirectory)?;
    fs::create_dir_all(&root).map_err(|source| ConnectionProfileError::Io {
        path: root.clone(),
        source,
    })?;
    let credentials = root.join("credentials");
    fs::create_dir_all(&credentials).map_err(|source| ConnectionProfileError::Io {
        path: credentials.clone(),
        source,
    })?;
    private_directory(&credentials)?;
    let endpoint_file = credentials.join("endpoint.ini");
    write_private(
        &endpoint_file,
        format!(
            "[default]\naws_access_key_id = {}\naws_secret_access_key = {}\n",
            provision.s3_access_key_id, provision.s3_secret_access_key
        )
        .as_bytes(),
    )?;
    let bearer_file = credentials.join("catalog-token");
    write_private(&bearer_file, provision.catalog_token.as_bytes())?;
    // The `--api-key` path's own long-lived key is the durable credential;
    // the WorkOS device-login path has no reusable credential of its own; it
    // is spent exactly once here, so the durable credential is instead the
    // control plane's freshly minted `api_token`.
    if let Some(api_token) = api_key.or(provision.api_token.as_deref()) {
        let api_file = credentials.join("control-plane-token");
        write_private(&api_file, api_token.as_bytes())?;
    }
    // The CA is only present when the control plane issues one (R6-D); older
    // servers and the frozen login tests' mocks omit it, so no file or
    // profile key is written in that case.
    let ca_file = match provision.ca_certificate.as_deref() {
        Some(ca_certificate) => {
            let ca_file = credentials.join("ca.pem");
            write_private(&ca_file, ca_certificate.as_bytes())?;
            Some(ca_file.to_string_lossy().to_string())
        }
        None => None,
    };
    let profile = Profile {
        control_plane_url: Some(url.trim_end_matches('/').to_owned()),
        query_uri: None,
        semantic_uri: Some(provision.s3_url.clone()),
        catalog_uri: Some(provision.catalog_url.clone()),
        ca_file,
        region: Some(DEFAULT_REGION.to_owned()),
        credentials_file: Some(endpoint_file.to_string_lossy().to_string()),
        bearer_file: Some(bearer_file.to_string_lossy().to_string()),
        tenant_id: provision.tenant_id.clone(),
    };
    let mut config = match fs::read_to_string(&config_path) {
        Ok(text) => {
            toml::from_str::<toml::Value>(&text).map_err(|source| ConnectionProfileError::Toml {
                path: config_path.clone(),
                source,
            })?
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            toml::Value::Table(Default::default())
        }
        Err(source) => {
            return Err(ConnectionProfileError::Io {
                path: config_path.clone(),
                source,
            });
        }
    };
    let document = config
        .as_table_mut()
        .ok_or_else(|| ConnectionProfileError::InvalidConfig(config_path.clone()))?;
    let profile_fields = toml::Value::try_from(profile).expect("profile serializes");
    let connection = document
        .entry("connection".to_owned())
        .or_insert_with(|| toml::Value::Table(Default::default()));
    let connection = connection
        .as_table_mut()
        .ok_or_else(|| ConnectionProfileError::InvalidConfig(config_path.clone()))?;
    let fields = profile_fields
        .as_table()
        .expect("serialized profile is a table");
    for (key, value) in fields {
        connection.insert(key.clone(), value.clone());
    }
    let encoded = toml::to_string_pretty(&config).expect("config serializes");
    write_config_atomic(&config_path, encoded.as_bytes())?;
    Ok(())
}

fn read_profile() -> Result<Profile, ConnectionProfileError> {
    let path = profile_path()?;
    let text = fs::read_to_string(&path).map_err(|source| {
        if source.kind() == std::io::ErrorKind::NotFound {
            ConnectionProfileError::MissingProfile(path.display().to_string())
        } else {
            ConnectionProfileError::Io {
                path: path.clone(),
                source,
            }
        }
    })?;
    let config: ConfigFile =
        toml::from_str(&text).map_err(|source| ConnectionProfileError::Toml {
            path: path.clone(),
            source,
        })?;
    config
        .connection
        .ok_or_else(|| ConnectionProfileError::MissingProfile(path.display().to_string()))
}

/// Reads the profile's control-plane URL, when a profile exists.
/// The tenant (deployment) id recorded at login, if the control plane
/// reported one.
pub fn tenant_id() -> Option<String> {
    let path = profile_path().ok()?;
    let text = fs::read_to_string(path).ok()?;
    let config: ConfigFile = toml::from_str(&text).ok()?;
    config.connection?.tenant_id
}

pub fn control_plane_url() -> Option<String> {
    let path = profile_path().ok()?;
    let text = std::fs::read_to_string(path).ok()?;
    let config: toml::Value = toml::from_str(&text).ok()?;
    config
        .get("connection")?
        .get("control_plane_url")?
        .as_str()
        .map(str::to_owned)
}

/// Removes the stored connection profile and its credential files, preserving
/// every other section of the config file (for example `[catalog]`).
pub fn logout() -> Result<bool, ConnectionProfileError> {
    let config_path = profile_path()?;
    let text = match fs::read_to_string(&config_path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(ConnectionProfileError::Io {
                path: config_path.clone(),
                source,
            });
        }
    };
    let mut config: toml::Value =
        toml::from_str(&text).map_err(|source| ConnectionProfileError::Toml {
            path: config_path.clone(),
            source,
        })?;
    let document = config
        .as_table_mut()
        .ok_or_else(|| ConnectionProfileError::InvalidConfig(config_path.clone()))?;
    let Some(connection) = document.remove("connection") else {
        return Ok(false);
    };
    if let Some(table) = connection.as_table() {
        for key in ["credentials_file", "bearer_file", "ca_file"] {
            if let Some(path) = table.get(key).and_then(toml::Value::as_str) {
                let _ = fs::remove_file(path);
            }
        }
    }
    let encoded = toml::to_string_pretty(&config).expect("config serializes");
    write_config_atomic(&config_path, encoded.as_bytes())?;
    Ok(true)
}

fn profile_path() -> Result<PathBuf, ConnectionProfileError> {
    if let Some(path) = std::env::var_os("VERGLAS_CONFIG") {
        return Ok(PathBuf::from(path));
    }
    Ok(profile_root()?.join("config.toml"))
}

/// Resolves the credentials directory sibling to the shared profile file —
/// the same root `login`/`logout` use for `endpoint.ini`, `catalog-token`,
/// and `ca.pem`, honoring `VERGLAS_CONFIG` when a caller has redirected the
/// profile to a custom location. `crate::auth` persists the durable
/// `control-plane-token` here too, so it moves with the profile instead of
/// always following `$HOME`.
pub(crate) fn credentials_dir() -> Result<PathBuf, ConnectionProfileError> {
    let config_path = profile_path()?;
    let root = config_path
        .parent()
        .map(Path::to_owned)
        .ok_or(ConnectionProfileError::NoConfigDirectory)?;
    Ok(root.join("credentials"))
}

fn profile_root() -> Result<PathBuf, ConnectionProfileError> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .map(|home| home.join(".verglas"))
        .ok_or(ConnectionProfileError::NoConfigDirectory)
}

fn normalized_url(url: &str) -> Result<reqwest::Url, ConnectionProfileError> {
    let normalized = format!("{}/", url.trim_end_matches('/'));
    reqwest::Url::parse(&normalized).map_err(|_| ConnectionProfileError::InvalidUrl(url.to_owned()))
}

fn expand_home(value: &str) -> Result<PathBuf, ConnectionProfileError> {
    if value == "~" || value.starts_with("~/") {
        return std::env::var_os("HOME")
            .map(PathBuf::from)
            .map(|home| home.join(value.strip_prefix("~/").unwrap_or("")))
            .ok_or(ConnectionProfileError::NoConfigDirectory);
    }
    Ok(PathBuf::from(value))
}

fn read_secret(path: &Path) -> Result<String, ConnectionProfileError> {
    harden_secret_file(path)?;
    fs::read_to_string(path)
        .map(|value| value.trim().to_owned())
        .map_err(|source| ConnectionProfileError::Io {
            path: path.to_owned(),
            source,
        })
}

fn read_aws_credentials(path: &Path) -> Result<(String, String), ConnectionProfileError> {
    harden_secret_file(path)?;
    let text = fs::read_to_string(path).map_err(|source| ConnectionProfileError::Io {
        path: path.to_owned(),
        source,
    })?;
    let values = text
        .lines()
        .filter_map(|line| {
            line.split_once('=')
                .map(|(key, value)| (key.trim(), value.trim()))
        })
        .collect::<BTreeMap<_, _>>();
    let access = values
        .get("aws_access_key_id")
        .ok_or(ConnectionProfileError::Incomplete("aws_access_key_id"))?
        .to_string();
    let secret = values
        .get("aws_secret_access_key")
        .ok_or(ConnectionProfileError::Incomplete("aws_secret_access_key"))?
        .to_string();
    Ok((access, secret))
}

/// Shared profiles may refer to credentials written by a user.  On Unix bring
/// those files to the same owner-only posture as files created by `login`.
fn harden_secret_file(path: &Path) -> Result<(), ConnectionProfileError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let metadata = fs::metadata(path).map_err(|source| ConnectionProfileError::Io {
            path: path.to_owned(),
            source,
        })?;
        if metadata.permissions().mode() & 0o077 != 0 {
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|source| {
                ConnectionProfileError::SecureSecret {
                    path: path.to_owned(),
                    source,
                }
            })?;
        }
    }
    Ok(())
}

pub(crate) fn write_private(path: &Path, contents: &[u8]) -> Result<(), ConnectionProfileError> {
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .map_err(|source| ConnectionProfileError::Io {
            path: path.to_owned(),
            source,
        })?;
    file.write_all(contents)
        .map_err(|source| ConnectionProfileError::Io {
            path: path.to_owned(),
            source,
        })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|source| {
            ConnectionProfileError::Io {
                path: path.to_owned(),
                source,
            }
        })?;
    }
    Ok(())
}

pub(crate) fn private_directory(path: &Path) -> Result<(), ConnectionProfileError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|source| {
            ConnectionProfileError::Io {
                path: path.to_owned(),
                source,
            }
        })?;
    }
    Ok(())
}

/// Replaces the profile atomically so a hook never observes an interrupted
/// login write.  The file is owner-only even though it carries no secrets.
fn write_config_atomic(path: &Path, contents: &[u8]) -> Result<(), ConnectionProfileError> {
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    write_private(&temporary, contents)?;
    fs::rename(&temporary, path).map_err(|source| ConnectionProfileError::Io {
        path: path.to_owned(),
        source,
    })
}
