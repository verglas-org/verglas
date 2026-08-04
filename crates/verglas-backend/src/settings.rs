//! Resolved backend origin endpoint settings (#221).
//!
//! These values select the origin endpoint, region, and addressing style. AWS
//! credentials deliberately resolve elsewhere, at request signing time, so an
//! instance-role or STS session can refresh without restarting the daemon.
//!
//! [`BackendSettings`] here is the S3-family resolution (AWS/OCI/MinIO): the
//! byte-exact raw client and the typed S3 client read it. Azure and GCP resolve
//! their own credentials ([`AzureCredentials`], [`GcpCredentials`]) — their
//! object-store clients take a different shape, so they do not go through the
//! SigV4 access/secret pair.
//!
use std::path::Path;

use verglas_core::config::Backend;

use crate::BackendError;

/// The effective S3 endpoint settings after resolving config over the AWS env
/// chain. Credentials are intentionally absent: origin clients ask their
/// refreshing provider for them while signing each request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendSettings {
    /// The origin base URL with no trailing slash.
    pub endpoint: String,
    /// The SigV4 signing region.
    pub region: String,
    /// Whether a plaintext `http://` endpoint is permitted.
    pub allow_http: bool,
    /// Whether to address buckets virtual-hosted-style rather than path-style.
    pub virtual_hosted_style: bool,
}

/// Builds a [`BackendError::Build`] carrying `msg` — construction has no bucket
/// context yet, so the bucket is left blank and filled in by the caller.
fn settings_error(msg: impl Into<String>) -> BackendError {
    BackendError::Build {
        bucket: String::new(),
        source: object_store::Error::Generic {
            store: "BackendSettings",
            source: msg.into().into(),
        },
    }
}

impl BackendSettings {
    /// Resolves the effective settings from `backend` config over the AWS env
    /// chain read through `env`. The config value wins per field; an unset config
    /// field falls back to the env, then to the documented default (region
    /// `us-east-1`, endpoint the regional AWS endpoint). `env` is injected so
    /// endpoint resolution is deterministic and unit-tested without touching
    /// the process environment. Credentials are resolved lazily by the origin
    /// client, not at daemon startup.
    pub fn resolve<F>(backend: &Backend, env: &F) -> Result<Self, BackendError>
    where
        F: Fn(&str) -> Option<String>,
    {
        let region = backend
            .region
            .clone()
            .filter(|s| !s.is_empty())
            .or_else(|| env("AWS_REGION").filter(|s| !s.is_empty()))
            .or_else(|| env("AWS_DEFAULT_REGION").filter(|s| !s.is_empty()))
            .unwrap_or_else(|| "us-east-1".to_owned());

        let endpoint = backend
            .endpoint
            .clone()
            .filter(|s| !s.is_empty())
            .or_else(|| env("AWS_ENDPOINT").filter(|s| !s.is_empty()))
            .or_else(|| env("AWS_ENDPOINT_URL").filter(|s| !s.is_empty()))
            .unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"));
        let endpoint = endpoint.trim_end_matches('/').to_owned();

        // http is opt-in: the config's `allow_http`, or `AWS_ALLOW_HTTP` when the
        // config leaves it at its default.
        let allow_http = backend.allow_http
            || env("AWS_ALLOW_HTTP")
                .map(|v| {
                    let v = v.to_ascii_lowercase();
                    v == "true" || v == "1"
                })
                .unwrap_or(false);
        if endpoint.starts_with("http://") && !allow_http {
            return Err(settings_error(
                "endpoint uses http; set backend.allow_http = true (or AWS_ALLOW_HTTP=true)",
            ));
        }

        Ok(BackendSettings {
            endpoint,
            region,
            allow_http,
            virtual_hosted_style: backend.virtual_hosted_style,
        })
    }
}

/// Parses an AWS-format credentials file (`~/.aws/credentials` style INI) and
/// returns the access key, secret key, and optional session token for `profile`.
/// Reads the `(access_key_id, secret_access_key)` for `profile` from the text
/// of an AWS-format credentials file. Public so the daemon loads the endpoint
/// keypair (`[auth]`) exactly the way the backend loads origin credentials —
/// one parser, one format. Returns `None` if the profile or either key is
/// missing.
pub fn read_aws_keypair(text: &str, profile: &str) -> Option<(String, String)> {
    parse_aws_credentials(text, profile).map(|(access, secret, _token)| (access, secret))
}

/// Returns `None` if the profile is absent or lacks the two required keys. The
/// format is a subset: `[profile]` section headers and `key = value` lines,
/// `#`/`;` comments, whitespace ignored. Pure so it is unit-tested directly.
pub(crate) fn parse_aws_credentials(
    text: &str,
    profile: &str,
) -> Option<(String, String, Option<String>)> {
    let mut in_profile = false;
    let mut access_key: Option<String> = None;
    let mut secret_key: Option<String> = None;
    let mut session_token: Option<String> = None;
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(name) = line.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
            in_profile = name.trim() == profile;
            continue;
        }
        if !in_profile {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            let key = key.trim().to_ascii_lowercase();
            let value = value.trim().to_owned();
            match key.as_str() {
                "aws_access_key_id" => access_key = Some(value),
                "aws_secret_access_key" => secret_key = Some(value),
                "aws_session_token" => session_token = Some(value),
                _ => {}
            }
        }
    }
    match (access_key, secret_key) {
        (Some(a), Some(s)) => Some((a, s, session_token)),
        _ => None,
    }
}

/// Resolved Azure Blob credentials. Exactly one authentication mode is present:
/// a shared account key, a SAS token, or a client-secret (service principal)
/// triple. The account name always accompanies them (it is not a secret and may
/// also come from `config.toml`, but the credentials file is where the operator
/// keeps it alongside the key).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AzureCredentials {
    /// The storage account name.
    pub account: String,
    /// The chosen authentication mode.
    pub auth: AzureAuth,
}

/// The Azure authentication mode resolved from the credentials file or env.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AzureAuth {
    /// A shared account key (`azure_storage_account_key`).
    AccountKey(String),
    /// A shared-access-signature token (`azure_storage_sas_key`), a raw query
    /// string like `sv=...&sig=...`.
    SasToken(String),
    /// A service-principal client-secret triple (client id, client secret,
    /// tenant id).
    ClientSecret {
        /// The app registration's client id.
        client_id: String,
        /// The app registration's client secret.
        client_secret: String,
        /// The Azure AD tenant id.
        tenant_id: String,
    },
}

impl AzureCredentials {
    /// Resolves Azure credentials from the config-named key=value file when
    /// `backend.credentials_file` is set, else from the `AZURE_STORAGE_*` /
    /// `AZURE_CLIENT_*` env vars. The account name is required. Exactly one auth
    /// mode must resolve; a file naming none, or an incomplete client-secret
    /// triple, is an error naming what to set. Never a silent connect-to-nothing.
    pub fn resolve<F>(backend: &Backend, env: &F) -> Result<Self, BackendError>
    where
        F: Fn(&str) -> Option<String>,
    {
        // Resolve to a flat key=value map either way (Azure carries one
        // credential, so there are no profiles): the config-named file when set,
        // else the same keys read from the env. A map lookup then reads each key.
        let map = azure_credential_map(backend, env)?;
        Self::from_map(map)
    }

    /// Parses one complete Azure credentials-file payload. The refreshing Azure
    /// provider calls this for every request so a rotated shared key or SAS
    /// token is visible without rebuilding the origin client.
    pub(crate) fn from_file_text(text: &str) -> Result<Self, BackendError> {
        Self::from_map(parse_kv_file(text))
    }

    /// Resolves one parsed Azure key/value map into its account and auth mode.
    fn from_map(map: std::collections::HashMap<String, String>) -> Result<Self, BackendError> {
        let get = |k: &str| map.get(k).filter(|s| !s.is_empty()).cloned();

        let account = get("azure_storage_account_name")
            .ok_or_else(|| {
                settings_error(
                    "no Azure account: set azure_storage_account_name in backend.credentials_file, or export AZURE_STORAGE_ACCOUNT_NAME",
                )
            })?;

        let auth = if let Some(key) = get("azure_storage_account_key") {
            AzureAuth::AccountKey(key)
        } else if let Some(sas) = get("azure_storage_sas_key") {
            AzureAuth::SasToken(sas)
        } else {
            // The client-secret triple must be complete or it is an error, not a
            // partial fall-through to a broken client.
            match (
                get("azure_client_id"),
                get("azure_client_secret"),
                get("azure_tenant_id"),
            ) {
                (Some(client_id), Some(client_secret), Some(tenant_id)) => {
                    AzureAuth::ClientSecret {
                        client_id,
                        client_secret,
                        tenant_id,
                    }
                }
                _ => {
                    return Err(settings_error(
                        "no Azure credential: set azure_storage_account_key, azure_storage_sas_key, or the azure_client_id/azure_client_secret/azure_tenant_id triple",
                    ));
                }
            }
        };
        Ok(AzureCredentials { account, auth })
    }
}

/// The Azure credential keys read from the env when no credentials file is named.
const AZURE_ENV_KEYS: &[&str] = &[
    "azure_storage_account_name",
    "azure_storage_account_key",
    "azure_storage_sas_key",
    "azure_client_id",
    "azure_client_secret",
    "azure_tenant_id",
];

/// Builds the Azure credential key=value map: the config-named file parsed when
/// set, else the known Azure keys read from the env (the `AZURE_STORAGE_*` /
/// `AZURE_CLIENT_*` story). Keeping both paths as one map means the resolution
/// logic reads a single source.
fn azure_credential_map<F>(
    backend: &Backend,
    env: &F,
) -> Result<std::collections::HashMap<String, String>, BackendError>
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(file) = backend.credentials_file.as_ref().filter(|s| !s.is_empty()) {
        let text = std::fs::read_to_string(Path::new(file)).map_err(|e| {
            settings_error(format!(
                "cannot read backend.credentials_file `{file}`: {e}"
            ))
        })?;
        return Ok(parse_kv_file(&text));
    }
    // No file named: read the known keys from the env by their exact names.
    let mut map = std::collections::HashMap::new();
    for key in AZURE_ENV_KEYS {
        if let Some(value) = env(key).filter(|s| !s.is_empty()) {
            map.insert((*key).to_owned(), value);
        }
    }
    Ok(map)
}

/// Resolved Google Cloud Storage credentials: the path to the service-account
/// JSON file. GCS auth is a single JSON key; the object-store client reads it
/// from the path (the JSON itself never lands in `config.toml`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcpCredentials {
    /// Filesystem path to the service-account JSON.
    pub service_account_path: String,
}

impl GcpCredentials {
    /// Resolves GCP credentials from the config-named key=value file
    /// (`google_service_account_path` or `google_application_credentials`) when
    /// `backend.credentials_file` is set, else from `GOOGLE_APPLICATION_CREDENTIALS`.
    /// A missing path is an error naming what to set.
    pub fn resolve<F>(backend: &Backend, env: &F) -> Result<Self, BackendError>
    where
        F: Fn(&str) -> Option<String>,
    {
        // Two accepted keys name the same thing; the object-store-flavoured key
        // wins, then the Google env-var name, so either convention works.
        if let Some(file) = backend.credentials_file.as_ref().filter(|s| !s.is_empty()) {
            let text = std::fs::read_to_string(Path::new(file)).map_err(|e| {
                settings_error(format!(
                    "cannot read backend.credentials_file `{file}`: {e}"
                ))
            })?;
            let map = parse_kv_file(&text);
            let path = map
                .get("google_service_account_path")
                .or_else(|| map.get("google_application_credentials"))
                .filter(|s| !s.is_empty())
                .cloned()
                .ok_or_else(|| {
                    settings_error(format!(
                        "credentials file `{file}` names no service account: set google_service_account_path"
                    ))
                })?;
            return Ok(GcpCredentials {
                service_account_path: path,
            });
        }
        let path = env("GOOGLE_APPLICATION_CREDENTIALS")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                settings_error(
                    "no GCP credential: set google_service_account_path in backend.credentials_file, or export GOOGLE_APPLICATION_CREDENTIALS",
                )
            })?;
        Ok(GcpCredentials {
            service_account_path: path,
        })
    }
}

/// Parses a flat `key = value` credentials file into a lowercased-key map. The
/// format mirrors the AWS parser minus profile sections: `key = value` lines,
/// `#`/`;` comments, blank lines and surrounding whitespace ignored. Azure and
/// GCP each carry a single credential, so there are no profiles. Pure so it is
/// unit-tested directly.
fn parse_kv_file(text: &str) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        // A stray `[section]` header is ignored rather than folded into a key.
        if line.starts_with('[') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            map.insert(key.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aws_credentials_parser_reads_the_named_profile() {
        // The parser reads the requested profile's keys, ignores other profiles,
        // comments, and blank lines, and picks up an optional session token.
        let text = "\
# a comment\n\
[default]\n\
aws_access_key_id = DEFAULT_KEY\n\
aws_secret_access_key = DEFAULT_SECRET\n\
\n\
[oci]\n\
; oci profile\n\
aws_access_key_id = OCI_KEY\n\
aws_secret_access_key = OCI_SECRET\n\
aws_session_token = OCI_TOKEN\n";
        let (a, s, t) = parse_aws_credentials(text, "oci").expect("oci profile present");
        assert_eq!(a, "OCI_KEY");
        assert_eq!(s, "OCI_SECRET");
        assert_eq!(t.as_deref(), Some("OCI_TOKEN"));

        let (a, s, t) = parse_aws_credentials(text, "default").expect("default profile present");
        assert_eq!(a, "DEFAULT_KEY");
        assert_eq!(s, "DEFAULT_SECRET");
        assert_eq!(t, None);
    }

    #[test]
    fn aws_credentials_parser_returns_none_for_incomplete_profile() {
        // A profile missing the secret key is not usable.
        let text = "[half]\naws_access_key_id = ONLY_KEY\n";
        assert!(parse_aws_credentials(text, "half").is_none());
        // An absent profile is None too.
        assert!(parse_aws_credentials(text, "nope").is_none());
    }

    #[test]
    fn credentials_file_does_not_block_endpoint_setting_resolution() {
        // A named credentials file is read later by the refreshing provider;
        // endpoint settings themselves remain pure and do no credential I/O.
        let dir = std::env::temp_dir().join(format!("verglas-creds-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let path = dir.join("oci");
        std::fs::write(
            &path,
            "[default]\naws_access_key_id = FILE_KEY\naws_secret_access_key = FILE_SECRET\n",
        )
        .expect("write creds");

        let backend = Backend {
            endpoint: Some("https://oci.example.com".to_owned()),
            credentials_file: Some(path.display().to_string()),
            ..Backend::default()
        };
        // No AWS_* env at all.
        let read = |_: &str| None;
        let settings = BackendSettings::resolve(&backend, &read).expect("resolves from file");
        assert_eq!(settings.endpoint, "https://oci.example.com");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Writes `contents` to a fresh temp file and returns its path. Helper for the
    /// per-provider credentials-file round-trip tests.
    fn write_creds(tag: &str, contents: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "verglas-creds-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let path = dir.join("creds");
        std::fs::write(&path, contents).expect("write creds");
        path
    }

    #[test]
    fn kv_parser_reads_keys_ignores_comments_and_sections() {
        // The flat key=value parser lowercases keys, trims whitespace, and skips
        // comments, blank lines, and a stray section header.
        let text = "\
# a comment\n\
; another\n\
\n\
[ignored]\n\
Azure_Storage_Account_Name = acct\n\
azure_storage_account_key =  KEY==  \n";
        let map = parse_kv_file(text);
        assert_eq!(
            map.get("azure_storage_account_name").map(String::as_str),
            Some("acct")
        );
        assert_eq!(
            map.get("azure_storage_account_key").map(String::as_str),
            Some("KEY==")
        );
    }

    #[test]
    fn azure_credentials_round_trip_account_key() {
        // An azure credentials file with an account key resolves to the account
        // key auth mode, file text -> resolved credential.
        let path = write_creds(
            "azkey",
            "azure_storage_account_name = myacct\nazure_storage_account_key = SECRETKEY==\n",
        );
        let backend = Backend {
            provider: verglas_core::config::BackendProvider::Azure,
            credentials_file: Some(path.display().to_string()),
            ..Backend::default()
        };
        let creds = AzureCredentials::resolve(&backend, &|_| None).expect("resolves");
        assert_eq!(creds.account, "myacct");
        assert_eq!(creds.auth, AzureAuth::AccountKey("SECRETKEY==".to_owned()));
    }

    #[test]
    fn azure_credentials_round_trip_sas_and_client_secret() {
        // A SAS token resolves to the SAS auth mode.
        let sas = write_creds(
            "azsas",
            "azure_storage_account_name = acct\nazure_storage_sas_key = sv=2021&sig=abc\n",
        );
        let backend = Backend {
            credentials_file: Some(sas.display().to_string()),
            ..Backend::default()
        };
        let creds = AzureCredentials::resolve(&backend, &|_| None).expect("resolves");
        assert_eq!(
            creds.auth,
            AzureAuth::SasToken("sv=2021&sig=abc".to_owned())
        );

        // A complete client-secret triple resolves to the client-secret mode.
        let sp = write_creds(
            "azsp",
            "azure_storage_account_name = acct\nazure_client_id = cid\nazure_client_secret = csecret\nazure_tenant_id = tid\n",
        );
        let backend = Backend {
            credentials_file: Some(sp.display().to_string()),
            ..Backend::default()
        };
        let creds = AzureCredentials::resolve(&backend, &|_| None).expect("resolves");
        assert_eq!(
            creds.auth,
            AzureAuth::ClientSecret {
                client_id: "cid".to_owned(),
                client_secret: "csecret".to_owned(),
                tenant_id: "tid".to_owned(),
            }
        );
    }

    #[test]
    fn azure_credentials_error_when_no_auth_mode() {
        // An account name with no key, SAS, or complete triple is an error naming
        // what to set — never a silent connect-to-nothing.
        let path = write_creds("aznone", "azure_storage_account_name = acct\n");
        let backend = Backend {
            credentials_file: Some(path.display().to_string()),
            ..Backend::default()
        };
        let err = AzureCredentials::resolve(&backend, &|_| None).expect_err("must error");
        assert!(
            err.to_string().contains("azure_storage_account_key"),
            "{err}"
        );
    }

    #[test]
    fn azure_credentials_fall_back_to_env() {
        // With no file named, the AZURE_STORAGE_* env supplies the account and key.
        let backend = Backend {
            provider: verglas_core::config::BackendProvider::Azure,
            ..Backend::default()
        };
        let env = |k: &str| match k {
            "azure_storage_account_name" => Some("envacct".to_owned()),
            "azure_storage_account_key" => Some("ENVKEY".to_owned()),
            _ => None,
        };
        let creds = AzureCredentials::resolve(&backend, &env).expect("resolves from env");
        assert_eq!(creds.account, "envacct");
        assert_eq!(creds.auth, AzureAuth::AccountKey("ENVKEY".to_owned()));
    }

    #[test]
    fn gcp_credentials_round_trip_service_account_path() {
        // A gcp credentials file naming the service-account path resolves to it,
        // file text -> resolved credential.
        let path = write_creds(
            "gcp",
            "google_service_account_path = /home/op/.verglas/sa.json\n",
        );
        let backend = Backend {
            provider: verglas_core::config::BackendProvider::Gcp,
            credentials_file: Some(path.display().to_string()),
            ..Backend::default()
        };
        let creds = GcpCredentials::resolve(&backend, &|_| None).expect("resolves");
        assert_eq!(creds.service_account_path, "/home/op/.verglas/sa.json");
    }

    #[test]
    fn gcp_credentials_accept_google_application_credentials_key_and_env() {
        // The google_application_credentials key is accepted as an alias.
        let path = write_creds(
            "gcpalias",
            "google_application_credentials = /etc/gcp/sa.json\n",
        );
        let backend = Backend {
            credentials_file: Some(path.display().to_string()),
            ..Backend::default()
        };
        let creds = GcpCredentials::resolve(&backend, &|_| None).expect("resolves");
        assert_eq!(creds.service_account_path, "/etc/gcp/sa.json");

        // With no file, GOOGLE_APPLICATION_CREDENTIALS from the env supplies it.
        let backend = Backend::default();
        let env =
            |k: &str| (k == "GOOGLE_APPLICATION_CREDENTIALS").then(|| "/env/sa.json".to_owned());
        let creds = GcpCredentials::resolve(&backend, &env).expect("resolves from env");
        assert_eq!(creds.service_account_path, "/env/sa.json");
    }

    #[test]
    fn gcp_credentials_error_when_no_path() {
        // A file with no service-account path is an error naming what to set.
        let path = write_creds("gcpnone", "# empty\n");
        let backend = Backend {
            credentials_file: Some(path.display().to_string()),
            ..Backend::default()
        };
        let err = GcpCredentials::resolve(&backend, &|_| None).expect_err("must error");
        assert!(
            err.to_string().contains("google_service_account_path"),
            "{err}"
        );
    }
}
