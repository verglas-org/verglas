//! Control plane HTTP client for the Verglas CLI.
//!
//! This is the CLI's one remote surface. When the user has run `verglas login`,
//! the CLI authenticates to a multi-tenant control plane with an API key and
//! reads the registry of worker deployments across the user's local nodes and
//! the cloud. The API key is stored at
//! `~/.verglas/credentials/control-plane-token` (mode 0600) and the base URL in
//! `~/.verglas/config.toml` under `[control_plane]`.
//!
//! Everything else in the CLI works without a login: local `workers` verbs
//! talk to the server endpoint alone. When the user is logged in, `list`/`show`
//! can enrich their output with the tenant's cloud deployments through this
//! module. This module reports a clear "run `verglas login`" error when no key
//! is stored.

use std::path::PathBuf;

use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Verglas Cloud control plane. `verglas login` uses this when `--url` is omitted
/// and no prior `[control_plane].url` is stored.
pub const DEFAULT_CONTROL_PLANE_URL: &str = "https://api.verglas.dev";

/// The me endpoint, relative to the control plane base URL.
const ME_PATH: &str = "/v1/me";
/// The lakehouse-config endpoint, relative to the control plane base URL. Returns
/// the tenant's SCOPED lakehouse config that `verglas login` writes the server
/// config from.
const LAKEHOUSE_PATH: &str = "/v1/lakehouse";
/// The deployments-list endpoint, relative to the control plane base URL.
const DEPLOYMENTS_PATH: &str = "/v1/deployments";
/// The credential file that holds the API key, under `~/.verglas/credentials/`.
const TOKEN_FILE: &str = "control-plane-token";

/// The OAuth client id the CLI always presents. There is exactly one CLI client.
const CLIENT_ID: &str = "verglas-cli";
/// The authorization endpoint the browser flow opens in a browser.
const AUTHORIZE_PATH: &str = "/oauth/authorize";
/// The token endpoint both OAuth flows POST to (authorization-code and device
/// grants). Distinct from the token FILE on disk.
const OAUTH_TOKEN_PATH: &str = "/oauth/token";
/// The device-code endpoint the device flow starts with.
const DEVICE_CODE_PATH: &str = "/oauth/device/code";
/// The provision endpoint both OAuth flows converge on to obtain the long-lived
/// api key and the scoped lakehouse config.
const PROVISION_PATH: &str = "/v1/provision";

/// The account a key belongs to, from `GET /v1/me`. Printed by `verglas login`
/// to confirm which account the CLI is now acting as.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Account {
    /// The tenant the key is scoped to.
    pub tenant_id: String,
    /// The email the account is registered under.
    pub account_email: String,
}

/// The tenant's SCOPED lakehouse config, from `GET /v1/lakehouse`. Everything a
/// server needs to serve this tenant's bucket: the S3 endpoint/region, the
/// Iceberg warehouse and catalog URI, and the tenant's OWN bucket-scoped
/// credentials (an S3 key pair and a catalog bearer token — never account-wide).
/// `verglas login` writes the server config and its 0600 credential files from
/// this. The secret fields are written only to mode-0600 files, never printed.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Lakehouse {
    /// The bucket this tenant's data lives in.
    pub bucket: String,
    /// S3 endpoint for the bucket (the backend origin the server serves).
    pub s3_endpoint: String,
    /// S3 signing region for the bucket.
    pub s3_region: String,
    /// Iceberg warehouse identifier for the catalog.
    pub warehouse: String,
    /// Base URI of the Iceberg REST catalog.
    pub catalog_uri: String,
    /// S3 access key id (an identifier; the matching secret is a secret).
    pub access_key_id: String,
    /// S3 secret access key. Secret — written only to a 0600 file.
    pub secret_access_key: String,
    /// Catalog bearer token. Secret — written only to a 0600 file.
    pub catalog_token: String,
}

/// An OAuth token response from `POST /oauth/token` (both grants). The CLI uses
/// the access token as the bearer for `POST /v1/provision`; it is never written
/// to disk or printed. The server also returns `token_type` and `expires_in`,
/// which the CLI does not act on, so they are not decoded here.
#[derive(Debug, Clone, Deserialize)]
pub struct TokenResponse {
    /// The bearer credential for the provision call.
    pub access_token: String,
}

/// A device-authorization response from `POST /oauth/device/code`. Carries the
/// human-facing user code and verification URIs plus the poll cadence. The server
/// also returns `expires_in`, which the CLI does not act on and does not decode.
#[derive(Debug, Clone, Deserialize)]
pub struct DeviceCodeResponse {
    /// The opaque code the CLI polls the token endpoint with.
    pub device_code: String,
    /// The short code the human types on the verification page.
    pub user_code: String,
    /// The URI the human opens to authorize.
    pub verification_uri: String,
    /// The pre-filled verification URI (user code already embedded).
    #[serde(default)]
    pub verification_uri_complete: Option<String>,
    /// Minimum seconds the CLI must wait between polls.
    pub interval: u64,
}

/// The `POST /v1/provision` response. Carries the tenant identity, the long-lived
/// control-plane api key the CLI stores, and the tenant's scoped lakehouse config
/// (identical field names to [`Lakehouse`], so it embeds one directly).
#[derive(Debug, Clone, Deserialize)]
pub struct ProvisionResponse {
    /// The tenant the account belongs to.
    pub tenant_id: String,
    /// The email the account is registered under.
    pub account_email: String,
    /// The long-lived control-plane api key for the server and CLI. Secret —
    /// written only to the 0600 token file, never printed.
    pub api_key: String,
    /// The tenant's scoped lakehouse config, written to the server config and
    /// its 0600 credential files.
    pub lakehouse: Lakehouse,
}

/// The `error` field of an OAuth error body (`POST /oauth/token` 400 responses).
#[derive(Debug, Deserialize)]
struct OauthError {
    /// The machine-readable error code, e.g. `authorization_pending`.
    error: String,
}

/// One deployment row from `GET /v1/deployments`. A deployment is a Source,
/// The default deployment trigger for control planes that omit it: run-once on
/// demand (mirrors the cloud `deployments.trigger` default).
fn default_trigger() -> String {
    "manual".to_owned()
}

/// Sink, or MV code artifact the control plane knows about, wherever it runs.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Deployment {
    /// Stable control-plane id.
    pub id: String,
    /// Human name of the deployment.
    pub name: String,
    /// `source`, `sink`, or `mv`.
    pub kind: String,
    /// `local` (one of the user's nodes) or `cloud`.
    pub placement: String,
    /// How the deployment is invoked: `cron`, `webhook`, or `manual` (§7.1).
    /// Older control planes omit it; it defaults to `manual`.
    #[serde(default = "default_trigger")]
    pub trigger: String,
    /// The node it runs on, when placed locally. Absent for cloud placements.
    #[serde(default)]
    pub node_id: Option<String>,
    /// Runtime status (for example `running`, `paused`).
    pub status: String,
    /// The schedule note, when the deployment carries one.
    #[serde(default)]
    pub schedule: Option<String>,
    /// The tables this deployment writes to.
    #[serde(default)]
    pub target_tables: Vec<String>,
    /// When the control plane last saw a change to this deployment.
    pub updated_at: String,
    /// The artifact's code — the ingestion/transform/delivery source. The
    /// deployment-list endpoint omits it (it would bloat the listing); the
    /// deployment-detail endpoint (`GET /v1/deployments/:id`) carries it, which
    /// is what `verglas source|mv|sink show` fetches to display the code.
    #[serde(default)]
    pub code: Option<String>,
}

/// Failures talking to the control plane, each with a message a user can act on.
#[derive(Debug, Error)]
pub enum ControlPlaneError {
    /// No stored key or URL: the control-plane verbs need a login first.
    #[error("not logged in to a control plane — run `verglas login` first")]
    NotLoggedIn,
    /// The control plane rejected the key (HTTP 401).
    #[error("the control plane rejected the API key")]
    Unauthorized,
    /// The resource does not exist for this tenant (HTTP 404).
    #[error("the control plane has no such resource for this tenant")]
    NotFound,
    /// The configured base URL could not be parsed.
    #[error("invalid control plane URL `{0}`")]
    InvalidUrl(String),
    /// The request could not be sent.
    #[error("control plane request failed: {0}")]
    Request(reqwest::Error),
    /// The control plane returned a non-success status other than 401.
    #[error("control plane returned HTTP {status} for {path}")]
    UnexpectedStatus {
        /// The status the control plane returned.
        status: StatusCode,
        /// The path that returned it.
        path: &'static str,
    },
    /// The response body could not be decoded into the expected shape.
    #[error("could not decode control plane response from {path}: {source}")]
    Decode {
        /// The path whose body failed to decode.
        path: &'static str,
        /// The underlying JSON error.
        source: reqwest::Error,
    },
    /// A resource route returned a non-success status carrying the server's own
    /// error message. The message is surfaced verbatim so the CLI exits with the
    /// server's explanation (e.g. `deployment already exists`).
    #[error("the control plane returned HTTP {status} for {path}: {message}")]
    Api {
        /// The status the control plane returned.
        status: u16,
        /// The path (with any interpolated id) that returned it.
        path: String,
        /// The server's error message (its `error` field, or the raw body).
        message: String,
    },
    /// A resource response body could not be read or parsed as JSON. `path`
    /// carries the interpolated id, so this is a runtime `String`.
    #[error("could not read the control plane response from {path}: {message}")]
    DecodeAt {
        /// The path whose body failed to read/parse.
        path: String,
        /// The read/parse failure.
        message: String,
    },
    /// A stored file could not be read.
    #[error("could not read {0}")]
    Io(PathBuf),
    /// HOME is unset, so `~/.verglas` cannot be located.
    #[error("HOME is not set, so ~/.verglas cannot be located")]
    NoHome,
    /// The device authorization has not completed yet: keep polling.
    #[error("authorization pending")]
    AuthorizationPending,
    /// The server asked the CLI to poll more slowly.
    #[error("slow down")]
    SlowDown,
    /// The user denied the authorization request.
    #[error("authorization was denied")]
    AccessDenied,
    /// The device or authorization code expired before the user finished.
    #[error("the authorization code expired before it was used")]
    Expired,
    /// The state parameter returned by the browser did not match the one sent, so
    /// the redirect cannot be trusted.
    #[error("the login response did not match this request (state mismatch); start over")]
    StateMismatch,
    /// The loopback callback could not be received or parsed.
    #[error("could not receive the browser redirect: {0}")]
    Callback(String),
    /// An OAuth endpoint returned an error the CLI does not handle specially.
    #[error("the control plane rejected the login: {0}")]
    Oauth(String),
}

impl ControlPlaneError {
    /// Whether the failure looks like the ROUTE is absent on the server rather
    /// than the resource being missing — a 404/405/501 from a control plane older
    /// than this CLI (a route a parallel effort has not shipped yet). The resource
    /// command groups use this to turn such a status into a clear "the control
    /// plane does not support this yet" message instead of a cryptic HTTP error.
    /// A 404 arrives as [`ControlPlaneError::NotFound`] (every status site maps
    /// it), and the CLI cannot tell a missing route from a missing resource, so
    /// `NotFound` counts as route-absent here too.
    pub fn route_absent(&self) -> bool {
        matches!(
            self,
            ControlPlaneError::NotFound
                | ControlPlaneError::Api {
                    status: 404 | 405 | 501,
                    ..
                }
        )
    }
}

/// HTTP client for the control plane API, carrying the base URL and the bearer
/// token for every call.
#[derive(Debug, Clone)]
pub struct ControlPlaneClient {
    base: reqwest::Url,
    token: String,
    http: reqwest::Client,
}

impl ControlPlaneClient {
    /// Builds a client against `base_url` with `token`. Public so `verglas
    /// login` can verify a key before storing it, and so tests can point one at
    /// a mock. A trailing slash is added to the base so relative paths join onto
    /// it rather than replacing it.
    pub fn new(base_url: &str, token: &str) -> Result<Self, ControlPlaneError> {
        let normalized = if base_url.ends_with('/') {
            base_url.to_owned()
        } else {
            format!("{base_url}/")
        };
        let base = reqwest::Url::parse(&normalized)
            .map_err(|_| ControlPlaneError::InvalidUrl(base_url.to_owned()))?;
        Ok(Self {
            base,
            token: token.to_owned(),
            http: reqwest::Client::new(),
        })
    }

    /// Builds a client from the stored token and URL. Returns
    /// [`ControlPlaneError::NotLoggedIn`] when either is missing, so a
    /// control-plane verb run without a login fails with a clear pointer to
    /// `verglas login` rather than a cryptic transport error.
    pub fn from_stored() -> Result<Self, ControlPlaneError> {
        let url = stored_url()?.ok_or(ControlPlaneError::NotLoggedIn)?;
        let token = stored_token()?.ok_or(ControlPlaneError::NotLoggedIn)?;
        Self::new(&url, &token)
    }

    /// Fetches the account the key belongs to (`GET /v1/me`). Used by `login`
    /// to verify a key and to confirm which account it belongs to.
    pub async fn me(&self) -> Result<Account, ControlPlaneError> {
        self.request(ME_PATH.to_owned(), ME_PATH).await
    }

    /// Fetches the tenant's scoped lakehouse config (`GET /v1/lakehouse`). Used
    /// by `login` to write the server config and its credential files after the
    /// key is verified.
    pub async fn lakehouse(&self) -> Result<Lakehouse, ControlPlaneError> {
        self.request(LAKEHOUSE_PATH.to_owned(), LAKEHOUSE_PATH)
            .await
    }

    /// Lists every deployment across the user's local nodes and the cloud
    /// (`GET /v1/deployments`). The `source|mv|sink list` verbs filter the
    /// result by `kind` and merge it with the local self-managed rows.
    pub async fn deployments(&self) -> Result<Vec<Deployment>, ControlPlaneError> {
        // The list endpoint wraps the rows in a `deployments` field.
        #[derive(serde::Deserialize)]
        struct Wrapper {
            deployments: Vec<Deployment>,
        }
        let wrapper: Wrapper = self
            .request(DEPLOYMENTS_PATH.to_owned(), DEPLOYMENTS_PATH)
            .await?;
        Ok(wrapper.deployments)
    }

    /// Builds a client against `base_url` with no stored token. The OAuth flows
    /// use it before any credential exists: the token endpoints are unauthenticated
    /// and `provision` carries the freshly obtained access token itself.
    pub fn anonymous(base_url: &str) -> Result<Self, ControlPlaneError> {
        Self::new(base_url, "")
    }

    /// Joins a path onto the base URL, mapping a parse failure to a clear error.
    fn join(&self, path: &str) -> Result<reqwest::Url, ControlPlaneError> {
        self.base
            .join(path.trim_start_matches('/'))
            .map_err(|_| ControlPlaneError::InvalidUrl(path.to_owned()))
    }

    /// Builds the authorization URL the browser flow opens. The redirect URI and
    /// state are percent-encoded into the query. The caller supplies the loopback
    /// `redirect_uri`, the PKCE `code_challenge`, and the random `state`.
    pub fn authorize_url(
        &self,
        redirect_uri: &str,
        code_challenge: &str,
        state: &str,
    ) -> Result<String, ControlPlaneError> {
        let mut url = self.join(AUTHORIZE_PATH)?;
        url.query_pairs_mut()
            .append_pair("response_type", "code")
            .append_pair("client_id", CLIENT_ID)
            .append_pair("redirect_uri", redirect_uri)
            .append_pair("code_challenge", code_challenge)
            .append_pair("code_challenge_method", "S256")
            .append_pair("state", state);
        Ok(url.to_string())
    }

    /// Exchanges an authorization code for an access token (browser flow), sending
    /// the PKCE `code_verifier` and the same `redirect_uri` the code was issued
    /// for.
    pub async fn exchange_code(
        &self,
        code: &str,
        code_verifier: &str,
        redirect_uri: &str,
    ) -> Result<TokenResponse, ControlPlaneError> {
        self.post_token(&[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("code_verifier", code_verifier),
            ("redirect_uri", redirect_uri),
            ("client_id", CLIENT_ID),
        ])
        .await
    }

    /// Starts the device flow (`POST /oauth/device/code`), returning the user code
    /// and verification URIs to show the human plus the poll interval.
    pub async fn device_code(&self) -> Result<DeviceCodeResponse, ControlPlaneError> {
        let url = self.join(DEVICE_CODE_PATH)?;
        let response = self
            .http
            .post(url)
            .form(&[("client_id", CLIENT_ID)])
            .send()
            .await
            .map_err(ControlPlaneError::Request)?;
        let status = response.status();
        if !status.is_success() {
            return Err(ControlPlaneError::UnexpectedStatus {
                status,
                path: DEVICE_CODE_PATH,
            });
        }
        response
            .json()
            .await
            .map_err(|source| ControlPlaneError::Decode {
                path: DEVICE_CODE_PATH,
                source,
            })
    }

    /// Polls the token endpoint once for a device authorization. Maps the OAuth
    /// pending/slow-down/denied/expired errors to their variants so the caller can
    /// keep polling or fail with a clear message.
    pub async fn poll_device_token(
        &self,
        device_code: &str,
    ) -> Result<TokenResponse, ControlPlaneError> {
        self.post_token(&[
            ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
            ("device_code", device_code),
            ("client_id", CLIENT_ID),
        ])
        .await
    }

    /// POSTs a form to the token endpoint and decodes the token, translating a 400
    /// OAuth error body into the matching [`ControlPlaneError`] variant.
    async fn post_token(
        &self,
        params: &[(&str, &str)],
    ) -> Result<TokenResponse, ControlPlaneError> {
        let url = self.join(OAUTH_TOKEN_PATH)?;
        let response = self
            .http
            .post(url)
            .form(params)
            .send()
            .await
            .map_err(ControlPlaneError::Request)?;
        let status = response.status();
        if status.is_success() {
            return response
                .json()
                .await
                .map_err(|source| ControlPlaneError::Decode {
                    path: OAUTH_TOKEN_PATH,
                    source,
                });
        }
        if status == StatusCode::BAD_REQUEST {
            let body: OauthError =
                response
                    .json()
                    .await
                    .map_err(|source| ControlPlaneError::Decode {
                        path: OAUTH_TOKEN_PATH,
                        source,
                    })?;
            return Err(map_oauth_error(body.error));
        }
        Err(ControlPlaneError::UnexpectedStatus {
            status,
            path: OAUTH_TOKEN_PATH,
        })
    }

    /// Provisions the tenant with a freshly obtained OAuth access token
    /// (`POST /v1/provision`, empty body). The bearer is the access token, NOT the
    /// client's stored token. Maps 401 to a clear "authorization expired" error.
    pub async fn provision(
        &self,
        access_token: &str,
    ) -> Result<ProvisionResponse, ControlPlaneError> {
        let url = self.join(PROVISION_PATH)?;
        let response = self
            .http
            .post(url)
            .bearer_auth(access_token)
            .send()
            .await
            .map_err(ControlPlaneError::Request)?;
        let status = response.status();
        if status == StatusCode::UNAUTHORIZED {
            return Err(ControlPlaneError::Unauthorized);
        }
        if status == StatusCode::NOT_FOUND {
            return Err(ControlPlaneError::NotFound);
        }
        if !status.is_success() {
            return Err(ControlPlaneError::UnexpectedStatus {
                status,
                path: PROVISION_PATH,
            });
        }
        response
            .json()
            .await
            .map_err(|source| ControlPlaneError::Decode {
                path: PROVISION_PATH,
                source,
            })
    }

    /// Bearer-authenticated `GET` returning the raw JSON value. Used by the cloud
    /// resource command groups (workers/containers/tables/db), which render the
    /// server's shape generically. A non-success status surfaces the server's own
    /// error message via [`ControlPlaneError::Api`].
    pub async fn get_value(&self, path: &str) -> Result<serde_json::Value, ControlPlaneError> {
        let url = self.join(path)?;
        self.send_value(self.http.get(url), path).await
    }

    /// Bearer-authenticated `POST` of `body`, returning the raw JSON response.
    pub async fn post_value(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, ControlPlaneError> {
        let url = self.join(path)?;
        self.send_value(self.http.post(url).json(body), path).await
    }

    /// Bearer-authenticated `PATCH` of `body`, returning the raw JSON response.
    pub async fn patch_value(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, ControlPlaneError> {
        let url = self.join(path)?;
        self.send_value(self.http.patch(url).json(body), path).await
    }

    /// Bearer-authenticated `PUT` of `body`, returning the raw JSON response.
    pub async fn put_value(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, ControlPlaneError> {
        let url = self.join(path)?;
        self.send_value(self.http.put(url).json(body), path).await
    }

    /// Bearer-authenticated `DELETE`, returning the raw JSON response (or JSON
    /// null when the server answers with an empty body).
    pub async fn delete_value(&self, path: &str) -> Result<serde_json::Value, ControlPlaneError> {
        let url = self.join(path)?;
        self.send_value(self.http.delete(url), path).await
    }

    /// Sends a bearer-authenticated request built by the caller and decodes the
    /// response as a raw JSON value. Maps 401 to [`ControlPlaneError::Unauthorized`],
    /// any other non-success status to [`ControlPlaneError::Api`] carrying the
    /// server's error message, and tolerates an empty success body (decoded as
    /// JSON null so a bodyless 204 does not look like a failure).
    async fn send_value(
        &self,
        builder: reqwest::RequestBuilder,
        path: &str,
    ) -> Result<serde_json::Value, ControlPlaneError> {
        let response = builder
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(ControlPlaneError::Request)?;
        let status = response.status();
        if status == StatusCode::UNAUTHORIZED {
            return Err(ControlPlaneError::Unauthorized);
        }
        if status == StatusCode::NOT_FOUND {
            return Err(ControlPlaneError::NotFound);
        }
        if !status.is_success() {
            let message = server_error_message(response).await;
            return Err(ControlPlaneError::Api {
                status: status.as_u16(),
                path: path.to_owned(),
                message,
            });
        }
        let text = response
            .text()
            .await
            .map_err(|e| ControlPlaneError::DecodeAt {
                path: path.to_owned(),
                message: e.to_string(),
            })?;
        if text.trim().is_empty() {
            return Ok(serde_json::Value::Null);
        }
        serde_json::from_str(&text).map_err(|e| ControlPlaneError::DecodeAt {
            path: path.to_owned(),
            message: e.to_string(),
        })
    }

    /// Performs a bearer-authenticated JSON `GET` against `path` (relative to the
    /// base URL), mapping 401 to [`ControlPlaneError::Unauthorized`]. `label` is
    /// the static endpoint name used in error messages (it does not carry the
    /// interpolated id).
    async fn request<T>(&self, path: String, label: &'static str) -> Result<T, ControlPlaneError>
    where
        T: serde::de::DeserializeOwned,
    {
        let url = self
            .base
            .join(path.trim_start_matches('/'))
            .map_err(|_| ControlPlaneError::InvalidUrl(path.clone()))?;
        let response = self
            .http
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(ControlPlaneError::Request)?;

        let status = response.status();
        if status == StatusCode::UNAUTHORIZED {
            return Err(ControlPlaneError::Unauthorized);
        }
        if status == StatusCode::NOT_FOUND {
            return Err(ControlPlaneError::NotFound);
        }
        if !status.is_success() {
            return Err(ControlPlaneError::UnexpectedStatus {
                status,
                path: label,
            });
        }
        response
            .json::<T>()
            .await
            .map_err(|source| ControlPlaneError::Decode {
                path: label,
                source,
            })
    }
}

/// Reads the server's error message from a non-success response: the `error`
/// field of a JSON body (the control plane's convention), or the raw body text
/// when it is not that shape. Never fails — a body that cannot be read yields an
/// empty string, so the caller always has SOMETHING to show with the status.
async fn server_error_message(response: reqwest::Response) -> String {
    let text = response.text().await.unwrap_or_default();
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text)
        && let Some(message) = value.get("error").and_then(|e| e.as_str())
    {
        return message.to_owned();
    }
    text.trim().to_owned()
}

/// Maps an OAuth `error` code to a [`ControlPlaneError`] variant. The device flow
/// keeps polling on `authorization_pending`/`slow_down` and fails on
/// `expired_token`/`access_denied`; any other code surfaces verbatim.
fn map_oauth_error(code: String) -> ControlPlaneError {
    match code.as_str() {
        "authorization_pending" => ControlPlaneError::AuthorizationPending,
        "slow_down" => ControlPlaneError::SlowDown,
        "access_denied" => ControlPlaneError::AccessDenied,
        "expired_token" => ControlPlaneError::Expired,
        _ => ControlPlaneError::Oauth(code),
    }
}

/// The `~/.verglas` directory. Errors [`ControlPlaneError::NoHome`] when HOME is
/// unset.
fn verglas_dir() -> Result<PathBuf, ControlPlaneError> {
    let home = std::env::var_os("HOME").ok_or(ControlPlaneError::NoHome)?;
    Ok(PathBuf::from(home).join(".verglas"))
}

/// The path the API key is stored at (`~/.verglas/credentials/control-plane-token`).
pub fn token_path() -> Result<PathBuf, ControlPlaneError> {
    Ok(verglas_dir()?.join("credentials").join(TOKEN_FILE))
}

/// The config file the control plane URL is recorded in (`~/.verglas/config.toml`).
pub fn config_path() -> Result<PathBuf, ControlPlaneError> {
    Ok(verglas_dir()?.join("config.toml"))
}

/// The backend (S3) credentials file `verglas login` writes the tenant's scoped
/// S3 key pair to, in AWS-INI form (`~/.verglas/credentials/backend.credentials`,
/// mode 0600). Referenced by `[backend].credentials_file` in the server config.
pub fn backend_credentials_path() -> Result<PathBuf, ControlPlaneError> {
    Ok(verglas_dir()?
        .join("credentials")
        .join("backend.credentials"))
}

/// The catalog credentials file `verglas login` writes the tenant's scoped
/// catalog bearer token to (`~/.verglas/credentials/catalog.token`, mode 0600).
/// Referenced by `[catalog].credentials_file` in the server config.
pub fn catalog_credentials_path() -> Result<PathBuf, ControlPlaneError> {
    Ok(verglas_dir()?.join("credentials").join("catalog.token"))
}

/// Reads the stored API key, or `None` when no token file exists yet.
fn stored_token() -> Result<Option<String>, ControlPlaneError> {
    let path = token_path()?;
    match std::fs::read_to_string(&path) {
        Ok(text) => {
            let trimmed = text.trim().to_owned();
            Ok((!trimmed.is_empty()).then_some(trimmed))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(ControlPlaneError::Io(path)),
    }
}

/// Reads the stored control plane URL from `[control_plane]` in the config, or
/// `None` when the config or the section is absent. `verglas login` prefers
/// this over the cloud default when re-logging in.
pub fn stored_url() -> Result<Option<String>, ControlPlaneError> {
    let path = config_path()?;
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(ControlPlaneError::Io(path)),
    };
    // Parse only the `[control_plane]` table, so an otherwise-incomplete config
    // (say, a hand-edited file) still yields the URL.
    let Ok(table) = text.parse::<toml::Table>() else {
        return Ok(None);
    };
    Ok(table
        .get("control_plane")
        .and_then(|section| section.get("url"))
        .and_then(|url| url.as_str())
        .map(str::to_owned))
}
