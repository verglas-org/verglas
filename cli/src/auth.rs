//! `CloudCredential`: the single choke point for the WorkOS device-login
//! ceremony and the durable control-plane token it mints.
//!
//! WorkOS is the login ceremony only. Its access token is spent exactly
//! once, as the bearer on `POST {access_endpoint}/v1/provision`; nothing
//! WorkOS-shaped is ever persisted to disk, and no runtime code path
//! contacts WorkOS again. The response's `api_token` — a durable tenant
//! machine token with a server-side 30-day sliding renewal window —
//! persists to `~/.verglas/credentials/control-plane-token` (owner-only).
//!
//! Every cloud-API call site resolves its bearer through [`resolved_bearer`]
//! (env → scoped-token store → durable token file) instead of scattering its
//! own token logic, and every retryable call site routes its error through
//! [`cloud_call`] / [`cloud_call_with_bearer`] for one loud, consistent 401
//! message. There is no refresh loop: a 401 always fails with guidance to
//! run `verglas login` again. As a value-add on top of the server's own
//! sliding window, a successful cloud command opportunistically asks the
//! control plane to rotate the durable token once it is a week old — see
//! [`maybe_renew_durable_token`] — entirely best-effort and never a
//! precondition for the loud-401 contract.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use serde::Deserialize;
use thiserror::Error;

/// Verglas Cloud's production WorkOS public client id, compiled in so a
/// fresh install can `verglas login` with no configuration.
const DEFAULT_CLIENT_ID: &str = "client_01M089ZKTV6PV8AX4DY6JV7X7N";
const DEFAULT_WORKOS_API_BASE: &str = "https://api.workos.com";

/// Floor under a WorkOS-advertised poll interval so a misbehaving or mocked
/// authorization server cannot spin the poll loop.
const MIN_POLL_INTERVAL_SECS: u64 = 1;
/// Overall wall-clock budget for the device code to be approved, mirroring a
/// typical WorkOS device-code lifetime.
const DEVICE_FLOW_DEADLINE: Duration = Duration::from_secs(600);
/// A durable token untouched for this long is offered for opportunistic
/// renewal on the next successful cloud command, keeping a monthly-cadence
/// user signed in indefinitely without ever touching WorkOS again.
const RENEWAL_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Failures in the WorkOS device-login or durable-token persistence paths.
#[derive(Debug, Error)]
pub enum AuthError {
    #[error("cannot read {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("WorkOS request failed: {0}")]
    Request(reqwest::Error),
    #[error("WorkOS returned HTTP {status}: {body}")]
    Workos { status: u16, body: String },
    #[error("timed out waiting for the device code to be approved; run `verglas login` again")]
    Timeout,
    #[error(transparent)]
    Provision(#[from] crate::connection_profile::ConnectionProfileError),
    #[error(transparent)]
    Credentials(#[from] crate::credentials::CredentialsError),
}

/// A cloud-API error that can be identified as a 401 so the call site can
/// fail loud with re-login guidance.
pub trait Unauthorized {
    fn is_unauthorized(&self) -> bool;
}

impl Unauthorized for verglas_sdk::ClientError {
    fn is_unauthorized(&self) -> bool {
        matches!(
            self,
            verglas_sdk::ClientError::Http { status, .. }
                if *status == reqwest::StatusCode::UNAUTHORIZED
        )
    }
}

impl Unauthorized for verglas_sdk::server::ServerError {
    fn is_unauthorized(&self) -> bool {
        matches!(self, verglas_sdk::server::ServerError::Api { status, .. } if *status == 401)
    }
}

impl Unauthorized for reqwest::Error {
    fn is_unauthorized(&self) -> bool {
        self.status() == Some(reqwest::StatusCode::UNAUTHORIZED)
    }
}

/// Runs the full WorkOS OAuth 2.0 Device Authorization Grant as a public
/// client: request a device code, print the user code and verification URI
/// (opening a browser to `verification_uri_complete` unless `no_browser`),
/// poll until the user finishes signing in, then spend the WorkOS access
/// token exactly once at `POST {access_endpoint}/v1/provision` and persist
/// the resulting connection profile and durable control-plane token. The
/// WorkOS access token and any refresh token are discarded the moment the
/// exchange completes; neither is ever written to disk.
pub async fn device_login(access_endpoint: &str, no_browser: bool) -> Result<(), AuthError> {
    let base = workos_api_base();
    let client_id = workos_client_id();
    let http = reqwest::Client::new();

    let authorize: DeviceAuthorizeResponse = {
        let response = http
            .post(format!("{base}/user_management/authorize/device"))
            .json(&serde_json::json!({ "client_id": client_id }))
            .send()
            .await
            .map_err(AuthError::Request)?;
        let status = response.status();
        if !status.is_success() {
            return Err(AuthError::Workos {
                status: status.as_u16(),
                body: response.text().await.unwrap_or_default(),
            });
        }
        response.json().await.map_err(AuthError::Request)?
    };

    println!("First copy this code: {}", authorize.user_code);
    println!(
        "Then open this URL to finish signing in: {}",
        authorize.verification_uri
    );
    if !no_browser {
        let _ = open_browser_url(&authorize.verification_uri_complete);
    }
    println!("Waiting for sign-in to complete...");

    let interval = Duration::from_secs(authorize.interval.unwrap_or(5).max(MIN_POLL_INTERVAL_SECS));
    let deadline = tokio::time::Instant::now() + DEVICE_FLOW_DEADLINE;
    let access_token = loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(AuthError::Timeout);
        }
        tokio::time::sleep(interval).await;
        let response = http
            .post(format!("{base}/user_management/authenticate"))
            .json(&serde_json::json!({
                "client_id": client_id,
                "device_code": authorize.device_code,
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            }))
            .send()
            .await
            .map_err(AuthError::Request)?;
        let status = response.status();
        if status.is_success() {
            let parsed: DeviceAuthenticateResponse =
                response.json().await.map_err(AuthError::Request)?;
            break parsed.access_token;
        }
        let body = response.text().await.unwrap_or_default();
        let pending = serde_json::from_str::<DeviceAuthenticateError>(&body)
            .map(|error| matches!(error.error.as_str(), "authorization_pending" | "slow_down"))
            .unwrap_or(false);
        if !pending {
            return Err(AuthError::Workos {
                status: status.as_u16(),
                body,
            });
        }
    };

    // The WorkOS access token is spent here, exactly once, and then dropped.
    crate::connection_profile::login_with_bearer(access_endpoint, &access_token).await?;
    Ok(())
}

/// Deletes the durable control-plane token, if one exists. Returns whether a
/// file was removed.
pub fn logout() -> Result<bool, AuthError> {
    let path = durable_token_path()?;
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(AuthError::Io { path, source }),
    }
}

/// Resolves the bearer for cloud-API calls: `VERGLAS_TOKEN` (automation)
/// wins, then the local scoped-token store keyed by the access endpoint,
/// then the durable control-plane token written by `verglas login`. Never
/// contacts WorkOS: WorkOS is spent once, at login, to mint this token.
pub async fn resolved_bearer(credentials_path: &Path) -> Result<Option<String>, AuthError> {
    if let Some(token) = std::env::var("VERGLAS_TOKEN")
        .ok()
        .filter(|token| !token.trim().is_empty())
    {
        return Ok(Some(token));
    }
    let endpoint = crate::cli::Cli::access_endpoint();
    if let Some(stored) = crate::credentials::load_token(credentials_path, &endpoint)? {
        return Ok(Some(stored.token));
    }
    load_durable_token()
}

/// Calls `attempt` once with the current (possibly absent) bearer. A 401
/// fails loud with guidance to run `verglas login` again — there is no
/// refresh loop; only re-running `verglas login` can replace a durable token
/// that the control plane has rejected. Used by call sites where a missing
/// bearer is not itself an error (self-hosted `workers`/`dashboard`).
pub async fn cloud_call<T, E, F, Fut>(
    bearer: Option<String>,
    mut attempt: F,
) -> Result<T, Box<dyn std::error::Error>>
where
    E: Unauthorized + std::error::Error + 'static,
    F: FnMut(Option<String>) -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    attempt(bearer).await.map_err(loud_unauthorized)
}

/// Wraps an unauthorized failure with re-login guidance; every other error
/// passes through with its own message unchanged.
fn loud_unauthorized<E: Unauthorized + std::error::Error + 'static>(
    error: E,
) -> Box<dyn std::error::Error> {
    if error.is_unauthorized() {
        format!("{error}; run `verglas login` to sign in again").into()
    } else {
        Box::new(error)
    }
}

/// Best-effort: if the durable control-plane token is at least
/// [`RENEWAL_AGE`] old, asks the control plane to rotate it and persists the
/// rotated value with the same owner-only permissions. Any failure —
/// no stored token, network error, non-2xx (including a control plane with
/// no renewal route at all), or an unparseable response — is silently
/// ignored. Renewal is a value-add on top of the durable token's own
/// server-side 30-day sliding window; it is never a precondition for the
/// loud-401 contract in [`cloud_call`] / [`cloud_call_with_bearer`].
pub async fn maybe_renew_durable_token() {
    let Ok(path) = durable_token_path() else {
        return;
    };
    let endpoint = crate::cli::Cli::access_endpoint();
    renew_if_stale(&path, &endpoint).await;
}

/// The renewal policy, parameterized on the token file path and access
/// endpoint so it can be exercised directly in tests without touching
/// process-wide environment state.
async fn renew_if_stale(path: &Path, endpoint: &str) {
    let Ok(metadata) = std::fs::metadata(path) else {
        return;
    };
    let Ok(modified) = metadata.modified() else {
        return;
    };
    let Ok(age) = SystemTime::now().duration_since(modified) else {
        return;
    };
    if age < RENEWAL_AGE {
        return;
    }
    let Ok(contents) = std::fs::read_to_string(path) else {
        return;
    };
    let token = contents.trim();
    if token.is_empty() {
        return;
    }
    let Ok(base) = reqwest::Url::parse(&format!("{}/", endpoint.trim_end_matches('/'))) else {
        return;
    };
    let Ok(url) = base.join("v1/tokens/renew") else {
        return;
    };
    let Ok(response) = reqwest::Client::new()
        .post(url)
        .bearer_auth(token)
        .send()
        .await
    else {
        return;
    };
    if !response.status().is_success() {
        return;
    }
    let Ok(renewed) = response.json::<RenewResponse>().await else {
        return;
    };
    let _ = crate::connection_profile::write_private(path, renewed.api_token.as_bytes());
}

/// The durable control-plane token file: `credentials/control-plane-token`
/// next to the shared connection profile.
fn durable_token_path() -> Result<PathBuf, AuthError> {
    Ok(crate::connection_profile::credentials_dir()?.join("control-plane-token"))
}

/// Reads the durable token file, treating a missing or blank file as "not
/// signed in" rather than an error.
fn load_durable_token() -> Result<Option<String>, AuthError> {
    let path = durable_token_path()?;
    read_token_file(&path)
}

/// Reads and trims one durable-token file at an explicit path, so both the
/// production lookup and unit tests share this logic without either one
/// needing to relocate the shared config root.
fn read_token_file(path: &Path) -> Result<Option<String>, AuthError> {
    match std::fs::read_to_string(path) {
        Ok(contents) => {
            let token = contents.trim();
            Ok((!token.is_empty()).then(|| token.to_owned()))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(AuthError::Io {
            path: path.to_owned(),
            source,
        }),
    }
}

fn workos_api_base() -> String {
    std::env::var("VERGLAS_WORKOS_API_BASE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_WORKOS_API_BASE.to_owned())
        .trim_end_matches('/')
        .to_owned()
}

fn workos_client_id() -> String {
    std::env::var("VERGLAS_WORKOS_CLIENT_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_CLIENT_ID.to_owned())
}

#[derive(Debug, Deserialize)]
struct DeviceAuthorizeResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    verification_uri_complete: String,
    #[serde(default)]
    interval: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct DeviceAuthenticateResponse {
    access_token: String,
}

#[derive(Debug, Deserialize)]
struct DeviceAuthenticateError {
    error: String,
}

/// The control plane's `POST /v1/tokens/renew` response. `expires_in` is
/// accepted (and ignored by serde's default field handling) since the
/// renewal cadence here is mtime-based, not expiry-based.
#[derive(Debug, Deserialize)]
struct RenewResponse {
    api_token: String,
}

/// Best-effort system browser launch, per-OS, with no dedicated crate: `open`
/// on macOS, `xdg-open` on Linux, `cmd /C start` on Windows. The URL is
/// always printed regardless, so a launch failure never strands the user.
fn open_browser_url(url: &str) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("open").arg(url).spawn()?;
    }
    #[cfg(target_os = "linux")]
    {
        std::process::Command::new("xdg-open").arg(url).spawn()?;
    }
    #[cfg(target_os = "windows")]
    {
        std::process::Command::new("cmd")
            .args(["/C", "start", "", url])
            .spawn()?;
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "no known system browser launcher for this platform",
        ));
    }
    #[allow(unreachable_code)]
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt as _;

    /// `read_token_file` treats a missing file as "not signed in", not an
    /// error, so `resolved_bearer` can fall through cleanly.
    #[test]
    fn read_token_file_returns_none_when_absent() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert_eq!(
            read_token_file(&dir.path().join("control-plane-token")).expect("no error"),
            None
        );
    }

    /// A freshly written durable token is not offered for renewal.
    #[tokio::test]
    async fn renew_if_stale_is_a_noop_for_a_fresh_token() {
        let dir = tempfile::tempdir().expect("tempdir");
        let token_path = dir.path().join("control-plane-token");
        fs::write(&token_path, "fresh-token").expect("seed token");
        fs::set_permissions(&token_path, fs::Permissions::from_mode(0o600)).expect("chmod");

        // No mock server is listening; a network call here would fail the
        // test with a connection error, proving no request was attempted.
        renew_if_stale(&token_path, "http://127.0.0.1:1").await;

        assert_eq!(
            fs::read_to_string(&token_path).expect("token unchanged"),
            "fresh-token",
            "a fresh token must not be rewritten"
        );
    }

    /// A durable token older than the renewal age is rotated from a
    /// successful `/v1/tokens/renew` response and persisted owner-only.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn renew_if_stale_rotates_a_stale_token_on_success() {
        let dir = tempfile::tempdir().expect("tempdir");
        let token_path = dir.path().join("control-plane-token");
        fs::write(&token_path, "stale-token").expect("seed token");
        fs::set_permissions(&token_path, fs::Permissions::from_mode(0o600)).expect("chmod");
        let stale = SystemTime::now() - Duration::from_secs(8 * 24 * 60 * 60);
        fs::File::open(&token_path)
            .expect("open for backdating")
            .set_modified(stale)
            .expect("backdate mtime");

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock");
        let address = listener.local_addr().expect("addr");
        let router = axum::Router::new().route(
            "/v1/tokens/renew",
            axum::routing::post(|| async {
                axum::Json(
                    serde_json::json!({ "api_token": "rotated-token", "expires_in": 2_592_000 }),
                )
            }),
        );
        tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve mock");
        });

        renew_if_stale(&token_path, &format!("http://{address}")).await;

        assert_eq!(
            fs::read_to_string(&token_path).expect("rotated token"),
            "rotated-token"
        );
        assert_eq!(
            fs::metadata(&token_path)
                .expect("metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600,
            "the rotated token file must stay owner-only"
        );
    }

    /// A control plane with no renewal route (a 404) must leave the stale
    /// token in place rather than failing the surrounding command.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn renew_if_stale_ignores_a_404_renewal_route() {
        let dir = tempfile::tempdir().expect("tempdir");
        let token_path = dir.path().join("control-plane-token");
        fs::write(&token_path, "stale-token").expect("seed token");
        let stale = SystemTime::now() - Duration::from_secs(30 * 24 * 60 * 60);
        fs::File::open(&token_path)
            .expect("open for backdating")
            .set_modified(stale)
            .expect("backdate mtime");

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock");
        let address = listener.local_addr().expect("addr");
        // No /v1/tokens/renew route registered: any request 404s.
        let router = axum::Router::new();
        tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve mock");
        });

        renew_if_stale(&token_path, &format!("http://{address}")).await;

        assert_eq!(
            fs::read_to_string(&token_path).expect("token unchanged"),
            "stale-token",
            "a 404 renewal response must be ignored, not clear the token"
        );
    }
}
