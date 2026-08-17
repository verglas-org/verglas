//! `AuthSession`: the single choke point for WorkOS device-flow login, token
//! persistence, expiry tracking, and refresh.
//!
//! Every cloud-API call site resolves its bearer through [`resolved_bearer`]
//! and retries a 401 through [`with_reauth`] / [`with_reauth_required`]
//! instead of scattering its own token logic. WorkOS tokens persist to
//! `workos-tokens.json` next to the shared connection profile (honoring
//! `VERGLAS_CONFIG` the same way `connection_profile` does), separately from
//! the scoped-token store in `credentials.rs`.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Verglas Cloud's production WorkOS public client id, compiled in so a
/// fresh install can `verglas login` with no configuration.
const DEFAULT_CLIENT_ID: &str = "client_01M089ZKFT1SFR9JTJRN1HQEF9";
const DEFAULT_WORKOS_API_BASE: &str = "https://api.workos.com";

/// Floor under a WorkOS-advertised poll interval so a misbehaving or mocked
/// authorization server cannot spin the poll loop.
const MIN_POLL_INTERVAL_SECS: u64 = 1;
/// Overall wall-clock budget for the device code to be approved, mirroring a
/// typical WorkOS device-code lifetime.
const DEVICE_FLOW_DEADLINE: Duration = Duration::from_secs(600);
/// Refresh this many seconds before the access token's JWT `exp` claim so a
/// proactive refresh always lands before the token actually expires.
const EXPIRY_SKEW_SECS: i64 = 30;

/// Failures in the WorkOS device-login, persistence, or refresh paths.
#[derive(Debug, Error)]
pub enum AuthError {
    #[error("cannot read {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("WorkOS token store {path} is not valid JSON: {source}")]
    Decode {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("could not encode WorkOS tokens: {0}")]
    Encode(serde_json::Error),
    #[error("WorkOS request failed: {0}")]
    Request(reqwest::Error),
    #[error("WorkOS returned HTTP {status}: {body}")]
    Workos { status: u16, body: String },
    #[error(
        "timed out waiting for the device code to be approved; run `verglas login` again"
    )]
    Timeout,
    #[error(transparent)]
    Provision(#[from] crate::connection_profile::ConnectionProfileError),
    #[error(transparent)]
    Credentials(#[from] crate::credentials::CredentialsError),
}

/// Tokens persisted after a successful WorkOS device login or refresh.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkosTokens {
    pub access_token: String,
    pub refresh_token: String,
    /// Unix-seconds expiry decoded from the access token's JWT `exp` claim,
    /// when the token is a parseable JWT. `None` disables proactive
    /// refresh for this token; the reactive 401 path still catches it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
}

/// A cloud-API error that can be resolved by presenting a fresh bearer.
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
/// poll until the user finishes signing in, then exchange the WorkOS access
/// token at `POST {access_endpoint}/v1/provision` and persist both the
/// connection profile and the WorkOS token pair.
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
    let tokens = loop {
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
            break WorkosTokens {
                expires_at: jwt_exp(&parsed.access_token),
                access_token: parsed.access_token,
                refresh_token: parsed.refresh_token,
            };
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

    crate::connection_profile::login_with_bearer(access_endpoint, &tokens.access_token).await?;
    store_tokens(&tokens)?;
    Ok(())
}

/// Deletes the WorkOS token store, if one exists. Returns whether a file was
/// removed.
pub fn logout() -> Result<bool, AuthError> {
    let path = tokens_path()?;
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(AuthError::Io { path, source }),
    }
}

/// Resolves the bearer for cloud-API calls: `VERGLAS_TOKEN` (automation)
/// wins, then the local scoped-token store keyed by the access endpoint,
/// then the WorkOS session — refreshed proactively when its decoded
/// expiry has passed.
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
    let Some(tokens) = load_tokens()? else {
        return Ok(None);
    };
    if is_expired(&tokens)
        && let Ok(rotated) = refresh(&tokens).await
    {
        let _ = store_tokens(&rotated);
        return Ok(Some(rotated.access_token));
    }
    Ok(Some(tokens.access_token))
}

/// Calls `attempt` with the current (possibly absent) bearer. On an
/// unauthorized failure with a stored WorkOS refresh token, refreshes,
/// persists the rotated pair, and retries `attempt` exactly once with the
/// new bearer. Self-hosted servers that run without a bearer at all simply
/// see their original error surface unchanged. Used by call sites where a
/// missing bearer is not itself an error (self-hosted `workers`/`dashboard`).
pub async fn with_reauth<T, E, F, Fut>(bearer: Option<String>, mut attempt: F) -> Result<T, E>
where
    E: Unauthorized,
    F: FnMut(Option<String>) -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    match attempt(bearer).await {
        Ok(value) => Ok(value),
        Err(error) if error.is_unauthorized() => match reauthorize().await {
            Some(new_bearer) => attempt(Some(new_bearer)).await,
            None => Err(error),
        },
        Err(error) => Err(error),
    }
}

/// Same retry contract as [`with_reauth`] for call sites that always require
/// a bearer (the standalone access service has no anonymous mode).
pub async fn with_reauth_required<T, E, F, Fut>(bearer: String, mut attempt: F) -> Result<T, E>
where
    E: Unauthorized,
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    match attempt(bearer).await {
        Ok(value) => Ok(value),
        Err(error) if error.is_unauthorized() => match reauthorize().await {
            Some(new_bearer) => attempt(new_bearer).await,
            None => Err(error),
        },
        Err(error) => Err(error),
    }
}

/// Refreshes the stored WorkOS session and persists the rotated pair,
/// returning the new access token on success.
async fn reauthorize() -> Option<String> {
    let tokens = load_tokens().ok().flatten()?;
    let rotated = refresh(&tokens).await.ok()?;
    store_tokens(&rotated).ok()?;
    Some(rotated.access_token)
}

/// Exchanges a stored refresh token for a rotated WorkOS access/refresh pair.
async fn refresh(tokens: &WorkosTokens) -> Result<WorkosTokens, AuthError> {
    let base = workos_api_base();
    let client_id = workos_client_id();
    let response = reqwest::Client::new()
        .post(format!("{base}/user_management/authenticate"))
        .json(&serde_json::json!({
            "client_id": client_id,
            "grant_type": "refresh_token",
            "refresh_token": tokens.refresh_token,
        }))
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
    let parsed: DeviceAuthenticateResponse = response.json().await.map_err(AuthError::Request)?;
    Ok(WorkosTokens {
        expires_at: jwt_exp(&parsed.access_token),
        access_token: parsed.access_token,
        refresh_token: parsed.refresh_token,
    })
}

/// Whether `tokens` has passed its decoded expiry (with a safety margin).
/// A token with no decodable expiry is never proactively refreshed; the
/// reactive 401 path in [`with_reauth`] still catches it.
fn is_expired(tokens: &WorkosTokens) -> bool {
    let Some(expires_at) = tokens.expires_at else {
        return false;
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0);
    now >= expires_at.saturating_sub(EXPIRY_SKEW_SECS)
}

/// The WorkOS token store: `credentials/workos-tokens.json` next to the
/// shared connection profile.
fn tokens_path() -> Result<PathBuf, AuthError> {
    Ok(crate::connection_profile::credentials_dir()?.join("workos-tokens.json"))
}

fn load_tokens() -> Result<Option<WorkosTokens>, AuthError> {
    let path = tokens_path()?;
    match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|source| AuthError::Decode { path, source }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(AuthError::Io { path, source }),
    }
}

fn store_tokens(tokens: &WorkosTokens) -> Result<(), AuthError> {
    let dir = crate::connection_profile::credentials_dir()?;
    std::fs::create_dir_all(&dir).map_err(|source| AuthError::Io {
        path: dir.clone(),
        source,
    })?;
    crate::connection_profile::private_directory(&dir)?;
    let encoded = serde_json::to_vec_pretty(tokens).map_err(AuthError::Encode)?;
    crate::connection_profile::write_private(&dir.join("workos-tokens.json"), &encoded)?;
    Ok(())
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
    refresh_token: String,
}

#[derive(Debug, Deserialize)]
struct DeviceAuthenticateError {
    error: String,
}

/// Decodes the unverified `exp` claim from a JWT's payload segment. Returns
/// `None` for anything that is not a three-segment, base64url-encoded JWT
/// with an integer `exp` — the caller treats that as "expiry unknown" and
/// relies on the reactive 401 path instead of failing the login.
fn jwt_exp(token: &str) -> Option<i64> {
    let payload = token.split('.').nth(1)?;
    let bytes = decode_base64url(payload)?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    value.get("exp")?.as_i64()
}

/// Minimal unpadded base64url decoder, just enough to read a JWT payload
/// segment without pulling in a dependency for one field.
fn decode_base64url(input: &str) -> Option<Vec<u8>> {
    fn sextet(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'-' => Some(62),
            b'_' => Some(63),
            _ => None,
        }
    }
    let mut out = Vec::with_capacity(input.len() * 3 / 4 + 3);
    let mut buffer: u32 = 0;
    let mut bits: u32 = 0;
    for byte in input.bytes().filter(|&byte| byte != b'=') {
        buffer = (buffer << 6) | sextet(byte)? as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
        }
    }
    Some(out)
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

    #[test]
    fn jwt_exp_decodes_the_exp_claim() {
        // {"sub":"user_1","exp":1735689600}
        let header = "eyJhbGciOiJIUzI1NiJ9";
        let payload = "eyJzdWIiOiJ1c2VyXzEiLCJleHAiOjE3MzU2ODk2MDB9";
        let token = format!("{header}.{payload}.signature");
        assert_eq!(jwt_exp(&token), Some(1_735_689_600));
    }

    #[test]
    fn jwt_exp_returns_none_for_a_non_jwt_token() {
        assert_eq!(jwt_exp("workos-access-token-1"), None);
    }

    #[test]
    fn is_expired_treats_an_unknown_expiry_as_not_expired() {
        let tokens = WorkosTokens {
            access_token: "a".to_owned(),
            refresh_token: "r".to_owned(),
            expires_at: None,
        };
        assert!(!is_expired(&tokens));
    }

    #[test]
    fn is_expired_honors_the_safety_margin() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_secs() as i64;
        let almost_expired = WorkosTokens {
            access_token: "a".to_owned(),
            refresh_token: "r".to_owned(),
            expires_at: Some(now + EXPIRY_SKEW_SECS - 1),
        };
        assert!(is_expired(&almost_expired));
        let plenty_of_time = WorkosTokens {
            access_token: "a".to_owned(),
            refresh_token: "r".to_owned(),
            expires_at: Some(now + EXPIRY_SKEW_SECS + 3600),
        };
        assert!(!is_expired(&plenty_of_time));
    }
}
