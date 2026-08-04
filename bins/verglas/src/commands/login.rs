//! `verglas login`: authenticate the CLI to the control plane and configure the
//! local server for the tenant's lakehouse.
//!
//! Three modes, one result. The default browser flow opens a browser to an
//! authorization-code + PKCE(S256) grant and completes over a loopback redirect.
//! `--device` runs the headless device-code flow (authorize from a printed code
//! on any device). `--api-key` verifies a long-lived key directly — the CI and
//! automation path. All three end at the same provisioning: they obtain the
//! tenant's long-lived api key and SCOPED lakehouse config and, on success, write:
//!   * the api key to `~/.verglas/credentials/control-plane-token` (mode 0600),
//!   * the server config `~/.verglas/config.toml` — `[control_plane]` (url),
//!     `[backend]` (S3 endpoint/bucket/region + credentials_file), and
//!     `[catalog]` (uri/warehouse + credentials_file),
//!   * the tenant's scoped S3 key pair to a 0600 AWS-INI backend credentials
//!     file, and the scoped catalog bearer token to a 0600 catalog file.
//!
//! Every write happens only after authorization and provisioning succeed, so a
//! rejected login leaves no trace. Re-running overwrites in place, so a re-login
//! is idempotent. After login the server is fully configured for the tenant with
//! credentials scoped to ONLY its bucket — no account-wide creds.
//!
//! Secret values (the OAuth access token, the api key, the S3 secret key, the
//! catalog token) are never printed; the on-disk secrets live only in mode-0600
//! files.

use std::error::Error;
use std::io::BufRead;
use std::net::TcpListener;
use std::path::Path;
use std::time::Duration;

use base64::Engine as _;
use sha2::{Digest as _, Sha256};

use crate::cli::LoginArgs;
use crate::controlplane::{
    self, ControlPlaneClient, ControlPlaneError, DeviceCodeResponse, Lakehouse,
};

/// The environment variable that suppresses opening a real browser. It exists so
/// tests (which cannot click a browser) can drive the loopback callback
/// themselves. Its ONLY effect is to skip the OS `open` call — the authorize URL
/// is printed in every case, so production behavior is otherwise unchanged.
const NO_BROWSER_ENV: &str = "VERGLAS_LOGIN_NO_BROWSER";

/// Runs `verglas login`. Resolves the URL, dispatches to the requested mode
/// (browser by default, `--device`, or `--api-key`), and on success writes the
/// token / server config / credential files and prints a non-secret summary.
pub async fn run(args: LoginArgs) -> Result<(), Box<dyn Error>> {
    let url = resolve_url(args.url)?;
    if args.api_key_mode {
        return run_api_key(&url, args.api_key).await;
    }
    // The positional key only means anything in `--api-key` mode; refuse it in a
    // browser/device login rather than silently ignoring a supplied key.
    if args.api_key.is_some() {
        return Err(
            "the positional API key is only used with --api-key; run `verglas login --api-key <KEY>`"
                .into(),
        );
    }
    if args.device {
        run_device(&url).await
    } else {
        run_browser(&url).await
    }
}

/// The api-key flow: verify a long-lived key against `GET /v1/me`, fetch the
/// scoped lakehouse config, then persist everything. A rejected key writes
/// nothing. This is the automation path, not a fallback.
async fn run_api_key(url: &str, arg: Option<String>) -> Result<(), Box<dyn Error>> {
    let api_key = resolve_api_key(arg)?;
    let client = ControlPlaneClient::new(url, &api_key)?;
    let account = match client.me().await {
        Ok(account) => account,
        Err(ControlPlaneError::Unauthorized) => {
            return Err("the control plane rejected that API key; nothing was saved".into());
        }
        Err(other) => return Err(other.into()),
    };
    // A tenant on the shared-bucket model may have NO per-tenant lakehouse row —
    // that is normal, not a failed login. Persist the key either way.
    let lakehouse = match client.lakehouse().await {
        Ok(lakehouse) => Some(lakehouse),
        Err(ControlPlaneError::Unauthorized) => {
            return Err("the control plane rejected that API key; nothing was saved".into());
        }
        Err(ControlPlaneError::NotFound) => None,
        Err(other) => return Err(other.into()),
    };
    // Verification done; nothing above wrote to disk. Now persist everything.
    match &lakehouse {
        Some(lake) => {
            persist(url, &api_key, lake)?;
            print_summary(&account.account_email, &account.tenant_id, lake)?;
        }
        None => {
            persist_key_only(url, &api_key)?;
            println!(
                "logged in as {} (tenant {}); no per-tenant lakehouse config (shared-bucket tenant)",
                account.account_email, account.tenant_id
            );
        }
    }
    Ok(())
}

/// The browser flow: authorization-code grant with PKCE(S256). Generates the
/// verifier/challenge and a state, binds a loopback listener, opens the browser
/// to the authorize URL, receives the redirect, exchanges the code for an access
/// token, provisions, then persists. Nothing is written until provision succeeds.
async fn run_browser(url: &str) -> Result<(), Box<dyn Error>> {
    let client = ControlPlaneClient::anonymous(url)?;

    let verifier = pkce_verifier()?;
    let challenge = pkce_challenge(&verifier);
    let state = random_token::<16>()?;

    // Bind the loopback first so the redirect URI carries the real port.
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|e| ControlPlaneError::Callback(format!("could not bind loopback: {e}")))?;
    let port = listener
        .local_addr()
        .map_err(|e| ControlPlaneError::Callback(format!("could not read loopback port: {e}")))?
        .port();
    let redirect_uri = format!("http://127.0.0.1:{port}/callback");
    let authorize = client.authorize_url(&redirect_uri, &challenge, &state)?;

    // Print the loopback URL (so a harness can discover the port) and the
    // authorize URL (always, as a paste-able fallback) before we block on the
    // redirect. Flush so a piped reader sees them immediately.
    println!("listening on {redirect_uri}");
    println!("Open this URL in your browser to authorize verglas:");
    println!("  {authorize}");
    flush_stdout();

    if std::env::var_os(NO_BROWSER_ENV).is_none() {
        open_browser(&authorize);
    }

    let (code, returned_state) = accept_callback(listener).await?;
    if returned_state != state {
        return Err(ControlPlaneError::StateMismatch.into());
    }

    let token = client
        .exchange_code(&code, &verifier, &redirect_uri)
        .await?;
    let provisioned = provision(&client, &token.access_token).await?;
    persist(url, &provisioned.api_key, &provisioned.lakehouse)?;
    print_summary(
        &provisioned.account_email,
        &provisioned.tenant_id,
        &provisioned.lakehouse,
    )?;
    Ok(())
}

/// The device flow: start a device authorization, print the user code and
/// verification URIs for the human to open on any device, poll the token endpoint
/// at the server's interval (backing off on `slow_down`), provision, then persist.
async fn run_device(url: &str) -> Result<(), Box<dyn Error>> {
    let client = ControlPlaneClient::anonymous(url)?;
    let device = client.device_code().await?;

    println!("To authorize verglas, open this URL on any device:");
    println!("  {}", device.verification_uri);
    println!("and enter the code:  {}", device.user_code);
    if let Some(complete) = &device.verification_uri_complete {
        println!("Or open this URL to jump straight there:");
        println!("  {complete}");
    }
    println!("Waiting for you to authorize...");
    flush_stdout();

    let token = poll_device(&client, &device).await?;
    let provisioned = provision(&client, &token.access_token).await?;
    persist(url, &provisioned.api_key, &provisioned.lakehouse)?;
    print_summary(
        &provisioned.account_email,
        &provisioned.tenant_id,
        &provisioned.lakehouse,
    )?;
    Ok(())
}

/// Polls the token endpoint until the user authorizes. Keeps polling on
/// `authorization_pending`, adds 5 seconds to the interval on `slow_down`, and
/// fails with a clear message on denial or expiry. Honors the server's interval.
async fn poll_device(
    client: &ControlPlaneClient,
    device: &DeviceCodeResponse,
) -> Result<controlplane::TokenResponse, Box<dyn Error>> {
    let mut interval = device.interval.max(1);
    loop {
        match client.poll_device_token(&device.device_code).await {
            Ok(token) => return Ok(token),
            Err(ControlPlaneError::AuthorizationPending) => {}
            Err(ControlPlaneError::SlowDown) => interval += 5,
            Err(ControlPlaneError::AccessDenied) => {
                return Err("authorization was denied; nothing was saved".into());
            }
            Err(ControlPlaneError::Expired) => {
                return Err(
                    "the code expired before you authorized; run `verglas login --device` again"
                        .into(),
                );
            }
            Err(other) => return Err(other.into()),
        }
        tokio::time::sleep(Duration::from_secs(interval)).await;
    }
}

/// Provisions with a freshly obtained access token, mapping a 401 to a clear
/// "authorization expired" message. The bearer is the access token, not any
/// stored key.
async fn provision(
    client: &ControlPlaneClient,
    access_token: &str,
) -> Result<controlplane::ProvisionResponse, Box<dyn Error>> {
    match client.provision(access_token).await {
        Ok(response) => Ok(response),
        Err(ControlPlaneError::Unauthorized) => {
            Err("authorization expired before provisioning; run `verglas login` again".into())
        }
        Err(other) => Err(other.into()),
    }
}

/// Writes everything a successful login persists: the api key to the 0600 token
/// file, the scoped S3 key pair and catalog token to their 0600 credential files,
/// and the server config sections. Shared by all three flows. Nothing here runs
/// until authorization and provisioning have succeeded.
/// Persist just the key + control-plane URL for a tenant with no per-tenant
/// lakehouse row (the shared-bucket model): cloud resource groups work; the
/// server's backend/catalog files are left untouched.
fn persist_key_only(url: &str, api_key: &str) -> Result<(), Box<dyn Error>> {
    write_token(api_key)?;
    let path = controlplane::config_path()?;
    let existing = std::fs::read_to_string(&path).unwrap_or_default();
    let merged = replace_or_append_section(&existing, "control_plane", &control_plane_block(url));
    write_private(&path, &merged)?;
    Ok(())
}

fn persist(url: &str, api_key: &str, lakehouse: &Lakehouse) -> Result<(), Box<dyn Error>> {
    write_token(api_key)?;
    let backend_creds = controlplane::backend_credentials_path()?;
    let catalog_creds = controlplane::catalog_credentials_path()?;
    write_backend_credentials(&backend_creds, lakehouse)?;
    write_catalog_credentials(&catalog_creds, lakehouse)?;
    write_server_config(url, lakehouse, &backend_creds, &catalog_creds)?;
    Ok(())
}

/// Prints the non-secret login summary: the account/tenant, the configured
/// bucket/warehouse/endpoint/catalog, the 0600 files written, and the next steps.
/// No secret value is printed.
fn print_summary(email: &str, tenant: &str, lake: &Lakehouse) -> Result<(), Box<dyn Error>> {
    let token = controlplane::token_path()?;
    let backend_creds = controlplane::backend_credentials_path()?;
    let catalog_creds = controlplane::catalog_credentials_path()?;
    println!("logged in as {email} (tenant {tenant})");
    println!(
        "configured the server for bucket {} (warehouse {})",
        lake.bucket, lake.warehouse
    );
    println!(
        "  S3 endpoint:  {} (region {})",
        lake.s3_endpoint, lake.s3_region
    );
    println!("  catalog URI:  {}", lake.catalog_uri);
    println!("  control-plane token:  {} (0600)", token.display());
    println!("  backend credentials:  {} (0600)", backend_creds.display());
    println!("  catalog credentials:  {} (0600)", catalog_creds.display());
    println!(
        "next: use cloud verbs (`verglas workers`, …), or point a self-hosted server with VERGLAS_ENDPOINT"
    );
    Ok(())
}

/// A PKCE code verifier: 32 secure-random bytes, base64url without padding.
fn pkce_verifier() -> Result<String, Box<dyn Error>> {
    random_token::<32>()
}

/// The PKCE code challenge for `verifier`: base64url-no-pad of its SHA-256 digest.
fn pkce_challenge(verifier: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(verifier.as_bytes());
    base64_url(hasher.finalize().as_slice())
}

/// `N` secure-random bytes as a base64url-no-pad string. Used for the PKCE
/// verifier and the OAuth `state`.
fn random_token<const N: usize>() -> Result<String, Box<dyn Error>> {
    let mut bytes = [0u8; N];
    getrandom::getrandom(&mut bytes)
        .map_err(|e| format!("could not read secure random bytes: {e}"))?;
    Ok(base64_url(&bytes))
}

/// Encodes bytes as base64url without padding — the encoding OAuth PKCE uses.
fn base64_url(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Flushes stdout so a piped reader (or the user) sees the prompt before the CLI
/// blocks waiting for authorization. A failed flush is not worth aborting over.
fn flush_stdout() {
    use std::io::Write as _;
    let _ = std::io::stdout().flush();
}

/// Opens `url` in the user's browser via the OS opener, best-effort. A failure is
/// silent because the authorize URL was already printed as a fallback. No browser
/// crate is used — just the platform's `open`/`xdg-open`/`start`.
fn open_browser(url: &str) {
    let opener: Option<(&str, Vec<&str>)> = if cfg!(target_os = "macos") {
        Some(("open", vec![url]))
    } else if cfg!(target_os = "linux") {
        Some(("xdg-open", vec![url]))
    } else if cfg!(target_os = "windows") {
        Some(("cmd", vec!["/C", "start", "", url]))
    } else {
        None
    };
    if let Some((program, args)) = opener {
        let _ = std::process::Command::new(program)
            .args(args)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();
    }
}

/// Accepts exactly one connection on the loopback listener, parses `code` and
/// `state` from the `/callback?...` request line, and replies with a small
/// success page. Runs the blocking socket work off the async runtime.
async fn accept_callback(listener: TcpListener) -> Result<(String, String), ControlPlaneError> {
    tokio::task::spawn_blocking(move || accept_callback_blocking(listener))
        .await
        .map_err(|e| ControlPlaneError::Callback(format!("callback task failed: {e}")))?
}

/// The blocking half of [`accept_callback`]: reads the HTTP request line from the
/// one accepted connection, extracts the query, writes a 200 HTML reply, and
/// returns the `code` and `state`.
fn accept_callback_blocking(listener: TcpListener) -> Result<(String, String), ControlPlaneError> {
    use std::io::Write as _;
    let (mut stream, _addr) = listener
        .accept()
        .map_err(|e| ControlPlaneError::Callback(format!("accept failed: {e}")))?;
    let mut reader = std::io::BufReader::new(
        stream
            .try_clone()
            .map_err(|e| ControlPlaneError::Callback(format!("clone stream failed: {e}")))?,
    );
    let mut request_line = String::new();
    reader
        .read_line(&mut request_line)
        .map_err(|e| ControlPlaneError::Callback(format!("read request failed: {e}")))?;
    // The request line is `GET /callback?code=...&state=... HTTP/1.1`.
    let target = request_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| ControlPlaneError::Callback("malformed request line".to_owned()))?;
    let parsed = parse_callback_query(target);

    let body = "<!doctype html><meta charset=\"utf-8\"><title>Verglas CLI</title>\
        <p>Verglas CLI: you're signed in — you can close this tab.</p>";
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();

    parsed
}

/// Parses `code` and `state` out of a `/callback?...` request target, decoding
/// percent-escapes. Surfaces an `error` query parameter as an OAuth error.
fn parse_callback_query(target: &str) -> Result<(String, String), ControlPlaneError> {
    let parsed = reqwest::Url::parse(&format!("http://127.0.0.1{target}"))
        .map_err(|_| ControlPlaneError::Callback(format!("could not parse callback `{target}`")))?;
    let mut code = None;
    let mut state = None;
    for (key, value) in parsed.query_pairs() {
        match key.as_ref() {
            "code" => code = Some(value.into_owned()),
            "state" => state = Some(value.into_owned()),
            "error" => return Err(ControlPlaneError::Oauth(value.into_owned())),
            _ => {}
        }
    }
    let code =
        code.ok_or_else(|| ControlPlaneError::Callback("callback missing code".to_owned()))?;
    let state =
        state.ok_or_else(|| ControlPlaneError::Callback("callback missing state".to_owned()))?;
    Ok((code, state))
}

/// Writes the tenant's scoped S3 key pair to `path` as an AWS-INI credentials
/// file (mode 0600), the format `[backend].credentials_file` reads. Creates the
/// credentials directory as needed.
fn write_backend_credentials(path: &Path, lake: &Lakehouse) -> Result<(), Box<dyn Error>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body = format!(
        "[default]\naws_access_key_id = {}\naws_secret_access_key = {}\n",
        lake.access_key_id, lake.secret_access_key
    );
    write_private(path, &body)?;
    Ok(())
}

/// Writes the tenant's scoped catalog bearer token to `path` (mode 0600), the
/// file `[catalog].credentials_file` reads in bearer mode. Creates the
/// credentials directory as needed.
fn write_catalog_credentials(path: &Path, lake: &Lakehouse) -> Result<(), Box<dyn Error>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    write_private(path, &lake.catalog_token)?;
    Ok(())
}

/// Resolves the control plane URL: `--url` if given, else a prior login's URL,
/// else Verglas Cloud ([`controlplane::DEFAULT_CONTROL_PLANE_URL`]).
fn resolve_url(flag: Option<String>) -> Result<String, Box<dyn Error>> {
    if let Some(url) = flag {
        let url = url.trim().to_owned();
        if !url.is_empty() {
            return Ok(url);
        }
    }
    if let Ok(Some(url)) = controlplane::stored_url() {
        return Ok(url);
    }
    Ok(controlplane::DEFAULT_CONTROL_PLANE_URL.to_owned())
}

/// Resolves the API key: the positional argument if given, else one line from
/// stdin (so the key is not recorded in shell history).
fn resolve_api_key(arg: Option<String>) -> Result<String, Box<dyn Error>> {
    if let Some(key) = arg {
        let key = key.trim().to_owned();
        if !key.is_empty() {
            return Ok(key);
        }
    }
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line)?;
    let key = line.trim().to_owned();
    if key.is_empty() {
        return Err("no API key provided (pass it as an argument or on stdin)".into());
    }
    Ok(key)
}

/// Writes the API key to the token file at mode 0600, creating the credentials
/// directory as needed.
fn write_token(api_key: &str) -> Result<(), Box<dyn Error>> {
    let path = controlplane::token_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    write_private(&path, api_key)?;
    Ok(())
}

/// Writes `contents` to `path` at mode 0600, so the key is never briefly
/// world-readable and a re-login re-tightens the mode even if the file existed.
fn write_private(path: &Path, contents: &str) -> Result<(), Box<dyn Error>> {
    #[cfg(unix)]
    {
        use std::io::Write as _;
        use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(contents.as_bytes())?;
        // `mode()` only applies to a freshly created file; set it explicitly so
        // an overwrite of an existing token is 0600 too.
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, contents.as_bytes())?;
    }
    Ok(())
}

/// Writes the server config: merges `[control_plane]`, `[backend]`, and
/// `[catalog]` into `~/.verglas/config.toml`, each pointing the server at the
/// tenant's scoped lakehouse. Every other section, comment, and byte of an
/// existing config survives; re-running replaces these three in place.
fn write_server_config(
    url: &str,
    lake: &Lakehouse,
    backend_creds: &Path,
    catalog_creds: &Path,
) -> Result<(), Box<dyn Error>> {
    let path = controlplane::config_path()?;
    let existing = std::fs::read_to_string(&path).unwrap_or_default();
    let with_cp = replace_or_append_section(&existing, "control_plane", &control_plane_block(url));
    let with_backend =
        replace_or_append_section(&with_cp, "backend", &backend_block(lake, backend_creds));
    let merged = replace_or_append_section(
        &with_backend,
        "catalog",
        &catalog_block(lake, catalog_creds),
    );
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&path, merged)?;
    Ok(())
}

/// A `key = "value"` TOML line with the value properly quoted/escaped.
fn toml_line(key: &str, value: &str) -> String {
    format!("{key} = {}\n", toml::Value::String(value.to_owned()))
}

/// The `[control_plane]` section body carrying the base URL.
fn control_plane_block(url: &str) -> String {
    format!("[control_plane]\n{}", toml_line("url", url))
}

/// The `[backend]` section body pointing the server at the tenant's bucket over
/// its scoped S3 credentials file.
fn backend_block(lake: &Lakehouse, creds: &Path) -> String {
    let mut block = String::from("[backend]\n");
    block.push_str(&toml_line("endpoint", &lake.s3_endpoint));
    block.push_str(&toml_line("bucket", &lake.bucket));
    block.push_str(&toml_line("region", &lake.s3_region));
    block.push_str(&toml_line("credentials_file", &creds.to_string_lossy()));
    block
}

/// The `[catalog]` section body pointing the server at the tenant's Iceberg
/// catalog over its scoped bearer-token file.
fn catalog_block(lake: &Lakehouse, creds: &Path) -> String {
    let mut block = String::from("[catalog]\n");
    block.push_str(&toml_line("uri", &lake.catalog_uri));
    block.push_str(&toml_line("warehouse", &lake.warehouse));
    block.push_str(&toml_line("credentials_file", &creds.to_string_lossy()));
    block
}

/// Replaces an existing `[header]` section (its body up to the next `[section]`
/// header) with `block`, or appends `block` when the section is absent. `block`
/// is the full section text starting with `[header]`. A pure text operation, so
/// every other byte of the user's config — other sections, comments, formatting
/// — survives.
fn replace_or_append_section(existing: &str, header: &str, block: &str) -> String {
    let block = block.trim_end();
    let bracket = format!("[{header}]");
    let mut out = String::new();
    let mut in_section = false;
    let mut replaced = false;
    for line in existing.lines() {
        let stripped = line.trim();
        if stripped.starts_with('[') {
            if stripped == bracket {
                in_section = true;
                replaced = true;
                out.push_str(block);
                out.push('\n');
                continue;
            }
            in_section = false;
        }
        if in_section {
            continue; // the old section body is replaced wholesale
        }
        out.push_str(line);
        out.push('\n');
    }
    if !replaced {
        if !out.is_empty() && !out.ends_with("\n\n") {
            out.push('\n');
        }
        out.push_str(block);
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// With no flag and no stored URL, login targets Verglas Cloud.
    #[test]
    fn resolve_url_defaults_to_verglas_cloud() {
        let dir = tempfile::tempdir().expect("tempdir");
        let prev_home = std::env::var_os("HOME");
        let prev_config = std::env::var_os("VERGLAS_CONFIG");
        // SAFETY: unit test, no other threads in this test.
        unsafe {
            std::env::set_var("HOME", dir.path());
            std::env::remove_var("VERGLAS_CONFIG");
        }
        let url = resolve_url(None).expect("default URL");
        assert_eq!(url, controlplane::DEFAULT_CONTROL_PLANE_URL);
        unsafe {
            match prev_home {
                Some(v) => std::env::set_var("HOME", v),
                None => std::env::remove_var("HOME"),
            }
            match prev_config {
                Some(v) => std::env::set_var("VERGLAS_CONFIG", v),
                None => std::env::remove_var("VERGLAS_CONFIG"),
            }
        }
    }

    /// An explicit `--url` wins over the cloud default.
    #[test]
    fn resolve_url_flag_wins() {
        let url = resolve_url(Some("https://cp.example.test".into())).expect("flag");
        assert_eq!(url, "https://cp.example.test");
    }

    /// Merging a section touches only that section: other sections and their
    /// comments survive byte-for-byte, a re-merge with the same body is a no-op,
    /// and an existing section is replaced in place.
    #[test]
    fn section_merge_is_additive_and_idempotent() {
        let existing = "# my server config\n\
                        [listen]\ns3_port = 8333\n\n\
                        [cache]\n# precious comment\ndir = \"/tmp/c\"\n";

        let merged = replace_or_append_section(
            existing,
            "control_plane",
            &control_plane_block("https://api.verglas.dev"),
        );
        assert!(merged.starts_with(existing), "other sections untouched");
        assert!(merged.contains("[control_plane]\nurl = \"https://api.verglas.dev\""));

        let again = replace_or_append_section(
            &merged,
            "control_plane",
            &control_plane_block("https://api.verglas.dev"),
        );
        assert_eq!(again, merged, "same body, identical output");
    }

    /// A new body replaces the old section rather than appending a second one.
    #[test]
    fn section_merge_replaces_in_place() {
        let first = replace_or_append_section(
            "",
            "control_plane",
            &control_plane_block("https://one.example"),
        );
        let second = replace_or_append_section(
            &first,
            "control_plane",
            &control_plane_block("https://two.example"),
        );
        assert_eq!(second.matches("[control_plane]").count(), 1, "one section");
        assert!(second.contains("https://two.example"));
        assert!(!second.contains("https://one.example"));
    }

    /// The `[backend]` and `[catalog]` blocks carry exactly the tenant's scoped
    /// coordinates and point at the credential files — and never inline a secret.
    #[test]
    fn backend_and_catalog_blocks_carry_scoped_config_without_secrets() {
        let lake = Lakehouse {
            bucket: "verglas-personal".to_owned(),
            s3_endpoint: "https://storage.example.com".to_owned(),
            s3_region: "auto".to_owned(),
            warehouse: "acct_verglas-personal".to_owned(),
            catalog_uri: "https://catalog.example.com/acct/verglas-personal".to_owned(),
            access_key_id: "AKID".to_owned(),
            secret_access_key: "SUPERSECRET".to_owned(),
            catalog_token: "BEARERSECRET".to_owned(),
        };
        let backend = backend_block(
            &lake,
            Path::new("/home/u/.verglas/credentials/backend.credentials"),
        );
        assert!(backend.contains("bucket = \"verglas-personal\""));
        assert!(backend.contains("endpoint = \"https://storage.example.com\""));
        assert!(backend.contains("region = \"auto\""));
        assert!(
            backend.contains(
                "credentials_file = \"/home/u/.verglas/credentials/backend.credentials\""
            )
        );
        // The S3 secret is never inlined into the config — it lives in the file.
        assert!(!backend.contains("SUPERSECRET"));

        let catalog = catalog_block(
            &lake,
            Path::new("/home/u/.verglas/credentials/catalog.token"),
        );
        assert!(catalog.contains("uri = \"https://catalog.example.com/acct/verglas-personal\""));
        assert!(catalog.contains("warehouse = \"acct_verglas-personal\""));
        assert!(
            catalog.contains("credentials_file = \"/home/u/.verglas/credentials/catalog.token\"")
        );
        assert!(!catalog.contains("BEARERSECRET"));
    }
}
