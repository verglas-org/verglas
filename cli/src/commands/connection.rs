//! Thin CLI adapters for `verglas login`/`logout`.

use std::error::Error;

use crate::auth;
use crate::connection_profile;

/// Runs `verglas login`. With `--api-key`, exchanges it directly. Otherwise
/// runs the WorkOS OAuth 2.0 Device Authorization Grant: print the user code
/// and verification URI, poll until sign-in completes, then exchange the
/// WorkOS access token for a connection profile.
pub async fn login(url: &str, api_key: Option<&str>, no_browser: bool) -> Result<(), Box<dyn Error>> {
    match api_key.filter(|value| !value.trim().is_empty()) {
        Some(api_key) => connection_profile::login(url, api_key).await?,
        None => auth::device_login(url, no_browser).await?,
    }
    println!("Signed in. Connection profile saved.");
    Ok(())
}

/// Runs `verglas logout`: removes the stored connection profile, its
/// credential files, and the WorkOS token store, leaving the rest of the
/// config untouched.
pub fn logout() -> Result<(), Box<dyn Error>> {
    let profile_removed = connection_profile::logout()?;
    let tokens_removed = auth::logout()?;
    if profile_removed || tokens_removed {
        println!("Logged out: connection profile removed.");
    } else {
        println!("No connection profile is stored.");
    }
    Ok(())
}
