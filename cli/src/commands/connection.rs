//! Thin CLI adapters for the reusable shared connection-profile resolver.

use std::error::Error;

use crate::browser_login;
use crate::connection_profile;

/// Runs `verglas login`. With `--api-key`, exchanges it directly. Otherwise
/// runs the browser flow: listen on a loopback callback, hand the user to the
/// dashboard authorize page, and exchange the one-time code it returns.
pub async fn login(
    url: &str,
    api_key: Option<&str>,
    dashboard_url: &str,
    no_browser: bool,
) -> Result<(), Box<dyn Error>> {
    match api_key.filter(|value| !value.trim().is_empty()) {
        Some(api_key) => connection_profile::login(url, api_key).await?,
        None => {
            let code = browser_login::await_authorization_code(dashboard_url, !no_browser).await?;
            connection_profile::login_with_code(url, &code).await?;
        }
    }
    println!("Signed in. Connection profile saved.");
    Ok(())
}

/// Runs `verglas logout`: removes the stored connection profile and its
/// credential files, leaving the rest of the config untouched.
pub fn logout() -> Result<(), Box<dyn Error>> {
    if connection_profile::logout()? {
        println!("Logged out: connection profile removed.");
    } else {
        println!("No connection profile is stored.");
    }
    Ok(())
}
