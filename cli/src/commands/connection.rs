//! Thin CLI adapters for the reusable shared connection-profile resolver.

use std::error::Error;

use crate::browser_login;
use crate::connection_profile::{self, ResolvedConnection};

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
    println!(
        "Verglas connection profile saved. Integrations can now run `verglas connection --json --include-secrets`."
    );
    Ok(())
}

pub fn connection(include_secrets: bool, json: bool) -> Result<(), Box<dyn Error>> {
    let mut profile =
        connection_profile::resolve_from_environment(&connection_profile::environment())?;
    if !include_secrets {
        profile.bearer_token = None;
        profile.access_key_id = None;
        profile.secret_access_key = None;
    }
    render(&profile, json)?;
    Ok(())
}

fn render(profile: &ResolvedConnection, json: bool) -> Result<(), Box<dyn Error>> {
    if json {
        println!("{}", serde_json::to_string(profile)?);
    } else {
        println!(
            "query: {}\nsemantic: {}\nregion: {}",
            profile.query_uri, profile.semantic_uri, profile.region
        );
    }
    Ok(())
}
