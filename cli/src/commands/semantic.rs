//! Shared client and credential resolution for `verglas graph`/`verglas vector`.
//!
//! Both command families wrap the pre-signed SDK clients in
//! `verglas_sdk::semantic`; this module owns only environment-credential
//! resolution and stdin-or-file input reading. It performs no request
//! signing or request shaping itself — that stays entirely inside the SDK.

use std::error::Error;
use std::io::Read;

use verglas_sdk::{S3VectorsClient, SigV4Credentials, VerglasGraphsClient};

/// Builds a signed Graph client for `s3_endpoint` from environment credentials.
pub fn graph_client(s3_endpoint: &str) -> Result<VerglasGraphsClient, Box<dyn Error>> {
    Ok(VerglasGraphsClient::new(
        s3_endpoint,
        resolve_credentials()?,
    ))
}

/// Builds a signed S3 Vectors client for `s3_endpoint` from environment credentials.
pub fn vector_client(s3_endpoint: &str) -> Result<S3VectorsClient, Box<dyn Error>> {
    Ok(S3VectorsClient::new(s3_endpoint, resolve_credentials()?))
}

/// Reads `VERGLAS_S3_ACCESS_KEY_ID`/`VERGLAS_S3_SECRET_ACCESS_KEY`/`VERGLAS_S3_REGION`
/// (region defaults to `auto`). These are read from the environment only,
/// never accepted as CLI flags, so they cannot land in shell history or a
/// process listing.
fn resolve_credentials() -> Result<SigV4Credentials, Box<dyn Error>> {
    let access_key_id = nonempty_env("VERGLAS_S3_ACCESS_KEY_ID")
        .ok_or("VERGLAS_S3_ACCESS_KEY_ID must be set to call the S3 semantic listener")?;
    let secret_access_key = nonempty_env("VERGLAS_S3_SECRET_ACCESS_KEY")
        .ok_or("VERGLAS_S3_SECRET_ACCESS_KEY must be set to call the S3 semantic listener")?;
    let region = nonempty_env("VERGLAS_S3_REGION").unwrap_or_else(|| "auto".to_owned());
    Ok(SigV4Credentials::new(access_key_id, secret_access_key).with_region(region))
}

/// Reads a non-empty, trimmed environment variable.
fn nonempty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

/// Reads a JSON array body from `source`: `-` reads stdin, anything else is
/// treated as a file path.
pub fn read_json_input(source: &str) -> Result<String, Box<dyn Error>> {
    if source == "-" {
        let mut buffer = String::new();
        std::io::stdin().read_to_string(&mut buffer)?;
        Ok(buffer)
    } else {
        Ok(std::fs::read_to_string(source)?)
    }
}

#[cfg(test)]
mod tests {
    use super::read_json_input;

    #[test]
    fn reads_json_input_from_a_file_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nodes.json");
        std::fs::write(&path, "[]").expect("write");
        let text = read_json_input(path.to_str().expect("utf8 path")).expect("read");
        assert_eq!(text, "[]");
    }

    #[test]
    fn missing_file_input_is_a_readable_error() {
        let error = read_json_input("/nonexistent/path/nodes.json")
            .expect_err("missing file must be a readable error");
        assert!(!error.to_string().is_empty());
    }
}
