//! The `~/.verglas/credentials/` scaffolds for `verglas init`.
//!
//! `verglas init` generates the annotated `config.toml` from the daemon's own
//! schema (`verglas_core::config_template::render`), so no config template lives
//! here. This module renders the credentials-file scaffolds the operator and the
//! daemon read: the backend credentials template (blank keys the operator fills
//! in) and the generated endpoint keypair file.
//!
//! Secrets never go in `config.toml`. Backend credentials and the endpoint
//! keypair live under `~/.verglas/credentials/` in the standard AWS
//! credentials-file format, mode 0600; the config names which file to read.

/// The backend credentials-file template for `provider` (`s3`/`azure`/`gcp`),
/// written to `~/.verglas/credentials/<name>` mode 0600. Each provider's format
/// differs: S3 is AWS-INI, Azure is a key=value file, GCP names a service-account
/// JSON path. Blanks the operator fills in; secrets never go in `config.toml`.
/// Pure so its shape is asserted.
pub fn render_credentials_template(provider: &str) -> String {
    match provider {
        "azure" => render_azure_credentials_template(),
        "gcp" => render_gcp_credentials_template(),
        // s3 (and anything else, though init validates the provider first).
        _ => render_s3_credentials_template(),
    }
}

/// The S3-family (AWS/OCI/MinIO) credentials template, AWS credentials-file
/// format with blanks the operator fills in.
fn render_s3_credentials_template() -> String {
    "# Verglas backend credentials, in AWS credentials-file format.\n\
     # Fill in the keys for your origin store. This file is created with mode\n\
     # 0600 and must not be committed. For temporary credentials, add an\n\
     # aws_session_token line.\n\
     \n\
     [default]\n\
     aws_access_key_id =\n\
     aws_secret_access_key =\n"
        .to_owned()
}

/// The Azure Blob credentials template: the account name plus one authentication
/// mode. The operator fills in exactly one of the account key, a SAS token, or
/// the client-secret triple; the others stay commented.
fn render_azure_credentials_template() -> String {
    "# Verglas backend credentials for Azure Blob Storage, key=value format.\n\
     # Set the account name and exactly ONE authentication mode. This file is\n\
     # created with mode 0600 and must not be committed.\n\
     \n\
     azure_storage_account_name =\n\
     \n\
     # Shared account key:\n\
     azure_storage_account_key =\n\
     \n\
     # Or a SAS token (raw query string, sv=...&sig=...):\n\
     # azure_storage_sas_key =\n\
     \n\
     # Or a service-principal client-secret triple:\n\
     # azure_client_id =\n\
     # azure_client_secret =\n\
     # azure_tenant_id =\n"
        .to_owned()
}

/// The GCP Cloud Storage credentials template: the path to the service-account
/// JSON key file. The JSON itself never lands here or in the config.
fn render_gcp_credentials_template() -> String {
    "# Verglas backend credentials for Google Cloud Storage, key=value format.\n\
     # Point this at your service-account JSON key file. This file is created\n\
     # with mode 0600 and must not be committed.\n\
     \n\
     google_service_account_path =\n"
        .to_owned()
}

/// Renders the endpoint keypair file contents in AWS credentials format: a
/// single `[default]` profile carrying the generated access key and secret. The
/// S3 endpoint reads this file (mode 0600) so the keypair never lives in
/// `config.toml`. Pure so its shape is asserted.
pub fn render_endpoint_credentials(access_key_id: &str, secret_access_key: &str) -> String {
    format!(
        "[default]\naws_access_key_id = {access_key_id}\naws_secret_access_key = {secret_access_key}\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_credentials_template_is_aws_format_with_blanks() {
        let template = render_credentials_template("s3");
        assert!(template.contains("[default]"), "has a default profile");
        assert!(
            template.contains("aws_access_key_id ="),
            "has the key blank"
        );
        assert!(
            template.contains("aws_secret_access_key ="),
            "has the secret blank"
        );
        assert!(
            template.to_lowercase().contains("0600"),
            "notes the file mode"
        );
    }

    #[test]
    fn azure_credentials_template_names_account_and_auth_modes() {
        // The azure template is key=value (no AWS profile), names the account, and
        // shows all three auth modes with exactly one active by default.
        let template = render_credentials_template("azure");
        assert!(!template.contains("[default]"), "not AWS-profile format");
        assert!(
            template.contains("azure_storage_account_name ="),
            "names the account"
        );
        assert!(
            template.contains("azure_storage_account_key ="),
            "shows the account-key mode"
        );
        assert!(
            template.contains("azure_storage_sas_key"),
            "shows the SAS mode"
        );
        assert!(template.contains("azure_client_id"), "shows the SP triple");
        assert!(template.to_lowercase().contains("0600"), "notes the mode");
    }

    #[test]
    fn gcp_credentials_template_names_the_service_account_path() {
        // The gcp template names the service-account JSON path and carries no key
        // material inline.
        let template = render_credentials_template("gcp");
        assert!(
            template.contains("google_service_account_path ="),
            "names the SA path"
        );
        assert!(!template.contains("private_key"), "no key material inline");
        assert!(template.to_lowercase().contains("0600"), "notes the mode");
    }

    #[test]
    fn endpoint_credentials_are_aws_format_with_the_keypair() {
        // The generated endpoint keypair is written in AWS format so the daemon
        // reads it with the same parser the backend uses. The secret appears in
        // the file (mode 0600 on disk), never in the config.
        let creds = render_endpoint_credentials("VGKEY", "SECRET");
        assert!(creds.contains("[default]"), "has a default profile");
        assert!(
            creds.contains("aws_access_key_id = VGKEY"),
            "carries the id"
        );
        assert!(
            creds.contains("aws_secret_access_key = SECRET"),
            "carries the secret"
        );
    }
}
