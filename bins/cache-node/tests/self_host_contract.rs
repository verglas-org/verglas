//! Frozen self-hosting acceptance contract for #20 and #96.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_owned()
}

fn read(relative: &str) -> String {
    fs::read_to_string(root().join(relative)).expect(relative)
}

#[test]
fn compose_is_one_verglas_node_without_a_bundled_object_store() {
    let compose = read("docker-compose.yml");
    assert!(!compose.to_ascii_lowercase().contains("minio"));
    assert!(!compose.contains("origin-init:"));
    assert_eq!(
        compose
            .lines()
            .filter(|line| line.starts_with("  ")
                && line.ends_with(":")
                && !line.starts_with("    "))
            .count(),
        1,
        "the self-host stack must be a single disposable Verglas process"
    );
    // The node serves its own catalog, so there is no external-catalog client
    // to point at a provider. `VERGLAS_CATALOG` decides whether it runs at all.
    assert!(!compose.contains("VERGLAS_PROVIDER"));
    assert!(!compose.contains("VERGLAS_CATALOG_URI"));
    assert!(compose.contains("VERGLAS_CATALOG"));
}

/// Renders the entrypoint config for one `VERGLAS_CATALOG` value.
///
/// Returns the config the node would have been started with, so a caller can
/// assert on what the catalog mode did and did not emit.
fn render_startup_config(catalog_mode: &str, extra: &[(&str, &str)]) -> String {
    let temp = tempfile::tempdir().expect("temp state");
    let capture = temp.path().join("captured.toml");
    let fake = temp.path().join("verglas-cache-node");
    fs::write(
        &fake,
        format!("#!/bin/sh\ncp \"$2\" '{}'\n", capture.display()),
    )
    .expect("fake node");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&fake, fs::Permissions::from_mode(0o700)).expect("chmod");
    }

    let mut command = Command::new("sh");
    command
        .arg(root().join("docker-entrypoint.sh"))
        .env("VERGLAS_CATALOG", catalog_mode)
        .env("VERGLAS_STATE_DIR", temp.path())
        .env("VERGLAS_CACHE_NODE_BIN", &fake)
        .env("VERGLAS_S3_ACCESS_KEY_ID", "local-access")
        .env("VERGLAS_S3_SECRET_ACCESS_KEY", "local-secret")
        .env("VERGLAS_STORAGE_ACCESS_KEY_ID", "storage-access")
        .env("VERGLAS_STORAGE_SECRET_ACCESS_KEY", "storage-secret")
        .env("VERGLAS_STORAGE_BUCKET", "test-bucket")
        .env("VERGLAS_STORAGE_ENDPOINT", "https://storage.invalid")
        .env("VERGLAS_STORAGE_REGION", "us-east-1");
    for (key, value) in extra {
        command.env(key, value);
    }
    let status = command.status().expect("run startup");
    assert!(
        status.success(),
        "startup failed for catalog={catalog_mode}"
    );
    fs::read_to_string(capture).expect("rendered config")
}

/// A cache-only node renders no catalog configuration and needs no catalog
/// identity. This is the edge and WAL-only shape.
#[test]
fn catalog_off_renders_a_cache_only_config_without_any_catalog_section() {
    let rendered = render_startup_config("off", &[]);
    assert!(!rendered.contains("[catalog_server]"));
    assert!(!rendered.contains("[catalog_archive]"));
    assert!(rendered.contains("[backend]"));
    assert!(rendered.contains("credentials_file"));
    assert!(!rendered.contains("storage-secret"));
}

/// `off` is the default, so an unset variable must not start a catalog.
#[test]
fn an_absent_catalog_variable_defaults_to_a_cache_only_config() {
    let temp = tempfile::tempdir().expect("temp state");
    let capture = temp.path().join("captured.toml");
    let fake = temp.path().join("verglas-cache-node");
    fs::write(
        &fake,
        format!("#!/bin/sh\ncp \"$2\" '{}'\n", capture.display()),
    )
    .expect("fake node");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&fake, fs::Permissions::from_mode(0o700)).expect("chmod");
    }
    let status = Command::new("sh")
        .arg(root().join("docker-entrypoint.sh"))
        .env_remove("VERGLAS_CATALOG")
        .env("VERGLAS_STATE_DIR", temp.path())
        .env("VERGLAS_CACHE_NODE_BIN", &fake)
        .env("VERGLAS_S3_ACCESS_KEY_ID", "local-access")
        .env("VERGLAS_S3_SECRET_ACCESS_KEY", "local-secret")
        .env("VERGLAS_STORAGE_ACCESS_KEY_ID", "storage-access")
        .env("VERGLAS_STORAGE_SECRET_ACCESS_KEY", "storage-secret")
        .env("VERGLAS_STORAGE_BUCKET", "test-bucket")
        .env("VERGLAS_STORAGE_ENDPOINT", "https://storage.invalid")
        .status()
        .expect("run startup");
    assert!(status.success(), "startup failed with no VERGLAS_CATALOG");
    let rendered = fs::read_to_string(capture).expect("rendered config");
    assert!(!rendered.contains("[catalog_server]"));
}

/// `on` renders both catalog sections and keeps secrets out of the config.
#[test]
fn catalog_on_renders_both_catalog_sections_without_inline_secrets() {
    let rendered = render_startup_config(
        "on",
        &[
            ("VERGLAS_CATALOG_AUTHZ_ISSUER", "https://issuer.invalid"),
            ("VERGLAS_CATALOG_AUTHZ_JWKS", "https://issuer.invalid/jwks"),
        ],
    );
    assert!(rendered.contains("[catalog_server]"));
    assert!(rendered.contains("[catalog_archive]"));
    assert!(rendered.contains("credentials_file"));
    assert!(!rendered.contains("storage-secret"));
    assert!(!rendered.contains("local-secret"));
}

/// An unrecognised value fails startup instead of silently picking a mode.
#[test]
fn an_invalid_catalog_value_fails_startup() {
    let temp = tempfile::tempdir().expect("temp state");
    let status = Command::new("sh")
        .arg(root().join("docker-entrypoint.sh"))
        .env("VERGLAS_CATALOG", "yes")
        .env("VERGLAS_STATE_DIR", temp.path())
        .env("VERGLAS_CACHE_NODE_BIN", "/usr/bin/true")
        .env("VERGLAS_S3_ACCESS_KEY_ID", "local-access")
        .env("VERGLAS_S3_SECRET_ACCESS_KEY", "local-secret")
        .env("VERGLAS_STORAGE_ACCESS_KEY_ID", "storage-access")
        .env("VERGLAS_STORAGE_SECRET_ACCESS_KEY", "storage-secret")
        .env("VERGLAS_STORAGE_BUCKET", "test-bucket")
        .env("VERGLAS_STORAGE_ENDPOINT", "https://storage.invalid")
        .status()
        .expect("run startup");
    assert!(!status.success(), "an invalid catalog mode must not start");
}

#[test]
fn public_docs_cover_all_three_catalog_modes_and_local_core_apis() {
    let docs = format!(
        "{}\n{}",
        read("README.md"),
        read("docs/get-started/self-host.mdx")
    );
    for required in [
        "Verglas Cloud",
        "Cloudflare Data Catalog",
        "Amazon S3 Tables",
        "Tables",
        "Graphs",
        "Vectors",
        "poll",
        "/admin/catalog/events",
    ] {
        assert!(
            docs.contains(required),
            "missing self-host documentation: {required}"
        );
    }
    assert!(!docs.to_ascii_lowercase().contains("minio"));
}
