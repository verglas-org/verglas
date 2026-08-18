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
    assert!(compose.contains("VERGLAS_PROVIDER"));
}

#[test]
fn startup_renders_cloudflare_and_s3_tables_profiles_without_inline_secrets() {
    for provider in ["cloudflare", "aws-s3-tables"] {
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
            .env("VERGLAS_PROVIDER", provider)
            .env("VERGLAS_STATE_DIR", temp.path())
            .env("VERGLAS_CACHE_NODE_BIN", &fake)
            .env("VERGLAS_S3_ACCESS_KEY_ID", "local-access")
            .env("VERGLAS_S3_SECRET_ACCESS_KEY", "local-secret")
            .env("VERGLAS_CATALOG_URI", "https://catalog.invalid/iceberg")
            .env("VERGLAS_CATALOG_WAREHOUSE", "warehouse")
            .env("VERGLAS_CATALOG_TOKEN", "catalog-secret")
            .env("VERGLAS_STORAGE_ACCESS_KEY_ID", "storage-access")
            .env("VERGLAS_STORAGE_SECRET_ACCESS_KEY", "storage-secret")
            .env("VERGLAS_STORAGE_BUCKET", "test-bucket")
            .env("VERGLAS_STORAGE_ENDPOINT", "https://storage.invalid")
            .env("VERGLAS_STORAGE_REGION", "us-east-1")
            .status()
            .expect("run startup");
        assert!(status.success(), "{provider} startup failed");

        let rendered = fs::read_to_string(capture).expect("rendered config");
        assert!(rendered.contains("consistency = \"eventual\""));
        assert!(rendered.contains("poll_interval_seconds"));
        assert!(rendered.contains("credentials_file"));
        assert!(!rendered.contains("catalog-secret"));
        assert!(!rendered.contains("storage-secret"));
        if provider == "aws-s3-tables" {
            assert!(rendered.contains("sigv4_signing_name = \"s3tables\""));
            assert!(rendered.contains("*--table-s3"));
        }
    }
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
