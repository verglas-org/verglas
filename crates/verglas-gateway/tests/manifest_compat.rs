//! Wrangler compatibility metadata tests for the gateway manifest parser.

use verglas_gateway::{Manifest, ManifestError};

const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// Preserves accepted compatibility, migration, and environment metadata.
#[test]
fn accepts_compatibility_migrations_and_vars() {
    let source = format!(
        r#"{{
            "name": "counter",
            "main": "src/index.ts",
            "compatibility_date": "2024-01-01",
            "compatibility_flags": ["nodejs_compat", "experimental"],
            "durable_objects": {{"bindings": []}},
            "migrations": [
                {{"tag": "v1", "new_classes": ["Counter"]}},
                {{"tag": "v2", "new_sqlite_classes": ["SqlCounter"]}}
            ],
            "vars": {{"API_URL": "https://example.test", "LIMIT": 3}},
            "component_digest": "{DIGEST}",
            "component_dir": "./components",
            "data_root": "./state"
        }}"#
    );
    let manifest = Manifest::parse(&source).expect("valid compatibility manifest");

    assert_eq!(manifest.compatibility_date(), Some("2024-01-01"));
    assert_eq!(
        manifest.compatibility_flags(),
        &["nodejs_compat".to_owned(), "experimental".to_owned()]
    );
    assert_eq!(manifest.migrations().len(), 2);
    assert_eq!(manifest.migrations()[0].tag(), "v1");
    assert_eq!(
        manifest.migrations()[0].new_classes(),
        &["Counter".to_owned()]
    );
    assert_eq!(
        manifest.migrations()[1].new_sqlite_classes(),
        &["SqlCounter".to_owned()]
    );
    assert_eq!(
        manifest.vars()["API_URL"],
        serde_json::json!("https://example.test")
    );
    assert_eq!(manifest.vars()["LIMIT"], serde_json::json!(3));
}

/// Rejects migration kinds outside the frozen compatibility contract.
#[test]
fn rejects_unsupported_migration_kind() {
    let source = format!(
        r#"{{
            "name": "counter",
            "main": "src/index.ts",
            "durable_objects": {{"bindings": []}},
            "migrations": [{{"tag": "v1", "deleted_classes": ["Counter"]}}],
            "component_digest": "{DIGEST}",
            "component_dir": "./components",
            "data_root": "./state"
        }}"#
    );

    let error = Manifest::parse(&source).expect_err("unsupported migration must fail");
    assert!(matches!(
        error,
        ManifestError::UnknownMigrationKey { key } if key == "deleted_classes"
    ));
}
