//! Strict Wrangler, Turso, and pipeline manifest acceptance tests.

use tempfile::tempdir;
use verglas_gateway::{Manifest, ManifestError};

const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

fn source(extra: &str) -> String {
    format!(
        r#"{{
            "name":"counter",
            "main":"src/index.ts",
            "durable_objects":{{"bindings":[{{"name":"COUNTER","class_name":"Counter"}}]}},
            "artifacts":{{
                "worker":{{"digest":"{DIGEST}","component_dir":"./components"}},
                "durable_object":{{"digest":"{DIGEST}","component_dir":"./components"}},
                "stream":{{"digest":"{DIGEST}","component_dir":"./components"}}
            }},
            "data_root":"./state",
            "turso":{{"url_template":"https://turso.test/{{binding}}/{{do_id}}","token_file":"/tokens/{{binding}}.token"}}
            {extra}
        }}"#
    )
}

#[test]
fn accepts_jsonc_and_explicit_turso_deployment() {
    let manifest = Manifest::parse(&source(",\n            // accepted Wrangler comment\n            \"pipelines\":[{\"binding\":\"STREAM\",\"stream\":\"stream-1\"}]")).expect("manifest");
    assert_eq!(manifest.name(), "counter");
    assert_eq!(manifest.bindings()[0].class_name(), "Counter");
    assert_eq!(
        manifest
            .turso_for("COUNTER")
            .expect("deployment")
            .url("COUNTER", "a"),
        "https://turso.test/COUNTER/a"
    );
    assert_eq!(
        manifest.pipeline("STREAM").expect("pipeline").stream(),
        "stream-1"
    );
}

#[test]
fn compatibility_migrations_and_vars_are_preserved() {
    let source = source(
        ",\n            \"compatibility_date\":\"2024-01-01\",\n            \"compatibility_flags\":[\"nodejs_compat\"],\n            \"migrations\":[{\"tag\":\"v1\",\"new_classes\":[\"Counter\"]}],\n            \"vars\":{\"LIMIT\":3}",
    );
    let manifest = Manifest::parse(&source).expect("manifest");
    assert_eq!(manifest.compatibility_date(), Some("2024-01-01"));
    assert_eq!(manifest.compatibility_flags(), &["nodejs_compat"]);
    assert_eq!(manifest.migrations()[0].new_classes(), &["Counter"]);
    assert_eq!(manifest.vars()["LIMIT"], serde_json::json!(3));
}

#[test]
fn missing_turso_for_a_worker_binding_fails_closed() {
    let source = source("").replace(",\n            \"turso\":{\"url_template\":\"https://turso.test/{binding}/{do_id}\",\"token_file\":\"/tokens/{binding}.token\"}", "");
    assert!(matches!(
        Manifest::parse(&source),
        Err(ManifestError::MissingField { field: "turso" })
    ));
}

#[test]
fn unknown_turso_field_fails_closed() {
    let source = source("").replace(
        "\"token_file\":\"/tokens/{binding}.token\"",
        "\"token_file\":\"/tokens/{binding}.token\",\"fallback\":true",
    );
    assert!(matches!(
        Manifest::parse(&source),
        Err(ManifestError::UnknownTursoKey { key }) if key == "fallback"
    ));
}

#[test]
fn old_managed_cas_shape_is_rejected_as_unknown() {
    let source = source(",\n            \"managed_cas\":{\"endpoint\":\"http://cas\"}");
    assert!(matches!(
        Manifest::parse(&source),
        Err(ManifestError::UnknownTopLevelKey { key }) if key == "managed_cas"
    ));
}

#[test]
fn accepts_json_and_jsonc_paths() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let source = r#"{"name":"counter","main":"src/index.ts","durable_objects":{"bindings":[]},"artifacts":{"worker":{"digest":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","component_dir":"components"}},"data_root":"state"}"#;
    let json_path = directory.path().join("wrangler.json");
    let jsonc_path = directory.path().join("wrangler.jsonc");
    std::fs::write(&json_path, source)?;
    std::fs::write(&jsonc_path, format!("// comment\n{source}"))?;
    assert_eq!(Manifest::from_path(json_path)?.name(), "counter");
    assert_eq!(Manifest::from_path(jsonc_path)?.name(), "counter");
    Ok(())
}

#[test]
fn rejects_unknown_top_level_and_invalid_digest() {
    let unknown = source(",\n            \"surprise\":true");
    assert!(matches!(
        Manifest::parse(&unknown),
        Err(ManifestError::UnknownTopLevelKey { key }) if key == "surprise"
    ));
    let invalid = source("").replace(DIGEST, "not-hex");
    assert!(matches!(
        Manifest::parse(&invalid),
        Err(ManifestError::InvalidComponentDigest { .. })
    ));
}
