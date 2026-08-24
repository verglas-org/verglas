//! Manifest acceptance tests for the strict wrangler JSONC subset.

use tempfile::tempdir;
use verglas_gateway::{Manifest, ManifestError};

const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// Accepts comments, trailing commas, and the documented prototype fields.
#[test]
fn accepts_wrangler_jsonc_subset() {
    let source = format!(
        r#"{{
            // The build pipeline consumes main.
            "name": "counter",
            "main": "src/index.ts",
            "durable_objects": {{
                "bindings": [{{"name": "COUNTER", "class_name": "Counter",}},],
            }},
            "component_digest": "{DIGEST}",
            "component_dir": "./components",
            "data_root": "./state",
        }}"#
    );
    let manifest = Manifest::parse(&source).expect("valid manifest");

    assert_eq!(manifest.name(), "counter");
    assert_eq!(manifest.main(), "src/index.ts");
    assert_eq!(manifest.bindings().len(), 1);
    assert_eq!(
        manifest
            .binding("COUNTER")
            .map(|binding| binding.class_name()),
        Some("Counter")
    );
    assert_eq!(manifest.component_digest(), DIGEST);
}

/// Parses the strict managed CAS launch object and preserves its fences.
#[test]
fn accepts_managed_cas_launch_parameters() {
    let source = format!(
        r#"{{
            "name": "counter",
            "main": "src/index.ts",
            "durable_objects": {{"bindings": []}},
            "component_digest": "{DIGEST}",
            "component_dir": "./components",
            "data_root": "./state",
            "managed_cas": {{
                "endpoint": "http://127.0.0.1:8333",
                "bucket": "objects",
                "prefix": "verglas",
                "region": "us-east-1",
                "access_key_id": "access",
                "secret_access_key": "secret",
                "lease_token": "opaque token",
                "lease_generation": 11,
                "start_sequence": 7,
                "lease_version": "version-7"
            }}
        }}"#
    );
    let manifest = Manifest::parse(&source).expect("managed CAS manifest");
    let cas = manifest.managed_cas().expect("CAS parameters");
    assert_eq!(cas.endpoint(), "http://127.0.0.1:8333");
    assert_eq!(cas.lease_generation(), 11);
    assert_eq!(cas.start_sequence(), 7);
    assert_eq!(cas.lease_etag(), None);
    assert_eq!(cas.lease_version(), Some("version-7"));
}

/// Rejects a managed CAS object without an ETag or version fence.
#[test]
fn rejects_managed_cas_without_fence() {
    let source = format!(
        r#"{{"name":"counter","main":"src/index.ts","durable_objects":{{"bindings":[]}},"component_digest":"{DIGEST}","component_dir":"components","data_root":"state","managed_cas":{{"endpoint":"http://cas","bucket":"objects","prefix":"verglas","region":"us-east-1","access_key_id":"access","secret_access_key":"secret","lease_token":"token","lease_generation":1,"start_sequence":0}}}}"#
    );
    assert!(matches!(
        Manifest::parse(&source),
        Err(ManifestError::MissingCasFence)
    ));
}

/// Accepts both supported manifest filename extensions when reading files.
#[test]
fn accepts_json_and_jsonc_paths() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let source = format!(
        "{{\"name\":\"counter\",\"main\":\"src/index.ts\",\"durable_objects\":{{\"bindings\":[]}},\"component_digest\":\"{DIGEST}\",\"component_dir\":\"components\",\"data_root\":\"state\"}}"
    );
    let json_path = directory.path().join("wrangler.json");
    let jsonc_path = directory.path().join("wrangler.jsonc");
    std::fs::write(&json_path, &source)?;
    std::fs::write(&jsonc_path, format!("// comment\n{source}"))?;
    assert_eq!(Manifest::from_path(json_path)?.name(), "counter");
    assert_eq!(Manifest::from_path(jsonc_path)?.name(), "counter");
    Ok(())
}

/// Rejects an unknown top-level key with its exact name.
#[test]
fn rejects_unknown_top_level_key() {
    let source = format!(
        r#"{{
            "name": "counter",
            "main": "src/index.ts",
            "durable_objects": {{"bindings": []}},
            "component_digest": "{DIGEST}",
            "component_dir": "./components",
            "data_root": "./state",
            "surprise": true
        }}"#
    );

    let error = Manifest::parse(&source).expect_err("unknown field must fail");
    assert!(matches!(error, ManifestError::UnknownTopLevelKey { key } if key == "surprise"));
}

/// Rejects manifest files whose extension is outside JSON and JSONC.
#[test]
fn rejects_unsupported_manifest_extension() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("wrangler.toml");
    std::fs::write(&path, "{}")?;
    let error = Manifest::from_path(path).expect_err("unsupported extension must fail");
    assert!(matches!(error, ManifestError::UnsupportedExtension { .. }));
    Ok(())
}

/// Rejects malformed hexadecimal component identities.
#[test]
fn rejects_invalid_component_digest() {
    let source = r#"{
        "name": "counter",
        "main": "src/index.ts",
        "durable_objects": {"bindings": []},
        "component_digest": "not-hex",
        "component_dir": "./components",
        "data_root": "./state"
    }"#;

    let error = Manifest::parse(source).expect_err("invalid digest must fail");
    assert!(matches!(
        error,
        ManifestError::InvalidComponentDigest { .. }
    ));
}

/// Rejects a binding whose required class name is missing.
#[test]
fn rejects_incomplete_binding() {
    let source = format!(
        r#"{{
            "name": "counter",
            "main": "src/index.ts",
            "durable_objects": {{"bindings": [{{"name": "COUNTER"}}]}},
            "component_digest": "{DIGEST}",
            "component_dir": "./components",
            "data_root": "./state"
        }}"#
    );

    let error = Manifest::parse(&source).expect_err("incomplete binding must fail");
    assert!(matches!(error, ManifestError::MissingField { field } if field == "class_name"));
}
