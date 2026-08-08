//! Contract-level parsing and semantic validation tests.

use verglas_vessel_contract::{ManifestError, VesselManifest, parse_manifest};

const VALID: &str = include_str!("fixtures/valid.yaml");

#[test]
fn parses_a_composable_versioned_vessel() {
    let vessel = parse_manifest(VALID).expect("valid fixture must parse");

    assert_eq!(vessel.name, "hormuz-tracker");
    assert_eq!(vessel.version, "1.0.0");
    assert_eq!(vessel.integrations[0].version, "2.1.0");
    assert_eq!(vessel.workers[0].version, "1.3.0");
    assert_eq!(vessel.interface.version, "3.0.0");
    assert_eq!(vessel.integrations[0].config.fields[0].name, "API_TOKEN");
}

#[test]
fn rejects_a_missing_component_version() {
    let yaml = VALID.replace("    version: 2.1.0\n", "");

    let error = parse_manifest(&yaml).expect_err("component version is required");
    assert!(matches!(error, ManifestError::Yaml(_)));
    assert!(error.to_string().contains("version"));
}

#[test]
fn rejects_duplicate_names_across_component_kinds() {
    let yaml = VALID.replace("  name: tracker\n", "  name: ais\n");

    let error = parse_manifest(&yaml).expect_err("component names share one namespace");
    assert!(error.to_string().contains("duplicate component name `ais`"));
}

#[test]
fn rejects_unknown_integration_grants() {
    let yaml = VALID.replace("        - ais\n", "        - missing\n");

    let error = parse_manifest(&yaml).expect_err("grant must reference an integration");
    assert!(error.to_string().contains("missing integration `missing`"));
}

#[test]
fn rejects_secret_defaults() {
    let yaml = VALID.replace(
        "          required: true\n",
        "          required: true\n          default: exposed\n",
    );

    let error = parse_manifest(&yaml).expect_err("secret values do not belong in manifests");
    assert!(error.to_string().contains("secret configuration field"));
}

#[test]
fn rejects_unsafe_project_paths() {
    let yaml = VALID.replace("    project: integrations/ais\n", "    project: ../ais\n");

    let error = parse_manifest(&yaml).expect_err("project traversal must be rejected");
    assert!(error.to_string().contains("project"));
}

#[test]
fn rejects_ambiguous_worker_triggers() {
    let yaml = VALID.replace(
        "      - event: maritime.ais.position\n",
        "      - event: maritime.ais.position\n        cron: \"* * * * *\"\n",
    );

    let error = parse_manifest(&yaml).expect_err("one trigger cannot declare two kinds");
    assert!(matches!(error, ManifestError::Yaml(_)));
}

#[test]
fn rejects_unknown_fields() {
    let yaml = VALID.replace("version: 1.0.0\n", "version: 1.0.0\nowner: example\n");

    let error = parse_manifest(&yaml).expect_err("unknown fields must be rejected");
    assert!(matches!(error, ManifestError::Yaml(_)));
    assert!(error.to_string().contains("unknown field"));
}

#[test]
fn generated_json_schema_is_current() {
    let expected = include_str!("../artifacts/vessel.schema.json").trim();
    let actual = VesselManifest::json_schema_pretty().expect("schema generation must succeed");

    assert_eq!(actual.trim(), expected);
}

#[test]
fn generated_typescript_contract_is_current() {
    let expected = include_str!("../artifacts/index.d.ts").trim();

    assert_eq!(VesselManifest::typescript_declarations().trim(), expected);
}
