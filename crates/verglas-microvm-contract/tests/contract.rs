//! Contract-level parsing and semantic validation tests.

use verglas_microvm_contract::{ManifestError, MicroVmStack, parse_manifest};

const VALID: &str = include_str!("fixtures/valid.yaml");

#[test]
fn parses_the_minimal_stack_contract() {
    let stack = parse_manifest(VALID).expect("valid fixture must parse");

    assert_eq!(stack.api_version, "verglas.io/v1alpha1");
    assert_eq!(stack.kind, "MicroVMStack");
    assert_eq!(stack.tenant.name, "tenant-runtime");
    assert_eq!(stack.components.len(), 3);
    assert_eq!(
        stack.components[0].cluster.as_ref().map(|c| c.members),
        Some(3)
    );
}

#[test]
fn rejects_unsafe_r2_object_keys() {
    let yaml = VALID.replace(
        "scheduler-v1/rootfs.ext4",
        "../scheduler-v1/rootfs.ext4",
    );

    let error = parse_manifest(&yaml).expect_err("unsafe object key must be rejected");
    assert!(matches!(error, ManifestError::Validation(_)));
    assert!(error.to_string().contains("runtime.object"));
}

#[test]
fn rejects_invalid_runtime_sha256() {
    let yaml = VALID.replacen(
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "not-a-sha",
        1,
    );

    let error = parse_manifest(&yaml).expect_err("invalid SHA-256 must be rejected");
    assert!(matches!(error, ManifestError::Validation(_)));
    assert!(error.to_string().contains("runtime.sha256"));
}

#[test]
fn rejects_dependency_cycles() {
    let yaml = VALID
        .replace(
            "    dependsOn:\n      - postgres\n",
            "    dependsOn:\n      - postgres\n      - cache\n",
        )
        .replace(
            "    health:\n      port: cache\n",
            "    health:\n      port: cache\n    dependsOn:\n      - scheduler\n",
        );

    let error = parse_manifest(&yaml).expect_err("cycle must be rejected");
    assert!(error.to_string().contains("cycle"));
}

#[test]
fn rejects_unknown_dependencies() {
    let yaml = VALID.replace("    dependsOn: [postgres]\n", "    dependsOn: [missing]\n");

    let error = parse_manifest(&yaml).expect_err("unknown dependency must be rejected");
    assert!(error.to_string().contains("missing"));
}

#[test]
fn rejects_ingress_that_is_not_a_declared_port() {
    let yaml = VALID.replace(
        "  port: api\n\ncomponents:",
        "  port: absent\n\ncomponents:",
    );

    let error = parse_manifest(&yaml).expect_err("unknown ingress port must be rejected");
    assert!(error.to_string().contains("ingress"));
}

#[test]
fn rejects_health_that_is_not_a_declared_port() {
    let yaml = VALID.replacen("      port: cache\n", "      port: absent\n", 1);

    let error = parse_manifest(&yaml).expect_err("unknown health port must be rejected");
    assert!(error.to_string().contains("health.port"));
}

#[test]
fn generated_json_schema_is_current() {
    let expected = include_str!("../artifacts/microvm-stack.schema.json").trim();
    let actual = MicroVmStack::json_schema_pretty().expect("schema generation must succeed");

    assert_eq!(actual.trim(), expected);
}

#[test]
fn generated_typescript_contract_is_current() {
    let expected = include_str!("../artifacts/index.d.ts").trim();

    assert_eq!(MicroVmStack::typescript_declarations().trim(), expected);
}
