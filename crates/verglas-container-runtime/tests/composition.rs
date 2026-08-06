//! Compositional Vessel planning and ownership tests.

use std::collections::BTreeMap;

use verglas_container_runtime::{
    CompositionError, TypescriptProject, VesselApplyPlan, VesselApplyRequest, VesselRole,
};

const MANIFEST: &str = r#"
apiVersion: verglas.io/v1alpha1
kind: Vessel
name: hormuz-tracker
version: 1.0.0
integrations:
  - name: ais
    version: 2.1.0
    project: integrations/ais
    config:
      fields:
        - name: API_TOKEN
          label: AIS token
          type: secret
          required: true
      setup:
        - title: Create token
          description: Create a read-only provider token.
          url: https://example.com/tokens
workers:
  - name: ingest
    version: 1.4.0
    project: workers/ingest
    triggers:
      - event: maritime.ais.position
    grants:
      tables:
        - write: maritime.positions
      integrations: [ais]
interface:
  name: map
  version: 3.0.0
  project: application
  port: 8380
  grants:
    tables:
      - read: maritime.positions
    integrations: [ais]
"#;

fn project(files: impl IntoIterator<Item = (&'static str, &'static str)>) -> TypescriptProject {
    TypescriptProject {
        files: files
            .into_iter()
            .map(|(path, source)| (path.to_owned(), source.to_owned()))
            .collect(),
    }
}

fn request() -> VesselApplyRequest {
    VesselApplyRequest {
        manifest: MANIFEST.to_owned(),
        projects: BTreeMap::from([
            (
                "integrations/ais".to_owned(),
                project([
                    (
                        "package.json",
                        r#"{"scripts":{"start":"bun vendor/verglas-integration-runtime/runtime.mjs"},"dependencies":{"hono":"4.8.3"}}"#,
                    ),
                    ("src/integration.ts", "export default {api:{},verify(){}}"),
                ]),
            ),
            (
                "workers/ingest".to_owned(),
                project([("src/worker.ts", "export default defineWorker({})")]),
            ),
            (
                "application".to_owned(),
                project([
                    (
                        "package.json",
                        r#"{"scripts":{"start":"bun src/server.ts"},"dependencies":{"hono":"4.8.3"}}"#,
                    ),
                    ("src/server.ts", "Bun.serve({port: 8380, fetch(){}})"),
                ]),
            ),
        ]),
        data_endpoint: "http://verglas-server:8334".to_owned(),
        data_token: "scoped-token".to_owned(),
    }
}

#[test]
fn plans_every_component_under_one_versioned_vessel_release() {
    let plan = VesselApplyPlan::new(request()).expect("composition must plan");

    assert_eq!(plan.manifest.name, "hormuz-tracker");
    assert_eq!(plan.manifest.version, "1.0.0");
    assert_eq!(plan.services.len(), 2);
    assert_eq!(plan.services[0].name, "hormuz-tracker-ais");
    assert_eq!(plan.services[0].role, VesselRole::Integration);
    assert_eq!(plan.services[1].name, "hormuz-tracker-map");
    assert_eq!(plan.workers.len(), 1);
    assert_eq!(plan.workers[0].name, "hormuz-tracker-ingest");
    assert_eq!(
        plan.workers[0].created_by,
        "vessel:hormuz-tracker@1.0.0/worker:ingest@1.4.0"
    );
    assert_eq!(plan.components[0].version, "2.1.0");
    assert_eq!(plan.components[1].version, "1.4.0");
    assert_eq!(plan.components[2].version, "3.0.0");
}

#[test]
fn carries_integration_configuration_schema_without_values() {
    let plan = VesselApplyPlan::new(request()).expect("composition must plan");
    let integration = &plan.services[0];
    let definition = integration
        .environment
        .get("VERGLAS_INTEGRATION_DEFINITION_JSON")
        .expect("definition environment");

    assert!(definition.contains("API_TOKEN"));
    assert!(definition.contains("Create a read-only provider token"));
    assert!(!definition.contains("scoped-token"));
    assert_eq!(
        integration.environment.get("VERGLAS_DATA_TOKEN"),
        Some(&"scoped-token".to_owned())
    );
}

#[test]
fn rejects_missing_and_unreferenced_projects_before_building() {
    let mut missing = request();
    missing.projects.remove("workers/ingest");
    assert!(matches!(
        VesselApplyPlan::new(missing),
        Err(CompositionError::MissingProject { .. })
    ));

    let mut extra = request();
    extra.projects.insert(
        "unused".to_owned(),
        project([("src/index.ts", "export {}")]),
    );
    assert!(matches!(
        VesselApplyPlan::new(extra),
        Err(CompositionError::UnexpectedProject { .. })
    ));
}

#[test]
fn changing_one_component_revision_changes_the_release_digest() {
    let first = VesselApplyPlan::new(request()).expect("first plan");
    let changed = request()
        .manifest
        .replace("version: 2.1.0", "version: 2.2.0");
    let second = VesselApplyPlan::new(VesselApplyRequest {
        manifest: changed,
        ..request()
    })
    .expect("second plan");

    assert_ne!(first.digest, second.digest);
    assert_ne!(first.components[0].version, second.components[0].version);
}

#[test]
fn credential_rotation_changes_runtime_binding_without_changing_release_identity() {
    let first = VesselApplyPlan::new(request()).expect("first plan");
    let second = VesselApplyPlan::new(VesselApplyRequest {
        data_token: "rotated-token".to_owned(),
        ..request()
    })
    .expect("rotated plan");

    assert_eq!(first.digest, second.digest);
    assert_ne!(first.runtime_digest(), second.runtime_digest());
}
