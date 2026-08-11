//! Planning for atomic local deployment of compositional Vessel releases.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use verglas_vessel_contract::{
    ConfigurationField, DataGrant, VesselManifest, WorkerTrigger, parse_manifest,
};

use crate::{
    RuntimeError, TypescriptProject, VesselHttp, VesselProjectSpec, VesselRole, VesselSpec,
    validate_project_files,
};

/// Complete source bundle and runtime binding request for one Vessel release.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct VesselApplyRequest {
    /// The portable compositional Vessel YAML document.
    pub manifest: String,
    /// Project files keyed by the exact relative `project` paths in the manifest.
    pub projects: BTreeMap<String, TypescriptProject>,
    /// Private Verglas data-plane URL injected into server-side component processes.
    pub data_endpoint: String,
    /// Scoped bearer credential injected into server-side component processes.
    pub data_token: String,
}

/// One Worker registry declaration produced by a Vessel release.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct WorkerRegistration {
    /// Vessel-qualified Worker name.
    pub name: String,
    /// Subprocess launch declaration serialized for `verglas_sys.workers`.
    pub code: String,
    /// Serialized scheduler triggers.
    pub triggers: String,
    /// First table write grant used as the conventional Worker output.
    pub output: Option<String>,
    /// Bundled source, grants, and immutable ownership metadata.
    pub config: String,
    /// Exact Vessel and component revision that owns derived Jobs.
    pub created_by: String,
}

/// Product kind of one independently versioned Vessel component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AppliedComponentKind {
    /// External API container.
    Integration,
    /// Stateless Worker definition materialized as Jobs.
    Worker,
    /// Graphical full-stack application container.
    Interface,
}

/// Immutable identity resolved for one component in a Vessel release.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AppliedComponent {
    /// Manifest-local component name.
    pub name: String,
    /// Component product kind.
    pub kind: AppliedComponentKind,
    /// Independently declared source version.
    pub version: String,
    /// SHA-256 of the normalized component project.
    pub project_digest: String,
    /// Vessel-qualified runtime or Worker registry name.
    pub runtime_name: String,
}

/// Integration setup contract joined to its Vessel-qualified runtime identity.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AppliedIntegration {
    /// Manifest-local Integration name.
    pub name: String,
    /// Independently declared source version.
    pub version: String,
    /// Vessel-qualified container and reflected namespace identity.
    pub runtime_name: String,
    /// Integration-owned configuration fields and setup guide.
    pub config: verglas_vessel_contract::IntegrationConfiguration,
}

/// Persisted desired state for one completely resolved Vessel release.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AppliedVessel {
    /// Stable Vessel identity.
    pub name: String,
    /// Manifest release version.
    pub version: String,
    /// Digest locking the manifest and every component project.
    pub digest: String,
    /// Digest of non-release runtime bindings such as rotated scoped credentials.
    #[serde(default)]
    pub runtime_digest: String,
    /// Exact independently versioned component identities.
    pub components: Vec<AppliedComponent>,
    /// Configuration contracts for the release's Integration components.
    pub integrations: Vec<AppliedIntegration>,
    /// Resolved long-lived container declarations.
    pub services: Vec<VesselSpec>,
    /// Resolved Worker declarations whose Jobs belong to this release.
    pub workers: Vec<WorkerRegistration>,
    /// Vessel-qualified Interface component used by local preview routing.
    pub interface_runtime: String,
}

/// Fully validated plan constructed before any image build or state mutation.
#[derive(Clone)]
pub struct VesselApplyPlan {
    /// Parsed portable contract.
    pub manifest: VesselManifest,
    /// Standalone Integration and Interface build inputs.
    pub services: Vec<VesselProjectSpec>,
    /// Worker registry declarations.
    pub workers: Vec<WorkerRegistration>,
    /// Independent component identities.
    pub components: Vec<AppliedComponent>,
    /// Content digest locking this complete release.
    pub digest: String,
    /// Data-plane endpoint used for Worker registration and component SDK access.
    pub(crate) data_endpoint: String,
    /// Scoped token used for Worker registration and component SDK access.
    pub(crate) data_token: String,
    runtime_digest: String,
}

/// Rejection while resolving a complete Vessel release.
#[derive(Debug, Error)]
pub enum CompositionError {
    /// The YAML contract was invalid.
    #[error(transparent)]
    Manifest(#[from] verglas_vessel_contract::ManifestError),
    /// A referenced project was absent from the source bundle.
    #[error("Vessel component references missing project `{project}`")]
    MissingProject {
        /// Missing manifest-relative project path.
        project: String,
    },
    /// The source bundle contained a project no component references.
    #[error("Vessel source bundle contains unexpected project `{project}`")]
    UnexpectedProject {
        /// Extra project path.
        project: String,
    },
    /// A Worker project omitted its conventional entrypoint.
    #[error("Worker `{worker}` project must contain src/worker.ts")]
    MissingWorkerEntrypoint {
        /// Worker component name.
        worker: String,
    },
    /// Data-plane bindings were missing.
    #[error("Vessel runtime requires non-empty dataEndpoint and dataToken")]
    MissingDataBinding,
    /// A container project violated the standalone build contract.
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    /// A normalized plan could not be encoded.
    #[error("could not encode Vessel release: {0}")]
    Encoding(#[from] serde_json::Error),
}

impl VesselApplyPlan {
    /// Parses and resolves every component without building or mutating runtime state.
    pub fn new(request: VesselApplyRequest) -> Result<Self, CompositionError> {
        if request.data_endpoint.trim().is_empty() || request.data_token.trim().is_empty() {
            return Err(CompositionError::MissingDataBinding);
        }
        let manifest = parse_manifest(&request.manifest)?;
        let data_endpoint = request.data_endpoint.trim_end_matches('/').to_owned();
        let data_token = request.data_token.clone();
        let runtime_digest = hex::encode(Sha256::digest(
            format!("{data_endpoint}\0{data_token}").as_bytes(),
        ));
        let referenced = manifest
            .integrations
            .iter()
            .map(|component| component.project.as_str())
            .chain(
                manifest
                    .workers
                    .iter()
                    .map(|component| component.project.as_str()),
            )
            .chain(std::iter::once(manifest.interface.project.as_str()))
            .collect::<BTreeSet<_>>();
        for project in &referenced {
            if !request.projects.contains_key(*project) {
                return Err(CompositionError::MissingProject {
                    project: (*project).to_owned(),
                });
            }
        }
        for project in request.projects.keys() {
            if !referenced.contains(project.as_str()) {
                return Err(CompositionError::UnexpectedProject {
                    project: project.clone(),
                });
            }
        }

        let mut services = Vec::new();
        let mut workers = Vec::new();
        let mut components = Vec::new();
        for integration in &manifest.integrations {
            let project = request.projects[&integration.project].clone();
            let runtime_name = runtime_name(&manifest.name, &integration.name);
            let definition = integration_definition(integration)?;
            let service = VesselProjectSpec {
                name: runtime_name.clone(),
                role: VesselRole::Integration,
                project: project.clone(),
                environment: BTreeMap::from([
                    (
                        "VERGLAS_DATA_ENDPOINT".to_owned(),
                        request.data_endpoint.clone(),
                    ),
                    ("VERGLAS_DATA_TOKEN".to_owned(), request.data_token.clone()),
                    ("VERGLAS_INTEGRATION_NAME".to_owned(), runtime_name.clone()),
                    ("VERGLAS_INTEGRATION_PORT".to_owned(), "8370".to_owned()),
                    (
                        "VERGLAS_INTEGRATION_ENTRYPOINT".to_owned(),
                        "file:///app/src/integration.ts".to_owned(),
                    ),
                    ("VERGLAS_INTEGRATION_DEFINITION_JSON".to_owned(), definition),
                    ("VERGLAS_VESSEL_NAME".to_owned(), manifest.name.clone()),
                    (
                        "VERGLAS_VESSEL_VERSION".to_owned(),
                        manifest.version.clone(),
                    ),
                    (
                        "VERGLAS_COMPONENT_VERSION".to_owned(),
                        integration.version.clone(),
                    ),
                ]),
                http: VesselHttp {
                    port: 8370,
                    health_path: Some("/health".to_owned()),
                },
            };
            service.build_context()?;
            components.push(component_identity(
                &integration.name,
                AppliedComponentKind::Integration,
                &integration.version,
                &runtime_name,
                &project,
            )?);
            services.push(service);
        }
        for worker in &manifest.workers {
            let project = request.projects[&worker.project].clone();
            validate_project_files(&project.files, false)?;
            if !project.files.contains_key("src/worker.ts") {
                return Err(CompositionError::MissingWorkerEntrypoint {
                    worker: worker.name.clone(),
                });
            }
            let runtime_name = runtime_name(&manifest.name, &worker.name);
            workers.push(worker_registration(&manifest, worker, &project)?);
            components.push(component_identity(
                &worker.name,
                AppliedComponentKind::Worker,
                &worker.version,
                &runtime_name,
                &project,
            )?);
        }
        let interface = &manifest.interface;
        let interface_project = request.projects[&interface.project].clone();
        let interface_runtime = runtime_name(&manifest.name, &interface.name);
        let service = VesselProjectSpec {
            name: interface_runtime.clone(),
            role: VesselRole::Application,
            project: interface_project.clone(),
            environment: BTreeMap::from([
                ("VERGLAS_DATA_ENDPOINT".to_owned(), request.data_endpoint),
                ("VERGLAS_DATA_TOKEN".to_owned(), request.data_token),
                ("VERGLAS_VESSEL_NAME".to_owned(), manifest.name.clone()),
                (
                    "VERGLAS_VESSEL_VERSION".to_owned(),
                    manifest.version.clone(),
                ),
                (
                    "VERGLAS_COMPONENT_VERSION".to_owned(),
                    interface.version.clone(),
                ),
                ("PORT".to_owned(), interface.port.to_string()),
            ]),
            http: VesselHttp {
                port: interface.port,
                health_path: Some("/health".to_owned()),
            },
        };
        service.build_context()?;
        components.push(component_identity(
            &interface.name,
            AppliedComponentKind::Interface,
            &interface.version,
            &interface_runtime,
            &interface_project,
        )?);
        services.push(service);

        let release = serde_json::to_vec(&json!({
            "manifest": manifest,
            "components": components,
        }))?;
        let digest = hex::encode(Sha256::digest(release));
        Ok(Self {
            manifest,
            services,
            workers,
            components,
            digest,
            data_endpoint,
            data_token,
            runtime_digest,
        })
    }

    /// Returns the non-secret digest used to detect scoped binding rotation.
    pub fn runtime_digest(&self) -> &str {
        &self.runtime_digest
    }

    /// Resolves built images into persistable desired runtime state.
    pub fn applied(self, services: Vec<VesselSpec>) -> AppliedVessel {
        let integrations = self
            .manifest
            .integrations
            .iter()
            .map(|integration| AppliedIntegration {
                name: integration.name.clone(),
                version: integration.version.clone(),
                runtime_name: runtime_name(&self.manifest.name, &integration.name),
                config: integration.config.clone(),
            })
            .collect();
        AppliedVessel {
            name: self.manifest.name,
            version: self.manifest.version,
            digest: self.digest,
            runtime_digest: self.runtime_digest,
            components: self.components,
            integrations,
            services,
            workers: self.workers,
            interface_runtime: self
                .services
                .last()
                .map(|service| service.name.clone())
                .expect("validated Vessel always has one Interface service"),
        }
    }
}

/// Builds the generic Integration runtime's configuration screen definition.
fn integration_definition(
    integration: &verglas_vessel_contract::IntegrationComponent,
) -> Result<String, serde_json::Error> {
    let fields = integration
        .config
        .fields
        .iter()
        .map(configuration_field)
        .collect::<Vec<_>>();
    serde_json::to_string(&json!({
        "title": integration.name,
        "description": format!("{} Integration", integration.name),
        "fields": fields,
        "instructions": integration.config.setup,
    }))
}

/// Converts a portable configuration field to the existing Integration host contract.
fn configuration_field(field: &ConfigurationField) -> Value {
    let mut value = json!({
        "name": field.name,
        "label": field.label,
        "type": field.field_type,
        "required": field.required,
        "description": field.help,
        "placeholder": field.placeholder,
    });
    if let Some(default) = &field.default {
        value["defaultValue"] = default.clone();
    }
    value
}

/// Converts one Worker component into the local append-only registry contract.
fn worker_registration(
    manifest: &VesselManifest,
    worker: &verglas_vessel_contract::WorkerComponent,
    project: &TypescriptProject,
) -> Result<WorkerRegistration, serde_json::Error> {
    let name = runtime_name(&manifest.name, &worker.name);
    let triggers = worker.triggers.iter().map(trigger_json).collect::<Vec<_>>();
    let output = worker.grants.tables.iter().find_map(|grant| match grant {
        DataGrant::Write(grant) => Some(grant.write.clone()),
        DataGrant::Read(_) => None,
    });
    Ok(WorkerRegistration {
        name,
        code: serde_json::to_string(&json!({
            "exec": [
                "sh", "-c",
                "exec /usr/local/bin/bun /sdks/typescript/src/subprocess/endpoint-run.ts file://$PWD/src/worker.ts"
            ],
            "cwd": ".",
        }))?,
        triggers: serde_json::to_string(&triggers)?,
        output,
        config: serde_json::to_string(&json!({
            "files": project.files,
            "vessel": {
                "name": manifest.name,
                "version": manifest.version,
                "component": worker.name,
                "componentVersion": worker.version,
            },
            "grants": worker.grants,
        }))?,
        created_by: format!(
            "vessel:{}@{}/worker:{}@{}",
            manifest.name, manifest.version, worker.name, worker.version
        ),
    })
}

/// Converts a portable trigger into the Worker registry's wire contract.
fn trigger_json(trigger: &WorkerTrigger) -> Value {
    match trigger {
        WorkerTrigger::Event(trigger) => {
            json!({"type": "event", "eventType": trigger.event})
        }
        WorkerTrigger::Cron(trigger) => json!({"type": "cron", "schedule": trigger.cron}),
        WorkerTrigger::Table(trigger) => json!({
            "type": "event",
            "eventType": "org.verglas.table.commit",
            "subject": trigger.table,
        }),
        WorkerTrigger::Webhook(trigger) => {
            json!({"type": "webhook", "path": trigger.webhook})
        }
    }
}

/// Creates the stable runtime name shared by containers and Worker declarations.
fn runtime_name(vessel: &str, component: &str) -> String {
    format!("{vessel}-{component}")
}

/// Resolves the immutable identity of one component project.
fn component_identity(
    name: &str,
    kind: AppliedComponentKind,
    version: &str,
    runtime_name: &str,
    project: &TypescriptProject,
) -> Result<AppliedComponent, serde_json::Error> {
    let encoded = serde_json::to_vec(project)?;
    Ok(AppliedComponent {
        name: name.to_owned(),
        kind,
        version: version.to_owned(),
        project_digest: hex::encode(Sha256::digest(encoded)),
        runtime_name: runtime_name.to_owned(),
    })
}
