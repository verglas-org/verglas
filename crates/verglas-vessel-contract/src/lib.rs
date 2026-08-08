//! Portable contract for composable, independently versioned Verglas Vessels.

use std::collections::BTreeSet;
use std::path::{Component as PathComponent, Path};

use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ts_rs::TS;

/// The supported contract API version.
pub const API_VERSION: &str = "verglas.io/v1alpha1";

/// The supported manifest kind.
pub const KIND: &str = "Vessel";

/// A versioned composition of Integrations, Workers, and one Interface.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct VesselManifest {
    /// Contract API version. Must be `verglas.io/v1alpha1`.
    #[serde(rename = "apiVersion")]
    #[ts(rename = "apiVersion")]
    pub api_version: String,
    /// Manifest kind. Must be `Vessel`.
    pub kind: String,
    /// Stable Vessel name.
    pub name: String,
    /// Immutable source release version of this composition.
    pub version: String,
    /// External API components available to Workers and the Interface.
    #[serde(default)]
    pub integrations: Vec<IntegrationComponent>,
    /// Data and agentic operations materialized as Jobs at runtime.
    #[serde(default)]
    pub workers: Vec<WorkerComponent>,
    /// The Vessel's sole graphical application component.
    pub interface: InterfaceComponent,
}

/// One independently versioned external API component.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct IntegrationComponent {
    /// Name used by grants and the reflected Verglas SDK namespace.
    pub name: String,
    /// Independent source version pinned by the containing Vessel release.
    pub version: String,
    /// Relative directory containing the component project.
    pub project: String,
    /// Configuration UI and credential requirements owned by this Integration.
    pub config: IntegrationConfiguration,
}

/// Declarative configuration screen supplied by an Integration.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct IntegrationConfiguration {
    /// Values the user must or may configure at runtime.
    #[serde(default)]
    pub fields: Vec<ConfigurationField>,
    /// Ordered provider setup guidance displayed alongside the fields.
    #[serde(default)]
    pub setup: Vec<ConfigurationStep>,
}

/// One Integration configuration input definition, never its runtime value.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct ConfigurationField {
    /// Environment-style identifier exposed to the Integration process.
    pub name: String,
    /// Human-readable form label.
    pub label: String,
    /// Input and storage behavior.
    #[serde(rename = "type")]
    #[ts(rename = "type")]
    pub field_type: ConfigurationFieldType,
    /// Whether the Integration may start without a value.
    #[serde(default)]
    pub required: bool,
    /// Optional non-secret initial value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "unknown")]
    pub default: Option<serde_json::Value>,
    /// Concise field-level setup help.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub help: Option<String>,
    /// Non-sensitive example shown when the field is empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub placeholder: Option<String>,
}

/// Supported Integration configuration input behaviors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(rename_all = "lowercase")]
#[ts(rename_all = "lowercase")]
pub enum ConfigurationFieldType {
    /// Ordinary UTF-8 text.
    Text,
    /// A credential stored and rendered as a secret.
    Secret,
    /// An HTTP or HTTPS URL.
    Url,
    /// A numeric value.
    Number,
    /// A true or false value.
    Boolean,
}

/// One ordered instruction in an Integration setup guide.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct ConfigurationStep {
    /// Short action-oriented heading.
    pub title: String,
    /// Instructions the user can follow without outside context.
    pub description: String,
    /// Optional HTTPS documentation or provider-console link.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub url: Option<String>,
}

/// One independently versioned Worker definition materialized as Jobs.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct WorkerComponent {
    /// Stable Worker definition name.
    pub name: String,
    /// Independent source version pinned by the containing Vessel release.
    pub version: String,
    /// Relative directory containing the Worker project.
    pub project: String,
    /// Events that create Jobs from this Worker definition; empty means manual only.
    #[serde(default)]
    pub triggers: Vec<WorkerTrigger>,
    /// Lakehouse and Integration capabilities granted to each derived Job.
    #[serde(default)]
    pub grants: ComponentGrants,
}

/// One independently versioned graphical application component.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct InterfaceComponent {
    /// Stable application component name.
    pub name: String,
    /// Independent source version pinned by the containing Vessel release.
    pub version: String,
    /// Relative directory containing the full-stack Interface project.
    pub project: String,
    /// Private HTTP port exposed to the local preview proxy.
    pub port: u16,
    /// Lakehouse and Integration capabilities granted to the Interface.
    #[serde(default)]
    pub grants: ComponentGrants,
}

/// A single source of Jobs for a Worker definition.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(untagged)]
pub enum WorkerTrigger {
    /// Subscribe to one CloudEvents type.
    Event(EventTrigger),
    /// Run from a cron expression.
    Cron(CronTrigger),
    /// Run when a lakehouse table changes.
    Table(TableTrigger),
    /// Accept a webhook on an absolute path.
    Webhook(WebhookTrigger),
}

/// CloudEvents trigger configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct EventTrigger {
    /// Exact CloudEvents `type` value.
    pub event: String,
}

/// Cron trigger configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct CronTrigger {
    /// Five-field UTC cron expression.
    pub cron: String,
}

/// Lakehouse table-change trigger configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct TableTrigger {
    /// Fully qualified table name to observe.
    pub table: String,
}

/// Webhook trigger configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct WebhookTrigger {
    /// Absolute path exposed through authenticated Vessel ingress.
    pub webhook: String,
}

/// Scoped capabilities supplied to a Worker Job or Interface.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct ComponentGrants {
    /// Table read and write grants.
    #[serde(default)]
    pub tables: Vec<DataGrant>,
    /// Vector-index read and write grants.
    #[serde(default)]
    pub vectors: Vec<DataGrant>,
    /// Graph read and write grants.
    #[serde(default)]
    pub graphs: Vec<DataGrant>,
    /// Integration API components exposed through the reflected SDK.
    #[serde(default)]
    pub integrations: Vec<String>,
}

/// One read or write grant for a named lakehouse resource.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(untagged)]
pub enum DataGrant {
    /// Read a named resource.
    Read(ReadGrant),
    /// Write a named resource.
    Write(WriteGrant),
}

/// Read permission for one resource.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct ReadGrant {
    /// Fully qualified resource name.
    pub read: String,
}

/// Write permission for one resource.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
pub struct WriteGrant {
    /// Fully qualified resource name.
    pub write: String,
}

/// Manifest decoding, validation, or artifact-generation failure.
#[derive(Debug, Error)]
pub enum ManifestError {
    /// YAML did not match the published contract shape.
    #[error("invalid Vessel YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    /// YAML decoded but violated cross-field contract semantics.
    #[error("invalid Vessel: {0}")]
    Validation(String),
    /// A generated consumer artifact could not be serialized.
    #[error("could not generate Vessel artifact: {0}")]
    Artifact(#[from] serde_json::Error),
}

/// Decode and semantically validate a `Vessel` YAML document.
pub fn parse_manifest(yaml: &str) -> Result<VesselManifest, ManifestError> {
    let vessel: VesselManifest = serde_yaml::from_str(yaml)?;
    vessel.validate()?;
    Ok(vessel)
}

impl VesselManifest {
    /// Validate invariants not expressible by the generated JSON Schema.
    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.api_version != API_VERSION {
            return validation(format!("apiVersion must be {API_VERSION}"));
        }
        if self.kind != KIND {
            return validation(format!("kind must be {KIND}"));
        }
        validate_name("Vessel name", &self.name)?;
        validate_version("Vessel", &self.version)?;

        let integration_names = self
            .integrations
            .iter()
            .map(|integration| integration.name.as_str())
            .collect::<BTreeSet<_>>();
        let mut component_names = BTreeSet::new();
        for integration in &self.integrations {
            integration.validate()?;
            insert_component_name(&mut component_names, &integration.name)?;
        }
        for worker in &self.workers {
            worker.validate(&integration_names)?;
            insert_component_name(&mut component_names, &worker.name)?;
        }
        self.interface.validate(&integration_names)?;
        insert_component_name(&mut component_names, &self.interface.name)?;
        Ok(())
    }

    /// Generate the canonical JSON Schema consumer artifact.
    pub fn json_schema_pretty() -> Result<String, ManifestError> {
        Ok(serde_json::to_string_pretty(&schema_for!(VesselManifest))?)
    }

    /// Generate TypeScript declarations for non-Rust consumers.
    pub fn typescript_declarations() -> String {
        let declarations = [
            ConfigurationFieldType::decl(),
            ConfigurationField::decl(),
            ConfigurationStep::decl(),
            IntegrationConfiguration::decl(),
            IntegrationComponent::decl(),
            EventTrigger::decl(),
            CronTrigger::decl(),
            TableTrigger::decl(),
            WebhookTrigger::decl(),
            WorkerTrigger::decl(),
            ReadGrant::decl(),
            WriteGrant::decl(),
            DataGrant::decl(),
            ComponentGrants::decl(),
            WorkerComponent::decl(),
            InterfaceComponent::decl(),
            VesselManifest::decl(),
        ];
        let declarations =
            declarations.map(|declaration| declaration.replacen("type ", "export type ", 1));
        format!(
            concat!(
                "// Generated by verglas-vessel-contract. Do not edit.\n\n",
                "{}\n\n",
                "/** Parse and validate a Vessel YAML document. */\n",
                "export declare function parseManifest(yaml: string): VesselManifest;\n"
            ),
            declarations.join("\n\n")
        )
    }
}

impl IntegrationComponent {
    /// Validate one Integration and its owned configuration contract.
    fn validate(&self) -> Result<(), ManifestError> {
        validate_component_identity("Integration", &self.name, &self.version, &self.project)?;
        self.config.validate(&self.name)
    }
}

impl IntegrationConfiguration {
    /// Validate field identifiers, defaults, and setup guidance.
    fn validate(&self, integration: &str) -> Result<(), ManifestError> {
        let mut field_names = BTreeSet::new();
        for field in &self.fields {
            field.validate(integration)?;
            if !field_names.insert(field.name.as_str()) {
                return validation(format!(
                    "Integration `{integration}` repeats configuration field `{}`",
                    field.name
                ));
            }
        }
        for step in &self.setup {
            if step.title.trim().is_empty() || step.description.trim().is_empty() {
                return validation(format!(
                    "Integration `{integration}` setup steps require a title and description"
                ));
            }
            if let Some(url) = &step.url
                && !url.starts_with("https://")
            {
                return validation(format!(
                    "Integration `{integration}` setup URL must use https"
                ));
            }
        }
        Ok(())
    }
}

impl ConfigurationField {
    /// Validate one configuration field without accepting credential material.
    fn validate(&self, integration: &str) -> Result<(), ManifestError> {
        let valid_name = !self.name.is_empty()
            && self.name.len() <= 128
            && self
                .name
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
            && self
                .name
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_uppercase);
        if !valid_name {
            return validation(format!(
                "Integration `{integration}` configuration field `{}` must be an uppercase identifier",
                self.name
            ));
        }
        if self.label.trim().is_empty() {
            return validation(format!(
                "Integration `{integration}` configuration field `{}` requires a label",
                self.name
            ));
        }
        if self.field_type == ConfigurationFieldType::Secret && self.default.is_some() {
            return validation(format!(
                "Integration `{integration}` secret configuration field `{}` cannot declare a default",
                self.name
            ));
        }
        if let Some(default) = &self.default {
            let type_matches = match self.field_type {
                ConfigurationFieldType::Text
                | ConfigurationFieldType::Secret
                | ConfigurationFieldType::Url => default.is_string(),
                ConfigurationFieldType::Number => default.is_number(),
                ConfigurationFieldType::Boolean => default.is_boolean(),
            };
            if !type_matches {
                return validation(format!(
                    "Integration `{integration}` configuration field `{}` default has the wrong type",
                    self.name
                ));
            }
        }
        Ok(())
    }
}

impl WorkerComponent {
    /// Validate one Worker definition and its runtime references.
    fn validate(&self, integrations: &BTreeSet<&str>) -> Result<(), ManifestError> {
        validate_component_identity("Worker", &self.name, &self.version, &self.project)?;
        for trigger in &self.triggers {
            trigger.validate(&self.name)?;
        }
        self.grants.validate(&self.name, integrations)
    }
}

impl InterfaceComponent {
    /// Validate the graphical Interface and its runtime references.
    fn validate(&self, integrations: &BTreeSet<&str>) -> Result<(), ManifestError> {
        validate_component_identity("Interface", &self.name, &self.version, &self.project)?;
        if self.port == 0 {
            return validation(format!(
                "Interface `{}` port must be greater than zero",
                self.name
            ));
        }
        self.grants.validate(&self.name, integrations)
    }
}

impl WorkerTrigger {
    /// Validate the value carried by one trigger kind.
    fn validate(&self, worker: &str) -> Result<(), ManifestError> {
        match self {
            Self::Event(trigger) => validate_resource("event", &trigger.event),
            Self::Cron(trigger) => {
                if trigger.cron.split_whitespace().count() != 5 {
                    validation(format!(
                        "Worker `{worker}` cron trigger must contain five fields"
                    ))
                } else {
                    Ok(())
                }
            }
            Self::Table(trigger) => validate_resource("table", &trigger.table),
            Self::Webhook(trigger) => {
                if trigger.webhook.starts_with('/')
                    && !trigger.webhook.contains(char::is_whitespace)
                {
                    Ok(())
                } else {
                    validation(format!(
                        "Worker `{worker}` webhook trigger must be an absolute path"
                    ))
                }
            }
        }
    }
}

impl ComponentGrants {
    /// Validate resource grants and Integration component references.
    fn validate(
        &self,
        component: &str,
        integrations: &BTreeSet<&str>,
    ) -> Result<(), ManifestError> {
        validate_grant_set("table", &self.tables)?;
        validate_grant_set("vector", &self.vectors)?;
        validate_grant_set("graph", &self.graphs)?;
        let mut names = BTreeSet::new();
        for integration in &self.integrations {
            if !integrations.contains(integration.as_str()) {
                return validation(format!(
                    "component `{component}` references missing integration `{integration}`"
                ));
            }
            if !names.insert(integration) {
                return validation(format!(
                    "component `{component}` repeats integration grant `{integration}`"
                ));
            }
        }
        Ok(())
    }
}

/// Validate a component's common immutable identity fields.
fn validate_component_identity(
    kind: &str,
    name: &str,
    version: &str,
    project: &str,
) -> Result<(), ManifestError> {
    validate_name(&format!("{kind} name"), name)?;
    validate_version(&format!("{kind} `{name}`"), version)?;
    validate_project_path(&format!("{kind} `{name}`"), project)
}

/// Add a component name to the Vessel-wide namespace.
fn insert_component_name<'a>(
    names: &mut BTreeSet<&'a str>,
    name: &'a str,
) -> Result<(), ManifestError> {
    if names.insert(name) {
        Ok(())
    } else {
        validation(format!("duplicate component name `{name}`"))
    }
}

/// Validate a lowercase DNS-label identifier.
fn validate_name(field: &str, name: &str) -> Result<(), ManifestError> {
    let valid = !name.is_empty()
        && name.len() <= 63
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && name
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && name
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric);
    if valid {
        Ok(())
    } else {
        validation(format!("{field} `{name}` must be a lowercase DNS label"))
    }
}

/// Validate an opaque, printable source version.
fn validate_version(owner: &str, version: &str) -> Result<(), ManifestError> {
    let valid = !version.is_empty()
        && version.len() <= 128
        && version.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'+' | b':')
        });
    if valid {
        Ok(())
    } else {
        validation(format!("{owner} version `{version}` is invalid"))
    }
}

/// Validate a safe project directory relative to the Vessel source root.
fn validate_project_path(owner: &str, project: &str) -> Result<(), ManifestError> {
    let path = Path::new(project);
    let valid = !project.is_empty()
        && project.len() <= 255
        && !project.contains('\\')
        && path.components().all(
            |component| matches!(component, PathComponent::Normal(segment) if !segment.is_empty()),
        );
    if valid {
        Ok(())
    } else {
        validation(format!(
            "{owner} project `{project}` must be a safe relative directory"
        ))
    }
}

/// Validate a qualified lakehouse or event resource name.
fn validate_resource(kind: &str, resource: &str) -> Result<(), ManifestError> {
    let valid = !resource.is_empty()
        && resource.len() <= 255
        && resource.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b':' | b'/')
        });
    if valid {
        Ok(())
    } else {
        validation(format!("invalid {kind} resource `{resource}`"))
    }
}

/// Validate and deduplicate grants for one resource kind.
fn validate_grant_set(kind: &str, grants: &[DataGrant]) -> Result<(), ManifestError> {
    let mut entries = BTreeSet::new();
    for grant in grants {
        let (access, resource) = match grant {
            DataGrant::Read(grant) => ("read", grant.read.as_str()),
            DataGrant::Write(grant) => ("write", grant.write.as_str()),
        };
        validate_resource(kind, resource)?;
        if !entries.insert((access, resource)) {
            return validation(format!("duplicate {access} {kind} grant `{resource}`"));
        }
    }
    Ok(())
}

/// Construct a semantic validation error without repeating its concrete type.
fn validation<T>(message: impl Into<String>) -> Result<T, ManifestError> {
    Err(ManifestError::Validation(message.into()))
}
