//! Portable desired-state contract for secure Verglas microVM dependency graphs.

use std::collections::{BTreeMap, BTreeSet};

use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ts_rs::TS;

/// The supported contract API version.
pub const API_VERSION: &str = "verglas.io/v1alpha1";

/// The supported manifest kind.
pub const KIND: &str = "MicroVMStack";

/// A complete portable microVM dependency graph.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct MicroVmStack {
    /// Contract API version. Must be `verglas.io/v1alpha1`.
    #[serde(rename = "apiVersion")]
    #[ts(rename = "apiVersion")]
    pub api_version: String,
    /// Manifest kind. Must be `MicroVMStack`.
    pub kind: String,
    /// Tenant identity and isolation network.
    pub tenant: Tenant,
    /// The sole platform-callable entry point for a dormant stack.
    pub ingress: Ingress,
    /// Desired microVM components and their dependency graph.
    pub components: Vec<Component>,
}

/// Tenant-scoped runtime identity and network isolation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct Tenant {
    /// Stable tenant runtime name.
    pub name: String,
    /// Tenant network implementation.
    pub network: TenantNetwork,
}

/// Supported tenant network implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(rename_all = "lowercase")]
#[ts(rename_all = "lowercase")]
pub enum TenantNetwork {
    /// A tenant-specific VXLAN shared by every component instance.
    Vxlan,
}

/// The single authenticated platform ingress into a dormant stack.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct Ingress {
    /// Component receiving platform traffic.
    pub component: String,
    /// Named port on the ingress component.
    pub port: String,
}

/// One microVM component in the desired dependency graph.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct Component {
    /// Unique name used for dependency and network resolution.
    pub name: String,
    /// Bootable runtime image.
    pub runtime: Runtime,
    /// Process and arguments started inside the microVM.
    pub exec: Vec<String>,
    /// Optional fixed-size quorum cluster.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub cluster: Option<Cluster>,
    /// Requested microVM compute resources.
    pub resources: Resources,
    /// Optional named ports provided inside the tenant network.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub network: Option<ComponentNetwork>,
    /// Optional readiness condition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub health: Option<Health>,
    /// Components that must be healthy before this component starts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub depends_on: Option<Vec<String>>,
}

/// Immutable runtime root filesystem stored in the platform R2 bucket.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct Runtime {
    /// Exact R2 object key for the root filesystem image.
    pub object: String,
    /// Lowercase hexadecimal SHA-256 digest of the object contents.
    pub sha256: String,
}

/// Fixed-size quorum cluster configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct Cluster {
    /// Number of stable ordinal members in the cluster.
    pub members: u16,
}

/// Requested microVM compute resources.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct Resources {
    /// Number of virtual CPUs.
    pub vcpus: u16,
    /// Memory allocation in mebibytes.
    #[serde(rename = "memoryMiB")]
    #[ts(rename = "memoryMiB")]
    pub memory_mib: u32,
}

/// Named ports provided by one component.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct ComponentNetwork {
    /// Ports addressable by dependencies or stack ingress.
    pub ports: Vec<NetworkPort>,
}

/// One named component port.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct NetworkPort {
    /// Stable port name used by health and ingress references.
    pub name: String,
    /// TCP port number.
    pub port: u16,
    /// Protocol served on this port.
    pub protocol: NetworkProtocol,
}

/// Supported component network protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(rename_all = "lowercase")]
#[ts(rename_all = "lowercase")]
pub enum NetworkProtocol {
    /// Raw TCP traffic.
    Tcp,
    /// HTTP traffic over TCP.
    Http,
}

/// Component readiness declaration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, TS)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase")]
pub struct Health {
    /// Named port that must accept traffic.
    pub port: String,
    /// Optional HTTP readiness path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub path: Option<String>,
}

/// Manifest decoding or semantic validation failure.
#[derive(Debug, Error)]
pub enum ManifestError {
    /// YAML did not match the published contract shape.
    #[error("invalid MicroVMStack YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    /// YAML decoded but violated cross-field contract semantics.
    #[error("invalid MicroVMStack: {0}")]
    Validation(String),
    /// A generated consumer artifact could not be serialized.
    #[error("could not generate MicroVMStack artifact: {0}")]
    Artifact(#[from] serde_json::Error),
}

/// Decode and semantically validate a `MicroVMStack` YAML document.
pub fn parse_manifest(yaml: &str) -> Result<MicroVmStack, ManifestError> {
    let stack: MicroVmStack = serde_yaml::from_str(yaml)?;
    stack.validate()?;
    Ok(stack)
}

impl MicroVmStack {
    /// Validate all invariants that JSON Schema alone cannot express.
    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.api_version != API_VERSION {
            return validation(format!("apiVersion must be {API_VERSION}"));
        }
        if self.kind != KIND {
            return validation(format!("kind must be {KIND}"));
        }
        validate_name("tenant.name", &self.tenant.name)?;
        if self.components.is_empty() {
            return validation("components must contain at least one component");
        }

        let mut components = BTreeMap::new();
        for component in &self.components {
            component.validate()?;
            if components
                .insert(component.name.as_str(), component)
                .is_some()
            {
                return validation(format!("duplicate component name `{}`", component.name));
            }
        }

        for component in &self.components {
            let mut dependencies = BTreeSet::new();
            for dependency in component.depends_on.iter().flatten() {
                if dependency == &component.name {
                    return validation(format!("component `{}` depends on itself", component.name));
                }
                if !components.contains_key(dependency.as_str()) {
                    return validation(format!(
                        "component `{}` depends on missing component `{dependency}`",
                        component.name
                    ));
                }
                if !dependencies.insert(dependency) {
                    return validation(format!(
                        "component `{}` repeats dependency `{dependency}`",
                        component.name
                    ));
                }
            }
        }

        validate_acyclic(&components)?;
        let ingress = components
            .get(self.ingress.component.as_str())
            .ok_or_else(|| {
                ManifestError::Validation(format!(
                    "ingress references missing component `{}`",
                    self.ingress.component
                ))
            })?;
        if !ingress.has_port(&self.ingress.port) {
            return validation(format!(
                "ingress port `{}` is not declared by component `{}`",
                self.ingress.port, self.ingress.component
            ));
        }
        Ok(())
    }

    /// Generate the canonical JSON Schema consumer artifact.
    pub fn json_schema_pretty() -> Result<String, ManifestError> {
        Ok(serde_json::to_string_pretty(&schema_for!(MicroVmStack))?)
    }

    /// Generate TypeScript declarations for non-Rust consumers.
    pub fn typescript_declarations() -> String {
        let declarations = [
            TenantNetwork::decl(),
            NetworkProtocol::decl(),
            Tenant::decl(),
            Ingress::decl(),
            Runtime::decl(),
            Cluster::decl(),
            Resources::decl(),
            NetworkPort::decl(),
            ComponentNetwork::decl(),
            Health::decl(),
            Component::decl(),
            MicroVmStack::decl(),
        ];
        let declarations =
            declarations.map(|declaration| declaration.replacen("type ", "export type ", 1));
        format!(
            concat!(
                "// Generated by verglas-microvm-contract. Do not edit.\n\n",
                "{}\n\n",
                "/** Parse and validate a MicroVMStack YAML document. */\n",
                "export declare function parseManifest(yaml: string): MicroVmStack;\n"
            ),
            declarations.join("\n\n")
        )
    }
}

impl Component {
    /// Validate component-local fields and references.
    fn validate(&self) -> Result<(), ManifestError> {
        validate_name("component.name", &self.name)?;
        validate_r2_object(&self.runtime.object)?;
        validate_sha256(&self.runtime.sha256)?;
        if self.exec.is_empty() || self.exec.iter().any(String::is_empty) {
            return validation(format!("component `{}` exec must not be empty", self.name));
        }
        if self.resources.vcpus == 0 || self.resources.memory_mib == 0 {
            return validation(format!(
                "component `{}` resources must be greater than zero",
                self.name
            ));
        }
        if self
            .cluster
            .as_ref()
            .is_some_and(|cluster| cluster.members == 0)
        {
            return validation(format!(
                "component `{}` cluster.members must be greater than zero",
                self.name
            ));
        }

        let mut names = BTreeSet::new();
        let mut numbers = BTreeSet::new();
        if let Some(network) = &self.network {
            if network.ports.is_empty() {
                return validation(format!(
                    "component `{}` network.ports must not be empty",
                    self.name
                ));
            }
            for port in &network.ports {
                validate_name("network port name", &port.name)?;
                if port.port == 0 {
                    return validation(format!(
                        "component `{}` network port must be greater than zero",
                        self.name
                    ));
                }
                if !names.insert(port.name.as_str()) {
                    return validation(format!(
                        "component `{}` repeats network port `{}`",
                        self.name, port.name
                    ));
                }
                if !numbers.insert(port.port) {
                    return validation(format!(
                        "component `{}` repeats network port number `{}`",
                        self.name, port.port
                    ));
                }
            }
        }
        if let Some(health) = &self.health {
            if !self.has_port(&health.port) {
                return validation(format!(
                    "component `{}` health.port `{}` is not declared",
                    self.name, health.port
                ));
            }
            if let Some(path) = &health.path
                && (!path.starts_with('/') || path.contains(char::is_whitespace))
            {
                return validation(format!(
                    "component `{}` health.path must be an absolute HTTP path",
                    self.name
                ));
            }
        }
        Ok(())
    }

    /// Return whether the component declares a port with the given name.
    fn has_port(&self, name: &str) -> bool {
        self.network
            .as_ref()
            .is_some_and(|network| network.ports.iter().any(|port| port.name == name))
    }
}

/// Validate a contract identifier.
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

/// Validate a safe, exact R2 object key for a Firecracker root filesystem.
fn validate_r2_object(object: &str) -> Result<(), ManifestError> {
    let valid = !object.is_empty()
        && !object.starts_with('/')
        && object.ends_with("/rootfs.ext4")
        && !object.contains(char::is_whitespace)
        && !object.contains(['\\', '?', '#'])
        && object
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..");
    if valid {
        Ok(())
    } else {
        validation(format!(
            "runtime.object `{object}` must be a relative R2 key ending in /rootfs.ext4"
        ))
    }
}

/// Validate a canonical SHA-256 digest.
fn validate_sha256(digest: &str) -> Result<(), ManifestError> {
    let valid = digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase());
    if valid {
        Ok(())
    } else {
        validation(format!(
            "runtime.sha256 `{digest}` must be 64 lowercase hexadecimal characters"
        ))
    }
}

/// Validate that component dependencies form a directed acyclic graph.
fn validate_acyclic(components: &BTreeMap<&str, &Component>) -> Result<(), ManifestError> {
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for name in components.keys() {
        visit(name, components, &mut visiting, &mut visited)?;
    }
    Ok(())
}

/// Depth-first traversal used for dependency cycle detection.
fn visit<'a>(
    name: &'a str,
    components: &BTreeMap<&'a str, &'a Component>,
    visiting: &mut BTreeSet<&'a str>,
    visited: &mut BTreeSet<&'a str>,
) -> Result<(), ManifestError> {
    if visited.contains(name) {
        return Ok(());
    }
    if !visiting.insert(name) {
        return validation(format!("dependency cycle includes component `{name}`"));
    }
    if let Some(component) = components.get(name) {
        for dependency in component.depends_on.iter().flatten() {
            visit(dependency, components, visiting, visited)?;
        }
    }
    visiting.remove(name);
    visited.insert(name);
    Ok(())
}

/// Construct a semantic validation error without repeating its concrete type.
fn validation<T>(message: impl Into<String>) -> Result<T, ManifestError> {
    Err(ManifestError::Validation(message.into()))
}
