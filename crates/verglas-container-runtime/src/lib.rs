//! Places open-source Verglas workloads on an operator-owned Docker Engine.
//!
//! The Docker daemon is part of the trusted local control plane. Container
//! specifications cannot forward its socket or client configuration into a
//! workload. Verglas-owned labels provide the authority for every mutation.

use std::collections::{BTreeMap, HashMap};
use std::io::Read as _;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bollard::Docker;
use bollard::body_full;
use bollard::errors::Error as BollardError;
use bollard::models::{
    ContainerCreateBody, HostConfig, NetworkCreateRequest, PortBinding as DockerPortBinding,
};
use bollard::query_parameters::{
    BuildImageOptionsBuilder, CreateContainerOptionsBuilder, CreateImageOptionsBuilder,
    DownloadFromContainerOptionsBuilder, ListContainersOptionsBuilder, LogsOptionsBuilder,
    UploadToContainerOptionsBuilder, WaitContainerOptionsBuilder,
};
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

mod composition;
mod service;

pub use composition::{
    AppliedComponent, AppliedComponentKind, AppliedIntegration, AppliedVessel, CompositionError,
    VesselApplyPlan, VesselApplyRequest, WorkerRegistration,
};
pub use service::{RuntimeService, ServiceError};

/// Label marking a container as owned by the Verglas runtime.
pub const LABEL_MANAGED: &str = "io.verglas.managed";

/// Label holding the stable Verglas deployment identity.
pub const LABEL_DEPLOYMENT: &str = "io.verglas.deployment";

/// Label holding the digest of the immutable runtime specification.
pub const LABEL_SPEC_DIGEST: &str = "io.verglas.spec-sha256";

const CONTAINER_NAME_PREFIX: &str = "verglas-";
const TYPESCRIPT_BASE_IMAGE: &str = "oven/bun:1.3.8";
const PYTHON_BASE_IMAGE: &str = "python:3.13-slim";
const UV_BASE_IMAGE: &str = "ghcr.io/astral-sh/uv:0.9.17";
const MAX_PROJECT_FILES: usize = 128;
const MAX_PROJECT_FILE_BYTES: usize = 512 * 1024;
const MAX_PROJECT_BYTES: usize = 2 * 1024 * 1024;
const MAX_BUILD_ERROR_BYTES: usize = 8 * 1024;
const MAX_CONTAINER_FILES: usize = 64;
const MAX_CONTAINER_FILE_BYTES: usize = 1024 * 1024;
const MAX_WORKER_LOG_BYTES: usize = 64 * 1024;
const WORKER_RESULT_PATH: &str = "/tmp/verglas-result.json";

fn valid_absolute_container_path(path: &str) -> bool {
    path.starts_with('/')
        && path.len() > 1
        && path
            .trim_start_matches('/')
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

/// Stable local TLS paths generated beside the desired-state document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresTlsFiles {
    /// PEM certificate suitable for the local PostgreSQL proxy listener.
    pub certificate: PathBuf,
    /// PEM private key paired with the local certificate.
    pub private_key: PathBuf,
}

/// Creates the local self-signed PostgreSQL proxy identity once and reuses it.
pub fn ensure_local_postgres_tls(state_path: &Path) -> Result<PostgresTlsFiles, RuntimeError> {
    let directory = state_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("postgres-proxy");
    let files = PostgresTlsFiles {
        certificate: directory.join("tls.crt"),
        private_key: directory.join("tls.key"),
    };
    match (files.certificate.exists(), files.private_key.exists()) {
        (true, true) => return Ok(files),
        (true, false) | (false, true) => {
            return Err(RuntimeError::TlsIdentity(
                "local Postgres TLS identity is incomplete".to_owned(),
            ));
        }
        (false, false) => {}
    }
    std::fs::create_dir_all(&directory)
        .map_err(|error| RuntimeError::TlsIdentity(error.to_string()))?;
    let rcgen::CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
            .map_err(|error| RuntimeError::TlsIdentity(error.to_string()))?;
    write_private_file(&files.private_key, key_pair.serialize_pem().as_bytes())?;
    write_private_file(&files.certificate, cert.pem().as_bytes())?;
    Ok(files)
}

/// Atomically installs one generated private file with owner-only permissions.
fn write_private_file(path: &Path, contents: &[u8]) -> Result<(), RuntimeError> {
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, contents)
        .map_err(|error| RuntimeError::TlsIdentity(error.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))
            .map_err(|error| RuntimeError::TlsIdentity(error.to_string()))?;
    }
    std::fs::rename(&temporary, path).map_err(|error| RuntimeError::TlsIdentity(error.to_string()))
}

/// Product role assigned to one long-lived local Vessel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VesselRole {
    /// An API connection to an external application.
    Integration,
    /// A full-stack application over Verglas data or integrations.
    Application,
}

/// Internal HTTP endpoint exposed by a Vessel on the shared runtime network.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VesselHttp {
    /// TCP port listened to inside the Vessel container.
    pub port: u16,
    /// Optional readiness endpoint relative to the Vessel origin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_path: Option<String>,
}

/// Desired declaration for one isolated long-lived HTTP service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VesselSpec {
    /// Stable local name used by the CLI and runtime proxy.
    pub name: String,
    /// Product behavior exposed by this Vessel.
    pub role: VesselRole,
    /// OCI image containing the service.
    pub image: String,
    /// Optional command overriding the image command.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    /// Optional executable overriding the image entrypoint.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entrypoint: Vec<String>,
    /// Non-secret service configuration.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
    /// Private HTTP service contract.
    pub http: VesselHttp,
}

/// A bounded multi-file TypeScript project compiled into one Vessel image.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TypescriptProject {
    /// UTF-8 project files keyed by relative POSIX path.
    pub files: BTreeMap<String, String>,
}

/// Desired standalone Vessel project and its runtime configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VesselProjectSpec {
    /// Stable local name used by the CLI, image tag, and runtime proxy.
    pub name: String,
    /// Product behavior exposed by the built Vessel.
    pub role: VesselRole,
    /// TypeScript source and dependency declaration.
    pub project: TypescriptProject,
    /// Non-secret configuration injected only when the built image starts.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
    /// Private HTTP service contract.
    pub http: VesselHttp,
}

/// Normalized content-addressed OCI build shared by Vessels and workers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectBuildContext {
    /// Immutable local OCI image tag derived from the normalized project.
    pub image: String,
    /// SHA-256 digest of the exact deterministic build context.
    pub image_digest: String,
    /// Platform-owned Dockerfile included in the build context.
    pub dockerfile: String,
    /// Deterministic uncompressed tar archive sent to the Docker Engine.
    pub context: Vec<u8>,
}

/// Platform-owned build policy selected for one bounded worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerRuntime {
    /// Python dependencies resolved exactly from `pyproject.toml` and `uv.lock`.
    Python,
    /// JavaScript or TypeScript dependencies resolved exactly from `package.json` and `bun.lock`.
    Bun,
    /// An operator-auditable Dockerfile supplied with the worker project.
    Container,
}

/// One dependency-bearing bounded worker project.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerProjectSpec {
    /// Stable worker name used in the immutable image tag.
    pub name: String,
    /// Platform build policy or explicit container build.
    pub runtime: WorkerRuntime,
    /// Command executed for each trigger invocation.
    pub entrypoint: Vec<String>,
    /// UTF-8 source, dependency declarations, and lockfiles.
    pub files: BTreeMap<String, String>,
}

/// One fully resolved bounded worker invocation accepted by the runtime.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerInvocation {
    /// Unique run identity. It also names the ephemeral Docker container.
    pub run_id: String,
    /// Stable worker deployment name exposed to the worker SDK.
    pub worker: String,
    /// Immutable image produced from the registered worker project.
    pub image: String,
    /// Command and arguments executed inside the image.
    pub entrypoint: Vec<String>,
    /// Resolved runtime environment, including secret values.
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    /// Configured output table exposed as `TARGET`.
    #[serde(default)]
    pub target: String,
    /// Verglas data endpoint exposed to the worker SDK.
    pub endpoint: String,
    /// Short-lived data-plane bearer token.
    #[serde(default)]
    pub token: String,
    /// Existing Docker network used to reach Verglas and bound services.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<String>,
    /// Complete CloudEvents 1.0 envelope for this invocation.
    pub event: serde_json::Value,
    /// Hard cgroup ceilings applied by Docker.
    pub resources: WorkerResources,
    /// Wall-clock ceiling for the invocation.
    pub timeout_seconds: u64,
    /// Optional absolute container path backed by operator-owned scratch storage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scratch_target: Option<String>,
}

/// Completed output returned from one bounded worker container.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerRunResult {
    /// Rows reported by the worker SDK result contract.
    pub rows_produced: u64,
    /// Bounded combined stdout and stderr captured for run diagnostics.
    pub logs: String,
}

#[derive(Debug, Deserialize)]
struct WorkerResultFile {
    rows: u64,
    error: Option<String>,
}

impl WorkerProjectSpec {
    /// Validates dependency locking and archives one deterministic worker build.
    pub fn build_context(&self) -> Result<ProjectBuildContext, RuntimeError> {
        ContainerSpec::new(format!("worker-{}", self.name), "validation").validate()?;
        if self.entrypoint.is_empty() || self.entrypoint.iter().any(|part| part.is_empty()) {
            return Err(RuntimeError::MissingWorkerEntrypoint);
        }
        validate_project_files(&self.files, self.runtime == WorkerRuntime::Container)?;
        let dockerfile = match self.runtime {
            WorkerRuntime::Python => {
                require_project_file(&self.files, "pyproject.toml")?;
                require_project_file(&self.files, "uv.lock")?;
                python_worker_dockerfile()
            }
            WorkerRuntime::Bun => {
                let package = require_project_file(&self.files, "package.json")?;
                serde_json::from_str::<serde_json::Value>(package)
                    .map_err(|error| RuntimeError::InvalidPackageJson(error.to_string()))?;
                require_project_file(&self.files, "bun.lock")?;
                bun_worker_dockerfile()
            }
            WorkerRuntime::Container => require_project_file(&self.files, "Dockerfile")?.to_owned(),
        };
        let mut files = self.files.clone();
        files.remove("Dockerfile");
        build_project_context(ProjectKind::Worker, &self.name, &files, dockerfile)
    }
}

/// Returns one required project file or a stable validation error.
fn require_project_file<'a>(
    files: &'a BTreeMap<String, String>,
    path: &str,
) -> Result<&'a str, RuntimeError> {
    files
        .get(path)
        .filter(|source| !source.trim().is_empty())
        .map(String::as_str)
        .ok_or_else(|| RuntimeError::MissingProjectFile {
            path: path.to_owned(),
        })
}

/// Returns the locked Python worker image policy.
fn python_worker_dockerfile() -> String {
    format!(
        "FROM {UV_BASE_IMAGE} AS uv\nFROM {PYTHON_BASE_IMAGE}\nCOPY --from=uv /uv /uvx /bin/\nWORKDIR /app\nCOPY pyproject.toml uv.lock ./\nRUN uv sync --frozen --no-dev\nCOPY . .\nENV PATH=\"/app/.venv/bin:$PATH\" PYTHONUNBUFFERED=1\n"
    )
}

/// Returns the locked Bun worker image policy for JavaScript and TypeScript.
fn bun_worker_dockerfile() -> String {
    format!(
        "FROM {TYPESCRIPT_BASE_IMAGE}\nWORKDIR /app\nCOPY package.json bun.lock ./\nRUN bun install --frozen-lockfile --production\nCOPY . .\nUSER bun\n"
    )
}

impl VesselProjectSpec {
    /// Validates and archives this project into a platform-owned Docker build.
    pub fn build_context(&self) -> Result<ProjectBuildContext, RuntimeError> {
        ContainerSpec::new(format!("vessel-{}", self.name), "validation").validate()?;
        if self.http.port == 0 {
            return Err(RuntimeError::InvalidPort);
        }
        if self
            .http
            .health_path
            .as_ref()
            .is_some_and(|path| !path.starts_with('/'))
        {
            return Err(RuntimeError::InvalidHealthPath);
        }
        validate_project_files(&self.project.files, false)?;
        validate_package_json(self.project.files.get("package.json").ok_or_else(|| {
            RuntimeError::MissingProjectFile {
                path: "package.json".to_owned(),
            }
        })?)?;

        let dockerfile = typescript_dockerfile();
        let files = materialize_project(&self.project.files, self.role)?;
        build_project_context(ProjectKind::Vessel, &self.name, &files, dockerfile)
    }

    /// Maps a completed project build to the existing Vessel runtime record.
    pub fn vessel_spec(&self, image: impl Into<String>) -> VesselSpec {
        VesselSpec {
            name: self.name.clone(),
            role: self.role,
            image: image.into(),
            command: Vec::new(),
            entrypoint: Vec::new(),
            environment: self.environment.clone(),
            http: self.http.clone(),
        }
    }
}

/// Product shape that selects the stable OCI repository namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectKind {
    /// A long-running Vessel service.
    Vessel,
    /// A bounded worker invocation image.
    Worker,
}

impl ProjectKind {
    /// Returns the image repository prefix for this product shape.
    fn image_prefix(self) -> &'static str {
        match self {
            Self::Vessel => "vessel",
            Self::Worker => "worker",
        }
    }
}

/// Creates one deterministic build context and content identity for any project.
fn build_project_context(
    kind: ProjectKind,
    name: &str,
    files: &BTreeMap<String, String>,
    dockerfile: String,
) -> Result<ProjectBuildContext, RuntimeError> {
    let context = archive_project(files, &dockerfile)?;
    let digest = hex::encode(Sha256::digest(&context));
    Ok(ProjectBuildContext {
        image: format!("verglas/{}-{}:sha256-{digest}", kind.image_prefix(), name),
        image_digest: format!("sha256:{digest}"),
        dockerfile,
        context,
    })
}

/// Adds the platform SDK as an ordinary local package inside the standalone image.
fn materialize_project(
    submitted: &BTreeMap<String, String>,
    role: VesselRole,
) -> Result<BTreeMap<String, String>, RuntimeError> {
    let mut files = submitted.clone();
    let source = files
        .get("package.json")
        .ok_or_else(|| RuntimeError::MissingProjectFile {
            path: "package.json".to_owned(),
        })?;
    let mut package: serde_json::Value = serde_json::from_str(source)
        .map_err(|error| RuntimeError::InvalidPackageJson(error.to_string()))?;
    let dependencies = package
        .as_object_mut()
        .ok_or_else(|| RuntimeError::InvalidPackageJson("root must be an object".to_owned()))?
        .entry("dependencies")
        .or_insert_with(|| serde_json::json!({}));
    let dependencies = dependencies.as_object_mut().ok_or_else(|| {
        RuntimeError::InvalidPackageJson("dependencies must be an object".to_owned())
    })?;
    dependencies.insert(
        "@verglas/sdk".to_owned(),
        serde_json::Value::String("file:vendor/verglas-sdk".to_owned()),
    );
    files.insert(
        "package.json".to_owned(),
        serde_json::to_string_pretty(&package)
            .map_err(|error| RuntimeError::SpecificationEncoding(error.to_string()))?,
    );
    files.insert(
        "vendor/verglas-sdk/package.json".to_owned(),
        r#"{"name":"@verglas/sdk","version":"0.0.0","type":"module","main":"./src/index.ts","exports":{".":"./src/index.ts","./logging":"./src/logging.ts","./examples":"./src/examples/index.ts"},"dependencies":{"apache-arrow":"21.2.0"}}"#.to_owned(),
    );
    for (path, source) in TYPESCRIPT_SDK_FILES {
        files.insert(
            format!("vendor/verglas-sdk/src/{path}"),
            (*source).to_owned(),
        );
    }
    if role == VesselRole::Integration {
        files.insert(
            "vendor/verglas-integration-runtime/runtime.mjs".to_owned(),
            include_str!("../../verglas-integration-runtime/runtime.mjs").to_owned(),
        );
        files.insert(
            "vendor/verglas-integration-runtime/contract.mjs".to_owned(),
            include_str!("../../verglas-integration-runtime/contract.mjs").to_owned(),
        );
        for (path, source) in TYPESCRIPT_SDK_FILES {
            files.insert(
                format!("vendor/verglas-integration-runtime/sdk/{path}"),
                (*source).to_owned(),
            );
        }
    }
    Ok(files)
}

const TYPESCRIPT_SDK_FILES: &[(&str, &str)] = &[
    (
        "client.ts",
        include_str!("../../../sdks/typescript/src/client.ts"),
    ),
    (
        "contracts.ts",
        include_str!("../../../sdks/typescript/src/contracts.ts"),
    ),
    (
        "feed.ts",
        include_str!("../../../sdks/typescript/src/feed.ts"),
    ),
    (
        "http.ts",
        include_str!("../../../sdks/typescript/src/http.ts"),
    ),
    (
        "index.ts",
        include_str!("../../../sdks/typescript/src/index.ts"),
    ),
    (
        "logging.ts",
        include_str!("../../../sdks/typescript/src/logging.ts"),
    ),
    (
        "namespace.ts",
        include_str!("../../../sdks/typescript/src/namespace.ts"),
    ),
    (
        "types.ts",
        include_str!("../../../sdks/typescript/src/types.ts"),
    ),
    (
        "examples/change-fanout-worker.ts",
        include_str!("../../../sdks/typescript/src/examples/change-fanout-worker.ts"),
    ),
    (
        "examples/http-poll-worker.ts",
        include_str!("../../../sdks/typescript/src/examples/http-poll-worker.ts"),
    ),
    (
        "examples/index.ts",
        include_str!("../../../sdks/typescript/src/examples/index.ts"),
    ),
    (
        "examples/webhook-worker.ts",
        include_str!("../../../sdks/typescript/src/examples/webhook-worker.ts"),
    ),
];

/// Validates bounded safe paths before creating an engine build context.
fn validate_project_files(
    files: &BTreeMap<String, String>,
    allow_dockerfile: bool,
) -> Result<(), RuntimeError> {
    if files.len() > MAX_PROJECT_FILES {
        return Err(RuntimeError::ProjectTooLarge);
    }
    let mut total = 0usize;
    for (path, source) in files {
        let valid_path = !path.is_empty()
            && !path.starts_with('/')
            && !path.ends_with('/')
            && path.split('/').all(|part| {
                !part.is_empty() && part != "." && part != ".." && !part.contains('\\')
            })
            && (allow_dockerfile || path != "Dockerfile")
            && path != ".dockerignore";
        if !valid_path {
            return Err(RuntimeError::InvalidProjectPath { path: path.clone() });
        }
        if source.len() > MAX_PROJECT_FILE_BYTES {
            return Err(RuntimeError::ProjectTooLarge);
        }
        total = total
            .saturating_add(path.len())
            .saturating_add(source.len());
        if total > MAX_PROJECT_BYTES {
            return Err(RuntimeError::ProjectTooLarge);
        }
    }
    Ok(())
}

/// Requires the standard start contract used by the generated runtime image.
fn validate_package_json(source: &str) -> Result<(), RuntimeError> {
    let package: serde_json::Value = serde_json::from_str(source)
        .map_err(|error| RuntimeError::InvalidPackageJson(error.to_string()))?;
    if package
        .pointer("/scripts/start")
        .and_then(serde_json::Value::as_str)
        .is_none_or(str::is_empty)
    {
        return Err(RuntimeError::MissingStartScript);
    }
    Ok(())
}

/// Returns the fixed TypeScript build policy owned by the local runtime.
fn typescript_dockerfile() -> String {
    format!(
        "FROM {TYPESCRIPT_BASE_IMAGE}\nWORKDIR /app\nCOPY package.json ./\nCOPY . .\nRUN bun install\nRUN bun run --if-present build\nRUN bun install --production\nENV NODE_ENV=production\nCMD [\"bun\", \"run\", \"start\"]\n"
    )
}

/// Creates a deterministic tar archive without accepting filesystem objects.
fn archive_project(
    files: &BTreeMap<String, String>,
    dockerfile: &str,
) -> Result<Vec<u8>, RuntimeError> {
    let mut context = Vec::new();
    {
        let mut archive = tar::Builder::new(&mut context);
        append_archive_file(&mut archive, "Dockerfile", dockerfile.as_bytes())?;
        for (path, source) in files {
            append_archive_file(&mut archive, path, source.as_bytes())?;
        }
        archive
            .finish()
            .map_err(|error| RuntimeError::BuildContext(error.to_string()))?;
    }
    Ok(context)
}

/// Appends one regular file with stable metadata to a Docker build archive.
fn append_archive_file<W: std::io::Write>(
    archive: &mut tar::Builder<W>,
    path: &str,
    contents: &[u8],
) -> Result<(), RuntimeError> {
    let mut header = tar::Header::new_gnu();
    header.set_size(contents.len() as u64);
    header.set_mode(0o644);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_cksum();
    archive
        .append_data(&mut header, path, contents)
        .map_err(|error| RuntimeError::BuildContext(error.to_string()))
}

impl VesselSpec {
    /// Validates the Vessel boundary and maps it to one unpublished container.
    pub fn container_spec(&self) -> Result<ContainerSpec, RuntimeError> {
        if self.http.port == 0 {
            return Err(RuntimeError::InvalidPort);
        }
        if self
            .http
            .health_path
            .as_ref()
            .is_some_and(|path| !path.starts_with('/'))
        {
            return Err(RuntimeError::InvalidHealthPath);
        }
        let mut container = ContainerSpec::new(format!("vessel-{}", self.name), &self.image)
            .with_command(self.command.clone())
            .with_entrypoint(self.entrypoint.clone());
        container.environment = self.environment.clone();
        container.validate()?;
        Ok(container)
    }
}

/// One host path exposed inside a managed workload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindMount {
    /// Path on the Docker Engine host.
    pub source: String,
    /// Absolute path visible inside the workload.
    pub target: String,
}

/// One TCP port published from a managed container to the local host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedPort {
    /// TCP port listened to inside the container.
    pub container_port: u16,
    /// Fixed TCP host port, or `None` when Docker must assign a free loopback port.
    pub host_port: Option<u16>,
}

/// One immutable regular file materialized before a managed workload starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerFile {
    /// Absolute source path readable by the trusted runtime manager.
    pub source: String,
    /// Absolute path inside the container.
    pub path: String,
    /// Unix permission bits without executable or special bits.
    pub mode: u32,
}

/// Hard Docker limits applied to one bounded worker invocation.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerResources {
    /// Maximum CPU bandwidth expressed as fractional host CPUs.
    pub vcpus: f64,
    /// Maximum resident memory in mebibytes.
    pub memory_mib: u64,
    /// Maximum number of processes visible to the worker cgroup.
    pub pids: i64,
}

/// Immutable declaration for one locally placed container.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerSpec {
    /// Stable deployment identity used to derive the Docker container name.
    pub deployment_id: String,
    /// OCI image reference supplied to the Docker Engine.
    pub image: String,
    /// Optional OCI operating-system and architecture pair.
    #[serde(default)]
    pub platform: Option<String>,
    /// Optional command overriding the image command.
    #[serde(default)]
    pub command: Vec<String>,
    /// Optional executable overriding the image entrypoint.
    #[serde(default)]
    pub entrypoint: Vec<String>,
    /// Workload environment sorted for deterministic hashing.
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    /// Immutable files copied into the stopped container before first start.
    #[serde(default)]
    pub files: Vec<ContainerFile>,
    /// Explicit host bind mounts sorted in declaration order.
    #[serde(default)]
    pub bind_mounts: Vec<BindMount>,
    /// Docker network shared with declared local dependencies.
    #[serde(default)]
    pub network: Option<String>,
    /// TCP ports explicitly published to the Docker Engine host.
    #[serde(default)]
    pub published_ports: Vec<PublishedPort>,
    /// Optional hard cgroup limits for bounded invocations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<WorkerResources>,
}

impl ContainerSpec {
    /// Creates a minimal immutable deployment specification.
    pub fn new(deployment_id: impl Into<String>, image: impl Into<String>) -> Self {
        Self {
            deployment_id: deployment_id.into(),
            image: image.into(),
            platform: None,
            command: Vec::new(),
            entrypoint: Vec::new(),
            environment: BTreeMap::new(),
            files: Vec::new(),
            bind_mounts: Vec::new(),
            network: None,
            published_ports: Vec::new(),
            resources: None,
        }
    }

    /// Selects an exact OCI platform for a cross-architecture image.
    #[must_use]
    pub fn with_platform(mut self, platform: impl Into<String>) -> Self {
        self.platform = Some(platform.into());
        self
    }

    /// Replaces the image command with the supplied argument sequence.
    pub fn with_command<I, S>(mut self, command: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.command = command.into_iter().map(Into::into).collect();
        self
    }

    /// Replaces the image entrypoint with the supplied executable sequence.
    pub fn with_entrypoint<I, S>(mut self, entrypoint: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.entrypoint = entrypoint.into_iter().map(Into::into).collect();
        self
    }

    /// Adds or replaces one workload environment entry.
    pub fn with_environment(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.environment.insert(key.into(), value.into());
        self
    }

    /// Adds one bounded immutable regular file to the container filesystem.
    pub fn with_file(
        mut self,
        source: impl Into<String>,
        path: impl Into<String>,
        mode: u32,
    ) -> Self {
        self.files.push(ContainerFile {
            source: source.into(),
            path: path.into(),
            mode,
        });
        self
    }

    /// Adds one explicit host bind mount.
    pub fn with_bind_mount(mut self, source: impl Into<String>, target: impl Into<String>) -> Self {
        self.bind_mounts.push(BindMount {
            source: source.into(),
            target: target.into(),
        });
        self
    }

    /// Attaches the workload to one existing Docker network.
    pub fn with_network(mut self, network: impl Into<String>) -> Self {
        self.network = Some(network.into());
        self
    }

    /// Publishes one container TCP port on a fixed local host port.
    pub fn with_published_port(mut self, container_port: u16, host_port: u16) -> Self {
        self.published_ports.push(PublishedPort {
            container_port,
            host_port: Some(host_port),
        });
        self
    }

    /// Publishes one container TCP port on a Docker-assigned loopback port.
    pub fn with_ephemeral_port(mut self, container_port: u16) -> Self {
        self.published_ports.push(PublishedPort {
            container_port,
            host_port: None,
        });
        self
    }

    /// Applies hard CPU, memory, and process ceilings to this container.
    #[must_use]
    pub fn with_resources(mut self, resources: WorkerResources) -> Self {
        self.resources = Some(resources);
        self
    }

    /// Validates identity, image, and the Docker authority boundary.
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.deployment_id.is_empty()
            || !self
                .deployment_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(RuntimeError::InvalidDeploymentId {
                deployment_id: self.deployment_id.clone(),
            });
        }
        if self.image.trim().is_empty() {
            return Err(RuntimeError::MissingImage);
        }
        if self.platform.as_ref().is_some_and(|platform| {
            let mut parts = platform.split('/');
            let valid = |part: &str| {
                !part.is_empty()
                    && part.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')
                    })
            };
            !parts.next().is_some_and(valid)
                || !parts.next().is_some_and(valid)
                || parts.next().is_some()
        }) {
            return Err(RuntimeError::InvalidPlatform);
        }
        for key in self.environment.keys() {
            if key.starts_with("DOCKER_") {
                return Err(RuntimeError::DockerAuthority {
                    detail: format!("environment key {key}"),
                });
            }
        }
        if self.files.len() > MAX_CONTAINER_FILES {
            return Err(RuntimeError::ContainerFilesTooLarge);
        }
        for file in &self.files {
            if !valid_absolute_container_path(&file.source)
                || !valid_absolute_container_path(&file.path)
                || !(0o400..=0o666).contains(&file.mode)
                || file.mode & 0o111 != 0
                || is_docker_socket(&file.source)
            {
                return Err(RuntimeError::InvalidFile {
                    path: file.path.clone(),
                });
            }
        }
        for mount in &self.bind_mounts {
            if is_docker_socket(&mount.source) || is_docker_socket(&mount.target) {
                return Err(RuntimeError::DockerAuthority {
                    detail: format!("bind mount {}:{}", mount.source, mount.target),
                });
            }
        }
        if self
            .network
            .as_ref()
            .is_some_and(|network| network.is_empty())
        {
            return Err(RuntimeError::InvalidNetwork);
        }
        if self
            .published_ports
            .iter()
            .any(|port| port.container_port == 0 || port.host_port == Some(0))
        {
            return Err(RuntimeError::InvalidPort);
        }
        if self.resources.is_some_and(|resources| {
            !resources.vcpus.is_finite()
                || resources.vcpus <= 0.0
                || resources.vcpus * 1_000_000_000.0 > i64::MAX as f64
                || resources.memory_mib == 0
                || resources.memory_mib > (i64::MAX as u64) / (1024 * 1024)
                || resources.pids <= 0
        }) {
            return Err(RuntimeError::InvalidWorkerResources);
        }
        Ok(())
    }

    /// Computes the stable SHA-256 digest of the immutable declaration.
    pub fn digest(&self) -> Result<String, RuntimeError> {
        self.validate()?;
        let encoded = serde_json::to_vec(self)
            .map_err(|error| RuntimeError::SpecificationEncoding(error.to_string()))?;
        Ok(hex::encode(Sha256::digest(encoded)))
    }

    /// Builds the complete ownership label set for this declaration.
    pub fn labels(&self) -> Result<BTreeMap<String, String>, RuntimeError> {
        Ok(BTreeMap::from([
            (LABEL_MANAGED.to_owned(), "true".to_owned()),
            (LABEL_DEPLOYMENT.to_owned(), self.deployment_id.clone()),
            (LABEL_SPEC_DIGEST.to_owned(), self.digest()?),
        ]))
    }

    /// Returns the deterministic Docker container name for this deployment.
    fn container_name(&self) -> String {
        format!("{CONTAINER_NAME_PREFIX}{}", self.deployment_id)
    }

    /// Normalizes this declaration into an engine create request.
    fn create_request(&self) -> Result<EngineCreateRequest, RuntimeError> {
        self.validate()?;
        let files = materialize_container_files(&self.files)?;
        let mut labels = self.labels()?;
        if !files.is_empty() {
            labels.insert(
                LABEL_SPEC_DIGEST.to_owned(),
                runtime_digest(self.digest()?, &files),
            );
        }
        Ok(EngineCreateRequest {
            name: self.container_name(),
            image: self.image.clone(),
            platform: self.platform.clone(),
            command: self.command.clone(),
            entrypoint: self.entrypoint.clone(),
            environment: self.environment.clone(),
            files,
            bind_mounts: self.bind_mounts.clone(),
            network: self.network.clone(),
            published_ports: self.published_ports.clone(),
            resources: self.resources,
            labels,
        })
    }
}

/// Normalized lifecycle state independent of Docker response spelling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservedState {
    /// The workload process is running.
    Running,
    /// The container exists but its workload process is not running.
    Stopped,
}

/// Public observation of one Verglas-managed deployment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedContainer {
    /// Docker Engine container identifier.
    pub id: String,
    /// Stable Verglas deployment identity.
    pub deployment_id: String,
    /// Normalized lifecycle state.
    pub state: ObservedState,
    /// Host loopback ports currently assigned by the Docker Engine.
    pub published_ports: Vec<PublishedPort>,
}

/// Result of reconciling one desired container declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconcileOutcome {
    /// A missing container was created and started.
    Created,
    /// An unchanged stopped container was started.
    Started,
    /// A changed owned container was replaced and started.
    Replaced,
    /// The matching container was already running.
    Unchanged,
}

/// Failures produced by local container placement.
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// A deployment identity cannot be mapped safely to a Docker name.
    #[error("invalid deployment id: {deployment_id}")]
    InvalidDeploymentId {
        /// Rejected deployment identity.
        deployment_id: String,
    },
    /// An OCI image reference was not supplied.
    #[error("container image must not be empty")]
    MissingImage,
    /// An OCI platform was not in `os/architecture` form.
    #[error("container platform must use os/architecture form")]
    InvalidPlatform,
    /// An explicitly selected Docker network was empty.
    #[error("container network must not be empty")]
    InvalidNetwork,
    /// A published TCP port used the reserved zero value.
    #[error("published container and host ports must be non-zero")]
    InvalidPort,
    /// Worker resource limits were zero, negative, or not finite.
    #[error("worker CPU, memory, and PID limits must be positive")]
    InvalidWorkerResources,
    /// A worker invocation used a zero-second wall-clock ceiling.
    #[error("worker timeout must be positive")]
    InvalidWorkerTimeout,
    /// A worker requested scratch storage but the runtime has no operator root.
    #[error("worker scratch storage is not configured")]
    WorkerScratchUnavailable,
    /// A worker scratch target or worker identity was unsafe.
    #[error("worker scratch target or identity is invalid")]
    InvalidWorkerScratch,
    /// A run identity already belongs to an existing container.
    #[error("worker run {run_id} already exists")]
    RunAlreadyExists {
        /// Colliding run identity.
        run_id: String,
    },
    /// A worker exceeded its declared wall-clock ceiling.
    #[error("worker {worker} exceeded its {timeout_seconds}-second timeout")]
    WorkerTimedOut {
        /// Worker deployment name.
        worker: String,
        /// Enforced wall-clock ceiling.
        timeout_seconds: u64,
    },
    /// A worker process or result file reported failure.
    #[error("worker {worker} failed: {message}")]
    WorkerFailed {
        /// Worker deployment name.
        worker: String,
        /// Bounded diagnostic detail.
        message: String,
    },
    /// A Vessel health path was not origin-relative.
    #[error("vessel health path must begin with '/'")]
    InvalidHealthPath,
    /// A submitted source path cannot be represented safely in a build context.
    #[error("invalid Vessel project path: {path}")]
    InvalidProjectPath {
        /// Rejected relative path.
        path: String,
    },
    /// A desired container file path or mode is unsafe.
    #[error("invalid container file: {path}")]
    InvalidFile {
        /// Rejected absolute target path.
        path: String,
    },
    /// Desired inline files exceeded the count or aggregate byte ceiling.
    #[error("container inline files exceed the size or count limit")]
    ContainerFilesTooLarge,
    /// A runtime-managed file could not be read without exposing its contents.
    #[error("could not read container file {path}: {message}")]
    FileRead {
        /// Operator-configured source path.
        path: String,
        /// Filesystem failure without file content.
        message: String,
    },
    /// Local PostgreSQL TLS identity generation or persistence failed.
    #[error("local Postgres TLS identity failed: {0}")]
    TlsIdentity(String),
    /// A required TypeScript project file was not supplied.
    #[error("Vessel project is missing required file {path}")]
    MissingProjectFile {
        /// Required relative path.
        path: String,
    },
    /// The submitted package declaration was not valid JSON.
    #[error("invalid Vessel package.json: {0}")]
    InvalidPackageJson(String),
    /// The submitted package does not define the standalone runtime command.
    #[error("Vessel package.json must define scripts.start")]
    MissingStartScript,
    /// A bounded worker omitted the command executed for each invocation.
    #[error("worker project must define a non-empty entrypoint")]
    MissingWorkerEntrypoint,
    /// The submitted project exceeded a bounded source limit.
    #[error("Vessel project exceeds the source size or file count limit")]
    ProjectTooLarge,
    /// Encoding the in-memory Docker build context failed.
    #[error("failed to encode Vessel build context: {0}")]
    BuildContext(String),
    /// The Docker Engine failed the standalone Vessel image build.
    #[error("Vessel image build failed: {0}")]
    ImageBuild(String),
    /// A workload attempted to receive Docker daemon authority.
    #[error("workload cannot receive Docker authority through {detail}")]
    DockerAuthority {
        /// Rejected environment entry or mount.
        detail: String,
    },
    /// A same-named container is not owned by Verglas.
    #[error("container {name} exists without Verglas ownership labels")]
    UnmanagedCollision {
        /// Colliding Docker container name.
        name: String,
    },
    /// A managed container omitted its stable deployment identity label.
    #[error("managed container {name} has no deployment identity label")]
    MissingDeploymentLabel {
        /// Docker container name with incomplete labels.
        name: String,
    },
    /// Serialization of an immutable declaration failed.
    #[error("failed to encode container specification: {0}")]
    SpecificationEncoding(String),
    /// The Docker Engine rejected or could not execute an operation.
    #[error("Docker Engine operation failed: {0}")]
    Engine(String),
}

/// Trusted local Docker Engine placement adapter.
pub struct DockerRuntime {
    core: DockerRuntimeCore<BollardDockerApi>,
}

impl DockerRuntime {
    /// Connects to the configured local Docker socket using Docker defaults.
    pub fn connect_local(worker_scratch_root: PathBuf) -> Result<Self, RuntimeError> {
        let docker = Docker::connect_with_local_defaults()
            .map_err(|error| RuntimeError::Engine(error.to_string()))?;
        Ok(Self {
            core: DockerRuntimeCore::new(BollardDockerApi { docker })
                .with_worker_scratch_root(worker_scratch_root),
        })
    }

    /// Reconciles a desired immutable container declaration.
    pub async fn reconcile(
        &self,
        specification: &ContainerSpec,
    ) -> Result<ReconcileOutcome, RuntimeError> {
        self.core.reconcile(specification).await
    }

    /// Inspects one managed deployment, returning None when it does not exist.
    pub async fn inspect(
        &self,
        deployment_id: &str,
    ) -> Result<Option<ManagedContainer>, RuntimeError> {
        self.core.inspect(deployment_id).await
    }

    /// Stops one managed deployment and reports whether state changed.
    pub async fn stop(&self, deployment_id: &str) -> Result<bool, RuntimeError> {
        self.core.stop(deployment_id).await
    }

    /// Removes one managed deployment and reports whether it existed.
    pub async fn remove(&self, deployment_id: &str) -> Result<bool, RuntimeError> {
        self.core.remove(deployment_id).await
    }

    /// Lists only containers carrying valid Verglas ownership labels.
    pub async fn list(&self) -> Result<Vec<ManagedContainer>, RuntimeError> {
        self.core.list().await
    }

    /// Ensures one labelled shared Docker network exists.
    pub async fn ensure_network(&self, name: &str) -> Result<bool, RuntimeError> {
        self.core.ensure_network(name).await
    }

    /// Builds one normalized TypeScript Vessel project into its immutable image.
    pub async fn build_project(
        &self,
        project: &VesselProjectSpec,
    ) -> Result<ProjectBuildContext, RuntimeError> {
        let build = project.build_context()?;
        self.core
            .api
            .build(&build.image, build.context.clone())
            .await?;
        Ok(build)
    }

    /// Builds one locked worker project into its immutable local image.
    pub async fn build_worker_project(
        &self,
        project: &WorkerProjectSpec,
    ) -> Result<ProjectBuildContext, RuntimeError> {
        let build = project.build_context()?;
        self.core
            .api
            .build(&build.image, build.context.clone())
            .await?;
        Ok(build)
    }

    /// Executes one bounded invocation and removes its ephemeral container.
    pub async fn run_worker(
        &self,
        invocation: &WorkerInvocation,
    ) -> Result<WorkerRunResult, RuntimeError> {
        self.core.run_worker(invocation).await
    }
}

#[derive(Debug, Clone)]
struct EngineContainer {
    id: String,
    name: String,
    labels: BTreeMap<String, String>,
    state: ObservedState,
    published_ports: Vec<PublishedPort>,
}

#[derive(Debug, Clone)]
struct EngineCreateRequest {
    name: String,
    image: String,
    platform: Option<String>,
    command: Vec<String>,
    entrypoint: Vec<String>,
    environment: BTreeMap<String, String>,
    files: Vec<MaterializedContainerFile>,
    bind_mounts: Vec<BindMount>,
    network: Option<String>,
    published_ports: Vec<PublishedPort>,
    resources: Option<WorkerResources>,
    labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct MaterializedContainerFile {
    path: String,
    contents: Vec<u8>,
    mode: u32,
}

#[async_trait]
trait DockerApi: Send + Sync {
    /// Builds one immutable image from an in-memory Docker build archive.
    async fn build(&self, image: &str, context: Vec<u8>) -> Result<(), RuntimeError>;

    /// Finds a container by its exact engine name.
    async fn inspect(&self, name: &str) -> Result<Option<EngineContainer>, RuntimeError>;

    /// Pulls the image when necessary and creates one stopped container.
    async fn create(&self, request: EngineCreateRequest) -> Result<(), RuntimeError>;

    /// Starts one existing stopped container.
    async fn start(&self, name: &str) -> Result<(), RuntimeError>;

    /// Waits for the workload process and returns its exit code.
    async fn wait(&self, name: &str) -> Result<i64, RuntimeError>;

    /// Reads bounded combined stdout and stderr after the workload exits.
    async fn logs(&self, name: &str) -> Result<String, RuntimeError>;

    /// Reads one regular file from a stopped container.
    async fn read_file(&self, name: &str, path: &str) -> Result<Option<Vec<u8>>, RuntimeError>;

    /// Stops one existing running container.
    async fn stop(&self, name: &str) -> Result<(), RuntimeError>;

    /// Removes one existing stopped container.
    async fn remove(&self, name: &str) -> Result<(), RuntimeError>;

    /// Lists all containers visible to the engine client.
    async fn list(&self) -> Result<Vec<EngineContainer>, RuntimeError>;

    /// Finds a Docker network and returns its labels.
    async fn inspect_network(
        &self,
        name: &str,
    ) -> Result<Option<BTreeMap<String, String>>, RuntimeError>;

    /// Creates one labelled bridge network.
    async fn create_network(
        &self,
        name: &str,
        labels: BTreeMap<String, String>,
    ) -> Result<(), RuntimeError>;
}

struct DockerRuntimeCore<A> {
    api: A,
    worker_scratch_root: Option<PathBuf>,
}

impl<A> DockerRuntimeCore<A>
where
    A: DockerApi,
{
    /// Creates a reconciler around one Docker API implementation.
    fn new(api: A) -> Self {
        Self {
            api,
            worker_scratch_root: None,
        }
    }

    /// Selects the operator-owned host root for persistent worker scratch data.
    #[must_use]
    fn with_worker_scratch_root(mut self, root: PathBuf) -> Self {
        self.worker_scratch_root = Some(root);
        self
    }

    /// Creates, runs, observes, and removes one bounded worker container.
    async fn run_worker(
        &self,
        invocation: &WorkerInvocation,
    ) -> Result<WorkerRunResult, RuntimeError> {
        if invocation.timeout_seconds == 0 {
            return Err(RuntimeError::InvalidWorkerTimeout);
        }
        if invocation.entrypoint.is_empty()
            || invocation.entrypoint.iter().any(|part| part.is_empty())
        {
            return Err(RuntimeError::MissingWorkerEntrypoint);
        }
        let event = serde_json::to_string(&invocation.event)
            .map_err(|error| RuntimeError::SpecificationEncoding(error.to_string()))?;
        let mut specification = ContainerSpec::new(&invocation.run_id, &invocation.image)
            .with_command(invocation.entrypoint.clone())
            .with_resources(invocation.resources);
        if let Some(target) = &invocation.scratch_target {
            let root = self
                .worker_scratch_root
                .as_ref()
                .ok_or(RuntimeError::WorkerScratchUnavailable)?;
            if !valid_absolute_container_path(target)
                || invocation.worker.is_empty()
                || !invocation
                    .worker
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            {
                return Err(RuntimeError::InvalidWorkerScratch);
            }
            specification = specification
                .with_bind_mount(root.join(&invocation.worker).to_string_lossy(), target);
        }
        specification.network = invocation.network.clone();
        specification.environment = invocation.environment.clone();
        specification.environment.extend(BTreeMap::from([
            ("DEPLOYMENT".to_owned(), invocation.worker.clone()),
            ("TARGET".to_owned(), invocation.target.clone()),
            ("VERGLAS_ENDPOINT".to_owned(), invocation.endpoint.clone()),
            ("VERGLAS_TOKEN".to_owned(), invocation.token.clone()),
            ("RESULT_PATH".to_owned(), WORKER_RESULT_PATH.to_owned()),
            ("VERGLAS_CLOUD_EVENT".to_owned(), event),
        ]));
        let request = specification.create_request()?;
        if self.api.inspect(&request.name).await?.is_some() {
            return Err(RuntimeError::RunAlreadyExists {
                run_id: invocation.run_id.clone(),
            });
        }
        self.api.create(request.clone()).await?;
        let result: Result<WorkerRunResult, RuntimeError> = async {
            self.api.start(&request.name).await?;
            let wait = tokio::time::timeout(
                std::time::Duration::from_secs(invocation.timeout_seconds),
                self.api.wait(&request.name),
            )
            .await;
            match wait {
                Ok(exit) => {
                    let exit_code = exit?;
                    let logs = self.api.logs(&request.name).await?;
                    let body = self
                        .api
                        .read_file(&request.name, WORKER_RESULT_PATH)
                        .await?;
                    parse_worker_result(&invocation.worker, exit_code, body, logs)
                }
                Err(_) => {
                    self.api.stop(&request.name).await?;
                    Err(RuntimeError::WorkerTimedOut {
                        worker: invocation.worker.clone(),
                        timeout_seconds: invocation.timeout_seconds,
                    })
                }
            }
        }
        .await;
        let cleanup = self.api.remove(&request.name).await;
        match (result, cleanup) {
            (Ok(result), Ok(())) => Ok(result),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    /// Reconciles one immutable container specification.
    async fn reconcile(
        &self,
        specification: &ContainerSpec,
    ) -> Result<ReconcileOutcome, RuntimeError> {
        let request = specification.create_request()?;
        let existing = self.api.inspect(&request.name).await?;
        let Some(existing) = existing else {
            self.api.create(request.clone()).await?;
            self.api.start(&request.name).await?;
            return Ok(ReconcileOutcome::Created);
        };
        ensure_owned(&existing)?;

        let desired_digest = request.labels.get(LABEL_SPEC_DIGEST).map(String::as_str);
        let observed_digest = existing.labels.get(LABEL_SPEC_DIGEST).map(String::as_str);
        if desired_digest != observed_digest {
            if existing.state == ObservedState::Running {
                self.api.stop(&existing.name).await?;
            }
            self.api.remove(&existing.name).await?;
            self.api.create(request.clone()).await?;
            self.api.start(&request.name).await?;
            return Ok(ReconcileOutcome::Replaced);
        }
        if existing.state == ObservedState::Stopped {
            self.api.start(&existing.name).await?;
            return Ok(ReconcileOutcome::Started);
        }
        Ok(ReconcileOutcome::Unchanged)
    }

    /// Inspects one managed deployment by stable identity.
    async fn inspect(&self, deployment_id: &str) -> Result<Option<ManagedContainer>, RuntimeError> {
        let name = container_name(deployment_id)?;
        self.api
            .inspect(&name)
            .await?
            .map(|container| normalize_managed(&container))
            .transpose()
    }

    /// Stops one managed deployment idempotently.
    async fn stop(&self, deployment_id: &str) -> Result<bool, RuntimeError> {
        let name = container_name(deployment_id)?;
        let Some(container) = self.api.inspect(&name).await? else {
            return Ok(false);
        };
        ensure_owned(&container)?;
        if container.state == ObservedState::Stopped {
            return Ok(false);
        }
        self.api.stop(&name).await?;
        Ok(true)
    }

    /// Removes one managed deployment idempotently.
    async fn remove(&self, deployment_id: &str) -> Result<bool, RuntimeError> {
        let name = container_name(deployment_id)?;
        let Some(container) = self.api.inspect(&name).await? else {
            return Ok(false);
        };
        ensure_owned(&container)?;
        if container.state == ObservedState::Running {
            self.api.stop(&name).await?;
        }
        self.api.remove(&name).await?;
        Ok(true)
    }

    /// Lists and normalizes all valid Verglas-owned containers.
    async fn list(&self) -> Result<Vec<ManagedContainer>, RuntimeError> {
        self.api
            .list()
            .await?
            .into_iter()
            .filter(is_owned)
            .map(|container| normalize_managed(&container))
            .collect()
    }

    /// Creates a missing shared network and fails closed on a foreign collision.
    async fn ensure_network(&self, name: &str) -> Result<bool, RuntimeError> {
        if name.is_empty() {
            return Err(RuntimeError::InvalidNetwork);
        }
        if let Some(labels) = self.api.inspect_network(name).await? {
            if labels.get(LABEL_MANAGED).map(String::as_str) == Some("true") {
                return Ok(false);
            }
            return Err(RuntimeError::UnmanagedCollision {
                name: name.to_owned(),
            });
        }
        self.api
            .create_network(
                name,
                BTreeMap::from([(LABEL_MANAGED.to_owned(), "true".to_owned())]),
            )
            .await?;
        Ok(true)
    }
}

/// Interprets the shared SDK result-file contract after a worker exits.
fn parse_worker_result(
    worker: &str,
    exit_code: i64,
    body: Option<Vec<u8>>,
    logs: String,
) -> Result<WorkerRunResult, RuntimeError> {
    let parsed = body
        .as_deref()
        .map(serde_json::from_slice::<WorkerResultFile>)
        .transpose()
        .map_err(|error| RuntimeError::WorkerFailed {
            worker: worker.to_owned(),
            message: format!("invalid result file: {error}"),
        })?;
    if let Some(WorkerResultFile {
        error: Some(message),
        ..
    }) = parsed
    {
        return Err(RuntimeError::WorkerFailed {
            worker: worker.to_owned(),
            message,
        });
    }
    if exit_code != 0 {
        return Err(RuntimeError::WorkerFailed {
            worker: worker.to_owned(),
            message: format!("container exited {exit_code}: {}", logs.trim()),
        });
    }
    Ok(WorkerRunResult {
        rows_produced: parsed.map_or(0, |result| result.rows),
        logs,
    })
}

#[derive(Clone)]
struct BollardDockerApi {
    docker: Docker,
}

#[async_trait]
impl DockerApi for BollardDockerApi {
    /// Builds one content-addressed Vessel image and returns bounded failures.
    async fn build(&self, image: &str, context: Vec<u8>) -> Result<(), RuntimeError> {
        let options = BuildImageOptionsBuilder::default()
            .dockerfile("Dockerfile")
            .t(image)
            .rm(true)
            .build();
        let mut stream = self
            .docker
            .build_image(options, None, Some(body_full(context.into())));
        while let Some(message) = stream.next().await {
            let message = message.map_err(engine_error)?;
            if let Some(error) = message.error_detail.and_then(|detail| detail.message) {
                return Err(RuntimeError::ImageBuild(
                    error.chars().take(MAX_BUILD_ERROR_BYTES).collect(),
                ));
            }
        }
        Ok(())
    }

    /// Inspects one Docker container and maps a missing name to None.
    async fn inspect(&self, name: &str) -> Result<Option<EngineContainer>, RuntimeError> {
        match self.docker.inspect_container(name, None).await {
            Ok(response) => {
                let labels = response
                    .config
                    .and_then(|config| config.labels)
                    .unwrap_or_default()
                    .into_iter()
                    .collect();
                Ok(Some(EngineContainer {
                    id: response.id.unwrap_or_default(),
                    name: response
                        .name
                        .unwrap_or_else(|| name.to_owned())
                        .trim_start_matches('/')
                        .to_owned(),
                    labels,
                    state: if response
                        .state
                        .and_then(|state| state.running)
                        .unwrap_or(false)
                    {
                        ObservedState::Running
                    } else {
                        ObservedState::Stopped
                    },
                    published_ports: response
                        .network_settings
                        .and_then(|settings| settings.ports)
                        .map(observed_port_bindings)
                        .unwrap_or_default(),
                }))
            }
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(engine_error(error)),
        }
    }

    /// Pulls the declared image and creates one stopped Docker container.
    async fn create(&self, request: EngineCreateRequest) -> Result<(), RuntimeError> {
        let container_name = request.name.clone();
        let files = request.files.clone();
        let must_pull = match self.docker.inspect_image(&request.image).await {
            Ok(_) => false,
            Err(error) if is_not_found(&error) => true,
            Err(error) => return Err(engine_error(error)),
        };
        if must_pull {
            let mut options = CreateImageOptionsBuilder::new().from_image(&request.image);
            if let Some(platform) = &request.platform {
                options = options.platform(platform);
            }
            self.docker
                .create_image(Some(options.build()), None, None)
                .try_collect::<Vec<_>>()
                .await
                .map_err(engine_error)?;
        }
        let environment = request
            .environment
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>();
        let binds = request
            .bind_mounts
            .into_iter()
            .map(|mount| format!("{}:{}", mount.source, mount.target))
            .collect::<Vec<_>>();
        let exposed_ports = request
            .published_ports
            .iter()
            .map(|port| format!("{}/tcp", port.container_port))
            .collect::<Vec<_>>();
        let port_bindings = request
            .published_ports
            .into_iter()
            .map(|port| {
                (
                    format!("{}/tcp", port.container_port),
                    Some(vec![DockerPortBinding {
                        host_ip: Some("127.0.0.1".to_owned()),
                        host_port: port.host_port.map(|value| value.to_string()),
                    }]),
                )
            })
            .collect::<HashMap<_, _>>();
        let resources = request.resources;
        let host_config = if binds.is_empty()
            && request.network.is_none()
            && port_bindings.is_empty()
            && resources.is_none()
        {
            None
        } else {
            Some(HostConfig {
                binds: (!binds.is_empty()).then_some(binds),
                network_mode: request.network,
                port_bindings: (!port_bindings.is_empty()).then_some(port_bindings),
                nano_cpus: resources.map(|limits| (limits.vcpus * 1_000_000_000.0) as i64),
                memory: resources.map(|limits| (limits.memory_mib * 1024 * 1024) as i64),
                memory_swap: resources.map(|limits| (limits.memory_mib * 1024 * 1024) as i64),
                pids_limit: resources.map(|limits| limits.pids),
                ..Default::default()
            })
        };
        let body = ContainerCreateBody {
            image: Some(request.image),
            cmd: (!request.command.is_empty()).then_some(request.command),
            entrypoint: (!request.entrypoint.is_empty()).then_some(request.entrypoint),
            env: (!environment.is_empty()).then_some(environment),
            labels: Some(request.labels.into_iter().collect::<HashMap<_, _>>()),
            exposed_ports: (!exposed_ports.is_empty()).then_some(exposed_ports),
            host_config,
            ..Default::default()
        };
        let mut options = CreateContainerOptionsBuilder::new().name(&request.name);
        if let Some(platform) = &request.platform {
            options = options.platform(platform);
        }
        self.docker
            .create_container(Some(options.build()), body)
            .await
            .map_err(engine_error)?;
        if !files.is_empty() {
            let archive = archive_container_files(&files)?;
            let options = UploadToContainerOptionsBuilder::default().path("/").build();
            self.docker
                .upload_to_container(&container_name, Some(options), body_full(archive.into()))
                .await
                .map_err(engine_error)?;
        }
        Ok(())
    }

    /// Starts one Docker container.
    async fn start(&self, name: &str) -> Result<(), RuntimeError> {
        self.docker
            .start_container(name, None)
            .await
            .map_err(engine_error)
    }

    /// Waits until one Docker container is no longer running.
    async fn wait(&self, name: &str) -> Result<i64, RuntimeError> {
        let options = WaitContainerOptionsBuilder::default()
            .condition("not-running")
            .build();
        match self.docker.wait_container(name, Some(options)).next().await {
            Some(Ok(response)) => Ok(response.status_code),
            Some(Err(BollardError::DockerContainerWaitError { code, .. })) => Ok(code),
            Some(Err(error)) => Err(engine_error(error)),
            None => Err(RuntimeError::Engine(format!(
                "Docker returned no wait result for {name}"
            ))),
        }
    }

    /// Reads bounded stdout and stderr from one Docker container.
    async fn logs(&self, name: &str) -> Result<String, RuntimeError> {
        let options = LogsOptionsBuilder::default()
            .stdout(true)
            .stderr(true)
            .build();
        let mut bytes = Vec::new();
        let mut stream = self.docker.logs(name, Some(options));
        while let Some(output) = stream.next().await {
            let output = output.map_err(engine_error)?.to_string();
            let remaining = MAX_WORKER_LOG_BYTES.saturating_sub(bytes.len());
            bytes.extend_from_slice(&output.as_bytes()[..output.len().min(remaining)]);
            if bytes.len() == MAX_WORKER_LOG_BYTES {
                break;
            }
        }
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    /// Downloads and extracts one regular file from a stopped container.
    async fn read_file(&self, name: &str, path: &str) -> Result<Option<Vec<u8>>, RuntimeError> {
        let options = DownloadFromContainerOptionsBuilder::default()
            .path(path)
            .build();
        let chunks = match self
            .docker
            .download_from_container(name, Some(options))
            .try_collect::<Vec<_>>()
            .await
        {
            Ok(chunks) => chunks,
            Err(error) if is_not_found(&error) => return Ok(None),
            Err(error) => return Err(engine_error(error)),
        };
        let archive_bytes = chunks.concat();
        let mut archive = tar::Archive::new(archive_bytes.as_slice());
        let mut entries = archive
            .entries()
            .map_err(|error| RuntimeError::BuildContext(error.to_string()))?;
        let Some(entry) = entries.next() else {
            return Ok(None);
        };
        let mut entry = entry.map_err(|error| RuntimeError::BuildContext(error.to_string()))?;
        let mut body = Vec::new();
        entry
            .read_to_end(&mut body)
            .map_err(|error| RuntimeError::BuildContext(error.to_string()))?;
        Ok(Some(body))
    }

    /// Stops one Docker container.
    async fn stop(&self, name: &str) -> Result<(), RuntimeError> {
        self.docker
            .stop_container(name, None)
            .await
            .map_err(engine_error)
    }

    /// Removes one Docker container.
    async fn remove(&self, name: &str) -> Result<(), RuntimeError> {
        self.docker
            .remove_container(name, None)
            .await
            .map_err(engine_error)
    }

    /// Lists every Docker container, including stopped containers.
    async fn list(&self) -> Result<Vec<EngineContainer>, RuntimeError> {
        let summaries = self
            .docker
            .list_containers(Some(ListContainersOptionsBuilder::new().all(true).build()))
            .await
            .map_err(engine_error)?;
        Ok(summaries
            .into_iter()
            .map(|summary| EngineContainer {
                id: summary.id.unwrap_or_default(),
                name: summary
                    .names
                    .and_then(|names| names.into_iter().next())
                    .unwrap_or_default()
                    .trim_start_matches('/')
                    .to_owned(),
                labels: summary.labels.unwrap_or_default().into_iter().collect(),
                state: if summary.state.map(|state| state.to_string()) == Some("running".to_owned())
                {
                    ObservedState::Running
                } else {
                    ObservedState::Stopped
                },
                published_ports: summary
                    .ports
                    .unwrap_or_default()
                    .into_iter()
                    .map(|port| PublishedPort {
                        container_port: port.private_port,
                        host_port: port.public_port,
                    })
                    .collect(),
            })
            .collect())
    }

    /// Inspects one Docker network and maps a missing name to None.
    async fn inspect_network(
        &self,
        name: &str,
    ) -> Result<Option<BTreeMap<String, String>>, RuntimeError> {
        match self.docker.inspect_network(name, None).await {
            Ok(network) => Ok(Some(
                network.labels.unwrap_or_default().into_iter().collect(),
            )),
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(engine_error(error)),
        }
    }

    /// Creates one labelled Docker bridge network.
    async fn create_network(
        &self,
        name: &str,
        labels: BTreeMap<String, String>,
    ) -> Result<(), RuntimeError> {
        self.docker
            .create_network(NetworkCreateRequest {
                name: name.to_owned(),
                labels: Some(labels.into_iter().collect()),
                ..Default::default()
            })
            .await
            .map_err(engine_error)?;
        Ok(())
    }
}

/// Rejects host paths and targets that convey the Docker control socket.
fn is_docker_socket(path: &str) -> bool {
    path.rsplit('/').next() == Some("docker.sock")
}

/// Derives a Docker container name after validating the deployment identity.
fn container_name(deployment_id: &str) -> Result<String, RuntimeError> {
    let specification = ContainerSpec::new(deployment_id, "identity-validation");
    specification.validate()?;
    Ok(specification.container_name())
}

/// Returns whether a container carries the exact Verglas ownership marker.
fn is_owned(container: &EngineContainer) -> bool {
    container.labels.get(LABEL_MANAGED).map(String::as_str) == Some("true")
}

/// Enforces ownership before any mutation of an existing Docker container.
fn ensure_owned(container: &EngineContainer) -> Result<(), RuntimeError> {
    if is_owned(container) {
        Ok(())
    } else {
        Err(RuntimeError::UnmanagedCollision {
            name: container.name.clone(),
        })
    }
}

/// Converts one valid owned engine record into the public observed vocabulary.
fn normalize_managed(container: &EngineContainer) -> Result<ManagedContainer, RuntimeError> {
    ensure_owned(container)?;
    let deployment_id = container
        .labels
        .get(LABEL_DEPLOYMENT)
        .cloned()
        .ok_or_else(|| RuntimeError::MissingDeploymentLabel {
            name: container.name.clone(),
        })?;
    Ok(ManagedContainer {
        id: container.id.clone(),
        deployment_id,
        state: container.state,
        published_ports: container.published_ports.clone(),
    })
}

/// Encodes immutable regular files into a Docker upload archive rooted at `/`.
fn archive_container_files(files: &[MaterializedContainerFile]) -> Result<Vec<u8>, RuntimeError> {
    let mut archive = tar::Builder::new(Vec::new());
    archive.mode(tar::HeaderMode::Deterministic);
    for file in files {
        let mut header = tar::Header::new_gnu();
        header.set_size(file.contents.len() as u64);
        header.set_mode(file.mode);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_cksum();
        archive
            .append_data(
                &mut header,
                file.path.trim_start_matches('/'),
                file.contents.as_slice(),
            )
            .map_err(|error| RuntimeError::BuildContext(error.to_string()))?;
    }
    archive
        .into_inner()
        .map_err(|error| RuntimeError::BuildContext(error.to_string()))
}

/// Reads source paths only at reconciliation time so desired state stores no bearer material.
fn materialize_container_files(
    files: &[ContainerFile],
) -> Result<Vec<MaterializedContainerFile>, RuntimeError> {
    let mut total = 0usize;
    files
        .iter()
        .map(|file| {
            let contents = std::fs::read(&file.source).map_err(|error| RuntimeError::FileRead {
                path: file.source.clone(),
                message: error.to_string(),
            })?;
            total = total.saturating_add(contents.len());
            if total > MAX_CONTAINER_FILE_BYTES {
                return Err(RuntimeError::ContainerFilesTooLarge);
            }
            Ok(MaterializedContainerFile {
                path: file.path.clone(),
                contents,
                mode: file.mode,
            })
        })
        .collect()
}

/// Extends the declaration digest with file bytes read by the trusted manager.
fn runtime_digest(base: String, files: &[MaterializedContainerFile]) -> String {
    let mut digest = Sha256::new();
    digest.update(base.as_bytes());
    for file in files {
        digest.update([0]);
        digest.update(file.path.as_bytes());
        digest.update(file.mode.to_be_bytes());
        digest.update(&file.contents);
    }
    hex::encode(digest.finalize())
}

/// Normalizes Docker inspect port bindings to the public runtime vocabulary.
fn observed_port_bindings(
    bindings: HashMap<String, Option<Vec<DockerPortBinding>>>,
) -> Vec<PublishedPort> {
    let mut ports = bindings
        .into_iter()
        .filter_map(|(container, bindings)| {
            let container_port = container.strip_suffix("/tcp")?.parse().ok()?;
            Some(
                bindings
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(move |binding| {
                        let host_port = binding.host_port?.parse().ok()?;
                        Some(PublishedPort {
                            container_port,
                            host_port: Some(host_port),
                        })
                    }),
            )
        })
        .flatten()
        .collect::<Vec<_>>();
    ports.sort_by_key(|port| (port.container_port, port.host_port));
    ports
}

/// Identifies Docker's name-not-found response without matching error strings.
fn is_not_found(error: &BollardError) -> bool {
    matches!(
        error,
        BollardError::DockerResponseServerError {
            status_code: 404,
            ..
        }
    )
}

/// Erases Bollard implementation details from the crate's public error type.
fn engine_error(error: BollardError) -> RuntimeError {
    RuntimeError::Engine(error.to_string())
}

#[cfg(test)]
mod tests;
