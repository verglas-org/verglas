//! Cloudflare-shaped management metadata API for one tenant cell.
//!
//! This module stores uploaded Worker modules and durable-object namespace
//! metadata. Process launch is deliberately not inferred from management data:
//! an explicit deployment supplies the Turso URL template and token file, and
//! the gateway/control plane owns the one active placement owner.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Multipart, Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{HostSupervisor, SupervisorError};

const SCRIPT_ROOT: &str = "workers/scripts";
const NAMESPACE_FILE: &str = "workers/namespaces.json";

/// Shared HTTP management state for one celld host.
#[derive(Clone)]
pub struct ManagementApi {
    state: Arc<ManagementState>,
}

/// Mutable records shared by HTTP handlers without request-local state.
struct ManagementState {
    root: PathBuf,
    _supervisor: Arc<Mutex<HostSupervisor>>,
    records: Mutex<ManagementRecords>,
}

/// In-memory index backed by the celld root's script and namespace records.
struct ManagementRecords {
    scripts: BTreeMap<String, ScriptRecord>,
    namespaces: BTreeMap<String, NamespaceRecord>,
}

/// Cloudflare-compatible response envelope used by every management route.
#[derive(Debug, Serialize)]
struct Envelope<T> {
    success: bool,
    errors: Vec<ApiMessage>,
    messages: Vec<ApiMessage>,
    result: Option<T>,
}

/// One Cloudflare-style response message.
#[derive(Debug, Serialize)]
struct ApiMessage {
    code: u16,
    message: String,
}

/// Stored metadata for one uploaded Worker script.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScriptRecord {
    id: String,
    name: String,
    main_module: String,
    bindings: Vec<Value>,
    modules: Vec<String>,
}

/// Cloudflare durable-object namespace shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct NamespaceRecord {
    id: String,
    name: String,
    script: String,
    #[serde(rename = "class")]
    class_name: String,
    objects: BTreeMap<String, ObjectRecord>,
}

/// One deterministic Durable Object identity and metadata-only state.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObjectRecord {
    id: String,
    name: String,
    do_id: String,
    #[serde(default = "default_object_status")]
    status: String,
}

/// Request body for creating a namespace.
#[derive(Debug, Deserialize)]
struct NamespaceRequest {
    name: String,
    script: String,
    #[serde(rename = "class")]
    class_name: String,
}

/// Optional request body for creating an object metadata record.
#[derive(Debug, Default, Deserialize)]
struct ObjectRequest {
    name: Option<String>,
    unique: Option<bool>,
}

/// One failure returned from the management API.
#[derive(Debug, thiserror::Error)]
enum ApiError {
    /// The request shape or metadata violates the supported Workers shape.
    #[error("invalid request: {0}")]
    BadRequest(String),
    /// The requested script, namespace, or object does not exist.
    #[error("resource not found: {0}")]
    NotFound(String),
    /// The requested name already identifies a resource.
    #[error("resource already exists: {0}")]
    Conflict(String),
    /// Local filesystem or supervisor state could not complete the operation.
    #[error("management operation failed: {0}")]
    Internal(String),
}

impl ManagementApi {
    /// Creates a metadata API rooted below one celld directory.
    pub fn new(root: impl AsRef<Path>, supervisor: Arc<Mutex<HostSupervisor>>) -> Self {
        Self {
            state: Arc::new(ManagementState {
                root: root.as_ref().to_path_buf(),
                _supervisor: supervisor,
                records: Mutex::new(ManagementRecords {
                    scripts: BTreeMap::new(),
                    namespaces: BTreeMap::new(),
                }),
            }),
        }
    }

    /// Builds account-prefix-free Workers management paths.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/workers/scripts", get(list_scripts))
            .route(
                "/workers/scripts/{script_name}",
                put(upload_script).get(fetch_script).delete(delete_script),
            )
            .route(
                "/workers/durable_objects/namespaces",
                post(create_namespace).get(list_namespaces),
            )
            .route(
                "/workers/durable_objects/namespaces/{namespace_id}/objects",
                get(list_objects).post(create_collection_object),
            )
            .route(
                "/workers/durable_objects/namespaces/{namespace_id}/objects/{object_name}",
                get(get_object).post(create_named_object),
            )
            .route(
                "/workers/durable_objects/namespaces/{namespace_id}/objects/{object_name}/status",
                get(get_object),
            )
            .with_state(self.state.clone())
    }
}

/// Returns the default status for a metadata-only object record.
fn default_object_status() -> String {
    "unprovisioned".to_owned()
}

/// Handles a script upload in Cloudflare module-syntax multipart form.
async fn upload_script(
    State(state): State<Arc<ManagementState>>,
    AxumPath(script_name): AxumPath<String>,
    multipart: Option<Multipart>,
) -> Response {
    let Some(multipart) = multipart else {
        return ApiError::BadRequest("multipart form body is required".to_owned()).into_response();
    };
    if let Err(error) = hydrate_records(&state).await {
        return error.into_response();
    }
    match upload_script_inner(&state, &script_name, multipart).await {
        Ok(record) => success_response(StatusCode::OK, record),
        Err(error) => error.into_response(),
    }
}

/// Parses, validates, stores, and indexes one uploaded script.
async fn upload_script_inner(
    state: &ManagementState,
    script_name: &str,
    mut multipart: Multipart,
) -> Result<ScriptRecord, ApiError> {
    validate_name(script_name, "script name")?;
    let mut metadata = None;
    let mut modules = BTreeMap::new();
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|error| ApiError::BadRequest(format!("invalid multipart body: {error}")))?
    {
        let field_name = field
            .name()
            .ok_or_else(|| ApiError::BadRequest("multipart field has no name".to_owned()))?
            .to_owned();
        let module_file_name = field.file_name().map(ToOwned::to_owned);
        let bytes = field
            .bytes()
            .await
            .map_err(|error| ApiError::BadRequest(format!("invalid multipart field: {error}")))?;
        if field_name == "metadata" {
            if metadata.replace(bytes.to_vec()).is_some() {
                return Err(ApiError::BadRequest(
                    "multipart metadata part appears more than once".to_owned(),
                ));
            }
            continue;
        }
        let module_name = module_file_name.unwrap_or(field_name);
        validate_module_path(&module_name)?;
        if modules
            .insert(module_name.clone(), bytes.to_vec())
            .is_some()
        {
            return Err(ApiError::BadRequest(format!(
                "module part {module_name} appears more than once"
            )));
        }
    }
    let metadata = metadata
        .ok_or_else(|| ApiError::BadRequest("multipart metadata part is required".to_owned()))?;
    let metadata = parse_script_metadata(&metadata)?;
    if !modules.contains_key(&metadata.main_module) {
        return Err(ApiError::BadRequest(format!(
            "main_module {} has no module part",
            metadata.main_module
        )));
    }
    let script = ScriptRecord {
        id: script_name.to_owned(),
        name: script_name.to_owned(),
        main_module: metadata.main_module,
        bindings: metadata.bindings,
        modules: modules.keys().cloned().collect(),
    };
    store_script(state, script_name, &script, modules).await?;
    state
        .records
        .lock()
        .await
        .scripts
        .insert(script_name.to_owned(), script.clone());
    Ok(script)
}

/// Loads persisted script and namespace indexes into the process cache.
async fn hydrate_records(state: &ManagementState) -> Result<(), ApiError> {
    let mut records = state.records.lock().await;
    if records.scripts.is_empty() {
        let root = state.root.join(SCRIPT_ROOT);
        let mut entries = match tokio::fs::read_dir(root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return hydrate_namespaces(state, records).await;
            }
            Err(error) => return Err(ApiError::Internal(format!("read script index: {error}"))),
        };
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|error| ApiError::Internal(format!("read script index entry: {error}")))?
        {
            let file_name = entry.file_name();
            let Some(script_name) = file_name.to_str() else {
                return Err(ApiError::Internal(
                    "script index has a non-UTF-8 name".to_owned(),
                ));
            };
            if script_name.starts_with('.') {
                continue;
            }
            validate_name(script_name, "script name")
                .map_err(|error| ApiError::Internal(error.to_string()))?;
            let metadata_path = entry.path().join("metadata.json");
            let bytes = tokio::fs::read(metadata_path)
                .await
                .map_err(|error| ApiError::Internal(format!("read script metadata: {error}")))?;
            let script: ScriptRecord = serde_json::from_slice(&bytes)
                .map_err(|error| ApiError::Internal(format!("decode script metadata: {error}")))?;
            if script.name != script_name {
                return Err(ApiError::Internal(format!(
                    "script metadata name {} disagrees with directory {script_name}",
                    script.name
                )));
            }
            records.scripts.insert(script_name.to_owned(), script);
        }
    }
    hydrate_namespaces(state, records).await
}

/// Loads the persisted namespace index after the script index is available.
async fn hydrate_namespaces(
    state: &ManagementState,
    mut records: tokio::sync::MutexGuard<'_, ManagementRecords>,
) -> Result<(), ApiError> {
    if records.namespaces.is_empty() {
        let path = state.root.join(NAMESPACE_FILE);
        let bytes = match tokio::fs::read(path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(ApiError::Internal(format!("read namespace index: {error}"))),
        };
        let namespaces: Vec<NamespaceRecord> = serde_json::from_slice(&bytes)
            .map_err(|error| ApiError::Internal(format!("decode namespace index: {error}")))?;
        for namespace in namespaces {
            records.namespaces.insert(namespace.id.clone(), namespace);
        }
    }
    Ok(())
}

/// Parses and validates metadata without accepting malformed object bindings.
fn parse_script_metadata(bytes: &[u8]) -> Result<ParsedScriptMetadata, ApiError> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|error| ApiError::BadRequest(format!("metadata is not valid JSON: {error}")))?;
    let object = value
        .as_object()
        .ok_or_else(|| ApiError::BadRequest("metadata must be a JSON object".to_owned()))?;
    let main_module = object
        .get("main_module")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::BadRequest("metadata.main_module is required".to_owned()))?
        .to_owned();
    validate_module_path(&main_module)?;
    let bindings = object
        .get("bindings")
        .and_then(Value::as_array)
        .ok_or_else(|| ApiError::BadRequest("metadata.bindings must be an array".to_owned()))?
        .clone();
    for binding in &bindings {
        validate_binding(binding)?;
    }
    Ok(ParsedScriptMetadata {
        main_module,
        bindings,
    })
}

/// Parsed fields needed to index an uploaded script.
struct ParsedScriptMetadata {
    main_module: String,
    bindings: Vec<Value>,
}

/// Validates a single Cloudflare binding and its durable-object fields.
fn validate_binding(binding: &Value) -> Result<(), ApiError> {
    let object = binding
        .as_object()
        .ok_or_else(|| ApiError::BadRequest("every binding must be an object".to_owned()))?;
    let binding_type = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::BadRequest("every binding needs a string type".to_owned()))?;
    if binding_type != "durable_object_namespace" {
        return Ok(());
    }
    for field in ["name", "class_name"] {
        if object
            .get(field)
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        {
            return Err(ApiError::BadRequest(format!(
                "durable_object_namespace binding needs nonempty {field}"
            )));
        }
    }
    if let Some(script_name) = object.get("script_name") {
        let script_name = script_name.as_str().ok_or_else(|| {
            ApiError::BadRequest("durable_object_namespace script_name must be a string".to_owned())
        })?;
        validate_name(script_name, "binding script name")?;
    }
    Ok(())
}

/// Writes a script's metadata and module content beneath the celld root.
async fn store_script(
    state: &ManagementState,
    script_name: &str,
    script: &ScriptRecord,
    modules: BTreeMap<String, Vec<u8>>,
) -> Result<(), ApiError> {
    let scripts_root = state.root.join(SCRIPT_ROOT);
    tokio::fs::create_dir_all(&scripts_root)
        .await
        .map_err(|error| ApiError::Internal(format!("create script root: {error}")))?;
    let temporary = scripts_root.join(format!(".{script_name}.upload-{}", Uuid::new_v4()));
    tokio::fs::create_dir_all(&temporary)
        .await
        .map_err(|error| ApiError::Internal(format!("create upload directory: {error}")))?;
    for (module_name, bytes) in &modules {
        let path = temporary.join(module_name);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| ApiError::Internal(format!("create module directory: {error}")))?;
        }
        tokio::fs::write(path, bytes)
            .await
            .map_err(|error| ApiError::Internal(format!("write module {module_name}: {error}")))?;
    }
    let metadata = serde_json::to_vec_pretty(script)
        .map_err(|error| ApiError::Internal(format!("encode script metadata: {error}")))?;
    tokio::fs::write(temporary.join("metadata.json"), metadata)
        .await
        .map_err(|error| ApiError::Internal(format!("write script metadata: {error}")))?;
    let destination = scripts_root.join(script_name);
    match tokio::fs::remove_dir_all(&destination).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            let _ = tokio::fs::remove_dir_all(&temporary).await;
            return Err(ApiError::Internal(format!("replace script: {error}")));
        }
    }
    tokio::fs::rename(&temporary, destination)
        .await
        .map_err(|error| ApiError::Internal(format!("publish script: {error}")))
}

/// Lists all scripts currently indexed by the host.
async fn list_scripts(State(state): State<Arc<ManagementState>>) -> Response {
    if let Err(error) = hydrate_records(&state).await {
        return error.into_response();
    }
    let records = state.records.lock().await;
    success_response(
        StatusCode::OK,
        records.scripts.values().cloned().collect::<Vec<_>>(),
    )
}

/// Fetches one script's metadata.
async fn fetch_script(
    State(state): State<Arc<ManagementState>>,
    AxumPath(script_name): AxumPath<String>,
) -> Response {
    if let Err(error) = validate_name(&script_name, "script name") {
        return error.into_response();
    }
    if let Err(error) = hydrate_records(&state).await {
        return error.into_response();
    }
    let records = state.records.lock().await;
    match records.scripts.get(&script_name).cloned() {
        Some(script) => success_response(StatusCode::OK, script),
        None => ApiError::NotFound(format!("script {script_name}")).into_response(),
    }
}

/// Deletes one script and its stored module tree.
async fn delete_script(
    State(state): State<Arc<ManagementState>>,
    AxumPath(script_name): AxumPath<String>,
) -> Response {
    if let Err(error) = validate_name(&script_name, "script name") {
        return error.into_response();
    }
    if let Err(error) = hydrate_records(&state).await {
        return error.into_response();
    }
    let removed = {
        let mut records = state.records.lock().await;
        records.scripts.remove(&script_name)
    };
    if removed.is_none() {
        return ApiError::NotFound(format!("script {script_name}")).into_response();
    }
    let path = state.root.join(SCRIPT_ROOT).join(&script_name);
    if let Err(error) = tokio::fs::remove_dir_all(path).await {
        return ApiError::Internal(format!("delete script: {error}")).into_response();
    }
    success_response(StatusCode::OK, true)
}

/// Creates one namespace bound to an uploaded script and class.
async fn create_namespace(
    State(state): State<Arc<ManagementState>>,
    body: Option<Json<NamespaceRequest>>,
) -> Response {
    let Some(Json(request)) = body else {
        return ApiError::BadRequest("namespace JSON body is required".to_owned()).into_response();
    };
    if let Err(error) = hydrate_records(&state).await {
        return error.into_response();
    }
    match create_namespace_inner(&state, request).await {
        Ok(namespace) => success_response(StatusCode::CREATED, namespace.public()),
        Err(error) => error.into_response(),
    }
}

/// Validates and records a new namespace before persisting its metadata.
async fn create_namespace_inner(
    state: &ManagementState,
    request: NamespaceRequest,
) -> Result<NamespaceRecord, ApiError> {
    validate_name(&request.name, "namespace name")?;
    validate_name(&request.script, "script name")?;
    validate_name(&request.class_name, "class name")?;
    let mut records = state.records.lock().await;
    let script = records
        .scripts
        .get(&request.script)
        .ok_or_else(|| ApiError::NotFound(format!("script {}", request.script)))?;
    let class_bound = script.bindings.iter().any(|binding| {
        let Some(binding) = binding.as_object() else {
            return false;
        };
        binding.get("type").and_then(Value::as_str) == Some("durable_object_namespace")
            && binding.get("class_name").and_then(Value::as_str)
                == Some(request.class_name.as_str())
            && binding
                .get("script_name")
                .and_then(Value::as_str)
                .is_none_or(|name| name == request.script)
    });
    if !class_bound {
        return Err(ApiError::BadRequest(format!(
            "class {} is not bound by script {}",
            request.class_name, request.script
        )));
    }
    if records
        .namespaces
        .values()
        .any(|namespace| namespace.name == request.name)
    {
        return Err(ApiError::Conflict(format!("namespace {}", request.name)));
    }
    let namespace = NamespaceRecord {
        id: Uuid::new_v4().simple().to_string(),
        name: request.name,
        script: request.script,
        class_name: request.class_name,
        objects: BTreeMap::new(),
    };
    records
        .namespaces
        .insert(namespace.id.clone(), namespace.clone());
    let snapshot = records.namespaces.values().cloned().collect::<Vec<_>>();
    drop(records);
    persist_namespaces(state, &snapshot).await?;
    Ok(namespace)
}

/// Lists namespace metadata without exposing process internals.
async fn list_namespaces(State(state): State<Arc<ManagementState>>) -> Response {
    if let Err(error) = hydrate_records(&state).await {
        return error.into_response();
    }
    let records = state.records.lock().await;
    let namespaces = records
        .namespaces
        .values()
        .map(NamespaceRecord::public)
        .collect::<Vec<_>>();
    success_response(StatusCode::OK, namespaces)
}

/// Lists all metadata records in one namespace.
async fn list_objects(
    State(state): State<Arc<ManagementState>>,
    AxumPath(namespace_id): AxumPath<String>,
) -> Response {
    if let Err(error) = hydrate_records(&state).await {
        return error.into_response();
    }
    let records = state.records.lock().await;
    let Some(namespace) = records.namespaces.get(&namespace_id) else {
        return ApiError::NotFound(format!("namespace {namespace_id}")).into_response();
    };
    success_response(
        StatusCode::OK,
        namespace.objects.values().cloned().collect::<Vec<_>>(),
    )
}

/// Creates a named object metadata record without silently provisioning it.
async fn create_named_object(
    State(state): State<Arc<ManagementState>>,
    AxumPath((namespace_id, object_name)): AxumPath<(String, String)>,
) -> Response {
    create_object_response(&state, &namespace_id, object_name).await
}

/// Creates a named or random object metadata record without provisioning it.
async fn create_collection_object(
    State(state): State<Arc<ManagementState>>,
    AxumPath(namespace_id): AxumPath<String>,
    body: Bytes,
) -> Response {
    if let Err(error) = hydrate_records(&state).await {
        return error.into_response();
    }
    let request = if body.is_empty() {
        ObjectRequest::default()
    } else {
        match serde_json::from_slice::<ObjectRequest>(&body) {
            Ok(request) => request,
            Err(error) => {
                return ApiError::BadRequest(format!("invalid object request: {error}"))
                    .into_response();
            }
        }
    };
    let name = match (request.name, request.unique.unwrap_or(false)) {
        (Some(name), _) => name,
        (None, true) | (None, false) => Uuid::new_v4().simple().to_string(),
    };
    create_object_response(&state, &namespace_id, name).await
}

/// Creates one metadata record and fails closed before any process launch.
async fn create_object_response(
    state: &ManagementState,
    namespace_id: &str,
    object_name: String,
) -> Response {
    if let Err(error) = hydrate_records(state).await {
        return error.into_response();
    }
    if let Err(error) = validate_name(&object_name, "object name") {
        return error.into_response();
    }
    let mut records = state.records.lock().await;
    let Some(namespace) = records.namespaces.get_mut(namespace_id) else {
        return ApiError::NotFound(format!("namespace {namespace_id}")).into_response();
    };
    if let Some(object) = namespace.objects.get(&object_name).cloned() {
        return success_response(StatusCode::OK, object);
    }
    let object_hash = object_identity(namespace_id, &object_name);
    let object = ObjectRecord {
        id: object_hash.clone(),
        name: object_name.clone(),
        do_id: format!("do-{}", &object_hash[..16]),
        status: default_object_status(),
    };
    namespace.objects.insert(object_name, object.clone());
    let snapshot = records.namespaces.values().cloned().collect::<Vec<_>>();
    drop(records);
    if let Err(error) = persist_namespaces(state, &snapshot).await {
        return error.into_response();
    }
    ApiError::Internal(
        "Turso deployment credentials and component are required before object activation"
            .to_owned(),
    )
    .into_response()
}

/// Returns one object metadata record without activating a process.
async fn get_object(
    State(state): State<Arc<ManagementState>>,
    AxumPath((namespace_id, object_name)): AxumPath<(String, String)>,
) -> Response {
    if let Err(error) = hydrate_records(&state).await {
        return error.into_response();
    }
    let records = state.records.lock().await;
    let Some(namespace) = records.namespaces.get(&namespace_id) else {
        return ApiError::NotFound(format!("namespace {namespace_id}")).into_response();
    };
    match namespace.objects.get(&object_name).cloned() {
        Some(object) => success_response(StatusCode::OK, object),
        None => ApiError::NotFound(format!("object {object_name}")).into_response(),
    }
}

/// Persists namespace metadata independently from script module files.
async fn persist_namespaces(
    state: &ManagementState,
    namespaces: &[NamespaceRecord],
) -> Result<(), ApiError> {
    let path = state.root.join(NAMESPACE_FILE);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| ApiError::Internal(format!("create namespace root: {error}")))?;
    }
    let bytes = serde_json::to_vec_pretty(namespaces)
        .map_err(|error| ApiError::Internal(format!("encode namespace index: {error}")))?;
    tokio::fs::write(path, bytes)
        .await
        .map_err(|error| ApiError::Internal(format!("write namespace index: {error}")))
}

/// Returns the public namespace metadata shape.
impl NamespaceRecord {
    /// Converts one private record to the management response object.
    fn public(&self) -> PublicNamespace {
        PublicNamespace {
            id: self.id.clone(),
            name: self.name.clone(),
            script: self.script.clone(),
            class_name: self.class_name.clone(),
        }
    }
}

/// Public namespace response without private object state.
#[derive(Debug, Serialize)]
struct PublicNamespace {
    id: String,
    name: String,
    script: String,
    #[serde(rename = "class")]
    class_name: String,
}

/// Validates one API name against path traversal and separators.
fn validate_name(value: &str, field: &str) -> Result<(), ApiError> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.chars().any(|character| character.is_control())
        || Path::new(value)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(ApiError::BadRequest(format!("{field} is not a safe name")));
    }
    Ok(())
}

/// Validates one uploaded module path against traversal and absolute paths.
fn validate_module_path(value: &str) -> Result<(), ApiError> {
    if value.is_empty()
        || Path::new(value)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(ApiError::BadRequest(format!(
            "module path {value} is not safe"
        )));
    }
    Ok(())
}

/// Derives the deterministic API object identity from namespace and name.
fn object_identity(namespace_id: &str, object_name: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(namespace_id.as_bytes());
    digest.update([0]);
    digest.update(object_name.as_bytes());
    hex::encode(digest.finalize())
}

/// Builds a successful Cloudflare response envelope.
fn success_response<T: Serialize>(status: StatusCode, result: T) -> Response {
    Json(Envelope {
        success: true,
        errors: Vec::new(),
        messages: Vec::new(),
        result: Some(result),
    })
    .into_response()
    .with_status(status)
}

/// Builds a failed Cloudflare response envelope.
impl IntoResponse for ApiError {
    /// Converts a management error into a status and Cloudflare envelope.
    fn into_response(self) -> Response {
        let status = match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let message = self.to_string();
        (
            status,
            Json(Envelope::<Value> {
                success: false,
                errors: vec![ApiMessage {
                    code: status.as_u16(),
                    message,
                }],
                messages: Vec::new(),
                result: None,
            }),
        )
            .into_response()
    }
}

/// Adds an HTTP status to a JSON response.
trait ResponseStatus {
    /// Sets one response status without changing its body.
    fn with_status(self, status: StatusCode) -> Response;
}

impl ResponseStatus for Response {
    /// Sets one response status without changing its body.
    fn with_status(mut self, status: StatusCode) -> Response {
        *self.status_mut() = status;
        self
    }
}

impl From<SupervisorError> for ApiError {
    /// Converts an unavailable supervisor into an internal management error.
    fn from(error: SupervisorError) -> Self {
        Self::Internal(error.to_string())
    }
}
