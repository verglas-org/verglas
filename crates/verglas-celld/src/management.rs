//! Cloudflare Workers-shaped management API for scripts, namespaces, and objects.
//!
//! The paths intentionally omit Cloudflare's `/accounts/{account_id}` prefix: a
//! celld host is already scoped to one tenant cell. Script uploads use the
//! Cloudflare module-syntax multipart shape, while namespace and object state is
//! owned by the local supervisor. Script execution remains an extension point
//! for the future WASM or microVM runtime; this module only stores source and
//! wires object lifecycle to `verglasd`.

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

use crate::{
    ChildSpec, HostSupervisor, ReplicaRole, SupervisorError, SuspendFence, WorkerDurability,
};

const SCRIPT_ROOT: &str = "workers/scripts";
const NAMESPACE_FILE: &str = "workers/namespaces.json";

/// Shared HTTP management state for one celld host.
#[derive(Clone)]
pub struct ManagementApi {
    state: Arc<ManagementState>,
}

/// Mutable records shared by HTTP handlers without sharing request-local data.
struct ManagementState {
    root: PathBuf,
    supervisor: Arc<Mutex<HostSupervisor>>,
    durability: WorkerDurability,
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

/// One deterministic Durable Object identity and its local route state.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObjectRecord {
    id: String,
    name: String,
    do_id: String,
    #[serde(default)]
    socket_path: Option<String>,
    #[serde(default)]
    pid: Option<u32>,
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

/// Optional request body for creating an object without a path name.
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
    /// Creates a management API using replica durability rooted below `root`.
    pub fn new(root: impl AsRef<Path>, supervisor: Arc<Mutex<HostSupervisor>>) -> Self {
        let root = root.as_ref().to_path_buf();
        let durability = WorkerDurability::Replica {
            socket: root.join("management-replica.sock"),
            lease_token: "celld-management".to_owned(),
            generation: 0,
            start_sequence: 0,
            offload_dir: None,
        };
        Self::with_durability(root, supervisor, durability)
    }

    /// Creates a management API with the caller's replica durability authority.
    pub fn with_durability(
        root: impl AsRef<Path>,
        supervisor: Arc<Mutex<HostSupervisor>>,
        durability: WorkerDurability,
    ) -> Self {
        Self {
            state: Arc::new(ManagementState {
                root: root.as_ref().to_path_buf(),
                supervisor,
                durability,
                records: Mutex::new(ManagementRecords {
                    scripts: BTreeMap::new(),
                    namespaces: BTreeMap::new(),
                }),
            }),
        }
    }

    /// Builds the router for the account-prefix-free Workers management paths.
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
                "/workers/durable_objects/namespaces/{namespace_id}/objects/{object_name}/suspend",
                post(suspend_object),
            )
            .route(
                "/workers/durable_objects/namespaces/{namespace_id}/objects/{object_name}/route",
                post(get_object),
            )
            .route(
                "/workers/durable_objects/namespaces/{namespace_id}/objects/{object_name}/status",
                get(get_object),
            )
            .with_state(self.state.clone())
    }
}

/// Returns the default status for a newly loaded object record.
fn default_object_status() -> String {
    "unknown".to_owned()
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

/// Loads the persisted script and namespace indexes into the process cache.
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

/// Parses and validates metadata without accepting malformed durable-object bindings.
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

/// Validates and records a new namespace before persisting the namespace index.
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

/// Lists namespace metadata without exposing the private object registry.
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

/// Lists all instantiated objects in one namespace.
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
    let objects = namespace.objects.values().cloned().collect::<Vec<_>>();
    success_response(StatusCode::OK, objects)
}

/// Creates an object from a path name using deterministic `idFromName` semantics.
async fn create_named_object(
    State(state): State<Arc<ManagementState>>,
    AxumPath((namespace_id, object_name)): AxumPath<(String, String)>,
) -> Response {
    if let Err(error) = hydrate_records(&state).await {
        return error.into_response();
    }
    match create_object(&state, &namespace_id, object_name).await {
        Ok((object, created)) => success_response(
            if created {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            },
            object,
        ),
        Err(error) => error.into_response(),
    }
}

/// Creates a named or random object from a collection request.
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
    match create_object(&state, &namespace_id, name).await {
        Ok((object, created)) => success_response(
            if created {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            },
            object,
        ),
        Err(error) => error.into_response(),
    }
}

/// Creates one object, starts its worker, and verifies its stateful route.
async fn create_object(
    state: &ManagementState,
    namespace_id: &str,
    object_name: String,
) -> Result<(ObjectRecord, bool), ApiError> {
    validate_name(&object_name, "object name")?;
    let (object, created) = {
        let mut records = state.records.lock().await;
        let namespace = records
            .namespaces
            .get_mut(namespace_id)
            .ok_or_else(|| ApiError::NotFound(format!("namespace {namespace_id}")))?;
        if let Some(object) = namespace.objects.get(&object_name).cloned() {
            (object, false)
        } else {
            let object_hash = object_identity(namespace_id, &object_name);
            let object = ObjectRecord {
                id: object_hash.clone(),
                name: object_name.clone(),
                // Unix socket paths have a hard platform limit, so the
                // supervisor identity carries the first 64 hash bits while
                // the API still exposes the complete deterministic ID.
                do_id: format!("do-{}", &object_hash[..16]),
                socket_path: None,
                pid: None,
                status: "starting".to_owned(),
            };
            namespace.objects.insert(object_name, object.clone());
            (object, true)
        }
    };
    let spawned_for_request = if created {
        true
    } else {
        let supervised = {
            let supervisor = state.supervisor.lock().await;
            supervisor.state(&object.do_id).is_some()
        };
        if supervised {
            let refreshed = refresh_object(state, object, false).await?;
            update_object(state, namespace_id, &refreshed.0).await?;
            return Ok(refreshed);
        }
        false
    };
    // Extension point: hand stored modules to the future WASM or microVM runtime.
    let spec = ChildSpec::new(&object.do_id, 0, ReplicaRole::Leader, 0)
        .map_err(|error| ApiError::Internal(error.to_string()))?
        .with_durability(state.durability.clone())
        .map_err(|error| ApiError::Internal(error.to_string()))?;
    let spawn_result = {
        let mut supervisor = state.supervisor.lock().await;
        supervisor.spawn(spec).await
    };
    let descriptor = match spawn_result {
        Ok(descriptor) => descriptor,
        Err(error) => {
            remove_object(state, namespace_id, &object.name).await?;
            return Err(ApiError::Internal(error.to_string()));
        }
    };
    let mut object = object;
    object.socket_path = Some(descriptor.socket_path().display().to_string());
    object.pid = Some(descriptor.pid());
    object.status = "running".to_owned();
    update_object(state, namespace_id, &object).await?;
    Ok((object, spawned_for_request))
}

/// Refreshes an existing object's state through the supervisor route.
async fn refresh_object(
    state: &ManagementState,
    object: ObjectRecord,
    created: bool,
) -> Result<(ObjectRecord, bool), ApiError> {
    let route = {
        let mut supervisor = state.supervisor.lock().await;
        supervisor.route_stateful(&object.do_id)
    };
    let mut object = object;
    match route {
        Ok(path) => {
            object.socket_path = Some(path.display().to_string());
            object.pid = {
                let supervisor = state.supervisor.lock().await;
                supervisor.pid(&object.do_id)
            };
            object.status = "running".to_owned();
        }
        Err(SupervisorError::RouteFenced(_)) => {
            object.status = "suspended".to_owned();
            object.pid = None;
        }
        Err(error) => return Err(ApiError::Internal(error.to_string())),
    }
    Ok((object, created))
}

/// Updates one object's persisted in-memory route metadata.
async fn update_object(
    state: &ManagementState,
    namespace_id: &str,
    object: &ObjectRecord,
) -> Result<(), ApiError> {
    let mut records = state.records.lock().await;
    let namespace = records
        .namespaces
        .get_mut(namespace_id)
        .ok_or_else(|| ApiError::NotFound(format!("namespace {namespace_id}")))?;
    namespace
        .objects
        .insert(object.name.clone(), object.clone());
    let snapshot = records.namespaces.values().cloned().collect::<Vec<_>>();
    drop(records);
    persist_namespaces(state, &snapshot).await
}

/// Removes an object reservation after its worker failed during launch.
async fn remove_object(
    state: &ManagementState,
    namespace_id: &str,
    object_name: &str,
) -> Result<(), ApiError> {
    let mut records = state.records.lock().await;
    let namespace = records
        .namespaces
        .get_mut(namespace_id)
        .ok_or_else(|| ApiError::NotFound(format!("namespace {namespace_id}")))?;
    namespace.objects.remove(object_name);
    let snapshot = records.namespaces.values().cloned().collect::<Vec<_>>();
    drop(records);
    persist_namespaces(state, &snapshot).await
}

/// Suspends one object through the supervisor's durability fence.
async fn suspend_object(
    State(state): State<Arc<ManagementState>>,
    AxumPath((namespace_id, object_name)): AxumPath<(String, String)>,
) -> Response {
    if let Err(error) = hydrate_records(&state).await {
        return error.into_response();
    }
    let object = {
        let records = state.records.lock().await;
        let Some(namespace) = records.namespaces.get(&namespace_id) else {
            return ApiError::NotFound(format!("namespace {namespace_id}")).into_response();
        };
        let Some(object) = namespace.objects.get(&object_name) else {
            return ApiError::NotFound(format!("object {object_name}")).into_response();
        };
        object.clone()
    };
    let result = {
        let mut supervisor = state.supervisor.lock().await;
        supervisor
            .suspend(&object.do_id, SuspendFence::new(0, 0, 0))
            .await
    };
    match result {
        Ok(()) => {
            let mut object = object;
            object.status = "suspended".to_owned();
            object.pid = None;
            object.socket_path = None;
            match update_object(&state, &namespace_id, &object).await {
                Ok(()) => success_response(StatusCode::OK, object),
                Err(error) => error.into_response(),
            }
        }
        Err(error) => ApiError::Internal(error.to_string()).into_response(),
    }
}

/// Gets one object and asks the supervisor for its current stateful route.
async fn get_object(
    State(state): State<Arc<ManagementState>>,
    AxumPath((namespace_id, object_name)): AxumPath<(String, String)>,
) -> Response {
    if let Err(error) = hydrate_records(&state).await {
        return error.into_response();
    }
    let object = {
        let records = state.records.lock().await;
        let Some(namespace) = records.namespaces.get(&namespace_id) else {
            return ApiError::NotFound(format!("namespace {namespace_id}")).into_response();
        };
        let Some(object) = namespace.objects.get(&object_name).cloned() else {
            return ApiError::NotFound(format!("object {object_name}")).into_response();
        };
        object
    };
    match refresh_object(&state, object, false).await {
        Ok((object, _)) => match update_object(&state, &namespace_id, &object).await {
            Ok(()) => success_response(StatusCode::OK, object),
            Err(error) => error.into_response(),
        },
        Err(error) => error.into_response(),
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
    let encoded = serde_json::to_vec_pretty(namespaces)
        .map_err(|error| ApiError::Internal(format!("encode namespaces: {error}")))?;
    tokio::fs::write(path, encoded)
        .await
        .map_err(|error| ApiError::Internal(format!("write namespaces: {error}")))
}

/// Returns a public namespace shape without private object process fields.
impl NamespaceRecord {
    /// Converts a namespace's internal object registry to its CF metadata shape.
    fn public(&self) -> PublicNamespace {
        PublicNamespace {
            id: self.id.clone(),
            name: self.name.clone(),
            script: self.script.clone(),
            class_name: self.class_name.clone(),
        }
    }
}

/// Namespace response shape required by the Workers API.
#[derive(Debug, Serialize)]
struct PublicNamespace {
    id: String,
    name: String,
    script: String,
    #[serde(rename = "class")]
    class_name: String,
}

/// Computes a stable object ID from namespace ID and object name.
fn object_identity(namespace_id: &str, object_name: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(namespace_id.as_bytes());
    hasher.update([0]);
    hasher.update(object_name.as_bytes());
    hex::encode(hasher.finalize())
}

/// Validates a path parameter that becomes a filesystem or object identity.
fn validate_name(value: &str, field: &str) -> Result<(), ApiError> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(ApiError::BadRequest(format!("invalid {field}")));
    }
    Ok(())
}

/// Validates one relative module path without allowing traversal or aliases.
fn validate_module_path(value: &str) -> Result<(), ApiError> {
    if value.is_empty()
        || Path::new(value).components().any(|component| {
            matches!(
                component,
                Component::CurDir
                    | Component::ParentDir
                    | Component::RootDir
                    | Component::Prefix(_)
            )
        })
    {
        return Err(ApiError::BadRequest(format!("invalid module path {value}")));
    }
    Ok(())
}

/// Builds a successful Cloudflare response envelope.
fn success_response<T: Serialize>(status: StatusCode, result: T) -> Response {
    (
        status,
        Json(Envelope {
            success: true,
            errors: Vec::new(),
            messages: Vec::new(),
            result: Some(result),
        }),
    )
        .into_response()
}

/// Converts an API failure into the standard error envelope.
impl IntoResponse for ApiError {
    /// Returns the status and envelope corresponding to one management error.
    fn into_response(self) -> Response {
        let status = match &self {
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
