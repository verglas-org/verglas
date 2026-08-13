//! CRaft implementations of the hosted Iceberg route service contracts.
//!
//! This module translates the standard Lakekeeper Iceberg DTOs into durable
//! JSON documents and submits every mutation as one CRaft catalog batch.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use lakekeeper::api::{
    ApiContext, ErrorModel, IcebergErrorResponse, RequestMetadata, Result,
    iceberg::v1::{
        CommitTableRequest, CommitTableResponse, CommitTransactionRequest, CommitViewRequest,
        CreateNamespaceRequest, CreateNamespaceResponse, CreateTableRequest, CreateViewRequest,
        GetNamespaceResponse, ListNamespacesResponse, ListTablesResponse, LoadTableResult,
        LoadTableResultOrNotModified, LoadViewResult, NamespaceIdent, NamespaceParameters, Prefix,
        RegisterTableRequest, RenameTableRequest, TableIdent, TableParameters,
        UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse,
        namespace::{
            GetNamespacePropertiesQuery, ListNamespacesQuery, NamespaceDropFlags, NamespaceService,
        },
        tables::{
            DataAccess, DataAccessMode, ListTablesQuery, LoadTableCredentialsRequest,
            LoadTableRequest, TablesService,
        },
        views::{LoadViewRequest, ViewParameters, ViewService},
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{json, to_string};
use uuid::Uuid;
use verglas_consensus::CatalogEntity;

use crate::{
    VerglasCatalog, VerglasCatalogError,
    domain::{NamespaceDocument, TableDocument, ViewDocument},
    idempotency::HostedIdempotency,
};

#[async_trait]
impl lakekeeper::api::iceberg::v1::config::Service<VerglasCatalog> for VerglasCatalog {
    /// Advertises the hosted CRaft route set without persisting or vending secrets.
    async fn get_config(
        _query: lakekeeper::api::iceberg::v1::config::GetConfigQueryParams,
        state: ApiContext<VerglasCatalog>,
        _request_metadata: RequestMetadata,
    ) -> Result<lakekeeper::api::CatalogConfig> {
        Ok(lakekeeper::api::CatalogConfig {
            defaults: hosted_defaults(state.v1_state.warehouse()),
            overrides: HashMap::new(),
            endpoints: hosted_endpoints().to_vec(),
        })
    }
}

/// Returns the exact route capabilities exported by the mounted Lakekeeper router.
fn hosted_endpoints() -> &'static [String] {
    lakekeeper::api::iceberg::supported_endpoints()
}

/// Builds standard client defaults from the warehouse route bound to CRaft.
fn hosted_defaults(warehouse: &str) -> HashMap<String, String> {
    HashMap::from([("prefix".to_owned(), warehouse.to_owned())])
}

/// Makes a stable warehouse-qualified namespace record key.
fn namespace_key(prefix: &Prefix, namespace: &NamespaceIdent) -> String {
    format!("{}:{}", prefix.as_str(), namespace.to_url_string())
}

/// Derives a stable UUID from the CRaft tenant-visible route identity.
fn durable_id(kind: &str, identity: &str) -> String {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("verglas:{kind}:{identity}").as_bytes(),
    )
    .to_string()
}

/// Converts durable transport failures into the standard Iceberg error model.
fn storage_error(error: VerglasCatalogError) -> IcebergErrorResponse {
    match error {
        VerglasCatalogError::Client(verglas_catalog::ManagedCatalogError::Conflict) => {
            ErrorModel::conflict(
                "catalog transaction conflicts with current state",
                "CommitFailedException",
                None,
            )
            .into()
        }
        VerglasCatalogError::IdempotencyConflict => ErrorModel::conflict(
            "idempotency key is bound to a different catalog mutation",
            "IdempotencyConflict",
            None,
        )
        .into(),
        error => ErrorModel::service_unavailable(
            format!("CRaft catalog is unavailable: {error}"),
            "CatalogUnavailable",
            Some(Box::new(error)),
        )
        .into(),
    }
}

/// The complete durable result required to replay a table commit after failover or restart.
#[derive(Deserialize, Serialize)]
struct CommitTableResult {
    metadata_location: String,
}

/// The complete durable result of a multi-table commit, even though Iceberg returns no body.
#[derive(Deserialize, Serialize)]
struct CommitTransactionResult {
    metadata_locations: Vec<String>,
}

/// Requires a routed warehouse prefix for a hosted Iceberg endpoint.
pub(crate) fn required_prefix(prefix: Option<Prefix>) -> Result<Prefix> {
    prefix.ok_or_else(|| {
        ErrorModel::bad_request("missing warehouse prefix", "MissingPrefix", None).into()
    })
}

/// Resolves one durable namespace document by its endpoint identifier.
pub(crate) async fn namespace_document(
    catalog: &VerglasCatalog,
    prefix: &Prefix,
    namespace: &NamespaceIdent,
) -> Result<(String, NamespaceDocument)> {
    let key = namespace_key(prefix, namespace);
    let document = catalog
        .read(CatalogEntity::Namespace, &key)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            ErrorModel::not_found("namespace does not exist", "NoSuchNamespaceException", None)
        })?;
    Ok((key, document))
}

/// Makes the stable CRaft record key for a table name.
fn table_key(prefix: &Prefix, table: &TableIdent) -> String {
    format!(
        "{}:{}:{}",
        prefix.as_str(),
        table.namespace.to_url_string(),
        table.name
    )
}

/// Resolves a CRaft table document by the routed Iceberg identifier.
pub(crate) async fn table_document(
    catalog: &VerglasCatalog,
    prefix: &Prefix,
    table: &TableIdent,
) -> Result<(String, TableDocument)> {
    let key = table_key(prefix, table);
    let document = catalog
        .read(CatalogEntity::Table, &key)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            ErrorModel::not_found("table does not exist", "NoSuchTableException", None)
        })?;
    Ok((key, document))
}

/// Makes the stable CRaft record key for a view name.
fn view_key(prefix: &Prefix, view: &TableIdent) -> String {
    table_key(prefix, view)
}

/// Resolves one CRaft view document by its routed Iceberg identifier.
pub(crate) async fn view_document(
    catalog: &VerglasCatalog,
    prefix: &Prefix,
    view: &TableIdent,
) -> Result<(String, ViewDocument)> {
    let key = view_key(prefix, view);
    let document = catalog
        .read(CatalogEntity::View, &key)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| ErrorModel::not_found("view does not exist", "NoSuchViewException", None))?;
    Ok((key, document))
}

#[async_trait]
impl NamespaceService<VerglasCatalog> for VerglasCatalog {
    /// Lists CRaft-owned namespaces immediately beneath the requested parent.
    async fn list_namespaces(
        prefix: Option<Prefix>,
        query: ListNamespacesQuery,
        state: ApiContext<VerglasCatalog>,
        _request_metadata: RequestMetadata,
    ) -> Result<ListNamespacesResponse> {
        let prefix = required_prefix(prefix)?;
        let mut namespaces = state
            .v1_state
            .list::<NamespaceDocument>(CatalogEntity::Namespace)
            .await
            .map_err(storage_error)?
            .into_iter()
            .filter(|(_, document)| document.warehouse_id == prefix.as_str())
            .filter(|(_, document)| match query.parent.as_ref() {
                Some(parent) => {
                    let parent = parent.as_ref();
                    document.name.len() == parent.len() + 1 && document.name.starts_with(parent)
                }
                None => document.name.len() == 1,
            })
            .map(|(_, document)| {
                NamespaceIdent::from_vec(document.name).map_err(|error| {
                    ErrorModel::internal(
                        format!("invalid namespace document: {error}"),
                        "InvalidCatalogDocument",
                        None,
                    )
                    .into()
                })
            })
            .collect::<Result<Vec<_>>>()?;
        namespaces.sort_by_key(NamespaceIdent::to_url_string);
        Ok(ListNamespacesResponse {
            next_page_token: None,
            namespaces: Arc::new(namespaces),
            namespace_uuids: None,
            protection_status: None,
        })
    }

    /// Creates a namespace through one absent-record CRaft transaction.
    async fn create_namespace(
        prefix: Option<Prefix>,
        request: CreateNamespaceRequest,
        state: ApiContext<VerglasCatalog>,
        request_metadata: RequestMetadata,
    ) -> Result<CreateNamespaceResponse> {
        let prefix = required_prefix(prefix)?;
        let key = namespace_key(&prefix, &request.namespace);
        let parent_id = match request.namespace.parent() {
            Some(parent) => Some(
                namespace_document(&state.v1_state, &prefix, &parent)
                    .await?
                    .1
                    .id,
            ),
            None => None,
        };
        let document = NamespaceDocument {
            id: durable_id("namespace", &key),
            warehouse_id: durable_id("warehouse", prefix.as_str()),
            parent_id,
            name: request.namespace.clone().inner(),
            properties: request.properties.unwrap_or_default().into_iter().collect(),
            version: 0,
            protected: false,
        };
        let encoded = to_string(&document).map_err(|error| {
            ErrorModel::internal(
                format!("cannot encode namespace: {error}"),
                "CatalogEncoding",
                None,
            )
        })?;
        let mut transaction = state
            .v1_state
            .transaction(request_metadata.request_id().as_u128());
        transaction.require_absent(CatalogEntity::Namespace, key.clone());
        transaction.put(CatalogEntity::Namespace, key, encoded);
        transaction.commit().await.map_err(|error| match error {
            VerglasCatalogError::InvalidBatch => ErrorModel::conflict(
                "namespace already exists",
                "NamespaceAlreadyExistsException",
                None,
            )
            .into(),
            other => storage_error(other),
        })?;
        Ok(CreateNamespaceResponse {
            namespace: request.namespace,
            properties: Some(document.properties.into_iter().collect::<HashMap<_, _>>()),
        })
    }

    /// Loads a namespace's authoritative durable properties.
    async fn load_namespace_metadata(
        parameters: NamespaceParameters,
        _query: GetNamespacePropertiesQuery,
        state: ApiContext<VerglasCatalog>,
        _request_metadata: RequestMetadata,
    ) -> Result<GetNamespaceResponse> {
        let prefix = required_prefix(parameters.prefix)?;
        let (_, document) =
            namespace_document(&state.v1_state, &prefix, &parameters.namespace).await?;
        Ok(GetNamespaceResponse {
            namespace: parameters.namespace,
            namespace_uuid: None,
            properties: Some(Arc::new(document.properties.into_iter().collect())),
        })
    }

    /// Confirms a namespace exists after a linearizable CRaft read.
    async fn namespace_exists(
        parameters: NamespaceParameters,
        state: ApiContext<VerglasCatalog>,
        _request_metadata: RequestMetadata,
    ) -> Result<()> {
        let prefix = required_prefix(parameters.prefix)?;
        namespace_document(&state.v1_state, &prefix, &parameters.namespace)
            .await
            .map(|_| ())
    }

    /// Removes an empty namespace through one compare-and-delete CRaft transaction.
    async fn drop_namespace(
        parameters: NamespaceParameters,
        _flags: NamespaceDropFlags,
        state: ApiContext<VerglasCatalog>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        let prefix = required_prefix(parameters.prefix)?;
        let (key, document) =
            namespace_document(&state.v1_state, &prefix, &parameters.namespace).await?;
        let expected = to_string(&document).map_err(|error| {
            ErrorModel::internal(
                format!("cannot encode namespace: {error}"),
                "CatalogEncoding",
                None,
            )
        })?;
        let mut transaction = state
            .v1_state
            .transaction(request_metadata.request_id().as_u128());
        transaction.require_document(CatalogEntity::Namespace, key.clone(), expected);
        transaction.delete(CatalogEntity::Namespace, key);
        transaction.commit().await.map_err(storage_error)
    }

    /// Applies namespace property changes through one compare-and-replace CRaft transaction.
    async fn update_namespace_properties(
        parameters: NamespaceParameters,
        request: UpdateNamespacePropertiesRequest,
        state: ApiContext<VerglasCatalog>,
        request_metadata: RequestMetadata,
    ) -> Result<UpdateNamespacePropertiesResponse> {
        let prefix = required_prefix(parameters.prefix)?;
        let (key, mut document) =
            namespace_document(&state.v1_state, &prefix, &parameters.namespace).await?;
        let expected = to_string(&document).map_err(|error| {
            ErrorModel::internal(
                format!("cannot encode namespace: {error}"),
                "CatalogEncoding",
                None,
            )
        })?;
        let removals = request.removals.unwrap_or_default();
        let updates = request.updates.unwrap_or_default();
        let mut removed = Vec::new();
        let mut missing = Vec::new();
        for property in removals {
            if document.properties.remove(&property).is_some() {
                removed.push(property);
            } else {
                missing.push(property);
            }
        }
        let updated = updates.keys().cloned().collect();
        document.properties.extend(updates);
        document.version = document.version.saturating_add(1);
        let replacement = to_string(&document).map_err(|error| {
            ErrorModel::internal(
                format!("cannot encode namespace: {error}"),
                "CatalogEncoding",
                None,
            )
        })?;
        let mut transaction = state
            .v1_state
            .transaction(request_metadata.request_id().as_u128());
        transaction.require_document(CatalogEntity::Namespace, key.clone(), expected);
        transaction.put(CatalogEntity::Namespace, key, replacement);
        transaction.commit().await.map_err(storage_error)?;
        Ok(UpdateNamespacePropertiesResponse {
            updated,
            removed,
            missing: (!missing.is_empty()).then_some(missing),
        })
    }
}

#[async_trait]
impl TablesService<VerglasCatalog> for VerglasCatalog {
    /// Lists CRaft-owned table names in one verified namespace.
    async fn list_tables(
        parameters: NamespaceParameters,
        _query: ListTablesQuery,
        state: ApiContext<VerglasCatalog>,
        _metadata: RequestMetadata,
    ) -> Result<ListTablesResponse> {
        let prefix = required_prefix(parameters.prefix)?;
        let namespace = namespace_document(&state.v1_state, &prefix, &parameters.namespace)
            .await?
            .1;
        let mut identifiers = state
            .v1_state
            .list::<TableDocument>(CatalogEntity::Table)
            .await
            .map_err(storage_error)?
            .into_iter()
            .filter(|(_, table)| table.namespace_id == namespace.id)
            .map(|(_, table)| TableIdent {
                namespace: parameters.namespace.clone(),
                name: table.name,
            })
            .collect::<Vec<_>>();
        identifiers.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(ListTablesResponse {
            next_page_token: None,
            identifiers: Arc::new(identifiers),
            table_uuids: None,
            protection_status: None,
        })
    }

    /// Creates immutable table metadata then atomically publishes its CRaft pointer.
    async fn create_table(
        parameters: NamespaceParameters,
        request: CreateTableRequest,
        _access: impl Into<DataAccessMode> + Send,
        state: ApiContext<VerglasCatalog>,
        metadata: RequestMetadata,
    ) -> Result<LoadTableResult> {
        let prefix = required_prefix(parameters.prefix)?;
        let namespace = namespace_document(&state.v1_state, &prefix, &parameters.namespace)
            .await?
            .1;
        let location = request.location.clone().unwrap_or_else(|| {
            state.v1_state.metadata().table_root(
                metadata.request_id().as_u128(),
                parameters.namespace.as_ref(),
                &request.name,
            )
        });
        let (metadata_location, table_metadata) = state
            .v1_state
            .metadata()
            .create_table(metadata.request_id().as_u128(), &location, &request)
            .await
            .map_err(|error| {
                ErrorModel::failed_dependency(
                    error.message.clone(),
                    "MetadataPublicationFailed",
                    Some(Box::new(error)),
                )
            })?;
        let ident = TableIdent {
            namespace: parameters.namespace,
            name: request.name,
        };
        let key = table_key(&prefix, &ident);
        let document = TableDocument {
            id: durable_id("table", &key),
            warehouse_id: durable_id("warehouse", prefix.as_str()),
            namespace_id: namespace.id,
            name: ident.name,
            metadata_location: metadata_location.clone(),
            location,
            version: 0,
            protected: false,
        };
        let mut transaction = state.v1_state.transaction(metadata.request_id().as_u128());
        transaction.require_absent(CatalogEntity::Table, key.clone());
        transaction.put(
            CatalogEntity::Table,
            key,
            to_string(&document).map_err(|error| {
                ErrorModel::internal(
                    format!("cannot encode table: {error}"),
                    "CatalogEncoding",
                    None,
                )
            })?,
        );
        transaction.commit().await.map_err(storage_error)?;
        Ok(LoadTableResult {
            metadata_location: Some(metadata_location),
            metadata: table_metadata,
            config: None,
            storage_credentials: None,
            credentials_revalidate_after_ms: None,
        })
    }

    /// Registers a verified immutable table metadata object before publishing its CRaft pointer.
    async fn register_table(
        parameters: NamespaceParameters,
        request: RegisterTableRequest,
        state: ApiContext<VerglasCatalog>,
        metadata: RequestMetadata,
    ) -> Result<LoadTableResult> {
        let prefix = required_prefix(parameters.prefix)?;
        let namespace = namespace_document(&state.v1_state, &prefix, &parameters.namespace)
            .await?
            .1;
        let table_metadata = state
            .v1_state
            .metadata()
            .load_table(&request.metadata_location)
            .await
            .map_err(|error| {
                ErrorModel::failed_dependency(
                    error.message.clone(),
                    "MetadataVerificationFailed",
                    Some(Box::new(error)),
                )
            })?;
        let ident = TableIdent {
            namespace: parameters.namespace,
            name: request.name,
        };
        let key = table_key(&prefix, &ident);
        let document = TableDocument {
            id: durable_id("table", &key),
            warehouse_id: durable_id("warehouse", prefix.as_str()),
            namespace_id: namespace.id,
            name: ident.name,
            metadata_location: request.metadata_location.clone(),
            location: table_metadata.location().to_owned(),
            version: 0,
            protected: false,
        };
        let mut transaction = state.v1_state.transaction(metadata.request_id().as_u128());
        transaction.require_absent(CatalogEntity::Table, key.clone());
        transaction.put(
            CatalogEntity::Table,
            key,
            to_string(&document).map_err(|error| {
                ErrorModel::internal(
                    format!("cannot encode table: {error}"),
                    "CatalogEncoding",
                    None,
                )
            })?,
        );
        transaction.commit().await.map_err(storage_error)?;
        Ok(LoadTableResult {
            metadata_location: Some(request.metadata_location),
            metadata: table_metadata,
            config: None,
            storage_credentials: None,
            credentials_revalidate_after_ms: None,
        })
    }

    /// Loads verified immutable metadata for a CRaft table pointer.
    async fn load_table(
        parameters: TableParameters,
        _request: LoadTableRequest,
        state: ApiContext<VerglasCatalog>,
        _metadata: RequestMetadata,
    ) -> Result<LoadTableResultOrNotModified> {
        let prefix = required_prefix(parameters.prefix)?;
        let (_, document) = table_document(&state.v1_state, &prefix, &parameters.table).await?;
        let table_metadata = state
            .v1_state
            .metadata()
            .load_table(&document.metadata_location)
            .await
            .map_err(|error| {
                ErrorModel::failed_dependency(
                    error.message.clone(),
                    "MetadataVerificationFailed",
                    Some(Box::new(error)),
                )
            })?;
        Ok(LoadTableResultOrNotModified::LoadTableResult(
            LoadTableResult {
                metadata_location: Some(document.metadata_location),
                metadata: table_metadata,
                config: None,
                storage_credentials: None,
                credentials_revalidate_after_ms: None,
            },
        ))
    }

    /// Does not vend credentials because CRaft catalog records never contain secrets.
    async fn load_table_credentials(
        _parameters: TableParameters,
        _request: LoadTableCredentialsRequest,
        _access: DataAccess,
        _state: ApiContext<VerglasCatalog>,
        _metadata: RequestMetadata,
    ) -> Result<lakekeeper::api::LoadCredentialsResponse> {
        Ok(lakekeeper::api::LoadCredentialsResponse {
            storage_credentials: Vec::new(),
        })
    }

    /// Publishes a new verified immutable metadata pointer through a CRaft compare-and-write.
    async fn commit_table(
        parameters: TableParameters,
        request: CommitTableRequest,
        state: ApiContext<VerglasCatalog>,
        metadata: RequestMetadata,
    ) -> Result<CommitTableResponse> {
        let prefix = required_prefix(parameters.prefix)?;
        let idempotency = metadata
            .idempotency_key()
            .copied()
            .map(|key| {
                HostedIdempotency::new(
                    key,
                    "commit_table",
                    &json!({"table": &parameters.table, "request": &request}),
                    &CommitTableResult {
                        metadata_location: String::new(),
                    },
                )
            })
            .transpose()
            .map_err(storage_error)?;
        if let Some(idempotency) = &idempotency {
            // This linearizable read only short-circuits a finalized result. The
            // ensuing write still carries RecordAbsent in the same CRaft batch.
            if let Some(result) = idempotency
                .replay::<CommitTableResult>(&state.v1_state)
                .await
                .map_err(storage_error)?
            {
                let table_metadata = state
                    .v1_state
                    .metadata()
                    .load_table(&result.metadata_location)
                    .await
                    .map_err(|error| {
                        ErrorModel::failed_dependency(
                            error.message.clone(),
                            "MetadataVerificationFailed",
                            Some(Box::new(error)),
                        )
                    })?;
                return Ok(CommitTableResponse {
                    metadata_location: result.metadata_location,
                    metadata: table_metadata,
                    config: None,
                });
            }
        }
        let (key, mut document) =
            table_document(&state.v1_state, &prefix, &parameters.table).await?;
        let expected = to_string(&document).map_err(|error| {
            ErrorModel::internal(
                format!("cannot encode table: {error}"),
                "CatalogEncoding",
                None,
            )
        })?;
        let (metadata_location, table_metadata) = state
            .v1_state
            .metadata()
            .commit_table(
                idempotency.as_ref().map_or_else(
                    || metadata.request_id().as_u128(),
                    HostedIdempotency::request_id,
                ),
                &document.metadata_location,
                &request,
            )
            .await
            .map_err(|error| {
                ErrorModel::failed_dependency(
                    error.message.clone(),
                    "MetadataPublicationFailed",
                    Some(Box::new(error)),
                )
            })?;
        document.metadata_location = metadata_location.clone();
        let result = CommitTableResult {
            metadata_location: metadata_location.clone(),
        };
        let idempotency = metadata
            .idempotency_key()
            .copied()
            .map(|key| {
                HostedIdempotency::new(
                    key,
                    "commit_table",
                    &json!({"table": &parameters.table, "request": &request}),
                    &result,
                )
            })
            .transpose()
            .map_err(storage_error)?;
        let mut transaction = state.v1_state.transaction(idempotency.as_ref().map_or_else(
            || metadata.request_id().as_u128(),
            HostedIdempotency::request_id,
        ));
        transaction.require_document(CatalogEntity::Table, key.clone(), expected);
        transaction.put(
            CatalogEntity::Table,
            key,
            to_string(&document).map_err(|error| {
                ErrorModel::internal(
                    format!("cannot encode table: {error}"),
                    "CatalogEncoding",
                    None,
                )
            })?,
        );
        if let Some(idempotency) = &idempotency {
            idempotency
                .attach(&mut transaction)
                .map_err(storage_error)?;
        }
        match transaction.commit().await {
            Ok(()) => {}
            Err(error) if error.is_conflict() => {
                let replayed = match &idempotency {
                    Some(idempotency) => idempotency
                        .replay::<CommitTableResult>(&state.v1_state)
                        .await
                        .map_err(storage_error)?,
                    None => None,
                };
                let Some(replayed) = replayed else {
                    return Err(storage_error(error));
                };
                let table_metadata = state
                    .v1_state
                    .metadata()
                    .load_table(&replayed.metadata_location)
                    .await
                    .map_err(|metadata_error| {
                        ErrorModel::failed_dependency(
                            metadata_error.message.clone(),
                            "MetadataVerificationFailed",
                            Some(Box::new(metadata_error)),
                        )
                    })?;
                return Ok(CommitTableResponse {
                    metadata_location: replayed.metadata_location,
                    metadata: table_metadata,
                    config: None,
                });
            }
            Err(error) => return Err(storage_error(error)),
        }
        Ok(CommitTableResponse {
            metadata_location,
            metadata: table_metadata,
            config: None,
        })
    }

    /// Deletes a table pointer through a CRaft compare-and-delete transaction.
    async fn drop_table(
        parameters: TableParameters,
        _drop: lakekeeper::api::iceberg::types::DropParams,
        state: ApiContext<VerglasCatalog>,
        metadata: RequestMetadata,
    ) -> Result<()> {
        let prefix = required_prefix(parameters.prefix)?;
        let (key, document) = table_document(&state.v1_state, &prefix, &parameters.table).await?;
        let mut transaction = state.v1_state.transaction(metadata.request_id().as_u128());
        transaction.require_document(
            CatalogEntity::Table,
            key.clone(),
            to_string(&document).map_err(|error| {
                ErrorModel::internal(
                    format!("cannot encode table: {error}"),
                    "CatalogEncoding",
                    None,
                )
            })?,
        );
        transaction.delete(CatalogEntity::Table, key);
        transaction.commit().await.map_err(storage_error)
    }

    /// Confirms a table pointer exists after a linearizable CRaft read.
    async fn table_exists(
        parameters: TableParameters,
        state: ApiContext<VerglasCatalog>,
        _metadata: RequestMetadata,
    ) -> Result<()> {
        let prefix = required_prefix(parameters.prefix)?;
        table_document(&state.v1_state, &prefix, &parameters.table)
            .await
            .map(|_| ())
    }

    /// Renames a table in one CRaft transaction by moving its durable record.
    async fn rename_table(
        prefix: Option<Prefix>,
        request: RenameTableRequest,
        state: ApiContext<VerglasCatalog>,
        metadata: RequestMetadata,
    ) -> Result<()> {
        let prefix = required_prefix(prefix)?;
        let (source_key, mut document) =
            table_document(&state.v1_state, &prefix, &request.source).await?;
        let expected = to_string(&document).map_err(|error| {
            ErrorModel::internal(
                format!("cannot encode table: {error}"),
                "CatalogEncoding",
                None,
            )
        })?;
        let destination_key = table_key(&prefix, &request.destination);
        document.id = destination_key.clone();
        document.name = request.destination.name;
        let mut transaction = state.v1_state.transaction(metadata.request_id().as_u128());
        transaction.require_document(CatalogEntity::Table, source_key.clone(), expected);
        transaction.require_absent(CatalogEntity::Table, destination_key.clone());
        transaction.delete(CatalogEntity::Table, source_key);
        transaction.put(
            CatalogEntity::Table,
            destination_key,
            to_string(&document).map_err(|error| {
                ErrorModel::internal(
                    format!("cannot encode table: {error}"),
                    "CatalogEncoding",
                    None,
                )
            })?,
        );
        transaction.commit().await.map_err(storage_error)
    }

    /// Commits a transaction through the metadata authority; multi-table metadata commits are delegated as one request.
    async fn commit_transaction(
        prefix: Option<Prefix>,
        request: CommitTransactionRequest,
        state: ApiContext<VerglasCatalog>,
        metadata: RequestMetadata,
    ) -> Result<()> {
        let prefix = required_prefix(prefix)?;
        let idempotency_input = json!({"prefix": prefix.as_str(), "request": &request});
        let idempotency = metadata
            .idempotency_key()
            .copied()
            .map(|key| {
                HostedIdempotency::new(
                    key,
                    "commit_transaction",
                    &idempotency_input,
                    &CommitTransactionResult {
                        metadata_locations: Vec::new(),
                    },
                )
            })
            .transpose()
            .map_err(storage_error)?;
        if let Some(idempotency) = &idempotency {
            // This only reads a finalized record; RecordAbsent below remains the
            // authoritative protection against concurrent same-key submissions.
            if idempotency
                .replay::<CommitTransactionResult>(&state.v1_state)
                .await
                .map_err(storage_error)?
                .is_some()
            {
                return Ok(());
            }
        }
        let mut records = Vec::with_capacity(request.table_changes.len());
        let mut metadata_changes = Vec::with_capacity(request.table_changes.len());
        for change in request.table_changes {
            let identifier = change.identifier.clone().ok_or_else(|| {
                ErrorModel::bad_request(
                    "each table transaction change requires an identifier",
                    "MissingTableIdentifier",
                    None,
                )
            })?;
            let (key, document) = table_document(&state.v1_state, &prefix, &identifier).await?;
            metadata_changes.push((document.metadata_location.clone(), change));
            records.push((key, document));
        }
        let published = state
            .v1_state
            .metadata()
            .commit_tables(
                idempotency.as_ref().map_or_else(
                    || metadata.request_id().as_u128(),
                    HostedIdempotency::request_id,
                ),
                &metadata_changes,
            )
            .await
            .map_err(|error| {
                ErrorModel::failed_dependency(
                    error.message.clone(),
                    "MetadataPublicationFailed",
                    Some(Box::new(error)),
                )
            })?;
        if published.len() != records.len() {
            return Err(ErrorModel::internal(
                "metadata authority returned a mismatched transaction result",
                "MetadataTransactionMismatch",
                None,
            )
            .into());
        }
        let result = CommitTransactionResult {
            metadata_locations: published
                .iter()
                .map(|(metadata_location, _)| metadata_location.clone())
                .collect(),
        };
        let idempotency = metadata
            .idempotency_key()
            .copied()
            .map(|key| {
                HostedIdempotency::new(key, "commit_transaction", &idempotency_input, &result)
            })
            .transpose()
            .map_err(storage_error)?;
        let mut transaction = state.v1_state.transaction(idempotency.as_ref().map_or_else(
            || metadata.request_id().as_u128(),
            HostedIdempotency::request_id,
        ));
        for ((key, mut document), (metadata_location, _)) in records.into_iter().zip(published) {
            let expected = to_string(&document).map_err(|error| {
                ErrorModel::internal(
                    format!("cannot encode table: {error}"),
                    "CatalogEncoding",
                    None,
                )
            })?;
            document.metadata_location = metadata_location;
            transaction.require_document(CatalogEntity::Table, key.clone(), expected);
            transaction.put(
                CatalogEntity::Table,
                key,
                to_string(&document).map_err(|error| {
                    ErrorModel::internal(
                        format!("cannot encode table: {error}"),
                        "CatalogEncoding",
                        None,
                    )
                })?,
            );
        }
        if let Some(idempotency) = &idempotency {
            idempotency
                .attach(&mut transaction)
                .map_err(storage_error)?;
        }
        match transaction.commit().await {
            Ok(()) => Ok(()),
            Err(error) if error.is_conflict() => {
                let replayed = match &idempotency {
                    Some(idempotency) => idempotency
                        .replay::<CommitTransactionResult>(&state.v1_state)
                        .await
                        .map_err(storage_error)?,
                    None => None,
                };
                if replayed.is_some() {
                    Ok(())
                } else {
                    Err(storage_error(error))
                }
            }
            Err(error) => Err(storage_error(error)),
        }
    }
}

#[async_trait]
impl ViewService<VerglasCatalog> for VerglasCatalog {
    /// Lists CRaft-owned views in a namespace.
    async fn list_views(
        parameters: NamespaceParameters,
        _query: ListTablesQuery,
        state: ApiContext<VerglasCatalog>,
        _metadata: RequestMetadata,
    ) -> Result<ListTablesResponse> {
        let prefix = required_prefix(parameters.prefix)?;
        let namespace = namespace_document(&state.v1_state, &prefix, &parameters.namespace)
            .await?
            .1;
        let identifiers = state
            .v1_state
            .list::<ViewDocument>(CatalogEntity::View)
            .await
            .map_err(storage_error)?
            .into_iter()
            .filter(|(_, view)| view.namespace_id == namespace.id)
            .map(|(_, view)| TableIdent {
                namespace: parameters.namespace.clone(),
                name: view.name,
            })
            .collect();
        Ok(ListTablesResponse {
            next_page_token: None,
            identifiers: Arc::new(identifiers),
            table_uuids: None,
            protection_status: None,
        })
    }
    /// Publishes immutable view metadata before atomically publishing its CRaft pointer.
    async fn create_view(
        parameters: NamespaceParameters,
        request: CreateViewRequest,
        state: ApiContext<VerglasCatalog>,
        _access: impl Into<DataAccessMode> + Send,
        metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        let prefix = required_prefix(parameters.prefix)?;
        let namespace = namespace_document(&state.v1_state, &prefix, &parameters.namespace)
            .await?
            .1;
        let location = request.location.clone().unwrap_or_else(|| {
            state.v1_state.metadata().view_root(
                metadata.request_id().as_u128(),
                parameters.namespace.as_ref(),
                &request.name,
            )
        });
        let (metadata_location, view_metadata) = state
            .v1_state
            .metadata()
            .create_view(metadata.request_id().as_u128(), &location, &request)
            .await
            .map_err(|error| {
                ErrorModel::failed_dependency(
                    error.message.clone(),
                    "MetadataPublicationFailed",
                    Some(Box::new(error)),
                )
            })?;
        let ident = TableIdent {
            namespace: parameters.namespace,
            name: request.name,
        };
        let key = view_key(&prefix, &ident);
        let document = ViewDocument {
            id: durable_id("view", &key),
            warehouse_id: durable_id("warehouse", prefix.as_str()),
            namespace_id: namespace.id,
            name: ident.name,
            metadata_location: metadata_location.clone(),
            version: 0,
            protected: false,
        };
        let mut transaction = state.v1_state.transaction(metadata.request_id().as_u128());
        transaction.require_absent(CatalogEntity::View, key.clone());
        transaction.put(
            CatalogEntity::View,
            key,
            to_string(&document).map_err(|error| {
                ErrorModel::internal(
                    format!("cannot encode view: {error}"),
                    "CatalogEncoding",
                    None,
                )
            })?,
        );
        transaction.commit().await.map_err(storage_error)?;
        Ok(LoadViewResult {
            metadata_location,
            metadata: view_metadata,
            config: None,
        })
    }
    /// Loads verified immutable metadata through a CRaft view pointer.
    async fn load_view(
        parameters: ViewParameters,
        _request: LoadViewRequest,
        state: ApiContext<VerglasCatalog>,
        _metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        let prefix = required_prefix(parameters.prefix)?;
        let (_, document) = view_document(&state.v1_state, &prefix, &parameters.view).await?;
        let view_metadata = state
            .v1_state
            .metadata()
            .load_view(&document.metadata_location)
            .await
            .map_err(|error| {
                ErrorModel::failed_dependency(
                    error.message.clone(),
                    "MetadataVerificationFailed",
                    Some(Box::new(error)),
                )
            })?;
        Ok(LoadViewResult {
            metadata_location: document.metadata_location,
            metadata: view_metadata,
            config: None,
        })
    }
    /// Publishes a committed immutable view object before a CRaft compare-and-write pointer update.
    async fn commit_view(
        parameters: ViewParameters,
        request: CommitViewRequest,
        state: ApiContext<VerglasCatalog>,
        _access: impl Into<DataAccessMode> + Send,
        metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        let prefix = required_prefix(parameters.prefix)?;
        let (key, mut document) = view_document(&state.v1_state, &prefix, &parameters.view).await?;
        let expected = to_string(&document).map_err(|error| {
            ErrorModel::internal(
                format!("cannot encode view: {error}"),
                "CatalogEncoding",
                None,
            )
        })?;
        let (metadata_location, view_metadata) = state
            .v1_state
            .metadata()
            .commit_view(
                metadata.request_id().as_u128(),
                &document.metadata_location,
                &request,
            )
            .await
            .map_err(|error| {
                ErrorModel::failed_dependency(
                    error.message.clone(),
                    "MetadataPublicationFailed",
                    Some(Box::new(error)),
                )
            })?;
        document.metadata_location = metadata_location.clone();
        let mut transaction = state.v1_state.transaction(metadata.request_id().as_u128());
        transaction.require_document(CatalogEntity::View, key.clone(), expected);
        transaction.put(
            CatalogEntity::View,
            key,
            to_string(&document).map_err(|error| {
                ErrorModel::internal(
                    format!("cannot encode view: {error}"),
                    "CatalogEncoding",
                    None,
                )
            })?,
        );
        transaction.commit().await.map_err(storage_error)?;
        Ok(LoadViewResult {
            metadata_location,
            metadata: view_metadata,
            config: None,
        })
    }
    /// Deletes a CRaft view pointer after a compare-and-delete check.
    async fn drop_view(
        parameters: ViewParameters,
        _drop: lakekeeper::api::iceberg::types::DropParams,
        state: ApiContext<VerglasCatalog>,
        metadata: RequestMetadata,
    ) -> Result<()> {
        let prefix = required_prefix(parameters.prefix)?;
        let (key, document) = view_document(&state.v1_state, &prefix, &parameters.view).await?;
        let mut transaction = state.v1_state.transaction(metadata.request_id().as_u128());
        transaction.require_document(
            CatalogEntity::View,
            key.clone(),
            to_string(&document).map_err(|error| {
                ErrorModel::internal(
                    format!("cannot encode view: {error}"),
                    "CatalogEncoding",
                    None,
                )
            })?,
        );
        transaction.delete(CatalogEntity::View, key);
        transaction.commit().await.map_err(storage_error)
    }
    /// Confirms a CRaft view pointer exists.
    async fn view_exists(
        parameters: ViewParameters,
        state: ApiContext<VerglasCatalog>,
        _metadata: RequestMetadata,
    ) -> Result<()> {
        let prefix = required_prefix(parameters.prefix)?;
        view_document(&state.v1_state, &prefix, &parameters.view)
            .await
            .map(|_| ())
    }
    /// Renames a view by moving its CRaft record in one atomic transaction.
    async fn rename_view(
        prefix: Option<Prefix>,
        request: RenameTableRequest,
        state: ApiContext<VerglasCatalog>,
        metadata: RequestMetadata,
    ) -> Result<()> {
        let prefix = required_prefix(prefix)?;
        let (source, mut document) =
            view_document(&state.v1_state, &prefix, &request.source).await?;
        let expected = to_string(&document).map_err(|error| {
            ErrorModel::internal(
                format!("cannot encode view: {error}"),
                "CatalogEncoding",
                None,
            )
        })?;
        let destination = view_key(&prefix, &request.destination);
        document.id = destination.clone();
        document.name = request.destination.name;
        let mut transaction = state.v1_state.transaction(metadata.request_id().as_u128());
        transaction.require_document(CatalogEntity::View, source.clone(), expected);
        transaction.require_absent(CatalogEntity::View, destination.clone());
        transaction.delete(CatalogEntity::View, source);
        transaction.put(
            CatalogEntity::View,
            destination,
            to_string(&document).map_err(|error| {
                ErrorModel::internal(
                    format!("cannot encode view: {error}"),
                    "CatalogEncoding",
                    None,
                )
            })?,
        );
        transaction.commit().await.map_err(storage_error)
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for hosted durable namespace keying.

    use lakekeeper::api::iceberg::v1::{NamespaceIdent, Prefix};

    use super::{namespace_key, storage_error};
    use crate::VerglasCatalogError;

    /// Namespace keys include the routed warehouse and every namespace level.
    #[test]
    fn namespace_key_is_warehouse_qualified() {
        let namespace = NamespaceIdent::from_vec(vec!["finance".into(), "daily".into()])
            .expect("nonempty namespace");
        assert_eq!(
            namespace_key(&Prefix::from("warehouse-a"), &namespace),
            "warehouse-a:finance\u{1f}daily"
        );
    }

    /// Stable durable IDs do not change across an exact client retry.
    #[test]
    fn durable_id_is_repeatable() {
        assert_eq!(
            super::durable_id("table", "warehouse:ns:orders"),
            super::durable_id("table", "warehouse:ns:orders")
        );
    }

    /// Hosted config uses the same complete route declaration as the mounted REST router.
    #[test]
    fn hosted_catalog_advertises_the_canonical_endpoint_set() {
        let endpoints = super::hosted_endpoints();
        assert_eq!(endpoints, lakekeeper::api::iceberg::supported_endpoints());
        assert!(
            endpoints
                .iter()
                .any(|endpoint| endpoint == "POST /v1/{prefix}/namespaces")
        );
        assert!(
            endpoints
                .iter()
                .any(|endpoint| endpoint == "POST /v1/{prefix}/namespaces/{namespace}/tables")
        );
    }

    /// Standard REST clients receive the bound warehouse route as their request prefix.
    #[test]
    fn hosted_catalog_config_sets_the_bound_warehouse_prefix() {
        assert_eq!(
            super::hosted_defaults("warehouse-a").get("prefix"),
            Some(&"warehouse-a".to_owned())
        );
    }

    /// Optimistic catalog failures use the standard Iceberg commit-conflict response.
    #[test]
    fn catalog_conflict_maps_to_commit_failed_response() {
        let response = storage_error(VerglasCatalogError::Client(
            verglas_catalog::ManagedCatalogError::Conflict,
        ));
        assert_eq!(response.error.code, 409);
        assert_eq!(response.error.r#type, "CommitFailedException");
    }

    /// Reusing an idempotency key for different input is also a final HTTP 409.
    #[test]
    fn idempotency_conflict_maps_to_conflict_response() {
        let response = storage_error(VerglasCatalogError::IdempotencyConflict);
        assert_eq!(response.error.code, 409);
        assert_eq!(response.error.r#type, "IdempotencyConflict");
    }
}
