//! Hosted-state wrapper that keeps CRaft storage and authorization co-located.
//!
//! Route implementations use this state to make Lakekeeper authorization
//! decisions before they expose or mutate consensus-backed catalog documents.

use std::{
    collections::{BTreeMap, HashMap},
    ops::Deref,
    sync::Arc,
};

use lakekeeper::{
    api::{
        ApiContext, ErrorModel, RequestMetadata, Result,
        iceberg::v1::{
            CommitTableRequest, CommitTableResponse, CommitTransactionRequest, CommitViewRequest,
            CreateNamespaceRequest, CreateNamespaceResponse, CreateTableRequest, CreateViewRequest,
            GetNamespaceResponse, ListNamespacesResponse, ListTablesResponse, LoadTableResult,
            LoadTableResultOrNotModified, LoadViewResult, Prefix, RegisterTableRequest,
            RenameTableRequest, TableIdent, TableParameters, UpdateNamespacePropertiesRequest,
            UpdateNamespacePropertiesResponse,
            namespace::{
                GetNamespacePropertiesQuery, ListNamespacesQuery, NamespaceDropFlags,
                NamespaceParameters, NamespaceService,
            },
            tables::{
                DataAccess, DataAccessMode, ListTablesQuery, LoadTableCredentialsRequest,
                LoadTableRequest, TablesService,
            },
            views::{LoadViewRequest, ViewParameters, ViewService},
        },
    },
    service::{
        AuthZTableInfo, AuthZViewInfo, Namespace, NamespaceHierarchy, NamespaceId,
        NamespaceVersion, NamespaceWithParent, ResolvedWarehouse, TableId, ViewId, WarehouseId,
        authz::{
            AuthZTableOps, AuthZViewOps, Authorizer, AuthzNamespaceOps, AuthzWarehouseOps,
            CatalogNamespaceAction, CatalogTableAction, CatalogViewAction, CatalogWarehouseAction,
        },
    },
};

use crate::{
    VerglasCatalog,
    domain::{NamespaceDocument, TableDocument, ViewDocument},
    hosted::{namespace_document, required_prefix, table_document, view_document},
};

/// CRaft table document projected to Lakekeeper's table authorization contract.
struct CraftTableInfo {
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    table_id: TableId,
    ident: lakekeeper::api::iceberg::v1::TableIdent,
    protected: bool,
    properties: HashMap<String, String>,
}
impl AuthZTableInfo for CraftTableInfo {
    fn warehouse_id(&self) -> WarehouseId {
        self.warehouse_id
    }
    fn table_ident(&self) -> &lakekeeper::api::iceberg::v1::TableIdent {
        &self.ident
    }
    fn table_id(&self) -> TableId {
        self.table_id
    }
    fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }
    fn is_protected(&self) -> bool {
        self.protected
    }
    fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

/// CRaft view document projected to Lakekeeper's view authorization contract.
struct CraftViewInfo {
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    view_id: ViewId,
    ident: lakekeeper::api::iceberg::v1::TableIdent,
    protected: bool,
    properties: HashMap<String, String>,
}
impl AuthZViewInfo for CraftViewInfo {
    fn warehouse_id(&self) -> WarehouseId {
        self.warehouse_id
    }
    fn view_ident(&self) -> &lakekeeper::api::iceberg::v1::TableIdent {
        &self.ident
    }
    fn view_id(&self) -> ViewId {
        self.view_id
    }
    fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }
    fn is_protected(&self) -> bool {
        self.protected
    }
    fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

/// Converts one durable namespace document to Lakekeeper authorization context.
fn namespace_info(
    document: NamespaceDocument,
) -> std::result::Result<NamespaceWithParent, lakekeeper::api::IcebergErrorResponse> {
    let namespace_id = NamespaceId::from_str_or_internal(&document.id)
        .map_err(lakekeeper::api::IcebergErrorResponse::from)?;
    let warehouse_id = WarehouseId::from_str_or_internal(&document.warehouse_id)
        .map_err(lakekeeper::api::IcebergErrorResponse::from)?;
    let ident =
        lakekeeper::api::iceberg::v1::NamespaceIdent::from_vec(document.name).map_err(|error| {
            lakekeeper::api::ErrorModel::internal(
                format!("invalid CRaft namespace: {error}"),
                "InvalidCatalogDocument",
                None,
            )
        })?;
    let parent = document
        .parent_id
        .map(|id| {
            NamespaceId::from_str_or_internal(&id).map(|parent| (parent, NamespaceVersion::from(0)))
        })
        .transpose()
        .map_err(lakekeeper::api::IcebergErrorResponse::from)?;
    Ok(NamespaceWithParent {
        namespace: Arc::new(Namespace {
            namespace_ident: ident,
            protected: document.protected,
            namespace_id,
            warehouse_id,
            properties: Some(document.properties.into_iter().collect()),
            created_at: chrono::Utc::now(),
            updated_at: None,
            version: document.version.into(),
        }),
        parent,
        requested_ident: None,
    })
}

/// Converts one durable table document and route identity to a Lakekeeper authz resource.
fn table_info(
    document: TableDocument,
    ident: lakekeeper::api::iceberg::v1::TableIdent,
    properties: HashMap<String, String>,
) -> std::result::Result<CraftTableInfo, lakekeeper::api::IcebergErrorResponse> {
    Ok(CraftTableInfo {
        warehouse_id: WarehouseId::from_str_or_internal(&document.warehouse_id)
            .map_err(lakekeeper::api::IcebergErrorResponse::from)?,
        namespace_id: NamespaceId::from_str_or_internal(&document.namespace_id)
            .map_err(lakekeeper::api::IcebergErrorResponse::from)?,
        table_id: TableId::from_str_or_internal(&document.id)
            .map_err(lakekeeper::api::IcebergErrorResponse::from)?,
        ident,
        protected: document.protected,
        properties,
    })
}

/// Converts one durable view document and route identity to a Lakekeeper authz resource.
fn view_info(
    document: ViewDocument,
    ident: lakekeeper::api::iceberg::v1::TableIdent,
    properties: HashMap<String, String>,
) -> std::result::Result<CraftViewInfo, lakekeeper::api::IcebergErrorResponse> {
    Ok(CraftViewInfo {
        warehouse_id: WarehouseId::from_str_or_internal(&document.warehouse_id)
            .map_err(lakekeeper::api::IcebergErrorResponse::from)?,
        namespace_id: NamespaceId::from_str_or_internal(&document.namespace_id)
            .map_err(lakekeeper::api::IcebergErrorResponse::from)?,
        view_id: ViewId::from_str_or_internal(&document.id)
            .map_err(lakekeeper::api::IcebergErrorResponse::from)?,
        ident,
        protected: document.protected,
        properties,
    })
}

/// A hosted CRaft catalog paired with the authorizer for its request routes.
#[derive(Clone)]
pub struct AuthorizedVerglasCatalog<A: Authorizer + Clone> {
    catalog: VerglasCatalog,
    authorizer: A,
    warehouse: Arc<ResolvedWarehouse>,
}

impl<A: Authorizer + Clone> AuthorizedVerglasCatalog<A> {
    /// Binds one CRaft catalog to the authorizer that decides its hosted actions.
    pub fn new(catalog: VerglasCatalog, authorizer: A, warehouse: ResolvedWarehouse) -> Self {
        Self {
            catalog,
            authorizer,
            warehouse: Arc::new(warehouse),
        }
    }

    /// Returns the authorizer that must decide the route's resource action.
    pub fn authorizer(&self) -> &A {
        &self.authorizer
    }

    /// Returns the underlying CRaft storage handle for explicit composition.
    pub fn catalog(&self) -> &VerglasCatalog {
        &self.catalog
    }

    /// Returns the explicit Lakekeeper warehouse context used for auth decisions.
    pub fn warehouse(&self) -> &ResolvedWarehouse {
        &self.warehouse
    }
}

impl<A: Authorizer + Clone> Deref for AuthorizedVerglasCatalog<A> {
    type Target = VerglasCatalog;

    /// Exposes only CRaft storage operations; authorization remains explicit.
    fn deref(&self) -> &Self::Target {
        &self.catalog
    }
}

impl<A: Authorizer + Clone> lakekeeper::api::ThreadSafe for AuthorizedVerglasCatalog<A> {}

#[async_trait::async_trait]
impl<A: Authorizer + Clone>
    lakekeeper::api::iceberg::v1::config::Service<AuthorizedVerglasCatalog<A>>
    for AuthorizedVerglasCatalog<A>
{
    /// Returns the same CRaft-hosted catalog configuration as the storage service.
    async fn get_config(
        query: lakekeeper::api::iceberg::v1::config::GetConfigQueryParams,
        state: ApiContext<AuthorizedVerglasCatalog<A>>,
        metadata: RequestMetadata,
    ) -> Result<lakekeeper::api::CatalogConfig> {
        <VerglasCatalog as lakekeeper::api::iceberg::v1::config::Service<VerglasCatalog>>::get_config(
            query,
            ApiContext { v1_state: state.v1_state.catalog().clone() },
            metadata,
        ).await
    }
}

/// Converts an authorizer denial into the standard hosted Iceberg forbidden response.
fn denied() -> lakekeeper::api::IcebergErrorResponse {
    ErrorModel::forbidden(
        "not authorized for CRaft warehouse action",
        "AuthorizationDenied",
        None,
    )
    .into()
}

/// Requires the wrapper's Lakekeeper warehouse action before delegating to CRaft storage.
async fn warehouse_action<A: Authorizer + Clone>(
    state: &AuthorizedVerglasCatalog<A>,
    metadata: &RequestMetadata,
    action: CatalogWarehouseAction,
) -> Result<()> {
    let allowed = state
        .authorizer()
        .is_allowed_warehouse_action(metadata, None, state.warehouse(), action)
        .await
        .map_err(|error| {
            ErrorModel::service_unavailable(
                format!("authorization backend unavailable: {error:?}"),
                "AuthorizationUnavailable",
                None,
            )
        })?
        .into_inner();
    if allowed { Ok(()) } else { Err(denied()) }
}

/// Builds the complete namespace ancestry required by resource authorization.
async fn namespace_hierarchy(
    catalog: &VerglasCatalog,
    prefix: &Prefix,
    ident: &lakekeeper::api::iceberg::v1::NamespaceIdent,
) -> Result<NamespaceHierarchy> {
    let (_, leaf) = namespace_document(catalog, prefix, ident).await?;
    let all = catalog
        .list::<NamespaceDocument>(verglas_consensus::CatalogEntity::Namespace)
        .await
        .map_err(|error| {
            ErrorModel::service_unavailable(
                format!("CRaft catalog is unavailable: {error}"),
                "CatalogUnavailable",
                None,
            )
        })?;
    let by_id: HashMap<_, _> = all
        .into_iter()
        .map(|(_, value)| (value.id.clone(), value))
        .collect();
    let mut parents = Vec::new();
    let mut parent_id = leaf.parent_id.clone();
    while let Some(id) = parent_id {
        let document = by_id.get(&id).cloned().ok_or_else(|| {
            ErrorModel::internal(
                "CRaft namespace parent is missing",
                "InvalidCatalogDocument",
                None,
            )
        })?;
        parent_id = document.parent_id.clone();
        parents.push(namespace_info(document)?);
    }
    Ok(NamespaceHierarchy {
        namespace: namespace_info(leaf)?,
        parents,
    })
}

/// Requires one namespace-scoped action against the durable namespace hierarchy.
async fn namespace_action<A: Authorizer + Clone>(
    state: &AuthorizedVerglasCatalog<A>,
    metadata: &RequestMetadata,
    prefix: &Prefix,
    ident: &lakekeeper::api::iceberg::v1::NamespaceIdent,
    action: CatalogNamespaceAction,
) -> Result<NamespaceHierarchy> {
    let hierarchy = namespace_hierarchy(state.catalog(), prefix, ident).await?;
    let allowed = state
        .authorizer()
        .is_allowed_namespace_action(
            metadata,
            None,
            state.warehouse(),
            &hierarchy.parents,
            &hierarchy.namespace,
            action,
        )
        .await
        .map_err(|error| {
            ErrorModel::service_unavailable(
                format!("authorization backend unavailable: {error:?}"),
                "AuthorizationUnavailable",
                None,
            )
        })?
        .into_inner();
    if allowed {
        Ok(hierarchy)
    } else {
        Err(denied())
    }
}

/// Requires one table action after resolving its durable namespace and table identities.
async fn table_action<A: Authorizer + Clone>(
    state: &AuthorizedVerglasCatalog<A>,
    metadata: &RequestMetadata,
    prefix: &Prefix,
    ident: &TableIdent,
    action: CatalogTableAction,
) -> Result<()> {
    let hierarchy = namespace_hierarchy(state.catalog(), prefix, &ident.namespace).await?;
    let (_, document) = table_document(state.catalog(), prefix, ident).await?;
    let metadata_document = state
        .catalog()
        .metadata()
        .load_table(&document.metadata_location)
        .await
        .map_err(|error| {
            ErrorModel::failed_dependency(error.message, "MetadataVerificationFailed", None)
        })?;
    let resource = table_info(
        document,
        ident.clone(),
        metadata_document.properties().clone(),
    )?;
    let allowed = state
        .authorizer()
        .is_allowed_table_action(
            metadata,
            None,
            state.warehouse(),
            &hierarchy,
            &resource,
            action,
        )
        .await
        .map_err(|error| {
            ErrorModel::service_unavailable(
                format!("authorization backend unavailable: {error:?}"),
                "AuthorizationUnavailable",
                None,
            )
        })?
        .into_inner();
    if allowed { Ok(()) } else { Err(denied()) }
}

/// Requires one view action after resolving its durable namespace and view identities.
async fn view_action<A: Authorizer + Clone>(
    state: &AuthorizedVerglasCatalog<A>,
    metadata: &RequestMetadata,
    prefix: &Prefix,
    ident: &TableIdent,
    action: CatalogViewAction,
) -> Result<()> {
    let hierarchy = namespace_hierarchy(state.catalog(), prefix, &ident.namespace).await?;
    let (_, document) = view_document(state.catalog(), prefix, ident).await?;
    let metadata_document = state
        .catalog()
        .metadata()
        .load_view(&document.metadata_location)
        .await
        .map_err(|error| {
            ErrorModel::failed_dependency(error.message, "MetadataVerificationFailed", None)
        })?;
    let resource = view_info(
        document,
        ident.clone(),
        metadata_document.properties().clone(),
    )?;
    let allowed = state
        .authorizer()
        .is_allowed_view_action(
            metadata,
            None,
            state.warehouse(),
            &hierarchy,
            &resource,
            action,
        )
        .await
        .map_err(|error| {
            ErrorModel::service_unavailable(
                format!("authorization backend unavailable: {error:?}"),
                "AuthorizationUnavailable",
                None,
            )
        })?
        .into_inner();
    if allowed { Ok(()) } else { Err(denied()) }
}

/// Rebinds an authorized request context to its raw CRaft storage state.
fn raw_context<A: Authorizer + Clone>(
    state: &ApiContext<AuthorizedVerglasCatalog<A>>,
) -> ApiContext<VerglasCatalog> {
    ApiContext {
        v1_state: state.v1_state.catalog().clone(),
    }
}

#[async_trait::async_trait]
impl<A: Authorizer + Clone> NamespaceService<AuthorizedVerglasCatalog<A>>
    for AuthorizedVerglasCatalog<A>
{
    /// Lists namespaces only after the caller can list this warehouse.
    async fn list_namespaces(
        prefix: Option<Prefix>,
        query: ListNamespacesQuery,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<ListNamespacesResponse> {
        warehouse_action(
            &state.v1_state,
            &metadata,
            CatalogWarehouseAction::ListNamespaces,
        )
        .await?;
        <VerglasCatalog as NamespaceService<VerglasCatalog>>::list_namespaces(
            prefix,
            query,
            ApiContext {
                v1_state: state.v1_state.catalog().clone(),
            },
            metadata,
        )
        .await
    }

    /// Creates a namespace only after the caller has the matching warehouse action.
    async fn create_namespace(
        prefix: Option<Prefix>,
        request: CreateNamespaceRequest,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<CreateNamespaceResponse> {
        let action = CatalogWarehouseAction::CreateNamespace {
            name: Some(request.namespace.to_url_string()),
            properties: std::sync::Arc::new(
                request
                    .properties
                    .clone()
                    .unwrap_or_default()
                    .into_iter()
                    .collect(),
            ),
        };
        warehouse_action(&state.v1_state, &metadata, action).await?;
        <VerglasCatalog as NamespaceService<VerglasCatalog>>::create_namespace(
            prefix,
            request,
            ApiContext {
                v1_state: state.v1_state.catalog().clone(),
            },
            metadata,
        )
        .await
    }

    /// Loads namespace metadata only after warehouse metadata access is granted.
    async fn load_namespace_metadata(
        parameters: NamespaceParameters,
        query: GetNamespacePropertiesQuery,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<GetNamespaceResponse> {
        warehouse_action(
            &state.v1_state,
            &metadata,
            CatalogWarehouseAction::GetMetadata,
        )
        .await?;
        <VerglasCatalog as NamespaceService<VerglasCatalog>>::load_namespace_metadata(
            parameters,
            query,
            ApiContext {
                v1_state: state.v1_state.catalog().clone(),
            },
            metadata,
        )
        .await
    }

    /// Tests namespace visibility only after warehouse metadata access is granted.
    async fn namespace_exists(
        parameters: NamespaceParameters,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<()> {
        warehouse_action(
            &state.v1_state,
            &metadata,
            CatalogWarehouseAction::GetMetadata,
        )
        .await?;
        <VerglasCatalog as NamespaceService<VerglasCatalog>>::namespace_exists(
            parameters,
            ApiContext {
                v1_state: state.v1_state.catalog().clone(),
            },
            metadata,
        )
        .await
    }

    /// Drops a namespace only after warehouse deletion authority is granted.
    async fn drop_namespace(
        parameters: NamespaceParameters,
        flags: NamespaceDropFlags,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<()> {
        warehouse_action(&state.v1_state, &metadata, CatalogWarehouseAction::Delete).await?;
        <VerglasCatalog as NamespaceService<VerglasCatalog>>::drop_namespace(
            parameters,
            flags,
            ApiContext {
                v1_state: state.v1_state.catalog().clone(),
            },
            metadata,
        )
        .await
    }

    /// Updates namespace properties only after warehouse metadata authority is granted.
    async fn update_namespace_properties(
        parameters: NamespaceParameters,
        request: UpdateNamespacePropertiesRequest,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<UpdateNamespacePropertiesResponse> {
        warehouse_action(
            &state.v1_state,
            &metadata,
            CatalogWarehouseAction::GetMetadata,
        )
        .await?;
        <VerglasCatalog as NamespaceService<VerglasCatalog>>::update_namespace_properties(
            parameters,
            request,
            ApiContext {
                v1_state: state.v1_state.catalog().clone(),
            },
            metadata,
        )
        .await
    }
}

#[async_trait::async_trait]
impl<A: Authorizer + Clone> TablesService<AuthorizedVerglasCatalog<A>>
    for AuthorizedVerglasCatalog<A>
{
    /// Lists tables only after the namespace grants list access.
    async fn list_tables(
        parameters: NamespaceParameters,
        query: ListTablesQuery,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<ListTablesResponse> {
        let prefix = required_prefix(parameters.prefix.clone())?;
        namespace_action(
            &state.v1_state,
            &metadata,
            &prefix,
            &parameters.namespace,
            CatalogNamespaceAction::ListTables,
        )
        .await?;
        <VerglasCatalog as TablesService<VerglasCatalog>>::list_tables(
            parameters,
            query,
            raw_context(&state),
            metadata,
        )
        .await
    }

    /// Creates a table only after the namespace grants the concrete create action.
    async fn create_table(
        parameters: NamespaceParameters,
        request: CreateTableRequest,
        data_access: impl Into<DataAccessMode> + Send,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<LoadTableResult> {
        let prefix = required_prefix(parameters.prefix.clone())?;
        namespace_action(
            &state.v1_state,
            &metadata,
            &prefix,
            &parameters.namespace,
            CatalogNamespaceAction::CreateTable {
                name: Some(request.name.clone()),
                table_id: None,
                properties: Arc::new(
                    request
                        .properties
                        .clone()
                        .unwrap_or_default()
                        .into_iter()
                        .collect(),
                ),
            },
        )
        .await?;
        <VerglasCatalog as TablesService<VerglasCatalog>>::create_table(
            parameters,
            request,
            data_access,
            raw_context(&state),
            metadata,
        )
        .await
    }

    /// Registers a table only after the namespace grants create authority.
    async fn register_table(
        parameters: NamespaceParameters,
        request: RegisterTableRequest,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<LoadTableResult> {
        let prefix = required_prefix(parameters.prefix.clone())?;
        let registered_metadata = state
            .v1_state
            .catalog()
            .metadata()
            .load_table(&request.metadata_location)
            .await
            .map_err(|error| {
                ErrorModel::failed_dependency(error.message, "MetadataVerificationFailed", None)
            })?;
        namespace_action(
            &state.v1_state,
            &metadata,
            &prefix,
            &parameters.namespace,
            CatalogNamespaceAction::CreateTable {
                name: Some(request.name.clone()),
                table_id: None,
                properties: Arc::new(
                    registered_metadata
                        .properties()
                        .clone()
                        .into_iter()
                        .collect(),
                ),
            },
        )
        .await?;
        <VerglasCatalog as TablesService<VerglasCatalog>>::register_table(
            parameters,
            request,
            raw_context(&state),
            metadata,
        )
        .await
    }

    /// Loads table metadata only after the table grants metadata access.
    async fn load_table(
        parameters: TableParameters,
        request: LoadTableRequest,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<LoadTableResultOrNotModified> {
        let prefix = required_prefix(parameters.prefix.clone())?;
        table_action(
            &state.v1_state,
            &metadata,
            &prefix,
            &parameters.table,
            CatalogTableAction::GetMetadata,
        )
        .await?;
        <VerglasCatalog as TablesService<VerglasCatalog>>::load_table(
            parameters,
            request,
            raw_context(&state),
            metadata,
        )
        .await
    }

    /// Vends table credentials only after the table grants data reads.
    async fn load_table_credentials(
        parameters: TableParameters,
        request: LoadTableCredentialsRequest,
        data_access: DataAccess,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<lakekeeper::api::LoadCredentialsResponse> {
        let prefix = required_prefix(parameters.prefix.clone())?;
        table_action(
            &state.v1_state,
            &metadata,
            &prefix,
            &parameters.table,
            CatalogTableAction::ReadData,
        )
        .await?;
        <VerglasCatalog as TablesService<VerglasCatalog>>::load_table_credentials(
            parameters,
            request,
            data_access,
            raw_context(&state),
            metadata,
        )
        .await
    }

    /// Commits table metadata only after the table grants commit authority.
    async fn commit_table(
        parameters: TableParameters,
        request: CommitTableRequest,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<CommitTableResponse> {
        let prefix = required_prefix(parameters.prefix.clone())?;
        let (updated_properties, removed_properties) =
            lakekeeper::server::tables::parse_table_property_updates(&request.updates);
        table_action(
            &state.v1_state,
            &metadata,
            &prefix,
            &parameters.table,
            CatalogTableAction::Commit {
                updated_properties: Arc::new(updated_properties),
                removed_properties: Arc::new(removed_properties),
                target_refs: Arc::new(lakekeeper::server::commit_tables::refs_from_updates(
                    &request.updates,
                )),
                update_kinds: Arc::new(lakekeeper::server::commit_tables::update_kinds(
                    &request.updates,
                )),
            },
        )
        .await?;
        <VerglasCatalog as TablesService<VerglasCatalog>>::commit_table(
            parameters,
            request,
            raw_context(&state),
            metadata,
        )
        .await
    }

    /// Drops a table only after the table grants the requested destructive action.
    async fn drop_table(
        parameters: TableParameters,
        drop_params: lakekeeper::api::iceberg::types::DropParams,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<()> {
        let prefix = required_prefix(parameters.prefix.clone())?;
        table_action(
            &state.v1_state,
            &metadata,
            &prefix,
            &parameters.table,
            CatalogTableAction::Drop {
                force: drop_params.force,
                purge: drop_params.purge_requested,
            },
        )
        .await?;
        <VerglasCatalog as TablesService<VerglasCatalog>>::drop_table(
            parameters,
            drop_params,
            raw_context(&state),
            metadata,
        )
        .await
    }

    /// Checks table visibility through the table metadata permission.
    async fn table_exists(
        parameters: TableParameters,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<()> {
        let prefix = required_prefix(parameters.prefix.clone())?;
        table_action(
            &state.v1_state,
            &metadata,
            &prefix,
            &parameters.table,
            CatalogTableAction::GetMetadata,
        )
        .await?;
        <VerglasCatalog as TablesService<VerglasCatalog>>::table_exists(
            parameters,
            raw_context(&state),
            metadata,
        )
        .await
    }

    /// Renames a table only after source rename and destination create checks pass.
    async fn rename_table(
        prefix: Option<Prefix>,
        request: RenameTableRequest,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<()> {
        let prefix_value = required_prefix(prefix.clone())?;
        table_action(
            &state.v1_state,
            &metadata,
            &prefix_value,
            &request.source,
            CatalogTableAction::Rename,
        )
        .await?;
        namespace_action(
            &state.v1_state,
            &metadata,
            &prefix_value,
            &request.destination.namespace,
            CatalogNamespaceAction::CreateTable {
                name: Some(request.destination.name.clone()),
                table_id: None,
                properties: Arc::new(BTreeMap::new()),
            },
        )
        .await?;
        <VerglasCatalog as TablesService<VerglasCatalog>>::rename_table(
            prefix,
            request,
            raw_context(&state),
            metadata,
        )
        .await
    }

    /// Commits a multi-table transaction only after every target grants commit authority.
    async fn commit_transaction(
        prefix: Option<Prefix>,
        request: CommitTransactionRequest,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<()> {
        let prefix_value = required_prefix(prefix.clone())?;
        for change in &request.table_changes {
            let identifier = change.identifier.as_ref().ok_or_else(|| {
                lakekeeper::api::IcebergErrorResponse::from(ErrorModel::bad_request(
                    "every table transaction change requires an identifier",
                    "MissingTableIdentifier",
                    None,
                ))
            })?;
            let (updated_properties, removed_properties) =
                lakekeeper::server::tables::parse_table_property_updates(&change.updates);
            table_action(
                &state.v1_state,
                &metadata,
                &prefix_value,
                identifier,
                CatalogTableAction::Commit {
                    updated_properties: Arc::new(updated_properties),
                    removed_properties: Arc::new(removed_properties),
                    target_refs: Arc::new(lakekeeper::server::commit_tables::refs_from_updates(
                        &change.updates,
                    )),
                    update_kinds: Arc::new(lakekeeper::server::commit_tables::update_kinds(
                        &change.updates,
                    )),
                },
            )
            .await?;
        }
        <VerglasCatalog as TablesService<VerglasCatalog>>::commit_transaction(
            prefix,
            request,
            raw_context(&state),
            metadata,
        )
        .await
    }
}

#[async_trait::async_trait]
impl<A: Authorizer + Clone> ViewService<AuthorizedVerglasCatalog<A>>
    for AuthorizedVerglasCatalog<A>
{
    /// Lists views only after the namespace grants list access.
    async fn list_views(
        parameters: NamespaceParameters,
        query: ListTablesQuery,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<ListTablesResponse> {
        let prefix = required_prefix(parameters.prefix.clone())?;
        namespace_action(
            &state.v1_state,
            &metadata,
            &prefix,
            &parameters.namespace,
            CatalogNamespaceAction::ListViews,
        )
        .await?;
        <VerglasCatalog as ViewService<VerglasCatalog>>::list_views(
            parameters,
            query,
            raw_context(&state),
            metadata,
        )
        .await
    }

    /// Creates a view only after the namespace grants create authority.
    async fn create_view(
        parameters: NamespaceParameters,
        request: CreateViewRequest,
        state: ApiContext<Self>,
        data_access: impl Into<DataAccessMode> + Send,
        metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        let prefix = required_prefix(parameters.prefix.clone())?;
        namespace_action(
            &state.v1_state,
            &metadata,
            &prefix,
            &parameters.namespace,
            CatalogNamespaceAction::CreateView {
                name: Some(request.name.clone()),
                properties: Arc::new(request.properties.clone().into_iter().collect()),
            },
        )
        .await?;
        <VerglasCatalog as ViewService<VerglasCatalog>>::create_view(
            parameters,
            request,
            raw_context(&state),
            data_access,
            metadata,
        )
        .await
    }

    /// Loads a view only after it grants metadata access.
    async fn load_view(
        parameters: ViewParameters,
        request: LoadViewRequest,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        let prefix = required_prefix(parameters.prefix.clone())?;
        view_action(
            &state.v1_state,
            &metadata,
            &prefix,
            &parameters.view,
            CatalogViewAction::GetMetadata,
        )
        .await?;
        <VerglasCatalog as ViewService<VerglasCatalog>>::load_view(
            parameters,
            request,
            raw_context(&state),
            metadata,
        )
        .await
    }

    /// Commits a view only after it grants commit authority.
    async fn commit_view(
        parameters: ViewParameters,
        request: CommitViewRequest,
        state: ApiContext<Self>,
        data_access: impl Into<DataAccessMode> + Send,
        metadata: RequestMetadata,
    ) -> Result<LoadViewResult> {
        let prefix = required_prefix(parameters.prefix.clone())?;
        let (updated_properties, removed_properties) =
            lakekeeper::server::views::commit::parse_view_property_updates(&request.updates);
        view_action(
            &state.v1_state,
            &metadata,
            &prefix,
            &parameters.view,
            CatalogViewAction::Commit {
                updated_properties: Arc::new(updated_properties),
                removed_properties: Arc::new(removed_properties),
            },
        )
        .await?;
        <VerglasCatalog as ViewService<VerglasCatalog>>::commit_view(
            parameters,
            request,
            raw_context(&state),
            data_access,
            metadata,
        )
        .await
    }

    /// Drops a view only after it grants the requested destructive action.
    async fn drop_view(
        parameters: ViewParameters,
        drop_params: lakekeeper::api::iceberg::types::DropParams,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<()> {
        let prefix = required_prefix(parameters.prefix.clone())?;
        view_action(
            &state.v1_state,
            &metadata,
            &prefix,
            &parameters.view,
            CatalogViewAction::Drop {
                force: drop_params.force,
                purge: drop_params.purge_requested,
            },
        )
        .await?;
        <VerglasCatalog as ViewService<VerglasCatalog>>::drop_view(
            parameters,
            drop_params,
            raw_context(&state),
            metadata,
        )
        .await
    }

    /// Checks view visibility through its metadata permission.
    async fn view_exists(
        parameters: ViewParameters,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<()> {
        let prefix = required_prefix(parameters.prefix.clone())?;
        view_action(
            &state.v1_state,
            &metadata,
            &prefix,
            &parameters.view,
            CatalogViewAction::GetMetadata,
        )
        .await?;
        <VerglasCatalog as ViewService<VerglasCatalog>>::view_exists(
            parameters,
            raw_context(&state),
            metadata,
        )
        .await
    }

    /// Renames a view only after source rename and destination create checks pass.
    async fn rename_view(
        prefix: Option<Prefix>,
        request: RenameTableRequest,
        state: ApiContext<Self>,
        metadata: RequestMetadata,
    ) -> Result<()> {
        let prefix_value = required_prefix(prefix.clone())?;
        view_action(
            &state.v1_state,
            &metadata,
            &prefix_value,
            &request.source,
            CatalogViewAction::Rename,
        )
        .await?;
        namespace_action(
            &state.v1_state,
            &metadata,
            &prefix_value,
            &request.destination.namespace,
            CatalogNamespaceAction::CreateView {
                name: Some(request.destination.name.clone()),
                properties: Arc::new(BTreeMap::new()),
            },
        )
        .await?;
        <VerglasCatalog as ViewService<VerglasCatalog>>::rename_view(
            prefix,
            request,
            raw_context(&state),
            metadata,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_namespace_projects_complete_authorization_identity() {
        let namespace_id = uuid::Uuid::new_v4();
        let warehouse_id = uuid::Uuid::new_v4();
        let document = NamespaceDocument {
            id: namespace_id.to_string(),
            warehouse_id: warehouse_id.to_string(),
            parent_id: None,
            name: vec!["analytics".to_owned()],
            properties: BTreeMap::from([("owner".to_owned(), "data".to_owned())]),
            version: 4,
            protected: true,
        };

        let projected = namespace_info(document).expect("valid durable namespace");
        assert_eq!(
            projected.namespace.namespace_id,
            NamespaceId::new(namespace_id)
        );
        assert_eq!(
            projected.namespace.warehouse_id,
            WarehouseId::new(warehouse_id)
        );
        assert_eq!(projected.namespace.version, NamespaceVersion::from(4));
        assert!(projected.namespace.protected);
        assert_eq!(
            projected
                .namespace
                .properties
                .as_ref()
                .and_then(|properties| properties.get("owner")),
            Some(&"data".to_owned())
        );
    }

    #[test]
    fn malformed_durable_resource_identity_fails_closed() {
        let error = table_info(
            TableDocument {
                id: "not-a-uuid".to_owned(),
                warehouse_id: uuid::Uuid::new_v4().to_string(),
                namespace_id: uuid::Uuid::new_v4().to_string(),
                name: "events".to_owned(),
                metadata_location: "s3://bucket/metadata.json".to_owned(),
                location: "s3://bucket/events".to_owned(),
                version: 0,
                protected: false,
            },
            TableIdent {
                namespace: lakekeeper::api::iceberg::v1::NamespaceIdent::new(
                    "analytics".to_owned(),
                ),
                name: "events".to_owned(),
            },
            HashMap::new(),
        );

        assert!(error.is_err());
    }
}
