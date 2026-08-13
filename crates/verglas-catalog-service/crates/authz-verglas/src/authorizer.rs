use std::collections::HashMap;

use async_trait::async_trait;
use axum::Router;
use futures::future::try_join_all;
use lakekeeper::{
    api::{ApiContext, ErrorModel, IcebergErrorResponse, RequestMetadata, iceberg::v1::Result},
    service::{
        ArcProjectId, AuthZGenericTableInfo, AuthZNamespaceInfo, AuthZTableInfo, AuthZViewInfo,
        CatalogStore, GenericTableId, NamespaceId, NamespaceWithParent, ProjectId,
        ResolvedWarehouse, Role, RoleId, SecretStore, ServerId, State, TableId, TagDefinition,
        TagDefinitionId, ViewId, WarehouseId,
        authn::UserId,
        authz::{
            ActionOnGenericTable, ActionOnTable, ActionOnView, AuthorizationBackendUnavailable,
            AuthorizationDecision, Authorizer, AuthzBackendErrorOrBadRequest,
            CannotInspectPermissions, CatalogGenericTableAction, CatalogNamespaceAction,
            CatalogProjectAction, CatalogRoleAction, CatalogServerAction, CatalogTableAction,
            CatalogTagAction, CatalogUserAction, CatalogViewAction, CatalogWarehouseAction,
            IsAllowedActionError, ListProjectsResponse, NamespaceParent, UserOrRole,
        },
        events::AuthorizationFailureSource,
        health::{Health, HealthExt},
    },
};
#[cfg(feature = "open-api")]
use utoipa::OpenApi;

use crate::{
    DecisionClient, DecisionClientError, ResourceSync, SyncResourceKind, VerglasAction,
    VerglasAuthzConfig, resource::ResourceMapper,
};

/// Lakekeeper authorizer that delegates every catalog decision to Verglas RBAC.
#[derive(Clone, Debug)]
pub struct VerglasAuthorizer {
    server_id: ServerId,
    tenant_id: String,
    resources: ResourceMapper,
    decisions: DecisionClient,
}

impl VerglasAuthorizer {
    /// Builds a fail-closed adapter for all databases in one tenant catalog.
    pub fn try_new(server_id: ServerId, config: VerglasAuthzConfig) -> anyhow::Result<Self> {
        if config.tenant_id.trim().is_empty() {
            anyhow::bail!("Verglas authz tenant_id cannot be empty");
        }
        Ok(Self {
            server_id,
            tenant_id: config.tenant_id,
            resources: ResourceMapper::new(),
            decisions: DecisionClient::new(config.endpoint, config.workload_credential_file)?,
        })
    }

    async fn check(
        &self,
        metadata: &RequestMetadata,
        resource: &str,
        action: VerglasAction,
    ) -> Result<AuthorizationDecision, AuthorizationBackendUnavailable> {
        let bearer = metadata.external_bearer_token().ok_or_else(|| {
            AuthorizationBackendUnavailable::new(DecisionClientError::MissingCallerBearer)
        })?;
        let decision = self
            .decisions
            .authorize(bearer, resource, action)
            .await
            .map_err(AuthorizationBackendUnavailable::new)?;
        if decision.tenant_id != self.tenant_id {
            return Err(AuthorizationBackendUnavailable::new(
                DecisionClientError::UnexpectedTenant {
                    expected: self.tenant_id.clone(),
                    actual: decision.tenant_id,
                },
            ));
        }
        Ok(AuthorizationDecision::from(decision.allowed))
    }

    async fn check_many(
        &self,
        metadata: &RequestMetadata,
        checks: Vec<(String, VerglasAction)>,
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        try_join_all(
            checks.into_iter().map(|(resource, action)| async move {
                self.check(metadata, &resource, action).await
            }),
        )
        .await
        .map_err(Into::into)
    }

    fn reject_selected_principal(
        &self,
        selected: Option<&UserOrRole>,
    ) -> Result<(), IsAllowedActionError> {
        match selected {
            None => Ok(()),
            Some(selected) => Err(CannotInspectPermissions::new(&format!("{selected:?}")).into()),
        }
    }

    async fn sync_resource(
        &self,
        id: String,
        kind: SyncResourceKind,
        parent_id: String,
    ) -> Result<()> {
        self.decisions
            .upsert_resource(&ResourceSync::new(id, kind, parent_id))
            .await
            .map_err(sync_error)
    }

    async fn delete_synced_resource(&self, id: &str) -> Result<()> {
        self.decisions.delete_resource(id).await.map_err(sync_error)
    }
}

fn sync_error(error: DecisionClientError) -> IcebergErrorResponse {
    AuthorizationBackendUnavailable::new(error)
        .into_error_model()
        .into()
}

#[async_trait]
impl HealthExt for VerglasAuthorizer {
    async fn health(&self) -> Vec<Health> {
        Vec::new()
    }

    async fn update_health(&self) {}
}

#[cfg(feature = "open-api")]
#[derive(Debug, OpenApi)]
#[openapi()]
struct ApiDoc;

#[async_trait]
impl Authorizer for VerglasAuthorizer {
    type ServerAction = CatalogServerAction;
    type ProjectAction = CatalogProjectAction;
    type WarehouseAction = CatalogWarehouseAction;
    type NamespaceAction = CatalogNamespaceAction;
    type TableAction = CatalogTableAction;
    type ViewAction = CatalogViewAction;
    type GenericTableAction = CatalogGenericTableAction;
    type UserAction = CatalogUserAction;
    type RoleAction = CatalogRoleAction;
    type TagAction = CatalogTagAction;

    fn implementation_name() -> &'static str {
        "verglas"
    }

    fn uses_external_bearer_authentication(&self) -> bool {
        true
    }

    fn server_id(&self) -> ServerId {
        self.server_id
    }

    #[cfg(feature = "open-api")]
    fn api_doc() -> utoipa::openapi::OpenApi {
        ApiDoc::openapi()
    }

    fn new_router<C: CatalogStore, S: SecretStore>(&self) -> Router<ApiContext<State<Self, C, S>>> {
        Router::new()
    }

    async fn check_assume_role_impl(
        &self,
        _principal: &UserId,
        assumed_role: &Role,
        request_metadata: &RequestMetadata,
    ) -> Result<bool, AuthzBackendErrorOrBadRequest> {
        self.check(
            request_metadata,
            &self.resources.role(assumed_role.id()),
            VerglasAction::Execute,
        )
        .await
        .map(|decision| decision.allowed)
        .map_err(Into::into)
    }

    async fn can_bootstrap(&self, metadata: &RequestMetadata) -> Result<()> {
        if self
            .check(
                metadata,
                self.resources.control_root(),
                VerglasAction::ManageGrants,
            )
            .await
            .map_err(|error| IcebergErrorResponse::from(error.into_error_model()))?
            .allowed
        {
            Ok(())
        } else {
            Err(ErrorModel::forbidden(
                "Caller cannot bootstrap the catalog",
                "BootstrapForbidden",
                None,
            )
            .into())
        }
    }

    async fn bootstrap(&self, _metadata: &RequestMetadata, _is_operator: bool) -> Result<()> {
        Ok(())
    }

    async fn list_projects_impl(
        &self,
        _metadata: &RequestMetadata,
    ) -> Result<ListProjectsResponse, AuthzBackendErrorOrBadRequest> {
        Ok(ListProjectsResponse::Unsupported)
    }

    async fn can_search_users_impl(
        &self,
        metadata: &RequestMetadata,
    ) -> Result<bool, AuthzBackendErrorOrBadRequest> {
        self.check(
            metadata,
            self.resources.control_root(),
            VerglasAction::Discover,
        )
        .await
        .map(|decision| decision.allowed)
        .map_err(Into::into)
    }

    async fn are_allowed_user_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        actions: &[(&UserId, Self::UserAction)],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        self.reject_selected_principal(for_user)?;
        self.check_many(
            metadata,
            actions
                .iter()
                .map(|(user, action)| {
                    (self.resources.user(user), VerglasAction::from_user(*action))
                })
                .collect(),
        )
        .await
    }

    async fn are_allowed_role_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        actions: &[(&Role, Self::RoleAction)],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        self.reject_selected_principal(for_user)?;
        self.check_many(
            metadata,
            actions
                .iter()
                .map(|(role, action)| {
                    (
                        self.resources.role(role.id()),
                        VerglasAction::from_role(action),
                    )
                })
                .collect(),
        )
        .await
    }

    async fn are_allowed_tag_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        actions: &[(&TagDefinition, Self::TagAction)],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        self.reject_selected_principal(for_user)?;
        self.check_many(
            metadata,
            actions
                .iter()
                .map(|(tag, action)| {
                    (
                        self.resources.tag(tag.tag_definition_id),
                        VerglasAction::from_tag(action.clone()),
                    )
                })
                .collect(),
        )
        .await
    }

    async fn are_allowed_server_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        actions: &[Self::ServerAction],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        self.reject_selected_principal(for_user)?;
        self.check_many(
            metadata,
            actions
                .iter()
                .map(|action| {
                    (
                        self.resources.control_root().to_owned(),
                        VerglasAction::from_server(action),
                    )
                })
                .collect(),
        )
        .await
    }

    async fn are_allowed_project_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        actions: &[(&ArcProjectId, Self::ProjectAction)],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        self.reject_selected_principal(for_user)?;
        self.check_many(
            metadata,
            actions
                .iter()
                .map(|(project, action)| {
                    (
                        self.resources.project(project),
                        VerglasAction::from_project(action),
                    )
                })
                .collect(),
        )
        .await
    }

    async fn are_allowed_warehouse_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        actions: &[(&ResolvedWarehouse, Self::WarehouseAction)],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        self.reject_selected_principal(for_user)?;
        self.check_many(
            metadata,
            actions
                .iter()
                .map(|(warehouse, action)| {
                    (
                        self.resources.warehouse(warehouse.warehouse_id),
                        VerglasAction::from_warehouse(action),
                    )
                })
                .collect(),
        )
        .await
    }

    async fn are_allowed_namespace_actions_impl(
        &self,
        metadata: &RequestMetadata,
        for_user: Option<&UserOrRole>,
        _warehouse: &ResolvedWarehouse,
        _parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(&impl AuthZNamespaceInfo, Self::NamespaceAction)],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        self.reject_selected_principal(for_user)?;
        self.check_many(
            metadata,
            actions
                .iter()
                .map(|(namespace, action)| {
                    (
                        self.resources.namespace(namespace.namespace_id()),
                        VerglasAction::from_namespace(action),
                    )
                })
                .collect(),
        )
        .await
    }

    async fn are_allowed_table_actions_impl<A: Into<Self::TableAction> + Send + Clone + Sync>(
        &self,
        metadata: &RequestMetadata,
        warehouse: &ResolvedWarehouse,
        _parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(
            &NamespaceWithParent,
            ActionOnTable<'_, '_, impl AuthZTableInfo, A>,
        )],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        let mut checks = Vec::with_capacity(actions.len());
        for (_, item) in actions {
            self.reject_selected_principal(item.user)?;
            let action = item.action.clone().into();
            checks.push((
                self.resources
                    .table(warehouse.warehouse_id, item.info.table_id()),
                VerglasAction::from_table(&action),
            ));
        }
        self.check_many(metadata, checks).await
    }

    async fn are_allowed_view_actions_impl<A: Into<Self::ViewAction> + Send + Clone + Sync>(
        &self,
        metadata: &RequestMetadata,
        warehouse: &ResolvedWarehouse,
        _parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(
            &NamespaceWithParent,
            ActionOnView<'_, '_, impl AuthZViewInfo, A>,
        )],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        let mut checks = Vec::with_capacity(actions.len());
        for (_, item) in actions {
            self.reject_selected_principal(item.user)?;
            let action = item.action.clone().into();
            checks.push((
                self.resources
                    .view(warehouse.warehouse_id, item.info.view_id()),
                VerglasAction::from_view(&action),
            ));
        }
        self.check_many(metadata, checks).await
    }

    async fn are_allowed_generic_table_actions_impl<
        A: Into<Self::GenericTableAction> + Send + Clone + Sync,
    >(
        &self,
        metadata: &RequestMetadata,
        warehouse: &ResolvedWarehouse,
        _parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(
            &NamespaceWithParent,
            ActionOnGenericTable<'_, '_, impl AuthZGenericTableInfo, A>,
        )],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        let mut checks = Vec::with_capacity(actions.len());
        for (_, item) in actions {
            self.reject_selected_principal(item.user)?;
            let action = item.action.clone().into();
            checks.push((
                self.resources
                    .generic_table(warehouse.warehouse_id, item.info.generic_table_id()),
                VerglasAction::from_generic_table(&action),
            ));
        }
        self.check_many(metadata, checks).await
    }

    async fn create_user(&self, _metadata: &RequestMetadata, _user_id: &UserId) -> Result<()> {
        Ok(())
    }

    async fn delete_user(&self, _metadata: &RequestMetadata, _user_id: UserId) -> Result<()> {
        Ok(())
    }

    async fn create_role(
        &self,
        _metadata: &RequestMetadata,
        role_id: RoleId,
        parent_project_id: ArcProjectId,
    ) -> Result<()> {
        let resource_id = self.resources.role(role_id);
        self.sync_resource(
            resource_id.clone(),
            SyncResourceKind::Role,
            self.resources.project(&parent_project_id),
        )
        .await
    }

    async fn delete_role(&self, _metadata: &RequestMetadata, role_id: RoleId) -> Result<()> {
        self.delete_synced_resource(&self.resources.role(role_id))
            .await
    }

    async fn create_tag(
        &self,
        _metadata: &RequestMetadata,
        tag_definition_id: TagDefinitionId,
        parent_project_id: ArcProjectId,
    ) -> Result<()> {
        self.sync_resource(
            self.resources.tag(tag_definition_id),
            SyncResourceKind::Tag,
            self.resources.project(&parent_project_id),
        )
        .await
    }

    async fn delete_tag(
        &self,
        _metadata: &RequestMetadata,
        tag_definition_id: TagDefinitionId,
    ) -> Result<()> {
        self.delete_synced_resource(&self.resources.tag(tag_definition_id))
            .await
    }

    async fn create_project(
        &self,
        _metadata: &RequestMetadata,
        project_id: &ProjectId,
    ) -> Result<()> {
        self.sync_resource(
            self.resources.project(project_id),
            SyncResourceKind::Project,
            self.resources.control_root().to_owned(),
        )
        .await
    }

    async fn delete_project(
        &self,
        _metadata: &RequestMetadata,
        project_id: &ProjectId,
    ) -> Result<()> {
        self.delete_synced_resource(&self.resources.project(project_id))
            .await
    }

    async fn create_warehouse(
        &self,
        metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        _parent_project_id: &ProjectId,
    ) -> Result<()> {
        let database_id = metadata.verglas_database_id().ok_or_else(|| {
            ErrorModel::bad_request(
                "The trusted Verglas database identity is required when creating a warehouse",
                "MissingVerglasDatabaseId",
                None,
            )
        })?;
        self.sync_resource(
            self.resources.warehouse(warehouse_id),
            SyncResourceKind::Warehouse,
            format!("database/{database_id}"),
        )
        .await
    }

    async fn delete_warehouse(
        &self,
        _metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
    ) -> Result<()> {
        self.delete_synced_resource(&self.resources.warehouse(warehouse_id))
            .await
    }

    async fn create_namespace(
        &self,
        _metadata: &RequestMetadata,
        namespace_id: NamespaceId,
        parent: NamespaceParent,
    ) -> Result<()> {
        let parent_id = match parent {
            NamespaceParent::Warehouse(warehouse_id) => self.resources.warehouse(warehouse_id),
            NamespaceParent::Namespace(namespace_id) => self.resources.namespace(namespace_id),
        };
        self.sync_resource(
            self.resources.namespace(namespace_id),
            SyncResourceKind::Namespace,
            parent_id,
        )
        .await
    }

    async fn delete_namespace(
        &self,
        _metadata: &RequestMetadata,
        namespace_id: NamespaceId,
    ) -> Result<()> {
        self.delete_synced_resource(&self.resources.namespace(namespace_id))
            .await
    }

    async fn create_table(
        &self,
        _metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        table_id: TableId,
        parent: NamespaceId,
    ) -> Result<()> {
        self.sync_resource(
            self.resources.table(warehouse_id, table_id),
            SyncResourceKind::Table,
            self.resources.namespace(parent),
        )
        .await
    }

    async fn delete_table(&self, warehouse_id: WarehouseId, table_id: TableId) -> Result<()> {
        self.delete_synced_resource(&self.resources.table(warehouse_id, table_id))
            .await
    }

    async fn create_view(
        &self,
        _metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        view_id: ViewId,
        parent: NamespaceId,
    ) -> Result<()> {
        self.sync_resource(
            self.resources.view(warehouse_id, view_id),
            SyncResourceKind::View,
            self.resources.namespace(parent),
        )
        .await
    }

    async fn delete_view(&self, warehouse_id: WarehouseId, view_id: ViewId) -> Result<()> {
        self.delete_synced_resource(&self.resources.view(warehouse_id, view_id))
            .await
    }

    async fn create_generic_table(
        &self,
        _metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        generic_table_id: GenericTableId,
        parent: NamespaceId,
    ) -> Result<()> {
        self.sync_resource(
            self.resources.generic_table(warehouse_id, generic_table_id),
            SyncResourceKind::GenericTable,
            self.resources.namespace(parent),
        )
        .await
    }

    async fn delete_generic_table(
        &self,
        warehouse_id: WarehouseId,
        generic_table_id: GenericTableId,
    ) -> Result<()> {
        self.delete_synced_resource(&self.resources.generic_table(warehouse_id, generic_table_id))
            .await
    }
}
