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
    CloudflareAuthzConfig, DecisionClient, DecisionClientError, VerglasAction,
    resource::ResourceMapper,
};

/// Lakekeeper authorizer that verifies every catalog decision locally.
#[derive(Clone, Debug)]
pub struct VerglasAuthorizer {
    server_id: ServerId,
    tenant_id: String,
    resources: ResourceMapper,
    decisions: DecisionClient,
}

impl VerglasAuthorizer {
    /// Builds a fail-closed adapter for all databases in one tenant catalog.
    pub fn try_new(server_id: ServerId, config: CloudflareAuthzConfig) -> anyhow::Result<Self> {
        if config.tenant_id.trim().is_empty() {
            anyhow::bail!("Verglas authz tenant_id cannot be empty");
        }
        if config.issuer.trim().is_empty() {
            anyhow::bail!("Cloudflare credential issuer cannot be empty");
        }
        Ok(Self {
            server_id,
            tenant_id: config.tenant_id.clone(),
            resources: ResourceMapper::new(),
            decisions: DecisionClient::new(config.issuer, &config.jwks, config.tenant_id)?,
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

    /// Acknowledges a local catalog lifecycle event without mutating Cloudflare grants.
    async fn acknowledge_catalog_lifecycle(
        &self,
        _resource_id: String,
        _parent_id: String,
    ) -> Result<()> {
        // D1 grants and the signed credential scope are authoritative. Catalog
        // resource creation must never publish policy state to the tenant plane.
        Ok(())
    }
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
        warehouse: &ResolvedWarehouse,
        _parent_namespaces: &HashMap<NamespaceId, NamespaceWithParent>,
        actions: &[(&impl AuthZNamespaceInfo, Self::NamespaceAction)],
    ) -> Result<Vec<AuthorizationDecision>, IsAllowedActionError> {
        self.reject_selected_principal(for_user)?;
        // Namespace PERMISSION checks authorize against the warehouse
        // ancestor: issued credentials are warehouse-granular (warehouse/<id>
        // or warehouse/*), and the bare "namespace/<id>" resource escaped
        // that hierarchy — a warehouse-admin token was denied every
        // namespace-level action (tables list, table auto-create), found
        // live 2026-08-19. Lifecycle bookkeeping below keeps the flat
        // namespace resource string; only the decision resource changes.
        self.check_many(
            metadata,
            actions
                .iter()
                .map(|(_, action)| {
                    (
                        self.resources.warehouse(warehouse.warehouse_id),
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
        self.acknowledge_catalog_lifecycle(
            self.resources.role(role_id),
            self.resources.project(&parent_project_id),
        )
        .await
    }

    async fn delete_role(&self, _metadata: &RequestMetadata, role_id: RoleId) -> Result<()> {
        self.acknowledge_catalog_lifecycle(self.resources.role(role_id), String::new())
            .await
    }

    async fn create_tag(
        &self,
        _metadata: &RequestMetadata,
        tag_definition_id: TagDefinitionId,
        parent_project_id: ArcProjectId,
    ) -> Result<()> {
        self.acknowledge_catalog_lifecycle(
            self.resources.tag(tag_definition_id),
            self.resources.project(&parent_project_id),
        )
        .await
    }

    async fn delete_tag(
        &self,
        _metadata: &RequestMetadata,
        tag_definition_id: TagDefinitionId,
    ) -> Result<()> {
        self.acknowledge_catalog_lifecycle(self.resources.tag(tag_definition_id), String::new())
            .await
    }

    async fn create_project(
        &self,
        _metadata: &RequestMetadata,
        project_id: &ProjectId,
    ) -> Result<()> {
        self.acknowledge_catalog_lifecycle(
            self.resources.project(project_id),
            self.resources.control_root().to_owned(),
        )
        .await
    }

    async fn delete_project(
        &self,
        _metadata: &RequestMetadata,
        project_id: &ProjectId,
    ) -> Result<()> {
        self.acknowledge_catalog_lifecycle(self.resources.project(project_id), String::new())
            .await
    }

    async fn create_warehouse(
        &self,
        _metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        _parent_project_id: &ProjectId,
    ) -> Result<()> {
        self.acknowledge_catalog_lifecycle(
            self.resources.warehouse(warehouse_id),
            format!("project/{_parent_project_id}"),
        )
        .await
    }

    async fn delete_warehouse(
        &self,
        _metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
    ) -> Result<()> {
        self.acknowledge_catalog_lifecycle(self.resources.warehouse(warehouse_id), String::new())
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
        self.acknowledge_catalog_lifecycle(self.resources.namespace(namespace_id), parent_id)
            .await
    }

    async fn delete_namespace(
        &self,
        _metadata: &RequestMetadata,
        namespace_id: NamespaceId,
    ) -> Result<()> {
        self.acknowledge_catalog_lifecycle(self.resources.namespace(namespace_id), String::new())
            .await
    }

    async fn create_table(
        &self,
        _metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        table_id: TableId,
        parent: NamespaceId,
    ) -> Result<()> {
        self.acknowledge_catalog_lifecycle(
            self.resources.table(warehouse_id, table_id),
            self.resources.namespace(parent),
        )
        .await
    }

    async fn delete_table(&self, warehouse_id: WarehouseId, table_id: TableId) -> Result<()> {
        self.acknowledge_catalog_lifecycle(
            self.resources.table(warehouse_id, table_id),
            String::new(),
        )
        .await
    }

    async fn create_view(
        &self,
        _metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        view_id: ViewId,
        parent: NamespaceId,
    ) -> Result<()> {
        self.acknowledge_catalog_lifecycle(
            self.resources.view(warehouse_id, view_id),
            self.resources.namespace(parent),
        )
        .await
    }

    async fn delete_view(&self, warehouse_id: WarehouseId, view_id: ViewId) -> Result<()> {
        self.acknowledge_catalog_lifecycle(
            self.resources.view(warehouse_id, view_id),
            String::new(),
        )
        .await
    }

    async fn create_generic_table(
        &self,
        _metadata: &RequestMetadata,
        warehouse_id: WarehouseId,
        generic_table_id: GenericTableId,
        parent: NamespaceId,
    ) -> Result<()> {
        self.acknowledge_catalog_lifecycle(
            self.resources.generic_table(warehouse_id, generic_table_id),
            self.resources.namespace(parent),
        )
        .await
    }

    async fn delete_generic_table(
        &self,
        warehouse_id: WarehouseId,
        generic_table_id: GenericTableId,
    ) -> Result<()> {
        self.acknowledge_catalog_lifecycle(
            self.resources.generic_table(warehouse_id, generic_table_id),
            String::new(),
        )
        .await
    }
}
