//! Durable principal, resource, grant, and policy-revision registry in tenant Postgres.
//!
//! OpenFGA evaluates relationships; this crate stores the canonical objects and
//! explanations in the separate `verglas_permissions` logical database.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use verglas_authz::{
    AccessCheck, AccessTokenMetadata, AccessTokenRegistry, Action, AuthorizationRepository,
    AuthzError, Grant, Principal, PrincipalKind, Resource, ResourceId, ResourceKind, SecretError,
    SecretKind, SecretMetadata, SecretRepository, StoredSecret,
};

/// Idempotent schema installed only in the platform-owned permissions database.
const SCHEMA: &str = include_str!("schema.sql");

/// Postgres-backed authorization object registry.
#[derive(Debug, Clone)]
pub struct PostgresAuthorizationRepository {
    pool: PgPool,
}

impl PostgresAuthorizationRepository {
    /// Connects to `verglas_permissions`, installs its schema, and bounds the pool.
    pub async fn connect(database_url: &str) -> Result<Self, AuthzError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await
            .map_err(database_error)?;
        sqlx::raw_sql(SCHEMA)
            .execute(&pool)
            .await
            .map_err(database_error)?;
        Ok(Self { pool })
    }

    /// Wraps an existing pool after installing the required schema.
    pub async fn from_pool(pool: PgPool) -> Result<Self, AuthzError> {
        sqlx::raw_sql(SCHEMA)
            .execute(&pool)
            .await
            .map_err(database_error)?;
        Ok(Self { pool })
    }

    /// Advances one tenant revision in the caller's transaction.
    async fn bump_policy_version(
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        tenant_id: &str,
    ) -> Result<(), AuthzError> {
        sqlx::query(
            "INSERT INTO verglas_authz.policy_versions (tenant_id, version) VALUES ($1, 1) \
             ON CONFLICT (tenant_id) DO UPDATE SET version = verglas_authz.policy_versions.version + 1",
        )
        .bind(tenant_id)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
        Ok(())
    }

    /// Loads the action set belonging to each requested grant ID.
    async fn actions_for_grants(
        &self,
        tenant_id: &str,
        grant_ids: &[String],
    ) -> Result<BTreeMap<String, BTreeSet<Action>>, AuthzError> {
        if grant_ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let rows = sqlx::query(
            "SELECT grant_id, action FROM verglas_authz.grant_actions \
             WHERE tenant_id = $1 AND grant_id = ANY($2)",
        )
        .bind(tenant_id)
        .bind(grant_ids)
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        let mut actions = BTreeMap::<String, BTreeSet<Action>>::new();
        for row in rows {
            let grant_id: String = row.get("grant_id");
            let action: String = row.get("action");
            actions
                .entry(grant_id)
                .or_default()
                .insert(decode_name(&action)?);
        }
        Ok(actions)
    }

    /// Converts one grant row plus its separately normalized actions.
    fn grant_from_row(
        row: &sqlx::postgres::PgRow,
        actions: &BTreeMap<String, BTreeSet<Action>>,
    ) -> Result<Grant, AuthzError> {
        let id: String = row.get("id");
        Ok(Grant {
            id: id.clone(),
            tenant_id: row.get("tenant_id"),
            principal_id: row.get("principal_id"),
            resource_id: row.get("resource_id"),
            actions: actions
                .get(&id)
                .cloned()
                .ok_or_else(|| AuthzError::Backend(format!("grant {id} has no action records")))?,
        })
    }
}

#[async_trait]
impl AuthorizationRepository for PostgresAuthorizationRepository {
    /// Persists one validated principal and advances the tenant policy revision.
    async fn create_principal(&self, principal: Principal) -> Result<Principal, AuthzError> {
        principal.validate()?;
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        sqlx::query(
            "INSERT INTO verglas_authz.principals (tenant_id, id, kind, parent_id) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&principal.tenant_id)
        .bind(&principal.id)
        .bind(encode_name(&principal.kind)?)
        .bind(&principal.parent_id)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        Self::bump_policy_version(&mut transaction, &principal.tenant_id).await?;
        transaction.commit().await.map_err(database_error)?;
        Ok(principal)
    }

    /// Returns one tenant-scoped principal.
    async fn get_principal(
        &self,
        tenant_id: &str,
        principal_id: &str,
    ) -> Result<Principal, AuthzError> {
        let row = sqlx::query(
            "SELECT tenant_id, id, kind, parent_id FROM verglas_authz.principals \
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant_id)
        .bind(principal_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?
        .ok_or_else(|| AuthzError::NotFound(format!("principal {principal_id}")))?;
        let kind: String = row.get("kind");
        Ok(Principal {
            tenant_id: row.get("tenant_id"),
            id: row.get("id"),
            kind: decode_name::<PrincipalKind>(&kind)?,
            parent_id: row.get("parent_id"),
        })
    }

    /// Lists principals in deterministic identifier order.
    async fn list_principals(&self, tenant_id: &str) -> Result<Vec<Principal>, AuthzError> {
        let rows = sqlx::query(
            "SELECT tenant_id, id, kind, parent_id FROM verglas_authz.principals \
             WHERE tenant_id = $1 ORDER BY id",
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        rows.into_iter()
            .map(|row| {
                let kind: String = row.get("kind");
                Ok(Principal {
                    tenant_id: row.get("tenant_id"),
                    id: row.get("id"),
                    kind: decode_name::<PrincipalKind>(&kind)?,
                    parent_id: row.get("parent_id"),
                })
            })
            .collect()
    }

    /// Deletes one principal and its durable grants in one transaction.
    async fn delete_principal(
        &self,
        tenant_id: &str,
        principal_id: &str,
    ) -> Result<(), AuthzError> {
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        let result =
            sqlx::query("DELETE FROM verglas_authz.principals WHERE tenant_id = $1 AND id = $2")
                .bind(tenant_id)
                .bind(principal_id)
                .execute(&mut *transaction)
                .await
                .map_err(database_error)?;
        if result.rows_affected() == 0 {
            return Err(AuthzError::NotFound(format!("principal {principal_id}")));
        }
        Self::bump_policy_version(&mut transaction, tenant_id).await?;
        transaction.commit().await.map_err(database_error)
    }

    /// Persists one validated resource and advances the tenant policy revision.
    async fn create_resource(&self, resource: Resource) -> Result<Resource, AuthzError> {
        resource.validate()?;
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        sqlx::query(
            "INSERT INTO verglas_authz.resources (tenant_id, id, kind, parent_id) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&resource.tenant_id)
        .bind(&resource.id)
        .bind(encode_name(&resource.kind)?)
        .bind(&resource.parent_id)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        Self::bump_policy_version(&mut transaction, &resource.tenant_id).await?;
        transaction.commit().await.map_err(database_error)?;
        Ok(resource)
    }

    /// Returns one tenant-scoped resource.
    async fn get_resource(
        &self,
        tenant_id: &str,
        resource_id: &str,
    ) -> Result<Resource, AuthzError> {
        let row = sqlx::query(
            "SELECT tenant_id, id, kind, parent_id FROM verglas_authz.resources \
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant_id)
        .bind(resource_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?
        .ok_or_else(|| AuthzError::NotFound(format!("resource {resource_id}")))?;
        let kind: String = row.get("kind");
        Ok(Resource {
            tenant_id: row.get("tenant_id"),
            id: row.get("id"),
            kind: decode_name::<ResourceKind>(&kind)?,
            parent_id: row.get("parent_id"),
        })
    }

    /// Lists resources in deterministic identifier order.
    async fn list_resources(&self, tenant_id: &str) -> Result<Vec<Resource>, AuthzError> {
        let rows = sqlx::query(
            "SELECT tenant_id, id, kind, parent_id FROM verglas_authz.resources \
             WHERE tenant_id = $1 ORDER BY id",
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        rows.into_iter()
            .map(|row| {
                let kind: String = row.get("kind");
                Ok(Resource {
                    tenant_id: row.get("tenant_id"),
                    id: row.get("id"),
                    kind: decode_name::<ResourceKind>(&kind)?,
                    parent_id: row.get("parent_id"),
                })
            })
            .collect()
    }

    /// Detects a cross-tenant resource ID without returning its metadata.
    async fn resource_exists_elsewhere(
        &self,
        tenant_id: &str,
        resource_id: &str,
    ) -> Result<bool, AuthzError> {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM verglas_authz.resources WHERE tenant_id <> $1 AND id = $2)",
        )
        .bind(tenant_id)
        .bind(resource_id)
        .fetch_one(&self.pool)
        .await
        .map_err(database_error)
    }

    /// Deletes one childless resource and its durable grants in one transaction.
    async fn delete_resource(&self, tenant_id: &str, resource_id: &str) -> Result<(), AuthzError> {
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        let result =
            sqlx::query("DELETE FROM verglas_authz.resources WHERE tenant_id = $1 AND id = $2")
                .bind(tenant_id)
                .bind(resource_id)
                .execute(&mut *transaction)
                .await
                .map_err(database_error)?;
        if result.rows_affected() == 0 {
            return Err(AuthzError::NotFound(format!("resource {resource_id}")));
        }
        Self::bump_policy_version(&mut transaction, tenant_id).await?;
        transaction.commit().await.map_err(database_error)
    }

    /// Persists one grant and its normalized action rows atomically.
    async fn create_grant(&self, grant: Grant) -> Result<Grant, AuthzError> {
        grant.validate()?;
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        sqlx::query(
            "INSERT INTO verglas_authz.grants (tenant_id, id, principal_id, resource_id) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&grant.tenant_id)
        .bind(&grant.id)
        .bind(&grant.principal_id)
        .bind(&grant.resource_id)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        for action in &grant.actions {
            sqlx::query(
                "INSERT INTO verglas_authz.grant_actions (tenant_id, grant_id, action) \
                 VALUES ($1, $2, $3)",
            )
            .bind(&grant.tenant_id)
            .bind(&grant.id)
            .bind(action.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
        }
        Self::bump_policy_version(&mut transaction, &grant.tenant_id).await?;
        transaction.commit().await.map_err(database_error)?;
        Ok(grant)
    }

    /// Returns one grant with its complete action set.
    async fn get_grant(&self, tenant_id: &str, grant_id: &str) -> Result<Grant, AuthzError> {
        let row = sqlx::query(
            "SELECT tenant_id, id, principal_id, resource_id FROM verglas_authz.grants \
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant_id)
        .bind(grant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?
        .ok_or_else(|| AuthzError::NotFound(format!("grant {grant_id}")))?;
        let ids = vec![grant_id.to_owned()];
        let actions = self.actions_for_grants(tenant_id, &ids).await?;
        Self::grant_from_row(&row, &actions)
    }

    /// Lists grants and their action sets in deterministic identifier order.
    async fn list_grants(&self, tenant_id: &str) -> Result<Vec<Grant>, AuthzError> {
        let rows = sqlx::query(
            "SELECT tenant_id, id, principal_id, resource_id FROM verglas_authz.grants \
             WHERE tenant_id = $1 ORDER BY id",
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        let ids = rows
            .iter()
            .map(|row| row.get("id"))
            .collect::<Vec<String>>();
        let actions = self.actions_for_grants(tenant_id, &ids).await?;
        rows.iter()
            .map(|row| Self::grant_from_row(row, &actions))
            .collect()
    }

    /// Deletes one durable grant and advances the tenant policy revision.
    async fn delete_grant(&self, tenant_id: &str, grant_id: &str) -> Result<(), AuthzError> {
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        let result =
            sqlx::query("DELETE FROM verglas_authz.grants WHERE tenant_id = $1 AND id = $2")
                .bind(tenant_id)
                .bind(grant_id)
                .execute(&mut *transaction)
                .await
                .map_err(database_error)?;
        if result.rows_affected() == 0 {
            return Err(AuthzError::NotFound(format!("grant {grant_id}")));
        }
        Self::bump_policy_version(&mut transaction, tenant_id).await?;
        transaction.commit().await.map_err(database_error)
    }

    /// Resolves the nearest exact or inherited grant for an explanation.
    async fn matching_grant(
        &self,
        check: &AccessCheck,
    ) -> Result<Option<(Grant, ResourceId)>, AuthzError> {
        check.validate()?;
        let rows = sqlx::query(
            "WITH RECURSIVE ancestors AS (\
               SELECT id, parent_id, 0::bigint AS depth, ARRAY[id] AS path \
                 FROM verglas_authz.resources WHERE tenant_id = $1 AND id = $2 \
               UNION ALL \
               SELECT parent.id, parent.parent_id, child.depth + 1, child.path || parent.id \
                 FROM ancestors child \
                 JOIN verglas_authz.resources parent \
                   ON parent.tenant_id = $1 AND parent.id = child.parent_id \
                WHERE NOT parent.id = ANY(child.path)\
             ) \
             SELECT grants.tenant_id, grants.id, grants.principal_id, grants.resource_id, ancestors.depth \
               FROM ancestors \
               JOIN verglas_authz.grants grants \
                 ON grants.tenant_id = $1 AND grants.resource_id = ancestors.id \
               JOIN verglas_authz.grant_actions actions \
                 ON actions.tenant_id = grants.tenant_id AND actions.grant_id = grants.id \
              WHERE grants.principal_id = $3 AND actions.action = ANY($4) \
              ORDER BY ancestors.depth, grants.id LIMIT 1",
        )
        .bind(&check.tenant_id)
        .bind(&check.resource_id)
        .bind(&check.principal_id)
        .bind(granting_action_names(check.action))
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let grant_id: String = row.get("id");
        let ids = vec![grant_id];
        let actions = self.actions_for_grants(&check.tenant_id, &ids).await?;
        let grant = Self::grant_from_row(row, &actions)?;
        let matched_resource_id = grant.resource_id.clone();
        Ok(Some((grant, matched_resource_id)))
    }

    /// Returns zero before the first tenant mutation and the durable revision afterward.
    async fn policy_version(&self, tenant_id: &str) -> Result<u64, AuthzError> {
        let version = sqlx::query_scalar::<_, i64>(
            "SELECT version FROM verglas_authz.policy_versions WHERE tenant_id = $1",
        )
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?
        .unwrap_or_default();
        u64::try_from(version)
            .map_err(|_| AuthzError::Backend("negative policy version in Postgres".to_owned()))
    }
}

#[async_trait]
impl AccessTokenRegistry for PostgresAuthorizationRepository {
    /// Persists public token metadata without accepting bearer material or a signing secret.
    async fn create_token(
        &self,
        metadata: AccessTokenMetadata,
    ) -> Result<AccessTokenMetadata, AuthzError> {
        metadata.validate()?;
        sqlx::query(
            "INSERT INTO verglas_authz.access_tokens \
             (tenant_id, id, principal_id, parent_principal_id, name, audience, policy_version, \
              run_id, created_at, expires_at, last_used_at, revoked_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL, NULL)",
        )
        .bind(&metadata.tenant_id)
        .bind(&metadata.id)
        .bind(&metadata.principal_id)
        .bind(&metadata.parent_principal_id)
        .bind(&metadata.name)
        .bind(&metadata.audience)
        .bind(i64::try_from(metadata.policy_version).map_err(|_| {
            AuthzError::Invalid("token policy version exceeds Postgres range".to_owned())
        })?)
        .bind(&metadata.run_id)
        .bind(i64::try_from(metadata.created_at).map_err(|_| {
            AuthzError::Invalid("token creation time exceeds Postgres range".to_owned())
        })?)
        .bind(i64::try_from(metadata.expires_at).map_err(|_| {
            AuthzError::Invalid("token expiration time exceeds Postgres range".to_owned())
        })?)
        .execute(&self.pool)
        .await
        .map_err(database_error)?;
        Ok(metadata)
    }

    /// Returns one tenant-scoped token's public metadata.
    async fn get_token(
        &self,
        tenant_id: &str,
        token_id: &str,
    ) -> Result<AccessTokenMetadata, AuthzError> {
        let row = sqlx::query(
            "SELECT tenant_id, id, principal_id, parent_principal_id, name, audience, policy_version, \
                    run_id, created_at, expires_at, last_used_at, revoked_at \
             FROM verglas_authz.access_tokens WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant_id)
        .bind(token_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?
        .ok_or_else(|| AuthzError::NotFound(format!("token {token_id}")))?;
        token_metadata_from_row(&row)
    }

    /// Lists public metadata delegated from one parent in stable newest-first order.
    async fn list_tokens(
        &self,
        tenant_id: &str,
        parent_principal_id: &str,
    ) -> Result<Vec<AccessTokenMetadata>, AuthzError> {
        let rows = sqlx::query(
            "SELECT tenant_id, id, principal_id, parent_principal_id, name, audience, policy_version, \
                    run_id, created_at, expires_at, last_used_at, revoked_at \
             FROM verglas_authz.access_tokens \
             WHERE tenant_id = $1 AND parent_principal_id = $2 \
             ORDER BY created_at DESC, id",
        )
        .bind(tenant_id)
        .bind(parent_principal_id)
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        rows.iter().map(token_metadata_from_row).collect()
    }

    /// Revokes a credential once while retaining its inventory and audit metadata.
    async fn revoke_token(
        &self,
        tenant_id: &str,
        token_id: &str,
        revoked_at: u64,
    ) -> Result<AccessTokenMetadata, AuthzError> {
        let revoked_at = i64::try_from(revoked_at).map_err(|_| {
            AuthzError::Invalid("token revocation time exceeds Postgres range".to_owned())
        })?;
        let row = sqlx::query(
            "UPDATE verglas_authz.access_tokens \
             SET revoked_at = COALESCE(revoked_at, $3) \
             WHERE tenant_id = $1 AND id = $2 AND $3 >= created_at \
             RETURNING tenant_id, id, principal_id, parent_principal_id, name, audience, policy_version, \
                       run_id, created_at, expires_at, last_used_at, revoked_at",
        )
        .bind(tenant_id)
        .bind(token_id)
        .bind(revoked_at)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?
        .ok_or_else(|| AuthzError::NotFound(format!("token {token_id}")))?;
        token_metadata_from_row(&row)
    }

    /// Advances last-use time only for an active token inside its validity interval.
    async fn record_token_use(
        &self,
        tenant_id: &str,
        token_id: &str,
        used_at: u64,
    ) -> Result<(), AuthzError> {
        let used_at = i64::try_from(used_at)
            .map_err(|_| AuthzError::Invalid("token use time exceeds Postgres range".to_owned()))?;
        let result = sqlx::query(
            "UPDATE verglas_authz.access_tokens \
             SET last_used_at = GREATEST(COALESCE(last_used_at, $3), $3) \
             WHERE tenant_id = $1 AND id = $2 AND revoked_at IS NULL \
               AND $3 >= created_at AND $3 <= expires_at",
        )
        .bind(tenant_id)
        .bind(token_id)
        .bind(used_at)
        .execute(&self.pool)
        .await
        .map_err(database_error)?;
        if result.rows_affected() == 0 {
            return Err(AuthzError::Token("token is not active".to_owned()));
        }
        Ok(())
    }
}

#[async_trait]
impl SecretRepository for PostgresAuthorizationRepository {
    /// Creates secret metadata and its first encrypted value in one transaction.
    async fn create(
        &self,
        metadata: SecretMetadata,
        ciphertext: Vec<u8>,
    ) -> Result<SecretMetadata, SecretError> {
        let mut transaction = self.pool.begin().await.map_err(secret_database_error)?;
        sqlx::query(
            "INSERT INTO verglas_secrets.secrets (tenant_id, id, kind, scope, current_version) \
             VALUES ($1, $2, $3, $4, 1)",
        )
        .bind(&metadata.tenant_id)
        .bind(&metadata.id)
        .bind(encode_name(&metadata.kind).map_err(secret_authz_error)?)
        .bind(&metadata.scope)
        .execute(&mut *transaction)
        .await
        .map_err(secret_database_error)?;
        sqlx::query(
            "INSERT INTO verglas_secrets.secret_versions \
             (tenant_id, secret_id, version, ciphertext) VALUES ($1, $2, 1, $3)",
        )
        .bind(&metadata.tenant_id)
        .bind(&metadata.id)
        .bind(ciphertext)
        .execute(&mut *transaction)
        .await
        .map_err(secret_database_error)?;
        transaction.commit().await.map_err(secret_database_error)?;
        Ok(metadata)
    }

    /// Appends an encrypted version while holding the current-version row lock.
    async fn replace(
        &self,
        tenant_id: &str,
        id: &str,
        ciphertext: Vec<u8>,
    ) -> Result<SecretMetadata, SecretError> {
        let mut transaction = self.pool.begin().await.map_err(secret_database_error)?;
        let row = sqlx::query(
            "SELECT kind, scope, current_version FROM verglas_secrets.secrets \
             WHERE tenant_id = $1 AND id = $2 FOR UPDATE",
        )
        .bind(tenant_id)
        .bind(id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(secret_database_error)?
        .ok_or_else(|| SecretError::NotFound(id.to_owned()))?;
        let current_version: i64 = row.get("current_version");
        let next_version = current_version
            .checked_add(1)
            .ok_or_else(|| SecretError::Backend("secret version overflow".to_owned()))?;
        sqlx::query(
            "INSERT INTO verglas_secrets.secret_versions \
             (tenant_id, secret_id, version, ciphertext) VALUES ($1, $2, $3, $4)",
        )
        .bind(tenant_id)
        .bind(id)
        .bind(next_version)
        .bind(ciphertext)
        .execute(&mut *transaction)
        .await
        .map_err(secret_database_error)?;
        sqlx::query(
            "UPDATE verglas_secrets.secrets SET current_version = $3 \
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant_id)
        .bind(id)
        .bind(next_version)
        .execute(&mut *transaction)
        .await
        .map_err(secret_database_error)?;
        transaction.commit().await.map_err(secret_database_error)?;
        let kind: String = row.get("kind");
        Ok(SecretMetadata {
            tenant_id: tenant_id.to_owned(),
            id: id.to_owned(),
            kind: decode_name(&kind).map_err(secret_authz_error)?,
            scope: row.get("scope"),
            current_version: u64::try_from(next_version)
                .map_err(|_| SecretError::Backend("negative secret version".to_owned()))?,
            resource_kind: ResourceKind::Secret,
        })
    }

    /// Returns public metadata without joining value rows.
    async fn get(&self, tenant_id: &str, id: &str) -> Result<SecretMetadata, SecretError> {
        let row = sqlx::query(
            "SELECT tenant_id, id, kind, scope, current_version \
             FROM verglas_secrets.secrets WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant_id)
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(secret_database_error)?
        .ok_or_else(|| SecretError::NotFound(id.to_owned()))?;
        secret_metadata_from_row(&row)
    }

    /// Lists public metadata without joining value rows.
    async fn list(&self, tenant_id: &str) -> Result<Vec<SecretMetadata>, SecretError> {
        sqlx::query(
            "SELECT tenant_id, id, kind, scope, current_version \
             FROM verglas_secrets.secrets WHERE tenant_id = $1 ORDER BY id",
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await
        .map_err(secret_database_error)?
        .iter()
        .map(secret_metadata_from_row)
        .collect()
    }

    /// Loads one stable resource's current encrypted value.
    async fn current(&self, tenant_id: &str, id: &str) -> Result<StoredSecret, SecretError> {
        let row = sqlx::query(
            "SELECT secrets.tenant_id, secrets.id, secrets.kind, secrets.scope, \
                    secrets.current_version, versions.ciphertext \
             FROM verglas_secrets.secrets secrets \
             JOIN verglas_secrets.secret_versions versions \
               ON versions.tenant_id = secrets.tenant_id \
              AND versions.secret_id = secrets.id \
              AND versions.version = secrets.current_version \
             WHERE secrets.tenant_id = $1 AND secrets.id = $2",
        )
        .bind(tenant_id)
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(secret_database_error)?
        .ok_or_else(|| SecretError::NotFound(id.to_owned()))?;
        Ok(StoredSecret {
            metadata: secret_metadata_from_row(&row)?,
            ciphertext: row.get("ciphertext"),
        })
    }

    /// Loads only current encrypted values for the requested provider contract.
    async fn candidates(
        &self,
        tenant_id: &str,
        kind: SecretKind,
    ) -> Result<Vec<StoredSecret>, SecretError> {
        let rows = sqlx::query(
            "SELECT secrets.tenant_id, secrets.id, secrets.kind, secrets.scope, \
                    secrets.current_version, versions.ciphertext \
             FROM verglas_secrets.secrets secrets \
             JOIN verglas_secrets.secret_versions versions \
               ON versions.tenant_id = secrets.tenant_id \
              AND versions.secret_id = secrets.id \
              AND versions.version = secrets.current_version \
             WHERE secrets.tenant_id = $1 AND secrets.kind = $2 ORDER BY secrets.id",
        )
        .bind(tenant_id)
        .bind(encode_name(&kind).map_err(secret_authz_error)?)
        .fetch_all(&self.pool)
        .await
        .map_err(secret_database_error)?;
        rows.iter()
            .map(|row| {
                Ok(StoredSecret {
                    metadata: secret_metadata_from_row(row)?,
                    ciphertext: row.get("ciphertext"),
                })
            })
            .collect()
    }

    /// Deletes metadata and all value versions through the schema cascade.
    async fn delete(&self, tenant_id: &str, id: &str) -> Result<(), SecretError> {
        let result =
            sqlx::query("DELETE FROM verglas_secrets.secrets WHERE tenant_id = $1 AND id = $2")
                .bind(tenant_id)
                .bind(id)
                .execute(&self.pool)
                .await
                .map_err(secret_database_error)?;
        if result.rows_affected() == 0 {
            return Err(SecretError::NotFound(id.to_owned()));
        }
        Ok(())
    }
}

/// Converts one public metadata row without reading a ciphertext column.
fn secret_metadata_from_row(row: &sqlx::postgres::PgRow) -> Result<SecretMetadata, SecretError> {
    let kind: String = row.get("kind");
    let current_version: i64 = row.get("current_version");
    Ok(SecretMetadata {
        tenant_id: row.get("tenant_id"),
        id: row.get("id"),
        kind: decode_name(&kind).map_err(secret_authz_error)?,
        scope: row.get("scope"),
        current_version: u64::try_from(current_version)
            .map_err(|_| SecretError::Backend("negative secret version".to_owned()))?,
        resource_kind: ResourceKind::Secret,
    })
}

/// Converts one token row without ever selecting bearer material or signing state.
fn token_metadata_from_row(row: &sqlx::postgres::PgRow) -> Result<AccessTokenMetadata, AuthzError> {
    let policy_version: i64 = row.get("policy_version");
    let created_at: i64 = row.get("created_at");
    let expires_at: i64 = row.get("expires_at");
    let last_used_at: Option<i64> = row.get("last_used_at");
    let revoked_at: Option<i64> = row.get("revoked_at");
    let metadata = AccessTokenMetadata {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        principal_id: row.get("principal_id"),
        parent_principal_id: row.get("parent_principal_id"),
        name: row.get("name"),
        audience: row.get("audience"),
        policy_version: u64::try_from(policy_version).map_err(|_| {
            AuthzError::Backend("negative token policy version in Postgres".to_owned())
        })?,
        run_id: row.get("run_id"),
        created_at: u64::try_from(created_at).map_err(|_| {
            AuthzError::Backend("negative token creation time in Postgres".to_owned())
        })?,
        expires_at: u64::try_from(expires_at).map_err(|_| {
            AuthzError::Backend("negative token expiration time in Postgres".to_owned())
        })?,
        last_used_at: last_used_at.map(u64::try_from).transpose().map_err(|_| {
            AuthzError::Backend("negative token last-use time in Postgres".to_owned())
        })?,
        revoked_at: revoked_at.map(u64::try_from).transpose().map_err(|_| {
            AuthzError::Backend("negative token revocation time in Postgres".to_owned())
        })?,
    };
    metadata.validate()?;
    Ok(metadata)
}

/// Converts an authorization serialization failure into a secret backend failure.
fn secret_authz_error(error: AuthzError) -> SecretError {
    SecretError::Backend(error.to_string())
}

/// Maps Postgres failures without ever formatting bound secret values.
fn secret_database_error(error: sqlx::Error) -> SecretError {
    if let Some(database) = error.as_database_error() {
        return match database.code().as_deref() {
            Some("23505") | Some("23503") => SecretError::Conflict(database.message().to_owned()),
            _ => SecretError::Backend(database.message().to_owned()),
        };
    }
    SecretError::Backend(error.to_string())
}

/// Serializes a snake-case enum using its public JSON contract.
fn encode_name<T: Serialize>(value: &T) -> Result<String, AuthzError> {
    serde_json::to_value(value)
        .map_err(|error| AuthzError::Backend(error.to_string()))?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| {
            AuthzError::Backend("authorization enum did not serialize as text".to_owned())
        })
}

/// Deserializes a snake-case enum stored as text.
fn decode_name<T: DeserializeOwned>(value: &str) -> Result<T, AuthzError> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(|error| AuthzError::Backend(error.to_string()))
}

/// Returns direct and implied action names that satisfy one request.
fn granting_action_names(requested: Action) -> Vec<&'static str> {
    [
        Action::Discover,
        Action::Describe,
        Action::Query,
        Action::Append,
        Action::Modify,
        Action::CreateChild,
        Action::Execute,
        Action::UseSecret,
        Action::Deploy,
        Action::Connect,
        Action::PassGrants,
        Action::ManageGrants,
        Action::Own,
    ]
    .into_iter()
    .filter(|granted| granted.covers(requested))
    .map(Action::as_str)
    .collect()
}

/// Maps constraint and connectivity failures without leaking credentials.
fn database_error(error: sqlx::Error) -> AuthzError {
    if let Some(database) = error.as_database_error() {
        return match database.code().as_deref() {
            Some("23505") => AuthzError::Conflict(database.message().to_owned()),
            Some("23503") => AuthzError::Conflict(database.message().to_owned()),
            _ => AuthzError::Backend(database.message().to_owned()),
        };
    }
    AuthzError::Backend(error.to_string())
}
