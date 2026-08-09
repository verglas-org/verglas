//! Provisions managed Neon databases through the local desired-container runtime.
//!
//! One storage broker is shared by the tenant. Each database owns a pageserver
//! and compute container, while pages and WAL remain durable in object storage.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::{StatusCode, Url};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio_postgres::NoTls;
use verglas_container_runtime::ContainerSpec;
use verglas_database::DatabaseServiceError;

use crate::database_runtime::ManagedPostgresRuntime;

const STORAGE_IMAGE: &str =
    "ghcr.io/verglas-org/neon-storage:da4e33a6e8090585d8181a0aa5d033c44b2006cd";
const COMPUTE_IMAGE: &str =
    "ghcr.io/verglas-org/neon-compute-v16:da4e33a6e8090585d8181a0aa5d033c44b2006cd";
const COMPUTE_PORT: u16 = 55_433;
const PAGESERVER_HTTP_PORT: u16 = 9_898;
const PAGESERVER_PG_PORT: u16 = 6_400;
const BROKER_PORT: u16 = 50_051;
const READY_ATTEMPTS: usize = 90;
static LAST_ATTACH_GENERATION: AtomicU64 = AtomicU64::new(0);

/// Private configuration for one tenant's managed Postgres runtime.
#[derive(Clone)]
pub(crate) struct ManagedPostgresConfig {
    /// Authenticated desired-container API.
    pub(crate) runtime_endpoint: String,
    /// Bearer credential accepted by the desired-container API.
    pub(crate) runtime_token: String,
    /// Tenant whose persisted database records drive recovery.
    pub(crate) tenant_id: String,
    /// S3-compatible endpoint used for durable Neon page layers.
    pub(crate) remote_endpoint: String,
    /// Bucket containing isolated database prefixes.
    pub(crate) remote_bucket: String,
    /// Region accepted by the S3-compatible endpoint.
    pub(crate) remote_region: String,
    /// Access key accepted by the S3-compatible endpoint.
    pub(crate) remote_access_key_id: String,
    /// Secret accepted by the S3-compatible endpoint.
    pub(crate) remote_secret_access_key: String,
    /// Reachable Verglas cache-node safekeeper address.
    pub(crate) safekeepers: String,
    /// Required 256-bit key used for deterministic credential derivation.
    pub(crate) credential_key: Vec<u8>,
}

/// Private connection coordinates issued only to authorized tenant processes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedPostgresConnection {
    /// Private runtime hostname of the compute.
    pub(crate) host: String,
    /// Neon compute PostgreSQL port.
    pub(crate) port: u16,
    /// Tenant-selected database name.
    pub(crate) database: String,
    /// Isolated login role inside the compute.
    pub(crate) username: String,
    /// Restart-stable database credential.
    pub(crate) password: String,
}

/// Deterministic desired state for one database.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ManagedPostgresPlan {
    tenant_id: String,
    timeline_id: String,
    containers: Vec<ContainerSpec>,
    connection: ManagedPostgresConnection,
}

/// Reconciles managed Neon desired state and verifies SQL readiness.
#[derive(Clone)]
pub(crate) struct ManagedPostgresProvisioner {
    config: ManagedPostgresConfig,
    runtime_endpoint: Url,
    http: reqwest::Client,
}

impl ManagedPostgresProvisioner {
    /// Validates all required runtime and durability coordinates.
    pub(crate) fn new(config: ManagedPostgresConfig) -> Result<Self, DatabaseServiceError> {
        let mut runtime_endpoint = Url::parse(&config.runtime_endpoint).map_err(runtime_error)?;
        if !matches!(runtime_endpoint.scheme(), "http" | "https") {
            return Err(provisioning(
                "container runtime endpoint must use http or https",
            ));
        }
        if !runtime_endpoint.path().ends_with('/') {
            runtime_endpoint.set_path(&format!("{}/", runtime_endpoint.path()));
        }
        if [
            config.runtime_token.as_str(),
            config.tenant_id.as_str(),
            config.remote_endpoint.as_str(),
            config.remote_bucket.as_str(),
            config.remote_region.as_str(),
            config.remote_access_key_id.as_str(),
            config.remote_secret_access_key.as_str(),
            config.safekeepers.as_str(),
        ]
        .into_iter()
        .any(str::is_empty)
        {
            return Err(provisioning(
                "managed Neon runtime configuration must not be empty",
            ));
        }
        if config.credential_key.len() != 32 {
            return Err(provisioning(
                "managed Neon credential key must contain exactly 32 bytes",
            ));
        }
        Ok(Self {
            config,
            runtime_endpoint,
            http: reqwest::Client::new(),
        })
    }

    /// Reconciles the shared broker and one database's pageserver and compute.
    async fn ensure(&self, database: &str) -> Result<(), DatabaseServiceError> {
        let plan = self.plan(database)?;
        self.put_container(&plan.containers[0]).await?;
        self.put_container(&plan.containers[1]).await?;
        self.ensure_timeline(&plan).await?;
        self.put_container(&plan.containers[2]).await?;
        self.wait_for_sql(&plan.connection).await
    }

    /// Removes database-owned compute and pageserver declarations in dependency order.
    async fn delete(&self, database: &str) -> Result<(), DatabaseServiceError> {
        let plan = self.plan(database)?;
        self.delete_container(&plan.containers[2].deployment_id)
            .await?;
        self.delete_container(&plan.containers[1].deployment_id)
            .await
    }

    /// Builds stable component declarations from the durable tenant/name identity.
    fn plan(&self, database: &str) -> Result<ManagedPostgresPlan, DatabaseServiceError> {
        validate_database_name(database)?;
        let resource_key = format!("{}\0{database}", self.config.tenant_id);
        let slug = digest_prefix("postgres-runtime", &resource_key, 16);
        let tenant_id = digest_prefix("neon-tenant", &resource_key, 32);
        let timeline_id = digest_prefix("neon-timeline", &resource_key, 32);
        let broker_id = "neon-broker".to_owned();
        let pageserver_id = format!("neon-{slug}-pageserver");
        let compute_id = format!("neon-{slug}-compute");
        let broker_host = docker_hostname(&broker_id);
        let pageserver_host = docker_hostname(&pageserver_id);
        let password = derive_credential(&self.config.credential_key, &resource_key)?;

        let broker = ContainerSpec::new(&broker_id, STORAGE_IMAGE)
            .with_platform("linux/amd64")
            .with_command([
                "/usr/local/bin/storage_broker",
                "--listen-addr=0.0.0.0:50051",
            ]);
        let pageserver = ContainerSpec::new(&pageserver_id, STORAGE_IMAGE)
            .with_platform("linux/amd64")
            .with_entrypoint(["/bin/sh", "-ec"])
            .with_command([pageserver_script()])
            .with_environment(
                "VERGLAS_NEON_BROKER",
                format!("{broker_host}:{BROKER_PORT}"),
            )
            .with_environment("VERGLAS_NEON_REMOTE_ENDPOINT", &self.config.remote_endpoint)
            .with_environment("VERGLAS_NEON_REMOTE_BUCKET", &self.config.remote_bucket)
            .with_environment("VERGLAS_NEON_REMOTE_REGION", &self.config.remote_region)
            .with_environment("AWS_REGION", &self.config.remote_region)
            .with_environment("AWS_ACCESS_KEY_ID", &self.config.remote_access_key_id)
            .with_environment(
                "AWS_SECRET_ACCESS_KEY",
                &self.config.remote_secret_access_key,
            )
            .with_environment(
                "VERGLAS_NEON_REMOTE_PREFIX",
                format!("postgres/{tenant_id}"),
            );
        let compute = ContainerSpec::new(&compute_id, COMPUTE_IMAGE)
            .with_platform("linux/amd64")
            .with_entrypoint(["/bin/sh", "-ec"])
            .with_command([compute_script()])
            .with_environment("VERGLAS_PG_DATABASE", database)
            .with_environment("VERGLAS_PG_PASSWORD", &password)
            .with_environment("VERGLAS_PG_SAFEKEEPERS", &self.config.safekeepers)
            .with_environment(
                "VERGLAS_PG_PAGESERVER",
                format!("{pageserver_host}:{PAGESERVER_PG_PORT}"),
            )
            .with_environment("VERGLAS_PG_TENANT_ID", &tenant_id)
            .with_environment("VERGLAS_PG_TIMELINE_ID", &timeline_id);
        Ok(ManagedPostgresPlan {
            tenant_id,
            timeline_id,
            containers: vec![broker, pageserver, compute],
            connection: ManagedPostgresConnection {
                host: docker_hostname(&compute_id),
                port: COMPUTE_PORT,
                database: database.to_owned(),
                username: "verglas".to_owned(),
                password,
            },
        })
    }

    /// Stores and reconciles one declaration through the runtime manager.
    async fn put_container(&self, spec: &ContainerSpec) -> Result<(), DatabaseServiceError> {
        let uri = self
            .runtime_endpoint
            .join(&format!("v1/containers/{}", spec.deployment_id))
            .map_err(runtime_error)?;
        let response = self
            .http
            .put(uri)
            .bearer_auth(&self.config.runtime_token)
            .json(spec)
            .send()
            .await
            .map_err(runtime_error)?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(provisioning(format!(
                "container runtime rejected {} with HTTP {}",
                spec.deployment_id,
                response.status()
            )))
        }
    }

    /// Deletes one declaration and its owned container idempotently.
    async fn delete_container(&self, deployment_id: &str) -> Result<(), DatabaseServiceError> {
        let uri = self
            .runtime_endpoint
            .join(&format!("v1/containers/{deployment_id}"))
            .map_err(runtime_error)?;
        let response = self
            .http
            .delete(uri)
            .bearer_auth(&self.config.runtime_token)
            .send()
            .await
            .map_err(runtime_error)?;
        if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(provisioning(format!(
                "container runtime could not delete {deployment_id}: HTTP {}",
                response.status()
            )))
        }
    }

    /// Attaches the stable tenant and creates its stable timeline idempotently.
    async fn ensure_timeline(
        &self,
        plan: &ManagedPostgresPlan,
    ) -> Result<(), DatabaseServiceError> {
        let origin = pageserver_origin(&plan.containers[1].deployment_id)?;
        self.wait_for_pageserver(&origin).await?;
        let response = self
            .http
            .put(
                origin
                    .join(&format!("v1/tenant/{}/location_config", plan.tenant_id))
                    .map_err(runtime_error)?,
            )
            .json(&json!({
                "mode":"AttachedSingle",
                "generation": attach_generation()?,
                "tenant_conf":{}
            }))
            .send()
            .await
            .map_err(runtime_error)?;
        if !response.status().is_success() {
            return Err(provisioning(format!(
                "pageserver tenant attach returned HTTP {}",
                response.status()
            )));
        }
        self.wait_for_tenant(&origin, &plan.tenant_id).await?;
        let response = self
            .http
            .post(
                origin
                    .join(&format!("v1/tenant/{}/timeline/", plan.tenant_id))
                    .map_err(runtime_error)?,
            )
            .json(&json!({"new_timeline_id":plan.timeline_id,"pg_version":16}))
            .send()
            .await
            .map_err(runtime_error)?;
        if response.status().is_success() || response.status() == StatusCode::CONFLICT {
            Ok(())
        } else {
            Err(provisioning(format!(
                "pageserver timeline create returned HTTP {}",
                response.status()
            )))
        }
    }

    /// Waits until the pageserver accepts control requests.
    async fn wait_for_pageserver(&self, origin: &Url) -> Result<(), DatabaseServiceError> {
        let status = origin.join("v1/status").map_err(runtime_error)?;
        for _ in 0..READY_ATTEMPTS {
            if self
                .http
                .get(status.clone())
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Err(provisioning("managed Neon pageserver did not become ready"))
    }

    /// Waits until the asynchronously attached tenant becomes active.
    async fn wait_for_tenant(
        &self,
        origin: &Url,
        tenant_id: &str,
    ) -> Result<(), DatabaseServiceError> {
        let uri = origin
            .join(&format!("v1/tenant/{tenant_id}"))
            .map_err(runtime_error)?;
        for _ in 0..READY_ATTEMPTS {
            if let Ok(response) = self.http.get(uri.clone()).send().await
                && response.status().is_success()
                && let Ok(body) = response.json::<Value>().await
                && tenant_is_active(&body)
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        Err(provisioning("managed Neon tenant did not become active"))
    }

    /// Runs an authenticated SQL query against the created database.
    async fn wait_for_sql(
        &self,
        connection: &ManagedPostgresConnection,
    ) -> Result<(), DatabaseServiceError> {
        let parameters = format!(
            "host={} port={} dbname={} user={} password={}",
            connection.host,
            connection.port,
            connection.database,
            connection.username,
            connection.password
        );
        for _ in 0..READY_ATTEMPTS {
            if let Ok((client, transport)) = tokio_postgres::connect(&parameters, NoTls).await {
                tokio::spawn(async move {
                    let _ = transport.await;
                });
                if client
                    .query_one("SELECT current_database()", &[])
                    .await
                    .is_ok_and(|row| row.get::<_, String>(0) == connection.database)
                {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Err(provisioning(
            "managed Neon compute did not pass its authenticated SQL probe",
        ))
    }
}

#[async_trait]
impl ManagedPostgresRuntime for ManagedPostgresProvisioner {
    /// Reconciles all persistent components and waits for authenticated SQL.
    async fn ensure_database(&self, name: &str) -> Result<(), DatabaseServiceError> {
        self.ensure(name).await
    }

    /// Removes database-owned runtime components before record deletion.
    async fn delete_database(&self, name: &str) -> Result<(), DatabaseServiceError> {
        self.delete(name).await
    }
}

/// Returns true for either pageserver representation of the active state.
fn tenant_is_active(body: &Value) -> bool {
    body.get("state").is_some_and(|state| {
        state.as_str() == Some("Active")
            || state.get("slug").and_then(Value::as_str) == Some("Active")
    })
}

/// Produces a process-monotonic attach generation seeded from the wall clock.
fn attach_generation() -> Result<u32, DatabaseServiceError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(runtime_error)?;
    let wall = elapsed.as_secs();
    let mut observed = LAST_ATTACH_GENERATION.load(Ordering::Relaxed);
    loop {
        let next = wall.max(observed.saturating_add(1));
        match LAST_ATTACH_GENERATION.compare_exchange_weak(
            observed,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return u32::try_from(next).map_err(runtime_error),
            Err(current) => observed = current,
        }
    }
}

/// Derives one context-separated hexadecimal identifier.
fn digest_prefix(context: &str, value: &str, length: usize) -> String {
    let mut digest = Sha256::new();
    digest.update(context.as_bytes());
    digest.update([0]);
    digest.update(value.as_bytes());
    hex::encode(digest.finalize())[..length].to_owned()
}

/// Derives a restart-stable password without persisting credential plaintext.
fn derive_credential(key: &[u8], value: &str) -> Result<String, DatabaseServiceError> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(runtime_error)?;
    mac.update(b"managed-neon-password\0");
    mac.update(value.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

/// Maps a runtime deployment ID to the manager's stable private DNS name.
fn docker_hostname(deployment_id: &str) -> String {
    format!("verglas-{deployment_id}")
}

/// Returns the private pageserver control origin.
fn pageserver_origin(deployment_id: &str) -> Result<Url, DatabaseServiceError> {
    Url::parse(&format!(
        "http://{}:{PAGESERVER_HTTP_PORT}/",
        docker_hostname(deployment_id)
    ))
    .map_err(runtime_error)
}

/// Validates the identifier before bootstrap SQL interpolation.
fn validate_database_name(database: &str) -> Result<(), DatabaseServiceError> {
    let mut chars = database.chars();
    let Some(first) = chars.next() else {
        return Err(provisioning("managed Postgres database name is invalid"));
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !chars.all(|value| value.is_ascii_alphanumeric() || matches!(value, '_' | '-'))
    {
        return Err(provisioning("managed Postgres database name is invalid"));
    }
    Ok(())
}

/// Shell program that renders pageserver configuration and starts it.
fn pageserver_script() -> &'static str {
    r#"mkdir -p /data/verglas
printf 'id=1\n' > /data/verglas/identity.toml
cat > /data/verglas/pageserver.toml <<EOF
broker_endpoint = 'http://${VERGLAS_NEON_BROKER}'
pg_distrib_dir = '/usr/local/'
listen_pg_addr = '0.0.0.0:6400'
listen_http_addr = '0.0.0.0:9898'
availability_zone = 'verglas-oss'
control_plane_api = 'http://127.0.0.1:6666'
control_plane_emergency_mode = true
disk_usage_based_eviction = { max_usage_pct = 95, min_avail_bytes = 134217728, period = '60s' }
virtual_file_io_engine = 'std-fs'
virtual_file_io_mode = 'buffered'
[remote_storage]
endpoint = '${VERGLAS_NEON_REMOTE_ENDPOINT}'
bucket_name = '${VERGLAS_NEON_REMOTE_BUCKET}'
bucket_region = '${VERGLAS_NEON_REMOTE_REGION}'
prefix_in_bucket = '${VERGLAS_NEON_REMOTE_PREFIX}/pageserver/'
EOF
exec /usr/local/bin/pageserver -D /data/verglas"#
}

/// Shell program that starts compute and creates its isolated role/database.
fn compute_script() -> &'static str {
    r#"mkdir -p /var/db/postgres/verglas-compute /var/db/postgres/verglas-spec
cat > /var/db/postgres/verglas-spec/compute.json <<EOF
{"spec":{"format_version":1.0,"timestamp":"1970-01-01T00:00:00.000Z","operation_uuid":"00000000-0000-0000-0000-000000000000","suspend_timeout_seconds":-1,"cluster":{"cluster_id":"verglas","name":"managed-postgres","state":"restarted","roles":[{"name":"cloud_admin","encrypted_password":"b093c0d3b281ba6da1eacc608620abd8","options":null}],"databases":[],"settings":[{"name":"port","value":"55433","vartype":"integer"},{"name":"listen_addresses","value":"0.0.0.0","vartype":"string"},{"name":"fsync","value":"off","vartype":"bool"},{"name":"wal_level","value":"logical","vartype":"enum"},{"name":"wal_log_hints","value":"on","vartype":"bool"},{"name":"synchronous_standby_names","value":"walproposer","vartype":"string"},{"name":"shared_preload_libraries","value":"neon,pg_stat_statements","vartype":"string"},{"name":"neon.safekeepers","value":"${VERGLAS_PG_SAFEKEEPERS}","vartype":"string"},{"name":"neon.timeline_id","value":"${VERGLAS_PG_TIMELINE_ID}","vartype":"string"},{"name":"neon.tenant_id","value":"${VERGLAS_PG_TENANT_ID}","vartype":"string"},{"name":"neon.pageserver_connstring","value":"host=${VERGLAS_PG_PAGESERVER%:*} port=${VERGLAS_PG_PAGESERVER##*:}","vartype":"string"}]},"delta_operations":[]},"compute_ctl_config":{"jwks":{"keys":[]}}}
EOF
export OTEL_SDK_DISABLED=true PGPASSWORD=cloud_admin
/usr/local/bin/compute_ctl --pgdata /var/db/postgres/verglas-compute -C postgresql://cloud_admin@127.0.0.1:55433/postgres -b /usr/local/bin/postgres --compute-id "verglas-${VERGLAS_PG_TENANT_ID}" --config /var/db/postgres/verglas-spec/compute.json --dev &
compute_pid=$!
until /usr/local/bin/psql postgresql://cloud_admin@127.0.0.1:55433/postgres -tAc 'SELECT 1' >/dev/null 2>&1; do kill -0 "$compute_pid"; sleep 1; done
/usr/local/bin/psql postgresql://cloud_admin@127.0.0.1:55433/postgres -v ON_ERROR_STOP=1 <<SQL
DO \$\$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'verglas') THEN
    ALTER ROLE verglas LOGIN PASSWORD '${VERGLAS_PG_PASSWORD}';
  ELSE
    CREATE ROLE verglas LOGIN PASSWORD '${VERGLAS_PG_PASSWORD}';
  END IF;
END \$\$;
SELECT 'CREATE DATABASE "${VERGLAS_PG_DATABASE}" OWNER verglas' WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = '${VERGLAS_PG_DATABASE}') \gexec
SQL
wait "$compute_pid""#
}

/// Converts private transport failures to the provisioning boundary.
fn runtime_error(error: impl std::fmt::Display) -> DatabaseServiceError {
    provisioning(error.to_string())
}

/// Creates a provisioning error without a compatibility path.
fn provisioning(message: impl Into<String>) -> DatabaseServiceError {
    DatabaseServiceError::Provisioning(message.into())
}

#[cfg(test)]
impl ManagedPostgresConfig {
    /// Supplies complete deterministic configuration for unit tests.
    fn fixture() -> Self {
        Self {
            runtime_endpoint: "http://runtime:8360".to_owned(),
            runtime_token: "runtime-token".to_owned(),
            tenant_id: "tenant-a".to_owned(),
            remote_endpoint: "http://cache:8333".to_owned(),
            remote_bucket: "managed".to_owned(),
            remote_region: "auto".to_owned(),
            remote_access_key_id: "access".to_owned(),
            remote_secret_access_key: "secret".to_owned(),
            safekeepers: "cache-a:5454".to_owned(),
            credential_key: vec![7; 32],
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{ManagedPostgresConfig, ManagedPostgresProvisioner, tenant_is_active};

    /// Pins raw component images and cross-architecture execution explicitly.
    #[test]
    fn managed_postgres_uses_published_images_on_their_only_platform() {
        let provisioner = ManagedPostgresProvisioner::new(ManagedPostgresConfig::fixture())
            .expect("valid provisioner");
        let plan = provisioner.plan("analytics").expect("plan");

        assert_eq!(plan.containers.len(), 3);
        assert_eq!(plan.containers[0].deployment_id, "neon-broker");
        assert_eq!(plan.containers[0].image, super::STORAGE_IMAGE);
        assert_eq!(plan.containers[1].image, super::STORAGE_IMAGE);
        assert_eq!(plan.containers[2].image, super::COMPUTE_IMAGE);
        assert!(
            plan.containers
                .iter()
                .all(|container| container.platform.as_deref() == Some("linux/amd64"))
        );
    }

    /// Recovers the same identity while separating different database resources.
    #[test]
    fn managed_postgres_identity_and_credentials_are_stable_and_isolated() {
        let provisioner = ManagedPostgresProvisioner::new(ManagedPostgresConfig::fixture())
            .expect("valid provisioner");
        let first = provisioner.plan("analytics").expect("first plan");
        let again = provisioner.plan("analytics").expect("second plan");
        let other = provisioner.plan("operations").expect("other plan");

        assert_eq!(first, again);
        assert_ne!(first.tenant_id, other.tenant_id);
        assert_ne!(first.timeline_id, other.timeline_id);
        assert_ne!(first.connection.password, other.connection.password);
        assert_eq!(first.connection.database, "analytics");
        assert_eq!(first.connection.username, "verglas");
    }

    /// Carries every recovery coordinate in the persisted desired declarations.
    #[test]
    fn desired_specs_recover_every_durable_coordinate() {
        let provisioner = ManagedPostgresProvisioner::new(ManagedPostgresConfig::fixture())
            .expect("valid provisioner");
        let plan = provisioner.plan("analytics").expect("plan");

        assert_eq!(
            plan.containers[1].environment["VERGLAS_NEON_REMOTE_PREFIX"],
            format!("postgres/{}", plan.tenant_id)
        );
        assert_eq!(
            plan.containers[2].environment["VERGLAS_PG_TIMELINE_ID"],
            plan.timeline_id
        );
        assert_eq!(
            plan.containers[2].environment["VERGLAS_PG_PASSWORD"],
            plan.connection.password
        );
    }

    /// Accepts the two state representations emitted by supported pageservers.
    #[test]
    fn pageserver_accepts_both_active_state_shapes() {
        assert!(tenant_is_active(&json!({"state":"Active"})));
        assert!(tenant_is_active(&json!({"state":{"slug":"Active"}})));
        assert!(!tenant_is_active(&json!({"state":"Attaching"})));
    }
}
