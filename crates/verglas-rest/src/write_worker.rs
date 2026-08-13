//! Dispatches bounded Arrow writes to an isolated `verglas-write` process.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::body::Bytes;
use axum::response::Response;
use tokio::sync::Mutex;
use verglas_catalog::{CatalogRuntimeRegistry, DatabaseId};

use crate::query_worker::{relay_response, wait_for_port};

/// Monotonic suffix for per-request ports files.
static DISPATCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// On-demand logical write-role launcher.
pub struct WriteWorkerDispatcher {
    binary: PathBuf,
    runtime: WriteWorkerRuntimeConfig,
    catalogs: CatalogRuntimeRegistry,
    lock: Arc<Mutex<()>>,
}

/// Static settings used to render one token-free writer config per Lakehouse database.
#[derive(Debug, Clone, serde::Serialize)]
pub struct WriteWorkerRuntimeConfig {
    /// Directory that receives database-specific TOML files.
    pub config_dir: PathBuf,
    /// Cache-routed S3 endpoints used for every object write.
    pub cache_s3_endpoints: Vec<String>,
    /// Region passed to the S3 client.
    pub region: String,
    /// Restricted credentials file for the cache S3 endpoint.
    pub credentials_file: PathBuf,
    /// Admin origin containing `/v1/databases/{database}/catalog`.
    pub admin_origin: String,
    /// Access edge that owns durable database commit subscriptions.
    pub access_uri: String,
}

impl WriteWorkerRuntimeConfig {
    /// Renders a database-scoped configuration that deliberately has no bearer credential.
    fn render(&self, database: &DatabaseId) -> Result<PathBuf, String> {
        std::fs::create_dir_all(&self.config_dir)
            .map_err(|error| format!("create {}: {error}", self.config_dir.display()))?;
        let config_path = self.config_dir.join(format!("{}.toml", database.as_str()));
        let rendered = format!(
            "[listen]\nadmin_port = 0\n\n\
             [cache]\ns3_endpoints = {}\nregion = \"{}\"\ncredentials_file = \"{}\"\n\n\
             [catalog]\nuri = \"{}/v1/databases/{}/catalog\"\n",
            toml_string_array(&self.cache_s3_endpoints),
            self.region,
            self.credentials_file.display(),
            self.admin_origin.trim_end_matches('/'),
            database.as_str(),
        );
        std::fs::write(&config_path, rendered)
            .map_err(|error| format!("write write role config: {error}"))?;
        Ok(config_path)
    }
}

/// Renders endpoint values as a valid TOML string array.
fn toml_string_array(values: &[String]) -> String {
    toml::Value::Array(values.iter().cloned().map(toml::Value::String).collect()).to_string()
}

impl WriteWorkerDispatcher {
    /// Creates a dispatcher using the live Lakehouse registry as its activation boundary.
    pub fn new(
        binary: PathBuf,
        runtime: WriteWorkerRuntimeConfig,
        catalogs: CatalogRuntimeRegistry,
    ) -> Self {
        Self {
            binary,
            runtime,
            catalogs,
            lock: Arc::new(Mutex::new(())),
        }
    }

    /// Reports whether a Lakehouse database currently has a live catalog runtime.
    pub fn has_database(&self, database: &DatabaseId) -> bool {
        self.catalogs.get(database).is_some()
    }

    /// Sends one bounded Arrow commit to a fresh isolated writer.
    pub async fn dispatch(
        &self,
        database: &DatabaseId,
        table: &str,
        body: Bytes,
        idempotency_key: Option<String>,
        caller_bearer: &str,
    ) -> Result<Response, String> {
        self.require_database(database)?;
        self.dispatch_request(
            database,
            &format!("/v1/write/{table}"),
            body,
            verglas_sdk::ARROW_STREAM_CONTENT_TYPE,
            idempotency_key,
            caller_bearer,
        )
        .await
    }

    /// Sends one bounded source-file ingest to an isolated writer.
    pub async fn dispatch_ingest(
        &self,
        database: &DatabaseId,
        table: &str,
        query: &str,
        body: Bytes,
        idempotency_key: Option<String>,
        caller_bearer: &str,
    ) -> Result<Response, String> {
        self.require_database(database)?;
        self.dispatch_request(
            database,
            &format!("/v1/ingest/{table}?{query}"),
            body,
            "application/octet-stream",
            idempotency_key,
            caller_bearer,
        )
        .await
    }

    /// Rejects a write before spawning when its database has no live Lakehouse catalog.
    fn require_database(&self, database: &DatabaseId) -> Result<(), String> {
        if self.has_database(database) {
            Ok(())
        } else {
            Err(format!(
                "database `{}` has no Lakehouse write runtime",
                database.as_str()
            ))
        }
    }

    async fn dispatch_request(
        &self,
        database: &DatabaseId,
        path: &str,
        body: Bytes,
        content_type: &str,
        idempotency_key: Option<String>,
        caller_bearer: &str,
    ) -> Result<Response, String> {
        if caller_bearer.is_empty() {
            return Err("write run bearer must not be empty".to_owned());
        }
        let guard = self.lock.clone().lock_owned().await;
        let config_path = self.runtime.render(database)?;
        let dispatch_id = DISPATCH_COUNTER.fetch_add(1, Ordering::Relaxed);
        let ports_file = std::env::temp_dir().join(format!(
            "verglas-write-worker-{}-{dispatch_id}.ports",
            std::process::id()
        ));
        let mut child = self
            .worker_command(&config_path, &ports_file, caller_bearer)
            .spawn()
            .map_err(|error| format!("spawn {}: {error}", self.binary.display()))?;
        let port = wait_for_port(&mut child, &ports_file).await;
        let _ = std::fs::remove_file(&ports_file);
        let port = match port {
            Ok(port) => port,
            Err(error) => {
                let _ = child.start_kill();
                return Err(error);
            }
        };
        let mut request = reqwest::Client::new()
            .post(format!("http://127.0.0.1:{port}{path}"))
            .header(reqwest::header::CONTENT_TYPE, content_type)
            .body(body);
        if let Some(key) = idempotency_key {
            request = request.header("idempotency-key", key);
        }
        let response = request
            .send()
            .await
            .map_err(|error| format!("write worker request failed: {error}"));
        match response {
            Ok(response) => relay_response(response, (guard, child)),
            Err(error) => {
                let _ = child.start_kill();
                Err(error)
            }
        }
    }

    /// Builds one ephemeral child command with the caller bearer held only in its environment.
    fn worker_command(
        &self,
        config_path: &std::path::Path,
        ports_file: &std::path::Path,
        caller_bearer: &str,
    ) -> tokio::process::Command {
        let mut command = tokio::process::Command::new(&self.binary);
        command
            .arg("--config")
            .arg(config_path)
            .env(verglas_core::RUN_BEARER_TOKEN_ENV, caller_bearer)
            .env("VERGLAS_ACCESS_URI", &self.runtime.access_uri)
            .arg("--ports-file")
            .arg(ports_file)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        command
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rendered role TOML selects one database catalog and cannot persist the caller bearer.
    #[test]
    fn rendered_config_is_database_scoped_and_bearer_free() {
        let dir = tempfile::tempdir().expect("config dir");
        let runtime = WriteWorkerRuntimeConfig {
            config_dir: dir.path().to_owned(),
            cache_s3_endpoints: vec!["http://127.0.0.1:8333".to_owned()],
            region: "auto".to_owned(),
            credentials_file: dir.path().join("credentials"),
            admin_origin: "http://127.0.0.1:8334".to_owned(),
            access_uri: "http://127.0.0.1:8345".to_owned(),
        };
        let database = DatabaseId::new("analytics").expect("database id");
        let bearer = "Bearer caller-secret";

        let path = runtime.render(&database).expect("render config");
        let config = std::fs::read_to_string(path).expect("read config");

        assert!(config.contains("uri = \"http://127.0.0.1:8334/v1/databases/analytics/catalog\""));
        assert!(!config.contains(bearer));
        assert!(!config.contains("bearer_token"));
    }

    /// The persisted launch declaration cannot serialize a per-request bearer or its env name.
    #[test]
    fn serialized_execution_declaration_is_bearer_free() {
        let dir = tempfile::tempdir().expect("config dir");
        let declaration = WriteWorkerRuntimeConfig {
            config_dir: dir.path().to_owned(),
            cache_s3_endpoints: vec!["http://127.0.0.1:8333".to_owned()],
            region: "auto".to_owned(),
            credentials_file: dir.path().join("credentials"),
            admin_origin: "http://127.0.0.1:8334".to_owned(),
            access_uri: "http://127.0.0.1:8345".to_owned(),
        };
        let caller_bearer = "caller-secret";

        let serialized = serde_json::to_string(&declaration).expect("serialize declaration");

        assert!(!serialized.contains(caller_bearer));
        assert!(!serialized.contains(verglas_core::RUN_BEARER_TOKEN_ENV));
    }

    /// The bearer belongs only to the ephemeral child environment, never its arguments.
    #[test]
    fn child_command_keeps_bearer_out_of_arguments() {
        let dir = tempfile::tempdir().expect("config dir");
        let dispatcher = WriteWorkerDispatcher::new(
            PathBuf::from("verglas-write"),
            WriteWorkerRuntimeConfig {
                config_dir: dir.path().to_owned(),
                cache_s3_endpoints: vec!["http://127.0.0.1:8333".to_owned()],
                region: "auto".to_owned(),
                credentials_file: dir.path().join("credentials"),
                admin_origin: "http://127.0.0.1:8334".to_owned(),
                access_uri: "http://127.0.0.1:8345".to_owned(),
            },
            CatalogRuntimeRegistry::default(),
        );
        let bearer = "Bearer caller-secret";
        let command = dispatcher.worker_command(
            &dir.path().join("analytics.toml"),
            &dir.path().join("ports"),
            bearer,
        );

        assert!(
            command
                .as_std()
                .get_args()
                .all(|argument| argument != std::ffi::OsStr::new(bearer))
        );
        assert!(command.as_std().get_envs().any(|(name, value)| {
            name == std::ffi::OsStr::new(verglas_core::RUN_BEARER_TOKEN_ENV)
                && value == Some(std::ffi::OsStr::new(bearer))
        }));
        assert!(command.as_std().get_envs().any(|(name, value)| {
            name == std::ffi::OsStr::new("VERGLAS_ACCESS_URI")
                && value == Some(std::ffi::OsStr::new("http://127.0.0.1:8345"))
        }));
    }

    /// A non-Lakehouse database is rejected before its bearer can reach a child process.
    #[tokio::test]
    async fn dispatch_requires_a_live_database_catalog() {
        let dir = tempfile::tempdir().expect("config dir");
        let dispatcher = WriteWorkerDispatcher::new(
            PathBuf::from("/binary/that/must/not/be/spawned"),
            WriteWorkerRuntimeConfig {
                config_dir: dir.path().to_owned(),
                cache_s3_endpoints: vec!["http://127.0.0.1:8333".to_owned()],
                region: "auto".to_owned(),
                credentials_file: dir.path().join("credentials"),
                admin_origin: "http://127.0.0.1:8334".to_owned(),
                access_uri: "http://127.0.0.1:8345".to_owned(),
            },
            CatalogRuntimeRegistry::default(),
        );
        let database = DatabaseId::new("postgres_only").expect("database id");

        let error = dispatcher
            .dispatch(&database, "events", Bytes::new(), None, "caller-secret")
            .await
            .expect_err("missing Lakehouse must fail");

        assert_eq!(
            error,
            "database `postgres_only` has no Lakehouse write runtime"
        );
    }
}
