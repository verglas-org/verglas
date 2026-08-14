//! Client contract for authoritative Verglas-managed Iceberg warehouses.

use serde::{Deserialize, Serialize};
use verglas_consensus::{CatalogBatch, CatalogEntity, RaftResponse};

/// A typed native-catalog request sent through any cache ingress.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ManagedCatalogRequest {
    /// Applies one atomic optimistic transaction.
    Commit {
        request_id: u128,
        batch: CatalogBatch,
    },
    /// Reads one table's current immutable metadata pointer.
    Table { table: String },
    /// Lists every namespace.
    Namespaces,
    /// Lists every table and current metadata pointer.
    Tables,
    /// Reads one typed Lakekeeper-domain record.
    Record {
        /// Domain collection.
        entity: CatalogEntity,
        /// Stable record identifier.
        id: String,
    },
    /// Lists every record in one typed Lakekeeper-domain collection.
    Records {
        /// Domain collection.
        entity: CatalogEntity,
    },
}

/// A linearizable native-catalog response.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ManagedCatalogResponse {
    /// One transaction applied through Raft.
    Applied(RaftResponse),
    /// One table pointer, or absent when the table is unregistered.
    Table(Option<String>),
    /// Lexically ordered namespaces.
    Namespaces(Vec<String>),
    /// Lexically ordered table identifiers and pointers.
    Tables(Vec<(String, String)>),
    /// One typed Lakekeeper-domain record, if present.
    Record(Option<String>),
    /// Lexically ordered records in one typed Lakekeeper-domain collection.
    Records(Vec<(String, String)>),
}

/// HTTP client used by the Lakekeeper REST/domain adapter.
pub struct ManagedCatalogClient {
    endpoints: Vec<String>,
    tenant: String,
    warehouse: String,
    http: reqwest::Client,
}

impl ManagedCatalogClient {
    /// Targets one warehouse through any healthy Verglas ingress.
    pub fn new(
        endpoint: impl Into<String>,
        tenant: impl Into<String>,
        warehouse: impl Into<String>,
    ) -> Self {
        Self::with_ingresses([endpoint.into()], tenant, warehouse)
            .expect("one explicit ingress is nonempty")
    }

    /// Creates a client with deterministic failover order across explicit ingresses.
    pub fn with_ingresses(
        ingresses: impl IntoIterator<Item = String>,
        tenant: impl Into<String>,
        warehouse: impl Into<String>,
    ) -> Result<Self, ManagedCatalogError> {
        let endpoints: Vec<_> = ingresses
            .into_iter()
            .map(|endpoint| endpoint.trim_end_matches('/').to_owned())
            .filter(|endpoint| !endpoint.is_empty())
            .collect();
        if endpoints.is_empty() {
            return Err(ManagedCatalogError::NoIngresses);
        }
        Ok(Self {
            endpoints,
            tenant: tenant.into(),
            warehouse: warehouse.into(),
            http: reqwest::Client::new(),
        })
    }

    /// Returns the warehouse route bound to every request from this client.
    #[must_use]
    pub fn warehouse(&self) -> &str {
        &self.warehouse
    }

    /// Sends one request and fails closed on non-success or malformed responses.
    pub async fn execute(
        &self,
        request: &ManagedCatalogRequest,
    ) -> Result<ManagedCatalogResponse, ManagedCatalogError> {
        let body = serde_json::to_vec(request).map_err(ManagedCatalogError::Encoding)?;
        for (index, endpoint) in self.endpoints.iter().enumerate() {
            let response = self
                .http
                .post(self.request_endpoint(endpoint))
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body.clone())
                .send()
                .await;
            match response {
                Ok(response) if response.status().is_success() => {
                    return response.json().await.map_err(ManagedCatalogError::Http);
                }
                Ok(response)
                    if retryable(response.status()) && index + 1 < self.endpoints.len() =>
                {
                    continue;
                }
                Ok(response) if response.status() == reqwest::StatusCode::CONFLICT => {
                    return Err(ManagedCatalogError::Conflict);
                }
                Ok(response) => {
                    return Err(ManagedCatalogError::Rejected(response.status().as_u16()));
                }
                Err(_error) if index + 1 < self.endpoints.len() => continue,
                Err(error) => return Err(ManagedCatalogError::Http(error)),
            }
        }
        Err(ManagedCatalogError::NoIngresses)
    }

    /// Builds the tenant-rooted warehouse route used by every catalog operation.
    fn request_endpoint(&self, endpoint: &str) -> String {
        let encode = |value: &str| {
            percent_encoding::utf8_percent_encode(value, percent_encoding::NON_ALPHANUMERIC)
                .to_string()
        };
        format!(
            "{}/catalog/v1/{}/{}",
            endpoint,
            encode(&self.tenant),
            encode(&self.warehouse)
        )
    }
}

/// Returns statuses that mean the request reached no currently serving leader.
fn retryable(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::SERVICE_UNAVAILABLE
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{Router, body::Bytes, extract::State, http::StatusCode, routing::post};
    use tokio::sync::Mutex;

    use super::{ManagedCatalogClient, ManagedCatalogRequest, ManagedCatalogResponse};

    /// Catalog clients must address a warehouse through its durable tenant root.
    #[test]
    fn managed_catalog_endpoint_contains_tenant_and_warehouse() {
        let client =
            ManagedCatalogClient::new("http://ingress.example/", "tenant/a", "warehouse one");
        assert_eq!(
            client.request_endpoint(&client.endpoints[0]),
            "http://ingress.example/catalog/v1/tenant%2Fa/warehouse%20one"
        );
    }

    /// Retries a byte-identical request at the next ingress after leader unavailability.
    #[tokio::test]
    async fn retries_identical_body_at_next_ingress_after_service_unavailable() {
        let first = Arc::new(Mutex::new(Vec::new()));
        let second = Arc::new(Mutex::new(Vec::new()));
        let first_addr = serve(StatusCode::SERVICE_UNAVAILABLE, first.clone()).await;
        let second_addr = serve(StatusCode::OK, second.clone()).await;
        let client = ManagedCatalogClient::with_ingresses(
            [
                format!("http://{first_addr}"),
                format!("http://{second_addr}"),
            ],
            "tenant",
            "warehouse",
        )
        .expect("ingresses");
        assert_eq!(
            client
                .execute(&ManagedCatalogRequest::Namespaces)
                .await
                .expect("failover"),
            ManagedCatalogResponse::Namespaces(vec![])
        );
        assert_eq!(*first.lock().await, *second.lock().await);
    }

    /// A typed catalog conflict is final and never ingress-retried.
    #[tokio::test]
    async fn conflict_is_not_retried_at_another_ingress() {
        let first = Arc::new(Mutex::new(Vec::new()));
        let second = Arc::new(Mutex::new(Vec::new()));
        let first_addr = serve(StatusCode::CONFLICT, first.clone()).await;
        let second_addr = serve(StatusCode::OK, second.clone()).await;
        let client = ManagedCatalogClient::with_ingresses(
            [
                format!("http://{first_addr}"),
                format!("http://{second_addr}"),
            ],
            "tenant",
            "warehouse",
        )
        .expect("ingresses");
        let result = client.execute(&ManagedCatalogRequest::Namespaces).await;
        assert!(matches!(result, Err(super::ManagedCatalogError::Conflict)));
        assert!(!first.lock().await.is_empty());
        assert!(second.lock().await.is_empty());
    }

    /// Starts one loopback ingress and captures its unmodified request body.
    async fn serve(status: StatusCode, captured: Arc<Mutex<Vec<u8>>>) -> std::net::SocketAddr {
        async fn handler(
            State((status, captured)): State<(StatusCode, Arc<Mutex<Vec<u8>>>)>,
            body: Bytes,
        ) -> (StatusCode, &'static str) {
            *captured.lock().await = body.to_vec();
            (status, "{\"Namespaces\":[]}")
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        tokio::spawn(async move {
            let _ = axum::serve(
                listener,
                Router::new()
                    .route("/catalog/v1/{tenant}/{warehouse}", post(handler))
                    .with_state((status, captured)),
            )
            .await;
        });
        address
    }
}

/// Failure to obtain an authoritative catalog result.
#[derive(Debug, thiserror::Error)]
pub enum ManagedCatalogError {
    /// No usable explicit ingress was supplied.
    #[error("managed catalog requires at least one ingress")]
    NoIngresses,
    /// The request cannot be serialized before its first transmission.
    #[error(transparent)]
    Encoding(serde_json::Error),
    /// Network or JSON framing failure.
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    /// The catalog atomically rejected an optimistic or idempotency conflict.
    #[error("managed catalog conflict")]
    Conflict,
    /// The consensus ingress refused the operation.
    #[error("managed catalog ingress returned HTTP {0}")]
    Rejected(u16),
}
