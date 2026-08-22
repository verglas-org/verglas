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
    /// Reads one typed Catalog-domain record.
    Record {
        /// Domain collection.
        entity: CatalogEntity,
        /// Stable record identifier.
        id: String,
    },
    /// Lists every record in one typed Catalog-domain collection.
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
    /// One typed Catalog-domain record, if present.
    Record(Option<String>),
    /// Lexically ordered records in one typed Catalog-domain collection.
    Records(Vec<(String, String)>),
}

/// Additional same-ingress attempts a `retryable` failure gets once every
/// configured ingress has had one try. A stateless Catalog instance often
/// has exactly one ingress (its own node's loopback safekeeper — the prod
/// contract mirrored by `scripts/local-lite.sh`'s ring topology: every
/// instance's ingress list names ONLY its own node, no cross-node failover
/// list). With one ingress, "try the next one" never applies, so a 503
/// ("reached no currently serving leader," see [`retryable`]) would
/// otherwise have zero recovery path — including the ordinary case of a
/// leader momentarily busy with a concurrent commit from the same process.
const EXTRA_RETRIES: usize = 2;

/// Backoff between same-ingress retries. Short: the condition this recovers
/// from (a leader busy with one other in-flight commit) normally clears in
/// low hundreds of milliseconds, not seconds.
const RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_millis(250);

/// How the catalog adapter reaches its warehouse's consensus groups.
///
/// The adapter is written against this rather than the HTTP client so a
/// process that already hosts the consensus plane can serve catalog requests
/// in-process. In the co-located cloud topology one stateless catalog runs
/// beside every ring node, so the HTTP implementation would otherwise be
/// talking to a plane inside its own address space.
///
/// Extension point: a peer that speaks a future wire format implements this
/// without touching the adapter. Not built now (prototype).
#[async_trait::async_trait]
pub trait ManagedCatalogTransport: Send + Sync + 'static {
    /// The warehouse route bound to every request on this transport.
    fn warehouse(&self) -> &str;

    /// Executes one catalog request against the authoritative plane.
    async fn execute(
        &self,
        request: &ManagedCatalogRequest,
    ) -> Result<ManagedCatalogResponse, ManagedCatalogError>;
}

#[async_trait::async_trait]
impl ManagedCatalogTransport for ManagedCatalogClient {
    /// See [`ManagedCatalogClient::warehouse`].
    fn warehouse(&self) -> &str {
        ManagedCatalogClient::warehouse(self)
    }

    /// See [`ManagedCatalogClient::execute`].
    async fn execute(
        &self,
        request: &ManagedCatalogRequest,
    ) -> Result<ManagedCatalogResponse, ManagedCatalogError> {
        ManagedCatalogClient::execute(self, request).await
    }
}

/// HTTP client used by the Catalog REST/domain adapter.
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
    ///
    /// Tries every configured ingress once (deterministic failover order),
    /// then — only for a `retryable` failure — keeps retrying the same
    /// rotation for [`EXTRA_RETRIES`] more rounds with a short backoff. This
    /// is what makes a single-ingress client (see [`EXTRA_RETRIES`]'s doc)
    /// resilient to a transient "no currently serving leader" at all: with
    /// one ingress the first loop pass is also the last one, so without the
    /// extra rounds a single 503 would be immediately fatal.
    pub async fn execute(
        &self,
        request: &ManagedCatalogRequest,
    ) -> Result<ManagedCatalogResponse, ManagedCatalogError> {
        let body = serde_json::to_vec(request).map_err(ManagedCatalogError::Encoding)?;
        // `with_ingresses` rejects an empty endpoint list at construction, so
        // `self.endpoints` is never empty here.
        let total_attempts = self.endpoints.len() * (1 + EXTRA_RETRIES);
        let mut last_error = None;
        for attempt in 0..total_attempts {
            if attempt >= self.endpoints.len() {
                tokio::time::sleep(RETRY_BACKOFF).await;
            }
            let endpoint = &self.endpoints[attempt % self.endpoints.len()];
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
                Ok(response) if response.status() == reqwest::StatusCode::CONFLICT => {
                    return Err(ManagedCatalogError::Conflict);
                }
                Ok(response) if retryable(response.status()) => {
                    last_error = Some(ManagedCatalogError::Rejected(response.status().as_u16()));
                }
                Ok(response) => {
                    return Err(ManagedCatalogError::Rejected(response.status().as_u16()));
                }
                Err(error) => last_error = Some(ManagedCatalogError::Http(error)),
            }
        }
        Err(last_error.unwrap_or(ManagedCatalogError::NoIngresses))
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
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    /// A single-ingress client (the stateless-Catalog contract: every
    /// instance's ingress list names only its own node) recovers from a
    /// transient leader-busy 503 by retrying the SAME ingress — there is no
    /// "next" one to fail over to.
    #[tokio::test]
    async fn single_ingress_retries_past_a_transient_service_unavailable() {
        let addr = serve_recovering_after(1).await;
        let client = ManagedCatalogClient::new(format!("http://{addr}"), "tenant", "warehouse");
        assert_eq!(
            client
                .execute(&ManagedCatalogRequest::Namespaces)
                .await
                .expect("retries past the single transient 503"),
            ManagedCatalogResponse::Namespaces(vec![])
        );
    }

    /// A single ingress failing on every attempt exhausts the retry budget
    /// and reports the failure instead of hanging or retrying forever.
    #[tokio::test]
    async fn single_ingress_exhausts_retries_on_sustained_unavailability() {
        let addr = serve(
            StatusCode::SERVICE_UNAVAILABLE,
            Arc::new(Mutex::new(Vec::new())),
        )
        .await;
        let client = ManagedCatalogClient::new(format!("http://{addr}"), "tenant", "warehouse");
        let result = client.execute(&ManagedCatalogRequest::Namespaces).await;
        assert!(matches!(
            result,
            Err(super::ManagedCatalogError::Rejected(503))
        ));
    }

    /// Starts one loopback ingress that answers 503 for its first `fail_times`
    /// requests, then 200 forever after — a leader that is momentarily busy
    /// with one other in-flight commit and then free.
    async fn serve_recovering_after(fail_times: usize) -> std::net::SocketAddr {
        async fn handler(
            State((calls, fail_times)): State<(Arc<AtomicUsize>, usize)>,
            _body: Bytes,
        ) -> (StatusCode, &'static str) {
            let call = calls.fetch_add(1, Ordering::SeqCst);
            let status = if call < fail_times {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::OK
            };
            (status, "{\"Namespaces\":[]}")
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        tokio::spawn(async move {
            let _ = axum::serve(
                listener,
                Router::new()
                    .route("/catalog/v1/{tenant}/{warehouse}", post(handler))
                    .with_state((calls, fail_times)),
            )
            .await;
        });
        address
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
