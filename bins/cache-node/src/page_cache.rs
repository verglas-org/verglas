//! Internal HTTP contract used by Neon pageservers to share reconstructed
//! PostgreSQL pages with the cache node's ordinary hybrid data tier.

use std::sync::{Arc, OnceLock};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use verglas_cache::MaterializedPageKey;

use crate::serve::CacheEngine;

/// Deferred engine handle. The admin listener binds before cache recovery and
/// returns 503 until the recovered engine is installed.
pub type PageCacheSlot = Arc<OnceLock<CacheEngine>>;

/// Parsed page coordinates carried in the internal URL.
type PagePath = (String, String, u32, u32, u32, u8, u32, u64);

/// Mounts the cache-only Neon page protocol on the cache-node admin listener.
pub fn router(cache: PageCacheSlot) -> Router {
    Router::new()
        .route(
            "/internal/v1/neon/pages/{tenant}/{timeline}/{spcnode}/{dbnode}/{relnode}/{forknum}/{block}/{lsn}",
            get(get_page).put(put_page),
        )
        .with_state(cache)
}

/// Converts URL coordinates into the shared physical page identity.
fn page_key(path: PagePath) -> MaterializedPageKey {
    let (tenant_shard_id, timeline_id, spcnode, dbnode, relnode, forknum, block, lsn) = path;
    MaterializedPageKey {
        tenant_shard_id,
        timeline_id,
        spcnode,
        dbnode,
        relnode,
        forknum,
        block,
        lsn,
    }
}

/// Returns an exact cached page without triggering an origin fill.
async fn get_page(State(cache): State<PageCacheSlot>, Path(path): Path<PagePath>) -> Response {
    let Some(cache) = cache.get() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match cache.get_materialized_page(&page_key(path)).await {
        Some(page) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            page,
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Offers a reconstructed page to shared admission. An admission rejection is
/// still a successful cache operation because the pageserver already has and
/// serves the bytes.
async fn put_page(
    State(cache): State<PageCacheSlot>,
    Path(path): Path<PagePath>,
    body: Bytes,
) -> Response {
    let Some(cache) = cache.get() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match cache.put_materialized_page(page_key(path), body).await {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, OnceLock};

    use object_store::memory::InMemory;
    use tempfile::TempDir;
    use verglas_cache::HybridCacheEngine;
    use verglas_cluster::peer::PeerClient;
    use verglas_core::config::{ByteSize, Cache as CacheConfig};
    use verglas_s3::{BackendStore, PassthroughRead};

    use super::{PageCacheSlot, router};

    const MIB: u64 = 1024 * 1024;

    /// Builds a ready router and serves it on an ephemeral loopback port.
    async fn server() -> (std::net::SocketAddr, TempDir) {
        let dir = TempDir::new().expect("temp dir");
        let store = Arc::new(InMemory::new());
        let backend = PassthroughRead::new(BackendStore::single("origin", store));
        let config = CacheConfig {
            dir: dir.path().to_path_buf(),
            capacity_bytes: ByteSize(96 * MIB),
            dram_bytes: ByteSize(80 * MIB),
            ..CacheConfig::default()
        };
        let node = verglas_core::node::NodeId::new("single");
        let engine = HybridCacheEngine::new(
            backend,
            PeerClient::disabled(),
            verglas_core::ring::RendezvousRing::single(node.clone()),
            node,
            &config,
        )
        .await
        .expect("engine");
        let slot: PageCacheSlot = Arc::new(OnceLock::new());
        assert!(slot.set(engine).is_ok(), "set engine");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listen");
        let addr = listener.local_addr().expect("address");
        tokio::spawn(async move {
            axum::serve(listener, router(slot)).await.expect("serve");
        });
        (addr, dir)
    }

    /// PUT and GET preserve exact bytes and different LSNs do not alias.
    #[tokio::test]
    async fn page_endpoint_round_trips_exact_versions() {
        let (addr, _dir) = server().await;
        let client = reqwest::Client::new();
        let base = format!(
            "http://{addr}/internal/v1/neon/pages/11111111111111111111111111111111/22222222222222222222222222222222/1663/16384/24576/0/7"
        );
        let page = vec![19_u8; 8192];

        assert_eq!(
            client
                .put(format!("{base}/100"))
                .body(page.clone())
                .send()
                .await
                .expect("put")
                .status(),
            reqwest::StatusCode::NO_CONTENT
        );
        let response = client.get(format!("{base}/100")).send().await.expect("get");
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(response.bytes().await.expect("body").as_ref(), page);
        assert_eq!(
            client
                .get(format!("{base}/101"))
                .send()
                .await
                .expect("other version")
                .status(),
            reqwest::StatusCode::NOT_FOUND
        );
    }

    /// Incomplete bodies are rejected rather than poisoning the cache.
    #[tokio::test]
    async fn page_endpoint_rejects_non_pages() {
        let (addr, _dir) = server().await;
        let status = reqwest::Client::new()
            .put(format!(
                "http://{addr}/internal/v1/neon/pages/t/tl/1663/1/2/0/3/4"
            ))
            .body("short")
            .send()
            .await
            .expect("put")
            .status();
        assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    }
}
