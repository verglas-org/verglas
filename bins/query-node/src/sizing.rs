//! Turning a plan-based memory estimate into a grant request, and applying it
//! against the currently held grant.
//!
//! This is the query role's use of the framework-level grant contract
//! (`verglas_sdk::grant`): the estimate is the *primary* sizing mechanism (a
//! request made before or at launch, from a real query shape and real table
//! stats), and growth is the escape hatch for when the estimate falls short —
//! not the other way around.

use std::sync::Arc;

use iceberg::Catalog;
use verglas_iceberg::TimeTravel;
use verglas_sdk::grant::{MemoryGrant, MemoryGrantHost, MemoryGrantRequest};

/// The floor this binary will not usefully run below — its own runtime
/// baseline. Matches the estimator's own fixed base-overhead term
/// (`verglas_iceberg::estimate`'s `BASE_OVERHEAD_BYTES`), so a grant sized
/// from an estimate is never rejected by its own floor.
pub const MINIMUM_GRANT_BYTES: u64 = 64 * 1024 * 1024;

/// Estimates `sql` (without executing it) and requests an initial grant sized
/// to that estimate. Called once at launch when the worker is started for a
/// specific query (`--for-query`), so the executor requests memory as part of
/// launching the worker rather than after it is already running and short.
pub async fn request_for_query(
    host: &dyn MemoryGrantHost,
    catalog: Arc<dyn Catalog>,
    sql: &str,
    at: Option<TimeTravel>,
) -> Result<MemoryGrant, String> {
    let estimate = verglas_iceberg::estimate(catalog, sql, at)
        .await
        .map_err(|e| e.to_string())?;
    tracing::info!(
        scan_bytes = estimate.scan_bytes,
        working_set_bytes = estimate.working_set_bytes,
        estimated_total_bytes = estimate.estimated_total_bytes,
        "requesting initial grant from query memory estimate"
    );
    let request = MemoryGrantRequest::new(estimate.estimated_total_bytes, MINIMUM_GRANT_BYTES);
    host.request(request).await.map_err(|e| e.to_string())
}

/// Grows `current` to at least `needed_bytes` if it does not already cover
/// it. Grow-only, and a no-op when the current grant is already enough — the
/// escape hatch for a query whose estimate (computed at launch, or missing
/// entirely for a worker that was not sized ahead of time) turns out to be
/// short of what this particular request needs.
pub async fn ensure_covers(
    host: &dyn MemoryGrantHost,
    current: MemoryGrant,
    needed_bytes: u64,
) -> MemoryGrant {
    if needed_bytes <= current.bytes {
        return current;
    }
    let additional = needed_bytes - current.bytes;
    match host.grow(current, additional).await {
        Ok(grown) => {
            tracing::info!(
                from_bytes = current.bytes,
                to_bytes = grown.bytes,
                "grew memory grant"
            );
            grown
        }
        Err(error) => {
            tracing::warn!(
                "grant grow to {needed_bytes} bytes failed, continuing on the existing {}-byte grant: {error}",
                current.bytes
            );
            current
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use verglas_sdk::grant::LocalGrantHost;

    /// A need already covered by the current grant is a no-op — no grow call
    /// changes the bytes held.
    #[tokio::test]
    async fn ensure_covers_is_a_noop_when_already_covered() {
        let host = LocalGrantHost;
        let current = MemoryGrant { bytes: 1000 };
        let result = ensure_covers(&host, current, 500).await;
        assert_eq!(result.bytes, 1000);
    }

    /// A need beyond the current grant grows it to exactly cover the need.
    #[tokio::test]
    async fn ensure_covers_grows_to_the_need() {
        let host = LocalGrantHost;
        let current = MemoryGrant { bytes: 1000 };
        let result = ensure_covers(&host, current, 1500).await;
        assert_eq!(result.bytes, 1500);
    }
}
