//! The memory grant contract: how a worker asks whatever launched it for
//! memory, and grows that ask while it runs.
//!
//! This lives on the framework side ([`WorkerContext`](crate::worker::WorkerContext),
//! [`crate::worker::Worker::initial_grant_request`]) so every worker kind
//! inherits it — not just the query role. A worker with no opinion about its
//! memory need simply never calls it, and the default [`LocalGrantHost`]
//! grants whatever is asked with no enforcement, so nothing changes for
//! today's workers.
//!
//! The seam is deliberately neutral about *how* a grant is enforced. A grant
//! is just a byte count the host agrees to. On a bare-metal or dev host there
//! may be nothing enforcing it at all ([`LocalGrantHost`]); a supervising host
//! may map a grant onto whatever primitive it manages memory with — a cgroup
//! limit, or something else later. None of that is this crate's concern or
//! knowledge: OSS ships the contract and the no-op local host; an enforcing
//! host implements the same trait outside the SDK.

use async_trait::async_trait;
use thiserror::Error;

/// A memory ask, in bytes, that a worker makes before or during a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryGrantRequest {
    /// The estimated working-set size the worker expects to need. The host
    /// grants at least `minimum_bytes` and at most this, subject to its own
    /// availability.
    pub estimated_bytes: u64,
    /// The floor below which the worker cannot usefully run (its own runtime
    /// baseline). The host never grants less than this; if it cannot, the
    /// request fails rather than handing back an unusable grant.
    pub minimum_bytes: u64,
}

impl MemoryGrantRequest {
    /// Builds a request, raising `estimated_bytes` to `minimum_bytes` if the
    /// caller's estimate somehow came in under its own floor.
    pub fn new(estimated_bytes: u64, minimum_bytes: u64) -> Self {
        Self {
            estimated_bytes: estimated_bytes.max(minimum_bytes),
            minimum_bytes,
        }
    }
}

/// A granted allowance, in bytes. Opaque beyond its size — the worker holding
/// one does not know or care what enforces it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryGrant {
    /// The granted size, in bytes.
    pub bytes: u64,
}

/// Why a grant request or a grow call failed.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum GrantError {
    /// The host has no more memory to give, even at the request's floor.
    #[error(
        "no memory available to grant ({requested} bytes requested, at least {minimum} needed)"
    )]
    Exhausted {
        /// What was asked for.
        requested: u64,
        /// The floor the caller said it could not run below.
        minimum: u64,
    },
    /// The host could not be reached or refused the request for a reason
    /// other than exhaustion.
    #[error("grant host unavailable: {0}")]
    Unavailable(String),
}

/// The host side of the grant contract: whatever launched the worker and can
/// give it more memory. A worker holds a handle to one of these — the
/// framework hands it one through [`WorkerContext`](crate::worker::WorkerContext),
/// or a standalone role binary (not driven through `WorkerContext` at all,
/// such as `verglas-query`) holds one directly — and calls it before starting
/// heavy work, and again if it needs to grow.
///
/// Grow-only: there is no `shrink`. A worker that no longer needs its grant
/// releases the whole thing and exits; it never gives back part of it while
/// still running.
#[async_trait]
pub trait MemoryGrantHost: Send + Sync {
    /// Requests an initial grant sized by `request`. The host may grant less
    /// than `estimated_bytes` (but never less than `minimum_bytes`) if it is
    /// itself constrained; the worker makes up the difference with `grow`
    /// calls as it discovers it needs more.
    async fn request(&self, request: MemoryGrantRequest) -> Result<MemoryGrant, GrantError>;

    /// Grows an existing grant by `additional_bytes`.
    async fn grow(
        &self,
        current: MemoryGrant,
        additional_bytes: u64,
    ) -> Result<MemoryGrant, GrantError>;

    /// Releases a grant when the worker finishes (or is being killed after
    /// use). Best-effort — a host that's gone is nothing to report back to.
    async fn release(&self, grant: MemoryGrant);
}

/// A host that grants exactly what is asked, unconditionally, and enforces
/// nothing. This is what a worker gets when it runs standalone or in dev with
/// no host agent attached — the request/grow/release calls still work, they
/// just have no effect beyond bookkeeping. An enforcing host (for example
/// cgroups) lives outside this crate and implements the same trait.
#[derive(Debug, Default, Clone, Copy)]
pub struct LocalGrantHost;

#[async_trait]
impl MemoryGrantHost for LocalGrantHost {
    async fn request(&self, request: MemoryGrantRequest) -> Result<MemoryGrant, GrantError> {
        Ok(MemoryGrant {
            bytes: request.estimated_bytes,
        })
    }

    async fn grow(
        &self,
        current: MemoryGrant,
        additional_bytes: u64,
    ) -> Result<MemoryGrant, GrantError> {
        Ok(MemoryGrant {
            bytes: current.bytes.saturating_add(additional_bytes),
        })
    }

    async fn release(&self, _grant: MemoryGrant) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A request below its own floor is raised to the floor, not rejected —
    /// the caller's estimate is advisory, the floor is not.
    #[test]
    fn request_floors_the_estimate() {
        let request = MemoryGrantRequest::new(10, 64);
        assert_eq!(request.estimated_bytes, 64);
        assert_eq!(request.minimum_bytes, 64);
    }

    /// The local host grants exactly the estimate and grows by exactly the
    /// delta asked — it's bookkeeping only, never a constraint.
    #[tokio::test]
    async fn local_host_grants_and_grows_unconstrained() {
        let host = LocalGrantHost;
        let grant = host
            .request(MemoryGrantRequest::new(100, 10))
            .await
            .expect("granted");
        assert_eq!(grant.bytes, 100);
        let grown = host.grow(grant, 50).await.expect("grown");
        assert_eq!(grown.bytes, 150);
        host.release(grown).await;
    }
}
