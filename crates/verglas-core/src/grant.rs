//! Memory budgets shared by independently deployed engine roles.
//!
//! A role asks its launcher for a byte budget before expensive work begins.
//! The contract does not prescribe the enforcement mechanism; hosted launchers
//! can map it to container limits, while standalone roles use [`LocalGrantHost`].

use async_trait::async_trait;
use thiserror::Error;

/// A role's estimated and minimum useful memory requirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryGrantRequest {
    /// The expected working-set size in bytes.
    pub estimated_bytes: u64,
    /// The smallest budget under which the role can make useful progress.
    pub minimum_bytes: u64,
}

impl MemoryGrantRequest {
    /// Creates a request whose estimate is never lower than its hard floor.
    pub fn new(estimated_bytes: u64, minimum_bytes: u64) -> Self {
        Self {
            estimated_bytes: estimated_bytes.max(minimum_bytes),
            minimum_bytes,
        }
    }
}

/// The current byte budget granted to a role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryGrant {
    /// Granted bytes.
    pub bytes: u64,
}

/// A memory-budget request failure.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum GrantError {
    /// The launcher cannot satisfy the role's minimum useful budget.
    #[error(
        "no memory available to grant ({requested} bytes requested, at least {minimum} needed)"
    )]
    Exhausted {
        /// Requested estimate.
        requested: u64,
        /// Required floor.
        minimum: u64,
    },
    /// The launcher could not process the request.
    #[error("grant host unavailable: {0}")]
    Unavailable(String),
}

/// The launcher-side interface used by a role to acquire and grow its budget.
#[async_trait]
pub trait MemoryGrantHost: Send + Sync {
    /// Acquires an initial budget satisfying the request's floor.
    async fn request(&self, request: MemoryGrantRequest) -> Result<MemoryGrant, GrantError>;

    /// Adds bytes to a live budget.
    async fn grow(
        &self,
        current: MemoryGrant,
        additional_bytes: u64,
    ) -> Result<MemoryGrant, GrantError>;

    /// Releases the complete budget when the role finishes.
    async fn release(&self, grant: MemoryGrant);
}

/// Standalone launcher that records the requested budget without enforcing it.
#[derive(Debug, Default, Clone, Copy)]
pub struct LocalGrantHost;

#[async_trait]
impl MemoryGrantHost for LocalGrantHost {
    /// Grants the requested estimate.
    async fn request(&self, request: MemoryGrantRequest) -> Result<MemoryGrant, GrantError> {
        Ok(MemoryGrant {
            bytes: request.estimated_bytes,
        })
    }

    /// Grows the recorded budget by the requested amount.
    async fn grow(
        &self,
        current: MemoryGrant,
        additional_bytes: u64,
    ) -> Result<MemoryGrant, GrantError> {
        Ok(MemoryGrant {
            bytes: current.bytes.saturating_add(additional_bytes),
        })
    }

    /// Releases a standalone budget; no external accounting exists to update.
    async fn release(&self, _grant: MemoryGrant) {}
}
