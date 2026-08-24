//! Stable identity for the external placement owner.
//!
//! Celld supervises the one process assigned by cloud placement. It does not
//! elect owners, validate leases, or run a local compare-and-swap path.

/// Stable identity of one tenant cell host.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HostId(String);

impl HostId {
    /// Creates a host identity from its scheduler name.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the scheduler-visible host name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
