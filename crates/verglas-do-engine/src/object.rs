//! Open-source SQL, Lakehouse, and non-durable query object contracts.

use crate::error::{Error, Result};

/// Engine primitive selected by cloud-side tenant composition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectKind {
    /// Durable transactional SQL with relational, vector, and graph projections.
    Sql,
    /// Durable managed Iceberg catalog and lake materialization state.
    Lakehouse,
    /// Non-durable query compute that pins cache over a source dataset.
    Query,
}

impl ObjectKind {
    /// Returns whether this object owns authoritative mutable state.
    pub fn requires_durable_authority(self) -> bool {
        matches!(self, Self::Sql | Self::Lakehouse)
    }
}

/// Validated durability and offload behavior for one object instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectPolicy {
    kind: ObjectKind,
    offload_enabled: bool,
}

impl ObjectPolicy {
    /// Validates one object kind and optional asynchronous offload capability.
    pub fn new(kind: ObjectKind, offload_enabled: bool) -> Result<Self> {
        if kind == ObjectKind::Query && offload_enabled {
            return Err(Error::InvalidObjectPolicy(
                "a query object has no authoritative transaction stream to offload".to_owned(),
            ));
        }
        Ok(Self {
            kind,
            offload_enabled,
        })
    }

    /// Returns the selected engine primitive.
    pub fn kind(self) -> ObjectKind {
        self.kind
    }

    /// Returns whether this instance requires a configured durable commit authority.
    pub fn requires_durable_authority(self) -> bool {
        self.kind.requires_durable_authority()
    }

    /// Returns whether committed ranges should be archived asynchronously.
    pub fn offload_enabled(self) -> bool {
        self.offload_enabled
    }

    /// Returns whether stopping requires a committed checkpoint first.
    pub fn requires_checkpoint_before_stop(self) -> bool {
        self.requires_durable_authority()
    }
}
