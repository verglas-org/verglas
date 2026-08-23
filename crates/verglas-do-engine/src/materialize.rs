//! Explicit coverage metadata and immutable bytes for asynchronous lake artifacts.

use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::transaction::TableId;

/// Kind of optional derived object produced after a durable transaction ACK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArtifactKind {
    /// Snapshot-bound Vamana index in an Iceberg Puffin container.
    VamanaPuffin,
    /// Snapshot-bound graph adjacency in an Iceberg Puffin container.
    GraphPuffin,
    /// Relational Arrow rows converted to Parquet.
    Parquet,
    /// Iceberg metadata referencing covered Parquet and Puffin objects.
    IcebergMetadata,
}

/// Contiguous DO transaction interval represented by one derived artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactCoverage {
    from_exclusive: u64,
    through: u64,
}

impl ArtifactCoverage {
    /// Creates a nonempty contiguous coverage interval.
    pub fn new(from_exclusive: u64, through: u64) -> Result<Self> {
        if through <= from_exclusive {
            return Err(Error::Materialization(format!(
                "coverage through {through} must exceed {from_exclusive}"
            )));
        }
        Ok(Self {
            from_exclusive,
            through,
        })
    }

    /// Returns the prior published sequence excluded from this artifact.
    pub fn from_exclusive(self) -> u64 {
        self.from_exclusive
    }

    /// Returns the highest transaction included in this artifact.
    pub fn through(self) -> u64 {
        self.through
    }
}

/// Immutable materialization result ready for explicit object-store publication.
pub struct DerivedArtifact {
    do_id: String,
    table: TableId,
    kind: ArtifactKind,
    coverage: ArtifactCoverage,
    bytes: Vec<u8>,
}

impl DerivedArtifact {
    /// Creates immutable derived bytes with explicit source coverage.
    pub fn new(
        do_id: String,
        table: TableId,
        kind: ArtifactKind,
        coverage: ArtifactCoverage,
        bytes: Vec<u8>,
    ) -> Self {
        Self {
            do_id,
            table,
            kind,
            coverage,
            bytes,
        }
    }

    /// Returns the owning Durable Object identity.
    pub fn do_id(&self) -> &str {
        &self.do_id
    }

    /// Returns the source table identity.
    pub fn table(&self) -> &TableId {
        &self.table
    }

    /// Returns the derived object kind.
    pub fn kind(&self) -> ArtifactKind {
        self.kind
    }

    /// Returns the exact transaction interval represented by these bytes.
    pub fn coverage(&self) -> ArtifactCoverage {
        self.coverage
    }

    /// Returns the immutable artifact bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Verified object identity and source coverage of one published artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactReceipt {
    object_path: String,
    sha256: String,
    coverage: ArtifactCoverage,
}

impl ArtifactReceipt {
    /// Returns the provider-neutral object path.
    pub fn object_path(&self) -> &str {
        &self.object_path
    }

    /// Returns the SHA-256 identity verified by reading the object back.
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    /// Returns the exact source interval represented by the object.
    pub fn coverage(&self) -> ArtifactCoverage {
        self.coverage
    }
}

/// Provider-neutral publisher for explicitly requested derived artifacts.
pub struct ObjectStoreDerivedArtifactPublisher {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl ObjectStoreDerivedArtifactPublisher {
    /// Creates a publisher beneath one managed lake/offload prefix.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl AsRef<str>) -> Self {
        Self {
            store,
            prefix: Path::from(prefix.as_ref()),
        }
    }

    /// Uploads immutable bytes, reads them back, and returns verified coverage.
    pub async fn publish(&self, artifact: &DerivedArtifact) -> Result<ArtifactReceipt> {
        let extension = match artifact.kind {
            ArtifactKind::VamanaPuffin | ArtifactKind::GraphPuffin => "puffin",
            ArtifactKind::Parquet => "parquet",
            ArtifactKind::IcebergMetadata => "json",
        };
        let kind = match artifact.kind {
            ArtifactKind::VamanaPuffin => "vamana-puffin",
            ArtifactKind::GraphPuffin => "graph-puffin",
            ArtifactKind::Parquet => "parquet",
            ArtifactKind::IcebergMetadata => "iceberg-metadata",
        };
        let path = self
            .prefix
            .clone()
            .join(artifact.do_id.as_str())
            .join(artifact.table.as_str())
            .join(kind)
            .join(format!("{}.{}", artifact.coverage.through, extension));
        match self
            .store
            .put_opts(
                &path,
                Bytes::copy_from_slice(&artifact.bytes).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(error) => return Err(Error::Materialization(error.to_string())),
        }
        let actual = self
            .store
            .get(&path)
            .await
            .map_err(|error| Error::Materialization(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| Error::Materialization(error.to_string()))?;
        if actual.as_ref() != artifact.bytes {
            return Err(Error::Materialization(format!(
                "verification mismatch for {path}"
            )));
        }
        Ok(ArtifactReceipt {
            object_path: path.to_string(),
            sha256: hex::encode(Sha256::digest(&actual)),
            coverage: artifact.coverage,
        })
    }
}
