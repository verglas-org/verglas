//! Verified object-store publication for standalone SQLite recovery checkpoints.

use std::path::Path as FilePath;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::{Error, Result, SqliteReplicaStore};

/// Verified immutable checkpoint identity returned to lifecycle coordination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointReceipt {
    through_sequence: u64,
    object_path: String,
    sha256: String,
}

impl CheckpointReceipt {
    /// Creates a checkpoint receipt discovered in managed object storage.
    pub fn new(
        through_sequence: u64,
        object_path: impl Into<String>,
        sha256: impl Into<String>,
    ) -> Result<Self> {
        let object_path = object_path.into();
        let sha256 = sha256.into();
        if object_path.is_empty() || sha256.is_empty() {
            return Err(Error::Materialization(
                "checkpoint receipt identity cannot be empty".to_owned(),
            ));
        }
        Ok(Self {
            through_sequence,
            object_path,
            sha256,
        })
    }

    /// Returns the contiguous transaction sequence captured by SQLite.
    pub fn through_sequence(&self) -> u64 {
        self.through_sequence
    }

    /// Returns the immutable managed-object path.
    pub fn object_path(&self) -> &str {
        &self.object_path
    }

    /// Returns the verified lowercase SHA-256 identity.
    pub fn sha256(&self) -> &str {
        &self.sha256
    }
}

/// Publishes SQLite recovery images to an explicitly managed object store.
pub struct ObjectStoreCheckpointPublisher {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl ObjectStoreCheckpointPublisher {
    /// Binds checkpoint publication to one managed bucket prefix.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl AsRef<str>) -> Self {
        Self {
            store,
            prefix: Path::from(prefix.as_ref()),
        }
    }

    /// Restores a read-verified checkpoint through an fsynced temporary file.
    pub async fn restore(
        &self,
        do_id: &str,
        receipt: &CheckpointReceipt,
        destination: impl AsRef<FilePath>,
    ) -> Result<SqliteReplicaStore> {
        if tokio::fs::try_exists(destination.as_ref())
            .await
            .map_err(|error| Error::Materialization(error.to_string()))?
        {
            return Err(Error::Materialization(
                "checkpoint restore destination already exists".to_owned(),
            ));
        }
        let bytes = self
            .store
            .get(&Path::from(receipt.object_path.as_str()))
            .await
            .map_err(|error| Error::Materialization(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| Error::Materialization(error.to_string()))?;
        let actual_hash = hex::encode(Sha256::digest(&bytes));
        if actual_hash != receipt.sha256 {
            return Err(Error::Materialization(
                "checkpoint restore hash mismatch".to_owned(),
            ));
        }
        if let Some(parent) = destination.as_ref().parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| Error::Materialization(error.to_string()))?;
        }
        let temporary = destination.as_ref().with_extension("restore.tmp");
        match tokio::fs::remove_file(&temporary).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::Materialization(error.to_string())),
        }
        let mut file = tokio::fs::File::create(&temporary)
            .await
            .map_err(|error| Error::Materialization(error.to_string()))?;
        file.write_all(&bytes)
            .await
            .map_err(|error| Error::Materialization(error.to_string()))?;
        file.sync_all()
            .await
            .map_err(|error| Error::Materialization(error.to_string()))?;
        drop(file);
        tokio::fs::rename(&temporary, destination.as_ref())
            .await
            .map_err(|error| Error::Materialization(error.to_string()))?;
        if let Some(parent) = destination.as_ref().parent() {
            tokio::fs::File::open(parent)
                .await
                .map_err(|error| Error::Materialization(error.to_string()))?
                .sync_all()
                .await
                .map_err(|error| Error::Materialization(error.to_string()))?;
        }
        SqliteReplicaStore::open(destination, do_id)
    }

    /// Creates, uploads, reads back, and only then records one checkpoint watermark.
    pub async fn publish(
        &self,
        replica: &SqliteReplicaStore,
        local_path: impl AsRef<FilePath>,
    ) -> Result<CheckpointReceipt> {
        let state = replica.state()?;
        let sequence = state.archive_sequence();
        if sequence == 0 || sequence != state.applied_sequence() {
            return Err(Error::ReplicaSequence(format!(
                "checkpoint requires archive coverage through applied sequence {}",
                state.applied_sequence()
            )));
        }
        match tokio::fs::remove_file(local_path.as_ref()).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::Materialization(error.to_string())),
        }
        replica.create_checkpoint(local_path.as_ref())?;
        let bytes = tokio::fs::read(local_path.as_ref())
            .await
            .map_err(|error| Error::Materialization(error.to_string()))?;
        let hash = hex::encode(Sha256::digest(&bytes));
        let object_path = self
            .prefix
            .clone()
            .join(replica.do_id())
            .join("checkpoints")
            .join(format!("{sequence:020}-{hash}.sqlite"));
        match self
            .store
            .put_opts(
                &object_path,
                Bytes::copy_from_slice(&bytes).into(),
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
        let verified = self
            .store
            .get(&object_path)
            .await
            .map_err(|error| Error::Materialization(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| Error::Materialization(error.to_string()))?;
        if verified.as_ref() != bytes {
            return Err(Error::Materialization(
                "checkpoint read-back bytes differ".to_owned(),
            ));
        }
        let identity = object_path.to_string();
        replica.mark_checkpointed(sequence, &identity)?;
        Ok(CheckpointReceipt {
            through_sequence: sequence,
            object_path: identity,
            sha256: hash,
        })
    }
}
