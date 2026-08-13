//! OpenRaft's authoritative storage conformance suite for Verglas persistence.

use openraft::StorageError;
use openraft::testing::{StoreBuilder, Suite};
use tempfile::TempDir;
use verglas_consensus::{PersistentLogStore, PersistentStateMachine, VerglasRaftConfig};

struct PersistentBuilder;

impl StoreBuilder<VerglasRaftConfig, PersistentLogStore, PersistentStateMachine, TempDir>
    for PersistentBuilder
{
    async fn build(
        &self,
    ) -> Result<(TempDir, PersistentLogStore, PersistentStateMachine), StorageError<u64>> {
        let root = TempDir::new().map_err(|error| openraft::StorageIOError::write(&error))?;
        let log = PersistentLogStore::open(root.path().join("raft-log.json")).await?;
        let state = PersistentStateMachine::open(root.path().join("state-machine.json")).await?;
        Ok((root, log, state))
    }
}

#[allow(clippy::result_large_err)]
#[test]
fn persistent_raft_storage_passes_openraft_conformance() -> Result<(), StorageError<u64>> {
    Suite::test_all(PersistentBuilder)
}
