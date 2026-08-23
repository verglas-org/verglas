//! SQLite pager for one recoverable lease-fenced DO worker or replica endpoint.

use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use rusqlite::{Connection, OptionalExtension, params};
use uuid::Uuid;

use crate::cas::LeaseIdentity;
use crate::error::{Error, Result};
use crate::offload::ManagedTransactionArchive;

/// Result of applying an authority-committed transaction to the local pager.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// The next contiguous sequence was inserted and made visible atomically.
    Applied,
    /// The exact transaction identity, sequence, and bytes were already applied.
    Duplicate,
}

/// One authority-committed transaction recovered in sequence order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaEntry {
    commit_sequence: u64,
    transaction_id: Uuid,
    canonical_envelope: Vec<u8>,
}

impl ReplicaEntry {
    /// Returns the authority-assigned transaction sequence.
    pub fn commit_sequence(&self) -> u64 {
        self.commit_sequence
    }

    /// Returns the retry-stable transaction identity.
    pub fn transaction_id(&self) -> Uuid {
        self.transaction_id
    }

    /// Returns the exact canonical envelope stored by the pager.
    pub fn canonical_envelope(&self) -> &[u8] {
        &self.canonical_envelope
    }
}

/// Durable replica watermarks recovered from SQLite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaState {
    applied_sequence: u64,
    archive_sequence: u64,
    checkpoint_sequence: u64,
}

impl ReplicaState {
    /// Returns the highest authority command applied to the pager.
    pub fn applied_sequence(self) -> u64 {
        self.applied_sequence
    }

    /// Returns the contiguous sequence verified in managed S3.
    pub fn archive_sequence(self) -> u64 {
        self.archive_sequence
    }

    /// Returns the newest committed SQLite/object checkpoint.
    pub fn checkpoint_sequence(self) -> u64 {
        self.checkpoint_sequence
    }
}

/// Thread-safe SQLite recovery image for one `verglasd` worker process.
pub struct SqliteReplicaStore {
    do_id: String,
    connection: Mutex<Connection>,
}

impl SqliteReplicaStore {
    /// Opens or creates a replica database and verifies its immutable DO identity.
    pub fn open(path: impl AsRef<Path>, do_id: impl Into<String>) -> Result<Self> {
        let do_id = do_id.into();
        let connection = Connection::open(path)?;
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             PRAGMA foreign_keys = ON;
             CREATE TABLE IF NOT EXISTS replica_state (
                 singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                 do_id TEXT NOT NULL,
                 applied_sequence INTEGER NOT NULL,
                 archive_sequence INTEGER NOT NULL,
                 checkpoint_sequence INTEGER NOT NULL,
                 checkpoint_identity TEXT,
                 lease_generation INTEGER NOT NULL,
                 lease_token_hash BLOB NOT NULL,
                 cleaned_sequence INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS committed_transactions (
                 commit_sequence INTEGER PRIMARY KEY,
                 transaction_id BLOB NOT NULL UNIQUE,
                 canonical_envelope BLOB NOT NULL,
                 archive_identity TEXT
             );",
        )?;
        connection.execute(
            "INSERT OR IGNORE INTO replica_state
             (singleton, do_id, applied_sequence, archive_sequence, checkpoint_sequence,
              lease_generation, lease_token_hash, cleaned_sequence)
             VALUES (1, ?1, 0, 0, 0, 0, X'', 0)",
            params![do_id],
        )?;
        let persisted: String = connection.query_row(
            "SELECT do_id FROM replica_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        if persisted != do_id {
            return Err(Error::ReplicaConflict(format!(
                "replica belongs to DO {persisted}, not {do_id}"
            )));
        }
        Ok(Self {
            do_id,
            connection: Mutex::new(connection),
        })
    }

    /// Returns the immutable Durable Object identity bound to this pager.
    pub fn do_id(&self) -> &str {
        &self.do_id
    }

    /// Returns all durable watermarks from one SQLite snapshot.
    pub fn state(&self) -> Result<ReplicaState> {
        let connection = self.connection()?;
        let (applied, archived, checkpointed): (i64, i64, i64) = connection.query_row(
            "SELECT applied_sequence, archive_sequence, checkpoint_sequence
             FROM replica_state WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        Ok(ReplicaState {
            applied_sequence: to_u64(applied)?,
            archive_sequence: to_u64(archived)?,
            checkpoint_sequence: to_u64(checkpointed)?,
        })
    }

    /// Atomically applies the next default-authority committed canonical envelope.
    pub fn apply_committed(
        &self,
        sequence: u64,
        transaction_id: Uuid,
        canonical_envelope: &[u8],
    ) -> Result<ApplyOutcome> {
        self.apply_with_lease(sequence, transaction_id, canonical_envelope, None)
    }

    /// Atomically applies one early-ack replica write under its launcher lease fence.
    pub fn apply_replicated(
        &self,
        lease: &LeaseIdentity,
        sequence: u64,
        transaction_id: Uuid,
        canonical_envelope: &[u8],
    ) -> Result<ApplyOutcome> {
        self.apply_with_lease(sequence, transaction_id, canonical_envelope, Some(lease))
    }

    /// Applies one canonical envelope and optional replica lease in one SQLite transaction.
    fn apply_with_lease(
        &self,
        sequence: u64,
        transaction_id: Uuid,
        canonical_envelope: &[u8],
        lease: Option<&LeaseIdentity>,
    ) -> Result<ApplyOutcome> {
        let sequence_i64 = to_i64(sequence)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        if let Some(lease) = lease {
            let (generation, token_hash): (i64, Vec<u8>) = transaction.query_row(
                "SELECT lease_generation, lease_token_hash
                 FROM replica_state WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let generation = to_u64(generation)?;
            let incoming_hash = lease.token_hash();
            if lease.generation() < generation
                || (lease.generation() == generation
                    && !token_hash.is_empty()
                    && token_hash.as_slice() != incoming_hash.as_slice())
            {
                return Err(Error::ReplicaConflict(
                    "replica write was fenced by a newer lease".to_owned(),
                ));
            }
            if lease.generation() > generation || token_hash.is_empty() {
                transaction.execute(
                    "UPDATE replica_state
                     SET lease_generation = ?1, lease_token_hash = ?2
                     WHERE singleton = 1",
                    params![to_i64(lease.generation())?, incoming_hash.as_slice()],
                )?;
            }
        }
        let existing = transaction
            .query_row(
                "SELECT commit_sequence, canonical_envelope
                 FROM committed_transactions WHERE transaction_id = ?1",
                params![transaction_id.as_bytes().as_slice()],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()?;
        if let Some((old_sequence, old_envelope)) = existing {
            if old_sequence == sequence_i64 && old_envelope == canonical_envelope {
                transaction.commit()?;
                return Ok(ApplyOutcome::Duplicate);
            }
            return Err(Error::ReplicaConflict(format!(
                "transaction {transaction_id} already names different committed content"
            )));
        }
        let applied: i64 = transaction.query_row(
            "SELECT applied_sequence FROM replica_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        let expected = to_u64(applied)?.saturating_add(1);
        if sequence != expected {
            return Err(Error::ReplicaSequence(format!(
                "commit sequence {sequence} does not follow applied sequence {}",
                expected.saturating_sub(1)
            )));
        }
        transaction.execute(
            "INSERT INTO committed_transactions
             (commit_sequence, transaction_id, canonical_envelope)
             VALUES (?1, ?2, ?3)",
            params![
                sequence_i64,
                transaction_id.as_bytes().as_slice(),
                canonical_envelope
            ],
        )?;
        transaction.execute(
            "UPDATE replica_state SET applied_sequence = ?1 WHERE singleton = 1",
            params![sequence_i64],
        )?;
        transaction.commit()?;
        Ok(ApplyOutcome::Applied)
    }

    /// Returns a prior transaction identity for exact worker retry handling.
    pub fn committed_transaction(&self, transaction_id: Uuid) -> Result<Option<ReplicaEntry>> {
        let connection = self.connection()?;
        let row = connection
            .query_row(
                "SELECT commit_sequence, canonical_envelope
                 FROM committed_transactions WHERE transaction_id = ?1",
                params![transaction_id.as_bytes().as_slice()],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
            )
            .optional()?;
        row.map(|(sequence, canonical_envelope)| {
            Ok(ReplicaEntry {
                commit_sequence: to_u64(sequence)?,
                transaction_id,
                canonical_envelope,
            })
        })
        .transpose()
    }

    /// Replays every committed pager entry in authority sequence order.
    pub fn replay(&self) -> Result<Vec<ReplicaEntry>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT commit_sequence, transaction_id, canonical_envelope
             FROM committed_transactions ORDER BY commit_sequence",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })?;
        let mut entries = Vec::new();
        for row in rows {
            let (sequence, transaction_id, canonical_envelope) = row?;
            entries.push(ReplicaEntry {
                commit_sequence: to_u64(sequence)?,
                transaction_id: Uuid::from_slice(&transaction_id)
                    .map_err(|error| Error::ReplicaConflict(error.to_string()))?,
                canonical_envelope,
            });
        }
        Ok(entries)
    }

    /// Returns a bounded failover tail after a replica client's recovered sequence.
    pub fn replay_after(&self, after: u64, limit: usize) -> Result<Vec<ReplicaEntry>> {
        if limit == 0 || limit > 1_024 {
            return Err(Error::ReplicaSequence(
                "replica replay limit must be between 1 and 1024".to_owned(),
            ));
        }
        let connection = self.connection()?;
        let cleaned: i64 = connection.query_row(
            "SELECT cleaned_sequence FROM replica_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        let cleaned = to_u64(cleaned)?;
        if after < cleaned {
            return Err(Error::ReplicaSequence(format!(
                "replay after {after} is below cleaned sequence {cleaned}"
            )));
        }
        let mut statement = connection.prepare(
            "SELECT commit_sequence, transaction_id, canonical_envelope
             FROM committed_transactions WHERE commit_sequence > ?1
             ORDER BY commit_sequence LIMIT ?2",
        )?;
        let rows = statement.query_map(params![to_i64(after)?, to_i64(limit as u64)?], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })?;
        let mut entries = Vec::new();
        for row in rows {
            let (sequence, transaction_id, canonical_envelope) = row?;
            entries.push(ReplicaEntry {
                commit_sequence: to_u64(sequence)?,
                transaction_id: Uuid::from_slice(&transaction_id)
                    .map_err(|error| Error::ReplicaConflict(error.to_string()))?,
                canonical_envelope,
            });
        }
        Ok(entries)
    }

    /// Deletes a replica tail only under its current lease and explicit clean watermark.
    pub fn clean_replicated(&self, lease: &LeaseIdentity, through: u64) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let (generation, token_hash, applied, cleaned, archived, checkpointed): (
            i64,
            Vec<u8>,
            i64,
            i64,
            i64,
            i64,
        ) = transaction.query_row(
            "SELECT lease_generation, lease_token_hash, applied_sequence, cleaned_sequence,
                    archive_sequence, checkpoint_sequence
             FROM replica_state WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )?;
        let incoming_hash = lease.token_hash();
        if lease.generation() != to_u64(generation)?
            || token_hash.as_slice() != incoming_hash.as_slice()
        {
            return Err(Error::ReplicaConflict(
                "replica clean was fenced by another lease".to_owned(),
            ));
        }
        let applied = to_u64(applied)?;
        let cleaned = to_u64(cleaned)?;
        let archived = to_u64(archived)?;
        let checkpointed = to_u64(checkpointed)?;
        if through < cleaned || through > applied {
            return Err(Error::ReplicaSequence(format!(
                "clean sequence {through} must be between {cleaned} and applied {applied}"
            )));
        }
        if through > archived {
            return Err(Error::ReplicaSequence(format!(
                "clean sequence {through} exceeds archived sequence {archived}"
            )));
        }
        if through > checkpointed {
            return Err(Error::ReplicaSequence(format!(
                "clean sequence {through} exceeds checkpointed sequence {checkpointed}"
            )));
        }
        transaction.execute(
            "DELETE FROM committed_transactions WHERE commit_sequence <= ?1",
            params![to_i64(through)?],
        )?;
        transaction.execute(
            "UPDATE replica_state SET cleaned_sequence = ?1 WHERE singleton = 1",
            params![to_i64(through)?],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Returns committed canonical envelopes not yet verified in managed S3.
    pub fn pending_archive(&self) -> Result<Vec<ManagedTransactionArchive>> {
        let connection = self.connection()?;
        let state = self.state_with(&connection)?;
        let mut statement = connection.prepare(
            "SELECT commit_sequence, transaction_id, canonical_envelope
             FROM committed_transactions
             WHERE commit_sequence > ?1
             ORDER BY commit_sequence",
        )?;
        let rows = statement.query_map(params![to_i64(state.archive_sequence)?], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })?;
        let mut pending = Vec::new();
        for row in rows {
            let (sequence, transaction_id, envelope) = row?;
            let transaction_id = Uuid::from_slice(&transaction_id)
                .map_err(|error| Error::ReplicaConflict(error.to_string()))?;
            pending.push(ManagedTransactionArchive::new(
                self.do_id.clone(),
                transaction_id,
                to_u64(sequence)?,
                envelope,
            ));
        }
        Ok(pending)
    }

    /// Propagates verified archive and checkpoint coverage under the active lease.
    pub fn mark_coverage(
        &self,
        lease: &LeaseIdentity,
        archived_through: u64,
        checkpointed_through: u64,
        checkpoint_identity: &str,
    ) -> Result<()> {
        if checkpoint_identity.is_empty() {
            return Err(Error::ReplicaSequence(
                "checkpoint identity cannot be empty".to_owned(),
            ));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let (generation, token_hash, applied, archived, checkpointed): (
            i64,
            Vec<u8>,
            i64,
            i64,
            i64,
        ) = transaction.query_row(
            "SELECT lease_generation, lease_token_hash, applied_sequence,
                        archive_sequence, checkpoint_sequence
                 FROM replica_state WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )?;
        let incoming_hash = lease.token_hash();
        if lease.generation() != to_u64(generation)?
            || token_hash.as_slice() != incoming_hash.as_slice()
        {
            return Err(Error::ReplicaConflict(
                "replica coverage was fenced by another lease".to_owned(),
            ));
        }
        let applied = to_u64(applied)?;
        let archived = to_u64(archived)?;
        let checkpointed = to_u64(checkpointed)?;
        if archived_through < archived || archived_through > applied {
            return Err(Error::ReplicaSequence(format!(
                "archive coverage {archived_through} must be between {archived} and applied {applied}"
            )));
        }
        if checkpointed_through < checkpointed || checkpointed_through > archived_through {
            return Err(Error::ReplicaSequence(format!(
                "checkpoint coverage {checkpointed_through} must be between {checkpointed} and archive {archived_through}"
            )));
        }
        let uncovered = archived_through.saturating_sub(archived);
        let committed: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM committed_transactions
             WHERE commit_sequence > ?1 AND commit_sequence <= ?2",
            params![to_i64(archived)?, to_i64(archived_through)?],
            |row| row.get(0),
        )?;
        if to_u64(committed)? != uncovered {
            return Err(Error::ReplicaSequence(
                "archive coverage skips a missing transaction".to_owned(),
            ));
        }
        if archived_through > archived {
            transaction.execute(
                "UPDATE committed_transactions
                 SET archive_identity = ?1
                 WHERE commit_sequence > ?2 AND commit_sequence <= ?3",
                params![
                    checkpoint_identity,
                    to_i64(archived)?,
                    to_i64(archived_through)?
                ],
            )?;
            transaction.execute(
                "UPDATE replica_state SET archive_sequence = ?1 WHERE singleton = 1",
                params![to_i64(archived_through)?],
            )?;
        }
        if checkpointed_through > checkpointed {
            transaction.execute(
                "UPDATE replica_state
                 SET checkpoint_sequence = ?1, checkpoint_identity = ?2
                 WHERE singleton = 1",
                params![to_i64(checkpointed_through)?, checkpoint_identity],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Advances the archive watermark by exactly one verified object.
    pub fn mark_archived(&self, sequence: u64, identity: &str) -> Result<()> {
        if identity.is_empty() {
            return Err(Error::ReplicaSequence(
                "archive identity cannot be empty".to_owned(),
            ));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let state = self.state_with(&transaction)?;
        let expected = state.archive_sequence.saturating_add(1);
        if sequence != expected || sequence > state.applied_sequence {
            return Err(Error::ReplicaSequence(format!(
                "archive sequence {sequence} must equal next sequence {expected} and not exceed applied {}",
                state.applied_sequence
            )));
        }
        let changed = transaction.execute(
            "UPDATE committed_transactions SET archive_identity = ?1
             WHERE commit_sequence = ?2",
            params![identity, to_i64(sequence)?],
        )?;
        if changed != 1 {
            return Err(Error::ReplicaSequence(format!(
                "no committed transaction exists at sequence {sequence}"
            )));
        }
        transaction.execute(
            "UPDATE replica_state SET archive_sequence = ?1 WHERE singleton = 1",
            params![to_i64(sequence)?],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Writes one transactionally consistent standalone SQLite recovery image.
    pub fn create_checkpoint(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path
            .as_ref()
            .to_str()
            .ok_or_else(|| Error::ReplicaSequence("checkpoint path is not UTF-8".to_owned()))?;
        let connection = self.connection()?;
        connection.execute_batch("PRAGMA wal_checkpoint(FULL);")?;
        connection.execute("VACUUM INTO ?1", params![path])?;
        Ok(())
    }

    /// Records a verified checkpoint that covers only archived committed state.
    pub fn mark_checkpointed(&self, sequence: u64, identity: &str) -> Result<()> {
        if identity.is_empty() {
            return Err(Error::ReplicaSequence(
                "checkpoint identity cannot be empty".to_owned(),
            ));
        }
        let connection = self.connection()?;
        let state = self.state_with(&connection)?;
        if sequence < state.checkpoint_sequence || sequence > state.archive_sequence {
            return Err(Error::ReplicaSequence(format!(
                "checkpoint sequence {sequence} must be monotonic and not exceed archive {}",
                state.archive_sequence
            )));
        }
        connection.execute(
            "UPDATE replica_state
             SET checkpoint_sequence = ?1, checkpoint_identity = ?2
             WHERE singleton = 1",
            params![to_i64(sequence)?, identity],
        )?;
        Ok(())
    }

    /// Acquires the SQLite connection after detecting a poisoned owner.
    fn connection(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection.lock().map_err(|_| Error::Poisoned)
    }

    /// Reads replica state inside an existing SQLite transaction or connection.
    fn state_with(&self, connection: &Connection) -> Result<ReplicaState> {
        let (applied, archived, checkpointed): (i64, i64, i64) = connection.query_row(
            "SELECT applied_sequence, archive_sequence, checkpoint_sequence
             FROM replica_state WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        Ok(ReplicaState {
            applied_sequence: to_u64(applied)?,
            archive_sequence: to_u64(archived)?,
            checkpoint_sequence: to_u64(checkpointed)?,
        })
    }
}

/// Converts an external sequence into SQLite's signed integer domain.
fn to_i64(value: u64) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| Error::ReplicaSequence(format!("sequence {value} exceeds SQLite INTEGER")))
}

/// Converts a persisted SQLite sequence after rejecting corrupt negative state.
fn to_u64(value: i64) -> Result<u64> {
    u64::try_from(value)
        .map_err(|_| Error::ReplicaSequence(format!("negative persisted sequence {value}")))
}
