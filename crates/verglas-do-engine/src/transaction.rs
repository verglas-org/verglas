//! Canonical cross-domain transaction envelopes and commit authority seam.

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use async_trait::async_trait;
use std::io::{Cursor, Read};
use uuid::Uuid;

use crate::error::{Error, Result};

/// Isolation level selected when a Durable Object transaction begins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Every statement reads the transaction's fixed base snapshot plus private writes.
    Snapshot,
    /// Commit validation must reject reads invalidated after the base snapshot.
    Serializable,
}

/// One mutation domain applied atomically from a transaction envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MutationDomain {
    /// Ordinary relational row state.
    Relational,
    /// Vamana vector delta state derived from canonical table rows.
    Vector,
    /// Graph adjacency delta state derived from canonical edge rows.
    Graph,
}

impl MutationDomain {
    /// Returns the stable byte used by the pre-release canonical envelope encoding.
    fn tag(self) -> u8 {
        match self {
            Self::Relational => 1,
            Self::Vector => 2,
            Self::Graph => 3,
        }
    }
}

/// Stable identity of one table inside a Durable Object.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(String);

impl TableId {
    /// Creates a table identity from its SQL-visible name.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the SQL-visible table name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One deterministic Arrow mutation batch in transaction statement order.
#[derive(Debug, Clone)]
pub struct MutationBatch {
    domain: MutationDomain,
    table: TableId,
    batch: RecordBatch,
}

impl MutationBatch {
    /// Builds one domain-specific mutation batch.
    pub fn new(domain: MutationDomain, table: TableId, batch: RecordBatch) -> Self {
        Self {
            domain,
            table,
            batch,
        }
    }

    /// Returns the state domain updated by this batch.
    pub fn domain(&self) -> MutationDomain {
        self.domain
    }

    /// Returns the table updated by this batch.
    pub fn table(&self) -> &TableId {
        &self.table
    }

    /// Returns the Arrow rows carried by this mutation.
    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Encodes the schema and rows as one deterministic Arrow IPC stream.
    fn ipc_bytes(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, &self.batch.schema())?;
            writer.write(&self.batch)?;
            writer.finish()?;
        }
        Ok(bytes)
    }
}

/// The one logical command submitted to the container's commit authority.
#[derive(Debug, Clone)]
pub struct TransactionEnvelope {
    do_id: String,
    transaction_id: Uuid,
    base_commit_sequence: u64,
    isolation: IsolationLevel,
    mutations: Vec<MutationBatch>,
}

impl TransactionEnvelope {
    /// Creates an empty transaction envelope at a fixed base snapshot.
    pub fn new(
        do_id: impl Into<String>,
        transaction_id: Uuid,
        base_commit_sequence: u64,
        isolation: IsolationLevel,
    ) -> Self {
        Self {
            do_id: do_id.into(),
            transaction_id,
            base_commit_sequence,
            isolation,
            mutations: Vec::new(),
        }
    }

    /// Returns the Durable Object identity whose worker owns this command.
    pub fn do_id(&self) -> &str {
        &self.do_id
    }

    /// Returns the retry-stable transaction identity.
    pub fn transaction_id(&self) -> Uuid {
        self.transaction_id
    }

    /// Returns the snapshot sequence from which validation begins.
    pub fn base_commit_sequence(&self) -> u64 {
        self.base_commit_sequence
    }

    /// Returns the selected isolation policy.
    pub fn isolation(&self) -> IsolationLevel {
        self.isolation
    }

    /// Returns transaction mutations in SQL statement order.
    pub fn mutations(&self) -> &[MutationBatch] {
        &self.mutations
    }

    /// Appends one Arrow mutation without publishing it outside the transaction.
    pub fn append(&mut self, domain: MutationDomain, table: TableId, batch: RecordBatch) {
        self.mutations
            .push(MutationBatch::new(domain, table, batch));
    }

    /// Serializes the exact command hashed and proposed to consensus.
    ///
    /// This pre-release encoding is intentionally direct and has no format-version
    /// negotiation. The envelope boundary is the future protocol extension point.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        let mut output = Cursor::new(Vec::new());
        put_bytes(&mut output, self.do_id.as_bytes());
        output
            .get_mut()
            .extend_from_slice(self.transaction_id.as_bytes());
        output
            .get_mut()
            .extend_from_slice(&self.base_commit_sequence.to_le_bytes());
        output.get_mut().push(match self.isolation {
            IsolationLevel::Snapshot => 1,
            IsolationLevel::Serializable => 2,
        });
        output
            .get_mut()
            .extend_from_slice(&(self.mutations.len() as u64).to_le_bytes());
        for mutation in &self.mutations {
            output.get_mut().push(mutation.domain.tag());
            put_bytes(&mut output, mutation.table.as_str().as_bytes());
            put_bytes(&mut output, &mutation.ipc_bytes()?);
        }
        Ok(output.into_inner())
    }

    /// Decodes canonical bytes for deterministic follower apply and restart replay.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self> {
        let mut input = Cursor::new(bytes);
        let do_id = read_string(&mut input)?;
        let mut transaction_id = [0_u8; 16];
        input
            .read_exact(&mut transaction_id)
            .map_err(invalid_envelope_io)?;
        let transaction_id = Uuid::from_bytes(transaction_id);
        let base_commit_sequence = read_u64(&mut input)?;
        let isolation = match read_u8(&mut input)? {
            1 => IsolationLevel::Snapshot,
            2 => IsolationLevel::Serializable,
            other => {
                return Err(Error::InvalidEnvelope(format!(
                    "unknown isolation tag {other}"
                )));
            }
        };
        let mutation_count = usize::try_from(read_u64(&mut input)?)
            .map_err(|_| Error::InvalidEnvelope("mutation count exceeds memory".to_owned()))?;
        let mut mutations = Vec::with_capacity(mutation_count);
        for _ in 0..mutation_count {
            let domain = match read_u8(&mut input)? {
                1 => MutationDomain::Relational,
                2 => MutationDomain::Vector,
                3 => MutationDomain::Graph,
                other => {
                    return Err(Error::InvalidEnvelope(format!(
                        "unknown mutation domain tag {other}"
                    )));
                }
            };
            let table = TableId::new(read_string(&mut input)?);
            let ipc = read_bytes(&mut input)?;
            let mut reader = StreamReader::try_new(Cursor::new(ipc), None)?;
            let batch = reader
                .next()
                .ok_or_else(|| Error::InvalidEnvelope("mutation IPC has no batch".to_owned()))??;
            if reader.next().is_some() {
                return Err(Error::InvalidEnvelope(
                    "mutation IPC contains more than one batch".to_owned(),
                ));
            }
            mutations.push(MutationBatch::new(domain, table, batch));
        }
        if input.position()
            != u64::try_from(bytes.len())
                .map_err(|_| Error::InvalidEnvelope("envelope length exceeds u64".to_owned()))?
        {
            return Err(Error::InvalidEnvelope(
                "trailing bytes after transaction envelope".to_owned(),
            ));
        }
        Ok(Self {
            do_id,
            transaction_id,
            base_commit_sequence,
            isolation,
            mutations,
        })
    }
}

/// Appends a length-prefixed byte string to the canonical command.
fn put_bytes(output: &mut Cursor<Vec<u8>>, value: &[u8]) {
    output
        .get_mut()
        .extend_from_slice(&(value.len() as u64).to_le_bytes());
    output.get_mut().extend_from_slice(value);
}

/// Reads one byte from a canonical envelope.
fn read_u8(input: &mut Cursor<&[u8]>) -> Result<u8> {
    let mut value = [0_u8; 1];
    input.read_exact(&mut value).map_err(invalid_envelope_io)?;
    Ok(value[0])
}

/// Reads one little-endian integer from a canonical envelope.
fn read_u64(input: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut value = [0_u8; 8];
    input.read_exact(&mut value).map_err(invalid_envelope_io)?;
    Ok(u64::from_le_bytes(value))
}

/// Reads one bounded length-prefixed byte string.
fn read_bytes(input: &mut Cursor<&[u8]>) -> Result<Vec<u8>> {
    let length = usize::try_from(read_u64(input)?)
        .map_err(|_| Error::InvalidEnvelope("byte string exceeds memory".to_owned()))?;
    let remaining = input
        .get_ref()
        .len()
        .saturating_sub(usize::try_from(input.position()).unwrap_or(usize::MAX));
    if length > remaining {
        return Err(Error::InvalidEnvelope(format!(
            "byte string length {length} exceeds remaining envelope {remaining}"
        )));
    }
    let mut value = vec![0_u8; length];
    input.read_exact(&mut value).map_err(invalid_envelope_io)?;
    Ok(value)
}

/// Reads one UTF-8 string from a canonical envelope.
fn read_string(input: &mut Cursor<&[u8]>) -> Result<String> {
    String::from_utf8(read_bytes(input)?).map_err(|error| Error::InvalidEnvelope(error.to_string()))
}

/// Converts a short-read into a canonical-envelope error.
fn invalid_envelope_io(error: std::io::Error) -> Error {
    Error::InvalidEnvelope(error.to_string())
}

/// Receipt proving the canonical envelope committed at one authority sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitReceipt {
    commit_sequence: u64,
    transaction_id: Uuid,
}

impl CommitReceipt {
    /// Creates a receipt returned by the sole commit authority.
    pub fn new(commit_sequence: u64, transaction_id: Uuid) -> Self {
        Self {
            commit_sequence,
            transaction_id,
        }
    }

    /// Returns the sequence shared by all domains in this transaction.
    pub fn commit_sequence(self) -> u64 {
        self.commit_sequence
    }

    /// Returns the exact transaction identity accepted by the authority.
    pub fn transaction_id(self) -> Uuid {
        self.transaction_id
    }
}

/// The sole authority allowed to assign a Durable Object commit sequence.
#[async_trait]
pub trait CommitAuthority: Send + Sync + 'static {
    /// Durably commits one canonical envelope before returning its receipt.
    async fn commit(&self, envelope: &TransactionEnvelope) -> Result<CommitReceipt>;
}

/// Private transaction state accumulated by DataFusion DML plans.
pub trait DoTransaction: Send + Sync {
    /// Adds one deterministic Arrow batch to this transaction's private write set.
    fn append(&mut self, domain: MutationDomain, table: TableId, batch: RecordBatch) -> Result<()>;

    /// Returns the immutable command view used for validation and commit.
    fn envelope(&self) -> &TransactionEnvelope;
}

/// Default transaction implementation owned by the embedded engine.
pub struct EngineTransaction {
    envelope: TransactionEnvelope,
}

impl EngineTransaction {
    /// Builds private transaction state around an empty canonical envelope.
    pub fn new(envelope: TransactionEnvelope) -> Self {
        Self { envelope }
    }
}

impl DoTransaction for EngineTransaction {
    /// Appends one mutation to the private write set.
    fn append(&mut self, domain: MutationDomain, table: TableId, batch: RecordBatch) -> Result<()> {
        self.envelope.append(domain, table, batch);
        Ok(())
    }

    /// Returns the transaction envelope accumulated so far.
    fn envelope(&self) -> &TransactionEnvelope {
        &self.envelope
    }
}
