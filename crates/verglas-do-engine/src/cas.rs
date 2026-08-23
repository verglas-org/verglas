//! Lease-generation-fenced conditional object-store commit authority.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{CommitAuthority, CommitReceipt, Error, Result, TransactionEnvelope};

const HEAD_MAGIC: &[u8; 8] = b"VGDOHEAD";

/// Opaque lock ownership passed to a worker by the launcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseIdentity {
    token: String,
    generation: u64,
}

impl LeaseIdentity {
    /// Creates one opaque ownership token at a monotonic lease generation.
    pub fn new(token: impl Into<String>, generation: u64) -> Self {
        Self {
            token: token.into(),
            generation,
        }
    }

    /// Returns the monotonic ownership generation.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the opaque token only for a private replica transport.
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Returns the stable token digest persisted by replica fences.
    pub(crate) fn token_hash(&self) -> [u8; 32] {
        Sha256::digest(self.token.as_bytes()).into()
    }
}

/// Exact conditional-object version and sequence held by one spawned worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseGrant {
    identity: LeaseIdentity,
    sequence: u64,
    version: UpdateVersion,
}

impl LeaseGrant {
    /// Returns the committed sequence covered by this already-held lease.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the opaque token carried by this held lease.
    pub fn token(&self) -> &str {
        self.identity.token()
    }

    /// Returns the monotonic generation carried by this held lease.
    pub fn generation(&self) -> u64 {
        self.identity.generation()
    }

    /// Returns the exact ETag required for the next conditional head update.
    pub fn e_tag(&self) -> Option<&str> {
        self.version.e_tag.as_deref()
    }

    /// Returns the exact object version required for the next conditional head update.
    pub fn version(&self) -> Option<&str> {
        self.version.version.as_deref()
    }

    /// Creates a launcher grant from the successful conditional lock result.
    pub fn new(
        identity: LeaseIdentity,
        sequence: u64,
        e_tag: Option<String>,
        version: Option<String>,
    ) -> Result<Self> {
        if e_tag.is_none() && version.is_none() {
            return Err(Error::Authority(
                "held lease requires an object ETag or version".to_owned(),
            ));
        }
        Ok(Self {
            identity,
            sequence,
            version: UpdateVersion { e_tag, version },
        })
    }
}

struct CasState {
    grant: LeaseGrant,
}

/// Default single-worker authority: immutable transaction object followed by head CAS.
pub struct CasCommitAuthority {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    do_id: String,
    state: Mutex<CasState>,
}

impl CasCommitAuthority {
    /// Creates a new empty DO and acquires its first conditional head version.
    pub async fn acquire(
        store: Arc<dyn ObjectStore>,
        prefix: impl AsRef<str>,
        do_id: impl Into<String>,
        identity: LeaseIdentity,
    ) -> Result<Self> {
        let prefix = Path::from(prefix.as_ref());
        let do_id = do_id.into();
        let head_path = head_path(&prefix, &do_id);
        let bytes = encode_head(&identity, 0, Uuid::nil(), [0; 32]);
        let result = store
            .put_opts(
                &head_path,
                Bytes::copy_from_slice(&bytes).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| Error::Authority(format!("acquire lease CAS failed: {error}")))?;
        verify_object(store.as_ref(), &head_path, &bytes).await?;
        Self::from_grant(
            store,
            prefix.as_ref(),
            do_id,
            LeaseGrant {
                identity,
                sequence: 0,
                version: result.into(),
            },
        )
    }

    /// Atomically hands an existing DO head to a higher launcher generation.
    pub async fn handoff(
        store: Arc<dyn ObjectStore>,
        prefix: impl AsRef<str>,
        do_id: impl Into<String>,
        previous: LeaseGrant,
        identity: LeaseIdentity,
    ) -> Result<Self> {
        if identity.generation <= previous.identity.generation {
            return Err(Error::Authority(
                "lease handoff generation must increase".to_owned(),
            ));
        }
        let prefix = Path::from(prefix.as_ref());
        let do_id = do_id.into();
        let path = head_path(&prefix, &do_id);
        let current = store
            .get(&path)
            .await
            .map_err(|error| Error::Authority(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| Error::Authority(error.to_string()))?;
        let decoded = decode_head(&current)?;
        if decoded.generation != previous.identity.generation
            || decoded.sequence != previous.sequence
            || decoded.token_hash != previous.identity.token_hash()
        {
            return Err(Error::Authority(
                "launcher lease grant does not match the current DO head".to_owned(),
            ));
        }
        let head = encode_head(
            &identity,
            decoded.sequence,
            decoded.transaction_id,
            decoded.payload_hash,
        );
        let result = store
            .put_opts(
                &path,
                Bytes::copy_from_slice(&head).into(),
                PutOptions {
                    mode: PutMode::Update(previous.version),
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| Error::Authority(format!("lease handoff CAS failed: {error}")))?;
        verify_object(store.as_ref(), &path, &head).await?;
        Self::from_grant(
            store,
            prefix.as_ref(),
            do_id,
            LeaseGrant {
                identity,
                sequence: decoded.sequence,
                version: result.into(),
            },
        )
    }

    /// Reconstructs an authority from a launcher-provided already-held lease grant.
    pub fn from_grant(
        store: Arc<dyn ObjectStore>,
        prefix: impl AsRef<str>,
        do_id: impl Into<String>,
        grant: LeaseGrant,
    ) -> Result<Self> {
        if grant.identity.token.is_empty() {
            return Err(Error::Authority("lease token cannot be empty".to_owned()));
        }
        Ok(Self {
            store,
            prefix: Path::from(prefix.as_ref()),
            do_id: do_id.into(),
            state: Mutex::new(CasState { grant }),
        })
    }

    /// Verifies that the launcher grant still names the current managed head.
    pub async fn validate_grant(&self) -> Result<()> {
        let grant = self.lease_grant()?;
        let path = head_path(&self.prefix, &self.do_id);
        let metadata = self
            .store
            .head(&path)
            .await
            .map_err(|error| Error::Authority(format!("managed head read failed: {error}")))?;
        if grant.version.e_tag.as_deref() != metadata.e_tag.as_deref()
            || grant.version.version.as_deref() != metadata.version.as_deref()
        {
            return Err(Error::Authority(
                "held lease version does not match the managed head".to_owned(),
            ));
        }
        let bytes = self
            .store
            .get(&path)
            .await
            .map_err(|error| Error::Authority(format!("managed head read failed: {error}")))?
            .bytes()
            .await
            .map_err(|error| Error::Authority(format!("managed head read failed: {error}")))?;
        let decoded = decode_head(&bytes)?;
        if decoded.generation != grant.identity.generation
            || decoded.sequence != grant.sequence
            || decoded.token_hash != grant.identity.token_hash()
        {
            return Err(Error::Authority(
                "held lease identity does not match the managed head".to_owned(),
            ));
        }
        Ok(())
    }

    /// Returns a copy suitable for passing an already-held lock into one worker.
    pub fn lease_grant(&self) -> Result<LeaseGrant> {
        self.state
            .try_lock()
            .map(|state| state.grant.clone())
            .map_err(|_| Error::Authority("lease grant is currently committing".to_owned()))
    }

    /// Builds the immutable transaction object path for one commit.
    fn transaction_path(&self, sequence: u64, transaction_id: Uuid) -> Path {
        self.prefix
            .clone()
            .join(self.do_id.as_str())
            .join("transactions")
            .join(format!("{sequence:020}-{transaction_id}.arrow"))
    }
}

#[async_trait]
impl CommitAuthority for CasCommitAuthority {
    /// Persists exact bytes and acknowledges only after the lease-fenced head CAS.
    async fn commit(&self, envelope: &TransactionEnvelope) -> Result<CommitReceipt> {
        if envelope.do_id() != self.do_id {
            return Err(Error::WrongDo {
                expected: self.do_id.clone(),
                actual: envelope.do_id().to_owned(),
            });
        }
        let canonical = envelope.canonical_bytes()?;
        let mut state = self.state.lock().await;
        if envelope.base_commit_sequence() != state.grant.sequence {
            return Err(Error::Authority(format!(
                "transaction base {} does not match CAS head {}",
                envelope.base_commit_sequence(),
                state.grant.sequence
            )));
        }
        let sequence = state.grant.sequence.saturating_add(1);
        let transaction_path = self.transaction_path(sequence, envelope.transaction_id());
        match self
            .store
            .put_opts(
                &transaction_path,
                Bytes::copy_from_slice(&canonical).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => {}
            Err(object_store::Error::AlreadyExists { .. }) => {
                verify_object(self.store.as_ref(), &transaction_path, &canonical).await?;
            }
            Err(error) => return Err(Error::Authority(error.to_string())),
        }
        verify_object(self.store.as_ref(), &transaction_path, &canonical).await?;
        let payload_hash: [u8; 32] = Sha256::digest(&canonical).into();
        let head = encode_head(
            &state.grant.identity,
            sequence,
            envelope.transaction_id(),
            payload_hash,
        );
        let result = match self
            .store
            .put_opts(
                &head_path(&self.prefix, &self.do_id),
                Bytes::copy_from_slice(&head).into(),
                PutOptions {
                    mode: PutMode::Update(state.grant.version.clone()),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(result) => result,
            Err(error @ object_store::Error::Precondition { .. }) => {
                discard_uncommitted_transaction(
                    self.store.as_ref(),
                    &transaction_path,
                    &head_path(&self.prefix, &self.do_id),
                    sequence,
                    envelope.transaction_id(),
                )
                .await;
                return Err(Error::Authority(format!("lease CAS failed: {error}")));
            }
            Err(error) => {
                return Err(Error::Authority(format!("lease CAS failed: {error}")));
            }
        };
        verify_object(
            self.store.as_ref(),
            &head_path(&self.prefix, &self.do_id),
            &head,
        )
        .await?;
        state.grant.sequence = sequence;
        state.grant.version = result.into();
        Ok(CommitReceipt::new(sequence, envelope.transaction_id()))
    }
}

/// Returns the one mutable conditional head object for a DO.
fn head_path(prefix: &Path, do_id: &str) -> Path {
    prefix.clone().join(do_id).join("head")
}

/// Encodes the lease fence and latest committed transaction in a fixed schema.
fn encode_head(
    identity: &LeaseIdentity,
    sequence: u64,
    transaction_id: Uuid,
    payload_hash: [u8; 32],
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(104);
    bytes.extend_from_slice(HEAD_MAGIC);
    bytes.extend_from_slice(&identity.generation.to_be_bytes());
    bytes.extend_from_slice(&sequence.to_be_bytes());
    bytes.extend_from_slice(transaction_id.as_bytes());
    bytes.extend_from_slice(&payload_hash);
    bytes.extend_from_slice(&Sha256::digest(identity.token.as_bytes()));
    bytes
}

struct DecodedHead {
    generation: u64,
    sequence: u64,
    transaction_id: Uuid,
    payload_hash: [u8; 32],
    token_hash: [u8; 32],
}

/// Decodes the fixed managed head schema used during launcher handoff.
fn decode_head(bytes: &[u8]) -> Result<DecodedHead> {
    if bytes.len() != 104 || &bytes[..8] != HEAD_MAGIC {
        return Err(Error::Authority("invalid managed DO head".to_owned()));
    }
    let generation = u64::from_be_bytes(
        bytes[8..16]
            .try_into()
            .map_err(|_| Error::Authority("invalid lease generation".to_owned()))?,
    );
    let sequence = u64::from_be_bytes(
        bytes[16..24]
            .try_into()
            .map_err(|_| Error::Authority("invalid head sequence".to_owned()))?,
    );
    let transaction_id =
        Uuid::from_slice(&bytes[24..40]).map_err(|error| Error::Authority(error.to_string()))?;
    let payload_hash = bytes[40..72]
        .try_into()
        .map_err(|_| Error::Authority("invalid payload hash".to_owned()))?;
    let token_hash = bytes[72..104]
        .try_into()
        .map_err(|_| Error::Authority("invalid lease token hash".to_owned()))?;
    Ok(DecodedHead {
        generation,
        sequence,
        transaction_id,
        payload_hash,
        token_hash,
    })
}

/// Removes an immutable object only after a failed precondition proves it was not committed.
async fn discard_uncommitted_transaction(
    store: &dyn ObjectStore,
    transaction_path: &Path,
    head_path: &Path,
    sequence: u64,
    transaction_id: Uuid,
) {
    let committed = match store.get(head_path).await {
        Ok(result) => match result.bytes().await {
            Ok(bytes) => match decode_head(&bytes) {
                Ok(head) => head.sequence == sequence && head.transaction_id == transaction_id,
                Err(_) => false,
            },
            Err(_) => false,
        },
        Err(_) => false,
    };
    if !committed {
        let _ = store.delete(transaction_path).await;
    }
}

/// Reads one just-written object back and verifies exact byte identity.
async fn verify_object(store: &dyn ObjectStore, path: &Path, expected: &[u8]) -> Result<()> {
    let actual = store
        .get(path)
        .await
        .map_err(|error| Error::Authority(error.to_string()))?
        .bytes()
        .await
        .map_err(|error| Error::Authority(error.to_string()))?;
    if actual.as_ref() != expected {
        return Err(Error::Authority(format!(
            "object verification mismatch for {path}"
        )));
    }
    Ok(())
}
