//! Fragment placement transport for the write-back coordinator (#180).
//!
//! The coordinator does not care whether a fragment lands on the local node's
//! NVMe or a peer's — it just places, loads, and deletes fragments by node id.
//! [`PeerFragmentTransport`] routes self-directed ops to the local store and
//! everything else over the peer fragment RPC. The trait is the seam the tests
//! replace with an in-memory fake to drive quorum, fallback, and repair without
//! a network.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use verglas_cluster::fragments::{FragmentIoError, FragmentKey, FragmentRecord, LoadedFragment};
use verglas_cluster::{FragmentClient, FragmentRpcError, LocalFragmentStore};
use verglas_core::node::NodeId;

/// A stream of a single fragment's stripe shards, in stripe order. Each item is
/// one stripe's chunk for that fragment; concatenated they are the fragment.
pub type ShardStream = BoxStream<'static, Bytes>;

/// Counts completed durable append receipts for one batch. Both the object
/// writer and WAL keeper use it, so RAM delivery cannot be mistaken for a
/// durable quorum acknowledgement.
pub struct DurableQuorum<T> {
    /// Required distinct durable watermarks.
    required: usize,
    /// Successful fdatasync-backed receipts seen so far.
    receipts: Vec<T>,
}

impl<T> DurableQuorum<T> {
    /// Starts collecting the required durable receipts for an append batch.
    pub fn new(required: usize) -> Self {
        Self {
            required,
            receipts: Vec::with_capacity(required),
        }
    }

    /// Records one completed append, ignoring failures, and reports quorum.
    pub fn record<E>(&mut self, result: Result<T, E>) -> bool {
        if let Ok(receipt) = result {
            self.receipts.push(receipt);
        }
        self.reached()
    }

    /// Returns whether this batch has its configured durable quorum.
    pub fn reached(&self) -> bool {
        self.receipts.len() >= self.required
    }

    /// Returns the number of durable receipts observed before the deadline.
    pub fn len(&self) -> usize {
        self.receipts.len()
    }

    /// Returns whether no durable receipt has completed for this batch yet.
    pub fn is_empty(&self) -> bool {
        self.receipts.is_empty()
    }

    /// Consumes the collector and yields exactly its durable receipts.
    pub fn into_receipts(self) -> Vec<T> {
        self.receipts
    }
}

/// A fragment transport failure. Any error here counts against quorum; it never
/// silently succeeds.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// A local fragment store IO error.
    #[error(transparent)]
    Local(#[from] FragmentIoError),
    /// A peer fragment RPC error.
    #[error(transparent)]
    Rpc(#[from] FragmentRpcError),
}

/// Places, loads, and deletes fragments on named nodes.
#[async_trait]
pub trait FragmentTransport: Send + Sync + 'static {
    /// True when `node`'s fragment store can still hold `bytes` under its hard
    /// budget. Placement consults this to exclude a full node up front, so the
    /// fragment ceiling holds without relying on a placement to fail. A peer we
    /// cannot ask (unreachable, or advertising no free-bytes figure) is treated
    /// as having no headroom — conservative, so the ceiling is never crossed by
    /// guessing yes.
    async fn has_headroom(&self, node: &NodeId, bytes: u64) -> bool;

    /// Places `record` on `node` durably. `Ok` means the fragment is on that
    /// node's NVMe.
    async fn place(&self, node: &NodeId, record: FragmentRecord) -> Result<(), TransportError>;

    /// Places one fragment on `node` by streaming its shards. `shards` yields
    /// the fragment's stripe chunks in order; the fragment becomes durable only
    /// after the whole stream is consumed and fsynced, so `Ok` means the whole
    /// fragment reached that node's NVMe. This is the streaming placement the
    /// coordinator uses so a large object never buffers a whole fragment — one
    /// stripe is resident at a time. A budget refusal mid-stream is an error the
    /// coordinator counts against quorum, exactly like `place`.
    async fn place_stream(
        &self,
        node: &NodeId,
        key: FragmentKey,
        shards: ShardStream,
    ) -> Result<(), TransportError>;

    /// Loads the fragment `key` from `node`; `None` if that node lacks it. The
    /// loaded fragment carries its checksum so the coordinator verifies it before
    /// reassembly (#220).
    async fn load(
        &self,
        node: &NodeId,
        key: &FragmentKey,
    ) -> Result<Option<LoadedFragment>, TransportError>;

    /// Deletes the fragment `key` from `node` (idempotent).
    async fn delete(&self, node: &NodeId, key: &FragmentKey) -> Result<(), TransportError>;

    /// Lists durable record keys with `prefix` on `node`. Recovery uses this
    /// private peer operation to discover immutable transaction records; an
    /// implementation that cannot enumerate returns an empty list, never a
    /// guessed key range.
    async fn list_prefix(
        &self,
        _node: &NodeId,
        _prefix: &str,
    ) -> Result<Vec<FragmentKey>, TransportError> {
        Ok(Vec::new())
    }
}

/// The production transport: local store for this node, peer RPC for the rest.
pub struct PeerFragmentTransport {
    /// This node's id, so self-directed ops skip the network.
    self_id: NodeId,
    /// The local fragment store.
    local: LocalFragmentStore,
    /// The peer fragment RPC client.
    client: FragmentClient,
}

impl PeerFragmentTransport {
    /// Builds the transport for `self_id` over `local` and `client`.
    pub fn new(self_id: NodeId, local: LocalFragmentStore, client: FragmentClient) -> Self {
        Self {
            self_id,
            local,
            client,
        }
    }
}

#[async_trait]
impl FragmentTransport for PeerFragmentTransport {
    /// Checks headroom against the local store when `node` is self, otherwise
    /// asks the peer over the fragment RPC. An unreachable peer answers `false`
    /// so a node we cannot ask is never assumed to have room (the ceiling is
    /// never crossed by guessing yes).
    async fn has_headroom(&self, node: &NodeId, bytes: u64) -> bool {
        if *node == self.self_id {
            self.local.has_headroom(bytes)
        } else {
            self.client.headroom(node, bytes).await.unwrap_or(false)
        }
    }

    /// Stores locally when `node` is self, otherwise over peer RPC.
    async fn place(&self, node: &NodeId, record: FragmentRecord) -> Result<(), TransportError> {
        if *node == self.self_id {
            self.local.append_batch(&[record])?;
            Ok(())
        } else {
            self.client.put_fragment(node, record).await?;
            Ok(())
        }
    }

    /// Streams a fragment's shards to `node`: to the local store when self,
    /// otherwise over the streaming peer RPC.
    async fn place_stream(
        &self,
        node: &NodeId,
        key: FragmentKey,
        mut shards: ShardStream,
    ) -> Result<(), TransportError> {
        use futures::StreamExt;
        if *node == self.self_id {
            let mut writer = self.local.open_fragment(&key)?;
            while let Some(shard) = shards.next().await {
                writer.append(&shard)?;
            }
            writer.commit()?;
            Ok(())
        } else {
            self.client.put_fragment_stream(node, key, shards).await?;
            Ok(())
        }
    }

    /// Loads locally when `node` is self, otherwise over peer RPC.
    async fn load(
        &self,
        node: &NodeId,
        key: &FragmentKey,
    ) -> Result<Option<LoadedFragment>, TransportError> {
        if *node == self.self_id {
            Ok(self.local.load_fragment(key)?)
        } else {
            Ok(self.client.get_fragment(node, key).await?)
        }
    }

    /// Deletes locally when `node` is self, otherwise over peer RPC.
    async fn delete(&self, node: &NodeId, key: &FragmentKey) -> Result<(), TransportError> {
        if *node == self.self_id {
            self.local.delete_fragment(key)?;
            Ok(())
        } else {
            self.client.delete_fragment(node, key).await?;
            Ok(())
        }
    }

    /// Lists local segment-index keys or asks the private peer list RPC.
    async fn list_prefix(
        &self,
        node: &NodeId,
        prefix: &str,
    ) -> Result<Vec<FragmentKey>, TransportError> {
        if *node == self.self_id {
            Ok(self
                .local
                .list_fragment_keys()
                .into_iter()
                .filter(|key| key.object_id.starts_with(prefix))
                .collect())
        } else {
            Ok(self.client.list_fragments(node, prefix).await?)
        }
    }
}
