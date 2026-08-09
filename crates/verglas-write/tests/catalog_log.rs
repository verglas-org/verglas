//! Quorum catalog-log contract tests for #82. They exercise durable EC append,
//! idempotency, monotonic ordering, minority-loss recovery, and stale-read refusal.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use verglas_cluster::fragments::{FragmentIoError, FragmentKey, FragmentRecord, LoadedFragment};
use verglas_core::node::NodeId;
use verglas_write::catalog_log::{CatalogLogError, CatalogMutation, EcCatalogLog};
use verglas_write::{FragmentTransport, LiveMembership, TransportError};

type StoredFragments = HashMap<(String, String, usize), (Bytes, u32)>;

#[derive(Default)]
struct MemoryTransport {
    fragments: Mutex<StoredFragments>,
    failed: Mutex<HashSet<String>>,
    failed_tail: Mutex<HashSet<String>>,
}

impl MemoryTransport {
    /// Marks one node unavailable for every subsequent transport operation.
    fn fail(&self, node: &str) {
        self.failed.lock().expect("failed lock").insert(node.into());
    }

    /// True when a node is unavailable.
    fn is_failed(&self, node: &NodeId) -> bool {
        self.failed
            .lock()
            .expect("failed lock")
            .contains(node.as_str())
    }

    /// Fails only publication of the replicated committed-tail marker.
    fn fail_tail(&self, node: &str) {
        self.failed_tail
            .lock()
            .expect("failed tail lock")
            .insert(node.into());
    }
}

#[async_trait::async_trait]
impl FragmentTransport for MemoryTransport {
    /// Failed nodes report no headroom; live nodes accept these small records.
    async fn has_headroom(&self, node: &NodeId, _bytes: u64) -> bool {
        !self.is_failed(node)
    }

    /// Persists one in-memory fragment unless the target node is failed.
    async fn place(&self, node: &NodeId, record: FragmentRecord) -> Result<(), TransportError> {
        if self.is_failed(node) {
            return Err(TransportError::Local(FragmentIoError::Io("failed".into())));
        }
        if record.key.object_id.ends_with("/tail")
            && self
                .failed_tail
                .lock()
                .expect("failed tail lock")
                .contains(node.as_str())
        {
            return Err(TransportError::Local(FragmentIoError::Io(
                "tail failed".into(),
            )));
        }
        self.fragments.lock().expect("fragment lock").insert(
            (node.as_str().into(), record.key.object_id, record.key.index),
            (record.bytes, record.checksum),
        );
        Ok(())
    }

    /// Catalog records are small and use whole-fragment placement.
    async fn place_stream(
        &self,
        _node: &NodeId,
        _key: FragmentKey,
        _shards: verglas_write::transport::ShardStream,
    ) -> Result<(), TransportError> {
        Err(TransportError::Local(FragmentIoError::Io(
            "unexpected streaming placement".into(),
        )))
    }

    /// Loads a healthy fragment from a live node.
    async fn load(
        &self,
        node: &NodeId,
        key: &FragmentKey,
    ) -> Result<Option<LoadedFragment>, TransportError> {
        if self.is_failed(node) {
            return Err(TransportError::Local(FragmentIoError::Io("failed".into())));
        }
        Ok(self
            .fragments
            .lock()
            .expect("fragment lock")
            .get(&(node.as_str().into(), key.object_id.clone(), key.index))
            .map(|(bytes, checksum)| LoadedFragment {
                bytes: bytes.clone(),
                checksum: *checksum,
            }))
    }

    /// Removes one in-memory fragment.
    async fn delete(&self, node: &NodeId, key: &FragmentKey) -> Result<(), TransportError> {
        self.fragments.lock().expect("fragment lock").remove(&(
            node.as_str().into(),
            key.object_id.clone(),
            key.index,
        ));
        Ok(())
    }
}

struct StaticMembership {
    nodes: Vec<NodeId>,
}

impl StaticMembership {
    /// Builds the fixed three-node membership used by the catalog ring.
    fn three() -> Arc<Self> {
        Arc::new(Self {
            nodes: ["box-01", "box-02", "box-03"]
                .into_iter()
                .map(NodeId::new)
                .collect(),
        })
    }
}

impl LiveMembership for StaticMembership {
    /// The test ingress is box-01.
    fn self_id(&self) -> NodeId {
        self.nodes[0].clone()
    }

    /// Returns all configured catalog-ring members.
    fn live_nodes(&self) -> Vec<NodeId> {
        self.nodes.clone()
    }

    /// Static membership has one fixed epoch.
    fn epoch(&self) -> u64 {
        1
    }
}

fn mutation(sequence: u64, event_id: &str, table: &str) -> CatalogMutation {
    CatalogMutation {
        sequence,
        event_id: event_id.into(),
        warehouse_id: "warehouse-1".into(),
        table_id: table.into(),
        namespace: vec!["analytics".into()],
        table: table.into(),
        previous_namespace: None,
        previous_table: None,
        operation: "updateTable".into(),
        metadata_location: Some(format!("s3://warehouse/{table}/v{sequence}.metadata.json")),
        snapshot_id: Some(sequence as i64),
    }
}

fn log(transport: Arc<MemoryTransport>) -> EcCatalogLog {
    EcCatalogLog::new("tenant-1", transport, StaticMembership::three())
}

#[tokio::test]
async fn append_is_ec_durable_and_recovers_after_one_node_loss() {
    let transport = Arc::new(MemoryTransport::default());
    let log = log(transport.clone());
    let event = mutation(41, "event-41", "orders");

    let ack = log.append(event.clone()).await.expect("quorum append");
    assert_eq!(ack.sequence, 41);
    assert_eq!(ack.event_id, "event-41");

    transport.fail("box-03");
    assert_eq!(
        log.committed_sequence().await.expect("surviving quorum"),
        41
    );
    assert_eq!(log.read_after(0).await.expect("reconstruct"), vec![event]);
}

#[tokio::test]
async fn append_refuses_to_ack_without_the_write_quorum() {
    let transport = Arc::new(MemoryTransport::default());
    transport.fail("box-03");
    let error = log(transport)
        .append(mutation(1, "event-1", "orders"))
        .await
        .expect_err("no sub-quorum ack");
    assert!(matches!(error, CatalogLogError::QuorumUnavailable { .. }));
}

#[tokio::test]
async fn duplicate_is_idempotent_and_stale_sequence_is_rejected() {
    let transport = Arc::new(MemoryTransport::default());
    let log = log(transport);
    let event = mutation(7, "event-7", "orders");
    let first = log.append(event.clone()).await.expect("first append");
    let duplicate = log.append(event).await.expect("duplicate append");
    assert_eq!(duplicate, first);

    let error = log
        .append(mutation(6, "different-event", "customers"))
        .await
        .expect_err("stale append");
    assert!(matches!(error, CatalogLogError::StaleSequence { .. }));
}

#[tokio::test]
async fn isolated_reader_cannot_claim_a_current_watermark() {
    let transport = Arc::new(MemoryTransport::default());
    let log = log(transport.clone());
    log.append(mutation(1, "event-1", "orders"))
        .await
        .expect("append");
    transport.fail("box-02");
    transport.fail("box-03");

    let error = log
        .committed_sequence()
        .await
        .expect_err("one node cannot prove the committed tail");
    assert!(matches!(error, CatalogLogError::QuorumUnavailable { .. }));
}

#[tokio::test]
async fn retry_recovers_a_commit_after_the_response_path_loses_one_tail_copy() {
    let transport = Arc::new(MemoryTransport::default());
    transport.fail_tail("box-03");
    let log = log(transport);
    let event = mutation(9, "event-9", "orders");
    assert!(log.append(event.clone()).await.is_err());

    let retry = log
        .append(event.clone())
        .await
        .expect("quorum-visible retry");
    assert_eq!(retry.sequence, 9);
    assert_eq!(log.read_after(0).await.expect("replay"), vec![event]);
}
