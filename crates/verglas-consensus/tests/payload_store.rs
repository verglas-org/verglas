//! Durable payload staging and reconstruction acceptance tests.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use tempfile::TempDir;
use verglas_consensus::{
    DistributedPayloadStore, FilePayloadReplica, PayloadError, PayloadRepresentation, PayloadSet,
    PayloadStore, ReconstructRequest, ReplicationMode, RepresentationTransport, RequestId,
    SealRequest,
};

/// Transport whose first voter is unavailable while every later committed voter fsyncs writes.
struct FirstVoterUnavailable {
    /// Voters that reached durable storage, in attempt order.
    stored: Mutex<Vec<u64>>,
}

impl FirstVoterUnavailable {
    /// Builds the controlled transport.
    fn new() -> Arc<Self> {
        Arc::new(Self {
            stored: Mutex::new(Vec::new()),
        })
    }

    /// Returns the voters that accepted a durable representation.
    fn stored(&self) -> Vec<u64> {
        self.stored.lock().expect("stored voters lock").clone()
    }
}

#[async_trait::async_trait]
impl RepresentationTransport for FirstVoterUnavailable {
    /// Refuses voter one and durably accepts every other committed voter.
    async fn store(
        &self,
        voter: u64,
        _representation: PayloadRepresentation,
    ) -> Result<(), PayloadError> {
        if voter == 1 {
            return Err(PayloadError::Transport(
                "voter one is unavailable".to_owned(),
            ));
        }
        self.stored.lock().expect("stored voters lock").push(voter);
        Ok(())
    }

    /// This staging-only test never reconstructs a representation.
    async fn load(
        &self,
        _voter: u64,
        _hash: [u8; 32],
        _group: &str,
        _configuration_generation: u64,
        _request: RequestId,
    ) -> Result<Option<PayloadRepresentation>, PayloadError> {
        Ok(None)
    }

    /// This staging-only test never reclaims a representation.
    async fn delete(
        &self,
        _voter: u64,
        _hash: [u8; 32],
        _group: &str,
        _configuration_generation: u64,
        _request: RequestId,
        _slot: usize,
    ) -> Result<(), PayloadError> {
        Ok(())
    }
}

/// Transport that retains successful stores while selected voters are unreachable.
struct SelectivelyUnavailableTransport {
    /// Persisted representations indexed by their configured voter.
    records: Mutex<BTreeMap<u64, PayloadRepresentation>>,
    /// Voters that currently fail peer operations.
    unavailable: Mutex<BTreeSet<u64>>,
}

impl SelectivelyUnavailableTransport {
    /// Builds an initially reachable in-memory peer transport.
    fn new() -> Arc<Self> {
        Arc::new(Self {
            records: Mutex::new(BTreeMap::new()),
            unavailable: Mutex::new(BTreeSet::new()),
        })
    }

    /// Makes one holder unreachable after its representation has been sealed.
    fn make_unavailable(&self, voter: u64) {
        self.unavailable
            .lock()
            .expect("unavailable voters lock")
            .insert(voter);
    }

    /// Rewrites one stored seal to exercise reconstruction identity validation.
    fn corrupt_term(&self, voter: u64) {
        let mut records = self.records.lock().expect("stored representations lock");
        let record = records
            .get_mut(&voter)
            .expect("stored representation for committed voter");
        record.term += 1;
    }

    /// Returns whether a peer operation can currently reach this voter.
    fn reachable(&self, voter: u64) -> bool {
        !self
            .unavailable
            .lock()
            .expect("unavailable voters lock")
            .contains(&voter)
    }
}

#[async_trait::async_trait]
impl RepresentationTransport for SelectivelyUnavailableTransport {
    /// Stores a representation while the voter remains reachable.
    async fn store(
        &self,
        voter: u64,
        representation: PayloadRepresentation,
    ) -> Result<(), PayloadError> {
        if !self.reachable(voter) {
            return Err(PayloadError::Transport("voter is unavailable".to_owned()));
        }
        self.records
            .lock()
            .expect("stored representations lock")
            .insert(voter, representation);
        Ok(())
    }

    /// Loads the requested representation while the voter remains reachable.
    async fn load(
        &self,
        voter: u64,
        hash: [u8; 32],
        group: &str,
        configuration_generation: u64,
        request: RequestId,
    ) -> Result<Option<PayloadRepresentation>, PayloadError> {
        if !self.reachable(voter) {
            return Err(PayloadError::Transport("voter is unavailable".to_owned()));
        }
        Ok(self
            .records
            .lock()
            .expect("stored representations lock")
            .get(&voter)
            .filter(|record| {
                record.hash == hash
                    && record.group == group
                    && record.configuration_generation == configuration_generation
                    && record.request == request
            })
            .cloned())
    }

    /// Deletes a matching representation while the voter remains reachable.
    async fn delete(
        &self,
        voter: u64,
        hash: [u8; 32],
        group: &str,
        configuration_generation: u64,
        request: RequestId,
        slot: usize,
    ) -> Result<(), PayloadError> {
        if !self.reachable(voter) {
            return Err(PayloadError::Transport("voter is unavailable".to_owned()));
        }
        let mut records = self.records.lock().expect("stored representations lock");
        if records.get(&voter).is_some_and(|record| {
            record.hash == hash
                && record.group == group
                && record.configuration_generation == configuration_generation
                && record.request == request
                && record.slot == slot
        }) {
            records.remove(&voter);
        }
        Ok(())
    }
}

#[tokio::test]
async fn staging_uses_reachable_committed_voters_after_a_prefix_voter_fails()
-> Result<(), Box<dyn std::error::Error>> {
    let transport = FirstVoterUnavailable::new();
    let payloads = DistributedPayloadStore::new(2, 2, vec![1, 2, 3, 4], transport.clone())?;

    let coded = payloads
        .stage(
            RequestId::from_u128(91),
            "timeline/available",
            7,
            ReplicationMode::Coded,
            b"coded payload",
            &[1, 2, 3],
        )
        .await?;
    assert_eq!(coded.certificate().voters(), &[1, 2, 3, 4]);
    assert_eq!(coded.certificate().holders(), &[2, 3, 4]);

    let complete = payloads
        .stage(
            RequestId::from_u128(92),
            "timeline/available",
            7,
            ReplicationMode::Complete,
            b"complete payload",
            &[1, 2, 3],
        )
        .await?;
    assert_eq!(complete.certificate().voters(), &[1, 2, 3, 4]);
    assert_eq!(complete.certificate().holders(), &[2, 3, 4]);
    assert_eq!(transport.stored(), vec![2, 3, 4, 2, 3, 4]);
    Ok(())
}

#[tokio::test]
async fn distributed_reconstruction_uses_remaining_certified_holders_when_one_is_unavailable()
-> Result<(), Box<dyn std::error::Error>> {
    for (mode, body) in [
        (
            ReplicationMode::Coded,
            Bytes::from_static(b"coded quorum payload"),
        ),
        (
            ReplicationMode::Complete,
            Bytes::from_static(b"complete quorum payload"),
        ),
    ] {
        let transport = SelectivelyUnavailableTransport::new();
        let payloads = DistributedPayloadStore::new(2, 2, vec![1, 2, 3, 4], transport.clone())?;
        let request = RequestId::from_u128(match mode {
            ReplicationMode::Coded => 93,
            ReplicationMode::Complete => 94,
        });
        let staged = payloads
            .stage(request, "timeline/quorum", 7, mode, &body, &[1, 2, 3, 4])
            .await?;
        payloads
            .seal(SealRequest {
                hash: staged.hash(),
                group: "timeline/quorum",
                configuration_generation: 7,
                request,
                term: 3,
                index: 11,
                certificate: staged.certificate(),
            })
            .await?;

        transport.make_unavailable(staged.certificate().holders()[0]);
        payloads
            .seal(SealRequest {
                hash: staged.hash(),
                group: "timeline/quorum",
                configuration_generation: 7,
                request,
                term: 3,
                index: 11,
                certificate: staged.certificate(),
            })
            .await?;

        assert_eq!(
            payloads
                .reconstruct(ReconstructRequest {
                    hash: staged.hash(),
                    group: "timeline/quorum",
                    configuration_generation: 7,
                    request,
                    length: body.len() as u64,
                    term: 3,
                    index: 11,
                    certificate: staged.certificate(),
                })
                .await?,
            body
        );
    }
    Ok(())
}

#[tokio::test]
async fn distributed_reconstruction_rejects_a_returned_mismatched_representation()
-> Result<(), Box<dyn std::error::Error>> {
    let transport = SelectivelyUnavailableTransport::new();
    let payloads = DistributedPayloadStore::new(2, 2, vec![1, 2, 3, 4], transport.clone())?;
    let request = RequestId::from_u128(95);
    let body = Bytes::from_static(b"identity validation payload");
    let staged = payloads
        .stage(
            request,
            "timeline/identity",
            7,
            ReplicationMode::Coded,
            &body,
            &[1, 2, 3, 4],
        )
        .await?;
    payloads
        .seal(SealRequest {
            hash: staged.hash(),
            group: "timeline/identity",
            configuration_generation: 7,
            request,
            term: 3,
            index: 12,
            certificate: staged.certificate(),
        })
        .await?;
    transport.corrupt_term(staged.certificate().holders()[0]);

    assert!(matches!(
        payloads
            .reconstruct(ReconstructRequest {
                hash: staged.hash(),
                group: "timeline/identity",
                configuration_generation: 7,
                request,
                length: body.len() as u64,
                term: 3,
                index: 12,
                certificate: staged.certificate(),
            })
            .await,
        Err(PayloadError::CorruptRepresentation)
    ));
    Ok(())
}

#[tokio::test]
async fn coded_payload_is_fsynced_sealed_and_reconstructs() -> Result<(), Box<dyn std::error::Error>>
{
    let root = TempDir::new()?;
    let replicas = (1..=5)
        .map(|node| FilePayloadReplica::open(node, root.path().join(node.to_string())))
        .collect::<Result<Vec<_>, _>>()?;
    let payloads = PayloadSet::new(3, 2, replicas)?;
    let request = RequestId::from_u128(41);
    let body = Bytes::from_static(b"catalog transaction large body");

    let staged = payloads.stage_local(
        request,
        "warehouse/test",
        1,
        ReplicationMode::Coded,
        &body,
        &[1, 2, 3, 4, 5],
    )?;

    assert_eq!(staged.certificate().holders(), &[1, 2, 3, 4, 5]);
    payloads
        .seal(SealRequest {
            hash: staged.hash(),
            group: "warehouse/test",
            configuration_generation: 1,
            request,
            term: 7,
            index: 19,
            certificate: staged.certificate(),
        })
        .await?;
    assert_eq!(
        payloads
            .reconstruct(ReconstructRequest {
                hash: staged.hash(),
                group: "warehouse/test",
                configuration_generation: 1,
                request,
                length: body.len() as u64,
                term: 7,
                index: 19,
                certificate: staged.certificate(),
            })
            .await?,
        body
    );
    Ok(())
}

#[test]
fn insufficient_staging_never_produces_a_commit_certificate()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempDir::new()?;
    let replicas = (1..=5)
        .map(|node| FilePayloadReplica::open(node, root.path().join(node.to_string())))
        .collect::<Result<Vec<_>, _>>()?;
    let payloads = PayloadSet::new(3, 2, replicas)?;

    let error = payloads
        .stage_local(
            RequestId::from_u128(42),
            "warehouse/test",
            1,
            ReplicationMode::Coded,
            &Bytes::from_static(b"not enough durable fragments"),
            &[1, 2, 3, 4],
        )
        .expect_err("four holders cannot intersect every five-node majority in three shards");

    assert_eq!(error.required(), 5);
    Ok(())
}

#[tokio::test]
async fn complete_fallback_reconstructs_from_one_surviving_majority_replica()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempDir::new()?;
    let replicas = (1..=5)
        .map(|node| FilePayloadReplica::open(node, root.path().join(node.to_string())))
        .collect::<Result<Vec<_>, _>>()?;
    let payloads = PayloadSet::new(3, 2, replicas)?;
    let body = Bytes::from_static(b"complete fallback");
    let staged = payloads.stage_local(
        RequestId::from_u128(43),
        "warehouse/test",
        1,
        ReplicationMode::Complete,
        &body,
        &[1, 2, 3],
    )?;

    payloads
        .seal(SealRequest {
            hash: staged.hash(),
            group: "warehouse/test",
            configuration_generation: 1,
            request: RequestId::from_u128(43),
            term: 8,
            index: 20,
            certificate: staged.certificate(),
        })
        .await?;
    assert_eq!(
        payloads
            .reconstruct(ReconstructRequest {
                hash: staged.hash(),
                group: "warehouse/test",
                configuration_generation: 1,
                request: RequestId::from_u128(43),
                length: body.len() as u64,
                term: 8,
                index: 20,
                certificate: staged.certificate(),
            })
            .await?,
        body
    );
    Ok(())
}

#[tokio::test]
async fn sealed_payload_rejects_wrong_allocation_identity() -> Result<(), Box<dyn std::error::Error>>
{
    let root = TempDir::new()?;
    let replicas = (1..=5)
        .map(|node| FilePayloadReplica::open(node, root.path().join(node.to_string())))
        .collect::<Result<Vec<_>, _>>()?;
    let payloads = PayloadSet::new(3, 2, replicas)?;
    let request = RequestId::from_u128(44);
    let body = Bytes::from_static(b"allocation-bound body");
    let staged = payloads.stage_local(
        request,
        "warehouse/test",
        9,
        ReplicationMode::Coded,
        &body,
        &[1, 2, 3, 4, 5],
    )?;
    payloads
        .seal(SealRequest {
            hash: staged.hash(),
            group: "warehouse/test",
            configuration_generation: 9,
            request,
            term: 4,
            index: 12,
            certificate: staged.certificate(),
        })
        .await?;

    assert!(
        payloads
            .reconstruct(ReconstructRequest {
                hash: staged.hash(),
                group: "warehouse/other",
                configuration_generation: 9,
                request,
                length: body.len() as u64,
                term: 4,
                index: 12,
                certificate: staged.certificate(),
            })
            .await
            .is_err()
    );
    assert!(
        payloads
            .reconstruct(ReconstructRequest {
                hash: staged.hash(),
                group: "warehouse/test",
                configuration_generation: 10,
                request,
                length: body.len() as u64,
                term: 4,
                index: 12,
                certificate: staged.certificate(),
            })
            .await
            .is_err()
    );
    assert!(
        payloads
            .reconstruct(ReconstructRequest {
                hash: staged.hash(),
                group: "warehouse/test",
                configuration_generation: 9,
                request,
                length: body.len() as u64,
                term: 5,
                index: 12,
                certificate: staged.certificate(),
            })
            .await
            .is_err()
    );
    Ok(())
}
