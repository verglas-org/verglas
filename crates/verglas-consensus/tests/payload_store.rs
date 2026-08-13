//! Durable payload staging and reconstruction acceptance tests.

use bytes::Bytes;
use tempfile::TempDir;
use verglas_consensus::{
    FilePayloadReplica, PayloadSet, PayloadStore, ReconstructRequest, ReplicationMode, RequestId,
    SealRequest,
};

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
