//! Wire-level tests for the transport-only Neon client protocol.

use verglas_safekeeper::{WalRequest, WalResponse, WalWireError};

/// Every client operation has one canonical binary representation.
#[test]
fn requests_round_trip_without_consensus_controls() -> Result<(), WalWireError> {
    let requests = [
        WalRequest::OpenTimeline {
            request_id: 7,
            start_lsn: 0x16_b6ff_f000,
        },
        WalRequest::AcquireWriter {
            request_id: u128::MAX - 7,
            writer: "compute-a".to_owned(),
        },
        WalRequest::Append {
            request_id: 42,
            writer_epoch: 9,
            start_lsn: 0x16_b6ff_f000,
            payload: vec![0, 1, 2, 0xff],
        },
        WalRequest::ReadWal {
            from: 10,
            to: 14,
            minimum_index: 81,
        },
        WalRequest::Status { minimum_index: 81 },
        WalRequest::ReleaseWriter {
            request_id: 43,
            writer_epoch: 9,
        },
    ];

    for request in requests {
        let encoded = request.encode()?;
        assert_eq!(WalRequest::decode(&encoded)?, request);
    }
    Ok(())
}

/// Responses carry only consensus-applied facts and committed WAL bytes.
#[test]
fn responses_round_trip() -> Result<(), WalWireError> {
    let responses = [
        WalResponse::Applied {
            index: 101,
            writer_epoch: Some(12),
            wal_end: None,
            archive_lsn: None,
        },
        WalResponse::Applied {
            index: 102,
            writer_epoch: None,
            wal_end: Some(99),
            archive_lsn: Some(88),
        },
        WalResponse::WalBytes {
            payload: vec![9, 8, 7],
        },
        WalResponse::Status {
            index: 103,
            wal_end: 100,
            archive_lsn: 88,
        },
    ];

    for response in responses {
        let encoded = response.encode()?;
        assert_eq!(WalResponse::decode(&encoded)?, response);
    }
    Ok(())
}

/// Truncation, trailing bytes, invalid flags, and unknown operations fail closed.
#[test]
fn malformed_frames_are_rejected() {
    let valid = WalRequest::ReleaseWriter {
        request_id: 1,
        writer_epoch: 2,
    }
    .encode()
    .expect("valid release");
    for end in 0..valid.len() {
        assert!(WalRequest::decode(&valid[..end]).is_err());
    }

    let mut trailing = valid.clone();
    trailing.push(0);
    assert!(WalRequest::decode(&trailing).is_err());
    assert!(WalRequest::decode(&[0xff]).is_err());

    let mut invalid_optional = WalResponse::Applied {
        index: 1,
        writer_epoch: None,
        wal_end: None,
        archive_lsn: None,
    }
    .encode()
    .expect("valid response");
    invalid_optional[9] = 2;
    assert!(WalResponse::decode(&invalid_optional).is_err());
}
