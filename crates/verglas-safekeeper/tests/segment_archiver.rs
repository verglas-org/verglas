//! Background segment archive tests that enforce checkpoint-before-release ordering.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;
use verglas_consensus::WalArchiveSegment;
use verglas_safekeeper::{
    ArchiveError, ArchiveObject, ArchiveTimeline, ImmutableSegmentStore, SegmentArchiver,
    SegmentRelease, read_archived_wal_prefix,
};

#[derive(Default)]
struct Timeline {
    checkpoints: Mutex<Vec<(u128, u64, String)>>,
}

#[async_trait]
impl ArchiveTimeline for Timeline {
    async fn read_committed_wal(
        &self,
        from: u64,
        to: u64,
        minimum_index: u64,
    ) -> Result<Bytes, ArchiveError> {
        assert_eq!((from, to, minimum_index), (0, 4, 7));
        Ok(Bytes::from_static(b"WAL!"))
    }

    async fn commit_archive_checkpoint(
        &self,
        request: u128,
        end_lsn: u64,
        object: &ArchiveObject,
    ) -> Result<(), ArchiveError> {
        self.checkpoints.lock().expect("checkpoint lock").push((
            request,
            end_lsn,
            object.key.clone(),
        ));
        Ok(())
    }

    async fn release_through(&self, end_lsn: u64) -> Result<SegmentRelease, ArchiveError> {
        Ok(SegmentRelease::checkpointed(end_lsn))
    }
}

struct Store {
    fail_upload: bool,
    writes: Mutex<Vec<ArchiveObject>>,
    objects: Mutex<BTreeMap<String, Bytes>>,
}

#[async_trait]
impl ImmutableSegmentStore for Store {
    async fn upload(&self, object: &ArchiveObject, bytes: Bytes) -> Result<(), ArchiveError> {
        assert_eq!(bytes, Bytes::from_static(b"WAL!"));
        if self.fail_upload {
            return Err(ArchiveError::ObjectStore("unavailable".to_owned()));
        }
        self.writes
            .lock()
            .expect("writes lock")
            .push(object.clone());
        self.objects
            .lock()
            .expect("objects lock")
            .insert(object.key.clone(), bytes);
        Ok(())
    }

    async fn verify(&self, object: &ArchiveObject) -> Result<(), ArchiveError> {
        self.writes
            .lock()
            .expect("writes lock")
            .contains(object)
            .then_some(())
            .ok_or_else(|| ArchiveError::ObjectStore("missing object".to_owned()))
    }

    async fn read(&self, object: &ArchiveObject) -> Result<Bytes, ArchiveError> {
        self.objects
            .lock()
            .expect("objects lock")
            .get(&object.key)
            .cloned()
            .ok_or_else(|| ArchiveError::ObjectStore("missing object".to_owned()))
    }
}

/// Creates the exact immutable identity committed for one small test WAL segment.
fn archived_segment(start_lsn: u64, bytes: &[u8]) -> (WalArchiveSegment, ArchiveObject) {
    use sha2::{Digest, Sha256};

    let end_lsn = start_lsn + bytes.len() as u64;
    let hash: [u8; 32] = Sha256::digest(bytes).into();
    let key = format!(
        "timeline/a/{start_lsn:016x}-{end_lsn:016x}-{}",
        hash.iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    let segment = WalArchiveSegment::from_key(key).expect("canonical archive object key");
    let object = ArchiveObject::from_segment(&segment).expect("archive object identity");
    (segment, object)
}

#[tokio::test]
async fn failed_upload_never_commits_a_checkpoint_or_releases_wal() {
    let timeline = Arc::new(Timeline::default());
    let store = Arc::new(Store {
        fail_upload: true,
        writes: Mutex::new(Vec::new()),
        objects: Mutex::new(BTreeMap::new()),
    });
    let archiver =
        SegmentArchiver::new(timeline.clone(), store, "timeline/a").expect("archive configuration");

    assert!(archiver.archive(55, 0, 4, 7).await.is_err());
    assert!(
        timeline
            .checkpoints
            .lock()
            .expect("checkpoint lock")
            .is_empty()
    );
}

#[tokio::test]
async fn verified_upload_commits_checkpoint_before_exposing_release() {
    let timeline = Arc::new(Timeline::default());
    let store = Arc::new(Store {
        fail_upload: false,
        writes: Mutex::new(Vec::new()),
        objects: Mutex::new(BTreeMap::new()),
    });
    let archiver = SegmentArchiver::new(timeline.clone(), store.clone(), "timeline/a")
        .expect("archive configuration");

    let release = archiver
        .archive_tail(56, 0, 4, 7)
        .await
        .expect("archive partial tail");
    assert_eq!(release.through_lsn(), 4);
    assert_eq!(
        timeline.checkpoints.lock().expect("checkpoint lock").len(),
        1
    );
    assert_eq!(store.writes.lock().expect("writes lock").len(), 1);
}

#[tokio::test]
async fn archived_prefix_reads_multiple_verified_segments_and_rejects_missing_or_corrupt_objects() {
    let store = Arc::new(Store {
        fail_upload: false,
        writes: Mutex::new(Vec::new()),
        objects: Mutex::new(BTreeMap::new()),
    });
    let (first, first_object) = archived_segment(0, b"arch");
    let (second, second_object) = archived_segment(4, b"ived");
    store.objects.lock().expect("objects lock").extend([
        (first_object.key.clone(), Bytes::from_static(b"arch")),
        (second_object.key.clone(), Bytes::from_static(b"ived")),
    ]);

    assert_eq!(
        read_archived_wal_prefix(store.as_ref(), &[first.clone(), second.clone()], 0, 8)
            .await
            .expect("read both contiguous archived segments"),
        Bytes::from_static(b"archived")
    );
    assert_eq!(
        read_archived_wal_prefix(store.as_ref(), &[first.clone(), second.clone()], 2, 6)
            .await
            .expect("read a subrange spanning archived objects"),
        Bytes::from_static(b"chiv")
    );

    store
        .objects
        .lock()
        .expect("objects lock")
        .remove(&first_object.key);
    assert!(
        read_archived_wal_prefix(store.as_ref(), &[first.clone(), second.clone()], 0, 8)
            .await
            .is_err()
    );

    store
        .objects
        .lock()
        .expect("objects lock")
        .insert(first_object.key.clone(), Bytes::from_static(b"arch"));
    store
        .objects
        .lock()
        .expect("objects lock")
        .insert(second_object.key, Bytes::from_static(b"rupt"));
    assert!(
        read_archived_wal_prefix(store.as_ref(), &[first, second], 0, 8)
            .await
            .is_err()
    );
}
