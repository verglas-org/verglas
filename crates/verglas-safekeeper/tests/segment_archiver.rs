//! Background segment archive tests that enforce checkpoint-before-release ordering.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use verglas_safekeeper::{
    ArchiveError, ArchiveObject, ArchiveTimeline, ImmutableSegmentStore, SegmentArchiver,
    SegmentRelease,
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
}

#[tokio::test]
async fn failed_upload_never_commits_a_checkpoint_or_releases_wal() {
    let timeline = Arc::new(Timeline::default());
    let store = Arc::new(Store {
        fail_upload: true,
        writes: Mutex::new(Vec::new()),
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
