//! Bounded-linger batching for immutable object-header consensus commits.
//!
//! `finish_stream_ack` (`verglas-writeback::coordinator`) acks a write-back
//! PUT only after its immutable header commits through Raft. Committing one
//! object per Raft round trip caps client throughput at
//! `shards / raft_round_trip` (`tests/cluster-local/PERF-OBJECTIVE.md`). This
//! module coalesces many concurrent `StagedObject` commits addressed to the
//! same consensus group into one Raft log entry: a bounded linger window
//! lets a burst land in one flush, and a max-batch size caps how large that
//! one entry gets. The caller still only learns success once its own object
//! is inside a durably committed batch — batching changes how many Raft
//! entries a burst costs, never what the ack promises.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, oneshot};

use verglas_writeback::{ObjectCommit, StagedObject};

/// Submits one already-coalesced batch of staged objects as a single
/// consensus commit. Implemented by the real Raft-backed plane
/// ([`crate::consensus::ObjectHeaderSubmitter`]); faked in tests so the
/// batching policy is verified without a running Raft group.
#[async_trait::async_trait]
pub trait BatchSubmitter: Send + Sync + 'static {
    /// Commits every object in `items` as one atomic unit: either all of them
    /// land at the returned index, or none do. The caller fans the identical
    /// result out to every enqueuing caller.
    async fn submit_batch(
        &self,
        group: &str,
        items: &[StagedObject],
    ) -> Result<ObjectCommit, String>;
}

/// One caller's still-pending commit: the staged object plus the channel its
/// shared batch outcome is delivered on.
struct PendingCommit {
    staged: StagedObject,
    reply: oneshot::Sender<Result<ObjectCommit, String>>,
}

/// One consensus group's accumulating batch. A batch starts empty; its first
/// enqueued item schedules the linger flush that closes it (unless a
/// max-batch flush closes it first).
#[derive(Default)]
struct GroupBatch {
    items: Vec<PendingCommit>,
    /// Bumped on every flush, so a linger timer that fires after another
    /// trigger already drained this batch finds a generation mismatch and
    /// does nothing, instead of submitting a second, empty commit.
    generation: u64,
}

/// Coalesces `StagedObject` commits addressed to the same consensus group
/// into one Raft entry per linger window (or per `max_batch`, whichever
/// closes the batch first).
pub struct ObjectCommitBatcher {
    linger: Duration,
    max_batch: usize,
    submitter: Arc<dyn BatchSubmitter>,
    groups: Arc<Mutex<HashMap<String, GroupBatch>>>,
}

impl ObjectCommitBatcher {
    /// Builds a batcher over `submitter`. `linger` is the hard ceiling on how
    /// long a lone commit waits for company before it flushes alone;
    /// `max_batch` caps how many objects one Raft entry carries. Both are
    /// `cache.writeback.object_commit_*` configuration (`verglas-core`), not
    /// fixed constants, so a deployment can tune the throughput/latency
    /// trade-off without a code change.
    pub fn new(submitter: Arc<dyn BatchSubmitter>, linger: Duration, max_batch: usize) -> Self {
        Self {
            linger,
            max_batch: max_batch.max(1),
            submitter,
            groups: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Enqueues `staged` into `group`'s current batch and resolves only once
    /// that batch — with `staged` inside it — has committed or been
    /// rejected. A commit to one group never waits on another group's batch.
    pub async fn commit(&self, group: String, staged: StagedObject) -> Result<ObjectCommit, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let should_flush_now = {
            let mut groups = self.groups.lock().await;
            let batch = groups.entry(group.clone()).or_default();
            batch.items.push(PendingCommit {
                staged,
                reply: reply_tx,
            });
            if batch.items.len() == 1 {
                self.schedule_linger_flush(group.clone(), batch.generation);
            }
            batch.items.len() >= self.max_batch
        };
        if should_flush_now {
            self.flush_current(&group).await;
        }
        match reply_rx.await {
            Ok(result) => result,
            Err(_) => Err("object commit batch dropped without a reply".to_owned()),
        }
    }

    /// Spawns the bounded wait that closes a fresh batch if nothing else (a
    /// max-batch flush) closes it first. The sleep lives off the hot path: it
    /// runs on its own task, never inside a caller's `commit` await.
    fn schedule_linger_flush(&self, group: String, generation: u64) {
        let groups = Arc::clone(&self.groups);
        let submitter = Arc::clone(&self.submitter);
        let linger = self.linger;
        tokio::spawn(async move {
            tokio::time::sleep(linger).await;
            flush_generation(&groups, submitter.as_ref(), &group, generation).await;
        });
    }

    /// Flushes whatever `group`'s batch currently holds, at its current
    /// generation. Used by the max-batch trigger, which fires from inside
    /// `commit` rather than waiting for the linger timer.
    async fn flush_current(&self, group: &str) {
        let generation = {
            let groups = self.groups.lock().await;
            groups.get(group).map(|batch| batch.generation)
        };
        let Some(generation) = generation else {
            return;
        };
        flush_generation(&self.groups, self.submitter.as_ref(), group, generation).await;
    }
}

/// Takes every pending item for `group` at `generation` and submits them as
/// one commit, replying to every waiter with the shared outcome. A
/// generation mismatch (someone else already flushed this batch) makes this
/// a no-op: the guard against the max-batch trigger and the linger timer
/// racing to flush the same batch twice.
async fn flush_generation(
    groups: &Mutex<HashMap<String, GroupBatch>>,
    submitter: &dyn BatchSubmitter,
    group: &str,
    generation: u64,
) {
    let items = {
        let mut groups = groups.lock().await;
        let Some(batch) = groups.get_mut(group) else {
            return;
        };
        if batch.generation != generation || batch.items.is_empty() {
            return;
        }
        batch.generation = batch.generation.wrapping_add(1);
        std::mem::take(&mut batch.items)
    };
    let (staged, replies): (Vec<StagedObject>, Vec<oneshot::Sender<Result<ObjectCommit, String>>>) =
        items.into_iter().map(|item| (item.staged, item.reply)).unzip();
    let result = submitter.submit_batch(group, &staged).await;
    for reply in replies {
        let _ = reply.send(result.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;
    use verglas_cache::writeback_codec::Geometry;
    use verglas_core::CacheKey;

    /// Records every batch it is asked to commit, and answers with a
    /// configurable outcome, so tests can assert on coalescing without a
    /// running Raft group.
    struct RecordingSubmitter {
        outcome: fn(u64) -> Result<ObjectCommit, String>,
        batches: StdMutex<Vec<Vec<String>>>,
    }

    impl RecordingSubmitter {
        fn ok() -> Self {
            Self {
                outcome: |index| Ok(ObjectCommit { index }),
                batches: StdMutex::new(Vec::new()),
            }
        }

        fn failing() -> Self {
            Self {
                outcome: |_| Err("simulated consensus failure".to_owned()),
                batches: StdMutex::new(Vec::new()),
            }
        }

        fn call_count(&self) -> usize {
            self.batches.lock().unwrap_or_else(|e| e.into_inner()).len()
        }

        fn total_items(&self) -> usize {
            self.batches
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .map(Vec::len)
                .sum()
        }
    }

    #[async_trait::async_trait]
    impl BatchSubmitter for RecordingSubmitter {
        async fn submit_batch(
            &self,
            _group: &str,
            items: &[StagedObject],
        ) -> Result<ObjectCommit, String> {
            let index = {
                let mut batches = self.batches.lock().unwrap_or_else(|e| e.into_inner());
                let object_ids = items.iter().map(|item| item.object_id.clone()).collect();
                batches.push(object_ids);
                batches.len() as u64
            };
            (self.outcome)(index)
        }
    }

    fn staged(object_id: &str) -> StagedObject {
        StagedObject {
            key: CacheKey {
                storage_binding_id: "default".to_owned(),
                bucket: "warehouse".to_owned(),
                key: format!("data/{object_id}.parquet"),
            },
            object_id: object_id.to_owned(),
            object_len: 4096,
            payload_hash: [0u8; 32],
            geometry: Geometry {
                k: 2,
                m: 2,
                chunk: 4096,
            },
            placements: Vec::new(),
        }
    }

    /// A burst of concurrent commits to the same group lands in one Raft
    /// entry: this is the amortization the perf objective requires.
    #[tokio::test]
    async fn concurrent_commits_to_the_same_group_coalesce_into_one_submission() {
        let submitter = Arc::new(RecordingSubmitter::ok());
        let batcher = ObjectCommitBatcher::new(
            Arc::clone(&submitter) as Arc<dyn BatchSubmitter>,
            Duration::from_millis(30),
            100,
        );
        let batcher = Arc::new(batcher);

        let mut handles = Vec::new();
        for i in 0..8 {
            let batcher = Arc::clone(&batcher);
            handles.push(tokio::spawn(async move {
                batcher
                    .commit("shard/0".to_owned(), staged(&format!("obj-{i}")))
                    .await
            }));
        }
        let mut indices = Vec::new();
        for handle in handles {
            indices.push(handle.await.expect("task join").expect("commit ok").index);
        }

        assert_eq!(submitter.call_count(), 1, "one Raft entry for the burst");
        assert_eq!(submitter.total_items(), 8);
        assert!(
            indices.iter().all(|index| *index == indices[0]),
            "every object in the batch shares the same committed index: {indices:?}"
        );
    }

    /// Reaching `max_batch` flushes immediately rather than waiting out the
    /// linger window, so a filled batch never adds latency past what the
    /// batch itself needs.
    #[tokio::test]
    async fn max_batch_flushes_before_the_linger_expires() {
        let submitter = Arc::new(RecordingSubmitter::ok());
        let batcher = Arc::new(ObjectCommitBatcher::new(
            Arc::clone(&submitter) as Arc<dyn BatchSubmitter>,
            Duration::from_secs(10),
            2,
        ));

        let a = {
            let batcher = Arc::clone(&batcher);
            tokio::spawn(async move { batcher.commit("shard/0".to_owned(), staged("a")).await })
        };
        let b = {
            let batcher = Arc::clone(&batcher);
            tokio::spawn(async move { batcher.commit("shard/0".to_owned(), staged("b")).await })
        };

        let result = tokio::time::timeout(Duration::from_millis(500), async {
            (a.await.expect("join a"), b.await.expect("join b"))
        })
        .await
        .expect("max-batch flush must not wait for the 10s linger");
        assert!(result.0.is_ok());
        assert!(result.1.is_ok());
        assert_eq!(submitter.call_count(), 1);
        assert_eq!(submitter.total_items(), 2);
    }

    /// A lone commit with no company still flushes on its own after the
    /// linger window, instead of waiting for a batch that will never fill.
    #[tokio::test]
    async fn a_lone_commit_still_flushes_promptly() {
        let submitter = Arc::new(RecordingSubmitter::ok());
        let batcher = ObjectCommitBatcher::new(
            Arc::clone(&submitter) as Arc<dyn BatchSubmitter>,
            Duration::from_millis(20),
            100,
        );

        let result = tokio::time::timeout(
            Duration::from_millis(500),
            batcher.commit("shard/0".to_owned(), staged("solo")),
        )
        .await
        .expect("a lone write must still ack promptly");
        assert!(result.is_ok());
        assert_eq!(submitter.call_count(), 1);
        assert_eq!(submitter.total_items(), 1);
    }

    /// Different consensus groups batch independently: one group's burst
    /// never waits on, or gets folded into, another group's batch.
    #[tokio::test]
    async fn different_groups_batch_independently() {
        let submitter = Arc::new(RecordingSubmitter::ok());
        let batcher = Arc::new(ObjectCommitBatcher::new(
            Arc::clone(&submitter) as Arc<dyn BatchSubmitter>,
            Duration::from_millis(30),
            100,
        ));

        let a = {
            let batcher = Arc::clone(&batcher);
            tokio::spawn(async move { batcher.commit("shard/0".to_owned(), staged("a")).await })
        };
        let b = {
            let batcher = Arc::clone(&batcher);
            tokio::spawn(async move { batcher.commit("shard/1".to_owned(), staged("b")).await })
        };
        a.await.expect("join a").expect("commit a ok");
        b.await.expect("join b").expect("commit b ok");

        assert_eq!(submitter.call_count(), 2, "one entry per group");
        assert_eq!(submitter.total_items(), 2);
    }

    /// A batch rejection is atomic: every object folded into a failed Raft
    /// entry is rejected identically, never partially applied.
    #[tokio::test]
    async fn a_batch_failure_rejects_every_item_in_it() {
        let submitter = Arc::new(RecordingSubmitter::failing());
        let batcher = Arc::new(ObjectCommitBatcher::new(
            Arc::clone(&submitter) as Arc<dyn BatchSubmitter>,
            Duration::from_millis(30),
            100,
        ));

        let mut handles = Vec::new();
        for i in 0..4 {
            let batcher = Arc::clone(&batcher);
            handles.push(tokio::spawn(async move {
                batcher
                    .commit("shard/0".to_owned(), staged(&format!("obj-{i}")))
                    .await
            }));
        }
        for handle in handles {
            let result = handle.await.expect("task join");
            assert_eq!(result, Err("simulated consensus failure".to_owned()));
        }
        assert_eq!(submitter.call_count(), 1, "one rejected entry, not four");
    }
}
