//! Group-commit batching for immutable object-header consensus commits.
//!
//! `finish_stream_ack` (`verglas-writeback::coordinator`) acks a write-back
//! PUT only after its immutable header commits through Raft. Committing one
//! object per Raft round trip caps client throughput at
//! `shards / raft_round_trip` (`tests/cluster-local/PERF-OBJECTIVE.md`).
//!
//! This module coalesces concurrent `StagedObject` commits for the same
//! consensus group into one Raft log entry, using the rule PostgreSQL's WAL
//! writer uses and for the same reason: **the first arrival submits
//! immediately, and everything that arrives while that entry is in flight is
//! folded into the next one.** There is no timer. The batch window is
//! whatever the previous commit's own latency happened to be, so it is
//! self-tuning at both extremes — an idle writer is never delayed, and under
//! load the batch grows to exactly match the round trip it is amortizing. A
//! fixed linger cannot do both: it taxes the idle case by its full length,
//! and it closes on a burst that is still arriving.
//!
//! The caller still only learns success once its own object is inside a
//! durably committed batch. Batching changes how many Raft entries a burst
//! costs, never what the acknowledgement promises.

use std::collections::HashMap;
use std::sync::Arc;

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

/// One consensus group's queue of commits waiting for their Raft entry.
#[derive(Default)]
struct GroupBatch {
    items: Vec<PendingCommit>,
    /// True while this group has an entry in flight, or a drain about to
    /// pick one up. It is the group's write lock: exactly one drain runs per
    /// group, so entries never race each other into the log, and an arrival
    /// that finds it set knows a drain will collect it.
    draining: bool,
}

/// Coalesces `StagedObject` commits for one consensus group into as few Raft
/// entries as the group's own commit latency allows.
pub struct ObjectCommitBatcher {
    max_batch: usize,
    submitter: Arc<dyn BatchSubmitter>,
    groups: Arc<Mutex<HashMap<String, GroupBatch>>>,
}

impl ObjectCommitBatcher {
    /// Builds a batcher over `submitter`. `max_batch` caps how many objects
    /// one Raft entry carries, bounding both the entry's payload and the
    /// blast radius of an all-or-nothing retry. There is deliberately no
    /// window to configure: the batch closes when the previous entry commits.
    pub fn new(submitter: Arc<dyn BatchSubmitter>, max_batch: usize) -> Self {
        Self {
            max_batch: max_batch.max(1),
            submitter,
            groups: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Enqueues `staged` into `group`'s queue and resolves only once the Raft
    /// entry carrying `staged` has committed or been rejected. A commit to
    /// one group never waits on another group's entry.
    pub async fn commit(
        &self,
        group: String,
        staged: StagedObject,
    ) -> Result<ObjectCommit, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let start_drain = {
            let mut groups = self.groups.lock().await;
            let batch = groups.entry(group.clone()).or_default();
            batch.items.push(PendingCommit {
                staged,
                reply: reply_tx,
            });
            // Whoever finds the group idle takes it and submits at once.
            // Everyone else is collected by the drain already running.
            let idle = !batch.draining;
            batch.draining = true;
            idle
        };
        if start_drain {
            // The drain runs on its own task so this caller returns as soon
            // as ITS entry commits, rather than staying to submit entries for
            // writers that arrived behind it.
            tokio::spawn(drain_group(
                Arc::clone(&self.groups),
                Arc::clone(&self.submitter),
                group,
                self.max_batch,
            ));
        }
        match reply_rx.await {
            Ok(result) => result,
            Err(_) => Err("object commit batch dropped without a reply".to_owned()),
        }
    }
}

/// Submits `group`'s queued commits, one Raft entry at a time, until the
/// queue is empty.
///
/// Each pass takes everything queued (up to `max_batch`) and commits it as
/// one entry. Whatever arrives while that entry is in flight is waiting when
/// the pass returns, and goes out in the next one — so the batch size tracks
/// the commit latency without a timer.
///
/// Clearing `draining` and observing the empty queue happen under one lock
/// acquisition. An arrival can therefore never slip in between the two and
/// be left with no drain to collect it.
async fn drain_group(
    groups: Arc<Mutex<HashMap<String, GroupBatch>>>,
    submitter: Arc<dyn BatchSubmitter>,
    group: String,
    max_batch: usize,
) {
    loop {
        let items = {
            let mut locked = groups.lock().await;
            let Some(batch) = locked.get_mut(&group) else {
                return;
            };
            if batch.items.is_empty() {
                batch.draining = false;
                return;
            }
            let take = batch.items.len().min(max_batch);
            batch.items.drain(..take).collect::<Vec<_>>()
        };
        let (staged, replies): (
            Vec<StagedObject>,
            Vec<oneshot::Sender<Result<ObjectCommit, String>>>,
        ) = items
            .into_iter()
            .map(|item| (item.staged, item.reply))
            .unzip();
        let result = submitter.submit_batch(&group, &staged).await;
        for reply in replies {
            let _ = reply.send(result.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;
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

    /// A burst of concurrent commits to one group costs fewer Raft entries
    /// than it has objects, and every object still gets its own answer.
    ///
    /// The exact split depends on task scheduling — that is inherent to
    /// closing a batch on the previous commit rather than on a clock — so
    /// this asserts the invariants that always hold. The deterministic proof
    /// that folding happens is
    /// `arrivals_during_an_in_flight_entry_fold_into_the_next_one`.
    #[tokio::test]
    async fn a_burst_to_one_group_costs_fewer_entries_than_objects() {
        let submitter = Arc::new(RecordingSubmitter::ok());
        let batcher = Arc::new(ObjectCommitBatcher::new(
            Arc::clone(&submitter) as Arc<dyn BatchSubmitter>,
            100,
        ));

        let mut handles = Vec::new();
        for i in 0..8 {
            let batcher = Arc::clone(&batcher);
            handles.push(tokio::spawn(async move {
                batcher
                    .commit("shard/0".to_owned(), staged(&format!("obj-{i}")))
                    .await
            }));
        }
        for handle in handles {
            handle.await.expect("task join").expect("commit ok");
        }

        assert_eq!(
            submitter.total_items(),
            8,
            "every object commits exactly once"
        );
        assert!(
            submitter.call_count() <= 8,
            "never more entries than objects: {}",
            submitter.call_count()
        );
    }

    /// `max_batch` caps one Raft entry even when far more work is queued
    /// behind the entry in flight, so one entry's payload and its
    /// all-or-nothing retry stay bounded.
    #[tokio::test]
    async fn max_batch_caps_a_single_entry() {
        let submitter = Arc::new(GatedSubmitter::new());
        let batcher = Arc::new(ObjectCommitBatcher::new(
            Arc::clone(&submitter) as Arc<dyn BatchSubmitter>,
            2,
        ));

        let first = {
            let batcher = Arc::clone(&batcher);
            tokio::spawn(async move { batcher.commit("shard/0".to_owned(), staged("first")).await })
        };
        tokio::time::timeout(Duration::from_secs(5), submitter.entered.notified())
            .await
            .expect("the first commit submits immediately");

        let mut rest = Vec::new();
        for i in 0..5 {
            let batcher = Arc::clone(&batcher);
            rest.push(tokio::spawn(async move {
                batcher
                    .commit("shard/0".to_owned(), staged(&format!("obj-{i}")))
                    .await
            }));
        }
        await_queued(&batcher, "shard/0", 5).await;
        submitter.release.notify_one();

        first.await.expect("join first").expect("first commits");
        for handle in rest {
            handle.await.expect("join").expect("commits");
        }
        assert_eq!(
            submitter.sizes(),
            vec![1, 2, 2, 1],
            "the queued five leave in entries of at most max_batch"
        );
    }

    /// A lone commit is never delayed waiting for company. This is the half
    /// a fixed linger window gets wrong: it would tax every quiet write by
    /// the full window length.
    #[tokio::test(start_paused = true)]
    async fn a_lone_commit_is_not_delayed() {
        let submitter = Arc::new(RecordingSubmitter::ok());
        let batcher =
            ObjectCommitBatcher::new(Arc::clone(&submitter) as Arc<dyn BatchSubmitter>, 100);

        // Virtual time, not wall clock. Under a paused clock the timer only
        // advances when a task actually awaits one, so this measures whether
        // the batcher waited for a window — not how loaded the machine is. A
        // wall-clock bound here failed at 20.9 ms during a full workspace run
        // for no reason but CPU contention.
        let started = tokio::time::Instant::now();
        let result = batcher.commit("shard/0".to_owned(), staged("solo")).await;
        let waited = tokio::time::Instant::now() - started;

        assert!(result.is_ok());
        assert_eq!(submitter.call_count(), 1);
        assert_eq!(submitter.total_items(), 1);
        assert_eq!(
            waited,
            Duration::ZERO,
            "a lone commit awaited a timer for {waited:?}, waiting for company that was never coming"
        );
    }

    /// Different consensus groups batch independently: one group's burst
    /// never waits on, or gets folded into, another group's batch.
    #[tokio::test]
    async fn different_groups_batch_independently() {
        let submitter = Arc::new(RecordingSubmitter::ok());
        let batcher = Arc::new(ObjectCommitBatcher::new(
            Arc::clone(&submitter) as Arc<dyn BatchSubmitter>,
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

    /// Records batches and holds the FIRST submission open until released,
    /// so a test can observe what accumulates while one entry is in flight.
    struct GatedSubmitter {
        outcome: fn(u64) -> Result<ObjectCommit, String>,
        batches: StdMutex<Vec<Vec<String>>>,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl GatedSubmitter {
        fn new() -> Self {
            Self::with_outcome(|index| Ok(ObjectCommit { index }))
        }

        fn failing() -> Self {
            Self::with_outcome(|_| Err("simulated consensus failure".to_owned()))
        }

        fn with_outcome(outcome: fn(u64) -> Result<ObjectCommit, String>) -> Self {
            Self {
                outcome,
                batches: StdMutex::new(Vec::new()),
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
                calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn sizes(&self) -> Vec<usize> {
            self.batches
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .map(Vec::len)
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl BatchSubmitter for GatedSubmitter {
        async fn submit_batch(
            &self,
            _group: &str,
            items: &[StagedObject],
        ) -> Result<ObjectCommit, String> {
            let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let index = {
                let mut batches = self.batches.lock().unwrap_or_else(|e| e.into_inner());
                batches.push(items.iter().map(|item| item.object_id.clone()).collect());
                batches.len() as u64
            };
            self.entered.notify_one();
            if call == 0 {
                self.release.notified().await;
            }
            (self.outcome)(index)
        }
    }

    /// Waits until `group` has exactly `expected` items queued behind the
    /// in-flight entry.
    async fn await_queued(batcher: &ObjectCommitBatcher, group: &str, expected: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                {
                    let groups = batcher.groups.lock().await;
                    if groups.get(group).map(|batch| batch.items.len()) == Some(expected) {
                        return;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("items queue behind the in-flight entry");
    }

    /// The first commit flushes immediately rather than waiting for company,
    /// and everything that arrives while that entry is in flight folds into
    /// the next one — the WAL group-commit rule Postgres uses, with no timer.
    ///
    /// A fixed linger cannot do both: it either delays the lone commit or
    /// closes the window before the burst lands.
    #[tokio::test(start_paused = true)]
    async fn arrivals_during_an_in_flight_entry_fold_into_the_next_one() {
        let submitter = Arc::new(GatedSubmitter::new());
        let batcher = Arc::new(ObjectCommitBatcher::new(
            Arc::clone(&submitter) as Arc<dyn BatchSubmitter>,
            64,
        ));

        let started = tokio::time::Instant::now();
        let first = {
            let batcher = Arc::clone(&batcher);
            tokio::spawn(async move { batcher.commit("shard/0".to_owned(), staged("first")).await })
        };
        // The lone commit does not wait for company that may never arrive.
        tokio::time::timeout(Duration::from_secs(5), submitter.entered.notified())
            .await
            .expect("a lone commit submits immediately");
        let lone_wait = tokio::time::Instant::now() - started;
        assert_eq!(submitter.sizes(), vec![1], "it flushed alone, at once");
        assert_eq!(
            lone_wait,
            Duration::ZERO,
            "the first commit must submit without awaiting a batching window, waited {lone_wait:?}"
        );

        let mut rest = Vec::new();
        for i in 0..7 {
            let batcher = Arc::clone(&batcher);
            rest.push(tokio::spawn(async move {
                batcher
                    .commit("shard/0".to_owned(), staged(&format!("obj-{i}")))
                    .await
            }));
        }
        await_queued(&batcher, "shard/0", 7).await;

        submitter.release.notify_one();
        first.await.expect("join first").expect("first commits");
        let mut indices = Vec::new();
        for handle in rest {
            indices.push(handle.await.expect("join").expect("commits").index);
        }

        assert_eq!(
            submitter.sizes(),
            vec![1, 7],
            "the 7 that arrived during the in-flight entry share the next one"
        );
        assert!(
            indices.iter().all(|index| *index == indices[0]),
            "the folded objects share one committed index: {indices:?}"
        );
    }

    /// A batch rejection is atomic: every object folded into a failed Raft
    /// entry is rejected identically, never partially applied.
    #[tokio::test]
    async fn a_batch_failure_rejects_every_item_in_it() {
        let submitter = Arc::new(GatedSubmitter::failing());
        let batcher = Arc::new(ObjectCommitBatcher::new(
            Arc::clone(&submitter) as Arc<dyn BatchSubmitter>,
            100,
        ));

        let first = {
            let batcher = Arc::clone(&batcher);
            tokio::spawn(async move { batcher.commit("shard/0".to_owned(), staged("first")).await })
        };
        tokio::time::timeout(Duration::from_secs(5), submitter.entered.notified())
            .await
            .expect("the first commit submits immediately");

        let mut rest = Vec::new();
        for i in 0..3 {
            let batcher = Arc::clone(&batcher);
            rest.push(tokio::spawn(async move {
                batcher
                    .commit("shard/0".to_owned(), staged(&format!("obj-{i}")))
                    .await
            }));
        }
        await_queued(&batcher, "shard/0", 3).await;
        submitter.release.notify_one();

        assert_eq!(
            first.await.expect("join first"),
            Err("simulated consensus failure".to_owned())
        );
        for handle in rest {
            assert_eq!(
                handle.await.expect("join"),
                Err("simulated consensus failure".to_owned()),
                "every object folded into the failed entry is rejected"
            );
        }
        assert_eq!(
            submitter.sizes(),
            vec![1, 3],
            "the three shared one rejected entry, not three"
        );
    }
}
