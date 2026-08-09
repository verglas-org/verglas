//! Hot-path guard: `classify()` must not allocate (issue #49 acceptance).
//!
//! A counting global allocator records allocations while a flag is set. We warm
//! up (so any one-time per-thread `arc-swap`/allocator init happens outside the
//! measured region), then classify every shape — unknown, whole-file data hit,
//! a Parquet range that resolves to a column chunk, and a footer range — and
//! assert zero allocations occurred.

mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use support::{BUCKET, FileSpec, SnapshotSpec};
use verglas_tables::catalog::TableIdent;
use verglas_tables::fetch::ObjectStoreFetch;
use verglas_tables::mapper::{BuildInputs, Mapper};

/// Counts allocations only while [`COUNTING`] is set, delegating to the system
/// allocator otherwise. Lets a test measure allocations of one code region.
struct CountingAllocator;

/// Number of allocations observed while counting is enabled.
static ALLOCS: AtomicUsize = AtomicUsize::new(0);
/// Whether allocations are currently being counted.
static COUNTING: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for CountingAllocator {
    /// Counts (when enabled) then delegates the allocation.
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    /// Delegates deallocation (not counted).
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    /// Counts (when enabled) then delegates the reallocation.
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Runs `f` with allocation counting enabled and returns the count.
fn count_allocs(f: impl FnOnce()) -> usize {
    let before = ALLOCS.load(Ordering::Relaxed);
    COUNTING.store(true, Ordering::Relaxed);
    f();
    COUNTING.store(false, Ordering::Relaxed);
    ALLOCS.load(Ordering::Relaxed) - before
}

#[test]
fn classify_does_not_allocate_on_the_hot_path() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let (mapper, key, span_start, footer_start) = rt.block_on(async {
        let store = support::new_store();
        let files = vec![FileSpec {
            rel_path: "data/f0.parquet".to_owned(),
            format: verglas_tables::mapper::FileFormat::Parquet,
            bytes: support::parquet_nested(0),
            partition: vec![],
        }];
        let fixture = support::build_table(
            &store,
            "warehouse/db/hot",
            &[SnapshotSpec {
                snapshot_id: 1,
                files,
            }],
            1,
        )
        .await;
        let ident = TableIdent::new(&["db"], "hot");
        let mapper = Arc::new(Mapper::new());
        let fetch = ObjectStoreFetch::new(Arc::clone(&store));
        mapper
            .apply_table(
                &fetch,
                &BuildInputs {
                    storage_binding_id: "managed-lakehouse".to_owned(),
                    ident: ident.clone(),
                    bucket: BUCKET.to_owned(),
                    metadata_key: fixture.metadata_key.clone(),
                    snapshot_ids: fixture.snapshot_ids.clone(),
                    current_snapshot_id: Some(fixture.current_snapshot_id),
                },
            )
            .await
            .expect("build");
        let table = mapper.table_id(&ident).expect("table id");
        let built = fixture.first_of(verglas_tables::mapper::FileFormat::Parquet);
        mapper
            .warm_footer(&fetch, table, &built.key, &built.etag)
            .await
            .expect("warm");
        let (span_start, footer_start) = {
            let state = mapper.state();
            let index = state.table(table).expect("present");
            let spans = index
                .file_by_path(&built.key)
                .expect("present")
                .chunks()
                .expect("present")
                .clone();
            (
                spans[spans.len() / 2].start,
                spans.last().expect("present").end,
            )
        };
        (
            Arc::clone(&mapper),
            built.key.clone(),
            span_start,
            footer_start,
        )
    });

    // Warm up: run every shape once so first-touch per-thread init (arc-swap
    // debt slot, etc.) is not attributed to the measured region.
    black_box(mapper.classify(BUCKET, &key, None));
    black_box(mapper.classify(BUCKET, &key, Some(span_start..span_start + 4)));
    black_box(mapper.classify(BUCKET, &key, Some(footer_start..footer_start + 4)));
    black_box(mapper.classify("nope", "nope/x", None));

    let allocs = count_allocs(|| {
        for _ in 0..1_000 {
            black_box(mapper.classify(black_box(BUCKET), black_box(&key), None));
            black_box(mapper.classify(
                black_box(BUCKET),
                black_box(&key),
                black_box(Some(span_start..span_start + 4)),
            ));
            black_box(mapper.classify(
                black_box(BUCKET),
                black_box(&key),
                black_box(Some(footer_start..footer_start + 4)),
            ));
            black_box(mapper.classify(black_box("nope"), black_box("nope/x"), None));
        }
    });

    assert_eq!(allocs, 0, "classify() must not allocate on the hot path");
}
