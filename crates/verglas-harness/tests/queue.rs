//! Acceptance for the platform queue (#327): the per-source segment log with
//! consumer-group watermarks, in its new shared home. These are the SegmentLog
//! tests moved out of verglas-memory-jobs (which no longer owns the queue),
//! rewritten over a generic payload.
//!
//! The criteria:
//! - Appends land in segments named by source + starting sequence, rolling to a
//!   new segment at the bounded size; reads return records with their stable
//!   global positions, in order, across segment boundaries.
//! - Consumption is by explicit consumer-group watermark: reads without an ack
//!   re-serve the same records (capture-before-ack), acks are monotone, and a
//!   crash between read and ack loses nothing.
//! - TTL cleanup removes expired inactive segments only; surviving records keep
//!   their positions.

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use verglas_harness::queue::{MAX_SEGMENT_BYTES, QueuePayload, SegmentLog};

/// A generic queue record with a distinctive body.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Record {
    id: usize,
    body: String,
}

impl QueuePayload for Record {}

/// A ~2 KB record.
fn rec(i: usize) -> Record {
    Record {
        id: i,
        body: format!("record {i} :: {}", "x".repeat(1900)),
    }
}

/// Appends roll into a second segment at the size bound, and a full read returns
/// every record in order with stable global positions.
#[test]
fn append_rolls_segments_and_read_is_ordered() {
    let root = TempDir::new().expect("test setup");
    let log = SegmentLog::<Record>::open(root.path(), "src").expect("open");

    let n = (MAX_SEGMENT_BYTES / 2000 + 200) as usize;
    for i in 0..n {
        assert!(log.append(&rec(i)).expect("append"), "not dropped");
    }

    let segs: Vec<_> = std::fs::read_dir(root.path().join("src"))
        .expect("dir")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|f| f.ends_with(".seg"))
        .collect();
    assert!(
        segs.len() >= 2,
        "rolled into at least two segments: {segs:?}"
    );
    assert!(segs.iter().all(|s| s.starts_with("src-")));

    let all = log.read_from(0, usize::MAX).expect("read");
    assert_eq!(all.len(), n, "every record is read back");
    for (i, (pos, record)) in all.iter().enumerate() {
        assert_eq!(*pos, i as u64, "positions are dense and ordered");
        assert_eq!(record.id, i);
    }

    let tail = log.read_from(n as u64 - 2, usize::MAX).expect("read tail");
    assert_eq!(tail.len(), 2);
    assert_eq!(tail[0].0, n as u64 - 2);
    assert_eq!(log.end_position().expect("end"), n as u64);
}

/// Consumer-group watermarks: capture-before-ack means an un-acked read is
/// re-served identically; acks are monotone and never regress.
#[test]
fn watermark_ack_is_monotone_and_capture_survives_until_ack() {
    let root = TempDir::new().expect("test setup");
    let log = SegmentLog::<Record>::open(root.path(), "src").expect("open");
    for i in 0..3 {
        log.append(&rec(i)).expect("append");
    }

    let group = "consumer:a";
    assert_eq!(log.watermark(group).expect("wm"), 0, "fresh group at zero");

    let first = log
        .read_from(log.watermark(group).expect("wm"), usize::MAX)
        .expect("read");
    assert_eq!(first.len(), 3);

    // No ack: a crash-restart re-reads the same records.
    let again = log
        .read_from(log.watermark(group).expect("wm"), usize::MAX)
        .expect("read");
    assert_eq!(first, again, "capture-before-ack: nothing was lost");

    log.ack(group, 2).expect("ack");
    assert_eq!(log.watermark(group).expect("wm"), 2);
    let rest = log
        .read_from(log.watermark(group).expect("wm"), usize::MAX)
        .expect("read");
    assert_eq!(rest.len(), 1, "only the un-acked tail remains");

    // A regressing ack is ignored.
    log.ack(group, 1).expect("ack");
    assert_eq!(log.watermark(group).expect("wm"), 2, "no regression");

    // Groups are independent.
    assert_eq!(log.watermark("consumer:b").expect("wm"), 0);
}

/// TTL cleanup removes expired inactive segments only; the active segment is
/// kept even when old, and surviving records keep their global positions.
#[test]
fn ttl_cleanup_expires_inactive_segments_and_keeps_positions() {
    let root = TempDir::new().expect("test setup");
    let log = SegmentLog::<Record>::open(root.path(), "src").expect("open");
    let n = (MAX_SEGMENT_BYTES / 2000 + 100) as usize;
    for i in 0..n {
        log.append(&rec(i)).expect("append");
    }
    let dir = root.path().join("src");
    let mut segs: Vec<_> = std::fs::read_dir(&dir)
        .expect("dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("seg"))
        .collect();
    segs.sort();
    assert!(segs.len() >= 2, "needs a rolled log: {segs:?}");

    let old = std::time::SystemTime::now() - std::time::Duration::from_secs(86_400 * 30);
    for s in &segs {
        filetime::set_file_mtime(s, filetime::FileTime::from_system_time(old)).expect("mtime");
    }
    let removed = log
        .cleanup(std::time::Duration::from_secs(86_400 * 14))
        .expect("cleanup");
    assert_eq!(
        removed,
        segs.len() - 1,
        "all inactive expired segments went"
    );

    let survivors = log.read_from(0, usize::MAX).expect("read");
    assert!(!survivors.is_empty());
    let (first_pos, first_record) = &survivors[0];
    assert!(
        *first_pos > 0,
        "early positions are gone with their segment"
    );
    assert_eq!(
        first_record.id, *first_pos as usize,
        "surviving positions still address the same records"
    );
}
