//! The local durable queue: per-source JSONL segments with consumer-group
//! watermarks — the queue output type a source or sink can target instead of a
//! table (WHITEPAPER §7.5, issue #327).
//!
//! Promoted out of `verglas-memory-jobs::stream` into the platform layer and made
//! generic over the payload it carries. Each source owns one directory of JSONL
//! segments named by source + starting sequence (`<source>-<start:012>.seg`).
//! The starting sequence is the global position of the segment's first record,
//! so positions stay stable when old segments are deleted. Producers append
//! ([`SegmentLog::append`]); consumers read by explicit consumer-group watermark
//! ([`SegmentLog::read_from`], [`SegmentLog::ack`]).
//!
//! # Semantics: at-least-once with consumer-side idempotency
//!
//! An append is durable (written and flushed) before any consumer sees it, and a
//! group's watermark advances only on an explicit [`SegmentLog::ack`] after the
//! consumer's downstream commit. A crash between read and ack loses nothing: the
//! next read re-serves the same records. Delivery is therefore **at-least-once**;
//! a consumer that must not act twice records what it has already handled
//! (**consumer-side idempotency**) — the same discipline the commit path
//! enforces with idempotency keys. Groups are independent: each advances on its
//! own acks and never blocks another. A group that lags past the segment TTL
//! loses the expired records — the TTL is the retention window, enforced by
//! [`SegmentLog::cleanup`].

use std::io::{BufRead, BufReader, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

/// Bound on a single segment file. Appends past it roll to a new segment. A
/// small bound keeps reads and cleanup granular without a config knob.
pub const MAX_SEGMENT_BYTES: u64 = 4 * 1024 * 1024;

/// Bound on one source's total log size (ported from the per-session spool cap):
/// beyond it appends are dropped and counted in a `.dropped` sidecar, so a
/// pathological burst can never balloon the disk. TTL cleanup reopens the log.
pub const MAX_SOURCE_BYTES: u64 = 64 * 1024 * 1024;

/// How long a segment is retained after its last write. Cleanup removes older
/// inactive segments; this is the queue's retention window.
pub const SEGMENT_TTL: Duration = Duration::from_secs(14 * 86_400);

/// The subdirectory of a spool/state dir that holds the per-source logs.
pub const STREAMS_DIR: &str = "streams";

/// A record a queue carries. Serialized as one JSONL line; [`QueuePayload::bound`]
/// clamps any oversized field before the write (a no-op by default).
pub trait QueuePayload: Serialize + DeserializeOwned + Clone {
    /// Bounds the payload before it is written, so one pathological record cannot
    /// balloon a segment or a later read. The default keeps it unchanged.
    fn bound(self) -> Self {
        self
    }
}

/// A plain JSON value carried through the queue — the default payload for
/// sources and sinks that target a queue with row-shaped data.
impl QueuePayload for serde_json::Value {}

/// The queue root under a spool/state directory.
pub fn queue_root(spool_dir: &Path) -> PathBuf {
    spool_dir.join(STREAMS_DIR)
}

/// Keeps only filename-safe characters.
fn safe_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// One source's segment log, carrying payloads of type `T`.
pub struct SegmentLog<T> {
    dir: PathBuf,
    source: String,
    _payload: PhantomData<fn() -> T>,
}

impl<T> SegmentLog<T> {
    /// Opens (creating if needed) the log for `source` under `queue_root`.
    pub fn open(queue_root: &Path, source: &str) -> std::io::Result<SegmentLog<T>> {
        let source = safe_name(source);
        let dir = queue_root.join(&source);
        std::fs::create_dir_all(dir.join("groups"))?;
        Ok(SegmentLog {
            dir,
            source,
            _payload: PhantomData,
        })
    }

    /// The source name this log serves.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// The segment file name for a starting position.
    fn segment_name(&self, start: u64) -> String {
        format!("{}-{start:012}.seg", self.source)
    }

    /// Lists segments as `(start_position, path)`, sorted by start.
    fn segments(&self) -> std::io::Result<Vec<(u64, PathBuf)>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Some(stem) = name
                .strip_prefix(&format!("{}-", self.source))
                .and_then(|s| s.strip_suffix(".seg"))
            else {
                continue;
            };
            let Ok(start) = stem.parse::<u64>() else {
                continue;
            };
            out.push((start, path));
        }
        out.sort_by_key(|(start, _)| *start);
        Ok(out)
    }

    /// Counts the lines of a segment file (its record count). Every line —
    /// including a malformed one — occupies one position, so positions stay
    /// stable regardless of parseability.
    fn line_count(path: &Path) -> std::io::Result<u64> {
        let file = match std::fs::File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e),
        };
        let mut n = 0u64;
        for line in BufReader::new(file).lines() {
            line?;
            n += 1;
        }
        Ok(n)
    }

    /// The position one past the last appended record (the log's end).
    pub fn end_position(&self) -> std::io::Result<u64> {
        match self.segments()?.last() {
            None => Ok(0),
            Some((start, path)) => Ok(start + Self::line_count(path)?),
        }
    }

    /// The watermark file for a consumer group.
    fn group_path(&self, group: &str) -> PathBuf {
        self.dir
            .join("groups")
            .join(format!("{}.wm", safe_name(group)))
    }

    /// The consumer group's watermark: the position it has acked through. Zero
    /// for a group that never acked.
    pub fn watermark(&self, group: &str) -> std::io::Result<u64> {
        match std::fs::read_to_string(self.group_path(group)) {
            Ok(s) => Ok(s.trim().parse().unwrap_or(0)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(e),
        }
    }

    /// Advances the group's watermark to `pos`. Monotone: a regressing ack is
    /// ignored, so a late or replayed ack can never rewind consumption.
    pub fn ack(&self, group: &str, pos: u64) -> std::io::Result<()> {
        let current = self.watermark(group)?;
        if pos <= current {
            return Ok(());
        }
        std::fs::create_dir_all(self.dir.join("groups"))?;
        std::fs::write(self.group_path(group), pos.to_string())?;
        Ok(())
    }

    /// Removes inactive segments whose last write is older than `ttl`, returning
    /// how many were removed. The active (highest-start) segment is never
    /// removed; this is the queue's retention bound, so a consumer group lagging
    /// past the TTL loses the expired records.
    pub fn cleanup(&self, ttl: Duration) -> std::io::Result<usize> {
        let segments = self.segments()?;
        let Some(active_start) = segments.last().map(|(start, _)| *start) else {
            return Ok(0);
        };
        let now = std::time::SystemTime::now();
        let mut removed = 0;
        for (start, path) in &segments {
            if *start == active_start {
                continue;
            }
            let modified = std::fs::metadata(path).and_then(|m| m.modified())?;
            if now.duration_since(modified).unwrap_or_default() > ttl {
                std::fs::remove_file(path)?;
                removed += 1;
            }
        }
        Ok(removed)
    }
}

impl<T: QueuePayload> SegmentLog<T> {
    /// Appends one record to the active segment, rolling to a new segment at the
    /// size bound. Returns `false` when the source-size cap dropped the record
    /// (the drop is counted in the `.dropped` sidecar). The payload is bounded
    /// first.
    pub fn append(&self, record: &T) -> std::io::Result<bool> {
        let segments = self.segments()?;

        // Source-size cap: drop-and-count beyond the bound.
        let total: u64 = segments
            .iter()
            .filter_map(|(_, p)| std::fs::metadata(p).ok())
            .map(|m| m.len())
            .sum();
        if total >= MAX_SOURCE_BYTES {
            let mut dropped = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.dir.join(format!("{}.dropped", self.source)))?;
            writeln!(dropped, "1")?;
            return Ok(false);
        }

        // Active segment: the highest start, rolled if it reached the bound.
        let path = match segments.last() {
            None => self.dir.join(self.segment_name(0)),
            Some((start, path)) => {
                let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
                if size >= MAX_SEGMENT_BYTES {
                    // The new segment starts where this one ends. Counting the
                    // lines here (once per roll, not per append) is what makes the
                    // name carry the global position.
                    let next = start + Self::line_count(path)?;
                    self.dir.join(self.segment_name(next))
                } else {
                    path.clone()
                }
            }
        };

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let line = serde_json::to_string(&record.clone().bound())?;
        writeln!(file, "{line}")?;
        Ok(true)
    }

    /// Reads up to `max` records with global position `>= pos`, in order, as
    /// `(position, record)` pairs. Malformed lines occupy their position but are
    /// skipped, so one bad line never blocks consumption.
    pub fn read_from(&self, pos: u64, max: usize) -> std::io::Result<Vec<(u64, T)>> {
        let segments = self.segments()?;
        let mut out = Vec::new();
        for (i, (start, path)) in segments.iter().enumerate() {
            // Skip segments that end before `pos` (the next segment's start bounds
            // this one's positions).
            if let Some((next_start, _)) = segments.get(i + 1)
                && *next_start <= pos
            {
                continue;
            }
            let file = match std::fs::File::open(path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            for (line_idx, line) in BufReader::new(file).lines().enumerate() {
                let line = line?;
                let position = start + line_idx as u64;
                if position < pos {
                    continue;
                }
                if out.len() >= max {
                    return Ok(out);
                }
                if let Ok(record) = serde_json::from_str::<T>(&line) {
                    out.push((position, record));
                }
            }
        }
        Ok(out)
    }
}

/// Lists the sources with a log under `queue_root` (its subdirectories).
pub fn sources(queue_root: &Path) -> std::io::Result<Vec<String>> {
    let mut out = Vec::new();
    let entries = match std::fs::read_dir(queue_root) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        if entry.path().is_dir()
            && let Some(name) = entry.file_name().to_str()
        {
            out.push(name.to_owned());
        }
    }
    out.sort();
    Ok(out)
}
