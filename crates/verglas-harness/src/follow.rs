//! Follow mode: run a worker as a long-lived local process that follows a
//! target, appending each captured output line to an Iceberg table as a row.
//!
//! Two sources are supported:
//!
//! - **command** — the worker's own `exec` command is spawned and its stdout and
//!   stderr are captured line by line.
//! - **file** — a file is tailed from its current end; each newly appended line
//!   becomes a row.
//!
//! Rows are batched (bounded by a row count and a flush interval) and committed
//! through the shared CAS append path ([`verglas_iceberg::write`]). Because the
//! append goes through the server's own catalog, the destination follows the
//! server's login state with no branch here: logged in, the catalog points at the
//! tenant's cloud lakehouse and the lines stream off the machine into the cloud;
//! logged out, they land in the local lakehouse.
//!
//! The write is inline in the read loop, so a slow sink pauses reading rather than
//! buffering without bound — the OS pipe (command mode) or the file (file mode)
//! holds the backpressure. Shutdown flushes whatever is buffered before it
//! returns.
//!
//! The row schema is fixed and documented on [`follow_log_schema`]: dashboards are
//! built on it, so it never grows a per-worker column.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef, TimeUnit};
use chrono::{DateTime, Utc};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader};
use tokio::process::Command;
use tokio::sync::watch;
use uuid::Uuid;

use crate::commit::HarnessError;
use crate::worker::WorkerExec;

/// The namespace follow-mode log tables live in when a worker names a bare table.
pub const FOLLOW_NAMESPACE: &str = "follow";

/// The most rows held before a flush is forced, so a fast producer cannot grow
/// the in-memory batch without bound between interval flushes.
const BATCH_MAX_ROWS: usize = 500;

/// How long a partially filled batch waits before it is flushed anyway, so a slow
/// producer's lines still reach the table promptly.
const FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

/// How often file mode checks the tailed file for newly appended bytes.
const FILE_POLL: std::time::Duration = std::time::Duration::from_millis(500);

/// The hard cap on a captured line, so one runaway line cannot bloat a row.
const MAX_LINE: usize = 8192;

/// The `stdout` stream label.
const STREAM_STDOUT: &str = "stdout";
/// The `stderr` stream label.
const STREAM_STDERR: &str = "stderr";
/// The file-tail stream label.
const STREAM_FILE: &str = "file";

/// Where a follow worker gets its lines.
#[derive(Debug, Clone)]
pub enum FollowSource {
    /// Run the worker's command, capturing its stdout and stderr.
    Command(WorkerExec),
    /// Tail a file from its current end.
    File(PathBuf),
}

/// Why a [`run_follow`] loop returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FollowEnd {
    /// The source ended on its own — the wrapped command exited, or the file
    /// could not be followed. The worker is done and should not be restarted.
    Completed,
    /// A shutdown was requested (the watch flipped or its sender dropped). The
    /// worker should be restarted if it is still declared.
    ShutdownRequested,
}

/// The fixed follow-mode log schema. Every follow worker writes exactly these
/// columns, in this order — dashboards are built on this shape, so it is stable:
///
/// - `ts` — when the line was captured (UTC microseconds).
/// - `stream` — `stdout`, `stderr`, or `file`.
/// - `line` — the captured text, clipped to a bounded length.
/// - `worker` — the worker name that produced the line.
/// - `run_id` — the id of this follow run, so restarts are distinguishable.
/// - `seq` — a per-run monotonic sequence, so lines sharing a `ts` still order.
/// - `day` — the UTC day of `ts`, the identity partition (derived, not input).
pub fn follow_log_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            false,
        ),
        Field::new("stream", DataType::Utf8, false),
        Field::new("line", DataType::Utf8, false),
        Field::new("worker", DataType::Utf8, false),
        Field::new("run_id", DataType::Utf8, false),
        Field::new("seq", DataType::Int64, false),
        Field::new("day", DataType::Utf8, false),
    ]))
}

/// Parses a follow worker's target table name into a table identifier. The name
/// must be `namespace.table`; a bare name is placed in the [`FOLLOW_NAMESPACE`].
pub fn follow_table_ident(target: &str) -> Result<TableIdent, HarnessError> {
    let parts: Vec<&str> = target.split('.').filter(|s| !s.is_empty()).collect();
    match parts.as_slice() {
        [] => Err(HarnessError::Job(
            "a follow worker needs a target table".to_owned(),
        )),
        [table] => Ok(TableIdent::new(
            NamespaceIdent::new(FOLLOW_NAMESPACE.to_owned()),
            (*table).to_owned(),
        )),
        [ns @ .., table] => Ok(TableIdent::new(
            NamespaceIdent::from_vec(ns.iter().map(|s| (*s).to_owned()).collect())
                .map_err(|e| HarnessError::Job(format!("bad target table `{target}`: {e}")))?,
            (*table).to_owned(),
        )),
    }
}

/// Clips a captured line to [`MAX_LINE`] on a char boundary.
fn clip(mut line: String) -> String {
    if line.len() > MAX_LINE {
        let mut end = MAX_LINE;
        while !line.is_char_boundary(end) {
            end -= 1;
        }
        line.truncate(end);
    }
    line
}

/// One captured line staged before it is encoded.
struct StagedLine {
    ts: DateTime<Utc>,
    stream: &'static str,
    line: String,
    seq: i64,
}

/// Buffers captured lines and commits them to the follow table in bounded
/// batches, creating the table on first write.
struct FollowWriter {
    catalog: Arc<dyn Catalog>,
    ident: TableIdent,
    worker: String,
    run_id: String,
    staged: Vec<StagedLine>,
    seq: i64,
}

impl FollowWriter {
    fn new(catalog: Arc<dyn Catalog>, ident: TableIdent, worker: String) -> FollowWriter {
        FollowWriter {
            catalog,
            ident,
            worker,
            run_id: Uuid::new_v4().to_string(),
            staged: Vec::new(),
            seq: 0,
        }
    }

    /// Stages one captured line, stamping it now and assigning the next sequence.
    fn push(&mut self, stream: &'static str, line: String) {
        self.staged.push(StagedLine {
            ts: Utc::now(),
            stream,
            line: clip(line),
            seq: self.seq,
        });
        self.seq += 1;
    }

    /// How many lines are buffered awaiting a flush.
    fn len(&self) -> usize {
        self.staged.len()
    }

    /// Commits the buffered lines in one append, creating the table on first
    /// write. Best-effort: a failed append is warned and the batch dropped, so a
    /// sink problem can never grow the buffer without bound or wedge the loop.
    async fn flush(&mut self) {
        if self.staged.is_empty() {
            return;
        }
        if let Err(e) = self.commit().await {
            tracing::warn!(
                "follow worker {}: appending {} line(s) to {} failed; dropping them: {e}",
                self.worker,
                self.staged.len(),
                self.ident.name()
            );
        }
        self.staged.clear();
    }

    /// Ensures the table exists, then appends the staged lines.
    async fn commit(&self) -> Result<(), HarnessError> {
        if self.catalog.load_table(&self.ident).await.is_err()
            && let Err(e) = verglas_iceberg::write::create_table_from_schema(
                self.catalog.as_ref(),
                &self.ident,
                &follow_log_schema(),
                Some("day"),
            )
            .await
            && self.catalog.load_table(&self.ident).await.is_err()
        {
            return Err(HarnessError::Job(format!(
                "create follow table {}: {e}",
                self.ident.name()
            )));
        }
        let batch = self.encode();
        verglas_iceberg::write::append_batches(
            self.catalog.as_ref(),
            &self.ident,
            vec![batch],
            HashMap::new(),
        )
        .await
        .map_err(|e| HarnessError::Job(e.to_string()))?;
        Ok(())
    }

    /// Encodes the staged lines into one record batch over [`follow_log_schema`].
    fn encode(&self) -> RecordBatch {
        let n = self.staged.len();
        let ts = TimestampMicrosecondArray::from(
            self.staged
                .iter()
                .map(|l| l.ts.timestamp_micros())
                .collect::<Vec<_>>(),
        )
        .with_timezone("+00:00");
        let day: Vec<String> = self
            .staged
            .iter()
            .map(|l| l.ts.format("%Y-%m-%d").to_string())
            .collect();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(ts),
            Arc::new(
                self.staged
                    .iter()
                    .map(|l| Some(l.stream))
                    .collect::<StringArray>(),
            ),
            Arc::new(
                self.staged
                    .iter()
                    .map(|l| Some(l.line.clone()))
                    .collect::<StringArray>(),
            ),
            Arc::new(StringArray::from(vec![self.worker.clone(); n])),
            Arc::new(StringArray::from(vec![self.run_id.clone(); n])),
            Arc::new(self.staged.iter().map(|l| l.seq).collect::<Int64Array>()),
            Arc::new(StringArray::from(day)),
        ];
        RecordBatch::try_new(follow_log_schema(), columns).expect("follow columns match the schema")
    }
}

/// Runs one follow worker until its source ends or `shutdown` flips. Every
/// captured line is appended to `ident` as a row; the batch is flushed on the
/// interval, when it fills, and once more on shutdown.
pub async fn run_follow(
    catalog: Arc<dyn Catalog>,
    ident: TableIdent,
    worker: String,
    source: FollowSource,
    mut shutdown: watch::Receiver<bool>,
) -> FollowEnd {
    let mut writer = FollowWriter::new(catalog, ident, worker.clone());
    let end = match &source {
        FollowSource::Command(exec) => follow_command(&mut writer, exec, &mut shutdown).await,
        FollowSource::File(path) => follow_file(&mut writer, path, &mut shutdown).await,
    };
    match end {
        Ok(end) => end,
        Err(e) => {
            tracing::warn!("follow worker {worker}: {e}");
            writer.flush().await;
            FollowEnd::Completed
        }
    }
}

/// The next captured line from an optional reader, or `None` when the stream is
/// absent or has ended (a read error is treated as end). When the reader is
/// absent it never resolves, so this is safe under a `reader.is_some()` select
/// guard without an unwrap.
async fn next_line_opt<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut Option<tokio::io::Lines<R>>,
) -> Option<String> {
    match reader {
        Some(lines) => lines.next_line().await.ok().flatten(),
        None => std::future::pending().await,
    }
}

/// A flush interval whose immediate first tick has been consumed, so the first
/// real flush is a full interval away.
async fn flush_ticker() -> tokio::time::Interval {
    let mut ticker = tokio::time::interval(FLUSH_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await;
    ticker
}

/// Runs the worker's command, capturing stdout and stderr as rows until the
/// command exits or shutdown is requested.
async fn follow_command(
    writer: &mut FollowWriter,
    exec: &WorkerExec,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<FollowEnd, HarnessError> {
    let mut cmd = Command::new(&exec.command);
    cmd.args(&exec.args);
    if let Some(cwd) = &exec.cwd {
        cmd.current_dir(cwd);
    }
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    let mut child = cmd
        .spawn()
        .map_err(|e| HarnessError::Job(format!("spawn follow command `{}`: {e}", exec.command)))?;
    let mut out = child.stdout.take().map(|s| BufReader::new(s).lines());
    let mut err = child.stderr.take().map(|s| BufReader::new(s).lines());
    let mut flush = flush_ticker().await;

    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                let _ = child.start_kill();
                writer.flush().await;
                return Ok(FollowEnd::ShutdownRequested);
            }
            line = next_line_opt(&mut out), if out.is_some() => {
                match line {
                    Some(l) => {
                        writer.push(STREAM_STDOUT, l);
                        if writer.len() >= BATCH_MAX_ROWS { writer.flush().await; }
                    }
                    None => out = None,
                }
            }
            line = next_line_opt(&mut err), if err.is_some() => {
                match line {
                    Some(l) => {
                        writer.push(STREAM_STDERR, l);
                        if writer.len() >= BATCH_MAX_ROWS { writer.flush().await; }
                    }
                    None => err = None,
                }
            }
            _ = flush.tick() => { writer.flush().await; }
        }
        if out.is_none() && err.is_none() {
            let _ = child.wait().await;
            writer.flush().await;
            return Ok(FollowEnd::Completed);
        }
    }
}

/// Tails a file from its current end, turning each newly appended line into a row
/// until shutdown is requested.
async fn follow_file(
    writer: &mut FollowWriter,
    path: &PathBuf,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<FollowEnd, HarnessError> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|e| HarnessError::Job(format!("open follow file `{}`: {e}", path.display())))?;
    file.seek(std::io::SeekFrom::End(0))
        .await
        .map_err(|e| HarnessError::Job(format!("seek follow file `{}`: {e}", path.display())))?;
    let mut reader = BufReader::new(file);
    let mut pending: Vec<u8> = Vec::new();
    let mut poll = tokio::time::interval(FILE_POLL);
    let mut flush = flush_ticker().await;

    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                read_available(&mut reader, &mut pending, writer).await;
                writer.flush().await;
                return Ok(FollowEnd::ShutdownRequested);
            }
            _ = poll.tick() => {
                read_available(&mut reader, &mut pending, writer).await;
                if writer.len() >= BATCH_MAX_ROWS { writer.flush().await; }
            }
            _ = flush.tick() => { writer.flush().await; }
        }
    }
}

/// Appends `chunk` to `pending`; if `chunk` completed a line (ended in `\n`),
/// takes the whole `pending` buffer as the line — its trailing newline and any
/// CR before it stripped — and clears `pending`. A partial `chunk` (no newline,
/// i.e. end of file mid-line) is held in `pending` for the next read.
fn complete_line(pending: &mut Vec<u8>, chunk: &[u8]) -> Option<String> {
    let complete = chunk.ends_with(b"\n");
    pending.extend_from_slice(chunk);
    if !complete {
        return None;
    }
    let mut line = std::mem::take(pending);
    line.pop();
    if line.ends_with(b"\r") {
        line.pop();
    }
    Some(String::from_utf8_lossy(&line).into_owned())
}

/// Drains every complete line currently available on `reader` into `writer`,
/// keeping any trailing partial line in `pending` until its newline arrives.
async fn read_available<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
    pending: &mut Vec<u8>,
    writer: &mut FollowWriter,
) {
    loop {
        let mut chunk = Vec::new();
        match reader.read_until(b'\n', &mut chunk).await {
            Ok(0) => return, // no more bytes for now
            Ok(_) => {
                if let Some(line) = complete_line(pending, &chunk) {
                    writer.push(STREAM_FILE, line);
                }
            }
            Err(_) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The follow log schema is exactly the documented columns, in order, all
    /// required. Dashboards depend on this being stable.
    #[test]
    fn schema_is_the_fixed_shape() {
        let schema = follow_log_schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["ts", "stream", "line", "worker", "run_id", "seq", "day"]
        );
        assert!(
            schema.fields().iter().all(|f| !f.is_nullable()),
            "every follow column is required"
        );
    }

    /// A bare target lands in the follow namespace; a dotted one keeps its
    /// namespace; an empty one is rejected.
    #[test]
    fn target_table_ident_parses() {
        let bare = follow_table_ident("app_logs").expect("bare");
        assert_eq!(bare.namespace().to_url_string(), "follow");
        assert_eq!(bare.name(), "app_logs");
        let dotted = follow_table_ident("logs.app").expect("dotted");
        assert_eq!(dotted.namespace().to_url_string(), "logs");
        assert_eq!(dotted.name(), "app");
        assert!(follow_table_ident("").is_err());
    }

    /// A line over the cap is clipped on a char boundary; a short one is left be.
    #[test]
    fn clip_bounds_long_lines() {
        assert_eq!(clip("short".to_owned()), "short");
        assert_eq!(clip("x".repeat(MAX_LINE + 100)).len(), MAX_LINE);
    }

    /// The line splitter emits only complete lines, holds a partial until its
    /// newline arrives, and strips a trailing CR.
    #[test]
    fn complete_line_emits_whole_lines_only() {
        let mut pending = Vec::new();
        assert_eq!(complete_line(&mut pending, b"a\r\n"), Some("a".to_owned()));
        assert!(pending.is_empty());
        assert_eq!(complete_line(&mut pending, b"b\n"), Some("b".to_owned()));
        // A chunk with no newline is a partial line, held in `pending`.
        assert_eq!(complete_line(&mut pending, b"par"), None);
        assert_eq!(pending, b"par");
        // Its newline completes it, joined with the held bytes.
        assert_eq!(
            complete_line(&mut pending, b"tial\n"),
            Some("partial".to_owned())
        );
        assert!(pending.is_empty());
    }
}
