//! The logical key mapper: object byte-range → table / file / column-chunk
//! identity, on a lock-free read path.
//!
//! [`Mapper`] holds an [`arc_swap::ArcSwap`] over an immutable [`MapperState`].
//! Reads ([`Mapper::classify`]) load the current state with a ~nanosecond
//! atomic and never lock or allocate; a single-writer updater builds a fresh
//! state (sharing unchanged `Arc<FileMeta>` from the old one) and swaps it in.
//! In-flight reads keep the old state alive through their `Arc`, so a swap is
//! seen atomically — a concurrent `classify()` returns consistently from the
//! old or the new state, never a mix.
//!
//! # Hot-path rule
//!
//! `classify()` is on the request path. It performs one longest-prefix binary
//! search, one hash lookup, and (for a Parquet range) one more binary search —
//! no locks, no allocation, no aggregation. The counting-allocator test in
//! `tests/` guards this.

mod build;
pub mod updater;

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwap;
use rustc_hash::FxHashMap;

use crate::catalog::TableIdent;

pub use build::BuildInputs;

/// Stable per-table identity within a [`MapperState`]. A small integer so the
/// hot-path maps key on a `Copy` value; the id survives incremental rebuilds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableId(pub u32);

/// Stable per-file identity within a table. Survives rebuilds by reusing the
/// prior `Arc<FileMeta>` for an unchanged file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FileId(pub u64);

/// A column's identity within a Parquet file: the leaf-column ordinal. Mapping
/// to Iceberg field ids is a future refinement (#60).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnId(pub u32);

impl ColumnId {
    /// Reserved sentinel for an access with no column dimension: a whole-file
    /// read, or any read of a row-oriented data file (Avro/ORC) that has no
    /// column layout (the Avro addendum). Heat still attaches to `(table,
    /// partition, WHOLE_FILE)` so it carries across compaction — including a
    /// cross-format rewrite, where partition identity is the carrier. A real
    /// leaf ordinal never reaches `u32::MAX`, so the sentinel cannot collide.
    pub const WHOLE_FILE: ColumnId = ColumnId(u32::MAX);
}

/// The physical format of a data file, taken from the manifest entry's
/// `file_format` — never inferred by trying to parse the bytes (the addendum's
/// first-class-Avro rule).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FileFormat {
    /// Columnar Parquet: has a footer and per-column-chunk byte ranges.
    Parquet,
    /// Row-oriented Avro object container: no footer, no column layout.
    Avro,
    /// Columnar ORC: has stripes, but column-chunk resolution is not
    /// implemented (v1 treats it like Avro for classification: whole-file).
    Orc,
}

impl FileFormat {
    /// Parses an Iceberg `file_format` string (`PARQUET` / `AVRO` / `ORC`),
    /// case-insensitively.
    pub fn parse(s: &str) -> Option<FileFormat> {
        match s.to_ascii_uppercase().as_str() {
            "PARQUET" => Some(FileFormat::Parquet),
            "AVRO" => Some(FileFormat::Avro),
            "ORC" => Some(FileFormat::Orc),
            _ => None,
        }
    }

    /// Whether this format carries a per-column-chunk byte layout the mapper
    /// can resolve a range against. Parquet only.
    fn has_column_chunks(self) -> bool {
        matches!(self, FileFormat::Parquet)
    }
}

/// A contiguous byte span of one column chunk within a data file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkSpan {
    /// Inclusive start offset of the chunk in the file.
    pub start: u64,
    /// Exclusive end offset of the chunk in the file.
    pub end: u64,
    /// Which column this chunk holds.
    pub column: ColumnId,
    /// Which row group this chunk belongs to.
    pub row_group: u32,
}

/// A reference to one column chunk: the answer a range → chunk resolution
/// yields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnChunkRef {
    /// The column the range fell in.
    pub column: ColumnId,
    /// The row group the range fell in.
    pub row_group: u32,
}

/// What kind of table-metadata object a key names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaKind {
    /// A `metadata.json` document.
    MetadataJson,
    /// A manifest-list Avro object.
    ManifestList,
    /// A manifest Avro object.
    Manifest,
    /// A read of a Parquet data file's footer region (tail bytes past the last
    /// column chunk). Classified as metadata so #50 pins it in the metadata
    /// store rather than aging it out as cold data.
    Footer {
        /// The data file whose footer was read.
        file: FileId,
    },
    /// Some other object under the table's `metadata/` prefix the mapper has
    /// not individually catalogued (statistics, Puffin, version-hint, …).
    Other,
}

/// The result of classifying an object key (+ optional byte range).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Classify {
    /// Not under any watched table.
    Unknown,
    /// A table-metadata object (or a footer range).
    TableMetadata {
        /// The owning table.
        table: TableId,
        /// What metadata it is.
        kind: MetaKind,
    },
    /// A table data file, optionally resolved to a column chunk.
    TableData {
        /// The owning table.
        table: TableId,
        /// The data file.
        file: FileId,
        /// The column chunk the range fell in, if a Parquet range resolved;
        /// `None` for a whole-file read, an Avro/ORC file, an unparsed footer,
        /// or a range not covered by any chunk (callers degrade gracefully).
        chunk: Option<ColumnChunkRef>,
    },
}

/// Errors building or refreshing the map. `classify()` never errors — these
/// arise only on the (background, off-hot-path) build side.
#[derive(Debug, thiserror::Error)]
pub enum MapError {
    /// A metadata fetch failed.
    #[error(transparent)]
    Fetch(#[from] crate::fetch::FetchError),
    /// An Avro object (manifest list / manifest) failed to decode.
    #[error("avro decode failed for {object}: {detail}")]
    Avro {
        /// The object that failed.
        object: String,
        /// Decoder detail.
        detail: String,
    },
    /// A Parquet footer failed to decode.
    #[error("parquet footer decode failed for {object}: {detail}")]
    Parquet {
        /// The object that failed.
        object: String,
        /// Decoder detail.
        detail: String,
    },
    /// A metadata object was well-formed for its container but missing a field
    /// or otherwise not what the mapper expects.
    #[error("malformed metadata {object}: {detail}")]
    Malformed {
        /// The object that was malformed.
        object: String,
        /// What was wrong.
        detail: String,
    },
}

/// Per-file identity plus the lazily-parsed column-chunk layout.
///
/// Shared as an `Arc` across snapshots and across rebuilds: an unchanged file
/// keeps the same `FileMeta` (hence the same parsed `chunks`) when the table
/// index is rebuilt, so a footer is parsed at most once per file version.
pub struct FileMeta {
    /// Stable file identity.
    pub file_id: FileId,
    /// Interned object key.
    pub path: Arc<str>,
    /// Size in bytes from the manifest.
    pub size: u64,
    /// Physical format from the manifest.
    pub format: FileFormat,
    /// Partition tuple as `(field, value)` pairs (empty if unpartitioned).
    pub partition: Arc<[(String, Option<String>)]>,
    /// Column-chunk spans, sorted by start offset. Filled lazily off the hot
    /// path (footer parse); absent until then. `classify()` only ever reads
    /// this via [`OnceLock::get`] — it never parses.
    chunks: OnceLock<Arc<Vec<ChunkSpan>>>,
}

impl FileMeta {
    /// Builds an unparsed file record (no column chunks yet).
    fn new(
        file_id: FileId,
        path: Arc<str>,
        size: u64,
        format: FileFormat,
        partition: Arc<[(String, Option<String>)]>,
    ) -> FileMeta {
        FileMeta {
            file_id,
            path,
            size,
            format,
            partition,
            chunks: OnceLock::new(),
        }
    }

    /// The parsed column-chunk spans, if a footer parse has populated them.
    pub fn chunks(&self) -> Option<&Arc<Vec<ChunkSpan>>> {
        self.chunks.get()
    }

    /// Installs parsed column-chunk spans. Idempotent: a second call is
    /// ignored (the footer is immutable). Off the hot path.
    pub fn set_chunks(&self, spans: Arc<Vec<ChunkSpan>>) {
        let _ = self.chunks.set(spans);
    }

    /// Resolves a byte `range` to a column chunk. Returns `None` unless the
    /// file is Parquet, its footer is parsed, and the range starts inside a
    /// chunk. No allocation — one binary search.
    fn resolve_chunk(&self, range: &Range<u64>) -> Option<ColumnChunkRef> {
        if !self.format.has_column_chunks() {
            return None;
        }
        let spans = self.chunks.get()?;
        let idx = spans.partition_point(|span| span.start <= range.start);
        let span = spans.get(idx.checked_sub(1)?)?;
        if range.start < span.end {
            Some(ColumnChunkRef {
                column: span.column,
                row_group: span.row_group,
            })
        } else {
            None
        }
    }

    /// Whether `range` starts at or past the end of the last column chunk —
    /// i.e. in the file's footer region. Used to classify footer reads as
    /// metadata. Returns false if the footer is not yet parsed.
    fn is_footer_range(&self, range: &Range<u64>) -> bool {
        if !self.format.has_column_chunks() {
            return false;
        }
        match self.chunks.get().and_then(|spans| spans.last()) {
            Some(last) => range.start >= last.end,
            None => false,
        }
    }
}

/// One snapshot's live file set. Retained for recent snapshots so time-travel
/// reads of them resolve.
pub struct FileSet {
    /// The snapshot id.
    pub snapshot_id: i64,
    /// The live data files in the snapshot.
    pub files: Vec<Arc<FileMeta>>,
}

/// The per-table lookup structures.
pub struct TableIndex {
    /// This table's stable id.
    pub table: TableId,
    /// Catalog identity, for reverse lookups / metrics.
    pub ident: Arc<TableIdent>,
    /// Immutable storage binding used to resolve this table's origin.
    pub storage_binding_id: Arc<str>,
    /// Origin bucket (verified on the hot path against the request bucket).
    pub bucket: Arc<str>,
    /// Table root as an object-key prefix with a trailing `/`
    /// (e.g. `warehouse/db/t/`). The longest-prefix match keys on this.
    prefix: Arc<str>,
    /// The `<root>/metadata/` prefix, for classifying uncatalogued metadata
    /// objects.
    metadata_prefix: Arc<str>,
    /// Object key → file identity, unioned over all retained snapshots (so a
    /// time-travel read of a retained snapshot's file still resolves).
    by_path: FxHashMap<Arc<str>, Arc<FileMeta>>,
    /// Object key → metadata kind for the catalogued metadata objects.
    meta_paths: FxHashMap<Arc<str>, MetaKind>,
    /// Recent snapshot lineage, oldest → newest, for time travel.
    snapshots: BTreeMap<i64, Arc<FileSet>>,
    /// The current snapshot id (`None` for a table with no snapshots).
    current_snapshot_id: Option<i64>,
}

impl TableIndex {
    /// The current snapshot id, if any.
    pub fn current_snapshot_id(&self) -> Option<i64> {
        self.current_snapshot_id
    }

    /// The retained snapshot ids, ascending.
    pub fn snapshot_ids(&self) -> Vec<i64> {
        self.snapshots.keys().copied().collect()
    }

    /// The file record for `key`, if this table indexes it (any retained
    /// snapshot). For the warmer and tests.
    pub fn file_by_path(&self, key: &str) -> Option<&Arc<FileMeta>> {
        self.by_path.get(key)
    }

    /// Number of distinct files indexed (union over retained snapshots).
    pub fn file_count(&self) -> usize {
        self.by_path.len()
    }
}

/// An immutable snapshot of the whole map: which prefix owns which table, and
/// each table's index. Swapped atomically; never mutated in place.
pub struct MapperState {
    /// `(table prefix, id)` sorted ascending by prefix for longest-prefix
    /// binary search.
    table_prefixes: Vec<(Arc<str>, TableId)>,
    /// Id → table index.
    tables: FxHashMap<TableId, Arc<TableIndex>>,
}

impl MapperState {
    /// The empty state: no tables, everything classifies as `Unknown`.
    fn empty() -> MapperState {
        MapperState {
            table_prefixes: Vec::new(),
            tables: FxHashMap::default(),
        }
    }

    /// Number of tables in this state.
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// The index for a table id, if present.
    pub fn table(&self, id: TableId) -> Option<&Arc<TableIndex>> {
        self.tables.get(&id)
    }

    /// Classifies `(bucket, key, range)` against this immutable state.
    ///
    /// Hot path: one longest-prefix binary search, one hash lookup, and — for a
    /// Parquet range — one more binary search. No lock, no allocation.
    pub fn classify(&self, bucket: &str, key: &str, range: Option<Range<u64>>) -> Classify {
        // Longest-prefix match: greatest prefix `<= key` that is a prefix.
        let idx = self
            .table_prefixes
            .partition_point(|(p, _)| p.as_ref() <= key);
        let Some((prefix, table_id)) = idx.checked_sub(1).map(|i| &self.table_prefixes[i]) else {
            return Classify::Unknown;
        };
        if !key.starts_with(prefix.as_ref()) {
            return Classify::Unknown;
        }
        let Some(index) = self.tables.get(table_id) else {
            return Classify::Unknown;
        };
        // A shared key prefix across two buckets is pathological but possible;
        // verify the bucket before attributing.
        if index.bucket.as_ref() != bucket {
            return Classify::Unknown;
        }
        let table = *table_id;

        // Catalogued metadata object?
        if let Some(kind) = index.meta_paths.get(key) {
            return Classify::TableMetadata { table, kind: *kind };
        }

        // Known data file?
        if let Some(file) = index.by_path.get(key) {
            if let Some(range) = range.as_ref() {
                if let Some(chunk) = file.resolve_chunk(range) {
                    return Classify::TableData {
                        table,
                        file: file.file_id,
                        chunk: Some(chunk),
                    };
                }
                if file.is_footer_range(range) {
                    return Classify::TableMetadata {
                        table,
                        kind: MetaKind::Footer { file: file.file_id },
                    };
                }
            }
            return Classify::TableData {
                table,
                file: file.file_id,
                chunk: None,
            };
        }

        // Under the table prefix but not individually catalogued. A metadata
        // object we have not enumerated is still table metadata; anything else
        // (e.g. a data file the index has not caught up to) we decline to
        // attribute rather than fabricate an identity.
        if key.starts_with(index.metadata_prefix.as_ref()) {
            Classify::TableMetadata {
                table,
                kind: MetaKind::Other,
            }
        } else {
            Classify::Unknown
        }
    }
}

/// The mapper: an atomically-swappable [`MapperState`] plus the memoized footer
/// cache and writer-side id bookkeeping. Cheap to share (`Arc<Mapper>`); reads
/// are lock-free, builds are single-writer.
pub struct Mapper {
    /// The current immutable state; loaded lock-free on every classify.
    state: ArcSwap<MapperState>,
    /// Memoized Parquet footer parses, keyed by `(path, etag)`.
    footer_cache: Arc<crate::parquet_meta::FooterCache>,
    /// Writer-only id bookkeeping. Never touched by `classify()`, so the hot
    /// path stays lock-free.
    ids: std::sync::Mutex<IdState>,
}

/// Writer-side id allocation state (single-writer; guarded only so `Mapper` is
/// `Sync`).
struct IdState {
    /// Next table id to hand out.
    next_table_id: u32,
    /// Next file id to hand out.
    next_file_id: u64,
    /// Assigned table ids, so a rebuild keeps a table's id stable.
    idents: std::collections::HashMap<TableIdent, TableId>,
}

impl Default for Mapper {
    /// An empty mapper.
    fn default() -> Mapper {
        Mapper::new()
    }
}

impl Mapper {
    /// A fresh mapper with no tables.
    pub fn new() -> Mapper {
        Mapper {
            state: ArcSwap::from_pointee(MapperState::empty()),
            footer_cache: Arc::new(crate::parquet_meta::FooterCache::new()),
            ids: std::sync::Mutex::new(IdState {
                next_table_id: 0,
                next_file_id: 0,
                idents: std::collections::HashMap::new(),
            }),
        }
    }

    /// Classifies an object key and optional byte range. Lock-free, no
    /// allocation — the request hot path.
    pub fn classify(&self, bucket: &str, key: &str, range: Option<Range<u64>>) -> Classify {
        self.state.load().classify(bucket, key, range)
    }

    /// Loads the current immutable state (for tests / introspection). The
    /// returned guard keeps the state alive; cheap and lock-free.
    pub fn state(&self) -> arc_swap::Guard<Arc<MapperState>> {
        self.state.load()
    }

    /// The table id assigned to `ident`, if the mapper has seen it.
    pub fn table_id(&self, ident: &TableIdent) -> Option<TableId> {
        self.ids
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .idents
            .get(ident)
            .copied()
    }
}
