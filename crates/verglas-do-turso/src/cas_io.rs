//! Turso virtual files backed by immutable object deltas and one CAS head.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use turso_core::io::clock::{DefaultClock, MonotonicInstant, WallClockInstant};
use turso_core::io::{FileId, FileSyncType};
use turso_core::{Buffer, Clock, Completion, CompletionError, File, IO, OpenFlags};
use verglas_backend::{BackendStores, MultipartObjectStore};
use verglas_cache::HybridCacheEngine;
use verglas_core::CacheKey;
use verglas_core::read::{ObjectRead, ReadRange};

use crate::{Error, Result};

const PAGE_BYTES: u64 = 4096;
const DATABASE_PATH: &str = "turso.db";

/// One DO's remote Turso authority and its shared Foyer cache capability.
#[derive(Clone)]
pub struct TursoCasStorage {
    store: Arc<dyn MultipartObjectStore>,
    cache: HybridCacheEngine,
    binding: Arc<str>,
    bucket: Arc<str>,
    prefix: Arc<str>,
}

impl TursoCasStorage {
    /// Fixes one backend binding, one per-DO bucket, and one object prefix.
    pub fn new(
        stores: Arc<dyn BackendStores>,
        cache: HybridCacheEngine,
        binding: impl Into<String>,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Self> {
        let binding = binding.into();
        let bucket = bucket.into();
        let prefix = prefix.into().trim_matches('/').to_owned();
        if binding.is_empty() || bucket.is_empty() || prefix.is_empty() {
            return Err(Error::StorageConfiguration(
                "binding, per-DO bucket, and Turso prefix must be non-empty".to_owned(),
            ));
        }
        let store = stores
            .store_for(&binding, &bucket)
            .map_err(|error| Error::StorageConfiguration(error.to_string()))?;
        Ok(Self {
            store,
            cache,
            binding: Arc::from(binding),
            bucket: Arc::from(bucket),
            prefix: Arc::from(prefix),
        })
    }

    /// Opens the virtual files after reconciling the Foyer cursor with S3 head.
    pub(crate) async fn open_io(&self) -> Result<(Arc<CasIo>, RecoveryStats)> {
        CasIo::recover(self.clone()).await
    }
}

/// Observable wake work, measured in immutable deltas rather than database bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryStats {
    /// Generation carried by the reusable Foyer cursor.
    pub local_generation: u64,
    /// Generation read from the authoritative object-store head.
    pub remote_generation: u64,
    /// Contiguous immutable segments applied after the local generation.
    pub applied_segments: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct MaterializedState {
    generation: u64,
    files: BTreeMap<String, MaterializedFile>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct MaterializedFile {
    size: u64,
    blocks: BTreeMap<u64, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Delta {
    generation: u64,
    parent: Option<String>,
    files: BTreeMap<String, FileDelta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileDelta {
    size: u64,
    blocks: BTreeMap<u64, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Head {
    generation: u64,
    delta: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalCursor {
    state: MaterializedState,
    delta: Option<String>,
}

#[derive(Debug, Default)]
struct LiveFile {
    materialized: MaterializedFile,
    dirty: BTreeMap<u64, Vec<u8>>,
    metadata_dirty: bool,
    gate: Arc<tokio::sync::Mutex<()>>,
}

struct LiveState {
    generation: u64,
    delta: Option<String>,
    head_version: Option<UpdateVersion>,
    files: HashMap<String, Arc<Mutex<LiveFile>>>,
}

/// The object-backed IO implementation installed into Turso's builder.
pub(crate) struct CasIo {
    storage: TursoCasStorage,
    state: Arc<Mutex<LiveState>>,
    commit: Arc<tokio::sync::Mutex<()>>,
    conflict: Arc<RwLock<Option<String>>>,
}

impl CasIo {
    async fn recover(storage: TursoCasStorage) -> Result<(Arc<Self>, RecoveryStats)> {
        let head_path = storage.object_path("head.json");
        let remote = match storage.store.get(&head_path).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let bytes = result.bytes().await.map_err(Error::ObjectStore)?;
                let head: Head = serde_json::from_slice(&bytes)?;
                Some((head, version))
            }
            Err(object_store::Error::NotFound { .. }) => None,
            Err(error) => return Err(Error::ObjectStore(error)),
        };
        let cursor_key = storage.cursor_key();
        let cursor = match storage.cache.local_state(&cursor_key).await {
            Some(bytes) => serde_json::from_slice::<LocalCursor>(&bytes).ok(),
            None => None,
        };
        let remote_generation = remote.as_ref().map_or(0, |(head, _)| head.generation);
        let mut materialized = cursor
            .as_ref()
            .filter(|cursor| cursor.state.generation <= remote_generation)
            .map(|cursor| cursor.state.clone())
            .unwrap_or_default();
        let local_generation = materialized.generation;
        let mut newest_delta = cursor.as_ref().and_then(|cursor| cursor.delta.clone());
        let mut missing = Vec::new();
        if let Some((head, _)) = &remote {
            let mut key = Some(head.delta.clone());
            while let Some(delta_key) = key {
                let delta = storage.read_delta(&delta_key).await?;
                if delta.generation <= materialized.generation {
                    break;
                }
                key = delta.parent.clone();
                missing.push((delta_key, delta));
            }
            missing.reverse();
            for (key, delta) in &missing {
                apply_delta(&mut materialized, delta)?;
                newest_delta = Some(key.clone());
            }
            if materialized.generation != head.generation {
                return Err(Error::Recovery(format!(
                    "CAS head generation {} is not a contiguous successor of cached generation {}",
                    head.generation, local_generation
                )));
            }
        }
        let files = materialized
            .files
            .iter()
            .map(|(path, file)| {
                (
                    path.clone(),
                    Arc::new(Mutex::new(LiveFile {
                        materialized: file.clone(),
                        dirty: BTreeMap::new(),
                        metadata_dirty: false,
                        gate: Arc::new(tokio::sync::Mutex::new(())),
                    })),
                )
            })
            .collect();
        let io = Arc::new(Self {
            storage,
            state: Arc::new(Mutex::new(LiveState {
                generation: materialized.generation,
                delta: newest_delta,
                head_version: remote.map(|(_, version)| version),
                files,
            })),
            commit: Arc::new(tokio::sync::Mutex::new(())),
            conflict: Arc::new(RwLock::new(None)),
        });
        io.persist_cursor();
        Ok((
            io,
            RecoveryStats {
                local_generation,
                remote_generation,
                applied_segments: missing.len() as u64,
            },
        ))
    }

    fn file(&self, path: &str) -> Arc<Mutex<LiveFile>> {
        let mut state = lock(&self.state);
        state
            .files
            .entry(path.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(LiveFile::default())))
            .clone()
    }

    async fn read_block(&self, path: &str, index: u64) -> Result<Vec<u8>> {
        let file = self.file(path);
        let object = {
            let file = lock(&file);
            if let Some(bytes) = file.dirty.get(&index) {
                return Ok(bytes.clone());
            }
            file.materialized.blocks.get(&index).cloned()
        };
        match object {
            Some(object) => {
                let key = self.storage.cache_key(&object);
                let response = self
                    .storage
                    .cache
                    .get(&key, ReadRange::Full)
                    .await
                    .map_err(|error| Error::Recovery(error.to_string()))?;
                let chunks = response
                    .body
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(|error| Error::Recovery(error.to_string()))?;
                let mut bytes = Vec::with_capacity(PAGE_BYTES as usize);
                for chunk in chunks {
                    bytes.extend_from_slice(&chunk);
                }
                bytes.resize(PAGE_BYTES as usize, 0);
                Ok(bytes)
            }
            None => Ok(vec![0; PAGE_BYTES as usize]),
        }
    }

    async fn commit_delta(&self) -> Result<()> {
        let _commit = self.commit.lock().await;
        let (generation, parent, head_version, files) = {
            let state = lock(&self.state);
            let files = state
                .files
                .iter()
                .filter_map(|(path, file)| {
                    let file = lock(file);
                    (!file.dirty.is_empty() || file.metadata_dirty)
                        .then(|| (path.clone(), file.materialized.size, file.dirty.clone()))
                })
                .collect::<Vec<_>>();
            (
                state.generation.saturating_add(1),
                state.delta.clone(),
                state.head_version.clone(),
                files,
            )
        };
        if files.is_empty() {
            return Ok(());
        }
        let mut delta_files = BTreeMap::new();
        for (path, size, blocks) in &files {
            let mut uploaded = BTreeMap::new();
            for (index, bytes) in blocks {
                let digest = hex::encode(Sha256::digest(bytes));
                let key = format!("blocks/{digest}");
                self.storage
                    .put_immutable(&key, Bytes::copy_from_slice(bytes))
                    .await?;
                uploaded.insert(*index, key);
            }
            delta_files.insert(
                path.clone(),
                FileDelta {
                    size: *size,
                    blocks: uploaded,
                },
            );
        }
        let delta = Delta {
            generation,
            parent,
            files: delta_files,
        };
        let delta_bytes = serde_json::to_vec(&delta)?;
        let delta_key = format!(
            "deltas/{generation:020}-{}.json",
            hex::encode(Sha256::digest(&delta_bytes))
        );
        self.storage
            .put_immutable(&delta_key, Bytes::from(delta_bytes))
            .await?;
        let head = serde_json::to_vec(&Head {
            generation,
            delta: delta_key.clone(),
        })?;
        let mode = head_version.map_or(PutMode::Create, PutMode::Update);
        let result = self
            .storage
            .store
            .put_opts(
                &self.storage.object_path("head.json"),
                Bytes::from(head).into(),
                PutOptions {
                    mode,
                    ..PutOptions::default()
                },
            )
            .await;
        let next_head_version = match result {
            Ok(result) => result.into(),
            Err(
                error @ (object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. }),
            ) => {
                let message = format!("DO head CAS rejected at generation {generation}: {error}");
                *write_lock(&self.conflict) = Some(message.clone());
                return Err(Error::Conflict(message));
            }
            Err(error) => {
                // A timed-out conditional PUT may have reached S3. Re-read the
                // uncached pointer before deciding whether it failed; blindly
                // retrying a CAS can fork the lineage.
                match self
                    .storage
                    .store
                    .get(&self.storage.object_path("head.json"))
                    .await
                {
                    Ok(readback) => {
                        let version = UpdateVersion {
                            e_tag: readback.meta.e_tag.clone(),
                            version: readback.meta.version.clone(),
                        };
                        let bytes = readback.bytes().await.map_err(Error::ObjectStore)?;
                        let observed: Head = serde_json::from_slice(&bytes)?;
                        if observed.generation == generation && observed.delta == delta_key {
                            version
                        } else {
                            return Err(Error::ObjectStore(error));
                        }
                    }
                    Err(_) => return Err(Error::ObjectStore(error)),
                }
            }
        };
        {
            let mut state = lock(&self.state);
            for (path, _, dirty) in files {
                if let Some(file) = state.files.get(&path) {
                    let mut file = lock(file);
                    for index in dirty.keys() {
                        let key = delta.files[&path].blocks[index].clone();
                        file.materialized.blocks.insert(*index, key);
                        file.dirty.remove(index);
                    }
                    file.metadata_dirty = false;
                }
            }
            state.generation = generation;
            state.delta = Some(delta_key);
            state.head_version = Some(next_head_version);
        }
        self.persist_cursor();
        Ok(())
    }

    fn persist_cursor(&self) {
        let state = lock(&self.state);
        let files = state
            .files
            .iter()
            .map(|(path, file)| (path.clone(), lock(file).materialized.clone()))
            .collect();
        let cursor = LocalCursor {
            state: MaterializedState {
                generation: state.generation,
                files,
            },
            delta: state.delta.clone(),
        };
        if let Ok(bytes) = serde_json::to_vec(&cursor) {
            self.storage
                .cache
                .put_local_state(self.storage.cursor_key(), Bytes::from(bytes));
        }
    }

    pub(crate) fn take_conflict(&self) -> Option<String> {
        write_lock(&self.conflict).take()
    }
}

impl TursoCasStorage {
    fn object_path(&self, suffix: &str) -> ObjectPath {
        ObjectPath::from(format!("{}/{suffix}", self.prefix))
    }

    fn cache_key(&self, suffix: &str) -> CacheKey {
        CacheKey {
            storage_binding_id: self.binding.to_string(),
            bucket: self.bucket.to_string(),
            key: format!("{}/{suffix}", self.prefix),
        }
    }

    fn cursor_key(&self) -> CacheKey {
        CacheKey {
            storage_binding_id: self.binding.to_string(),
            bucket: self.bucket.to_string(),
            key: format!("{}/local-cursor", self.prefix),
        }
    }

    async fn read_delta(&self, key: &str) -> Result<Delta> {
        let response = self
            .cache
            .get(&self.cache_key(key), ReadRange::Full)
            .await
            .map_err(|error| Error::Recovery(error.to_string()))?;
        let chunks = response
            .body
            .try_collect::<Vec<_>>()
            .await
            .map_err(|error| Error::Recovery(error.to_string()))?;
        let bytes = chunks.concat();
        Ok(serde_json::from_slice(&bytes)?)
    }

    async fn put_immutable(&self, key: &str, bytes: Bytes) -> Result<()> {
        match self
            .store
            .put_opts(
                &self.object_path(key),
                bytes.clone().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self
                    .store
                    .get(&self.object_path(key))
                    .await
                    .map_err(Error::ObjectStore)?
                    .bytes()
                    .await
                    .map_err(Error::ObjectStore)?;
                if existing == bytes {
                    Ok(())
                } else {
                    Err(Error::Recovery(format!(
                        "immutable CAS object `{key}` already exists with different bytes"
                    )))
                }
            }
            Err(error) => Err(Error::ObjectStore(error)),
        }
    }
}

impl Clock for CasIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        DefaultClock.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        DefaultClock.current_time_wall_clock()
    }
}

impl IO for CasIo {
    fn open_file(
        &self,
        path: &str,
        _flags: OpenFlags,
        _direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        let path = if path.ends_with("-wal") {
            format!("{DATABASE_PATH}-wal")
        } else if path.ends_with(DATABASE_PATH) {
            DATABASE_PATH.to_owned()
        } else {
            path.to_owned()
        };
        let file = self.file(&path);
        Ok(Arc::new(CasFile {
            path,
            io: Arc::new(self.clone_handle()),
            file,
        }))
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        lock(&self.state).files.remove(path);
        Ok(())
    }

    fn file_id(&self, path: &str) -> turso_core::Result<FileId> {
        Ok(FileId::from_path_hash(path))
    }
}

impl CasIo {
    fn clone_handle(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            state: Arc::clone(&self.state),
            commit: Arc::clone(&self.commit),
            conflict: Arc::clone(&self.conflict),
        }
    }
}

struct CasFile {
    path: String,
    io: Arc<CasIo>,
    file: Arc<Mutex<LiveFile>>,
}

impl File for CasFile {
    fn lock_file(&self, _exclusive: bool) -> turso_core::Result<()> {
        Ok(())
    }
    fn unlock_file(&self) -> turso_core::Result<()> {
        Ok(())
    }

    fn pread(&self, pos: u64, completion: Completion) -> turso_core::Result<Completion> {
        let io = Arc::clone(&self.io);
        let path = self.path.clone();
        let returned = completion.clone();
        spawn_io(async move {
            let gate = { Arc::clone(&lock(&io.file(&path)).gate) };
            let _operation = gate.lock().await;
            let destination = completion.as_read().buf();
            let length = destination.len();
            let mut written = 0usize;
            while written < length {
                let absolute = pos + written as u64;
                let index = absolute / PAGE_BYTES;
                let offset = (absolute % PAGE_BYTES) as usize;
                match io.read_block(&path, index).await {
                    Ok(block) => {
                        let count = (length - written).min(block.len() - offset);
                        destination.as_mut_slice()[written..written + count]
                            .copy_from_slice(&block[offset..offset + count]);
                        written += count;
                    }
                    Err(_) => {
                        completion.error(CompletionError::IOError(
                            std::io::ErrorKind::Other,
                            "s3 read",
                        ));
                        return;
                    }
                }
            }
            completion.complete(written as i32);
        });
        Ok(returned)
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<Buffer>,
        completion: Completion,
    ) -> turso_core::Result<Completion> {
        let io = Arc::clone(&self.io);
        let path = self.path.clone();
        let bytes = buffer.as_slice().to_vec();
        let returned = completion.clone();
        spawn_io(async move {
            let gate = { Arc::clone(&lock(&io.file(&path)).gate) };
            let _operation = gate.lock().await;
            let mut consumed = 0usize;
            while consumed < bytes.len() {
                let absolute = pos + consumed as u64;
                let index = absolute / PAGE_BYTES;
                let offset = (absolute % PAGE_BYTES) as usize;
                let count = (bytes.len() - consumed).min(PAGE_BYTES as usize - offset);
                let mut block = match io.read_block(&path, index).await {
                    Ok(block) => block,
                    Err(_) => {
                        completion.error(CompletionError::IOError(
                            std::io::ErrorKind::Other,
                            "s3 read before write",
                        ));
                        return;
                    }
                };
                block[offset..offset + count].copy_from_slice(&bytes[consumed..consumed + count]);
                let file = io.file(&path);
                let mut file = lock(&file);
                file.dirty.insert(index, block);
                file.materialized.size = file.materialized.size.max(absolute + count as u64);
                consumed += count;
            }
            completion.complete(bytes.len() as i32);
        });
        Ok(returned)
    }

    fn sync(
        &self,
        completion: Completion,
        _sync_type: FileSyncType,
    ) -> turso_core::Result<Completion> {
        let io = Arc::clone(&self.io);
        let returned = completion.clone();
        spawn_io(async move {
            match io.commit_delta().await {
                Ok(()) => completion.complete(0),
                Err(_) => completion.error(CompletionError::IOError(
                    std::io::ErrorKind::Other,
                    "s3 CAS",
                )),
            }
        });
        Ok(returned)
    }

    fn size(&self) -> turso_core::Result<u64> {
        Ok(lock(&self.file).materialized.size)
    }

    fn truncate(&self, len: u64, completion: Completion) -> turso_core::Result<Completion> {
        let mut file = lock(&self.file);
        file.materialized.size = len;
        file.metadata_dirty = true;
        let blocks = len.div_ceil(PAGE_BYTES);
        file.materialized.blocks.retain(|index, _| *index < blocks);
        file.dirty.retain(|index, _| *index < blocks);
        completion.complete(0);
        Ok(completion)
    }
}

fn apply_delta(state: &mut MaterializedState, delta: &Delta) -> Result<()> {
    if delta.generation != state.generation.saturating_add(1) {
        return Err(Error::Recovery(format!(
            "non-contiguous Turso delta: cached {}, next {}",
            state.generation, delta.generation
        )));
    }
    for (path, changed) in &delta.files {
        let file = state.files.entry(path.clone()).or_default();
        file.size = changed.size;
        file.blocks.extend(changed.blocks.clone());
        let blocks = file.size.div_ceil(PAGE_BYTES);
        file.blocks.retain(|index, _| *index < blocks);
    }
    state.generation = delta.generation;
    Ok(())
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Runs completion work on the host runtime that owns the shared Foyer engine.
fn spawn_io(future: impl std::future::Future<Output = ()> + Send + 'static) {
    tokio::spawn(future);
}
