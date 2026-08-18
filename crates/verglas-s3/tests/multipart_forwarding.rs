//! Multipart part-boundary forwarding contract (origin-store production incident,
//! 2026-08-02): the origin store requires every non-trailing part of a multipart
//! upload to be byte-identical in length, so the passthrough must (a) forward
//! each client UploadPart as exactly one backend part — same number, same
//! bytes — and (b) split an internally-streamed PUT body into exact-size parts
//! with no per-part overshoot. A recording backend store observes exactly what
//! the origin would receive.

use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartId, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use verglas_core::read::Checksums;
use verglas_s3::{
    BackendStore, CacheKey, CompletedPartRef, ObjectWrite, PassthroughWrite, WriteBodyStream,
    WriteChecksum, WriteMetadata,
};

/// The bucket the recording in-memory origin serves.
const BUCKET: &str = "part-bucket";

/// One backend part exactly as the origin received it.
#[derive(Clone, Debug)]
struct RecordedPart {
    /// Upload the part belongs to.
    upload_id: String,
    /// Zero-based part index (`object_store`'s form; S3 part number minus 1).
    part_idx: usize,
    /// The part's exact bytes.
    bytes: Bytes,
    /// ETag the backend assigned to this part.
    e_tag: String,
}

/// One completion exactly as the origin received it: the part ETags in
/// positional order.
#[derive(Clone)]
struct RecordedCompletion {
    /// Upload that was completed.
    upload_id: String,
    /// The manifest's part ETags, positional (index i = part number i+1).
    part_etags: Vec<String>,
}

/// An in-memory origin that records every multipart call the passthrough
/// makes, so tests can assert on the exact parts the backend received.
struct RecordingStore {
    /// The real in-memory store the calls delegate to.
    inner: Arc<InMemory>,
    /// Every backend part in arrival order — from the low-level
    /// `MultipartStore::put_part` and the high-level upload handle alike.
    parts: Arc<Mutex<Vec<RecordedPart>>>,
    /// Every `complete_multipart` in arrival order.
    completions: Mutex<Vec<RecordedCompletion>>,
}

/// Wraps the in-memory store's high-level upload handle, recording each part
/// exactly as it is handed to the backend.
#[derive(Debug)]
struct RecordingUpload {
    /// The real upload handle.
    inner: Box<dyn MultipartUpload>,
    /// Shared recording sink (same vec as the low-level path).
    parts: Arc<Mutex<Vec<RecordedPart>>>,
    /// Sequential part index for this upload.
    next_idx: usize,
}

#[async_trait]
impl MultipartUpload for RecordingUpload {
    /// Records the part's exact bytes, then delegates the upload.
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        let idx = self.next_idx;
        self.next_idx += 1;
        self.parts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(RecordedPart {
                upload_id: "streamed".to_owned(),
                part_idx: idx,
                bytes: Bytes::from(data.clone()),
                e_tag: String::new(),
            });
        self.inner.put_part(data)
    }

    /// Delegates completion.
    async fn complete(&mut self) -> Result<PutResult> {
        self.inner.complete().await
    }

    /// Delegates abort.
    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

impl RecordingStore {
    /// Wraps a fresh in-memory origin with empty recordings.
    fn new() -> Self {
        RecordingStore {
            inner: Arc::new(InMemory::new()),
            parts: Arc::new(Mutex::new(Vec::new())),
            completions: Mutex::new(Vec::new()),
        }
    }

    /// Snapshot of the recorded parts.
    fn parts(&self) -> Vec<RecordedPart> {
        self.parts.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Snapshot of the recorded completions.
    fn completions(&self) -> Vec<RecordedCompletion> {
        self.completions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

impl fmt::Debug for RecordingStore {
    /// Names the wrapper; the recordings are test state, not display state.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordingStore")
    }
}

impl fmt::Display for RecordingStore {
    /// Names the wrapper for `object_store` error text.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for RecordingStore {
    /// Delegates a whole-object PUT.
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    /// Wraps the high-level streaming upload (the path the passthrough's
    /// internal PUT split drives) so each of its parts is recorded exactly as
    /// the backend receives it.
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(RecordingUpload {
            inner,
            parts: self.parts.clone(),
            next_idx: 0,
        }))
    }

    /// Delegates a GET.
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    /// Delegates streamed deletes.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    /// Delegates a listing.
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    /// Delegates a delimited listing.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    /// Delegates a server-side copy.
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[async_trait]
impl MultipartStore for RecordingStore {
    /// Delegates upload creation.
    async fn create_multipart(&self, path: &Path) -> Result<MultipartId> {
        self.inner.create_multipart(path).await
    }

    /// Delegates upload creation with options.
    async fn create_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> Result<MultipartId> {
        self.inner.create_multipart_opts(path, opts).await
    }

    /// Records the part exactly as received, then delegates it.
    async fn put_part(
        &self,
        path: &Path,
        id: &MultipartId,
        part_idx: usize,
        data: PutPayload,
    ) -> Result<PartId> {
        let bytes = Bytes::from(data.clone());
        let part = self.inner.put_part(path, id, part_idx, data).await?;
        self.parts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(RecordedPart {
                upload_id: id.clone(),
                part_idx,
                bytes,
                e_tag: part.content_id.clone(),
            });
        Ok(part)
    }

    /// Records the completion manifest, then delegates it.
    async fn complete_multipart(
        &self,
        path: &Path,
        id: &MultipartId,
        parts: Vec<PartId>,
    ) -> Result<PutResult> {
        self.completions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(RecordedCompletion {
                upload_id: id.clone(),
                part_etags: parts.iter().map(|p| p.content_id.clone()).collect(),
            });
        self.inner.complete_multipart(path, id, parts).await
    }

    /// Delegates an abort.
    async fn abort_multipart(&self, path: &Path, id: &MultipartId) -> Result<()> {
        self.inner.abort_multipart(path, id).await
    }
}

/// A recording origin and a passthrough writer over it.
fn writer_over_recording() -> (Arc<RecordingStore>, PassthroughWrite) {
    let store = Arc::new(RecordingStore::new());
    let writer = PassthroughWrite::new(BackendStore::single("default", BUCKET, store.clone()));
    (store, writer)
}

/// Deterministic test payload: byte `i` is `i % 251` (prime, so no part or
/// chunk alignment can mask an offset bug).
fn pattern(offset: u64, len: u64) -> Bytes {
    (offset..offset + len)
        .map(|i| (i % 251) as u8)
        .collect::<Vec<u8>>()
        .into()
}

/// A client body stream delivering `bytes` in the given chunk lengths
/// (which must sum to `bytes.len()`).
fn chunked_body(bytes: &Bytes, chunk_lens: &[usize]) -> WriteBodyStream {
    assert_eq!(
        chunk_lens.iter().sum::<usize>(),
        bytes.len(),
        "chunk schedule must cover the body exactly"
    );
    let mut chunks = Vec::with_capacity(chunk_lens.len());
    let mut offset = 0;
    for &len in chunk_lens {
        chunks.push(Ok(bytes.slice(offset..offset + len)));
        offset += len;
    }
    futures::stream::iter(chunks).boxed()
}

/// A single-chunk client body stream.
fn one_chunk_body(bytes: Bytes) -> WriteBodyStream {
    futures::stream::iter([Ok(bytes)]).boxed()
}

/// The cache key tests write under.
fn key(name: &str) -> CacheKey {
    CacheKey {
        storage_binding_id: "default".to_owned(),
        bucket: BUCKET.to_owned(),
        key: name.to_owned(),
    }
}

/// Reads an object's assembled bytes straight from the recording origin.
async fn stored_bytes(store: &RecordingStore, name: &str) -> Bytes {
    store
        .inner
        .get(&Path::from(name))
        .await
        .expect("assembled object")
        .bytes()
        .await
        .expect("assembled bytes")
}

/// REPRODUCTION (origin InvalidPart, 2026-08-02): a streamed PUT larger than one
/// part buffer is split into backend parts internally. Several S3-compatible origins require all
/// non-trailing parts of one upload to be byte-identical in length, so the
/// split must produce exact-size parts regardless of how the client's chunks
/// arrive — overshooting a part boundary by "whatever the last chunk brought"
/// produces variable part sizes and fails the whole upload at completion.
#[tokio::test]
async fn streamed_put_splits_into_uniform_parts() {
    let (store, writer) = writer_over_recording();

    // Irregular, non-repeating chunk sizes (~21 MiB total): under the buggy
    // split, part 1 and part 2 absorb different overshoot and differ in size.
    let chunk_lens: Vec<usize> = (0..14).map(|i| 1_000_000 + i * 77_777).collect();
    let total: usize = chunk_lens.iter().sum();
    let body = pattern(0, total as u64);

    writer
        .put(
            &key("layers/big-layer.bin"),
            WriteMetadata::default(),
            chunked_body(&body, &chunk_lens),
        )
        .await
        .expect("streamed PUT");

    let parts = store.parts();
    assert!(
        parts.len() >= 3,
        "a {total}-byte body must split into at least 3 parts, got {}",
        parts.len()
    );
    let first_len = parts[0].bytes.len();
    for (i, part) in parts.iter().enumerate() {
        assert_eq!(part.part_idx, i, "parts must arrive in order");
        if i + 1 < parts.len() {
            assert_eq!(
                part.bytes.len(),
                first_len,
                "non-trailing part {} has length {} but part 0 has {first_len}; \
                 the origin rejects the upload unless all non-trailing parts are equal",
                i + 1,
                part.bytes.len()
            );
        } else {
            assert!(
                part.bytes.len() <= first_len,
                "the trailing part may not exceed the uniform part size"
            );
            assert!(!part.bytes.is_empty(), "no empty trailing part");
        }
    }
    let reassembled: Vec<u8> = parts.iter().flat_map(|p| p.bytes.to_vec()).collect();
    assert_eq!(
        Bytes::from(reassembled),
        body,
        "the split must preserve the body byte-for-byte"
    );
    assert_eq!(
        stored_bytes(&store, "layers/big-layer.bin").await,
        body,
        "the assembled object must equal the client body"
    );
}

/// Client-driven multipart: N uniform parts plus a smaller trailing part must
/// reach the backend as exactly N+1 parts — same order, same part numbers,
/// byte-identical sizes and content. No re-chunking, merging, or splitting,
/// even for parts larger than the internal PUT split size.
#[tokio::test]
async fn client_parts_forward_one_to_one() {
    let (store, writer) = writer_over_recording();
    let k = key("layers/uniform.bin");

    let creation = writer
        .create_multipart(&k, WriteMetadata::default())
        .await
        .expect("create upload");

    // Three uniform 10 MiB parts (over the internal 8 MiB split size, so any
    // re-chunking would show) plus a 1 MiB trailing part.
    let part_size: u64 = 10 * 1024 * 1024;
    let trailing: u64 = 1024 * 1024;
    let sizes = [part_size, part_size, part_size, trailing];
    let mut client_etags = Vec::new();
    let mut offset = 0u64;
    for (i, &size) in sizes.iter().enumerate() {
        let bytes = pattern(offset, size);
        offset += size;
        // Deliver each part in several chunks, as a real client body arrives.
        let third = (size / 3) as usize;
        let lens = [third, third, size as usize - 2 * third];
        let uploaded = writer
            .upload_part(
                &k,
                &creation.upload_id,
                (i + 1) as u16,
                WriteChecksum::default(),
                chunked_body(&bytes, &lens),
            )
            .await
            .expect("upload part");
        client_etags.push(uploaded.e_tag);
    }

    let parts = store.parts();
    assert_eq!(
        parts.len(),
        sizes.len(),
        "the backend must receive exactly one part per client part"
    );
    let mut offset = 0u64;
    for (i, (part, &size)) in parts.iter().zip(sizes.iter()).enumerate() {
        assert_eq!(part.upload_id, creation.upload_id, "wrong upload");
        assert_eq!(part.part_idx, i, "part number must be preserved");
        assert_eq!(
            part.bytes.len() as u64,
            size,
            "part {} size must be exactly the client's",
            i + 1
        );
        assert_eq!(
            part.bytes,
            pattern(offset, size),
            "part {} bytes must be the client's, unaltered",
            i + 1
        );
        offset += size;
    }

    let manifest = client_etags
        .iter()
        .enumerate()
        .map(|(i, e_tag)| CompletedPartRef {
            part_number: (i + 1) as u16,
            e_tag: e_tag.clone(),
            checksums: Checksums::default(),
        })
        .collect();
    writer
        .complete_multipart(&k, &creation.upload_id, manifest, WriteChecksum::default())
        .await
        .expect("complete upload");

    let completions = store.completions();
    assert_eq!(completions.len(), 1, "exactly one backend completion");
    assert_eq!(completions[0].upload_id, creation.upload_id);
    let backend_etags: Vec<String> = parts.iter().map(|p| p.e_tag.clone()).collect();
    assert_eq!(
        completions[0].part_etags, backend_etags,
        "the completion must reference the backend's part ETags in part order"
    );
    assert_eq!(
        stored_bytes(&store, "layers/uniform.bin").await,
        pattern(0, sizes.iter().sum()),
        "the assembled object must equal the concatenated client parts"
    );
}

/// A single-part multipart upload forwards as exactly one backend part and
/// one completion referencing it.
#[tokio::test]
async fn single_part_multipart_forwards_one_part() {
    let (store, writer) = writer_over_recording();
    let k = key("layers/single.bin");

    let creation = writer
        .create_multipart(&k, WriteMetadata::default())
        .await
        .expect("create upload");
    // Over the internal 8 MiB split size, so a splitting bug would show.
    let size = 11 * 1024 * 1024;
    let bytes = pattern(0, size);
    let uploaded = writer
        .upload_part(
            &k,
            &creation.upload_id,
            1,
            WriteChecksum::default(),
            one_chunk_body(bytes.clone()),
        )
        .await
        .expect("upload part");
    writer
        .complete_multipart(
            &k,
            &creation.upload_id,
            vec![CompletedPartRef {
                part_number: 1,
                e_tag: uploaded.e_tag,
                checksums: Checksums::default(),
            }],
            WriteChecksum::default(),
        )
        .await
        .expect("complete upload");

    let parts = store.parts();
    assert_eq!(parts.len(), 1, "exactly one backend part");
    assert_eq!(parts[0].part_idx, 0);
    assert_eq!(parts[0].bytes, bytes, "single part bytes must be unaltered");
    let completions = store.completions();
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].part_etags, vec![parts[0].e_tag.clone()]);
    assert_eq!(stored_bytes(&store, "layers/single.bin").await, bytes);
}

/// A retried part (same part number uploaded again) forwards as another
/// backend upload of the SAME part number, and the completion assembles the
/// retry's bytes — never both attempts, never a shifted part.
#[tokio::test]
async fn retried_part_replaces_same_part_number() {
    let (store, writer) = writer_over_recording();
    let k = key("layers/retry.bin");

    let creation = writer
        .create_multipart(&k, WriteMetadata::default())
        .await
        .expect("create upload");
    let size: u64 = 6 * 1024 * 1024;
    let part_one = pattern(0, size);
    let first_attempt = pattern(1000, size);
    let retry = pattern(2000, size);

    writer
        .upload_part(
            &k,
            &creation.upload_id,
            1,
            WriteChecksum::default(),
            one_chunk_body(part_one.clone()),
        )
        .await
        .expect("part 1");
    writer
        .upload_part(
            &k,
            &creation.upload_id,
            2,
            WriteChecksum::default(),
            one_chunk_body(first_attempt),
        )
        .await
        .expect("part 2, first attempt");
    let retried = writer
        .upload_part(
            &k,
            &creation.upload_id,
            2,
            WriteChecksum::default(),
            one_chunk_body(retry.clone()),
        )
        .await
        .expect("part 2, retry");

    let parts = store.parts();
    assert_eq!(parts.len(), 3, "each client attempt is one backend upload");
    assert_eq!(
        (parts[1].part_idx, parts[2].part_idx),
        (1, 1),
        "a retry re-uploads the SAME part number"
    );
    assert_eq!(parts[2].bytes, retry, "the retry's bytes must be unaltered");

    let etags = writer
        .list_parts(&k, &creation.upload_id)
        .await
        .expect("list parts");
    assert_eq!(etags.len(), 2, "ListParts reports two live parts");

    writer
        .complete_multipart(
            &k,
            &creation.upload_id,
            vec![
                CompletedPartRef {
                    part_number: 1,
                    e_tag: parts[0].e_tag.clone(),
                    checksums: Checksums::default(),
                },
                CompletedPartRef {
                    part_number: 2,
                    e_tag: retried.e_tag.clone(),
                    checksums: Checksums::default(),
                },
            ],
            WriteChecksum::default(),
        )
        .await
        .expect("complete upload");

    let completions = store.completions();
    assert_eq!(completions.len(), 1);
    assert_eq!(
        completions[0].part_etags,
        vec![parts[0].e_tag.clone(), retried.e_tag.clone()],
        "the completion maps the client's manifest to the retry's backend ETag"
    );
    let mut expected = part_one.to_vec();
    expected.extend_from_slice(&retry);
    assert_eq!(
        stored_bytes(&store, "layers/retry.bin").await,
        Bytes::from(expected),
        "the assembled object holds part 1 plus the RETRIED part 2"
    );
}

/// The typed low-level multipart API used above must be reachable — guard
/// against the writer silently taking a different path in this configuration.
#[tokio::test]
async fn recording_store_observes_the_typed_multipart_path() {
    let (store, writer) = writer_over_recording();
    let k = key("layers/probe.bin");
    let creation = writer
        .create_multipart(&k, WriteMetadata::default())
        .await
        .expect("create upload");
    writer
        .upload_part(
            &k,
            &creation.upload_id,
            1,
            WriteChecksum::default(),
            one_chunk_body(Bytes::from_static(b"probe")),
        )
        .await
        .expect("upload part");
    assert_eq!(store.parts().len(), 1, "the recording origin saw the part");
}
