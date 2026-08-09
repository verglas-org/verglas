//! Tests for the `MetadataFetch` trait: the offline `object_store` fetcher and
//! the server through-cache `ObjectRead` adapter must both serve ranges and
//! suffixes identically.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream;
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path as StorePath,
};
use verglas_core::CacheKey;
use verglas_core::read::{ObjectGet, ObjectMeta, ObjectRead, ReadError, ReadRange};
use verglas_tables::fetch::{MetadataFetch, ObjectReadFetch, ObjectStoreFetch};

#[tokio::test]
async fn object_store_fetch_serves_range_and_suffix() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let body: Vec<u8> = (0u8..=200).collect();
    store
        .put(&StorePath::from("db/t/obj"), PutPayload::from(body.clone()))
        .await
        .expect("put");

    let fetch = ObjectStoreFetch::new(Arc::clone(&store));
    let path = CacheKey {
        storage_binding_id: "managed-lakehouse".to_owned(),
        bucket: "lake".to_owned(),
        key: "db/t/obj".to_owned(),
    };

    let range = fetch.fetch_range(&path, 10..20).await.expect("range");
    assert_eq!(range.as_ref(), &body[10..20]);

    let suffix = fetch.fetch_suffix(&path, 8).await.expect("suffix");
    assert_eq!(suffix.as_ref(), &body[body.len() - 8..]);

    // A suffix longer than the object returns the whole object.
    let whole = fetch.fetch_suffix(&path, 10_000).await.expect("whole");
    assert_eq!(whole.as_ref(), &body[..]);
}

#[tokio::test]
async fn object_store_fetch_reports_not_found() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fetch = ObjectStoreFetch::new(store);
    let path = CacheKey {
        storage_binding_id: "managed-lakehouse".to_owned(),
        bucket: "lake".to_owned(),
        key: "missing".to_owned(),
    };
    let err = fetch.fetch_suffix(&path, 8).await.expect_err("not found");
    assert!(matches!(
        err,
        verglas_tables::fetch::FetchError::NotFound(_)
    ));
}

/// A minimal in-memory `ObjectRead` for the through-cache adapter test.
struct MockRead {
    /// key -> bytes.
    objects: HashMap<String, Vec<u8>>,
}

impl ObjectRead for MockRead {
    fn get(
        &self,
        key: &CacheKey,
        range: ReadRange,
    ) -> impl Future<Output = Result<ObjectGet, ReadError>> + Send {
        let bytes = self.objects.get(&key.key).cloned();
        async move {
            let bytes = bytes.ok_or(ReadError::NoSuchKey)?;
            let len = bytes.len() as u64;
            let (start, end) = match range {
                ReadRange::Full => (0, len),
                ReadRange::From(a) => (a, len),
                ReadRange::Bounded(a, b) => (a, (b + 1).min(len)),
                ReadRange::Suffix(n) => (len.saturating_sub(n), len),
            };
            let slice = Bytes::from(bytes[start as usize..end as usize].to_vec());
            let body = stream::once(async move { Ok(slice) });
            Ok(ObjectGet {
                meta: ObjectMeta {
                    size: len,
                    e_tag: Some("etag".to_owned()),
                    last_modified: None,
                    content_type: None,
                    ..ObjectMeta::default()
                },
                range: start..end,
                body: Box::pin(body),
                served_from: verglas_core::read::TierCell::new(),
            })
        }
    }

    fn head(&self, key: &CacheKey) -> impl Future<Output = Result<ObjectMeta, ReadError>> + Send {
        let size = self.objects.get(&key.key).map(|b| b.len() as u64);
        async move {
            Ok(ObjectMeta {
                size: size.ok_or(ReadError::NoSuchKey)?,
                e_tag: Some("etag".to_owned()),
                last_modified: None,
                content_type: None,
                ..ObjectMeta::default()
            })
        }
    }
}

#[tokio::test]
async fn through_cache_fetch_matches_range_and_suffix() {
    let body: Vec<u8> = (0u8..=150).collect();
    let mut objects = HashMap::new();
    objects.insert("db/t/obj".to_owned(), body.clone());
    let fetch = ObjectReadFetch::new(MockRead { objects });
    let path = CacheKey {
        storage_binding_id: "managed-lakehouse".to_owned(),
        bucket: "lake".to_owned(),
        key: "db/t/obj".to_owned(),
    };

    let range = fetch.fetch_range(&path, 5..25).await.expect("range");
    assert_eq!(range.as_ref(), &body[5..25]);

    let suffix = fetch.fetch_suffix(&path, 12).await.expect("suffix");
    assert_eq!(suffix.as_ref(), &body[body.len() - 12..]);

    // An empty range yields empty bytes without hitting the reader.
    let empty = fetch.fetch_range(&path, 10..10).await.expect("empty");
    assert!(empty.is_empty());

    let err = fetch
        .fetch_suffix(
            &CacheKey {
                storage_binding_id: "managed-lakehouse".to_owned(),
                bucket: "lake".to_owned(),
                key: "nope".to_owned(),
            },
            4,
        )
        .await
        .expect_err("missing");
    assert!(matches!(
        err,
        verglas_tables::fetch::FetchError::NotFound(_)
    ));
}
