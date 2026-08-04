//! The durability seam: a small key/value object backend the chunk store and
//! manifest writer persist through.
//!
//! Chunks and manifests are plain objects in the cache's configured backing
//! bucket — "the same backend path the cache already uses" (#382). This module
//! defines [`ObjectBackend`], the minimal put/get/exists surface the rest of the
//! crate needs, and [`ObjectStoreBackend`], the adapter over an `object_store`
//! handle the daemon already builds per bucket. verglas-block never names a
//! bucket or holds credentials; the caller hands it a built store.

use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

/// A failure talking to the durable object backend.
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    /// The underlying object store returned an error (network, auth, IO).
    #[error("object backend error for `{key}`: {source}")]
    Store {
        /// The object key the operation targeted.
        key: String,
        /// The underlying `object_store` error.
        #[source]
        source: object_store::Error,
    },
}

/// A minimal durable key/value object backend. Keys are slash-delimited object
/// paths (`blocks/<hex>`, `manifests/<device>/<version>`); values are opaque
/// bytes. Implementations must be safe to share across tasks.
#[async_trait]
pub trait ObjectBackend: Send + Sync {
    /// Writes `bytes` at `key`, overwriting any existing object. Returns only
    /// once the object is durable in the backend — this is what the flush
    /// barrier relies on to guarantee a chunk is durable before its manifest.
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), BackendError>;

    /// Reads the object at `key`, or `None` if no such object exists.
    async fn get(&self, key: &str) -> Result<Option<Bytes>, BackendError>;

    /// Whether an object exists at `key`. Used to skip re-uploading a chunk
    /// another device or version already made durable (dedup).
    async fn exists(&self, key: &str) -> Result<bool, BackendError>;
}

/// [`ObjectBackend`] over any `object_store` handle. Generic (and `?Sized`) so
/// the daemon can pass an `Arc<dyn MultipartObjectStore>` — a trait object that
/// implements `ObjectStore` through its supertrait — without an extra wrapper.
pub struct ObjectStoreBackend<S: ObjectStore + ?Sized> {
    /// The built store for the cache's backing bucket.
    store: std::sync::Arc<S>,
}

impl<S: ObjectStore + ?Sized> ObjectStoreBackend<S> {
    /// Wraps a built object store as the durable backend.
    pub fn new(store: std::sync::Arc<S>) -> Self {
        ObjectStoreBackend { store }
    }
}

#[async_trait]
impl<S: ObjectStore + ?Sized> ObjectBackend for ObjectStoreBackend<S> {
    /// Puts through the object store; a `PutResult` we do not need is discarded.
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), BackendError> {
        let path = Path::from(key);
        self.store
            .put(&path, bytes.into())
            .await
            .map(|_| ())
            .map_err(|source| BackendError::Store {
                key: key.to_owned(),
                source,
            })
    }

    /// Gets through the object store, mapping a `NotFound` to `None` and draining
    /// the body to `Bytes`.
    async fn get(&self, key: &str) -> Result<Option<Bytes>, BackendError> {
        let path = Path::from(key);
        match self.store.get(&path).await {
            Ok(result) => result
                .bytes()
                .await
                .map(Some)
                .map_err(|source| BackendError::Store {
                    key: key.to_owned(),
                    source,
                }),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(source) => Err(BackendError::Store {
                key: key.to_owned(),
                source,
            }),
        }
    }

    /// Existence via `head`, mapping `NotFound` to `false`.
    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        let path = Path::from(key);
        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(source) => Err(BackendError::Store {
                key: key.to_owned(),
                source,
            }),
        }
    }
}

#[cfg(test)]
pub(crate) mod testing {
    //! An in-memory [`ObjectBackend`] for tests, plus put-order recording so the
    //! manifest-barrier tests can assert every referenced chunk was made durable
    //! before the manifest that references it.

    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::*;

    /// An in-memory object backend. Records every key that was `put`, in order,
    /// so tests can assert durability ordering and count distinct chunk objects.
    #[derive(Default)]
    pub struct MemoryBackend {
        /// The stored objects.
        objects: Mutex<HashMap<String, Bytes>>,
        /// Every key passed to `put`, in call order (with duplicates).
        put_order: Mutex<Vec<String>>,
        /// When set, `put` of a key with this prefix fails — used to force a
        /// mid-flush chunk-upload failure and prove the manifest is not written.
        fail_prefix: Mutex<Option<String>>,
    }

    impl MemoryBackend {
        /// A fresh empty backend.
        pub fn new() -> Self {
            Self::default()
        }

        /// Makes every future `put` whose key starts with `prefix` fail.
        pub fn fail_puts_under(&self, prefix: &str) {
            *self.fail_prefix.lock().expect("lock") = Some(prefix.to_owned());
        }

        /// The number of distinct object keys stored under `prefix`.
        pub fn count_under(&self, prefix: &str) -> usize {
            self.objects
                .lock()
                .expect("lock")
                .keys()
                .filter(|k| k.starts_with(prefix))
                .count()
        }

        /// The recorded put order (keys, with duplicates), for barrier tests.
        pub fn put_order(&self) -> Vec<String> {
            self.put_order.lock().expect("lock").clone()
        }
    }

    #[async_trait]
    impl ObjectBackend for MemoryBackend {
        async fn put(&self, key: &str, bytes: Bytes) -> Result<(), BackendError> {
            if let Some(prefix) = self.fail_prefix.lock().expect("lock").as_deref()
                && key.starts_with(prefix)
            {
                return Err(BackendError::Store {
                    key: key.to_owned(),
                    source: object_store::Error::Generic {
                        store: "MemoryBackend",
                        source: "injected put failure".into(),
                    },
                });
            }
            self.put_order.lock().expect("lock").push(key.to_owned());
            self.objects
                .lock()
                .expect("lock")
                .insert(key.to_owned(), bytes);
            Ok(())
        }

        async fn get(&self, key: &str) -> Result<Option<Bytes>, BackendError> {
            Ok(self.objects.lock().expect("lock").get(key).cloned())
        }

        async fn exists(&self, key: &str) -> Result<bool, BackendError> {
            Ok(self.objects.lock().expect("lock").contains_key(key))
        }
    }
}
