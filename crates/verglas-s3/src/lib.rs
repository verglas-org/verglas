//! S3 protocol front-end.
//!
//! Implements the S3-compatible surface engines point at (`s3.endpoint`
//! swap) on the `s3s` crate: GetObject/HeadObject with range support (issue
//! #6) and the write surface — PutObject, DeleteObject(s), CopyObject, and
//! the multipart lifecycle (issue #9). Reads go through the [`ObjectRead`]
//! interface so the cache (#12) and the real backend client (#18) drop in behind
//! the protocol layer; writes go through the [`ObjectWrite`] interface and are
//! always passed through durably to the origin, firing the [`Invalidation`]
//! hook before the client is acked. The interface traits themselves live in
//! `verglas-core` (re-exported here for convenience) so implementations
//! never depend on this protocol crate; [`PassthroughRead`] and
//! [`PassthroughWrite`] are the direct-to-origin implementations.

mod auth;
pub mod frontend;
mod middleware;
pub mod passthrough;
pub mod passthrough_route;
pub mod preconditions;
pub mod serving_api;

pub use frontend::{VerglasS3, router, router_with_passthrough};
pub use passthrough::{PassthroughList, PassthroughRead, PassthroughWrite};
pub use passthrough_route::BucketConfigPassthrough;
pub use serving_api::{ApiRequest, ApiResponse, ServingApi};
pub use verglas_backend::{BackendRegistry, BackendStore, BackendStores, MultipartObjectStore};
pub use verglas_core::CacheKey;
pub use verglas_core::list::{ListError, ListObject, ListPage, ListRequest, ObjectList};
pub use verglas_core::read::{
    BodyStream, ObjectGet, ObjectMeta, ObjectRead, ReadError, ReadRange, Revalidation, ServedTier,
    TierCell,
};
pub use verglas_core::write::{
    CompletedPartRef, CopyOutcome, Invalidation, MultipartCreation, NoopInvalidation, ObjectWrite,
    PartInfo, PartUpload, PutOutcome, WriteBodyStream, WriteChecksum, WriteError, WriteMetadata,
};

/// Derives the logical cache key for an S3 request.
///
/// Placeholder: the real mapping will account for byte ranges and object
/// versions.
pub fn cache_key_for(storage_binding_id: &str, bucket: &str, key: &str) -> verglas_core::CacheKey {
    verglas_core::CacheKey {
        storage_binding_id: storage_binding_id.to_owned(),
        bucket: bucket.to_owned(),
        key: key.to_owned(),
    }
}
