//! Serde-able copy of write metadata for the journal (#180).
//!
//! `verglas_core::write::WriteMetadata` is not serde-able, so the journal keeps
//! its own mirror and converts both ways. The journal must round-trip the PUT
//! metadata so a read-your-writes GET and the eventual origin propagation both
//! report and store exactly what the client sent.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use verglas_core::read::ObjectMeta;
use verglas_core::write::WriteMetadata;

/// The subset of write metadata the journal persists. Mirrors
/// [`WriteMetadata`] field-for-field.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMetadata {
    /// `Content-Type`.
    pub content_type: Option<String>,
    /// `Cache-Control`.
    pub cache_control: Option<String>,
    /// `Content-Disposition`.
    pub content_disposition: Option<String>,
    /// `Content-Encoding`.
    pub content_encoding: Option<String>,
    /// `Content-Language`.
    pub content_language: Option<String>,
    /// `Expires` (raw HTTP-date string).
    pub expires: Option<String>,
    /// `x-amz-meta-*` user metadata, keys without the prefix.
    pub user_metadata: BTreeMap<String, String>,
}

impl From<&WriteMetadata> for StoredMetadata {
    /// Copies every field out of the protocol metadata.
    fn from(m: &WriteMetadata) -> Self {
        Self {
            content_type: m.content_type.clone(),
            cache_control: m.cache_control.clone(),
            content_disposition: m.content_disposition.clone(),
            content_encoding: m.content_encoding.clone(),
            content_language: m.content_language.clone(),
            expires: m.expires.clone(),
            user_metadata: m.user_metadata.clone(),
        }
    }
}

impl From<&StoredMetadata> for WriteMetadata {
    /// Rebuilds the protocol metadata for origin propagation.
    fn from(m: &StoredMetadata) -> Self {
        Self {
            content_type: m.content_type.clone(),
            cache_control: m.cache_control.clone(),
            content_disposition: m.content_disposition.clone(),
            content_encoding: m.content_encoding.clone(),
            content_language: m.content_language.clone(),
            expires: m.expires.clone(),
            // Checksums are the origin's to compute and verify, never Verglas's
            // (#154). The propagated PUT carries none from here.
            checksum: Default::default(),
            user_metadata: m.user_metadata.clone(),
        }
    }
}

impl StoredMetadata {
    /// Builds the [`ObjectMeta`] a read-your-writes HEAD/GET reports for a dirty
    /// object of `size` bytes with synthetic `etag`, last-modified `created_ms`.
    pub fn to_object_meta(&self, size: u64, etag: String, created_ms: u64) -> ObjectMeta {
        ObjectMeta {
            size,
            e_tag: Some(etag),
            last_modified: Some(UNIX_EPOCH + Duration::from_millis(created_ms)),
            content_type: self.content_type.clone(),
            cache_control: self.cache_control.clone(),
            content_disposition: self.content_disposition.clone(),
            content_encoding: self.content_encoding.clone(),
            content_language: self.content_language.clone(),
            expires: self.expires.clone(),
            user_metadata: self.user_metadata.clone(),
        }
    }
}

/// Current Unix time in milliseconds.
pub fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}
