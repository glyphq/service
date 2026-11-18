//! Backend-neutral object interface.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::Result;

/// Metadata returned by [`RawArchive::head`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadObject {
    /// Object size in bytes.
    pub size: u64,
    /// Last modification time when the backend can provide one.
    pub last_modified: Option<DateTime<Utc>>,
}

/// A byte-object store holding immutable raw tick bundles.
///
/// Keys are relative POSIX-style paths produced by [`crate::object_key`].
/// Implementations must return [`crate::Error::NotFound`] from [`get`] when the
/// object is absent and `Ok(None)` from [`head`]. Backends are shared across
/// tasks, so the trait requires `Send + Sync`.
///
/// [`get`]: RawArchive::get
/// [`head`]: RawArchive::head
#[async_trait]
pub trait RawArchive: Send + Sync {
    /// Writes `bytes` at `key`, replacing any existing object.
    ///
    /// `key` is validated by the implementation before use.
    async fn put(&self, key: &str, bytes: Bytes) -> Result<()>;

    /// Reads the object at `key`.
    async fn get(&self, key: &str) -> Result<Bytes>;

    /// Returns object metadata without reading the payload.
    ///
    /// A missing object is `Ok(None)`, not an error.
    async fn head(&self, key: &str) -> Result<Option<HeadObject>>;
}

/// Convenient shared handle for a runtime-selected archive.
pub type SharedArchive = std::sync::Arc<dyn RawArchive>;
