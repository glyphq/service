//! Local filesystem archive backend.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::key::validate_key;
use crate::object::{HeadObject, RawArchive};
use crate::{Error, Result};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Filesystem-backed raw archive.
///
/// Keys map directly to relative paths below `root`. Parent directories are
/// created on write. Writes use a temporary file in the same directory followed
/// by a rename, so a crash cannot expose a partially written bundle under the
/// final key.
#[derive(Debug, Clone)]
pub struct FileArchive {
    root: PathBuf,
}

impl FileArchive {
    /// Creates an archive rooted at `root`. The directory is created lazily on
    /// the first write.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The configured root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path_for(&self, key: &str) -> Result<PathBuf> {
        validate_key(key)?;
        Ok(self.root.join(key))
    }
}

#[async_trait]
impl RawArchive for FileArchive {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<()> {
        let path = self.path_for(key)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("object");
        let temp = path.with_file_name(format!(
            ".{file_name}.{}.{sequence}.tmp",
            std::process::id()
        ));
        tokio::fs::write(&temp, &bytes).await?;
        if let Err(error) = tokio::fs::rename(&temp, &path).await {
            let _ = tokio::fs::remove_file(&temp).await;
            return Err(error.into());
        }
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        let path = self.path_for(key)?;
        match tokio::fs::read(&path).await {
            Ok(bytes) => Ok(Bytes::from(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(Error::NotFound(key.to_owned()))
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn head(&self, key: &str) -> Result<Option<HeadObject>> {
        let path = self.path_for(key)?;
        match tokio::fs::metadata(&path).await {
            Ok(metadata) => Ok(Some(HeadObject {
                size: metadata.len(),
                last_modified: metadata.modified().ok().map(DateTime::<Utc>::from),
            })),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bundle::{RawBundle, get_bundle, put_bundle};
    use crate::key::object_key;

    fn sample_bundle() -> RawBundle {
        let mut bundle = RawBundle::new(42, 123);
        bundle.tick_header = vec![7; 352];
        bundle.tick_data = vec![9; 1024];
        bundle.transactions = vec![1, 2, 3, 4];
        bundle
    }

    #[tokio::test]
    async fn put_get_head_round_trip() {
        let root = tempfile::tempdir().unwrap();
        let archive = FileArchive::new(root.path());
        let key = "raw/epoch=042/tick=00000123.qbb.zst";
        let bytes = Bytes::from_static(b"bundle bytes");

        assert!(archive.head(key).await.unwrap().is_none());
        archive.put(key, bytes.clone()).await.unwrap();

        let head = archive.head(key).await.unwrap().unwrap();
        assert_eq!(head.size, bytes.len() as u64);
        assert!(head.last_modified.is_some());
        assert_eq!(archive.get(key).await.unwrap(), bytes);
        assert!(root.path().join(key).is_file());
    }

    #[tokio::test]
    async fn missing_object_is_not_found() {
        let root = tempfile::tempdir().unwrap();
        let archive = FileArchive::new(root.path());
        let key = "raw/epoch=001/tick=00000001.qbb.zst";
        assert!(matches!(archive.get(key).await, Err(Error::NotFound(_))));
        assert!(archive.head(key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn overwrite_replaces_contents() {
        let root = tempfile::tempdir().unwrap();
        let archive = FileArchive::new(root.path());
        let key = "raw/epoch=001/tick=00000001.qbb.zst";
        archive.put(key, Bytes::from_static(b"one")).await.unwrap();
        archive.put(key, Bytes::from_static(b"two")).await.unwrap();
        assert_eq!(&archive.get(key).await.unwrap()[..], b"two");
    }

    #[tokio::test]
    async fn bundle_helpers_use_expected_key() {
        let root = tempfile::tempdir().unwrap();
        let archive = FileArchive::new(root.path());
        let bundle = sample_bundle();
        let key = put_bundle(&archive, &bundle).await.unwrap();
        assert_eq!(key, object_key(42, 123));
        assert_eq!(get_bundle(&archive, 42, 123).await.unwrap(), Some(bundle));
        assert_eq!(get_bundle(&archive, 42, 124).await.unwrap(), None);
    }

    #[tokio::test]
    async fn computor_sidecar_round_trip() {
        use crate::{computor_key, get_computors, put_computors};

        let root = tempfile::tempdir().unwrap();
        let archive = FileArchive::new(root.path());
        let payload = vec![0x2a; 21_700];

        assert_eq!(
            get_computors(&archive, 42).await.unwrap(),
            None,
            "a missing sidecar must be Ok(None)"
        );

        let key = put_computors(&archive, 42, &payload).await.unwrap();
        assert_eq!(key, computor_key(42));
        assert_eq!(key, "raw/epoch=042/computors.bin");
        assert!(root.path().join(&key).is_file());
        assert_eq!(
            get_computors(&archive, 42).await.unwrap().as_deref(),
            Some(&payload[..])
        );

        // A different epoch does not alias the sidecar.
        assert_eq!(get_computors(&archive, 43).await.unwrap(), None);
    }

    #[tokio::test]
    async fn rejects_path_traversal() {
        let root = tempfile::tempdir().unwrap();
        let archive = FileArchive::new(root.path());
        for key in ["../escape", "/absolute", "raw/../../escape"] {
            assert!(matches!(
                archive.put(key, Bytes::from_static(b"x")).await,
                Err(Error::InvalidKey(_))
            ));
            assert!(matches!(archive.get(key).await, Err(Error::InvalidKey(_))));
            assert!(matches!(archive.head(key).await, Err(Error::InvalidKey(_))));
        }
    }
}
