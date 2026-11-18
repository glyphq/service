//! S3 and MinIO archive backend built on `object_store`.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::config::Config;
use crate::key::prefixed_key;
use crate::object::{HeadObject, RawArchive};
use crate::{Error, Result};

/// S3-compatible raw archive.
///
/// The client uses path-style addressing by default, which is what MinIO
/// serves. The configured `prefix`, when present, is prepended to every
/// [`crate::object_key`].
pub struct S3Archive {
    store: Arc<dyn ObjectStore>,
    prefix: Option<String>,
    bucket: String,
}

impl std::fmt::Debug for S3Archive {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3Archive")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl S3Archive {
    /// Builds a client from `config`. No network request is made until an
    /// archive operation runs.
    pub fn new(config: Config) -> Result<Self> {
        config.validate()?;
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_allow_http(config.allow_http)
            .with_virtual_hosted_style_request(false);
        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let (Some(access_key_id), Some(secret_access_key)) =
            (&config.access_key_id, &config.secret_access_key)
        {
            builder = builder
                .with_access_key_id(access_key_id)
                .with_secret_access_key(secret_access_key);
        }
        if let Some(session_token) = &config.session_token {
            builder = builder.with_token(session_token);
        }
        let store = builder.build()?;
        Ok(Self {
            store: Arc::new(store),
            prefix: config.prefix,
            bucket: config.bucket,
        })
    }

    /// Builds a client from the environment, or `None` when `S3_BUCKET` is
    /// unset.
    pub fn from_env() -> Result<Option<Self>> {
        match Config::from_env() {
            Some(config) => Self::new(config).map(Some),
            None => Ok(None),
        }
    }

    /// Bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Optional key prefix inside the bucket.
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// The underlying client, for advanced use.
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// Wraps an already-built object store. Useful for tests, custom client
    /// options, or sharing one client between components.
    pub fn with_store(
        store: Arc<dyn ObjectStore>,
        bucket: impl Into<String>,
        prefix: Option<String>,
    ) -> Self {
        Self {
            store,
            prefix,
            bucket: bucket.into(),
        }
    }

    fn path_for(&self, key: &str) -> Result<ObjectPath> {
        let full = prefixed_key(self.prefix.as_deref(), key)?;
        ObjectPath::parse(&full).map_err(|error| {
            Error::InvalidKey(format!("{full:?} is not a valid object path: {error}"))
        })
    }
}

#[async_trait]
impl RawArchive for S3Archive {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<()> {
        let path = self.path_for(key)?;
        self.store.put(&path, bytes.into()).await?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        let path = self.path_for(key)?;
        match self.store.get(&path).await {
            Ok(result) => Ok(result.bytes().await?),
            Err(object_store::Error::NotFound { .. }) => Err(Error::NotFound(key.to_owned())),
            Err(error) => Err(error.into()),
        }
    }

    async fn head(&self, key: &str) -> Result<Option<HeadObject>> {
        let path = self.path_for(key)?;
        match self.store.head(&path).await {
            Ok(meta) => Ok(Some(HeadObject {
                size: meta.size,
                last_modified: Some(meta.last_modified),
            })),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn config() -> Config {
        let mut config = Config::new("glyph-archive");
        config.endpoint = Some("http://127.0.0.1:9000".into());
        config.access_key_id = Some("minio".into());
        config.secret_access_key = Some("minio-secret".into());
        config.prefix = Some("cold".into());
        config.allow_http = true;
        config
    }

    #[test]
    fn builds_client_without_network() {
        let archive = S3Archive::new(config()).unwrap();
        assert_eq!(archive.bucket(), "glyph-archive");
        assert_eq!(archive.prefix(), Some("cold"));
    }

    #[test]
    fn validates_config_before_building() {
        let mut bad = config();
        bad.secret_access_key = None;
        assert!(matches!(S3Archive::new(bad), Err(Error::Config(_))));
    }

    #[test]
    fn applies_prefix_to_object_paths() {
        let archive = S3Archive::new(config()).unwrap();
        let path = archive
            .path_for("raw/epoch=042/tick=00000123.qbb.zst")
            .unwrap();
        assert_eq!(path.as_ref(), "cold/raw/epoch=042/tick=00000123.qbb.zst");
    }

    #[test]
    fn rejects_unsafe_keys_without_network() {
        let archive = S3Archive::new(config()).unwrap();
        assert!(matches!(
            archive.path_for("../escape"),
            Err(Error::InvalidKey(_))
        ));
    }

    #[tokio::test]
    async fn object_store_round_trip_with_prefix() {
        let archive = S3Archive::with_store(
            Arc::new(InMemory::new()),
            "glyph-archive",
            Some("cold".into()),
        );
        let key = "raw/epoch=042/tick=00000123.qbb.zst";
        let bytes = Bytes::from_static(b"bundle bytes");

        assert!(archive.head(key).await.unwrap().is_none());
        archive.put(key, bytes.clone()).await.unwrap();
        assert_eq!(archive.get(key).await.unwrap(), bytes);
        let head = archive.head(key).await.unwrap().unwrap();
        assert_eq!(head.size, bytes.len() as u64);
        assert!(head.last_modified.is_some());
    }

    #[tokio::test]
    async fn missing_object_reports_not_found() {
        let archive = S3Archive::with_store(Arc::new(InMemory::new()), "glyph-archive", None);
        let key = "raw/epoch=001/tick=00000001.qbb.zst";
        assert!(matches!(archive.get(key).await, Err(Error::NotFound(_))));
        assert!(archive.head(key).await.unwrap().is_none());
    }
}
