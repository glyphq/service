//! Raw tick archive for local disk and S3-compatible object storage.
//!
//! The archive is the cold store and replay source. One object holds one
//! tick's raw inputs: the winning tick header, `TickData`, votes, transactions,
//! log ranges, logs, and computors. [`RawBundle`] encodes those sections into a
//! documented compressed container and [`RawArchive`] stores the bytes.
//!
//! # Backends
//!
//! - [`FileArchive`] writes below a local directory.
//! - [`S3Archive`] uses `object_store` with path-style addressing, which works
//!   with AWS S3 and MinIO.
//!
//! An [`S3Archive`] is selected by [`Config::from_env`], which returns `None`
//! when `S3_BUCKET` is unset:
//!
//! ```no_run
//! use archive::S3Archive;
//!
//! # async fn run() -> archive::Result<()> {
//! if let Some(archive) = S3Archive::from_env()? {
//!     let _ = archive;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Object layout
//!
//! Keys are `raw/epoch=NNN/tick=NNNNNNNN.qbb.zst`, produced by [`object_key`].
//! A configured S3 prefix is prepended to that path.
//!
//! ```
//! # use archive::{FileArchive, RawBundle, put_bundle};
//! # async fn example() -> archive::Result<()> {
//! let archive = FileArchive::new("/tmp/glyph-archive");
//! let mut bundle = RawBundle::new(42, 123);
//! bundle.tick_header = vec![0; 352];
//! let key = put_bundle(&archive, &bundle).await?;
//! assert_eq!(key, "raw/epoch=042/tick=00000123.qbb.zst");
//! # Ok(())
//! # }
//! ```

pub mod bundle;
pub mod config;
pub mod digest;
pub mod file;
pub mod key;
pub mod object;
pub mod s3;

pub use bundle::{
    COMPRESSION_LEVEL, FLAG_ZSTD, FORMAT_VERSION, HEADER_LEN, MAGIC, MAX_BUNDLE_BYTES, RawBundle,
    Section, get_bundle, put_bundle,
};
pub use config::{Config, DEFAULT_REGION};
pub use digest::xxh64;
pub use file::FileArchive;
pub use key::{BUNDLE_SUFFIX, RAW_PREFIX, object_key, validate_key};
pub use object::{HeadObject, RawArchive, SharedArchive};
pub use s3::S3Archive;

/// Archive errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A bundle failed structural or integrity validation.
    #[error("invalid bundle: {0}")]
    InvalidBundle(String),
    /// An object key is empty, absolute, or unsafe.
    #[error("invalid archive key: {0}")]
    InvalidKey(String),
    /// Configuration is missing or contradictory.
    #[error("configuration: {0}")]
    Config(String),
    /// The requested object does not exist.
    #[error("archive object not found: {0}")]
    NotFound(String),
    /// Local filesystem or compression I/O failure.
    #[error("archive io: {0}")]
    Io(#[from] std::io::Error),
    /// Remote object store failure.
    #[error("object store: {0}")]
    ObjectStore(#[from] object_store::Error),
}

/// Result alias for archive operations.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_api_is_wired() {
        let _: fn(u16, u32) -> String = object_key;
        let _: fn(&str) -> Result<()> = validate_key;
        let _ = FileArchive::new("/tmp/does-not-need-to-exist");
        let _ = Config::new("bucket");
    }
}
