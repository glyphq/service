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
//! Tick bundle keys are `raw/epoch=NNN/tick=NNNNNNNN.qbb.zst`, produced by
//! [`object_key`]. The epoch's computor set is stored once at
//! `raw/epoch=NNN/computors.bin`, produced by [`computor_key`], instead of
//! being repeated in every tick bundle. A configured S3 prefix is prepended to
//! both paths.
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
pub use key::{
    BUNDLE_SUFFIX, COMPUTORS_SUFFIX, RAW_PREFIX, computor_key, object_key, validate_key,
};
pub use object::{HeadObject, RawArchive, SharedArchive};
pub use s3::S3Archive;

use bytes::Bytes;

/// Controls which optional sections are written into tick bundles.
///
/// The default policy omits full vote headers to keep the long-term archive
/// small. Operators can opt back in with `ARCHIVE_VOTES=true`. The computor
/// set is never repeated per tick; it is written once per epoch with
/// [`put_computors`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ArchivePolicy {
    /// Whether to include the `Votes` section in per-tick bundles.
    pub include_votes: bool,
}

impl ArchivePolicy {
    /// Policy that stores only the default sections.
    pub const fn default_policy() -> Self {
        Self {
            include_votes: false,
        }
    }

    /// Creates a policy with an explicit `include_votes` setting.
    pub const fn new(include_votes: bool) -> Self {
        Self { include_votes }
    }

    /// Reads `ARCHIVE_VOTES` from the process environment.
    ///
    /// Missing or unrecognized values fall back to the default (`false`).
    pub fn from_env() -> Self {
        Self::from_pairs(std::env::vars())
    }

    /// Builds a policy from key/value pairs, intended for tests.
    ///
    /// Only `ARCHIVE_VOTES` is read. When it appears more than once, the last
    /// occurrence wins, matching `std::env::var` semantics for a single key.
    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut policy = Self::default_policy();
        for (key, value) in pairs {
            if key.as_ref() == "ARCHIVE_VOTES" {
                policy.include_votes = parse_bool(value.as_ref()).unwrap_or(false);
            }
        }
        policy
    }
}

/// Parses a human-friendly boolean. Unrecognized values return `None`.
fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Writes the computor set for `epoch` to its sidecar object and returns the
/// key.
///
/// The payload is the encoded computor set as received from a peer. It is
/// written once per epoch, at [`computor_key`], rather than repeated in every
/// tick bundle.
pub async fn put_computors<A: RawArchive + ?Sized>(
    archive: &A,
    epoch: u16,
    bytes: &[u8],
) -> Result<String> {
    let key = computor_key(epoch);
    archive.put(&key, Bytes::copy_from_slice(bytes)).await?;
    Ok(key)
}

/// Reads the computor set sidecar for `epoch`.
///
/// A missing object returns `Ok(None)`. Any other storage failure is returned
/// as an error.
pub async fn get_computors<A: RawArchive + ?Sized>(
    archive: &A,
    epoch: u16,
) -> Result<Option<Bytes>> {
    let key = computor_key(epoch);
    match archive.get(&key).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(Error::NotFound(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

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
        let _: fn(u16) -> String = computor_key;
        let _: fn(&str) -> Result<()> = validate_key;
        let _ = FileArchive::new("/tmp/does-not-need-to-exist");
        let _ = Config::new("bucket");
    }

    #[test]
    fn policy_defaults_to_no_votes() {
        assert!(!ArchivePolicy::default().include_votes);
        assert!(!ArchivePolicy::default_policy().include_votes);
        assert!(!ArchivePolicy::from_pairs(Vec::<(String, String)>::new()).include_votes);
    }

    #[test]
    fn policy_parses_archive_votes() {
        for value in ["1", "true", "TRUE", " yes ", "On"] {
            let policy = ArchivePolicy::from_pairs([("ARCHIVE_VOTES", value)]);
            assert!(policy.include_votes, "{value:?} should enable votes");
        }
        for value in ["0", "false", "FALSE", " no ", "Off"] {
            let policy = ArchivePolicy::from_pairs([("ARCHIVE_VOTES", value)]);
            assert!(!policy.include_votes, "{value:?} should disable votes");
        }
    }

    #[test]
    fn policy_ignores_other_keys_and_bad_values() {
        let policy = ArchivePolicy::from_pairs([
            ("ARCHIVE_VOTES", "true"),
            ("OTHER", "false"),
            ("ARCHIVE_VOTES", "maybe"),
        ]);
        assert!(!policy.include_votes);
    }
}
