//! Object keys and key safety.
//!
//! Archive keys are relative POSIX-style paths. Every backend rejects absolute
//! paths, empty components, `.`/`..`, backslashes, and NUL bytes so a key can
//! never escape a filesystem root or address an unintended object.

use crate::{Error, Result};

/// Prefix under which raw tick bundles are stored.
pub const RAW_PREFIX: &str = "raw";
/// Bundle object suffix.
pub const BUNDLE_SUFFIX: &str = ".qbb.zst";

/// Builds the canonical object key for a tick bundle.
///
/// # Example
///
/// ```
/// assert_eq!(
///     archive::object_key(42, 123),
///     "raw/epoch=042/tick=00000123.qbb.zst"
/// );
/// ```
pub fn object_key(epoch: u16, tick: u32) -> String {
    format!("{RAW_PREFIX}/epoch={epoch:03}/tick={tick:08}{BUNDLE_SUFFIX}")
}

/// Rejects unsafe or non-canonical archive keys.
pub fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(Error::InvalidKey("key is empty".into()));
    }
    if key.starts_with('/') {
        return Err(Error::InvalidKey(format!("key {key:?} must be relative")));
    }
    if key.contains('\\') {
        return Err(Error::InvalidKey(format!(
            "key {key:?} contains a backslash"
        )));
    }
    if key.contains('\0') {
        return Err(Error::InvalidKey(format!(
            "key {key:?} contains a NUL byte"
        )));
    }
    if key.ends_with('/') {
        return Err(Error::InvalidKey(format!(
            "key {key:?} must not end with a slash"
        )));
    }
    for component in key.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(Error::InvalidKey(format!(
                "key {key:?} contains an invalid path component"
            )));
        }
    }
    Ok(())
}

/// Trims slashes from an optional S3 key prefix.
pub(crate) fn normalize_prefix(prefix: Option<&str>) -> Option<String> {
    let prefix = prefix?.trim_matches('/');
    if prefix.is_empty() {
        None
    } else {
        Some(prefix.to_owned())
    }
}

/// Joins a validated key to an optional prefix.
pub(crate) fn prefixed_key(prefix: Option<&str>, key: &str) -> Result<String> {
    validate_key(key)?;
    match normalize_prefix(prefix) {
        Some(prefix) => {
            let full = format!("{prefix}/{key}");
            validate_key(&full)?;
            Ok(full)
        }
        None => Ok(key.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_format_is_stable() {
        assert_eq!(object_key(0, 0), "raw/epoch=000/tick=00000000.qbb.zst");
        assert_eq!(object_key(42, 123), "raw/epoch=042/tick=00000123.qbb.zst");
        assert_eq!(
            object_key(u16::MAX, u32::MAX),
            "raw/epoch=65535/tick=4294967295.qbb.zst"
        );
    }

    #[test]
    fn generated_keys_validate() {
        validate_key(&object_key(1, 2)).unwrap();
    }

    #[test]
    fn rejects_unsafe_keys() {
        for key in [
            "",
            "/absolute",
            "raw/../escape",
            "raw/./tick",
            "raw//tick",
            "raw\\tick",
            "raw/tick/",
            "raw/tick\0",
        ] {
            assert!(validate_key(key).is_err(), "{key:?} was accepted");
        }
    }

    #[test]
    fn prefix_is_normalised() {
        assert_eq!(normalize_prefix(Some("/raw/")), Some("raw".into()));
        assert_eq!(normalize_prefix(Some("///")), None);
        assert_eq!(normalize_prefix(None), None);
        assert_eq!(
            prefixed_key(Some("archive/"), "raw/tick").unwrap(),
            "archive/raw/tick"
        );
        assert_eq!(prefixed_key(None, "raw/tick").unwrap(), "raw/tick");
    }
}
