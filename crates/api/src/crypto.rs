//! Shared decoding for deployment-provided 32-byte keys.

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};

/// Accepts 64 hex characters or standard/URL-safe base64.
pub fn decode_key(raw: &str) -> Option<[u8; 32]> {
    let bytes = if raw.len() == 64 && raw.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        hex::decode(raw).ok()?
    } else {
        URL_SAFE_NO_PAD
            .decode(raw)
            .or_else(|_| STANDARD.decode(raw))
            .ok()?
    };
    bytes.try_into().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_hex_and_base64_keys() {
        let hex = "00".repeat(32);
        assert_eq!(decode_key(&hex), Some([0u8; 32]));

        let encoded = URL_SAFE_NO_PAD.encode([3u8; 32]);
        assert_eq!(decode_key(&encoded), Some([3u8; 32]));
        assert_eq!(decode_key("too-short"), None);
    }
}
