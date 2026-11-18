use crate::crypto::k12::k12_32;
use crate::error::{Error, Result};

pub const IDENTITY_LEN: usize = 60;
pub const PUBLIC_KEY_LEN: usize = 32;

/// A 32-byte Qubic public key. Rendered as a 60-character uppercase identity.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PublicKey(pub [u8; PUBLIC_KEY_LEN]);

impl PublicKey {
    pub const fn new(bytes: [u8; PUBLIC_KEY_LEN]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; PUBLIC_KEY_LEN] {
        &self.0
    }

    pub fn to_identity(&self) -> String {
        encode_base26(&self.0, true, true)
    }

    pub fn from_identity(value: &str) -> Result<Self> {
        let bytes = decode_base26(value, false)?;
        let expected = encode_base26(&bytes, true, true);
        if expected != value {
            return Err(Error::InvalidIdentity(format!(
                "identity checksum mismatch: expected {expected}"
            )));
        }
        Ok(Self(bytes))
    }

    pub fn to_hex(&self) -> String {
        format!("0x{}", hex::encode(self.0))
    }
}

impl std::fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_identity())
    }
}

impl std::fmt::Display for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_identity())
    }
}

pub const ZERO_PUBLIC_KEY: PublicKey = PublicKey([0u8; PUBLIC_KEY_LEN]);

pub fn is_zero_public_key(key: &[u8; PUBLIC_KEY_LEN]) -> bool {
    key.iter().all(|b| *b == 0)
}

/// Encodes 32 bytes with the Qubic base-26 alphabet.
///
/// [lowercase] selects a-z instead of A-Z. [with_checksum] appends the four
/// checksum characters derived from KangarooTwelve(public_key, 32).
pub fn encode_base26(bytes: &[u8; PUBLIC_KEY_LEN], uppercase: bool, with_checksum: bool) -> String {
    let base = if uppercase { b'A' } else { b'a' };
    let mut out = String::with_capacity(IDENTITY_LEN);
    for chunk in bytes.chunks_exact(8) {
        let mut fragment = u64::from_le_bytes(chunk.try_into().expect("8 bytes"));
        for _ in 0..14 {
            out.push((base + (fragment % 26) as u8) as char);
            fragment /= 26;
        }
    }
    if with_checksum {
        let digest = k12_32(bytes);
        let mut checksum = u32::from_le_bytes([digest[0], digest[1], digest[2], 0]) & 0x3FFFF;
        for _ in 0..4 {
            out.push((base + (checksum % 26) as u8) as char);
            checksum /= 26;
        }
    }
    out
}

/// Decodes the first 56 characters of a Qubic identity or transaction hash.
pub fn decode_base26(value: &str, lowercase: bool) -> Result<[u8; PUBLIC_KEY_LEN]> {
    let expected_len = if lowercase { IDENTITY_LEN } else { IDENTITY_LEN };
    if value.len() != expected_len {
        return Err(Error::InvalidIdentity(format!(
            "expected {expected_len} characters, got {}",
            value.len()
        )));
    }
    let base = if lowercase { b'a' } else { b'A' };
    let mut out = [0u8; PUBLIC_KEY_LEN];
    for (chunk_index, chunk) in value.as_bytes()[..56].chunks_exact(14).enumerate() {
        let mut fragment = 0u64;
        for byte in chunk.iter().rev() {
            if *byte < base || *byte >= base + 26 {
                return Err(Error::InvalidIdentity(format!(
                    "invalid character {:?} at position {}",
                    *byte as char,
                    chunk_index * 14
                )));
            }
            fragment = fragment
                .checked_mul(26)
                .and_then(|v| v.checked_add((*byte - base) as u64))
                .ok_or_else(|| Error::InvalidIdentity("base-26 fragment overflow".into()))?;
        }
        out[chunk_index * 8..chunk_index * 8 + 8].copy_from_slice(&fragment.to_le_bytes());
    }
    Ok(out)
}

/// A 60-character lowercase transaction hash.
pub fn encode_tx_hash(hash: &[u8; PUBLIC_KEY_LEN]) -> String {
    encode_base26(hash, false, true)
}

pub fn decode_tx_hash(value: &str) -> Result<[u8; PUBLIC_KEY_LEN]> {
    let bytes = decode_base26(value, true)?;
    let expected = encode_tx_hash(&bytes);
    if expected != value {
        return Err(Error::InvalidTransactionHash(format!(
            "transaction hash checksum mismatch: expected {expected}"
        )));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_identity_round_trip() {
        let identity = "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK";
        let key = PublicKey::from_identity(identity).expect("valid identity");
        assert_eq!(key.to_identity(), identity);
    }

    #[test]
    fn rejects_bad_checksum() {
        let identity = "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXA";
        assert!(PublicKey::from_identity(identity).is_err());
    }

    #[test]
    fn transaction_hash_is_lowercase() {
        let hash = [7u8; 32];
        let encoded = encode_tx_hash(&hash);
        assert_eq!(encoded.len(), IDENTITY_LEN);
        assert!(encoded.chars().all(|c| c.is_ascii_lowercase()));
        assert_eq!(decode_tx_hash(&encoded).unwrap(), hash);
    }
}
