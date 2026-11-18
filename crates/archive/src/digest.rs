//! Deterministic 64-bit content hashing.
//!
//! The archive needs a stable, dependency-free digest that can be written to a
//! manifest and compared months later. `std`'s `DefaultHasher` explicitly does
//! not promise stability across releases, so bundles use XXH64 instead. This
//! is a checksum against corruption and accidental mixing, not a
//! cryptographic signature; verification of Qubic data happens before a bundle
//! is written.

const P1: u64 = 0x9E37_79B1_85EB_CA87;
const P2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const P3: u64 = 0x1656_67B1_9E37_79F9;
const P4: u64 = 0x85EB_CA77_C2B2_AE63;
const P5: u64 = 0x27D4_EB2F_1656_67C5;

/// Hash `data` with XXH64 and the given seed.
///
/// The implementation matches the reference XXH64 for all lengths, including
/// empty input.
pub fn xxh64(data: &[u8], seed: u64) -> u64 {
    let total_len = data.len() as u64;
    let mut rest = data;
    let mut hash;

    if rest.len() >= 32 {
        let mut v1 = seed.wrapping_add(P1).wrapping_add(P2);
        let mut v2 = seed.wrapping_add(P2);
        let mut v3 = seed;
        let mut v4 = seed.wrapping_sub(P1);

        while rest.len() >= 32 {
            v1 = round(v1, read_u64(&rest[0..8]));
            v2 = round(v2, read_u64(&rest[8..16]));
            v3 = round(v3, read_u64(&rest[16..24]));
            v4 = round(v4, read_u64(&rest[24..32]));
            rest = &rest[32..];
        }

        hash = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        hash = merge_round(hash, v1);
        hash = merge_round(hash, v2);
        hash = merge_round(hash, v3);
        hash = merge_round(hash, v4);
    } else {
        hash = seed.wrapping_add(P5);
    }

    hash = hash.wrapping_add(total_len);

    while rest.len() >= 8 {
        let k1 = round(0, read_u64(&rest[0..8]));
        hash ^= k1;
        hash = hash.rotate_left(27).wrapping_mul(P1).wrapping_add(P4);
        rest = &rest[8..];
    }

    if rest.len() >= 4 {
        hash ^= u64::from(read_u32(&rest[0..4])).wrapping_mul(P1);
        hash = hash.rotate_left(23).wrapping_mul(P2).wrapping_add(P3);
        rest = &rest[4..];
    }

    for &byte in rest {
        hash ^= u64::from(byte).wrapping_mul(P5);
        hash = hash.rotate_left(11).wrapping_mul(P1);
    }

    avalanche(hash)
}

/// Lower-case hexadecimal digest, 16 characters.
pub fn hex64(value: u64) -> String {
    format!("{value:016x}")
}

#[inline]
fn round(acc: u64, input: u64) -> u64 {
    acc.wrapping_add(input.wrapping_mul(P2))
        .rotate_left(31)
        .wrapping_mul(P1)
}

#[inline]
fn merge_round(acc: u64, value: u64) -> u64 {
    (acc ^ round(0, value)).wrapping_mul(P1).wrapping_add(P4)
}

#[inline]
fn avalanche(mut hash: u64) -> u64 {
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(P2);
    hash ^= hash >> 29;
    hash = hash.wrapping_mul(P3);
    hash ^= hash >> 32;
    hash
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("slice is exactly 8 bytes"))
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().expect("slice is exactly 4 bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        assert_eq!(xxh64(b"", 0), 0xEF46_DB37_51D8_E999);
        assert_eq!(xxh64(b"a", 0), 0xD24E_C4F1_A98C_6E5B);
        assert_eq!(xxh64(b"abc", 0), 0x44BC_2CF5_AD77_0999);
    }

    #[test]
    fn seeds_change_the_digest() {
        assert_ne!(xxh64(b"tick", 0), xxh64(b"tick", 1));
    }

    #[test]
    fn every_length_band_is_covered() {
        let data: Vec<u8> = (0..=255).cycle().take(97).collect();
        for len in [0, 1, 3, 4, 7, 8, 31, 32, 33, 64, 97] {
            let digest = xxh64(&data[..len], 0);
            assert_eq!(digest.to_le_bytes().len(), 8);
        }
        assert_ne!(xxh64(&data[..32], 0), xxh64(&data[..32], 7));
    }

    #[test]
    fn hex_is_zero_padded() {
        assert_eq!(hex64(0), "0000000000000000");
        assert_eq!(hex64(0xab), "00000000000000ab");
    }
}
