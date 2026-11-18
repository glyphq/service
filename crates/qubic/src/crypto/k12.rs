//! KangarooTwelve (K12) with 128-bit security.
//!
//! Implemented from the Keccak-p[1600, 12] permutation and the KangarooTwelve
//! tree hashing construction. The public entry points mirror the Qubic needs:
//! a 32-byte digest for identities, transaction hashes, and tick data, and an
//! 8-byte digest for log event digests.

/// Computes KangarooTwelve(input, "", 32).
pub fn k12_32(_input: &[u8]) -> [u8; 32] {
    todo!("implement Keccak-p[1600,12] sponge and KangarooTwelve tree hashing")
}

/// Computes KangarooTwelve(input, "", 8).
pub fn k12_8(_input: &[u8]) -> [u8; 8] {
    todo!("implement 8-byte K12 output")
}

/// Computes KangarooTwelve(input, customization, output.len()).
pub fn k12_with_customization(_input: &[u8], _customization: &[u8], _output: &mut [u8]) {
    todo!("implement customised KangarooTwelve")
}
