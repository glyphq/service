//! KangarooTwelve (K12 / KT128) with 128-bit security.
//!
//! NOTICE: this is an independent Rust implementation written for this
//! repository. Algorithms follow RFC 9861 (KangarooTwelve and TurboSHAKE) and
//! the Keccak Team's XKCP reference (`lib/high/KangarooTwelve` and
//! `lib/high/TurboSHAKE`); only the round constants, rotation offsets, and the
//! sponge/tree construction were used as a specification, not copied code.
//!
//! Implemented from the Keccak-p[1600, 12] permutation and the KangarooTwelve
//! tree hashing construction from RFC 9861. The public entry points mirror the
//! Qubic needs: a 32-byte digest for identities, transaction hashes, and tick
//! data, and an 8-byte digest for log event digests.
//!
//! KangarooTwelve applies the optional customization string `C` by hashing
//! `M || C || length_encode(|C|)`. Qubic always uses the empty customization
//! string, which contributes a single `0x00` byte.

/// Sponge rate for TurboSHAKE128: 1600-bit state minus a 256-bit capacity.
const RATE: usize = (1600 - 256) / 8;
/// KangarooTwelve chunk size between tree nodes.
const CHUNK_SIZE: usize = 8192;
/// Domain separation byte for a single (final) node.
const SUFFIX_SINGLE_NODE: u8 = 0x07;
/// Domain separation byte for an intermediate (leaf) node.
const SUFFIX_INTERMEDIATE_NODE: u8 = 0x0B;
/// Domain separation byte for the final node of a multi-chunk tree.
const SUFFIX_FINAL_NODE: u8 = 0x06;

/// Computes KangarooTwelve(input, "", 32).
pub fn k12_32(input: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    k12_with_customization(input, &[], &mut out);
    out
}

/// Computes KangarooTwelve(input, "", 8).
pub fn k12_8(input: &[u8]) -> [u8; 8] {
    let mut out = [0u8; 8];
    k12_with_customization(input, &[], &mut out);
    out
}

/// Computes KangarooTwelve(input, customization, output.len()).
pub fn k12_with_customization(input: &[u8], customization: &[u8], output: &mut [u8]) {
    // S = M || C || length_encode(|C|)
    let mut message = Vec::with_capacity(input.len() + customization.len() + 9);
    message.extend_from_slice(input);
    message.extend_from_slice(customization);
    message.extend_from_slice(&length_encode(customization.len()));

    if message.len() <= CHUNK_SIZE {
        turboshake(&message, SUFFIX_SINGLE_NODE, output);
        return;
    }

    // FinalNode = S_0 || `03 00 00 00 00 00 00 00`
    let mut final_node = Vec::with_capacity(
        CHUNK_SIZE + 8 + (message.len() - CHUNK_SIZE).div_ceil(CHUNK_SIZE) * 32 + 10,
    );
    final_node.extend_from_slice(&message[..CHUNK_SIZE]);
    final_node.extend_from_slice(&[0x03, 0, 0, 0, 0, 0, 0, 0]);

    let tail = &message[CHUNK_SIZE..];
    let chunks = tail.len().div_ceil(CHUNK_SIZE);
    for chunk in tail.chunks(CHUNK_SIZE) {
        let mut cv = [0u8; 32];
        turboshake(chunk, SUFFIX_INTERMEDIATE_NODE, &mut cv);
        final_node.extend_from_slice(&cv);
    }

    // FinalNode = FinalNode || length_encode(n) || `FF FF`, where n is the
    // number of chaining values (one per 8192-byte leaf).
    final_node.extend_from_slice(&length_encode(chunks));
    final_node.extend_from_slice(&[0xFF, 0xFF]);

    turboshake(&final_node, SUFFIX_FINAL_NODE, output);
}

/// `length_encode(x)` from RFC 9861: big-endian digits of `x`, followed by the
/// number of digits (a single byte). `length_encode(0)` is `00`.
fn length_encode(x: usize) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(9);
    let mut value = x;
    while value > 0 {
        encoded.push((value & 0xFF) as u8);
        value >>= 8;
    }
    let digits = encoded.len();
    encoded.reverse();
    encoded.push(digits as u8);
    encoded
}

/// TurboSHAKE128: Keccak-p[1600, 12] sponge with a 168-byte rate and the
/// supplied domain separation byte. `output` is squeezed directly.
fn turboshake(input: &[u8], domain: u8, output: &mut [u8]) {
    let mut state = [0u64; 25];

    let absorb = |data: &[u8], state: &mut [u64; 25]| {
        let mut offset = 0;
        while offset < data.len() {
            let take = (data.len() - offset).min(RATE);
            for (index, byte) in data[offset..offset + take].iter().enumerate() {
                xor_byte(state, index, *byte);
            }
            offset += take;
            if take == RATE {
                keccak_p1600_12(state);
            }
        }
    };
    absorb(input, &mut state);

    // If the input ended exactly on a rate boundary the previous block was
    // already permuted, so the padding starts a fresh block.
    let position = input.len() % RATE;
    xor_byte(&mut state, position, domain);
    xor_byte(&mut state, RATE - 1, 0x80);
    keccak_p1600_12(&mut state);

    let mut written = 0;
    while written < output.len() {
        let take = (output.len() - written).min(RATE);
        for (index, slot) in output[written..written + take].iter_mut().enumerate() {
            *slot = byte_at(&state, index);
        }
        written += take;
        if written < output.len() {
            keccak_p1600_12(&mut state);
        }
    }
}

#[inline]
fn xor_byte(state: &mut [u64; 25], index: usize, value: u8) {
    state[index / 8] ^= (value as u64) << (8 * (index % 8));
}

#[inline]
fn byte_at(state: &[u64; 25], index: usize) -> u8 {
    (state[index / 8] >> (8 * (index % 8))) as u8
}

/// Keccak-p[1600, 12]: the last 12 rounds of Keccak-f[1600].
fn keccak_p1600_12(state: &mut [u64; 25]) {
    const ROUND_CONSTANTS: [u64; 24] = [
        0x0000_0000_0000_0001,
        0x0000_0000_0000_8082,
        0x8000_0000_0000_808a,
        0x8000_0000_8000_8000,
        0x0000_0000_0000_808b,
        0x0000_0000_8000_0001,
        0x8000_0000_8000_8081,
        0x8000_0000_0000_8009,
        0x0000_0000_0000_008a,
        0x0000_0000_0000_0088,
        0x0000_0000_8000_8009,
        0x0000_0000_8000_000a,
        0x0000_0000_8000_808b,
        0x8000_0000_0000_008b,
        0x8000_0000_0000_8089,
        0x8000_0000_0000_8003,
        0x8000_0000_0000_8002,
        0x8000_0000_0000_0080,
        0x0000_0000_0000_800a,
        0x8000_0000_8000_000a,
        0x8000_0000_8000_8081,
        0x8000_0000_0000_8080,
        0x0000_0000_8000_0001,
        0x8000_0000_8000_8008,
    ];
    const ROTATION: [u32; 24] = [
        1, 3, 6, 10, 15, 21, 28, 36, 45, 55, 2, 14, 27, 41, 56, 8, 25, 43, 62, 18, 39, 61, 20, 44,
    ];
    const PI_LANES: [usize; 24] = [
        10, 7, 11, 17, 18, 3, 5, 16, 8, 21, 24, 4, 15, 23, 19, 13, 12, 2, 20, 14, 22, 9, 6, 1,
    ];

    for &round_constant in &ROUND_CONSTANTS[12..] {
        // Theta
        let mut c = [0u64; 5];
        for x in 0..5 {
            c[x] = state[x] ^ state[x + 5] ^ state[x + 10] ^ state[x + 15] ^ state[x + 20];
        }
        for x in 0..5 {
            let d = c[(x + 4) % 5] ^ c[(x + 1) % 5].rotate_left(1);
            for y in 0..5 {
                state[y * 5 + x] ^= d;
            }
        }

        // Rho and Pi
        let mut current = state[1];
        for i in 0..24 {
            let lane = PI_LANES[i];
            let next = state[lane];
            state[lane] = current.rotate_left(ROTATION[i]);
            current = next;
        }

        // Chi
        for y in 0..5 {
            let row: [u64; 5] = state[y * 5..y * 5 + 5].try_into().expect("five lanes");
            for x in 0..5 {
                state[y * 5 + x] = row[x] ^ (!row[(x + 1) % 5] & row[(x + 2) % 5]);
            }
        }

        // Iota
        state[0] ^= round_constant;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn ptn(length: usize) -> Vec<u8> {
        (0..length).map(|i| (i % 251) as u8).collect()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn empty_input_32_and_64_bytes() {
        assert_eq!(
            hex(&k12_32(b"")),
            "1ac2d450fc3b4205d19da7bfca1b37513c0803577ac7167f06fe2ce1f0ef39e5"
        );

        let mut out = [0u8; 64];
        k12_with_customization(b"", &[], &mut out);
        assert_eq!(
            hex(&out),
            concat!(
                "1ac2d450fc3b4205d19da7bfca1b37513c0803577ac7167f06fe2ce1f0ef39e5",
                "4269c056b8c82e48276038b6d292966cc07a3d4645272e31ff38508139eb0a71"
            )
        );
    }

    #[test]
    fn qubic_known_answer_vectors() {
        // Generated with the reference KangarooTwelve from Qubic core
        // (`/src/kangaroo_twelve.h`), which is RFC 9861 KT128. Each value is
        // also independently reproducible with XKCP.
        assert_eq!(
            hex(&k12_32(b"abc")),
            "ab174f328c55a5510b0b209791bf8b60e801a7cfc2aa42042dcb8f547fbe3a7d"
        );
        assert_eq!(
            hex(&k12_32(b"The quick brown fox jumps over the lazy dog")),
            "b4f249b4f77c58df170aa4d1723db1127d82f1d98d25ddda561ada459cd11a48"
        );
        assert_eq!(
            hex(&k12_32(&[0u8; 32])),
            "235c3c067d94fb13b1dbd3ec5ab5c4587078c4f1442580b3f3a47687cb778089"
        );

        // 8193 bytes of the repeating pattern 0,1,...,255 exercises the
        // multi-chunk tree path (one full 8192-byte node plus a one-byte leaf).
        let patterned: Vec<u8> = (0..8193).map(|index| (index % 256) as u8).collect();
        assert_eq!(
            hex(&k12_32(&patterned)),
            "4b66dc7868e6fd5cfc7c4150486fca03990f1b723a58e4db559aae985bd48b92"
        );
    }

    #[test]
    fn qubic_identity_checksum_vector() {
        // Public key behind the identity
        // BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK.
        let public_key: [u8; 32] = [
            31, 89, 13, 3, 230, 19, 189, 222, 211, 139, 76, 8, 32, 172, 68, 97, 95, 145, 175, 18,
            67, 89, 128, 179, 237, 227, 192, 140, 49, 90, 37, 68,
        ];
        let digest = k12_32(&public_key);
        assert_eq!(
            hex(&digest),
            "c4eb5aafb52e38bc7ec3c991d9a845d6fb9909dbaad68bd008ebc7ce9163568a"
        );

        // Identity checksum: low 18 bits of the first three digest bytes as a
        // little-endian u32, rendered as four base-26 characters.
        let mut checksum = u32::from_le_bytes([digest[0], digest[1], digest[2], 0]) & 0x3FFFF;
        let mut suffix = String::new();
        for _ in 0..4 {
            suffix.push((b'A' + (checksum % 26) as u8) as char);
            checksum /= 26;
        }
        assert_eq!(suffix, "QEXK");
    }

    #[test]
    fn rfc9861_kt128_vectors_empty_customization() {
        let vectors: &[(usize, &str)] = &[
            (
                0,
                "1ac2d450fc3b4205d19da7bfca1b37513c0803577ac7167f06fe2ce1f0ef39e5",
            ),
            (
                1,
                "2bda92450e8b147f8a7cb629e784a058efca7cf7d8218e02d345dfaa65244a1f",
            ),
            (
                17,
                "6bf75fa2239198db4772e36478f8e19b0f371205f6a9a93a273f51df37122888",
            ),
            (
                289,
                "0c315ebcdedbf61426de7dcf8fb725d1e74675d7f5327a5067f367b108ecb67c",
            ),
            (
                4913,
                "cb552e2ec77d9910701d578b457ddf772c12e322e4ee7fe417f92c758f0d59d0",
            ),
            (
                83521,
                "8701045e22205345ff4dda05555cbb5c3af1a771c2b89baef37db43d9998b9fe",
            ),
            (
                1419857,
                "844d610933b1b9963cbdeb5ae3b6b05cc7cbd67ceedf883eb678a0a8e0371682",
            ),
            (
                8191,
                "1b577636f723643e990cc7d6a659837436fd6a103626600eb8301cd1dbe553d6",
            ),
            (
                8192,
                "48f256f6772f9edfb6a8b661ec92dc93b95ebd05a08a17b39ae3490870c926c3",
            ),
        ];

        for &(length, expected) in vectors {
            let input = ptn(length);
            assert_eq!(hex(&k12_32(&input)), expected, "ptn({length})");
        }
    }

    #[test]
    fn rfc9861_kt128_vectors_with_customization() {
        // KT128(`00`^0, C=ptn(1), 32)
        let mut out = [0u8; 32];
        k12_with_customization(b"", &ptn(1), &mut out);
        assert_eq!(
            hex(&out),
            "fab658db63e94a246188bf7af69a133045f46ee984c56e3c3328caaf1aa1a583"
        );

        // KT128(`FF`, C=ptn(41), 32)
        k12_with_customization(&[0xFF], &ptn(41), &mut out);
        assert_eq!(
            hex(&out),
            "d848c5068ced736f4462159b9867fd4c20b808acc3d5bc48e0b06ba0a3762ec4"
        );

        // KT128(ptn(8192), C=ptn(8189), 32)
        k12_with_customization(&ptn(8192), &ptn(8189), &mut out);
        assert_eq!(
            hex(&out),
            "3ed12f70fb05ddb58689510ab3e4d23c6c6033849aa01e1d8c220a297fedcd0b"
        );

        // KT128(ptn(8192), C=ptn(8190), 32)
        k12_with_customization(&ptn(8192), &ptn(8190), &mut out);
        assert_eq!(
            hex(&out),
            "6a7c1b6a5cd0d8c9ca943a4a216cc64604559a2ea45f78570a15253d67ba00ae"
        );
    }

    #[test]
    fn eight_byte_output_is_prefix_of_32_byte_output() {
        let input = b"qubic log digest";
        let short = k12_8(input);
        let long = k12_32(input);
        assert_eq!(short, long[..8]);
    }

    #[test]
    fn length_encode_matches_rfc_examples() {
        assert_eq!(length_encode(0), vec![0x00]);
        assert_eq!(length_encode(12), vec![0x0C, 0x01]);
        assert_eq!(length_encode(65538), vec![0x01, 0x00, 0x02, 0x03]);
    }

    #[test]
    fn tree_boundary_lengths_are_stable() {
        // These exercise the single-node, exact-chunk and multi-chunk paths.
        for length in [8191, 8192, 8193, 16383, 16384, 16385] {
            let input = ptn(length);
            let digest = k12_32(&input);
            assert_eq!(digest, k12_32(&input));
        }
    }

    #[test]
    fn hash_is_deterministic_and_short_output_is_a_prefix() {
        for length in (0..=64).chain([100, 167, 168, 169, 8191, 8192, 8193, 9000]) {
            let input = ptn(length);
            let digest = k12_32(&input);
            assert_eq!(digest, k12_32(&input), "determinism at length {length}");
            assert_eq!(
                k12_8(&input),
                digest[..8],
                "8-byte output must prefix 32-byte output at length {length}"
            );
        }

        // A one-byte change must change the digest.
        let mut input = ptn(8193);
        let before = k12_32(&input);
        input[8192] ^= 0x01;
        assert_ne!(before, k12_32(&input));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[test]
        fn arbitrary_messages_are_deterministic_with_stable_prefixes(
            input in proptest::collection::vec(any::<u8>(), 0..4096)
        ) {
            let digest = k12_32(&input);
            prop_assert_eq!(digest, k12_32(&input));
            prop_assert_eq!(k12_8(&input).to_vec(), digest[..8].to_vec());
        }

        #[test]
        fn customization_separates_the_digest(
            input in proptest::collection::vec(any::<u8>(), 0..256),
            customization in proptest::collection::vec(any::<u8>(), 1..128),
        ) {
            let mut plain = [0u8; 32];
            k12_with_customization(&input, &[], &mut plain);
            let mut custom = [0u8; 32];
            k12_with_customization(&input, &customization, &mut custom);
            prop_assert_ne!(plain, custom);
        }
    }
}
