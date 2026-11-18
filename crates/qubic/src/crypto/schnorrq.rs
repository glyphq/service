//! SchnorrQ signatures over the FourQ curve.
//!
//! NOTICE: this is an independent Rust implementation written for this
//! repository. It follows the FourQ curve in "FourQ: four-dimensional
//! decompositions on a Q-curve over the Mersenne prime" (Costello and Longa,
//! ASIACRYPT 2015), the SchnorrQ technical report by the same authors, and
//! Microsoft's FourQlib reference implementation, plus the verification variant
//! used by the Qubic core (`src/four_q.h`), which hashes the message digest with
//! KangarooTwelve. No third-party source was copied verbatim.
//!
//! Implemented from scratch from the FourQ/SchnorrQ specification. The field is
//! GF(p^2) with p = 2^127 - 1 and i^2 = -1; the curve is the twisted Edwards
//! curve `-x^2 + y^2 = 1 + d*x^2*y^2`. Point arithmetic uses extended twisted
//! Edwards coordinates and point decoding follows the FourQ compression scheme.
//!
//! Qubic signs `KangarooTwelve(message)` and serializes the signature as
//! `R || S` (32 bytes each, little-endian), where
//! `h = K12(R || public_key || message_digest, 64)` and verification checks
//! `S*G + h*A == R`. Signatures with a non-canonical scalar `S >= r` are
//! rejected, and low-order public keys from the cofactor subgroup are rejected.

use crate::crypto::k12;

/// GF(p) modulus, p = 2^127 - 1.
const P: u128 = (1u128 << 127) - 1;
/// The prime-order subgroup order r of FourQ.
const ORDER: [u64; 4] = [
    0x2FB2_540E_C776_8CE7,
    0xDFBD_004D_FE0F_7999,
    0xF053_9782_9CBC_14E5,
    0x0029_CBC1_4E5E_0A72,
];

/// Field element of GF(p), stored canonically in `0..p`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Fp(u128);

impl Fp {
    const ZERO: Self = Self(0);
    const ONE: Self = Self(1);
    /// Inverse of two modulo p, i.e. 2^126, since 2 * 2^126 = 2^127 = 1 mod p.
    const HALF: Self = Self(1u128 << 126);
    /// d = d0 + d1*i, the FourQ curve parameter.
    const D: Fp2 = Fp2 {
        a: Fp(0x0000_0000_0000_00E4_0000_0000_0000_0142),
        b: Fp(0x5E47_2F84_6657_E0FC_B382_1488_F1FC_0C8D),
    };

    #[inline]
    fn new(value: u128) -> Self {
        Self(Self::reduce(value))
    }

    /// Reduces an arbitrary 128-bit value modulo 2^127 - 1.
    #[inline]
    fn reduce(value: u128) -> u128 {
        let mut folded = (value & P) + (value >> 127);
        if folded >= P {
            folded -= P;
        }
        folded
    }

    #[inline]
    fn is_zero(self) -> bool {
        self.0 == 0
    }

    #[inline]
    fn add(self, other: Self) -> Self {
        // Both operands are < p < 2^127, so the sum fits in a u128.
        Self(Self::reduce(self.0 + other.0))
    }

    #[inline]
    fn dbl(self) -> Self {
        self.add(self)
    }

    #[inline]
    fn sub(self, other: Self) -> Self {
        if self.0 >= other.0 {
            Self(self.0 - other.0)
        } else {
            Self(self.0 + P - other.0)
        }
    }

    #[inline]
    fn neg(self) -> Self {
        if self.0 == 0 { self } else { Self(P - self.0) }
    }

    #[inline]
    fn mul(self, other: Self) -> Self {
        // 128x128 -> 256-bit product in base 2^64, then fold modulo 2^127-1.
        let (a0, a1) = (self.0 as u64, (self.0 >> 64) as u64);
        let (b0, b1) = (other.0 as u64, (other.0 >> 64) as u64);

        let mut limbs = [0u64; 4];
        add_128(&mut limbs, (a0 as u128) * (b0 as u128), 0);
        add_128(&mut limbs, (a0 as u128) * (b1 as u128), 1);
        add_128(&mut limbs, (a1 as u128) * (b0 as u128), 1);
        add_128(&mut limbs, (a1 as u128) * (b1 as u128), 2);

        let low = (limbs[0] as u128) | (((limbs[1] & 0x7FFF_FFFF_FFFF_FFFF) as u128) << 64);
        let high =
            ((limbs[1] >> 63) as u128) | ((limbs[2] as u128) << 1) | ((limbs[3] as u128) << 65);
        Self(Self::reduce(low + high))
    }

    #[inline]
    fn sqr(self) -> Self {
        self.mul(self)
    }

    fn pow(self, mut exponent: u128) -> Self {
        let mut result = Self::ONE;
        let mut base = self;
        while exponent > 0 {
            if exponent & 1 == 1 {
                result = result.mul(base);
            }
            exponent >>= 1;
            if exponent > 0 {
                base = base.sqr();
            }
        }
        result
    }

    #[inline]
    fn inv(self) -> Self {
        // Fermat: a^(p-2) = a^(2^127-3).
        self.pow(P - 2)
    }

    #[inline]
    fn half(self) -> Self {
        self.mul(Self::HALF)
    }

    /// a^(2^125 - 1), the exponent used by FourQ point decoding.
    fn pow_2_125_minus_1(self) -> Self {
        let mut result = self;
        let mut base = self;
        for _ in 1..125 {
            base = base.sqr();
            result = result.mul(base);
        }
        result
    }
}

/// Adds a 128-bit value at 64-bit limb `index`, propagating carries.
fn add_128(limbs: &mut [u64; 4], value: u128, index: usize) {
    add_limb(limbs, value as u64, index);
    add_limb(limbs, (value >> 64) as u64, index + 1);
}

fn add_limb(limbs: &mut [u64; 4], value: u64, mut index: usize) {
    let mut carry = value;
    while carry != 0 && index < limbs.len() {
        let (sum, overflow) = limbs[index].overflowing_add(carry);
        limbs[index] = sum;
        carry = u64::from(overflow);
        index += 1;
    }
}

/// Element of GF(p^2) = GF(p)[i]/(i^2 + 1), stored as `a + b*i`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Fp2 {
    a: Fp,
    b: Fp,
}

impl Fp2 {
    const ZERO: Self = Self {
        a: Fp::ZERO,
        b: Fp::ZERO,
    };
    const ONE: Self = Self {
        a: Fp::ONE,
        b: Fp::ZERO,
    };

    const fn new(a: u128, b: u128) -> Self {
        Self { a: Fp(a), b: Fp(b) }
    }

    #[inline]
    fn is_zero(self) -> bool {
        self.a.is_zero() && self.b.is_zero()
    }

    #[inline]
    fn add(self, other: Self) -> Self {
        Self {
            a: self.a.add(other.a),
            b: self.b.add(other.b),
        }
    }

    #[inline]
    fn dbl(self) -> Self {
        self.add(self)
    }

    #[inline]
    fn sub(self, other: Self) -> Self {
        Self {
            a: self.a.sub(other.a),
            b: self.b.sub(other.b),
        }
    }

    #[inline]
    fn neg(self) -> Self {
        Self {
            a: self.a.neg(),
            b: self.b.neg(),
        }
    }

    #[inline]
    fn mul(self, other: Self) -> Self {
        let ac = self.a.mul(other.a);
        let bd = self.b.mul(other.b);
        let cross = self.a.add(self.b).mul(other.a.add(other.b));
        Self {
            a: ac.sub(bd),
            b: cross.sub(ac).sub(bd),
        }
    }

    #[inline]
    fn sqr(self) -> Self {
        self.mul(self)
    }

    fn inv(self) -> Self {
        // (a + b*i)^-1 = (a - b*i) / (a^2 + b^2)
        let norm = self.a.sqr().add(self.b.sqr());
        let norm_inv = norm.inv();
        Self {
            a: self.a.mul(norm_inv),
            b: self.b.neg().mul(norm_inv),
        }
    }
}

/// A FourQ point in extended twisted Edwards coordinates (X:Y:Z:T).
#[derive(Clone, Copy, Debug)]
struct Point {
    x: Fp2,
    y: Fp2,
    z: Fp2,
    t: Fp2,
}

impl Point {
    fn identity() -> Self {
        Self {
            x: Fp2::ZERO,
            y: Fp2::ONE,
            z: Fp2::ONE,
            t: Fp2::ZERO,
        }
    }

    fn affine(x: Fp2, y: Fp2) -> Self {
        Self {
            x,
            y,
            z: Fp2::ONE,
            t: x.mul(y),
        }
    }

    /// The FourQ generator.
    fn generator() -> Self {
        let x = Fp2::new(
            0x1A34_7223_7C2F_B305_2865_92AD_7B38_33AA,
            0x1E1F_553F_2878_AA9C_9686_9FB3_60AC_77F6,
        );
        let y = Fp2::new(
            0x0E3F_EE9B_A120_785A_B924_A246_2BCB_B287,
            0x6E1C_4AF8_630E_0242_49A7_C344_844C_8B5C,
        );
        Self::affine(x, y)
    }

    fn is_identity(&self) -> bool {
        self.x.is_zero() && self.y == self.z
    }

    /// Unified extended-coordinate addition (a = -1, `add-2008-hwcd-3`).
    fn add(&self, other: &Self) -> Self {
        let a = self.y.sub(self.x).mul(other.y.sub(other.x));
        let b = self.y.add(self.x).mul(other.y.add(other.x));
        let c = self.t.mul(Fp::D.dbl()).mul(other.t);
        let d = self.z.mul(other.z).dbl();
        let e = b.sub(a);
        let f = d.sub(c);
        let g = d.add(c);
        let h = b.add(a);
        Self {
            x: e.mul(f),
            y: g.mul(h),
            t: e.mul(h),
            z: f.mul(g),
        }
    }

    /// Extended-coordinate doubling (`dbl-2008-hwcd`, a = -1).
    fn dbl(&self) -> Self {
        let a = self.x.sqr();
        let b = self.y.sqr();
        let c = self.z.sqr().dbl();
        let d = a.neg();
        let e = self.x.add(self.y).sqr().sub(a).sub(b);
        let g = d.add(b);
        let f = g.sub(c);
        let h = d.sub(b);
        Self {
            x: e.mul(f),
            y: g.mul(h),
            t: e.mul(h),
            z: f.mul(g),
        }
    }

    fn mul(&self, scalar: &[u64; 4]) -> Self {
        let mut result = Self::identity();
        for bit in (0..256).rev() {
            result = result.dbl();
            if (scalar[bit / 64] >> (bit % 64)) & 1 == 1 {
                result = result.add(self);
            }
        }
        result
    }

    fn is_on_curve(&self) -> bool {
        // a*X^2 + Y^2 = Z^2 + d*T^2 with a = -1.
        let lhs = self.y.sqr().sub(self.x.sqr());
        let rhs = self.z.sqr().add(Fp::D.mul(self.t.sqr()));
        lhs == rhs
    }

    fn to_affine(self) -> (Fp2, Fp2) {
        let z_inv = self.z.inv();
        (self.x.mul(z_inv), self.y.mul(z_inv))
    }
}

/// Compresses a point to the 32-byte FourQ encoding. The low bit of the top
/// byte carries the sign bit of `x`, matching the FourQ reference encoding.
fn encode(point: &Point) -> [u8; 32] {
    let (x, y) = point.to_affine();
    let mut out = [0u8; 32];
    out[..16].copy_from_slice(&y.a.0.to_le_bytes());
    out[16..].copy_from_slice(&y.b.0.to_le_bytes());

    let x_component = if x.a.is_zero() { x.b } else { x.a };
    // The reference reads bit 126 of the real x-coordinate (bit 62 of its high
    // 64-bit limb), or of the imaginary part when the real part is zero.
    if (x_component.0 >> 126) & 1 == 1 {
        out[31] |= 0x80;
    }
    out
}

/// Decompresses a 32-byte FourQ point encoding, following the reference decode
/// algorithm. Returns `None` if the encoding is not a point on the curve.
fn decode(encoded: &[u8; 32]) -> Option<Point> {
    let y_a = u128::from_le_bytes(encoded[..16].try_into().expect("16 bytes"));
    let mut y_b = u128::from_le_bytes(encoded[16..].try_into().expect("16 bytes"));
    let sign = (y_b >> 127) & 1;
    y_b &= (1u128 << 127) - 1;
    let y = Fp2 {
        a: Fp::new(y_a),
        b: Fp::new(y_b),
    };

    let y_squared = y.sqr();
    let v = y_squared.mul(Fp::D).add(Fp2::ONE);
    let u = y_squared.sub(Fp2::ONE);

    // t0 = norm(v), t1 = re(u * conj(v)), t2 = im(u * conj(v))
    let t0 = v.a.sqr().add(v.b.sqr());
    let t1 = u.a.mul(v.a).add(u.b.mul(v.b));
    let t2 = u.b.mul(v.a).sub(u.a.mul(v.b));

    let mut t3 = t1.sqr().add(t2.sqr());
    for _ in 0..125 {
        t3 = t3.sqr();
    }

    let mut t = t1.add(t3);
    if t.is_zero() {
        t = t1.sub(t3);
    }
    t = t.dbl();

    let mut t3 = t0.sqr();
    t3 = t0.mul(t3);
    t3 = t3.mul(t);

    let r = t3.pow_2_125_minus_1();
    let t3 = t0.mul(r);

    let mut x0 = t.mul(t3);
    let t1 = t0.mul(x0.sqr());
    x0 = x0.half();
    let x1 = t3.mul(t2);

    let (mut real, mut imaginary) = (x0, x1);
    if !t.sub(t1).is_zero() {
        core::mem::swap(&mut real, &mut imaginary);
    }

    let mut x = Fp2 {
        a: real,
        b: imaginary,
    };
    let x_component = if x.a.is_zero() { x.b } else { x.a };
    if (x_component.0 >> 126) & 1 != sign {
        x = x.neg();
    }

    let point = Point::affine(x, y);
    if point.is_on_curve() {
        return Some(point);
    }

    // The square-root step can pick the other root; try the conjugate x.
    let point = Point::affine(
        Fp2 {
            a: x.a,
            b: x.b.neg(),
        },
        y,
    );
    point.is_on_curve().then_some(point)
}

/// Reads a 32-byte little-endian scalar.
fn scalar_from_bytes(bytes: &[u8; 32]) -> [u64; 4] {
    let mut limbs = [0u64; 4];
    for (index, chunk) in bytes.chunks_exact(8).enumerate() {
        limbs[index] = u64::from_le_bytes(chunk.try_into().expect("8 bytes"));
    }
    limbs
}

fn scalar_is_less(a: &[u64; 4], b: &[u64; 4]) -> bool {
    for index in (0..4).rev() {
        if a[index] < b[index] {
            return true;
        }
        if a[index] > b[index] {
            return false;
        }
    }
    false
}

#[cfg(test)]
fn scalar_sub(a: &[u64; 4], b: &[u64; 4]) -> [u64; 4] {
    let mut out = [0u64; 4];
    let mut borrow = 0u64;
    for index in 0..4 {
        let (value, first) = a[index].overflowing_sub(b[index]);
        let (value, second) = value.overflowing_sub(borrow);
        out[index] = value;
        borrow = u64::from(first || second);
    }
    out
}

/// Reduces a 256-bit scalar modulo the FourQ subgroup order.
#[cfg(test)]
fn reduce_scalar(mut scalar: [u64; 4]) -> [u64; 4] {
    while !scalar_is_less(&scalar, &ORDER) {
        scalar = scalar_sub(&scalar, &ORDER);
    }
    scalar
}

/// Verifies SchnorrQ(public_key, K12(message)).
pub fn verify(public_key: &[u8; 32], message: &[u8], signature: &[u8; 64]) -> bool {
    let digest = k12::k12_32(message);
    verify_digest(public_key, &digest, signature)
}

/// Verifies SchnorrQ(public_key, message_digest).
pub fn verify_digest(
    public_key: &[u8; 32],
    message_digest: &[u8; 32],
    signature: &[u8; 64],
) -> bool {
    // Qubic rejects encodings where the real part of y does not fit in 127 bits.
    if public_key[15] & 0x80 != 0 || signature[15] & 0x80 != 0 {
        return false;
    }

    // S must be canonical: 0 <= S < r. This blocks the malleable twin S + r.
    let s = scalar_from_bytes(&signature[32..].try_into().expect("32 bytes"));
    if !scalar_is_less(&s, &ORDER) {
        return false;
    }

    // The identity encoding is a trivially forgeable public key.
    if is_encoded_identity(public_key) {
        return false;
    }

    let Some(public_point) = decode(public_key) else {
        return false;
    };

    // Reject low-order public keys: any point in the 392-element cofactor
    // subgroup satisfies [392]A == O and permits forgery. Legitimate keys have
    // order r, so this check passes for them.
    let cofactor_multiple = public_point.mul(&[392, 0, 0, 0]);
    if cofactor_multiple.is_identity() {
        return false;
    }

    // h = K12(R || public_key || message_digest, 64), take the first 32 bytes.
    let mut input = [0u8; 96];
    input[..32].copy_from_slice(&signature[..32]);
    input[32..64].copy_from_slice(public_key);
    input[64..].copy_from_slice(message_digest);
    let mut hash = [0u8; 64];
    k12::k12_with_customization(&input, &[], &mut hash);
    // The reference double-scalar multiplication consumes the full 256-bit
    // value of h (K12's first 32 bytes, little-endian); it is not reduced
    // modulo r. For prime-order public keys this is equivalent, but keeping the
    // full scalar matches the core's semantics for every accepted key.
    let h = scalar_from_bytes(&hash[..32].try_into().expect("32 bytes"));

    // R' = S*G + h*A, then compare its encoding with the signature's R.
    let expected = Point::generator().mul(&s).add(&public_point.mul(&h));
    encode(&expected) == signature[..32]
}

fn is_encoded_identity(public_key: &[u8; 32]) -> bool {
    public_key[0] == 1
        && public_key[1..31].iter().all(|byte| *byte == 0)
        && public_key[31] & 0x7F == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::encode_base26;
    use proptest::prelude::*;

    fn hex_to_32(value: &str) -> [u8; 32] {
        let bytes = hex::decode(value).expect("hex");
        bytes.try_into().expect("32 bytes")
    }

    fn hex_to_64(value: &str) -> [u8; 64] {
        let bytes = hex::decode(value).expect("hex");
        bytes.try_into().expect("64 bytes")
    }

    /// Qubic key derivation: subseed = K12(seed letters), private = K12(subseed).
    fn private_key_from_seed(seed: &str) -> [u8; 32] {
        let mut letters = [0u8; 55];
        for (index, byte) in seed.bytes().enumerate() {
            assert!(byte.is_ascii_lowercase());
            letters[index] = byte - b'a';
        }
        let subseed = k12::k12_32(&letters);
        k12::k12_32(&subseed)
    }

    fn public_key_from_private(private_key: &[u8; 32]) -> [u8; 32] {
        let raw = scalar_from_bytes(private_key);
        let scalar = reduce_scalar(raw);
        assert_eq!(
            encode(&Point::generator().mul(&raw)),
            encode(&Point::generator().mul(&scalar)),
            "reduction mismatch raw={:?} reduced={:?}",
            raw,
            scalar
        );
        encode(&Point::generator().mul(&scalar))
    }

    #[test]
    fn generator_is_on_curve() {
        assert!(Point::generator().is_on_curve());
        assert!(!Point::generator().is_identity());
    }

    #[test]
    fn generator_has_order_r() {
        assert!(Point::generator().mul(&ORDER).is_identity());
    }

    #[test]
    fn known_qubic_identity() {
        // Seed "aaaa..." (55 letters) produces the well-known Qubic identity
        // BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK.
        let seed = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let private_key = private_key_from_seed(seed);
        assert_eq!(
            encode_base26(&private_key, true, true),
            "CCTWBAULWUYHYBIJYKXRMXNYRVZBALWRYIIAHLTFWANUAFHYFHEPCJJGVAEC"
        );
        let public_key = public_key_from_private(&private_key);
        assert_eq!(
            encode_base26(&public_key, true, true),
            "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK"
        );
    }

    #[test]
    fn qubic_signature_vectors_verify() {
        // (subseed, public key, message digest, signature). Generated with the
        // reference FourQ implementation shipped in Qubic's `four_q.h`, whose
        // `verify()` reports success for every row.
        let vectors: &[(&str, &str, &str, &str)] = &[
            (
                "31363b40454a4f54595e63686d72777c81868b90959a9fa4a9aeb3b8bdc2c7cc",
                "228af8607cef0cb91fc2677323bc13672a5825294ff6dbb7691df5f17a20a94b",
                "808386898c8f9295989b9ea1a4a7aaadb0b3b6b9bcbfc2c5c8cbced1d4d7dadd",
                "f99cf9a12e072df828b11ecead46e05d132463d0f25ced4a1133414eda139de8488bc2ad7120a670cb0087195c7c2675ad90565ad106c59b2f1f5ff2267f1f00",
            ),
            (
                "484d52575c61666b70757a7f84898e93989da2a7acb1b6bbc0c5cacfd4d9dee3",
                "e57371ded864875e79c044e2891c58721b99988116a8f350b93265b1f2329f8a",
                "9194979a9da0a3a6a9acafb2b5b8bbbec1c4c7cacdd0d3d6d9dcdfe2e5e8ebee",
                "6a2c82c4940bd41804a86f74e7fe28640d5c717e98dc1ab6bff08894baeeafc5a38230cac9ef3d0f2df0f6fafe4344ba4dfc897dfe118f28ed84d35140830200",
            ),
            (
                "5f64696e73787d82878c91969ba0a5aaafb4b9bec3c8cdd2d7dce1e6ebf0f5fa",
                "b3089e959e98a9e18e3d9ffbfbe1fc607b670bc397cd1b59a73ec3f03b4b6b1c",
                "a2a5a8abaeb1b4b7babdc0c3c6c9cccfd2d5d8dbdee1e4e7eaedf0f3f6f9fcff",
                "2d7a6fc803559fa0b0bf123f194f1049ff009c5e1c5c7c69d1e06ef1a3267ffbca30e24eca6715bb3cce4a1b6c609854bea6b2eca59d87e3600caaf668721500",
            ),
            (
                "767b80858a8f94999ea3a8adb2b7bcc1c6cbd0d5dadfe4e9eef3f8fd02070c11",
                "a9bddf4052aff9113489cd18ea641e3fb968414ff6e224097aeb330352b0bd97",
                "b3b6b9bcbfc2c5c8cbced1d4d7dadde0e3e6e9eceff2f5f8fbfe0104070a0d10",
                "87f7f0e6eeee5594b1a7df2b16b6897ce1a09ac00905903648debc65257803d37d657028d6beddd7fbeff214db9e1ad9fa138bc8b5feadc2729475480ce82000",
            ),
        ];

        for (index, (subseed, public_key, digest, signature)) in vectors.iter().enumerate() {
            let subseed = hex_to_32(subseed);
            let private_key = k12::k12_32(&subseed);
            let derived_public_key = public_key_from_private(&private_key);
            assert_eq!(
                hex::encode(derived_public_key),
                *public_key,
                "vector {index} public key derivation"
            );
            let digest = hex_to_32(digest);
            let signature = hex_to_64(signature);
            assert!(
                verify_digest(&derived_public_key, &digest, &signature),
                "vector {index} should verify"
            );
        }
    }

    #[test]
    #[ignore]
    fn bench_verify_digest_throughput() {
        let public_key =
            hex_to_32("1f590d03e613bdded38b4c0820ac44615f91af12435980b3ede3c08c315a2544");
        let signature = hex_to_64(
            "c8e4a68a5aa3c3588959e994fb958c25697ffe1631b4caafec7ee090292077b56\
             0c614d87ea660c0fcacf7522f533125e35eba9abd3c6fcf3b99ce66db9c1800",
        );
        let digest = super::k12::k12_32(&10u64.to_le_bytes());
        let start = std::time::Instant::now();
        let count = 20_000;
        for _ in 0..count {
            assert!(verify_digest(&public_key, &digest, &signature));
        }
        let elapsed = start.elapsed();
        println!(
            "{} verifies in {:?} = {:.1} us/verify, {:.0} verify/s/core",
            count,
            elapsed,
            elapsed.as_secs_f64() * 1e6 / count as f64,
            count as f64 / elapsed.as_secs_f64()
        );
    }

    #[test]
    fn known_qubic_identity_signature_verifies() {
        // Qubic documentation vector: the well-known identity
        // BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK signs
        // the little-endian u64 value 10.
        let public_key =
            hex_to_32("1f590d03e613bdded38b4c0820ac44615f91af12435980b3ede3c08c315a2544");
        let signature = hex_to_64(
            "c8e4a68a5aa3c3588959e994fb958c25697ffe1631b4caafec7ee090292077b56\
             0c614d87ea660c0fcacf7522f533125e35eba9abd3c6fcf3b99ce66db9c1800",
        );
        let message = 10u64.to_le_bytes();

        assert_eq!(
            hex::encode(k12::k12_32(&message)),
            "5b46aa533f2b2125f20143e54e65173a3db2fc6bd7bf2b134718e98c19f0802d"
        );
        assert!(verify(&public_key, &message, &signature));

        // Flipping any bit of R or S must invalidate the signature.
        for index in [0usize, 15, 31, 32, 47, 63] {
            let mut tampered = signature;
            tampered[index] ^= 0x01;
            assert!(!verify(&public_key, &message, &tampered));
        }
        let mut tampered_message = message;
        tampered_message[0] ^= 0x01;
        assert!(!verify(&public_key, &tampered_message, &signature));

        // The identity string maps to the same public key bytes.
        assert_eq!(
            hex::encode(
                crate::identity::PublicKey::from_identity(
                    "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK",
                )
                .expect("valid identity")
                .as_bytes(),
            ),
            "1f590d03e613bdded38b4c0820ac44615f91af12435980b3ede3c08c315a2544"
        );
    }

    #[test]
    fn rejects_tampered_signatures_and_messages() {
        let subseed = hex_to_32("4ac19e2bf0d3776519aabe31924f7dc2589b3d0e7411a65f84c9b16df72c038e");
        let digest = hex_to_32("94e120a4d3f58c217a53eb9046d9f2c5b11288a9fe340d6ce5a771cf04b82e63");
        let signature = hex_to_64(
            "357d47b1366f33eed311a4458ec7326d35728e9292328a9b7ff8d4ec0f7b0df9323f5d1cd01bd5a380a1a8e4f29ad3ae9c5d94e84f4181a61ca73030d6d11600",
        );
        let public_key = public_key_from_private(&k12::k12_32(&subseed));
        assert!(verify_digest(&public_key, &digest, &signature));

        for index in [0usize, 10, 31, 32, 47, 63] {
            let mut tampered = signature;
            tampered[index] ^= 0x01;
            assert!(!verify_digest(&public_key, &digest, &tampered));
        }

        let mut wrong_digest = digest;
        wrong_digest[0] ^= 0x80;
        assert!(!verify_digest(&public_key, &wrong_digest, &signature));

        let mut wrong_key = public_key;
        wrong_key[5] ^= 0x04;
        assert!(!verify_digest(&wrong_key, &digest, &signature));
    }

    #[test]
    fn every_single_byte_signature_mutation_is_rejected() {
        let public_key =
            hex_to_32("1f590d03e613bdded38b4c0820ac44615f91af12435980b3ede3c08c315a2544");
        let signature = hex_to_64(
            "c8e4a68a5aa3c3588959e994fb958c25697ffe1631b4caafec7ee090292077b56\
             0c614d87ea660c0fcacf7522f533125e35eba9abd3c6fcf3b99ce66db9c1800",
        );
        let message = 10u64.to_le_bytes();
        assert!(verify(&public_key, &message, &signature));

        for index in 0..signature.len() {
            for bit in 0..8 {
                let mut tampered = signature;
                tampered[index] ^= 1 << bit;
                assert!(
                    !verify(&public_key, &message, &tampered),
                    "mutation at byte {index} bit {bit} was accepted"
                );
            }
        }
    }

    #[test]
    fn rejects_non_canonical_scalar() {
        let subseed = hex_to_32("4ac19e2bf0d3776519aabe31924f7dc2589b3d0e7411a65f84c9b16df72c038e");
        let digest = hex_to_32("94e120a4d3f58c217a53eb9046d9f2c5b11288a9fe340d6ce5a771cf04b82e63");
        let signature = hex_to_64(
            "357d47b1366f33eed311a4458ec7326d35728e9292328a9b7ff8d4ec0f7b0df9323f5d1cd01bd5a380a1a8e4f29ad3ae9c5d94e84f4181a61ca73030d6d11600",
        );
        let public_key = public_key_from_private(&k12::k12_32(&subseed));

        // S = r exactly is non-canonical and must be rejected.
        let mut at_order = signature;
        for index in 0..4 {
            at_order[32 + index * 8..32 + (index + 1) * 8]
                .copy_from_slice(&ORDER[index].to_le_bytes());
        }
        assert!(!verify_digest(&public_key, &digest, &at_order));
    }

    #[test]
    fn rejects_weak_public_keys() {
        let digest = [0u8; 32];
        let signature = [0u8; 64];

        // Identity (0, 1), order 1.
        let mut identity = [0u8; 32];
        identity[0] = 1;
        assert!(!verify_digest(&identity, &digest, &signature));

        // (0, -1), order 2.
        let mut order_two = [0u8; 32];
        order_two[..16].copy_from_slice(&Fp::new(P - 1).0.to_le_bytes());
        assert!(!verify_digest(&order_two, &digest, &signature));

        // (i, 0) and (-i, 0), order 4.
        let mut order_four = [0u8; 32];
        order_four[16..].copy_from_slice(&Fp::new(0).0.to_le_bytes());
        assert!(!verify_digest(&order_four, &digest, &signature));
    }

    #[test]
    fn rejects_malformed_public_key() {
        let digest = [0u8; 32];
        let signature = [0u8; 64];

        // y = 2 is not on the curve.
        let mut bad = [0u8; 32];
        bad[0] = 2;
        assert!(!verify_digest(&bad, &digest, &signature));

        // High bit of the real y coordinate is forbidden.
        let mut bad = [0u8; 32];
        bad[15] = 0x80;
        bad[0] = 1;
        assert!(!verify_digest(&bad, &digest, &signature));
    }

    fn known_valid_vector() -> ([u8; 32], [u8; 32], [u8; 64]) {
        let public_key =
            hex_to_32("1f590d03e613bdded38b4c0820ac44615f91af12435980b3ede3c08c315a2544");
        let signature = hex_to_64(
            "c8e4a68a5aa3c3588959e994fb958c25697ffe1631b4caafec7ee090292077b56\
             0c614d87ea660c0fcacf7522f533125e35eba9abd3c6fcf3b99ce66db9c1800",
        );
        let digest = k12::k12_32(&10u64.to_le_bytes());
        (public_key, digest, signature)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Every single-bit change to a valid signature must be rejected.
        #[test]
        fn single_bit_signature_flips_are_rejected(bit in 0usize..512) {
            let (public_key, digest, signature) = known_valid_vector();
            let mut tampered = signature;
            tampered[bit / 8] ^= 1 << (bit % 8);
            prop_assert!(!verify_digest(&public_key, &digest, &tampered));
        }

        /// Arbitrary bytes in place of a signature are rejected (the known valid
        /// signature is excluded so the property is deterministic).
        #[test]
        fn arbitrary_signatures_are_rejected(candidate in any::<[u8; 64]>()) {
            let (public_key, digest, signature) = known_valid_vector();
            prop_assume!(candidate != signature);
            prop_assert!(!verify_digest(&public_key, &digest, &candidate));
        }
    }
}
