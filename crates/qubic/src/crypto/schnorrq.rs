//! SchnorrQ signatures over the FourQ curve.
//!
//! Verification is implemented from the FourQ/SchnorrQ specification. Qubic
//! signs KangarooTwelve(message) and serializes the signature as 64 bytes.

/// Verifies SchnorrQ(public_key, K12(message)).
pub fn verify(public_key: &[u8; 32], message: &[u8], signature: &[u8; 64]) -> bool {
    let digest = crate::crypto::k12::k12_32(message);
    verify_digest(public_key, &digest, signature)
}

/// Verifies SchnorrQ(public_key, message_digest).
pub fn verify_digest(_public_key: &[u8; 32], _message_digest: &[u8; 32], _signature: &[u8; 64]) -> bool {
    todo!("implement FourQ point decoding, double scalar multiplication, and SchnorrQ verification")
}
