//! In-repo cryptographic primitives used by the Qubic protocol.
//!
//! `k12` is a from-scratch KangarooTwelve implementation.
//! `schnorrq` is a from-scratch SchnorrQ verifier over the FourQ curve.

pub mod k12;
pub mod schnorrq;
