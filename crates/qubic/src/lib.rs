//! Qubic wire protocol, identity encoding, log decoding, K12, and SchnorrQ.

pub mod crypto;
pub mod error;
pub mod identity;
pub mod logs;
pub mod messages;
pub mod wire;

pub use error::{Error, Result};
pub use identity::{
    IDENTITY_LEN, PUBLIC_KEY_LEN, PublicKey, decode_base26, decode_tx_hash, encode_base26,
    encode_tx_hash,
};
pub use logs::{EventBody, LogEvent};
pub use messages::*;
pub use wire::{Header, MessageType, Packet};

#[cfg(test)]
mod wire_tests {
    use super::*;

    #[test]
    fn packet_round_trip() {
        let packet = Packet::new(
            MessageType::RequestTickData,
            77,
            42u32.to_le_bytes().to_vec(),
        );
        let bytes = packet.encode();
        assert_eq!(bytes.len(), 12);
        assert_eq!(bytes[0..3], [12, 0, 0]);
        assert_eq!(bytes[3], 16);
        assert_eq!(Packet::decode(&bytes).unwrap(), packet);
    }

    #[test]
    fn rejects_truncated_packet() {
        let packet = Packet::new(MessageType::RequestLog, 1, vec![0; 48]);
        let bytes = packet.encode();
        assert!(Packet::decode(&bytes[..10]).is_err());
    }
}
