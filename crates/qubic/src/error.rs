use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid packet: {0}")]
    InvalidPacket(String),
    #[error("invalid identity: {0}")]
    InvalidIdentity(String),
    #[error("invalid transaction hash: {0}")]
    InvalidTransactionHash(String),
    #[error("invalid message type: {0}")]
    InvalidMessageType(u8),
    #[error("invalid log event: {0}")]
    InvalidLog(String),
    #[error("invalid signature")]
    InvalidSignature,
}

pub type Result<T> = std::result::Result<T, Error>;
