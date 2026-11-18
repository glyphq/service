use qubic::messages::{Computors, LogEvent, Tick, TickData, Transaction};
use serde::Serialize;
use std::net::SocketAddr;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("wire: {0}")]
    Wire(#[from] qubic::Error),
    #[error("peer returned end response for request {0:?}")]
    EndResponse(qubic::MessageType),
    #[error("unexpected packet: expected {expected:?}, got {actual:?}")]
    UnexpectedPacket {
        expected: qubic::MessageType,
        actual: qubic::MessageType,
    },
    #[error("request timed out after {0:?}")]
    Timeout(std::time::Duration),
    #[error("no peers available")]
    NoPeers,
    #[error("peer is not synchronized: head {head}, requested {requested}")]
    NotSynchronized { head: u32, requested: u32 },
    #[error("verification failed: {0}")]
    Verification(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct PeerConfig {
    pub addr: SocketAddr,
    pub passcode: [u64; 4],
    pub label: Option<String>,
}

impl PeerConfig {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            passcode: [0; 4],
            label: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Head {
    pub epoch: u16,
    pub tick: u32,
    pub initial_tick: u32,
    pub tick_duration: u16,
}

#[derive(Debug, Clone)]
pub struct TickBundle {
    pub head: Head,
    pub computors: Computors,
    pub tick: Tick,
    pub tick_data: TickData,
    pub votes: Vec<Tick>,
    pub transactions: Vec<Transaction>,
    pub log_ranges: qubic::messages::LogRanges,
    pub logs: Vec<LogEvent>,
}

/// A direct TCP connection to a Qubic core or Bob peer.
pub struct Peer {
    pub(crate) config: PeerConfig,
    pub(crate) reader: tokio::io::BufReader<tokio::net::tcp::OwnedReadHalf>,
    pub(crate) writer: tokio::net::tcp::OwnedWriteHalf,
    pub(crate) next_dejavu: u32,
}

impl Peer {
    pub async fn connect(_config: PeerConfig) -> Result<Self> {
        todo!("implement TCP connect, exchange-public-peers handshake, and buffered reader")
    }

    pub async fn handshake(&mut self) -> Result<()> {
        todo!("implement peer handshake")
    }

    pub async fn head(&mut self) -> Result<Head> {
        todo!("request current tick info")
    }

    pub async fn computors(&mut self) -> Result<Computors> {
        todo!("request computors")
    }

    pub async fn tick_data(&mut self, _tick: u32) -> Result<TickData> {
        todo!("request tick data")
    }

    pub async fn votes(&mut self, _tick: u32, _known: &[bool; qubic::COMPUTOR_COUNT]) -> Result<Vec<Tick>> {
        todo!("request quorum votes")
    }

    pub async fn transactions(&mut self, _tick: u32, _known: &[bool; qubic::TX_PER_TICK]) -> Result<Vec<Transaction>> {
        todo!("request tick transactions")
    }

    pub async fn log_ranges(&mut self, _tick: u32) -> Result<qubic::messages::LogRanges> {
        todo!("request all log id ranges")
    }

    pub async fn logs(&mut self, _from_log_id: u64, _to_log_id: u64) -> Result<Vec<LogEvent>> {
        todo!("request log records")
    }

    pub async fn bundle(&mut self, _tick: u32) -> Result<TickBundle> {
        todo!("assemble and verify a full tick bundle")
    }
}

/// A pool of independent peer connections.
pub struct NodePool {
    pub(crate) peers: Vec<PeerConfig>,
}

impl NodePool {
    pub fn new(peers: Vec<PeerConfig>) -> Self {
        Self { peers }
    }

    pub async fn head(&self) -> Result<Head> {
        todo!("query peers until one answers")
    }

    pub async fn connect_best(&self) -> Result<Peer> {
        todo!("connect with failover and latency scoring")
    }
}
