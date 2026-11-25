use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use qubic::LogEvent;
use qubic::crypto::schnorrq;
use qubic::messages::{
    ASSET_REQUEST_GET_SIBLINGS, COMPUTOR_COUNT, Computors, CurrentTickInfo, LogRanges, MAX_AMOUNT,
    MAX_INPUT_SIZE, ORACLE_REQUEST_ACTIVE_SUBSCRIPTIONS, ORACLE_REQUEST_QUERY_AND_RESPONSE,
    ORACLE_REQUEST_SUBSCRIPTION, ORACLE_RESPOND_QUERY_DATA, ORACLE_RESPOND_QUERY_METADATA,
    ORACLE_RESPOND_REPLY_DATA, ORACLE_RESPOND_SUBSCRIPTION, ORACLE_RESPOND_SUBSCRIPTION_IDS,
    OracleQueryMetadata, OracleQueryStatistics, OracleSubscriber, OracleSubscription,
    PRICE_SERIES_MAX_POINTS, PeerAddress, QUORUM, RequestAssets, RespondAssets,
    RespondAssetsWithSiblings, RespondContractFunction, RespondEntity, RespondIssuedAssets,
    RespondOracleData, RespondOwnedAssets, RespondPossessedAssets, RespondPriceSeries,
    RespondRevenueData, RespondTxStatus, SystemInfo, TX_PER_TICK, Tick, TickAlignment, TickData,
    Transaction, request_all_log_ranges, request_assets, request_computors,
    request_contract_function, request_current_tick_info, request_entity, request_issued_assets,
    request_logs, request_oracle_data, request_owned_assets, request_possessed_assets,
    request_price_series, request_quorum_tick, request_revenue_data, request_system_info,
    request_tick_data, request_tick_transactions, request_transaction_info, request_tx_status,
};
use qubic::wire::{HEADER_SIZE, MessageType, Packet};
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

pub mod health;
mod prefetch;

/// How a peer exposes per-tick log ranges.
///
/// This is a protocol capability, not a completeness claim. A peer can return
/// `EndResponse` for a tick even when it supports a protocol (for example when
/// the tick is not verified yet), so the protocol cache must never be used to
/// decide that a tick has no logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogProtocol {
    /// One `RequestAllLogIdRangesFromTick` packet for the whole tick.
    AllRanges,
    /// One `RequestLogIdRangeFromTx` packet per transaction/special slot.
    TxRanges,
    /// The peer answered `EndResponse` to both protocols.
    NoLogging,
}

fn log_protocol_cache() -> &'static std::sync::Mutex<HashMap<SocketAddr, LogProtocol>> {
    static CACHE: std::sync::OnceLock<std::sync::Mutex<HashMap<SocketAddr, LogProtocol>>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// How long a peer may take to answer one packet of a request.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(8);
/// How long establishing the TCP connection may take.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Extra attempts after the first one for a request.
pub const DEFAULT_RETRIES: u32 = 2;
/// Broadcasts are fire-and-forget. A slow peer must not delay the submission or
/// the next retry, so broadcast connections use a tighter budget.
const BROADCAST_CONNECT_TIMEOUT: Duration = Duration::from_millis(1_500);
const BROADCAST_HANDSHAKE_TIMEOUT: Duration = Duration::from_millis(1_500);
/// Idle timeout for one all-ranges log response. Qubic serves this from an
/// in-memory/Redis lookup, so a slow answer means the peer does not have the
/// tick's log data.
pub const DEFAULT_LOG_RANGE_TIMEOUT: Duration = Duration::from_secs(1);

/// Guard against a peer that keeps returning a partial log page forever.
const MAX_LOG_PAGES: u32 = 1024;

/// Guard against a peer that streams an unbounded asset response.
const MAX_ASSET_RECORDS: usize = 1_000_000;

/// Guard against a peer that streams an unbounded oracle response.
const MAX_ORACLE_PACKETS: usize = 1024;
/// Largest query-ID list accepted from one peer.
const MAX_ORACLE_QUERY_IDS: usize = 1_000_000;
/// Largest active-subscription list accepted from one peer.
const MAX_ORACLE_SUBSCRIPTIONS: usize = 4096;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("wire: {0}")]
    Wire(#[from] qubic::Error),
    #[error("peer returned end response for request {0:?}")]
    EndResponse(MessageType),
    #[error("peer asked us to try again for request {0:?}")]
    TryAgain(MessageType),
    #[error("unexpected packet: expected {expected:?}, got {actual:?}")]
    UnexpectedPacket {
        expected: MessageType,
        actual: MessageType,
    },
    #[error("request timed out after {0:?}")]
    Timeout(Duration),
    #[error("request {request:?} failed after {attempts} attempts")]
    RetriesExhausted { request: MessageType, attempts: u32 },
    #[error("no peers available")]
    NoPeers,
    #[error("peer is not synchronized: head {head}, requested {requested}")]
    NotSynchronized { head: u32, requested: u32 },
    #[error("verification failed: {0}")]
    Verification(String),
    #[error("log data unavailable: {0}")]
    LogsUnavailable(String),
    #[error("tick {tick} has non-empty votes but no tick data")]
    MissingNonEmptyTick { tick: u32 },
    #[error("invalid request: {0}")]
    InvalidRequest(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct PeerConfig {
    pub addr: SocketAddr,
    pub passcode: [u64; 4],
    pub label: Option<String>,
    /// Per-packet read timeout.
    pub timeout: Duration,
    /// TCP connect timeout.
    pub connect_timeout: Duration,
    /// Extra attempts after the first one.
    pub retries: u32,
    /// Idle timeout for the all-ranges log request.
    pub log_range_timeout: Duration,
}

impl PeerConfig {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            passcode: [0; 4],
            label: None,
            timeout: DEFAULT_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            retries: DEFAULT_RETRIES,
            log_range_timeout: DEFAULT_LOG_RANGE_TIMEOUT,
        }
    }

    pub fn with_passcode(mut self, passcode: [u64; 4]) -> Self {
        self.passcode = passcode;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn with_retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    pub fn with_log_range_timeout(mut self, timeout: Duration) -> Self {
        self.log_range_timeout = timeout;
        self
    }
}

pub use health::{DEFAULT_DISCOVERY_LIMIT, PeerHealth, PeerManager, PeerPolicy};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
    pub log_ranges: LogRanges,
    pub logs: Vec<LogEvent>,
    /// True when every slot's log range was answered and every declared log
    /// record was fetched. A peer with logging disabled reports false.
    pub logs_complete: bool,
    /// Non-fatal problems, such as a peer that has logging disabled.
    pub warnings: Vec<String>,
}

/// Log data fetched for one tick, with an explicit proof flag.
///
/// `complete` is true only when a peer returned a decoded range payload and
/// every declared log record was fetched. An absent or timed-out range request
/// leaves this false even when the tick has no transaction slots: Qubic ticks
/// can carry BEGIN_TICK/END_TICK logs without user transactions.
#[derive(Debug, Clone)]
pub struct FetchedLogs {
    pub ranges: LogRanges,
    pub logs: Vec<LogEvent>,
    pub complete: bool,
    pub warnings: Vec<String>,
}

impl FetchedLogs {
    pub fn incomplete(reason: impl Into<String>) -> Self {
        Self {
            ranges: LogRanges::empty(),
            logs: Vec::new(),
            complete: false,
            warnings: vec![reason.into()],
        }
    }
}

struct BundlePackets {
    tick_data: Option<Packet>,
    votes: Vec<Packet>,
    transactions: Vec<Packet>,
}

/// Status of one confirmed transaction inside a tick.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransactionStatusEntry {
    pub digest: [u8; 32],
    /// Whether the transaction moved QU or assets. A `false` value usually
    /// means the transaction executed but had no effect.
    pub money_flew: bool,
}

/// Result of the tx-status add-on request: every confirmed transaction of one
/// tick in node order. The add-on reports ticks, not individual digests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TransactionStatus {
    /// The peer's current tick when it answered.
    pub current_tick: u32,
    pub tick: u32,
    pub transactions: Vec<TransactionStatusEntry>,
}

impl TransactionStatus {
    /// Money-flow flag for a digest, or `None` when the tick does not contain it.
    pub fn money_flew_for(&self, digest: &[u8; 32]) -> Option<bool> {
        self.transactions
            .iter()
            .find(|entry| &entry.digest == digest)
            .map(|entry| entry.money_flew)
    }

    pub fn contains(&self, digest: &[u8; 32]) -> bool {
        self.money_flew_for(digest).is_some()
    }
}

/// One record from a `RequestAssets` response.
///
/// The peer sends the fixed [`RespondAssets`] record unless the request set
/// [`ASSET_REQUEST_GET_SIBLINGS`], in which case the payload also carries the
/// asset-tree proof as [`RespondAssetsWithSiblings`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssetRecordResponse {
    Asset(RespondAssets),
    WithSiblings(RespondAssetsWithSiblings),
}

/// Common fields of a plain or sibling asset response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssetRecordView<'a> {
    pub asset: &'a qubic::messages::AssetRecord,
    pub tick: u32,
    pub universe_index: u32,
}

impl AssetRecordResponse {
    pub fn asset(&self) -> AssetRecordView<'_> {
        match self {
            Self::Asset(asset) => AssetRecordView {
                asset: &asset.asset,
                tick: asset.tick,
                universe_index: asset.universe_index,
            },
            Self::WithSiblings(asset) => AssetRecordView {
                asset: &asset.asset,
                tick: asset.tick,
                universe_index: asset.universe_index,
            },
        }
    }

    pub fn record(&self) -> &qubic::messages::AssetRecord {
        match self {
            Self::Asset(asset) => &asset.asset,
            Self::WithSiblings(asset) => &asset.asset,
        }
    }

    pub fn tick(&self) -> u32 {
        match self {
            Self::Asset(asset) => asset.tick,
            Self::WithSiblings(asset) => asset.tick,
        }
    }

    pub fn universe_index(&self) -> u32 {
        match self {
            Self::Asset(asset) => asset.universe_index,
            Self::WithSiblings(asset) => asset.universe_index,
        }
    }

    pub fn siblings(&self) -> Option<&[[u8; 32]]> {
        match self {
            Self::Asset(_) => None,
            Self::WithSiblings(asset) => Some(&asset.siblings),
        }
    }
}

/// Subscription info returned by `RequestOracleData` reqType 6.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleSubscriptionResponse {
    pub subscription: OracleSubscription,
    /// Initial query data sent by the core (`RespondOracleData.resType` 2).
    pub query: Vec<u8>,
}

/// Query metadata, query data, and (when revealed) reply data returned by
/// `RequestOracleData` reqType 5.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleQueryResponse {
    pub metadata: OracleQueryMetadata,
    pub query: Option<Vec<u8>>,
    pub reply: Option<Vec<u8>>,
}

/// Subscription info, its initial query, and every contract subscriber.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleSubscriptionDetail {
    pub subscription: OracleSubscription,
    pub query: Vec<u8>,
    pub subscribers: Vec<OracleSubscriber>,
}

/// A broadcast transaction that passed local validation and was written to the
/// peer socket. Qubic core does not acknowledge broadcasts, so receipt means
/// "sent", not "included".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BroadcastReceipt {
    pub digest: [u8; 32],
    pub tick: u32,
    pub peer: SocketAddr,
}

/// One peer's outcome in a broadcast fan-out.
#[derive(Debug, Clone)]
pub struct BroadcastAttempt {
    pub peer: SocketAddr,
    /// True when the TCP write completed. Qubic peers do not acknowledge
    /// broadcast packets, so this is delivery to the socket, not inclusion.
    pub sent: bool,
    pub error: Option<String>,
    pub elapsed: Duration,
}

/// A direct TCP connection to a Qubic core or Bob peer.
pub struct Peer {
    config: PeerConfig,
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    next_dejavu: u32,
    /// Requests we have sent and are still waiting for, keyed by dejavu.
    pending: HashMap<u32, MessageType>,
    /// Public peers learned from ExchangePublicPeers packets.
    public_peers: Vec<PeerAddress>,
    /// A timed-out read may have consumed part of a packet; reconnect first.
    desynced: bool,
    /// Cached log protocol. Set after the first successful/failed probe and
    /// shared across short-lived connections through [`log_protocol_cache`].
    log_protocol: Option<LogProtocol>,
    /// Latest head this connection reported. Used to reject backwards ticks
    /// and cached heads from before an epoch rollover.
    observed_head: Option<Head>,
}

impl Peer {
    /// Connects to a peer and completes the exchange-public-peers handshake.
    pub async fn connect(config: PeerConfig) -> Result<Self> {
        let stream = Self::open(&config).await?;
        let (reader, writer) = stream.into_split();
        let log_protocol = log_protocol_cache()
            .lock()
            .ok()
            .and_then(|cache| cache.get(&config.addr).copied());
        let mut peer = Self {
            config,
            reader: BufReader::new(reader),
            writer,
            next_dejavu: random_dejavu(),
            pending: HashMap::new(),
            public_peers: Vec::new(),
            desynced: false,
            log_protocol,
            observed_head: None,
        };
        peer.handshake().await?;
        Ok(peer)
    }

    async fn open(config: &PeerConfig) -> Result<TcpStream> {
        let connect = TcpStream::connect(config.addr);
        let stream = match tokio::time::timeout(config.connect_timeout, connect).await {
            Ok(result) => result?,
            Err(_) => return Err(Error::Timeout(config.connect_timeout)),
        };
        stream.set_nodelay(true)?;
        Ok(stream)
    }

    /// Sends ExchangePublicPeers and waits for the peer's own exchange packet.
    pub async fn handshake(&mut self) -> Result<()> {
        let peers = [PeerAddress {
            ip: [0, 0, 0, 0],
            port: qubic::messages::DEFAULT_PEER_PORT,
        }; 4];
        let dejavu = self.allocate_dejavu();
        self.writer
            .write_all(
                &Packet::new(
                    MessageType::ExchangePublicPeers,
                    dejavu,
                    qubic::messages::exchange_public_peers(&peers),
                )
                .encode(),
            )
            .await?;
        self.writer.flush().await?;

        let timeout = self.config.timeout;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                self.desynced = true;
                return Err(Error::Timeout(timeout));
            }
            let packet =
                match tokio::time::timeout(remaining, Self::read_packet(&mut self.reader)).await {
                    Ok(result) => result?,
                    Err(_) => {
                        self.desynced = true;
                        return Err(Error::Timeout(timeout));
                    }
                };
            if packet.kind == MessageType::ExchangePublicPeers {
                self.record_peers(&packet);
                return Ok(());
            }
            // Anything else is unsolicited gossip that arrived during the handshake.
        }
    }

    /// Public peers learned from the handshake and later peer-exchange packets.
    pub fn public_peers(&self) -> &[PeerAddress] {
        &self.public_peers
    }

    pub fn config(&self) -> &PeerConfig {
        &self.config
    }

    /// Requests current tick info.
    pub async fn head(&mut self) -> Result<Head> {
        let packet = self
            .request_single(
                MessageType::RequestCurrentTickInfo,
                request_current_tick_info(),
                MessageType::RespondCurrentTickInfo,
            )
            .await?;
        let info = CurrentTickInfo::decode(&packet.payload)?;
        let head = Head {
            epoch: info.epoch,
            tick: info.tick,
            initial_tick: info.initial_tick,
            tick_duration: info.tick_duration,
        };
        health::check_head(self.observed_head.as_ref(), &head)?;
        self.observed_head = Some(head.clone());
        Ok(head)
    }

    /// Requests the peer's packed system info snapshot.
    pub async fn system_info(&mut self) -> Result<SystemInfo> {
        let packet = self
            .request_single(
                MessageType::RequestSystemInfo,
                request_system_info(),
                MessageType::RespondSystemInfo,
            )
            .await?;
        Ok(SystemInfo::decode(&packet.payload)?)
    }

    /// Requests live per-computor revenue scores for the current epoch.
    pub async fn revenue_data(&mut self) -> Result<RespondRevenueData> {
        let packet = self
            .request_single(
                MessageType::RequestRevenueData,
                request_revenue_data(),
                MessageType::RespondRevenueData,
            )
            .await?;
        Ok(RespondRevenueData::decode(&packet.payload)?)
    }

    /// Requests the computors for the peer's current epoch.
    pub async fn computors(&mut self) -> Result<Computors> {
        let packet = self
            .request_single(
                MessageType::RequestComputors,
                request_computors(),
                MessageType::BroadcastComputors,
            )
            .await?;
        decode_computors(&packet.payload)
    }

    /// Requests computors and returns them only when they belong to `epoch`.
    ///
    /// During an epoch rollover a peer may still answer with the previous set;
    /// callers should treat that as [`Error::NotSynchronized`] and refresh
    /// their head instead of verifying against the wrong computors.
    pub async fn computors_for(&mut self, epoch: u16) -> Result<Computors> {
        let computors = self.computors().await?;
        health::check_computors_epoch(&computors, epoch)?;
        Ok(computors)
    }

    /// Requests the tick data (transaction digests and contract fees).
    pub async fn tick_data(&mut self, tick: u32) -> Result<TickData> {
        let packet = self
            .request_single(
                MessageType::RequestTickData,
                request_tick_data(tick),
                MessageType::BroadcastFutureTickData,
            )
            .await?;
        Ok(TickData::decode(&packet.payload)?)
    }

    /// Requests quorum votes. `known` bits mark computors we already have.
    pub async fn votes(&mut self, tick: u32, known: &[bool; COMPUTOR_COUNT]) -> Result<Vec<Tick>> {
        let packets = self
            .request_stream(
                MessageType::RequestQuorumTick,
                request_quorum_tick(tick, known),
                MessageType::BroadcastTick,
            )
            .await?;
        decode_votes(packets, tick)
    }

    /// Requests tick transactions. `known` bits mark slots we already have.
    pub async fn transactions(
        &mut self,
        tick: u32,
        known: &[bool; TX_PER_TICK],
    ) -> Result<Vec<Transaction>> {
        let packets = self
            .request_stream(
                MessageType::RequestTickTransactions,
                request_tick_transactions(tick, known),
                MessageType::BroadcastTransaction,
            )
            .await?;
        decode_transactions(packets, tick)
    }

    /// Requests one spectrum entity record, including its Merkle siblings.
    ///
    /// An unknown identity is not an error: the peer returns a zeroed record
    /// with `spectrum_index < 0`.
    pub async fn entity(&mut self, public_key: [u8; 32]) -> Result<RespondEntity> {
        let packet = self
            .request_single(
                MessageType::RequestEntity,
                request_entity(public_key),
                MessageType::RespondEntity,
            )
            .await?;
        Ok(RespondEntity::decode(&packet.payload)?)
    }

    /// Requests an entity by its 60-character identity.
    pub async fn entity_by_identity(&mut self, identity: &str) -> Result<RespondEntity> {
        let public_key = qubic::PublicKey::from_identity(identity).map_err(|error| {
            Error::Verification(format!("invalid identity for entity lookup: {error}"))
        })?;
        self.entity(public_key.0).await
    }

    /// Invokes a read-only contract function.
    ///
    /// An empty result means the peer rejected the contract index or input
    /// type, or the invocation failed. That is reported as `Ok(vec![])` so
    /// callers can distinguish it from a transport failure.
    pub async fn contract_function(
        &mut self,
        contract_index: u32,
        input_type: u16,
        input: &[u8],
    ) -> Result<Vec<u8>> {
        if input.len() > MAX_INPUT_SIZE {
            return Err(Error::Verification(format!(
                "contract function input is {} bytes, limit is {MAX_INPUT_SIZE}",
                input.len()
            )));
        }
        let packet = self
            .request_single(
                MessageType::RequestContractFunction,
                request_contract_function(contract_index, input_type, input),
                MessageType::RespondContractFunction,
            )
            .await?;
        Ok(RespondContractFunction::decode(&packet.payload)?.data)
    }

    /// Looks up a confirmed transaction by digest.
    ///
    /// Returns `None` when the peer answers `EndResponse` (unknown digest or
    /// outside its retained window).
    pub async fn transaction(&mut self, digest: [u8; 32]) -> Result<Option<Transaction>> {
        let packet = self
            .request_single_any(
                MessageType::RequestTransactionInfo,
                request_transaction_info(digest),
                &[MessageType::BroadcastTransaction, MessageType::EndResponse],
            )
            .await?;
        if packet.kind == MessageType::EndResponse {
            return Ok(None);
        }
        let transaction = Transaction::decode(&packet.payload)?;
        if transaction.hash() != digest {
            return Err(Error::Verification(format!(
                "peer returned transaction {} for requested digest {}",
                qubic::encode_tx_hash(&transaction.hash()),
                qubic::encode_tx_hash(&digest)
            )));
        }
        Ok(Some(transaction))
    }

    /// Requests the confirmed transaction status for one tick.
    ///
    /// Only peers running the qli tx-status add-on answer this request. A peer
    /// without the add-on, or a tick it no longer retains, silently drops the
    /// request and this returns [`Error::Timeout`].
    pub async fn tx_status(&mut self, tick: u32) -> Result<TransactionStatus> {
        let packet = self
            .request_single(
                MessageType::RequestTxStatus,
                request_tx_status(tick),
                MessageType::RespondTxStatus,
            )
            .await?;
        let status = RespondTxStatus::decode(&packet.payload)?;
        if status.tick != tick {
            return Err(Error::Verification(format!(
                "tx status is for tick {}, requested {tick}",
                status.tick
            )));
        }
        Ok(TransactionStatus {
            current_tick: status.current_tick,
            tick: status.tick,
            transactions: status
                .digests
                .into_iter()
                .zip(status.money_flew)
                .map(|(digest, money_flew)| TransactionStatusEntry { digest, money_flew })
                .collect(),
        })
    }

    /// Requests up to [`PRICE_SERIES_MAX_POINTS`] hourly price points starting
    /// at `from_hour`.
    ///
    /// `count` is clamped to 256, which is what a core enforces on its side.
    pub async fn price_series(&mut self, from_hour: u32, count: u32) -> Result<RespondPriceSeries> {
        let count = count.min(PRICE_SERIES_MAX_POINTS as u32);
        let packet = self
            .request_single(
                MessageType::RequestPriceSeries,
                request_price_series(from_hour, count),
                MessageType::RespondPriceSeries,
            )
            .await?;
        Ok(RespondPriceSeries::decode(&packet.payload)?)
    }

    /// Requests the IDs of oracle queries selected by `req_type`.
    ///
    /// Use the `ORACLE_REQUEST_*_BY_TICK` selectors or
    /// [`ORACLE_REQUEST_PENDING_QUERY_IDS`]. `tick_or_id` is a tick for the
    /// tick-scoped selectors and ignored for pending queries.
    pub async fn oracle_query_ids(&mut self, req_type: u32, tick_or_id: i64) -> Result<Vec<i64>> {
        let packets = self
            .request_stream_bounded(
                MessageType::RequestOracleData,
                request_oracle_data(req_type, tick_or_id),
                MessageType::RespondOracleData,
                MAX_ORACLE_PACKETS,
            )
            .await?;
        let mut ids = Vec::new();
        for packet in &packets {
            let (header, data) = RespondOracleData::decode(&packet.payload)?;
            match header.res_type {
                qubic::messages::ORACLE_RESPOND_QUERY_IDS => {
                    if data.len() % 8 != 0 {
                        return Err(Error::Verification(format!(
                            "oracle query ID list has {} trailing bytes",
                            data.len() % 8
                        )));
                    }
                    for chunk in data.chunks_exact(8) {
                        if ids.len() >= MAX_ORACLE_QUERY_IDS {
                            return Err(Error::Verification(format!(
                                "peer returned more than {MAX_ORACLE_QUERY_IDS} oracle query IDs"
                            )));
                        }
                        ids.push(i64::from_le_bytes(
                            chunk.try_into().expect("chunks_exact yields 8 bytes"),
                        ));
                    }
                }
                qubic::messages::ORACLE_RESPOND_TICK_RANGE => {
                    // The node reports its retained tick range instead of an
                    // empty ID list when the requested tick is out of range.
                    qubic::messages::OracleTickRange::decode(data)?;
                }
                other => {
                    return Err(Error::Verification(format!(
                        "unexpected oracle response type {other} for query ID list"
                    )));
                }
            }
        }
        Ok(ids)
    }

    /// Requests oracle query processing statistics from the peer.
    pub async fn oracle_query_statistics(&mut self) -> Result<OracleQueryStatistics> {
        let packet = self
            .request_single(
                MessageType::RequestOracleData,
                request_oracle_data(qubic::messages::ORACLE_REQUEST_QUERY_STATISTICS, 0),
                MessageType::RespondOracleData,
            )
            .await?;
        let (header, data) = RespondOracleData::decode(&packet.payload)?;
        if header.res_type != qubic::messages::ORACLE_RESPOND_QUERY_STATISTICS {
            return Err(Error::Verification(format!(
                "unexpected oracle response type {} for statistics",
                header.res_type
            )));
        }
        Ok(OracleQueryStatistics::decode(data)?)
    }

    /// Requests the 676 oracle revenue points for the current epoch.
    pub async fn oracle_revenue_points(&mut self) -> Result<Vec<u64>> {
        let packet = self
            .request_single(
                MessageType::RequestOracleData,
                request_oracle_data(qubic::messages::ORACLE_REQUEST_REVENUE_POINTS, 0),
                MessageType::RespondOracleData,
            )
            .await?;
        let (header, data) = RespondOracleData::decode(&packet.payload)?;
        if header.res_type != qubic::messages::ORACLE_RESPOND_REVENUE_POINTS {
            return Err(Error::Verification(format!(
                "unexpected oracle response type {} for revenue points",
                header.res_type
            )));
        }
        if data.len() != COMPUTOR_COUNT * 8 {
            return Err(Error::Verification(format!(
                "oracle revenue points have {} bytes, expected {}",
                data.len(),
                COMPUTOR_COUNT * 8
            )));
        }
        Ok(data
            .chunks_exact(8)
            .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("8 bytes")))
            .collect())
    }

    /// Requests the IDs of active oracle subscriptions of one contract.
    pub async fn oracle_active_contract_subscriptions(
        &mut self,
        contract_index: u32,
    ) -> Result<Vec<i32>> {
        let packets = self
            .request_stream_bounded(
                MessageType::RequestOracleData,
                request_oracle_data(
                    qubic::messages::ORACLE_REQUEST_ACTIVE_CONTRACT_SUBSCRIPTIONS,
                    i64::from(contract_index),
                ),
                MessageType::RespondOracleData,
                MAX_ORACLE_PACKETS,
            )
            .await?;
        let mut ids = Vec::new();
        for packet in &packets {
            let (header, data) = RespondOracleData::decode(&packet.payload)?;
            if header.res_type != ORACLE_RESPOND_SUBSCRIPTION_IDS {
                return Err(Error::Verification(format!(
                    "unexpected oracle response type {} for contract subscriptions",
                    header.res_type
                )));
            }
            if data.len() % 4 != 0 {
                return Err(Error::Verification(format!(
                    "oracle contract subscription list has {} trailing bytes",
                    data.len() % 4
                )));
            }
            for chunk in data.chunks_exact(4) {
                if ids.len() >= MAX_ORACLE_SUBSCRIPTIONS {
                    return Err(Error::Verification(format!(
                        "peer returned more than {MAX_ORACLE_SUBSCRIPTIONS} oracle contract subscriptions"
                    )));
                }
                ids.push(i32::from_le_bytes(
                    chunk.try_into().expect("chunks_exact yields 4 bytes"),
                ));
            }
        }
        Ok(ids)
    }

    /// Requests a subscription, its initial query, and every subscriber.
    pub async fn oracle_subscription_detail(
        &mut self,
        subscription_id: i32,
    ) -> Result<OracleSubscriptionDetail> {
        let packets = self
            .request_stream_bounded(
                MessageType::RequestOracleData,
                request_oracle_data(ORACLE_REQUEST_SUBSCRIPTION, i64::from(subscription_id)),
                MessageType::RespondOracleData,
                MAX_ORACLE_PACKETS,
            )
            .await?;
        let mut subscription = None;
        let mut query = None;
        let mut subscribers = Vec::new();
        for packet in &packets {
            let (header, data) = RespondOracleData::decode(&packet.payload)?;
            match header.res_type {
                ORACLE_RESPOND_SUBSCRIPTION => {
                    subscription = Some(OracleSubscription::decode(data)?);
                }
                ORACLE_RESPOND_QUERY_DATA => query = Some(data.to_vec()),
                qubic::messages::ORACLE_RESPOND_SUBSCRIBER => {
                    subscribers.push(OracleSubscriber::decode(data)?);
                }
                qubic::messages::ORACLE_RESPOND_NOTIFIED_SUBSCRIBER_CONTRACTS => {}
                other => {
                    return Err(Error::Verification(format!(
                        "unexpected oracle response type {other} for subscription {subscription_id}"
                    )));
                }
            }
        }
        let subscription = subscription.ok_or_else(|| {
            Error::Verification(format!(
                "peer did not return info for oracle subscription {subscription_id}"
            ))
        })?;
        if subscription.subscription_id != subscription_id {
            return Err(Error::Verification(format!(
                "peer returned subscription {}, requested {subscription_id}",
                subscription.subscription_id
            )));
        }
        let query = query.ok_or_else(|| {
            Error::Verification(format!(
                "peer did not return query data for subscription {subscription_id}"
            ))
        })?;
        Ok(OracleSubscriptionDetail {
            subscription,
            query,
            subscribers,
        })
    }

    /// Requests the IDs of all oracle subscriptions active in the current
    /// epoch (`RequestOracleData` reqType 9).
    ///
    /// The core splits the list over multiple `RespondOracleData` packets, so
    /// the stream is bounded by [`MAX_ORACLE_SUBSCRIPTIONS`] IDs and
    /// [`MAX_ORACLE_PACKETS`] packets.
    pub async fn oracle_subscription_ids(&mut self) -> Result<Vec<i32>> {
        let packets = self
            .request_stream_bounded(
                MessageType::RequestOracleData,
                request_oracle_data(ORACLE_REQUEST_ACTIVE_SUBSCRIPTIONS, 0),
                MessageType::RespondOracleData,
                MAX_ORACLE_PACKETS,
            )
            .await?;
        let mut ids = Vec::new();
        for packet in &packets {
            let (header, data) = RespondOracleData::decode(&packet.payload)?;
            if header.res_type != ORACLE_RESPOND_SUBSCRIPTION_IDS {
                return Err(Error::Verification(format!(
                    "unexpected oracle response type {} for active subscriptions",
                    header.res_type
                )));
            }
            if data.len() % 4 != 0 {
                return Err(Error::Verification(format!(
                    "oracle subscription ID list has {} trailing bytes",
                    data.len() % 4
                )));
            }
            if ids.len() + data.len() / 4 > MAX_ORACLE_SUBSCRIPTIONS {
                return Err(Error::Verification(format!(
                    "peer returned more than {MAX_ORACLE_SUBSCRIPTIONS} oracle subscriptions"
                )));
            }
            for chunk in data.chunks_exact(4) {
                ids.push(i32::from_le_bytes(
                    chunk.try_into().expect("chunks_exact yields 4 bytes"),
                ));
            }
        }
        Ok(ids)
    }

    /// Requests info and the initial query for one oracle subscription
    /// (`RequestOracleData` reqType 6).
    pub async fn oracle_subscription(
        &mut self,
        subscription_id: i32,
    ) -> Result<OracleSubscriptionResponse> {
        let packets = self
            .request_stream_bounded(
                MessageType::RequestOracleData,
                request_oracle_data(ORACLE_REQUEST_SUBSCRIPTION, i64::from(subscription_id)),
                MessageType::RespondOracleData,
                MAX_ORACLE_PACKETS,
            )
            .await?;
        let mut subscription = None;
        let mut query = None;
        for packet in &packets {
            let (header, data) = RespondOracleData::decode(&packet.payload)?;
            match header.res_type {
                ORACLE_RESPOND_SUBSCRIPTION => {
                    subscription = Some(OracleSubscription::decode(data)?);
                }
                ORACLE_RESPOND_QUERY_DATA => query = Some(data.to_vec()),
                // Subscriber notifications use many packets; they are not part
                // of the subscription query itself.
                qubic::messages::ORACLE_RESPOND_SUBSCRIBER => {}
                other => {
                    return Err(Error::Verification(format!(
                        "unexpected oracle response type {other} for subscription {subscription_id}"
                    )));
                }
            }
        }
        let subscription = subscription.ok_or_else(|| {
            Error::Verification(format!(
                "peer did not return info for oracle subscription {subscription_id}"
            ))
        })?;
        if subscription.subscription_id != subscription_id {
            return Err(Error::Verification(format!(
                "peer returned subscription {}, requested {subscription_id}",
                subscription.subscription_id
            )));
        }
        let query = query.ok_or_else(|| {
            Error::Verification(format!(
                "peer did not return query data for oracle subscription {subscription_id}"
            ))
        })?;
        Ok(OracleSubscriptionResponse {
            subscription,
            query,
        })
    }

    /// Requests metadata, query data, and reply data for one oracle query
    /// (`RequestOracleData` reqType 5).
    ///
    /// `reply` is `None` until the query has been revealed successfully.
    pub async fn oracle_query(&mut self, query_id: i64) -> Result<OracleQueryResponse> {
        let packets = self
            .request_stream_bounded(
                MessageType::RequestOracleData,
                request_oracle_data(ORACLE_REQUEST_QUERY_AND_RESPONSE, query_id),
                MessageType::RespondOracleData,
                MAX_ORACLE_PACKETS,
            )
            .await?;
        let mut metadata = None;
        let mut query = None;
        let mut reply = None;
        for packet in &packets {
            let (header, data) = RespondOracleData::decode(&packet.payload)?;
            match header.res_type {
                ORACLE_RESPOND_QUERY_METADATA => {
                    metadata = Some(OracleQueryMetadata::decode(data)?);
                }
                ORACLE_RESPOND_QUERY_DATA => query = Some(data.to_vec()),
                ORACLE_RESPOND_REPLY_DATA => reply = Some(data.to_vec()),
                qubic::messages::ORACLE_RESPOND_NOTIFIED_SUBSCRIBER_CONTRACTS => {}
                other => {
                    return Err(Error::Verification(format!(
                        "unexpected oracle response type {other} for query {query_id}"
                    )));
                }
            }
        }
        let metadata = metadata.ok_or_else(|| {
            Error::Verification(format!(
                "peer did not return metadata for oracle query {query_id}"
            ))
        })?;
        if metadata.query_id != query_id {
            return Err(Error::Verification(format!(
                "peer returned metadata for query {}, requested {query_id}",
                metadata.query_id
            )));
        }
        Ok(OracleQueryResponse {
            metadata,
            query,
            reply,
        })
    }

    /// Requests every asset issued by `public_key`, in peer order.
    ///
    /// A peer with no matches answers `EndResponse` directly, giving an empty
    /// result. Responses are streamed until `EndResponse`, with an upper bound
    /// of [`MAX_ASSET_RECORDS`] records.
    pub async fn issued_assets(
        &mut self,
        public_key: [u8; 32],
    ) -> Result<Vec<RespondIssuedAssets>> {
        let packets = self
            .request_stream_bounded(
                MessageType::RequestIssuedAssets,
                request_issued_assets(public_key),
                MessageType::RespondIssuedAssets,
                MAX_ASSET_RECORDS,
            )
            .await?;
        packets
            .iter()
            .map(|packet| Ok(RespondIssuedAssets::decode(&packet.payload)?))
            .collect()
    }

    /// Requests every asset owned by `public_key`, in peer order.
    pub async fn owned_assets(&mut self, public_key: [u8; 32]) -> Result<Vec<RespondOwnedAssets>> {
        let packets = self
            .request_stream_bounded(
                MessageType::RequestOwnedAssets,
                request_owned_assets(public_key),
                MessageType::RespondOwnedAssets,
                MAX_ASSET_RECORDS,
            )
            .await?;
        packets
            .iter()
            .map(|packet| Ok(RespondOwnedAssets::decode(&packet.payload)?))
            .collect()
    }

    /// Requests every asset possessed by `public_key`, in peer order.
    pub async fn possessed_assets(
        &mut self,
        public_key: [u8; 32],
    ) -> Result<Vec<RespondPossessedAssets>> {
        let packets = self
            .request_stream_bounded(
                MessageType::RequestPossessedAssets,
                request_possessed_assets(public_key),
                MessageType::RespondPossessedAssets,
                MAX_ASSET_RECORDS,
            )
            .await?;
        packets
            .iter()
            .map(|packet| Ok(RespondPossessedAssets::decode(&packet.payload)?))
            .collect()
    }

    /// Requests asset records matching an arbitrary [`RequestAssets`] filter or
    /// universe index, in peer order.
    pub async fn asset_records(
        &mut self,
        request: &RequestAssets,
    ) -> Result<Vec<AssetRecordResponse>> {
        let with_siblings = request.flags() & ASSET_REQUEST_GET_SIBLINGS != 0;
        let packets = self
            .request_stream_bounded(
                MessageType::RequestAssets,
                request_assets(request),
                MessageType::RespondAssets,
                MAX_ASSET_RECORDS,
            )
            .await?;
        packets
            .iter()
            .map(|packet| {
                if with_siblings {
                    Ok(AssetRecordResponse::WithSiblings(
                        RespondAssetsWithSiblings::decode(&packet.payload)?,
                    ))
                } else {
                    Ok(AssetRecordResponse::Asset(RespondAssets::decode(
                        &packet.payload,
                    )?))
                }
            })
            .collect()
    }

    /// Validates and broadcasts a signed transaction.
    ///
    /// The signature is checked locally against the source public key before
    /// anything is written, so an unsigned or corrupted transaction never
    /// reaches the network. Broadcasts use `dejavu = 0`, which is what makes a
    /// core relay the transaction to its other peers. There is no
    /// acknowledgement on this path; use [`Peer::transaction`] and
    /// [`Peer::tx_status`] to observe confirmation.
    pub async fn broadcast_transaction(
        &mut self,
        transaction: &Transaction,
    ) -> Result<BroadcastReceipt> {
        validate_signed_transaction(transaction)?;
        let bytes = transaction.encode();
        self.send_gossip(MessageType::BroadcastTransaction, bytes)
            .await?;
        Ok(BroadcastReceipt {
            digest: transaction.hash(),
            tick: transaction.tick,
            peer: self.config.addr,
        })
    }

    /// Requests the log id ranges for every transaction in a tick.
    ///
    /// A decoded [`LogRanges`] response is proof that the peer has finished
    /// log verification for this tick. It may legitimately contain only `-1`
    /// slots when the tick emitted no logs. `EndResponse`, `TryAgain`, and
    /// timeouts mean the peer cannot prove the tick's log state and must be
    /// treated as unavailable, never as an empty tick.
    pub async fn log_ranges(&mut self, tick: u32) -> Result<LogRanges> {
        let result = self
            .request_single_any_bounded(
                MessageType::RequestAllLogIdRangesFromTx,
                request_all_log_ranges(tick, self.config.passcode),
                &[MessageType::RespondAllLogIdRangesFromTx],
                self.config.log_range_timeout,
                0,
            )
            .await;
        match result {
            Ok(packet) => {
                let ranges = LogRanges::decode(&packet.payload)?;
                ranges.validate()?;
                Ok(ranges)
            }
            Err(Error::EndResponse(_)) => Err(Error::LogsUnavailable(format!(
                "peer {} has no verified log ranges for tick {tick}",
                self.config.addr
            ))),
            Err(Error::TryAgain(_)) => Err(Error::LogsUnavailable(format!(
                "peer {} is not ready to serve log ranges for tick {tick}",
                self.config.addr
            ))),
            Err(Error::Timeout(elapsed)) => Err(Error::LogsUnavailable(format!(
                "peer {} did not answer log range request for tick {tick} after {elapsed:?}",
                self.config.addr
            ))),
            Err(error) => Err(error),
        }
    }

    /// Detects which log-range request the peer implements.
    ///
    /// The answer is cached per address. A peer that supports neither request
    /// cannot supply log data and will be skipped by the ingestion pipeline.
    async fn log_protocol(&mut self, tick_data: &TickData) -> Result<LogProtocol> {
        if let Some(protocol) = self.log_protocol {
            return Ok(protocol);
        }
        let tick = tick_data.tick;
        match self.log_ranges(tick).await {
            Ok(_) => {
                self.set_log_protocol(LogProtocol::AllRanges);
                return Ok(LogProtocol::AllRanges);
            }
            Err(Error::LogsUnavailable(_)) => {}
            Err(error) => return Err(error),
        }

        let first = log_probe_indices(tick_data).into_iter().next().unwrap_or(0);
        match self.tx_log_range(tick, first).await {
            Ok(_) => {
                self.set_log_protocol(LogProtocol::TxRanges);
                Ok(LogProtocol::TxRanges)
            }
            Err(Error::EndResponse(_)) => {
                // Do not cache this: a peer that has not generated logs for
                // this particular tick may still serve a later tick.
                Ok(LogProtocol::NoLogging)
            }
            Err(Error::TryAgain(_)) | Err(Error::Timeout(_)) => {
                Err(Error::LogsUnavailable(format!(
                    "peer {} did not answer log protocol probe",
                    self.config.addr
                )))
            }
            Err(error) => Err(error),
        }
    }

    fn set_log_protocol(&mut self, protocol: LogProtocol) {
        self.log_protocol = Some(protocol);
        if let Ok(mut cache) = log_protocol_cache().lock() {
            cache.insert(self.config.addr, protocol);
        }
    }

    /// Operator-facing name for the log protocol this connection negotiated.
    pub async fn log_protocol_name(&mut self, tick_data: &TickData) -> Result<&'static str> {
        Ok(match self.log_protocol(tick_data).await? {
            LogProtocol::AllRanges => "all-ranges",
            LogProtocol::TxRanges => "tx-ranges",
            LogProtocol::NoLogging => "none",
        })
    }

    /// Requests one slot's log range with `RequestLogIdRangeFromTx`.
    ///
    /// `EndResponse` is deliberately propagated: it means the peer does not
    /// implement this request (or rejects the tick), not that the slot has no
    /// logs. A real no-log slot is a 16-byte response with `-1/-1`.
    async fn tx_log_range(&mut self, tick: u32, slot: usize) -> Result<(i64, i64)> {
        let packet = self
            .request_single_any_bounded(
                MessageType::RequestLogIdRangeFromTx,
                qubic::messages::request_log_id_range_from_tx(
                    tick,
                    slot as u32,
                    self.config.passcode,
                ),
                &[MessageType::RespondLogIdRangeFromTx],
                self.config.log_range_timeout,
                0,
            )
            .await?;
        if packet.payload.len() != 16 {
            return Err(Error::Verification(format!(
                "log range response needs 16 bytes, got {}",
                packet.payload.len()
            )));
        }
        let from = i64::from_le_bytes(packet.payload[0..8].try_into().expect("8 bytes"));
        let length = i64::from_le_bytes(packet.payload[8..16].try_into().expect("8 bytes"));
        if from == -3 || length == -3 {
            return Err(Error::LogsUnavailable(format!(
                "peer {} has not generated logs for tick {tick}",
                self.config.addr
            )));
        }
        Ok((from, length))
    }

    /// Requests log records in `[from_log_id, to_log_id]`.
    ///
    /// A core truncates an oversized range to fit one packet, so this keeps
    /// requesting the remainder until the range is covered or the peer sends
    /// an empty EndResponse (logging disabled or pruned).
    pub async fn logs(&mut self, from_log_id: u64, to_log_id: u64) -> Result<Vec<LogEvent>> {
        if from_log_id > to_log_id {
            return Ok(Vec::new());
        }
        let mut events = Vec::new();
        let mut cursor = from_log_id;
        for _ in 0..MAX_LOG_PAGES {
            if cursor > to_log_id {
                return Ok(events);
            }
            let packet = self
                .request_single_any_bounded(
                    MessageType::RequestLog,
                    request_logs(cursor, to_log_id, self.config.passcode),
                    &[MessageType::RespondLog, MessageType::EndResponse],
                    self.config.timeout,
                    0,
                )
                .await?;
            if packet.kind == MessageType::EndResponse {
                return Ok(events);
            }
            let page = LogEvent::decode_many(&packet.payload)?;
            let mut highest = None;
            for event in page {
                if event.log_id > to_log_id {
                    break;
                }
                if event.log_id >= cursor {
                    highest = Some(event.log_id);
                    events.push(event);
                }
            }
            match highest {
                Some(id) => cursor = id.saturating_add(1),
                None => return Ok(events),
            }
        }
        Err(Error::Verification(format!(
            "log range {from_log_id}..={to_log_id} exceeded {MAX_LOG_PAGES} pages"
        )))
    }

    /// Assembles all data for one tick into a bundle.
    ///
    /// Fetches the head and computors first. Callers that already have a head
    /// and computors for the same epoch should use [`Peer::bundle_with`] to
    /// skip those round trips.
    pub async fn bundle(&mut self, tick: u32) -> Result<TickBundle> {
        tracing::debug!(tick, "bundle: head");
        let head = self.head().await?;
        if tick > head.tick {
            return Err(Error::NotSynchronized {
                head: head.tick,
                requested: tick,
            });
        }

        tracing::debug!(tick, "bundle: computors");
        let computors = self.computors().await?;
        self.bundle_with(tick, head, computors).await
    }

    /// Assembles a tick bundle from an already-known head and computors.
    ///
    /// This is the loop path for indexers: head and computors change on epoch
    /// boundaries, not per tick. `head` must be at or after `tick`.
    pub async fn bundle_with(
        &mut self,
        tick: u32,
        head: Head,
        computors: Computors,
    ) -> Result<TickBundle> {
        // A worker holding the previous epoch's head must not fetch a stale
        // tick after rollover; the error carries the peer's current head tick.
        health::check_cached_head(self.observed_head.as_ref(), &head)?;
        let newer = self.observed_head.as_ref().is_none_or(|observed| {
            observed.epoch < head.epoch
                || (observed.epoch == head.epoch && observed.tick < head.tick)
        });
        if newer {
            self.observed_head = Some(head.clone());
        }
        if tick > head.tick {
            return Err(Error::NotSynchronized {
                head: head.tick,
                requested: tick,
            });
        }
        tracing::debug!(tick, "bundle: pipelined tick data, votes, transactions");
        let packets = self.request_bundle_packets(tick).await?;
        let votes = decode_votes(packets.votes, tick)?;
        let transactions = decode_transactions(packets.transactions, tick)?;

        // `RequestTickData` omits ticks with no transaction payload. Those
        // ticks are still part of the chain: quorum votes carry the timestamp
        // and prove the empty transaction set. Synthesize the data payload so
        // the verifier can accept the 226-vote empty-tick quorum.
        let tick_data = match packets.tick_data {
            Some(packet) => TickData::decode(&packet.payload)?,
            None => {
                tracing::debug!(tick, "tick data absent; checking for an empty tick");
                if votes.is_empty() || votes.iter().any(|vote| !vote.is_empty_vote()) {
                    return Err(Error::MissingNonEmptyTick { tick });
                } else {
                    empty_tick_from_votes(&votes, tick)?
                }
            }
        };
        if tick_data.tick != tick {
            return Err(Error::Verification(format!(
                "tick data is for tick {}, requested {tick}",
                tick_data.tick
            )));
        }

        tracing::debug!(
            tick,
            votes = votes.len(),
            transactions = transactions.len(),
            "bundle: streams decoded"
        );

        // Historical or unindexed ticks may not have log data; that is not fatal.
        tracing::debug!(
            tick,
            transactions = transactions.len(),
            "bundle: log ranges"
        );
        let mut warnings = Vec::new();
        let fetched = match self.complete_logs(&tick_data).await {
            Ok(fetched) => fetched,
            Err(error) => FetchedLogs::incomplete(format!(
                "peer {} could not fetch logs: {error}",
                self.config.addr
            )),
        };
        let logs_complete = fetched.complete;
        let log_ranges = fetched.ranges;
        let logs = fetched.logs;
        warnings.extend(fetched.warnings);

        tracing::debug!(tick, "bundle: logs done");
        Ok(TickBundle {
            head,
            computors,
            tick: first_vote_or_synthetic(&votes, &tick_data, tick)?,
            tick_data,
            votes,
            transactions,
            log_ranges,
            logs,
            logs_complete,
            warnings,
        })
    }

    /// Sends tick data, quorum votes, and tick transactions before reading any
    /// response. A live tick then costs one round trip for all three streams
    /// instead of one round trip per request.
    async fn request_bundle_packets(&mut self, tick: u32) -> Result<BundlePackets> {
        let tick_data = self
            .send(MessageType::RequestTickData, request_tick_data(tick))
            .await?;
        let votes = self
            .send(
                MessageType::RequestQuorumTick,
                request_quorum_tick(tick, &[false; COMPUTOR_COUNT]),
            )
            .await?;
        let transactions = self
            .send(
                MessageType::RequestTickTransactions,
                request_tick_transactions(tick, &[false; TX_PER_TICK]),
            )
            .await?;
        let deadline = tokio::time::Instant::now() + self.config.timeout;
        let mut out = BundlePackets {
            tick_data: None,
            votes: Vec::with_capacity(COMPUTOR_COUNT),
            transactions: Vec::with_capacity(TX_PER_TICK),
        };
        let mut tick_data_done = false;
        let mut votes_done = false;
        let mut transactions_done = false;

        while !(tick_data_done && votes_done && transactions_done) {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                self.desynced = true;
                return Err(Error::Timeout(self.config.timeout));
            }
            let packet = self.read_packet_timeout(remaining).await?;
            if packet.kind == MessageType::ExchangePublicPeers {
                self.record_peers(&packet);
                continue;
            }
            if packet.dejavu == tick_data {
                self.pending.remove(&tick_data);
                tick_data_done = true;
                match packet.kind {
                    MessageType::BroadcastFutureTickData => out.tick_data = Some(packet),
                    MessageType::EndResponse => {}
                    MessageType::TryAgain => {
                        self.desynced = true;
                        return Err(Error::TryAgain(MessageType::RequestTickData));
                    }
                    actual => {
                        self.desynced = true;
                        return Err(Error::UnexpectedPacket {
                            expected: MessageType::BroadcastFutureTickData,
                            actual,
                        });
                    }
                }
            } else if packet.dejavu == votes {
                match packet.kind {
                    MessageType::BroadcastTick => {
                        if out.votes.len() >= COMPUTOR_COUNT {
                            self.pending.remove(&votes);
                            self.desynced = true;
                            return Err(Error::InvalidRequest(
                                "peer sent too many quorum votes".into(),
                            ));
                        }
                        out.votes.push(packet);
                    }
                    MessageType::EndResponse => {
                        self.pending.remove(&votes);
                        votes_done = true;
                    }
                    MessageType::TryAgain => {
                        self.pending.remove(&votes);
                        self.desynced = true;
                        return Err(Error::TryAgain(MessageType::RequestQuorumTick));
                    }
                    actual => {
                        self.pending.remove(&votes);
                        self.desynced = true;
                        return Err(Error::UnexpectedPacket {
                            expected: MessageType::BroadcastTick,
                            actual,
                        });
                    }
                }
            } else if packet.dejavu == transactions {
                match packet.kind {
                    MessageType::BroadcastTransaction => {
                        if out.transactions.len() >= TX_PER_TICK {
                            self.pending.remove(&transactions);
                            self.desynced = true;
                            return Err(Error::InvalidRequest(
                                "peer sent too many transactions".into(),
                            ));
                        }
                        out.transactions.push(packet);
                    }
                    MessageType::EndResponse => {
                        self.pending.remove(&transactions);
                        transactions_done = true;
                    }
                    MessageType::TryAgain => {
                        self.pending.remove(&transactions);
                        self.desynced = true;
                        return Err(Error::TryAgain(MessageType::RequestTickTransactions));
                    }
                    actual => {
                        self.pending.remove(&transactions);
                        self.desynced = true;
                        return Err(Error::UnexpectedPacket {
                            expected: MessageType::BroadcastTransaction,
                            actual,
                        });
                    }
                }
            }
        }

        Ok(out)
    }

    // ---- request machinery -------------------------------------------------

    async fn request_single(
        &mut self,
        request: MessageType,
        payload: Vec<u8>,
        expected: MessageType,
    ) -> Result<Packet> {
        self.request_single_any(request, payload, &[expected]).await
    }

    async fn request_single_any(
        &mut self,
        request: MessageType,
        payload: Vec<u8>,
        expected: &[MessageType],
    ) -> Result<Packet> {
        self.request_single_any_bounded(
            request,
            payload,
            expected,
            self.config.timeout,
            self.config.retries,
        )
        .await
    }

    async fn request_single_any_bounded(
        &mut self,
        request: MessageType,
        payload: Vec<u8>,
        expected: &[MessageType],
        timeout: Duration,
        retries: u32,
    ) -> Result<Packet> {
        let attempts = retries.saturating_add(1);
        let mut attempt = 0;
        loop {
            attempt += 1;
            let dejavu = self.send(request, payload.clone()).await?;
            match self
                .receive_until_expected(dejavu, request, expected, timeout)
                .await
            {
                Ok(packet) => return Ok(packet),
                Err(Error::TryAgain(_)) if attempt < attempts => {
                    tracing::debug!(?request, "peer asked to try again");
                }
                Err(Error::Timeout(elapsed)) if attempt < attempts => {
                    tracing::debug!(?request, ?elapsed, "request timed out, reconnecting");
                    self.reconnect().await?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn request_stream(
        &mut self,
        request: MessageType,
        payload: Vec<u8>,
        expected: MessageType,
    ) -> Result<Vec<Packet>> {
        self.request_stream_bounded(request, payload, expected, usize::MAX)
            .await
    }

    async fn request_stream_bounded(
        &mut self,
        request: MessageType,
        payload: Vec<u8>,
        expected: MessageType,
        max_packets: usize,
    ) -> Result<Vec<Packet>> {
        let attempts = self.config.retries.saturating_add(1);
        let mut attempt = 0;
        loop {
            attempt += 1;
            let dejavu = self.send(request, payload.clone()).await?;
            match self
                .receive_stream(dejavu, request, expected, max_packets)
                .await
            {
                Ok(packets) => return Ok(packets),
                Err(Error::TryAgain(_)) if attempt < attempts => {
                    tracing::debug!(?request, "peer asked to try again");
                }
                Err(Error::Timeout(timeout)) if attempt < attempts => {
                    tracing::debug!(?request, ?timeout, "request timed out, reconnecting");
                    self.reconnect().await?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn send(&mut self, request: MessageType, payload: Vec<u8>) -> Result<u32> {
        if self.desynced {
            self.reconnect().await?;
        }
        let dejavu = self.allocate_dejavu();
        let packet = Packet::new(request, dejavu, payload);
        self.writer.write_all(&packet.encode()).await?;
        self.writer.flush().await?;
        self.pending.insert(dejavu, request);
        Ok(dejavu)
    }

    /// Writes a gossip packet with `dejavu = 0`. No response is expected and
    /// no pending slot is allocated.
    async fn send_gossip(&mut self, kind: MessageType, payload: Vec<u8>) -> Result<()> {
        if self.desynced {
            self.reconnect().await?;
        }
        self.writer
            .write_all(&Packet::new(kind, 0, payload).encode())
            .await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn receive_until_expected(
        &mut self,
        dejavu: u32,
        request: MessageType,
        expected: &[MessageType],
        timeout: Duration,
    ) -> Result<Packet> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                self.desynced = true;
                return Err(Error::Timeout(timeout));
            }
            let packet = self.read_packet_timeout(remaining).await?;
            if packet.kind == MessageType::ExchangePublicPeers {
                self.record_peers(&packet);
                continue;
            }
            // Unsolicited broadcast gossip carries a different dejavu.
            if packet.dejavu != dejavu {
                continue;
            }
            self.pending.remove(&dejavu);
            if expected.contains(&packet.kind) {
                return Ok(packet);
            }
            return Err(match packet.kind {
                MessageType::TryAgain => Error::TryAgain(request),
                MessageType::EndResponse => Error::EndResponse(request),
                actual => Error::UnexpectedPacket {
                    expected: expected.first().copied().unwrap_or(request),
                    actual,
                },
            });
        }
    }

    async fn receive_stream(
        &mut self,
        dejavu: u32,
        request: MessageType,
        expected: MessageType,
        max_packets: usize,
    ) -> Result<Vec<Packet>> {
        let mut packets = Vec::new();
        let timeout = self.config.timeout;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                self.desynced = true;
                return Err(Error::Timeout(timeout));
            }
            let packet = self.read_packet_timeout(remaining).await?;
            if packet.kind == MessageType::ExchangePublicPeers {
                self.record_peers(&packet);
                continue;
            }
            if packet.dejavu != dejavu {
                continue;
            }
            match packet.kind {
                MessageType::EndResponse => {
                    self.pending.remove(&dejavu);
                    return Ok(packets);
                }
                MessageType::TryAgain => {
                    self.pending.remove(&dejavu);
                    return Err(Error::TryAgain(request));
                }
                kind if kind == expected => {
                    if packets.len() >= max_packets {
                        // Stop reading mid-stream; the connection is no longer
                        // at a packet boundary, so reconnect before the next
                        // request.
                        self.pending.remove(&dejavu);
                        self.desynced = true;
                        return Err(Error::InvalidRequest(format!(
                            "peer sent more than {max_packets} {expected:?} packets"
                        )));
                    }
                    packets.push(packet);
                }
                actual => {
                    self.pending.remove(&dejavu);
                    return Err(Error::UnexpectedPacket { expected, actual });
                }
            }
        }
    }

    async fn read_packet_timeout(&mut self, timeout: Duration) -> Result<Packet> {
        match tokio::time::timeout(timeout, Self::read_packet(&mut self.reader)).await {
            Ok(result) => result,
            Err(_) => {
                // The cancelled read may have consumed part of a packet.
                self.desynced = true;
                Err(Error::Timeout(timeout))
            }
        }
    }

    async fn read_packet(reader: &mut BufReader<OwnedReadHalf>) -> Result<Packet> {
        let mut header = [0u8; HEADER_SIZE];
        reader.read_exact(&mut header).await?;
        let header = qubic::Header::decode(&header)?;
        let payload_len = header.size as usize - HEADER_SIZE;
        let mut payload = vec![0u8; payload_len];
        reader.read_exact(&mut payload).await?;
        Ok(Packet {
            kind: header.kind,
            dejavu: header.dejavu,
            payload,
        })
    }

    async fn reconnect(&mut self) -> Result<()> {
        let stream = Self::open(&self.config).await?;
        let (reader, writer) = stream.into_split();
        self.reader = BufReader::new(reader);
        self.writer = writer;
        self.pending.clear();
        self.desynced = false;
        self.handshake().await
    }

    fn allocate_dejavu(&mut self) -> u32 {
        loop {
            self.next_dejavu = self.next_dejavu.wrapping_add(1);
            if self.next_dejavu == 0 {
                self.next_dejavu = 1;
            }
            if !self.pending.contains_key(&self.next_dejavu) {
                return self.next_dejavu;
            }
        }
    }

    fn record_peers(&mut self, packet: &Packet) {
        if let Ok(peers) = PeerAddress::decode(&packet.payload) {
            for peer in peers {
                if !self.public_peers.contains(&peer) {
                    self.public_peers.push(peer);
                }
            }
        }
    }
}

/// A pool of independent peer connections with shared cross-peer health.
///
/// Cloning a pool shares the [`PeerManager`], so a long-lived API state keeps
/// remembering failures and latency across requests.
#[derive(Clone)]
pub struct NodePool {
    peers: Vec<PeerConfig>,
    manager: PeerManager,
    load: std::sync::Arc<std::sync::Mutex<HashMap<SocketAddr, usize>>>,
    connection_limit: usize,
    slot_available: std::sync::Arc<tokio::sync::Notify>,
    log_peer: std::sync::Arc<std::sync::Mutex<Option<SocketAddr>>>,
    log_slots: std::sync::Arc<tokio::sync::Semaphore>,
}

impl NodePool {
    pub fn new(peers: Vec<PeerConfig>) -> Self {
        let connections = std::env::var("INDEXER_PEER_CONNECTIONS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(4)
            .clamp(1, 32);
        Self::with_policy(peers, PeerPolicy::default()).with_connection_limit(connections)
    }

    /// Builds a pool with explicit health policy, primarily for tests and
    /// deployments that prefer a shorter quarantine.
    pub fn with_policy(peers: Vec<PeerConfig>, policy: PeerPolicy) -> Self {
        Self {
            peers,
            manager: PeerManager::new(policy),
            load: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
            connection_limit: 4,
            slot_available: std::sync::Arc::new(tokio::sync::Notify::new()),
            log_peer: std::sync::Arc::new(std::sync::Mutex::new(None)),
            log_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(2)),
        }
    }

    /// Caps concurrent TCP connections to one peer. Qubic full nodes accept a
    /// bounded number of connections from one address; exceeding the limit
    /// produces early EOFs and wastes handshakes.
    pub fn with_connection_limit(mut self, limit: usize) -> Self {
        self.connection_limit = limit.max(1);
        self
    }

    pub fn peer_configs(&self) -> &[PeerConfig] {
        &self.peers
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    /// Replace the peer list while keeping the shared health state.
    pub fn set_peers(&mut self, peers: Vec<PeerConfig>) {
        self.peers = peers;
    }

    /// Record a request success against a peer already returned by this pool.
    pub fn record_success(&self, addr: SocketAddr, latency: Duration) {
        self.manager.record_success(addr, latency);
    }

    /// Clear failures after a successful request without touching latency.
    pub fn record_healthy(&self, addr: SocketAddr) {
        self.manager.record_healthy(addr);
    }

    /// Record a request failure so repeated resets quarantine the peer.
    pub fn record_failure(&self, addr: SocketAddr) {
        self.manager.record_failure(addr);
    }

    /// Record completed fetch throughput for peer ranking.
    pub fn record_fetch(&self, addr: SocketAddr, ticks: usize, elapsed: Duration) {
        self.manager.record_fetch(addr, ticks, elapsed);
    }

    /// Connect to the best healthy peer with a free connection slot and hold a
    /// connection lease.
    ///
    /// `connect_best` is right for one-shot API reads. A pool of long-lived
    /// indexer workers needs `connect_balanced`, otherwise every worker picks
    /// the same low-latency peer and resets its connection under the herd.
    pub async fn connect_balanced(&self) -> Result<PooledPeer> {
        loop {
            let mut candidates = self.manager.attempt_order(&self.peers);
            if candidates.is_empty() {
                // Health quarantines must not turn a brief all-peer outage into
                // a long stall. Fall back to every configured peer; a successful
                // connect clears its quarantine.
                candidates = self.peers.clone();
            }
            if candidates.is_empty() {
                return Err(Error::NoPeers);
            }
            let mut last_error = None;
            while !candidates.is_empty() {
                let Some((config, index)) = self.reserve_best_available(&candidates) else {
                    break;
                };
                let addr = config.addr;
                let started = std::time::Instant::now();
                match Peer::connect(config).await {
                    Ok(peer) => {
                        self.manager.record_success(addr, started.elapsed());
                        return Ok(PooledPeer {
                            peer,
                            addr,
                            load: self.load.clone(),
                            slot_available: self.slot_available.clone(),
                        });
                    }
                    Err(error) => {
                        self.add_load(addr, -1);
                        self.manager.record_failure(addr);
                        candidates.remove(index);
                        last_error = Some(error);
                    }
                }
            }
            // Every usable peer is at its connection cap. Wait for a worker to
            // release a slot instead of opening an extra socket.
            tokio::select! {
                _ = self.slot_available.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(250)) => {}
            }
            if let Some(error) = last_error {
                tracing::debug!(%error, "no peer connection slot available");
            }
        }
    }

    /// Requests one tick's data from every configured peer in parallel and
    /// returns the first copy. Used when a peer's batch response lacks tick
    /// data that another peer still retains.
    pub async fn tick_data(&self, tick: u32) -> Result<TickData> {
        let mut candidates = self.manager.attempt_order(&self.peers);
        for config in &self.peers {
            if !candidates
                .iter()
                .any(|candidate| candidate.addr == config.addr)
            {
                candidates.push(config.clone());
            }
        }
        if candidates.is_empty() {
            return Err(Error::NoPeers);
        }
        let mut tasks = tokio::task::JoinSet::new();
        for config in candidates {
            tasks.spawn(async move {
                let started = std::time::Instant::now();
                let addr = config.addr;
                let mut peer = match Peer::connect(config).await {
                    Ok(peer) => peer,
                    Err(error) => return Err((error, addr)),
                };
                match peer.tick_data(tick).await {
                    Ok(data) => Ok((data, addr, started.elapsed())),
                    Err(error) => Err((error, addr)),
                }
            });
        }
        let mut last_error = None;
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Ok((data, addr, latency))) => {
                    self.manager.record_success(addr, latency);
                    tasks.abort_all();
                    return Ok(data);
                }
                Ok(Err((error, addr))) => {
                    if matches!(
                        error,
                        Error::Io(_)
                            | Error::Wire(_)
                            | Error::Timeout(_)
                            | Error::RetriesExhausted { .. }
                            | Error::TryAgain(_)
                            | Error::UnexpectedPacket { .. }
                    ) {
                        self.manager.record_failure(addr);
                    }
                    last_error = Some(error);
                }
                Err(_) => last_error = Some(Error::NoPeers),
            }
        }
        Err(last_error.unwrap_or(Error::NoPeers))
    }

    /// Requests quorum votes from every configured peer in parallel and
    /// returns the largest set. A batch can miss votes from one peer even when
    /// another peer has the full quorum.
    pub async fn votes(&self, tick: u32) -> Result<Vec<Tick>> {
        let mut candidates = self.manager.attempt_order(&self.peers);
        for config in &self.peers {
            if !candidates
                .iter()
                .any(|candidate| candidate.addr == config.addr)
            {
                candidates.push(config.clone());
            }
        }
        if candidates.is_empty() {
            return Err(Error::NoPeers);
        }
        let mut tasks = tokio::task::JoinSet::new();
        for config in candidates {
            tasks.spawn(async move {
                let started = std::time::Instant::now();
                let addr = config.addr;
                let mut peer = match Peer::connect(config).await {
                    Ok(peer) => peer,
                    Err(error) => return Err((error, addr)),
                };
                match peer.votes(tick, &[false; COMPUTOR_COUNT]).await {
                    Ok(votes) => Ok((votes, addr, started.elapsed())),
                    Err(error) => Err((error, addr)),
                }
            });
        }
        let mut best = Vec::new();
        let mut last_error = None;
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Ok((votes, addr, latency))) => {
                    self.manager.record_success(addr, latency);
                    if unique_votes(&votes) > unique_votes(&best) {
                        best = votes;
                    }
                }
                Ok(Err((error, addr))) => {
                    if matches!(
                        error,
                        Error::Io(_)
                            | Error::Wire(_)
                            | Error::Timeout(_)
                            | Error::RetriesExhausted { .. }
                            | Error::TryAgain(_)
                            | Error::UnexpectedPacket { .. }
                    ) {
                        self.manager.record_failure(addr);
                    }
                    last_error = Some(error);
                }
                Err(_) => last_error = Some(Error::NoPeers),
            }
        }
        if best.is_empty() {
            Err(last_error.unwrap_or(Error::NoPeers))
        } else {
            Ok(best)
        }
    }

    /// Reserve the best candidate with a free slot while holding the load
    /// lock, so simultaneous workers cannot all pick the same peer.
    fn reserve_best_available(&self, candidates: &[PeerConfig]) -> Option<(PeerConfig, usize)> {
        let mut loads = self.load.lock().ok()?;
        // `candidates` is already ordered best-first. Preserve that order and
        // take the first peer with a free connection slot; least-loaded-first
        // would send work to a slow peer just because it is idle.
        let (index, _) = candidates.iter().enumerate().find(|(_, config)| {
            loads.get(&config.addr).copied().unwrap_or(0) < self.connection_limit
        })?;
        let config = candidates[index].clone();
        *loads.entry(config.addr).or_insert(0) += 1;
        Some((config, index))
    }

    fn add_load(&self, addr: SocketAddr, delta: isize) {
        if let Ok(mut loads) = self.load.lock() {
            let entry = loads.entry(addr).or_insert(0);
            *entry = if delta < 0 {
                entry.saturating_sub(delta.unsigned_abs())
            } else {
                entry.saturating_add(delta as usize)
            };
        }
    }

    /// Per-peer operator view: score, counters, latency, and quarantine.
    pub fn health(&self) -> Vec<PeerHealth> {
        self.manager.report(&self.peers)
    }

    pub fn record_log_failure(&self, addr: SocketAddr) {
        self.manager.record_log_failure(addr);
    }

    pub fn record_log_success(&self, addr: SocketAddr) {
        self.manager.record_log_success(addr);
        *self.log_peer.lock().expect("log peer mutex poisoned") = Some(addr);
    }

    /// Adds peers advertised by one reachable peer, up to
    /// [`DEFAULT_DISCOVERY_LIMIT`] total. Discovery is opt-in; API request
    /// paths must not call this.
    pub async fn discover(&mut self) -> Result<Vec<PeerAddress>> {
        self.discover_with_limit(DEFAULT_DISCOVERY_LIMIT).await
    }

    /// Discovery with an explicit total peer cap.
    pub async fn discover_with_limit(&mut self, limit: usize) -> Result<Vec<PeerAddress>> {
        let peer = self.connect_best().await?;
        let advertised = peer.public_peers().to_vec();
        let added = health::discovered_configs(&self.peers, &advertised, limit);
        let addresses = added
            .iter()
            .filter_map(|config| match config.addr {
                SocketAddr::V4(address) => {
                    Some(PeerAddress::new(address.ip().octets(), address.port()))
                }
                SocketAddr::V6(_) => None,
            })
            .collect();
        self.peers.extend(added);
        Ok(addresses)
    }

    /// Queries every peer and returns the most advanced head. A lagging peer
    /// must not make the network look caught up.
    pub async fn head(&self) -> Result<Head> {
        let mut candidates = self.manager.attempt_order(&self.peers);
        for config in &self.peers {
            if !candidates
                .iter()
                .any(|candidate| candidate.addr == config.addr)
            {
                candidates.push(config.clone());
            }
        }
        if candidates.is_empty() {
            return Err(Error::NoPeers);
        }
        let mut tasks = tokio::task::JoinSet::new();
        for config in candidates {
            tasks.spawn(async move {
                let started = std::time::Instant::now();
                let addr = config.addr;
                let mut peer = match Peer::connect(config).await {
                    Ok(peer) => peer,
                    Err(error) => return Err((error, addr)),
                };
                match peer.head().await {
                    Ok(head) => Ok((head, addr, started.elapsed())),
                    Err(error) => Err((error, addr)),
                }
            });
        }
        let mut best: Option<Head> = None;
        let mut last_error = None;
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Ok((head, addr, latency))) => {
                    self.manager.record_success(addr, latency);
                    let newer = best.as_ref().is_none_or(|best| {
                        head.epoch > best.epoch
                            || (head.epoch == best.epoch && head.tick > best.tick)
                    });
                    if newer {
                        best = Some(head);
                    }
                }
                Ok(Err((error, addr))) => {
                    self.manager.record_failure(addr);
                    last_error = Some(error);
                }
                Err(_) => last_error = Some(Error::NoPeers),
            }
        }
        best.ok_or_else(|| last_error.unwrap_or(Error::NoPeers))
    }

    /// Requests system info from the best currently reachable peer.
    pub async fn system_info(&self) -> Result<SystemInfo> {
        self.connect_best().await?.system_info().await
    }

    /// Requests live revenue scores from the best currently reachable peer.
    pub async fn revenue_data(&self) -> Result<RespondRevenueData> {
        self.connect_best().await?.revenue_data().await
    }

    /// Looks up a spectrum entity on every reachable peer and returns the
    /// response with the highest observed tick. A lagging peer must not win
    /// just because its handshake was faster.
    pub async fn entity(&self, public_key: [u8; 32]) -> Result<RespondEntity> {
        let mut candidates = self.manager.attempt_order(&self.peers);
        for config in &self.peers {
            if !candidates
                .iter()
                .any(|candidate| candidate.addr == config.addr)
            {
                candidates.push(config.clone());
            }
        }
        let mut tasks = tokio::task::JoinSet::new();
        for config in candidates {
            tasks.spawn(async move {
                let started = std::time::Instant::now();
                let addr = config.addr;
                let mut peer = match Peer::connect(config).await {
                    Ok(peer) => peer,
                    Err(error) => return Err((error, addr)),
                };
                match peer.entity(public_key).await {
                    Ok(entity) => Ok((entity, addr, started.elapsed())),
                    Err(error) => Err((error, addr)),
                }
            });
        }
        let mut best: Option<RespondEntity> = None;
        let mut last_error = None;
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Ok((entity, addr, latency))) => {
                    self.manager.record_success(addr, latency);
                    if best.as_ref().is_none_or(|best| entity.tick > best.tick) {
                        best = Some(entity);
                    }
                }
                Ok(Err((error, addr))) => {
                    self.manager.record_failure(addr);
                    last_error = Some(error);
                }
                Err(_) => last_error = Some(Error::NoPeers),
            }
        }
        best.ok_or_else(|| last_error.unwrap_or(Error::NoPeers))
    }

    /// Looks up a spectrum entity by its 60-character identity.
    pub async fn entity_by_identity(&self, identity: &str) -> Result<RespondEntity> {
        let identity = identity.to_string();
        let mut candidates = self.manager.attempt_order(&self.peers);
        for config in &self.peers {
            if !candidates
                .iter()
                .any(|candidate| candidate.addr == config.addr)
            {
                candidates.push(config.clone());
            }
        }
        let mut tasks = tokio::task::JoinSet::new();
        for config in candidates {
            let identity = identity.clone();
            tasks.spawn(async move {
                let started = std::time::Instant::now();
                let addr = config.addr;
                let mut peer = match Peer::connect(config).await {
                    Ok(peer) => peer,
                    Err(error) => return Err((error, addr)),
                };
                match peer.entity_by_identity(&identity).await {
                    Ok(entity) => Ok((entity, addr, started.elapsed())),
                    Err(error) => Err((error, addr)),
                }
            });
        }
        let mut best: Option<RespondEntity> = None;
        let mut last_error = None;
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Ok((entity, addr, latency))) => {
                    self.manager.record_success(addr, latency);
                    if best.as_ref().is_none_or(|best| entity.tick > best.tick) {
                        best = Some(entity);
                    }
                }
                Ok(Err((error, addr))) => {
                    self.manager.record_failure(addr);
                    last_error = Some(error);
                }
                Err(_) => last_error = Some(Error::NoPeers),
            }
        }
        best.ok_or_else(|| last_error.unwrap_or(Error::NoPeers))
    }

    /// Invokes a read-only contract function on the best currently reachable
    /// peer.
    pub async fn contract_function(
        &self,
        contract_index: u32,
        input_type: u16,
        input: &[u8],
    ) -> Result<Vec<u8>> {
        self.connect_best()
            .await?
            .contract_function(contract_index, input_type, input)
            .await
    }

    /// Looks up a confirmed transaction by digest on the best currently
    /// reachable peer.
    pub async fn transaction(&self, digest: [u8; 32]) -> Result<Option<Transaction>> {
        self.connect_best().await?.transaction(digest).await
    }

    /// Requests the confirmed transaction status for one tick.
    pub async fn tx_status(&self, tick: u32) -> Result<TransactionStatus> {
        self.connect_best().await?.tx_status(tick).await
    }

    /// Requests an hourly price series from the best currently reachable peer.
    pub async fn price_series(&self, from_hour: u32, count: u32) -> Result<RespondPriceSeries> {
        self.connect_best()
            .await?
            .price_series(from_hour, count)
            .await
    }

    /// Requests active oracle subscription IDs from the best currently
    /// reachable peer.
    pub async fn oracle_subscription_ids(&self) -> Result<Vec<i32>> {
        self.connect_best().await?.oracle_subscription_ids().await
    }

    /// Requests one oracle subscription and its initial query from the best
    /// currently reachable peer.
    pub async fn oracle_subscription(
        &self,
        subscription_id: i32,
    ) -> Result<OracleSubscriptionResponse> {
        self.connect_best()
            .await?
            .oracle_subscription(subscription_id)
            .await
    }

    /// Requests one oracle query, its metadata, and any reply from the best
    /// currently reachable peer.
    pub async fn oracle_query(&self, query_id: i64) -> Result<OracleQueryResponse> {
        self.connect_best().await?.oracle_query(query_id).await
    }

    /// Requests oracle query IDs selected by `req_type`.
    pub async fn oracle_query_ids(&self, req_type: u32, tick_or_id: i64) -> Result<Vec<i64>> {
        self.connect_best()
            .await?
            .oracle_query_ids(req_type, tick_or_id)
            .await
    }

    /// Requests oracle query processing statistics.
    pub async fn oracle_query_statistics(&self) -> Result<OracleQueryStatistics> {
        self.connect_best().await?.oracle_query_statistics().await
    }

    /// Requests the current epoch's 676 oracle revenue points.
    pub async fn oracle_revenue_points(&self) -> Result<Vec<u64>> {
        self.connect_best().await?.oracle_revenue_points().await
    }

    /// Requests active oracle subscriptions of one contract.
    pub async fn oracle_active_contract_subscriptions(
        &self,
        contract_index: u32,
    ) -> Result<Vec<i32>> {
        self.connect_best()
            .await?
            .oracle_active_contract_subscriptions(contract_index)
            .await
    }

    /// Requests one subscription with its query and subscribers.
    pub async fn oracle_subscription_detail(
        &self,
        subscription_id: i32,
    ) -> Result<OracleSubscriptionDetail> {
        self.connect_best()
            .await?
            .oracle_subscription_detail(subscription_id)
            .await
    }

    /// Requests every asset issued by `public_key` from the best currently
    /// reachable peer.
    pub async fn issued_assets(&self, public_key: [u8; 32]) -> Result<Vec<RespondIssuedAssets>> {
        self.connect_best().await?.issued_assets(public_key).await
    }

    /// Requests every asset owned by `public_key` from the best currently
    /// reachable peer.
    pub async fn owned_assets(&self, public_key: [u8; 32]) -> Result<Vec<RespondOwnedAssets>> {
        self.connect_best().await?.owned_assets(public_key).await
    }

    /// Requests every asset possessed by `public_key` from the best currently
    /// reachable peer.
    pub async fn possessed_assets(
        &self,
        public_key: [u8; 32],
    ) -> Result<Vec<RespondPossessedAssets>> {
        self.connect_best()
            .await?
            .possessed_assets(public_key)
            .await
    }

    /// Requests asset records matching a filter from the best currently
    /// reachable peer.
    pub async fn asset_records(&self, request: &RequestAssets) -> Result<Vec<AssetRecordResponse>> {
        self.connect_best().await?.asset_records(request).await
    }

    /// Validates and broadcasts a signed transaction via the best currently
    /// reachable peer.
    pub async fn broadcast_transaction(
        &self,
        transaction: &Transaction,
    ) -> Result<BroadcastReceipt> {
        self.connect_best()
            .await?
            .broadcast_transaction(transaction)
            .await
    }

    /// Sends one signed transaction to up to `max_peers` peers concurrently.
    ///
    /// Broader fan-out reduces the chance that a single slow or partitioned
    /// peer drops the transaction before it propagates. Every peer validates
    /// the signature and payload locally, so an invalid transaction fails on
    /// every attempt rather than reaching a relay.
    ///
    /// Qubic has no broadcast acknowledgement. A `sent` attempt means the
    /// packet was written to the peer socket, not that the transaction will be
    /// included in a tick.
    pub async fn broadcast_transaction_fanout(
        &self,
        transaction: &Transaction,
        max_peers: usize,
    ) -> Vec<BroadcastAttempt> {
        // Validate once so an obviously bad transaction does not spend time
        // opening sockets. Each peer validates again before sending.
        if let Err(error) = validate_signed_transaction(transaction) {
            return self
                .peers
                .iter()
                .take(max_peers.max(1))
                .map(|config| BroadcastAttempt {
                    peer: config.addr,
                    sent: false,
                    error: Some(error.to_string()),
                    elapsed: Duration::ZERO,
                })
                .collect();
        }

        let candidates: Vec<PeerConfig> = self
            .manager
            .attempt_order(&self.peers)
            .into_iter()
            .take(max_peers.max(1))
            .collect();
        if candidates.is_empty() {
            return Vec::new();
        }

        let mut tasks = tokio::task::JoinSet::new();
        for mut config in candidates {
            config.connect_timeout = config.connect_timeout.min(BROADCAST_CONNECT_TIMEOUT);
            config.timeout = config.timeout.min(BROADCAST_HANDSHAKE_TIMEOUT);
            config.retries = 0;
            let transaction = transaction.clone();
            tasks.spawn(async move {
                let started = std::time::Instant::now();
                let addr = config.addr;
                let result = async {
                    let mut peer = Peer::connect(config).await?;
                    peer.broadcast_transaction(&transaction).await
                }
                .await;
                (addr, started.elapsed(), result)
            });
        }

        let mut attempts = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            let Ok((peer, elapsed, result)) = joined else {
                continue;
            };
            match result {
                Ok(_) => {
                    self.manager.record_success(peer, elapsed);
                    attempts.push(BroadcastAttempt {
                        peer,
                        sent: true,
                        error: None,
                        elapsed,
                    });
                }
                Err(error) => {
                    self.manager.record_failure(peer);
                    attempts.push(BroadcastAttempt {
                        peer,
                        sent: false,
                        error: Some(error.to_string()),
                        elapsed,
                    });
                }
            }
        }
        attempts.sort_by_key(|attempt| (!attempt.sent, attempt.elapsed));
        attempts
    }

    /// Connects to the best-scoring available peer, recording the handshake
    /// latency or failure for future calls. Quarantined peers are skipped.
    pub async fn connect_best(&self) -> Result<Peer> {
        let candidates = self.manager.attempt_order(&self.peers);
        if candidates.is_empty() {
            return Err(Error::NoPeers);
        }
        let mut last_error = None;
        for config in candidates {
            let started = std::time::Instant::now();
            match Peer::connect(config.clone()).await {
                Ok(peer) => {
                    self.manager.record_success(config.addr, started.elapsed());
                    return Ok(peer);
                }
                Err(error) => {
                    self.manager.record_failure(config.addr);
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.unwrap_or(Error::NoPeers))
    }

    /// Requests transaction bodies from every configured peer in parallel and
    /// returns the largest set. A batch response can drop transaction packets
    /// while another peer still serves them.
    pub async fn transactions(
        &self,
        tick: u32,
        known: &[bool; TX_PER_TICK],
    ) -> Result<Vec<Transaction>> {
        let mut candidates = self.manager.attempt_order(&self.peers);
        for config in &self.peers {
            if !candidates
                .iter()
                .any(|candidate| candidate.addr == config.addr)
            {
                candidates.push(config.clone());
            }
        }
        if candidates.is_empty() {
            return Err(Error::NoPeers);
        }
        let mut tasks = tokio::task::JoinSet::new();
        for config in candidates {
            let known = *known;
            tasks.spawn(async move {
                let started = std::time::Instant::now();
                let addr = config.addr;
                let mut peer = match Peer::connect(config).await {
                    Ok(peer) => peer,
                    Err(error) => return Err((error, addr)),
                };
                match peer.transactions(tick, &known).await {
                    Ok(transactions) => Ok((transactions, addr, started.elapsed())),
                    Err(error) => Err((error, addr)),
                }
            });
        }
        let mut best = Vec::new();
        let mut last_error = None;
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Ok((transactions, addr, latency))) => {
                    self.manager.record_success(addr, latency);
                    let best_hashes = best
                        .iter()
                        .map(Transaction::hash)
                        .collect::<std::collections::HashSet<_>>();
                    let candidate_hashes = transactions
                        .iter()
                        .map(Transaction::hash)
                        .collect::<std::collections::HashSet<_>>();
                    if candidate_hashes.len() > best_hashes.len() {
                        best = transactions;
                    }
                }
                Ok(Err((error, addr))) => {
                    if matches!(
                        error,
                        Error::Io(_)
                            | Error::Wire(_)
                            | Error::Timeout(_)
                            | Error::RetriesExhausted { .. }
                            | Error::TryAgain(_)
                            | Error::UnexpectedPacket { .. }
                    ) {
                        self.manager.record_failure(addr);
                    }
                    last_error = Some(error);
                }
                Err(_) => last_error = Some(Error::NoPeers),
            }
        }
        if best.is_empty() {
            return Err(last_error.unwrap_or(Error::NoPeers));
        }
        Ok(best)
    }

    /// Fetches one tick's logs from every configured peer and returns only a
    /// proof-complete set. A peer that answers `EndResponse` has not proved
    /// that the tick has no logs, so it is skipped rather than stored as empty.
    pub async fn logs(&self, tick_data: &TickData) -> Result<(LogRanges, Vec<LogEvent>)> {
        let mut fetched = self.log_batch(std::slice::from_ref(tick_data)).await?;
        let logs = fetched.pop().expect("one log result");
        if logs.complete {
            Ok((logs.ranges, logs.logs))
        } else {
            Err(Error::LogsUnavailable(
                logs.warnings
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| "no peer supplied verified log ranges".into()),
            ))
        }
    }

    /// Repairs logs for a batch of ticks over one connection. Log fallback is
    /// the slow path; opening a socket per tick turns an empty-tick run into a
    /// connection storm.
    pub async fn log_batch(&self, tick_data: &[TickData]) -> Result<Vec<FetchedLogs>> {
        if tick_data.is_empty() {
            return Ok(Vec::new());
        }
        let _permit = self
            .log_slots
            .acquire()
            .await
            .expect("log fallback semaphore is never closed");
        let mut candidates = self.manager.attempt_order(&self.peers);
        for config in &self.peers {
            if !candidates
                .iter()
                .any(|candidate| candidate.addr == config.addr)
            {
                candidates.push(config.clone());
            }
        }
        if candidates.is_empty() {
            return Err(Error::NoPeers);
        }
        let preferred = *self.log_peer.lock().expect("log peer mutex poisoned");
        if let Some(addr) = preferred {
            if let Some(index) = candidates.iter().position(|config| config.addr == addr) {
                candidates.swap(0, index);
            }
        }
        let mut results: Vec<Option<FetchedLogs>> = (0..tick_data.len()).map(|_| None).collect();
        let mut remaining = (0..tick_data.len()).collect::<Vec<_>>();
        let mut last_error = None;
        for config in candidates {
            if remaining.is_empty() {
                break;
            }
            let addr = config.addr;
            let subset = remaining
                .iter()
                .map(|index| tick_data[*index].clone())
                .collect::<Vec<_>>();
            let fetched = tokio::time::timeout(Duration::from_secs(30), async {
                let mut peer = Peer::connect(config).await?;
                peer.complete_logs_batch(&subset).await
            })
            .await
            .unwrap_or_else(|_| Err(Error::Timeout(Duration::from_secs(30))));
            let fetched = match fetched {
                Ok(fetched) if fetched.len() == subset.len() => fetched,
                Ok(_) => {
                    last_error = Some(Error::Verification(
                        "log batch returned the wrong number of ticks".into(),
                    ));
                    continue;
                }
                Err(error) => {
                    if preferred == Some(addr) {
                        self.clear_log_peer();
                    }
                    last_error = Some(error);
                    continue;
                }
            };
            let mut supplied = false;
            let mut supplied_logs = false;
            for (index, fetch) in remaining.iter().copied().zip(fetched) {
                if fetch.complete {
                    supplied = true;
                    supplied_logs |= fetch.ranges.count() > 0;
                }
                if fetch.complete || results[index].is_none() {
                    results[index] = Some(fetch);
                }
            }
            if supplied {
                self.manager.record_log_success(addr);
                *self.log_peer.lock().expect("log peer mutex poisoned") = Some(addr);
            }
            if supplied_logs {
                tracing::debug!(
                    peer = %addr,
                    ticks = remaining.len(),
                    "peer supplied verified logs for a repair batch"
                );
            }
            remaining.retain(|index| results[*index].as_ref().is_none_or(|fetch| !fetch.complete));
        }
        if !remaining.is_empty() {
            if let Some(error) = last_error {
                tracing::debug!(
                    ticks = remaining.len(),
                    %error,
                    "log repair exhausted peers"
                );
            }
        }
        Ok(results
            .into_iter()
            .map(|fetch| {
                fetch.unwrap_or_else(|| {
                    FetchedLogs::incomplete("no peer supplied verified log ranges")
                })
            })
            .collect())
    }

    fn clear_log_peer(&self) {
        *self.log_peer.lock().expect("log peer mutex poisoned") = None;
    }
}

fn unique_votes(votes: &[Tick]) -> usize {
    let mut seen = [false; COMPUTOR_COUNT];
    for vote in votes {
        if let Some(slot) = seen.get_mut(vote.computor_index as usize) {
            *slot = true;
        }
    }
    seen.iter().filter(|seen| **seen).count()
}

/// A peer connection that releases its slot in the pool's per-peer load
/// counter when dropped.
pub struct PooledPeer {
    peer: Peer,
    addr: SocketAddr,
    load: std::sync::Arc<std::sync::Mutex<HashMap<SocketAddr, usize>>>,
    slot_available: std::sync::Arc<tokio::sync::Notify>,
}

impl PooledPeer {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl std::ops::Deref for PooledPeer {
    type Target = Peer;

    fn deref(&self) -> &Self::Target {
        &self.peer
    }
}

impl std::ops::DerefMut for PooledPeer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.peer
    }
}

impl Drop for PooledPeer {
    fn drop(&mut self) {
        if let Ok(mut loads) = self.load.lock() {
            if let Some(entry) = loads.get_mut(&self.addr) {
                *entry = entry.saturating_sub(1);
            }
        }
        self.slot_available.notify_waiters();
    }
}

fn random_dejavu() -> u32 {
    let mut value = rand::random::<u32>();
    if value == 0 {
        value = 1;
    }
    value
}

/// Mirrors `Transaction::checkValidity` in Qubic core.
fn validate_transaction(transaction: &Transaction) -> Result<()> {
    if transaction.amount < 0 || transaction.amount > MAX_AMOUNT {
        return Err(Error::Verification(format!(
            "transaction amount {} is outside 0..={MAX_AMOUNT}",
            transaction.amount
        )));
    }
    if transaction.input.len() > MAX_INPUT_SIZE {
        return Err(Error::Verification(format!(
            "transaction input is {} bytes, limit is {MAX_INPUT_SIZE}",
            transaction.input.len()
        )));
    }
    Ok(())
}

/// Full local validation for a transaction that is about to be broadcast.
fn validate_signed_transaction(transaction: &Transaction) -> Result<()> {
    validate_transaction(transaction)?;
    let bytes = transaction.encode();
    let signable_len = Transaction::BASE_SIZE + transaction.input.len();
    if !schnorrq::verify(
        &transaction.source,
        &bytes[..signable_len],
        &transaction.signature,
    ) {
        return Err(Error::Verification(
            "transaction signature does not verify against its source public key".into(),
        ));
    }
    Ok(())
}

/// `BroadcastComputors` sometimes carries four trailing padding bytes.
fn decode_computors(payload: &[u8]) -> Result<Computors> {
    if payload.len() < qubic::messages::COMPUTORS_SIZE {
        return Err(qubic::Error::InvalidPacket(format!(
            "computors need {} bytes, got {}",
            qubic::messages::COMPUTORS_SIZE,
            payload.len()
        ))
        .into());
    }
    Ok(Computors::decode(
        &payload[..qubic::messages::COMPUTORS_SIZE],
    )?)
}

fn decode_votes(packets: Vec<Packet>, tick: u32) -> Result<Vec<Tick>> {
    let mut votes = Vec::with_capacity(packets.len());
    for packet in packets {
        let vote = Tick::decode(&packet.payload)?;
        if vote.tick == tick {
            votes.push(vote);
        }
    }
    Ok(votes)
}

fn decode_transactions(packets: Vec<Packet>, tick: u32) -> Result<Vec<Transaction>> {
    let mut transactions = Vec::with_capacity(packets.len());
    for packet in packets {
        let transaction = Transaction::decode(&packet.payload)?;
        // The node may pad empty slots with zeroed transactions.
        if transaction.signature == [0u8; 64] || transaction.tick != tick {
            continue;
        }
        transactions.push(transaction);
    }
    Ok(transactions)
}

/// Every slot that can own a log range for this tick: non-empty transaction
/// slots plus Qubic's six special-event slots.
fn log_probe_indices(tick_data: &TickData) -> Vec<usize> {
    let mut indices = tick_data
        .non_empty_transactions()
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    indices.extend(TX_PER_TICK..qubic::messages::LOG_TX_PER_TICK);
    indices.sort_unstable();
    indices.dedup();
    indices
}

/// The quorum's own tick is the tick header. Fall back to the tick-data header
/// when the peer returned no votes so the bundle still describes the tick.
fn first_vote_or_synthetic(votes: &[Tick], data: &TickData, tick: u32) -> Result<Tick> {
    if let Some(vote) = votes.first() {
        return Ok(vote.clone());
    }
    Ok(Tick {
        computor_index: data.computor_index,
        epoch: data.epoch,
        tick,
        millisecond: data.millisecond,
        second: data.second,
        minute: data.minute,
        hour: data.hour,
        day: data.day,
        month: data.month,
        year: data.year,
        prev_resource_testing_digest: 0,
        salted_resource_testing_digest: 0,
        prev_transaction_body_digest: 0,
        salted_transaction_body_digest: 0,
        prev_spectrum_digest: [0; 32],
        prev_universe_digest: [0; 32],
        prev_computer_digest: [0; 32],
        salted_spectrum_digest: [0; 32],
        salted_universe_digest: [0; 32],
        salted_computer_digest: [0; 32],
        transaction_digest: [0; 32],
        expected_next_tick_transaction_digest: [0; 32],
        signature: [0; 64],
    })
}

/// Synthesizes an empty tick only when a full aligned quorum of empty votes
/// exists. A smaller empty group is not enough: Qubic finalizes an empty tick
/// with the same 451-vote quorum as a non-empty tick, and a partial vote set
/// must never be mistaken for an empty tick.
fn empty_tick_from_votes(votes: &[Tick], tick: u32) -> Result<TickData> {
    let mut groups = HashMap::<TickAlignment, Vec<&Tick>>::new();
    for vote in votes {
        if vote.tick != tick || !vote.is_empty_vote() {
            continue;
        }
        groups.entry(vote.alignment()).or_default().push(vote);
    }

    let mut winners = Vec::new();
    for group in groups.values() {
        let unique = group
            .iter()
            .map(|vote| vote.computor_index)
            .collect::<std::collections::HashSet<_>>()
            .len();
        if unique >= QUORUM {
            winners.push((unique, group));
        }
    }
    if winners.is_empty() {
        return Err(Error::MissingNonEmptyTick { tick });
    }
    // A split empty vote set is a conflicting quorum, not an empty tick.
    if winners.len() > 1 {
        return Err(Error::Verification(format!(
            "tick {tick} has {} conflicting empty quorum groups",
            winners.len()
        )));
    }
    let (_, group) = winners.pop().expect("one winner");
    let vote = group
        .iter()
        .find(|vote| u32::from(vote.computor_index) == tick % COMPUTOR_COUNT as u32)
        .copied()
        .unwrap_or(group[0]);
    Ok(TickData::empty_for_tick(tick, vote.epoch, vote))
}

#[cfg(test)]
mod tests {
    use super::*;
    use qubic::messages::{
        ASSET_REQUEST_GET_SIBLINGS, ASSETS_DEPTH, AssetIssuanceRecord, AssetOwnershipRecord,
        AssetPossessionRecord, AssetRecord, AssetRecordKind, EntityRecord, LOG_TX_PER_TICK,
        MAX_CONTRACTS, PRICE_SERIES_MAX_POINTS, RESPOND_PRICE_SERIES_HEADER_SIZE,
        RESPOND_PRICE_SERIES_POINT_SIZE, RespondPriceSeries, RespondPriceSeriesPoint,
        SPECTRUM_DEPTH, TX_PER_TICK,
    };
    use qubic::wire::{Header, MessageType, Packet};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    const TICK: u32 = 999;

    async fn read_request(stream: &mut TcpStream) -> Option<(MessageType, u32, Vec<u8>)> {
        let mut header = [0u8; HEADER_SIZE];
        if stream.read_exact(&mut header).await.is_err() {
            return None;
        }
        let header = Header::decode(&header).ok()?;
        let mut payload = vec![0u8; header.size as usize - HEADER_SIZE];
        if stream.read_exact(&mut payload).await.is_err() {
            return None;
        }
        Some((header.kind, header.dejavu, payload))
    }

    async fn send(stream: &mut TcpStream, kind: MessageType, dejavu: u32, payload: Vec<u8>) {
        stream
            .write_all(&Packet::new(kind, dejavu, payload).encode())
            .await
            .unwrap();
        stream.flush().await.unwrap();
    }

    fn tick(computor_index: u16, tick: u32) -> Tick {
        Tick {
            computor_index,
            epoch: 240,
            tick,
            millisecond: 1,
            second: 2,
            minute: 3,
            hour: 4,
            day: 5,
            month: 6,
            year: 26,
            prev_resource_testing_digest: 0,
            salted_resource_testing_digest: 0,
            prev_transaction_body_digest: 0,
            salted_transaction_body_digest: 0,
            prev_spectrum_digest: [0; 32],
            prev_universe_digest: [0; 32],
            prev_computer_digest: [0; 32],
            salted_spectrum_digest: [0; 32],
            salted_universe_digest: [0; 32],
            salted_computer_digest: [0; 32],
            transaction_digest: [computor_index as u8; 32],
            expected_next_tick_transaction_digest: [0; 32],
            signature: [computor_index as u8; 64],
        }
    }

    fn tick_data(tick_number: u32) -> TickData {
        TickData {
            computor_index: 1,
            epoch: 240,
            tick: tick_number,
            millisecond: 1,
            second: 2,
            minute: 3,
            hour: 4,
            day: 5,
            month: 6,
            year: 26,
            timelock: [0; 32],
            transaction_digests: vec![[0u8; 32]; TX_PER_TICK],
            contract_fees: vec![0i64; MAX_CONTRACTS],
            signature: [0; 64],
        }
    }

    fn log_ranges_payload() -> Vec<u8> {
        let mut from = vec![-1i64; LOG_TX_PER_TICK];
        let mut length = vec![-1i64; LOG_TX_PER_TICK];
        from[0] = 5;
        length[0] = 2;
        let mut payload = Vec::with_capacity(from.len() * 16);
        for value in from.iter().chain(length.iter()) {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        payload
    }

    fn log_event(log_id: u64) -> Vec<u8> {
        let body = log_id.to_le_bytes();
        let mut out = Vec::new();
        out.extend_from_slice(&240u16.to_le_bytes());
        out.extend_from_slice(&TICK.to_le_bytes());
        let combo = (qubic::logs::CUSTOM_MESSAGE as u32) << 24 | body.len() as u32;
        out.extend_from_slice(&combo.to_le_bytes());
        out.extend_from_slice(&log_id.to_le_bytes());
        out.extend_from_slice(&0u64.to_le_bytes());
        out.extend_from_slice(&body);
        out
    }

    const PRICE_SERIES_FIXTURE: &[u8] = include_bytes!("../../../fixtures/price_series.bin");
    const ISSUED_ASSETS_FIXTURE: &[u8] = include_bytes!("../../../fixtures/issued_assets.bin");
    const ASSETS_SIBLINGS_FIXTURE: &[u8] = include_bytes!("../../../fixtures/assets_issuance.bin");

    fn fixture_payload(bytes: &[u8]) -> Vec<u8> {
        Packet::decode(bytes).unwrap().payload
    }

    async fn fake_peer(listener: TcpListener) {
        let (mut stream, _) = listener.accept().await.unwrap();
        send(
            &mut stream,
            MessageType::ExchangePublicPeers,
            77,
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        )
        .await;

        let mut tried_again = false;
        while let Some((kind, dejavu, payload)) = read_request(&mut stream).await {
            match kind {
                MessageType::ExchangePublicPeers => {}
                MessageType::RequestCurrentTickInfo => {
                    if !tried_again {
                        tried_again = true;
                        send(&mut stream, MessageType::TryAgain, dejavu, Vec::new()).await;
                        continue;
                    }
                    // Unsolicited gossip must be ignored by the client.
                    send(
                        &mut stream,
                        MessageType::BroadcastTick,
                        0xdead_beef,
                        tick(5, 123).encode().to_vec(),
                    )
                    .await;
                    let mut info = Vec::new();
                    info.extend_from_slice(&1000u16.to_le_bytes());
                    info.extend_from_slice(&240u16.to_le_bytes());
                    info.extend_from_slice(&1000u32.to_le_bytes());
                    info.extend_from_slice(&451u16.to_le_bytes());
                    info.extend_from_slice(&0u16.to_le_bytes());
                    info.extend_from_slice(&900u32.to_le_bytes());
                    send(
                        &mut stream,
                        MessageType::RespondCurrentTickInfo,
                        dejavu,
                        info,
                    )
                    .await;
                }
                MessageType::RequestComputors => {
                    let computors = Computors {
                        epoch: 240,
                        public_keys: vec![[0u8; 32]; COMPUTOR_COUNT],
                        signature: [0; 64],
                    };
                    send(
                        &mut stream,
                        MessageType::BroadcastComputors,
                        dejavu,
                        computors.encode(),
                    )
                    .await;
                }
                MessageType::RequestTickData => {
                    let requested = u32::from_le_bytes(payload[..4].try_into().unwrap());
                    send(
                        &mut stream,
                        MessageType::BroadcastFutureTickData,
                        dejavu,
                        tick_data(requested).encode(),
                    )
                    .await;
                }
                MessageType::RequestQuorumTick => {
                    let requested = u32::from_le_bytes(payload[..4].try_into().unwrap());
                    for index in [7u16, 9] {
                        send(
                            &mut stream,
                            MessageType::BroadcastTick,
                            dejavu,
                            tick(index, requested).encode().to_vec(),
                        )
                        .await;
                    }
                    send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                }
                MessageType::RequestTickTransactions => {
                    let requested = u32::from_le_bytes(payload[..4].try_into().unwrap());
                    let transaction = Transaction {
                        source: [1; 32],
                        destination: [2; 32],
                        amount: 123,
                        tick: requested,
                        input_type: 0,
                        input: Vec::new(),
                        signature: [3; 64],
                    };
                    send(
                        &mut stream,
                        MessageType::BroadcastTransaction,
                        dejavu,
                        transaction.encode(),
                    )
                    .await;
                    send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                }
                MessageType::RequestEntity => {
                    let public_key: [u8; 32] = payload.try_into().unwrap();
                    let entity = qubic::messages::EntityRecord {
                        public_key,
                        incoming_amount: 500,
                        outgoing_amount: 250,
                        number_of_incoming_transfers: 3,
                        number_of_outgoing_transfers: 2,
                        latest_incoming_transfer_tick: TICK,
                        latest_outgoing_transfer_tick: TICK - 1,
                    };
                    let response = RespondEntity {
                        entity,
                        tick: 1000,
                        spectrum_index: 42,
                        siblings: vec![[9u8; 32]; qubic::messages::SPECTRUM_DEPTH],
                    };
                    send(
                        &mut stream,
                        MessageType::RespondEntity,
                        dejavu,
                        response.encode(),
                    )
                    .await;
                }
                MessageType::RequestContractFunction => {
                    let contract_index = u32::from_le_bytes(payload[0..4].try_into().unwrap());
                    let input_type = u16::from_le_bytes(payload[4..6].try_into().unwrap());
                    let output = if contract_index == 1 && input_type == 1 {
                        vec![0xde, 0xad, 0xbe, 0xef]
                    } else {
                        Vec::new()
                    };
                    send(
                        &mut stream,
                        MessageType::RespondContractFunction,
                        dejavu,
                        output,
                    )
                    .await;
                }
                MessageType::RequestTransactionInfo => {
                    let transaction = Transaction {
                        source: [4; 32],
                        destination: [5; 32],
                        amount: 77,
                        tick: TICK,
                        input_type: 0,
                        input: Vec::new(),
                        signature: [6; 64],
                    };
                    if payload == transaction.hash() {
                        send(
                            &mut stream,
                            MessageType::BroadcastTransaction,
                            dejavu,
                            transaction.encode(),
                        )
                        .await;
                    } else {
                        send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                    }
                }
                MessageType::RequestTxStatus => {
                    let requested = u32::from_le_bytes(payload[0..4].try_into().unwrap());
                    let status = RespondTxStatus {
                        current_tick: 1000,
                        tick: requested,
                        digests: vec![[0x11; 32], [0x22; 32]],
                        money_flew: vec![true, false],
                    };
                    send(
                        &mut stream,
                        MessageType::RespondTxStatus,
                        dejavu,
                        status.encode(),
                    )
                    .await;
                }
                MessageType::RequestAllLogIdRangesFromTx => {
                    send(
                        &mut stream,
                        MessageType::RespondAllLogIdRangesFromTx,
                        dejavu,
                        log_ranges_payload(),
                    )
                    .await;
                }
                MessageType::RequestLog => {
                    let from = u64::from_le_bytes(payload[32..40].try_into().unwrap());
                    let to = u64::from_le_bytes(payload[40..48].try_into().unwrap());
                    if from <= to {
                        // Deliberately answer one log per request to exercise paging.
                        send(
                            &mut stream,
                            MessageType::RespondLog,
                            dejavu,
                            log_event(from),
                        )
                        .await;
                    } else {
                        send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                    }
                }
                MessageType::RequestPriceSeries => {
                    send(
                        &mut stream,
                        MessageType::RespondPriceSeries,
                        dejavu,
                        fixture_payload(PRICE_SERIES_FIXTURE),
                    )
                    .await;
                }
                MessageType::RequestIssuedAssets => {
                    send(
                        &mut stream,
                        MessageType::RespondIssuedAssets,
                        dejavu,
                        fixture_payload(ISSUED_ASSETS_FIXTURE),
                    )
                    .await;
                    send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                }
                MessageType::RequestAssets => {
                    send(
                        &mut stream,
                        MessageType::RespondAssets,
                        dejavu,
                        fixture_payload(ASSETS_SIBLINGS_FIXTURE),
                    )
                    .await;
                    send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                }
                MessageType::RequestOracleData => {
                    let request = qubic::messages::RequestOracleData::decode(&payload).unwrap();
                    match request.req_type {
                        qubic::messages::ORACLE_REQUEST_ACTIVE_SUBSCRIPTIONS => {
                            let mut response = qubic::messages::RespondOracleData {
                                res_type: qubic::messages::ORACLE_RESPOND_SUBSCRIPTION_IDS,
                            }
                            .encode()
                            .to_vec();
                            response.extend_from_slice(&5i32.to_le_bytes());
                            response.extend_from_slice(&6i32.to_le_bytes());
                            send(
                                &mut stream,
                                MessageType::RespondOracleData,
                                dejavu,
                                response,
                            )
                            .await;
                        }
                        qubic::messages::ORACLE_REQUEST_SUBSCRIPTION => {
                            assert_eq!(request.tick_or_id, 5);
                            let subscription = qubic::messages::OracleSubscription {
                                subscription_id: 5,
                                interface_index: qubic::messages::PRICE_ORACLE_INTERFACE_INDEX,
                                last_pending_query_id: 800,
                                last_revealed_query_id: 777,
                                generated_queries_count: 3,
                            };
                            let mut response = qubic::messages::RespondOracleData {
                                res_type: qubic::messages::ORACLE_RESPOND_SUBSCRIPTION,
                            }
                            .encode()
                            .to_vec();
                            response.extend_from_slice(&subscription.encode());
                            send(
                                &mut stream,
                                MessageType::RespondOracleData,
                                dejavu,
                                response,
                            )
                            .await;

                            let query = price_query();
                            let mut response = qubic::messages::RespondOracleData {
                                res_type: qubic::messages::ORACLE_RESPOND_QUERY_DATA,
                            }
                            .encode()
                            .to_vec();
                            response.extend_from_slice(&query.encode());
                            send(
                                &mut stream,
                                MessageType::RespondOracleData,
                                dejavu,
                                response,
                            )
                            .await;

                            let mut response = qubic::messages::RespondOracleData {
                                res_type: qubic::messages::ORACLE_RESPOND_SUBSCRIBER,
                            }
                            .encode()
                            .to_vec();
                            response.extend_from_slice(&[0u8; 16]);
                            send(
                                &mut stream,
                                MessageType::RespondOracleData,
                                dejavu,
                                response,
                            )
                            .await;
                        }
                        qubic::messages::ORACLE_REQUEST_QUERY_AND_RESPONSE => {
                            assert_eq!(request.tick_or_id, 777);
                            let metadata = qubic::messages::OracleQueryMetadata {
                                query_id: 777,
                                query_type: qubic::logs::ORACLE_QUERY_TYPE_CONTRACT_SUBSCRIPTION,
                                status: qubic::logs::ORACLE_QUERY_STATUS_SUCCESS,
                                status_flags: 0,
                                query_tick: 91_915_000,
                                querying_entity: [0; 32],
                                timeout: 0,
                                interface_index: qubic::messages::PRICE_ORACLE_INTERFACE_INDEX,
                                subscription_id: 5,
                                reveal_tick: 91_915_004,
                                total_commits: 0,
                                agreeing_commits: 0,
                            };
                            let mut response = qubic::messages::RespondOracleData {
                                res_type: qubic::messages::ORACLE_RESPOND_QUERY_METADATA,
                            }
                            .encode()
                            .to_vec();
                            response.extend_from_slice(&metadata.encode());
                            send(
                                &mut stream,
                                MessageType::RespondOracleData,
                                dejavu,
                                response,
                            )
                            .await;

                            let mut response = qubic::messages::RespondOracleData {
                                res_type: qubic::messages::ORACLE_RESPOND_QUERY_DATA,
                            }
                            .encode()
                            .to_vec();
                            response.extend_from_slice(&price_query().encode());
                            send(
                                &mut stream,
                                MessageType::RespondOracleData,
                                dejavu,
                                response,
                            )
                            .await;

                            let reply = qubic::messages::PriceOracleReply {
                                numerator: 3,
                                denominator: 2,
                            };
                            let mut response = qubic::messages::RespondOracleData {
                                res_type: qubic::messages::ORACLE_RESPOND_REPLY_DATA,
                            }
                            .encode()
                            .to_vec();
                            response.extend_from_slice(&reply.encode());
                            send(
                                &mut stream,
                                MessageType::RespondOracleData,
                                dejavu,
                                response,
                            )
                            .await;
                        }
                        other => panic!("unexpected oracle request type {other}"),
                    }
                    send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                }
                _ => {}
            }
        }
    }

    fn price_query() -> qubic::messages::PriceOracleQuery {
        qubic::messages::PriceOracleQuery {
            oracle: qubic::messages::currency_id("binance_mexc"),
            timestamp: 1_700_000_000_000_000,
            currency1: qubic::messages::currency_id("QUBIC"),
            currency2: qubic::messages::currency_id("USDT"),
        }
    }

    async fn peer_and_server() -> Peer {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(fake_peer(listener));
        Peer::connect(
            PeerConfig::new(addr)
                .with_timeout(Duration::from_secs(2))
                .with_connect_timeout(Duration::from_secs(2)),
        )
        .await
        .unwrap()
    }

    /// Serves a handshake and one scripted head per accepted connection.
    ///
    /// Repeated `RequestCurrentTickInfo` calls walk `heads`, which lets tests
    /// model a peer that reports a backwards or post-rollover head on the same
    /// connection.
    async fn spawn_scripted_peer(heads: Vec<(u16, u32)>, advertised: Vec<[u8; 4]>) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let heads = Arc::new(heads);
        let advertised = Arc::new(advertised);
        let cursor = Arc::new(AtomicUsize::new(0));
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let heads = heads.clone();
                let advertised = advertised.clone();
                let cursor = cursor.clone();
                tokio::spawn(async move {
                    let mut payload = Vec::with_capacity(advertised.len() * 4);
                    for ip in advertised.iter() {
                        payload.extend_from_slice(ip);
                    }
                    send(&mut stream, MessageType::ExchangePublicPeers, 77, payload).await;
                    while let Some((kind, dejavu, _)) = read_request(&mut stream).await {
                        match kind {
                            MessageType::RequestCurrentTickInfo => {
                                let index = cursor
                                    .fetch_add(1, Ordering::SeqCst)
                                    .min(heads.len().saturating_sub(1));
                                let (epoch, tick) =
                                    heads.get(index).copied().unwrap_or((240, 1_000));
                                let info = CurrentTickInfo {
                                    tick_duration: 1_000,
                                    epoch,
                                    tick,
                                    aligned_votes: 451,
                                    misaligned_votes: 0,
                                    initial_tick: tick.saturating_sub(100),
                                };
                                send(
                                    &mut stream,
                                    MessageType::RespondCurrentTickInfo,
                                    dejavu,
                                    info.encode().to_vec(),
                                )
                                .await;
                            }
                            MessageType::RequestComputors => {
                                let computors = Computors {
                                    epoch: 240,
                                    public_keys: vec![[0u8; 32]; COMPUTOR_COUNT],
                                    signature: [0; 64],
                                };
                                send(
                                    &mut stream,
                                    MessageType::BroadcastComputors,
                                    dejavu,
                                    computors.encode(),
                                )
                                .await;
                            }
                            _ => {}
                        }
                    }
                });
            }
        });
        addr
    }

    /// Accepts TCP connections and drops them before the handshake completes.
    async fn spawn_rejecting_peer() -> (SocketAddr, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = attempts.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                counter.fetch_add(1, Ordering::SeqCst);
                drop(stream);
            }
        });
        (addr, attempts)
    }

    fn fast_health_policy() -> PeerPolicy {
        PeerPolicy {
            quarantine_after: 3,
            quarantine_base: Duration::from_millis(40),
            quarantine_max: Duration::from_millis(160),
            failure_penalty: Duration::from_millis(10),
            exploration_bonus: Duration::from_millis(1),
            exploration_window: Duration::from_millis(20),
            unknown_latency: Duration::from_millis(100),
            latency_alpha: 0.5,
        }
    }

    fn quick_peer(addr: SocketAddr) -> PeerConfig {
        PeerConfig::new(addr)
            .with_timeout(Duration::from_secs(1))
            .with_connect_timeout(Duration::from_secs(1))
            .with_retries(0)
    }

    #[tokio::test]
    async fn pool_quarantines_failing_peer_and_recovers_after_cooldown() {
        let (bad, attempts) = spawn_rejecting_peer().await;
        let good = spawn_scripted_peer(vec![(240, 1_000)], Vec::new()).await;
        let mut pool = NodePool::with_policy(vec![quick_peer(bad)], fast_health_policy());

        for _ in 0..3 {
            assert!(pool.connect_best().await.is_err());
        }
        assert!(pool.manager.is_quarantined(bad));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);

        // A healthy peer is now available, but the failing one must not be
        // contacted while quarantined.
        pool.peers.push(quick_peer(good));
        let peer = pool.connect_best().await.unwrap();
        assert_eq!(peer.config().addr, good);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);

        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(!pool.manager.is_quarantined(bad));
        assert!(
            pool.manager
                .attempt_order(&pool.peers)
                .iter()
                .any(|config| config.addr == bad)
        );
    }

    #[tokio::test]
    async fn pool_orders_by_latency_and_shares_health_across_clones() {
        let slow = spawn_scripted_peer(vec![(240, 1_000)], Vec::new()).await;
        let fast = spawn_scripted_peer(vec![(240, 1_000)], Vec::new()).await;
        let pool = NodePool::new(vec![quick_peer(slow), quick_peer(fast)]);
        pool.manager.record_success(slow, Duration::from_millis(80));
        pool.manager.record_success(fast, Duration::from_millis(5));

        let peer = pool.connect_best().await.unwrap();
        assert_eq!(peer.config().addr, fast);

        let clone = pool.clone();
        clone.manager.record_failure(slow);
        let health = pool.health();
        let slow_health = health
            .iter()
            .find(|entry| entry.addr == slow)
            .expect("slow peer health");
        assert_eq!(slow_health.failures, 1);
        assert!(slow_health.last_latency_ms.is_some());
    }

    #[tokio::test]
    async fn pool_discovery_dedups_and_caps() {
        let seed = spawn_scripted_peer(
            vec![(240, 1_000)],
            vec![[127, 0, 0, 2], [127, 0, 0, 2], [127, 0, 0, 3]],
        )
        .await;
        let mut pool = NodePool::new(vec![quick_peer(seed)]);

        let added = pool.discover_with_limit(2).await.unwrap();
        assert_eq!(added, [PeerAddress::new([127, 0, 0, 2], 21_841)]);
        assert_eq!(pool.peer_configs().len(), 2);
        assert_eq!(pool.health().len(), 2);
    }

    #[tokio::test]
    async fn pool_caps_connections_per_peer_and_wakes_waiters() {
        let first = spawn_scripted_peer(vec![(240, 1_000)], Vec::new()).await;
        let second = spawn_scripted_peer(vec![(240, 1_000)], Vec::new()).await;
        let pool = NodePool::with_policy(
            vec![quick_peer(first), quick_peer(second)],
            fast_health_policy(),
        )
        .with_connection_limit(1);

        let held = pool.connect_balanced().await.expect("first slot");
        let other = pool.connect_balanced().await.expect("second peer slot");
        assert_ne!(held.addr(), other.addr());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), pool.connect_balanced())
                .await
                .is_err(),
            "a third connection must wait for a released slot"
        );

        drop(held);
        let reconnected = tokio::time::timeout(Duration::from_secs(1), pool.connect_balanced())
            .await
            .expect("waiter wakes")
            .expect("released slot reconnects");
        assert_eq!(reconnected.addr(), first);
    }

    #[tokio::test]
    async fn peer_rejects_backwards_and_stale_heads() {
        let addr = spawn_scripted_peer(vec![(240, 1_000), (240, 900)], Vec::new()).await;
        let mut peer = Peer::connect(quick_peer(addr)).await.unwrap();
        assert_eq!(peer.head().await.unwrap().tick, 1_000);
        assert!(matches!(
            peer.head().await,
            Err(Error::NotSynchronized {
                head: 900,
                requested: 1_000
            })
        ));

        // A new epoch may legitimately restart at a lower tick.
        let rollover = spawn_scripted_peer(vec![(240, 1_000), (241, 10)], Vec::new()).await;
        let mut peer = Peer::connect(quick_peer(rollover)).await.unwrap();
        assert_eq!(peer.head().await.unwrap().epoch, 240);
        let head = peer.head().await.unwrap();
        assert_eq!((head.epoch, head.tick), (241, 10));

        // A worker holding the previous epoch's head must refresh.
        let addr = spawn_scripted_peer(vec![(241, 50)], Vec::new()).await;
        let mut peer = Peer::connect(quick_peer(addr)).await.unwrap();
        peer.head().await.unwrap();
        let cached = Head {
            epoch: 240,
            tick: 9_000,
            initial_tick: 0,
            tick_duration: 1_000,
        };
        let computors = Computors {
            epoch: 240,
            public_keys: vec![[0u8; 32]; COMPUTOR_COUNT],
            signature: [0; 64],
        };
        assert!(matches!(
            peer.bundle_with(9_000, cached, computors).await,
            Err(Error::NotSynchronized {
                head: 50,
                requested: 9_000
            })
        ));
    }

    #[tokio::test]
    async fn computors_for_rejects_wrong_epoch() {
        let mut peer = peer_and_server().await;
        assert_eq!(peer.computors_for(240).await.unwrap().epoch, 240);
        assert!(matches!(
            peer.computors_for(241).await,
            Err(Error::NotSynchronized {
                head: 240,
                requested: 241
            })
        ));
    }

    #[tokio::test]
    async fn handshake_learns_public_peers() {
        let peer = peer_and_server().await;
        let peers: Vec<String> = peer
            .public_peers()
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            peers,
            [
                "1.2.3.4:21841",
                "5.6.7.8:21841",
                "9.10.11.12:21841",
                "13.14.15.16:21841"
            ]
        );
    }

    #[tokio::test]
    async fn oracle_requests_parse_streamed_packets() {
        let mut peer = peer_and_server().await;
        assert_eq!(peer.oracle_subscription_ids().await.unwrap(), vec![5, 6]);

        let subscription = peer.oracle_subscription(5).await.unwrap();
        assert_eq!(subscription.subscription.subscription_id, 5);
        assert_eq!(
            subscription.subscription.interface_index,
            qubic::messages::PRICE_ORACLE_INTERFACE_INDEX
        );
        assert_eq!(subscription.subscription.last_revealed_query_id, 777);
        let query = qubic::messages::PriceOracleQuery::decode(&subscription.query).unwrap();
        assert!(query.is_pair(
            &qubic::messages::currency_id("QUBIC"),
            &qubic::messages::currency_id("USDT")
        ));

        let response = peer.oracle_query(777).await.unwrap();
        assert_eq!(response.metadata.query_id, 777);
        assert_eq!(
            response.metadata.status,
            qubic::logs::ORACLE_QUERY_STATUS_SUCCESS
        );
        assert_eq!(response.metadata.query_tick, 91_915_000);
        assert_eq!(response.metadata.reveal_tick, 91_915_004);
        let reply =
            qubic::messages::PriceOracleReply::decode(response.reply.as_deref().unwrap()).unwrap();
        assert_eq!(reply.numerator, 3);
        assert_eq!(reply.denominator, 2);
    }

    #[tokio::test]
    async fn head_retries_try_again_and_skips_gossip() {
        let mut peer = peer_and_server().await;
        let head = peer.head().await.unwrap();
        assert_eq!(head.tick, 1000);
        assert_eq!(head.epoch, 240);
        assert_eq!(head.initial_tick, 900);
        assert_eq!(head.tick_duration, 1000);
    }

    #[tokio::test]
    async fn price_series_and_asset_requests_use_fixtures() {
        let mut peer = peer_and_server().await;

        let series = peer.price_series(40_300, 1000).await.unwrap();
        assert_eq!(series.count, 20);
        assert_eq!(series.points.len(), 20);
        assert_eq!(series.points[0].hour, 40_300);
        assert_eq!(series.base_hour, 40_316);
        assert!(series.synced);

        let issued = peer.issued_assets([0; 32]).await.unwrap();
        assert_eq!(issued.len(), 1);
        assert_eq!(issued[0].siblings.len(), ASSETS_DEPTH);
        assert!(matches!(
            issued[0].asset.kind,
            AssetRecordKind::Issuance(_) | AssetRecordKind::Empty
        ));

        let request = RequestAssets::ByUniverseIndex {
            flags: ASSET_REQUEST_GET_SIBLINGS,
            universe_index: 0,
        };
        let records = peer.asset_records(&request).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].universe_index(), 0);
        assert_eq!(records[0].siblings().unwrap().len(), ASSETS_DEPTH);
    }

    #[tokio::test]
    async fn bundle_collects_streams_and_partial_logs() {
        let mut peer = peer_and_server().await;
        let bundle = peer.bundle(TICK).await.unwrap();
        assert_eq!(bundle.head.tick, 1000);
        assert_eq!(bundle.tick.tick, TICK);
        assert_eq!(bundle.tick_data.tick, TICK);
        assert_eq!(bundle.votes.len(), 2);
        assert_eq!(bundle.transactions.len(), 1);
        assert_eq!(bundle.log_ranges.range(), Some((5, 6)));
        assert_eq!(
            bundle
                .logs
                .iter()
                .map(|event| event.log_id)
                .collect::<Vec<_>>(),
            [5, 6]
        );
    }

    #[tokio::test]
    async fn prefetch_bundles_pipelines_ticks_and_logs() {
        let mut peer = peer_and_server().await;
        let head = peer.head().await.unwrap();
        let computors = peer.computors().await.unwrap();
        let bundles = peer
            .prefetch_bundles(&[TICK, TICK + 1], head, computors, None)
            .await
            .unwrap();

        assert_eq!(bundles.len(), 2);
        for (offset, bundle) in bundles.into_iter().enumerate() {
            let bundle = bundle.unwrap();
            assert_eq!(bundle.tick.tick, TICK + offset as u32);
            assert_eq!(bundle.tick_data.tick, TICK + offset as u32);
            assert_eq!(bundle.votes.len(), 2);
            assert_eq!(bundle.transactions.len(), 1);
            assert_eq!(bundle.transactions[0].tick, TICK + offset as u32);
            assert_eq!(bundle.log_ranges.range(), Some((5, 6)));
            assert_eq!(
                bundle
                    .logs
                    .iter()
                    .map(|event| event.log_id)
                    .collect::<Vec<_>>(),
                [5, 6]
            );
        }
    }

    #[test]
    fn tx_status_and_request_layouts_are_fixed() {
        assert_eq!(request_tx_status(0x0102_0304), vec![0x04, 0x03, 0x02, 0x01]);
        assert_eq!(
            request_entity([0x5a; 32]).len(),
            qubic::messages::REQUEST_ENTITY_SIZE
        );
        assert_eq!(
            request_contract_function(1, 2, &[0xaa, 0xbb]),
            vec![1, 0, 0, 0, 2, 0, 2, 0, 0xaa, 0xbb]
        );

        let mut payload = Vec::new();
        payload.extend_from_slice(&1000u32.to_le_bytes());
        payload.extend_from_slice(&999u32.to_le_bytes());
        payload.extend_from_slice(&2u32.to_le_bytes());
        let mut bitmap = vec![0u8; qubic::messages::TX_PER_TICK.div_ceil(8)];
        // First transaction moved money; the second did not.
        bitmap[0] = 0b0000_0001;
        payload.extend_from_slice(&bitmap);
        payload.extend_from_slice(&[0xab; 32]);
        payload.extend_from_slice(&[0xcd; 32]);

        let status = RespondTxStatus::decode(&payload).unwrap();
        assert_eq!(status.current_tick, 1000);
        assert_eq!(status.tick, 999);
        assert_eq!(status.digests.len(), 2);
        assert_eq!(status.digests, [[0xab; 32], [0xcd; 32]]);
        assert_eq!(status.money_flew, [true, false]);
        assert_eq!(status.encode(), payload);
        assert_eq!(
            payload.len(),
            qubic::messages::RESPOND_TX_STATUS_HEADER_SIZE + 64
        );

        assert!(RespondTxStatus::decode(&payload[..payload.len() - 1]).is_err());
    }

    /// Peer that rejects the all-ranges request (type 50). When `fallback` is
    /// set it answers per-transaction log ranges (type 48), otherwise it has no
    /// logging at all.
    async fn fake_peer_optional_logs(
        listener: TcpListener,
        all_ranges: bool,
        tx_ranges: bool,
        requests: Option<tokio::sync::mpsc::UnboundedSender<MessageType>>,
    ) {
        let (mut stream, _) = listener.accept().await.unwrap();
        send(
            &mut stream,
            MessageType::ExchangePublicPeers,
            77,
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        )
        .await;

        while let Some((kind, dejavu, payload)) = read_request(&mut stream).await {
            if let Some(requests) = &requests {
                let _ = requests.send(kind);
            }
            match kind {
                MessageType::ExchangePublicPeers => {}
                MessageType::RequestCurrentTickInfo => {
                    let mut info = Vec::new();
                    info.extend_from_slice(&1000u16.to_le_bytes());
                    info.extend_from_slice(&240u16.to_le_bytes());
                    info.extend_from_slice(&1000u32.to_le_bytes());
                    info.extend_from_slice(&451u16.to_le_bytes());
                    info.extend_from_slice(&0u16.to_le_bytes());
                    info.extend_from_slice(&900u32.to_le_bytes());
                    send(
                        &mut stream,
                        MessageType::RespondCurrentTickInfo,
                        dejavu,
                        info,
                    )
                    .await;
                }
                MessageType::RequestComputors => {
                    let computors = Computors {
                        epoch: 240,
                        public_keys: vec![[0u8; 32]; COMPUTOR_COUNT],
                        signature: [0; 64],
                    };
                    send(
                        &mut stream,
                        MessageType::BroadcastComputors,
                        dejavu,
                        computors.encode(),
                    )
                    .await;
                }
                MessageType::RequestTickData => {
                    let requested = u32::from_le_bytes(payload[..4].try_into().unwrap());
                    send(
                        &mut stream,
                        MessageType::BroadcastFutureTickData,
                        dejavu,
                        tick_data(requested).encode(),
                    )
                    .await;
                }
                MessageType::RequestQuorumTick => {
                    send(
                        &mut stream,
                        MessageType::BroadcastTick,
                        dejavu,
                        tick(0, TICK).encode().to_vec(),
                    )
                    .await;
                    send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                }
                MessageType::RequestTickTransactions => {
                    send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                }
                MessageType::RequestAllLogIdRangesFromTx => {
                    if all_ranges {
                        send(
                            &mut stream,
                            MessageType::RespondAllLogIdRangesFromTx,
                            dejavu,
                            log_ranges_payload(),
                        )
                        .await;
                    } else {
                        send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                    }
                }
                MessageType::RequestLogIdRangeFromTx => {
                    if tx_ranges {
                        let tx_id = u32::from_le_bytes(payload[36..40].try_into().unwrap());
                        let (from, length) = if tx_id == TX_PER_TICK as u32 {
                            (5i64, 2i64)
                        } else {
                            (-1i64, -1i64)
                        };
                        let mut range = Vec::with_capacity(16);
                        range.extend_from_slice(&from.to_le_bytes());
                        range.extend_from_slice(&length.to_le_bytes());
                        send(
                            &mut stream,
                            MessageType::RespondLogIdRangeFromTx,
                            dejavu,
                            range,
                        )
                        .await;
                    } else {
                        send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                    }
                }
                MessageType::RequestLog => {
                    let from = u64::from_le_bytes(payload[32..40].try_into().unwrap());
                    let to = u64::from_le_bytes(payload[40..48].try_into().unwrap());
                    if (all_ranges || tx_ranges) && from <= to {
                        let mut page = Vec::new();
                        for log_id in from..=to {
                            page.extend_from_slice(&log_event(log_id));
                        }
                        send(&mut stream, MessageType::RespondLog, dejavu, page).await;
                    } else {
                        send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                    }
                }
                _ => {}
            }
        }
    }

    async fn peer_with_optional_logs(fallback: bool) -> Peer {
        peer_with_optional_logs_counted(fallback).await.0
    }

    async fn peer_with_tx_logs() -> Peer {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(fake_peer_optional_logs(listener, false, true, None));
        Peer::connect(
            PeerConfig::new(addr)
                .with_timeout(Duration::from_secs(2))
                .with_connect_timeout(Duration::from_secs(2)),
        )
        .await
        .unwrap()
    }

    async fn peer_with_optional_logs_counted(
        fallback: bool,
    ) -> (Peer, tokio::sync::mpsc::UnboundedReceiver<MessageType>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (requests, received) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(fake_peer_optional_logs(
            listener,
            fallback,
            false,
            Some(requests),
        ));
        let peer = Peer::connect(
            PeerConfig::new(addr)
                .with_timeout(Duration::from_secs(2))
                .with_connect_timeout(Duration::from_secs(2)),
        )
        .await
        .unwrap();
        (peer, received)
    }

    #[tokio::test]
    async fn bundle_fetches_logs_from_verified_ranges() {
        let mut peer = peer_with_optional_logs(true).await;
        let bundle = peer.bundle(TICK).await.unwrap();
        assert_eq!(bundle.log_ranges.range(), Some((5, 6)));
        assert_eq!(
            bundle
                .logs
                .iter()
                .map(|event| event.log_id)
                .collect::<Vec<_>>(),
            [5, 6]
        );
        assert!(
            bundle.warnings.is_empty(),
            "warnings: {:?}",
            bundle.warnings
        );
    }

    #[tokio::test]
    async fn bundle_rejects_peer_without_verified_log_ranges() {
        let mut peer = peer_with_optional_logs(false).await;
        let bundle = peer.bundle(TICK).await.unwrap();
        assert_eq!(bundle.tick.tick, TICK);
        assert_eq!(bundle.votes.len(), 1);
        assert!(bundle.logs.is_empty());
        assert_eq!(bundle.log_ranges.count(), 0);
        assert!(!bundle.logs_complete);
        assert!(!bundle.warnings.is_empty());
    }

    #[tokio::test]
    async fn bundle_fetches_logs_from_per_transaction_ranges() {
        let mut peer = peer_with_tx_logs().await;
        let bundle = peer.bundle(TICK).await.unwrap();
        assert_eq!(bundle.log_ranges.range(), Some((5, 6)));
        assert_eq!(
            bundle
                .logs
                .iter()
                .map(|event| event.log_id)
                .collect::<Vec<_>>(),
            [5, 6]
        );
        assert!(bundle.logs_complete);
        assert!(bundle.warnings.is_empty(), "{:?}", bundle.warnings);
    }

    #[tokio::test]
    async fn log_ranges_reports_unavailable_with_clear_message() {
        let mut peer = peer_with_optional_logs(false).await;
        match peer.log_ranges(TICK).await {
            Err(Error::LogsUnavailable(message)) => {
                assert!(message.contains("no verified log ranges"));
            }
            other => panic!("expected LogsUnavailable, got {other:?}"),
        }
    }

    fn drain_requests(
        requests: &mut tokio::sync::mpsc::UnboundedReceiver<MessageType>,
    ) -> Vec<MessageType> {
        let mut seen = Vec::new();
        while let Ok(kind) = requests.try_recv() {
            seen.push(kind);
        }
        seen
    }

    #[tokio::test]
    async fn bundle_with_skips_head_and_computors() {
        let (mut peer, mut requests) = peer_with_optional_logs_counted(true).await;

        let bundle = peer.bundle(TICK).await.unwrap();
        let first = drain_requests(&mut requests);
        assert!(first.contains(&MessageType::RequestCurrentTickInfo));
        assert!(first.contains(&MessageType::RequestComputors));

        let head = bundle.head.clone();
        let computors = bundle.computors.clone();
        let reused = peer.bundle_with(TICK, head, computors).await.unwrap();
        assert_eq!(reused.tick.tick, TICK);
        assert_eq!(reused.log_ranges.range(), Some((5, 6)));

        let second = drain_requests(&mut requests);
        assert!(
            !second.contains(&MessageType::RequestCurrentTickInfo),
            "bundle_with must not fetch the head again: {second:?}"
        );
        assert!(
            !second.contains(&MessageType::RequestComputors),
            "bundle_with must not fetch computors again: {second:?}"
        );
        assert!(second.contains(&MessageType::RequestTickData));
    }

    #[tokio::test]
    #[ignore = "requires a reachable Qubic peer; run with --ignored"]
    async fn live_head_smoke() {
        let addr: SocketAddr = std::env::var("GLYPH_LIVE_PEER")
            .unwrap_or_else(|_| "157.180.10.49:21841".into())
            .parse()
            .expect("GLYPH_LIVE_PEER must be host:port");
        let pool = NodePool::new(vec![
            PeerConfig::new(addr).with_timeout(Duration::from_secs(10)),
        ]);
        let head = pool.head().await.expect("live head");
        assert!(head.epoch > 0, "epoch should be nonzero: {head:?}");
        assert!(head.tick > 0, "tick should be nonzero: {head:?}");
        assert!(head.initial_tick <= head.tick, "invalid head: {head:?}");
        eprintln!("live head: {head:?}");
    }

    #[tokio::test]
    #[ignore = "requires GLYPH_LIVE_TICK and a reachable Qubic peer"]
    async fn live_tick_smoke() {
        let Ok(tick_number) = std::env::var("GLYPH_LIVE_TICK") else {
            eprintln!("GLYPH_LIVE_TICK not set; skipping");
            return;
        };
        let tick_number: u32 = tick_number.parse().expect("GLYPH_LIVE_TICK must be a u32");
        let addr: SocketAddr = std::env::var("GLYPH_LIVE_PEER")
            .unwrap_or_else(|_| "157.180.10.49:21841".into())
            .parse()
            .expect("GLYPH_LIVE_PEER must be host:port");
        let pool = NodePool::new(vec![
            PeerConfig::new(addr).with_timeout(Duration::from_secs(10)),
        ]);
        let bundle = pool_link_bundle(&pool, tick_number).await;
        assert_eq!(bundle.tick.tick, tick_number);
        assert_eq!(bundle.tick_data.tick, tick_number);
        eprintln!(
            "live tick {tick_number}: votes={} transactions={} slots={} digest={} logs={} warnings={:?}",
            bundle.votes.len(),
            bundle.transactions.len(),
            bundle.tick_data.non_empty_transactions().count(),
            hex::encode(bundle.tick_data.digest()),
            bundle.logs.len(),
            bundle.warnings
        );
    }

    #[tokio::test]
    #[ignore = "requires GLYPH_LIVE_PEERS and GLYPH_LIVE_TICK"]
    async fn live_log_protocol_peers() {
        let peers = std::env::var("GLYPH_LIVE_PEERS").expect("GLYPH_LIVE_PEERS");
        let tick = std::env::var("GLYPH_LIVE_TICK")
            .ok()
            .and_then(|tick| tick.parse().ok())
            .unwrap_or(92_273_000u32);
        for addr in peers
            .split(',')
            .map(str::trim)
            .filter(|addr| !addr.is_empty())
        {
            let started = std::time::Instant::now();
            let config = PeerConfig::new(addr.parse().expect("peer address"))
                .with_timeout(Duration::from_secs(10));
            let result = async {
                let mut peer = Peer::connect(config).await?;
                let data = peer.tick_data(tick).await?;
                let protocol = peer.log_protocol(&data).await?;
                Ok::<_, Error>((protocol, data.non_empty_transactions().count()))
            }
            .await;
            match result {
                Ok((protocol, transactions)) => eprintln!(
                    "{addr}: protocol={protocol:?} transactions={transactions} elapsed={:?}",
                    started.elapsed()
                ),
                Err(error) => eprintln!("{addr}: {error} elapsed={:?}", started.elapsed()),
            }
        }
    }

    #[tokio::test]
    #[ignore = "requires GLYPH_LIVE_PEERS and GLYPH_LIVE_TICK"]
    async fn live_raw_log_protocol() {
        let peers = std::env::var("GLYPH_LIVE_PEERS").expect("GLYPH_LIVE_PEERS");
        let tick = std::env::var("GLYPH_LIVE_TICK")
            .ok()
            .and_then(|tick| tick.parse().ok())
            .unwrap_or(92_273_000u32);
        for addr in peers
            .split(',')
            .map(str::trim)
            .filter(|addr| !addr.is_empty())
        {
            let config = PeerConfig::new(addr.parse().expect("peer address"))
                .with_timeout(Duration::from_secs(1));
            let Ok(mut peer) = Peer::connect(config).await else {
                eprintln!("{addr}: connect failed");
                continue;
            };
            for (label, kind, payload) in [
                (
                    "all-ranges",
                    MessageType::RequestAllLogIdRangesFromTx,
                    request_all_log_ranges(tick, [0; 4]),
                ),
                ("tx-range-0", MessageType::RequestLogIdRangeFromTx, {
                    let mut payload = Vec::new();
                    for part in [0u64; 4] {
                        payload.extend_from_slice(&part.to_le_bytes());
                    }
                    payload.extend_from_slice(&tick.to_le_bytes());
                    payload.extend_from_slice(&0u32.to_le_bytes());
                    payload
                }),
                ("tx-range-special", MessageType::RequestLogIdRangeFromTx, {
                    let mut payload = Vec::new();
                    for part in [0u64; 4] {
                        payload.extend_from_slice(&part.to_le_bytes());
                    }
                    payload.extend_from_slice(&tick.to_le_bytes());
                    payload.extend_from_slice(&(TX_PER_TICK as u32).to_le_bytes());
                    payload
                }),
            ] {
                let dejavu = peer.send(kind, payload).await.expect("send");
                let started = std::time::Instant::now();
                let mut seen = Vec::new();
                loop {
                    match peer.read_packet_timeout(Duration::from_secs(3)).await {
                        Ok(packet) => {
                            if packet.dejavu == dejavu {
                                eprintln!(
                                    "{addr}: {label} -> {:?} payload={} first={:?} after={:?} seen={seen:?}",
                                    packet.kind,
                                    packet.payload.len(),
                                    &packet.payload[..packet.payload.len().min(16)],
                                    started.elapsed()
                                );
                                break;
                            }
                            if seen.len() < 12 {
                                seen.push(packet.kind);
                            }
                        }
                        Err(error) => {
                            eprintln!(
                                "{addr}: {label} -> error {error} after={:?} seen={seen:?}",
                                started.elapsed()
                            );
                            break;
                        }
                    }
                }
            }
        }
    }

    #[tokio::test]
    #[ignore = "requires GLYPH_LIVE_PEERS and GLYPH_LIVE_TICK"]
    async fn live_log_range_peers() {
        let peers = std::env::var("GLYPH_LIVE_PEERS").expect("GLYPH_LIVE_PEERS");
        let tick = std::env::var("GLYPH_LIVE_TICK")
            .ok()
            .and_then(|tick| tick.parse().ok())
            .unwrap_or(92_273_000u32);
        for addr in peers.split(',') {
            let addr = addr.trim();
            if addr.is_empty() {
                continue;
            }
            let started = std::time::Instant::now();
            let config = PeerConfig::new(addr.parse().expect("peer address"))
                .with_timeout(Duration::from_secs(10))
                .with_connect_timeout(Duration::from_secs(5));
            let result = async {
                let mut peer = Peer::connect(config).await?;
                let head = peer.head().await?;
                let ranges = peer.log_ranges(tick).await?;
                Ok::<_, Error>((head, ranges))
            }
            .await;
            match result {
                Ok((head, ranges)) => eprintln!(
                    "{addr}: head={} ranges={} logs={} elapsed={:?}",
                    head.tick,
                    ranges.range().map_or(0, |(from, to)| to - from + 1),
                    ranges.count(),
                    started.elapsed()
                ),
                Err(error) => eprintln!("{addr}: {error} elapsed={:?}", started.elapsed()),
            }
        }
    }

    #[tokio::test]
    #[ignore = "requires GLYPH_LIVE_TICK and a reachable Qubic peer"]
    async fn live_prefetch_completeness_smoke() {
        let tick_number = std::env::var("GLYPH_LIVE_TICK")
            .ok()
            .and_then(|tick| tick.parse().ok())
            .unwrap_or(92_264_150u32);
        let mut configs = vec![PeerConfig::new(live_addr()).with_timeout(Duration::from_secs(10))];
        if let Ok(fallback) = std::env::var("GLYPH_LIVE_FALLBACK_PEER") {
            configs.push(
                PeerConfig::new(
                    fallback
                        .parse()
                        .expect("GLYPH_LIVE_FALLBACK_PEER must be host:port"),
                )
                .with_timeout(Duration::from_secs(10)),
            );
        }
        let pool = NodePool::new(configs);
        let mut peer =
            Peer::connect(PeerConfig::new(live_addr()).with_timeout(Duration::from_secs(10)))
                .await
                .expect("live connect");
        let head = peer.head().await.expect("live head");
        let computors = peer.computors().await.expect("live computors");
        let mut bundles = peer
            .prefetch_bundles(&[tick_number], head, computors, Some(&pool))
            .await
            .expect("live prefetch");
        let bundle = bundles.pop().expect("one bundle").expect("prefetch result");
        let expected = bundle
            .tick_data
            .non_empty_transactions()
            .map(|(_, digest)| *digest)
            .collect::<std::collections::HashSet<_>>();
        let fetched = bundle
            .transactions
            .iter()
            .map(Transaction::hash)
            .collect::<std::collections::HashSet<_>>();
        let missing = expected.difference(&fetched).count();
        eprintln!(
            "live prefetch raw tick {tick_number}: transactions={} unique={} \
             expected={} missing={missing} logs={} ranges={} complete={} warnings={:?}",
            bundle.transactions.len(),
            fetched.len(),
            expected.len(),
            bundle.logs.len(),
            bundle.log_ranges.count(),
            bundle.logs_complete,
            bundle.warnings
        );
        assert_eq!(missing, 0, "prefetch transaction completeness");
        assert!(bundle.logs_complete, "prefetch log completeness");
        eprintln!(
            "live prefetch tick {tick_number}: transactions={}/{} logs={} warnings={:?}",
            fetched.len(),
            expected.len(),
            bundle.logs.len(),
            bundle.warnings
        );
    }

    #[tokio::test]
    #[ignore = "requires a reachable Qubic peer; run with --ignored --nocapture"]
    async fn live_bundle_with_cached_logs_smoke() {
        let mut peer = live_peer().await;
        let head = peer.head().await.expect("live head");
        let computors = peer.computors().await.expect("live computors");
        let tick = head.tick.saturating_sub(4);

        let started = std::time::Instant::now();
        let first = peer
            .bundle_with(tick, head.clone(), computors.clone())
            .await
            .expect("first bundle_with");
        let first_elapsed = started.elapsed();

        let started = std::time::Instant::now();
        let second = peer
            .bundle_with(tick + 1, head, computors)
            .await
            .expect("second bundle_with");
        let second_elapsed = started.elapsed();

        eprintln!(
            "bundle_with tick {tick}: {:?} (logs={}, warnings={:?})",
            first_elapsed,
            first.logs.len(),
            first.warnings
        );
        eprintln!(
            "bundle_with tick {}: {:?} (logs={}, warnings={:?})",
            tick + 1,
            second_elapsed,
            second.logs.len(),
            second.warnings
        );
    }

    async fn pool_link_bundle(pool: &NodePool, tick_number: u32) -> TickBundle {
        let mut peer = pool.connect_best().await.expect("live connect");
        peer.bundle(tick_number).await.expect("live bundle")
    }

    fn live_pool() -> NodePool {
        let addr: SocketAddr = std::env::var("GLYPH_LIVE_PEER")
            .unwrap_or_else(|_| "157.180.10.49:21841".into())
            .parse()
            .expect("GLYPH_LIVE_PEER must be host:port");
        NodePool::new(vec![
            PeerConfig::new(addr).with_timeout(Duration::from_secs(10)),
        ])
    }

    #[tokio::test]
    #[ignore = "requires a reachable Qubic peer; run with --ignored"]
    async fn live_transaction_info_smoke() {
        let pool = live_pool();
        let head = pool.head().await.expect("live head");
        for back in 1..=20u32 {
            let Some(tick) = head.tick.checked_sub(back) else {
                break;
            };
            let mut peer = pool.connect_best().await.expect("live connect");
            let Ok(tick_data) = peer.tick_data(tick).await else {
                continue;
            };
            for (slot, digest) in tick_data.non_empty_transactions().take(3) {
                match peer
                    .transaction(*digest)
                    .await
                    .expect("live transaction info")
                {
                    Some(transaction) => {
                        assert_eq!(transaction.hash(), *digest);
                        assert_eq!(transaction.tick, tick);
                        eprintln!(
                            "live transaction info tick={tick} slot={slot} digest={} size={}",
                            qubic::encode_tx_hash(digest),
                            transaction.total_size()
                        );
                        return;
                    }
                    None => eprintln!("live transaction info: digest not retained at tick {tick}"),
                }
            }
        }
        eprintln!(
            "peer retains no transaction info for the last 20 ticks; \
             RequestTransactionInfo is optional on Bob peers"
        );
    }

    #[tokio::test]
    #[ignore = "requires a Qubic peer with the qli tx-status add-on"]
    async fn live_tx_status_smoke() {
        let pool = live_pool();
        let head = pool.head().await.expect("live head");
        let tick = head.tick.saturating_sub(2);
        match pool.tx_status(tick).await {
            Ok(status) => {
                assert_eq!(status.tick, tick);
                assert!(status.current_tick >= tick);
                eprintln!(
                    "live tx status tick={} current={} transactions={}",
                    status.tick,
                    status.current_tick,
                    status.transactions.len()
                );
            }
            Err(Error::Timeout(_)) => {
                eprintln!("live tx status: peer has no tx-status add-on; skipping");
            }
            Err(error) => panic!("live tx status failed: {error}"),
        }
    }

    #[tokio::test]
    async fn gossip_flood_does_not_extend_request_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            send(
                &mut stream,
                MessageType::ExchangePublicPeers,
                1,
                vec![0u8; 16],
            )
            .await;
            // Never answer the request, but keep the socket busy with gossip.
            loop {
                send(
                    &mut stream,
                    MessageType::BroadcastTick,
                    0xabcd,
                    tick(0, TICK).encode().to_vec(),
                )
                .await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        let mut peer = Peer::connect(
            PeerConfig::new(addr)
                .with_timeout(Duration::from_millis(300))
                .with_connect_timeout(Duration::from_secs(2))
                .with_retries(0),
        )
        .await
        .unwrap();
        let started = std::time::Instant::now();
        let error = peer.head().await.unwrap_err();
        assert!(matches!(error, Error::Timeout(_)), "{error:?}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "deadline was extended by gossip"
        );
        server.abort();
    }

    fn fixture_transaction() -> Transaction {
        Transaction::decode(include_bytes!("../fixtures/tx-91916364-1.bin"))
            .expect("fixture transaction decodes")
    }

    fn issuance_record(key: u8, name: &[u8; 7]) -> AssetRecord {
        AssetRecord {
            public_key: [key; 32],
            kind: AssetRecordKind::Issuance(AssetIssuanceRecord {
                public_key: [key; 32],
                name: *name,
                decimals: 0,
                unit: *b"0000000",
            }),
        }
    }

    fn ownership_record(key: u8, shares: i64) -> AssetRecord {
        AssetRecord {
            public_key: [key; 32],
            kind: AssetRecordKind::Ownership(AssetOwnershipRecord {
                public_key: [key; 32],
                managing_contract_index: 1,
                issuance_index: 10,
                shares,
            }),
        }
    }

    fn possession_record(key: u8, shares: i64) -> AssetRecord {
        AssetRecord {
            public_key: [key; 32],
            kind: AssetRecordKind::Possession(AssetPossessionRecord {
                public_key: [key; 32],
                managing_contract_index: 2,
                ownership_index: 5,
                shares,
            }),
        }
    }

    fn assets_siblings() -> Vec<[u8; 32]> {
        vec![[0u8; 32]; ASSETS_DEPTH]
    }

    async fn fake_peer_advanced(
        listener: TcpListener,
        sent: tokio::sync::mpsc::UnboundedSender<(MessageType, u32, Vec<u8>)>,
    ) {
        let (mut stream, _) = listener.accept().await.unwrap();
        send(
            &mut stream,
            MessageType::ExchangePublicPeers,
            77,
            vec![0u8; 16],
        )
        .await;

        while let Some((kind, dejavu, payload)) = read_request(&mut stream).await {
            match kind {
                MessageType::ExchangePublicPeers => {}
                MessageType::RequestEntity => {
                    assert_eq!(payload.len(), 32);
                    let public_key: [u8; 32] = payload.as_slice().try_into().unwrap();
                    let entity = EntityRecord {
                        public_key,
                        incoming_amount: 42,
                        outgoing_amount: 7,
                        number_of_incoming_transfers: 2,
                        number_of_outgoing_transfers: 1,
                        latest_incoming_transfer_tick: 998,
                        latest_outgoing_transfer_tick: 997,
                    };
                    let response = RespondEntity {
                        entity,
                        tick: 1000,
                        spectrum_index: 5,
                        siblings: vec![[7; 32]; SPECTRUM_DEPTH],
                    };
                    send(
                        &mut stream,
                        MessageType::RespondEntity,
                        dejavu,
                        response.encode(),
                    )
                    .await;
                }
                MessageType::RequestContractFunction => {
                    let input_type = u16::from_le_bytes(payload[4..6].try_into().unwrap());
                    let input_size = u16::from_le_bytes(payload[6..8].try_into().unwrap()) as usize;
                    assert_eq!(payload.len(), 8 + input_size);
                    let mut output = payload[8..].to_vec();
                    output.reverse();
                    if input_type == 0 {
                        output.clear();
                    }
                    send(
                        &mut stream,
                        MessageType::RespondContractFunction,
                        dejavu,
                        output,
                    )
                    .await;
                }
                MessageType::RequestTransactionInfo => {
                    assert_eq!(payload.len(), 32);
                    let digest: [u8; 32] = payload.as_slice().try_into().unwrap();
                    let transaction = fixture_transaction();
                    if digest == transaction.hash() {
                        send(
                            &mut stream,
                            MessageType::BroadcastTransaction,
                            dejavu,
                            transaction.encode(),
                        )
                        .await;
                    } else {
                        send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                    }
                }
                MessageType::RequestTxStatus => {
                    let tick = u32::from_le_bytes(payload.try_into().unwrap());
                    let status = RespondTxStatus {
                        current_tick: 1000,
                        tick,
                        digests: vec![[1; 32], [2; 32]],
                        money_flew: vec![true, false],
                    };
                    send(
                        &mut stream,
                        MessageType::RespondTxStatus,
                        dejavu,
                        status.encode(),
                    )
                    .await;
                }
                MessageType::RequestIssuedAssets => {
                    assert_eq!(payload.len(), 32);
                    let public_key: [u8; 32] = payload.as_slice().try_into().unwrap();
                    if public_key != [0u8; 32] {
                        for (index, universe_index) in [10u32, 11].into_iter().enumerate() {
                            let response = RespondIssuedAssets {
                                asset: issuance_record(
                                    index as u8 + 1,
                                    if index == 0 {
                                        b"QX\0\0\0\0\0"
                                    } else {
                                        b"QY\0\0\0\0\0"
                                    },
                                ),
                                tick: 1000,
                                universe_index,
                                siblings: assets_siblings(),
                            };
                            send(
                                &mut stream,
                                MessageType::RespondIssuedAssets,
                                dejavu,
                                response.encode(),
                            )
                            .await;
                        }
                    }
                    send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                }
                MessageType::RequestOwnedAssets => {
                    assert_eq!(payload.len(), 32);
                    for (index, universe_index) in [20u32, 21].into_iter().enumerate() {
                        let response = RespondOwnedAssets {
                            asset: ownership_record(index as u8 + 1, 100 + index as i64),
                            issuance_asset: issuance_record(index as u8 + 1, b"QX\0\0\0\0\0"),
                            tick: 1000,
                            universe_index,
                            siblings: assets_siblings(),
                        };
                        send(
                            &mut stream,
                            MessageType::RespondOwnedAssets,
                            dejavu,
                            response.encode(),
                        )
                        .await;
                    }
                    send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                }
                MessageType::RequestPossessedAssets => {
                    assert_eq!(payload.len(), 32);
                    for (index, universe_index) in [30u32, 31].into_iter().enumerate() {
                        let response = RespondPossessedAssets {
                            asset: possession_record(index as u8 + 1, 200 + index as i64),
                            ownership_asset: ownership_record(index as u8 + 1, 100),
                            issuance_asset: issuance_record(index as u8 + 1, b"QX\0\0\0\0\0"),
                            tick: 1000,
                            universe_index,
                            siblings: assets_siblings(),
                        };
                        send(
                            &mut stream,
                            MessageType::RespondPossessedAssets,
                            dejavu,
                            response.encode(),
                        )
                        .await;
                    }
                    send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                }
                MessageType::RequestAssets => {
                    assert_eq!(payload.len(), qubic::messages::REQUEST_ASSETS_SIZE);
                    let request = RequestAssets::decode(&payload).unwrap();
                    for universe_index in [40u32, 41] {
                        let response = RespondAssets {
                            asset: issuance_record(1, b"QX\0\0\0\0\0"),
                            tick: 1000,
                            universe_index,
                        };
                        let payload =
                            if request.flags() & qubic::messages::ASSET_REQUEST_GET_SIBLINGS != 0 {
                                RespondAssetsWithSiblings {
                                    asset: response.asset,
                                    tick: response.tick,
                                    universe_index: response.universe_index,
                                    siblings: assets_siblings(),
                                }
                                .encode()
                            } else {
                                response.encode()
                            };
                        send(&mut stream, MessageType::RespondAssets, dejavu, payload).await;
                    }
                    send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                }
                MessageType::RequestPriceSeries => {
                    let request = qubic::messages::RequestPriceSeries::decode(&payload).unwrap();
                    assert!(request.count <= PRICE_SERIES_MAX_POINTS as u32);
                    let points = (0..request.count)
                        .map(|offset| RespondPriceSeriesPoint {
                            hour: request.from_hour + offset,
                            append_tick: 1000 + offset,
                            flags: if offset % 2 == 0 { 1 } else { 0 },
                            price_e8: 1_000_000_000 + offset as i64,
                        })
                        .collect::<Vec<_>>();
                    let response = RespondPriceSeries {
                        tick: 1000,
                        epoch: 240,
                        synced: true,
                        bound: false,
                        source: 3,
                        next_hour: request.from_hour + request.count,
                        failed_attempts: 0,
                        first_fail_ms: 0,
                        last_fail_ms: 0,
                        series_digest: [0xab; 32],
                        num_success: 1,
                        num_timeout: 0,
                        num_unresolvable: 0,
                        num_invalid_reply: 0,
                        num_start_failed: 0,
                        num_filled: points.len() as u64,
                        base_hour: 100,
                        count: points.len() as u32,
                        points,
                    };
                    send(
                        &mut stream,
                        MessageType::RespondPriceSeries,
                        dejavu,
                        response.encode(),
                    )
                    .await;
                }
                MessageType::BroadcastTransaction => {
                    sent.send((kind, dejavu, payload)).unwrap();
                }
                _ => {}
            }
        }
    }

    async fn advanced_peer() -> (
        Peer,
        tokio::sync::mpsc::UnboundedReceiver<(MessageType, u32, Vec<u8>)>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (sent, received) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(fake_peer_advanced(listener, sent));
        let peer = Peer::connect(
            PeerConfig::new(addr)
                .with_timeout(Duration::from_secs(2))
                .with_connect_timeout(Duration::from_secs(2)),
        )
        .await
        .unwrap();
        (peer, received)
    }

    #[tokio::test]
    async fn entity_lookup_returns_record_and_proof() {
        let (mut peer, _sent) = advanced_peer().await;
        let entity = peer.entity([9; 32]).await.unwrap();
        assert_eq!(entity.entity.public_key, [9; 32]);
        assert_eq!(entity.entity.incoming_amount, 42);
        assert_eq!(entity.entity.outgoing_amount, 7);
        assert_eq!(entity.entity.latest_incoming_transfer_tick, 998);
        assert_eq!(entity.tick, 1000);
        assert_eq!(entity.spectrum_index, 5);
        assert_eq!(entity.siblings, vec![[7; 32]; SPECTRUM_DEPTH]);
    }

    #[tokio::test]
    async fn entity_lookup_accepts_identity() {
        let (mut peer, _sent) = advanced_peer().await;
        let identity = "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK";
        let expected = qubic::PublicKey::from_identity(identity).unwrap().0;
        let entity = peer.entity_by_identity(identity).await.unwrap();
        assert_eq!(entity.entity.public_key, expected);
        assert!(matches!(
            peer.entity_by_identity("not-an-identity").await,
            Err(Error::Verification(_))
        ));
    }

    #[tokio::test]
    async fn contract_function_returns_output_and_empty_failure() {
        let (mut peer, _sent) = advanced_peer().await;
        let output = peer.contract_function(1, 7, &[1, 2, 3]).await.unwrap();
        assert_eq!(output, vec![3, 2, 1]);
        let empty = peer.contract_function(1, 0, &[]).await.unwrap();
        assert!(empty.is_empty());
        let oversized = vec![0u8; MAX_INPUT_SIZE + 1];
        assert!(matches!(
            peer.contract_function(1, 7, &oversized).await,
            Err(Error::Verification(_))
        ));
    }

    #[tokio::test]
    async fn transaction_info_found_and_missing() {
        let (mut peer, _sent) = advanced_peer().await;
        let transaction = fixture_transaction();
        let found = peer.transaction(transaction.hash()).await.unwrap();
        let found = found.expect("fixture transaction should be found");
        assert_eq!(found, transaction);
        assert!(peer.transaction([0xab; 32]).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn tx_status_decodes_money_flew_flags() {
        let (mut peer, _sent) = advanced_peer().await;
        let status = peer.tx_status(TICK).await.unwrap();
        assert_eq!(status.tick, TICK);
        assert_eq!(status.current_tick, 1000);
        assert_eq!(status.transactions.len(), 2);
        assert_eq!(status.transactions[0].digest, [1; 32]);
        assert!(status.transactions[0].money_flew);
        assert!(!status.transactions[1].money_flew);
        assert_eq!(status.money_flew_for(&[2; 32]), Some(false));
        assert!(!status.contains(&[3; 32]));
    }

    #[tokio::test]
    async fn issued_assets_empty_then_multiple_in_order() {
        let (mut peer, _sent) = advanced_peer().await;
        let empty = peer.issued_assets([0; 32]).await.unwrap();
        assert!(empty.is_empty());

        let issued = peer.issued_assets([7; 32]).await.unwrap();
        assert_eq!(issued.len(), 2);
        assert_eq!(issued[0].universe_index, 10);
        assert_eq!(issued[1].universe_index, 11);
        assert_eq!(issued[0].asset, issuance_record(1, b"QX\0\0\0\0\0"));
        assert_eq!(issued[1].asset, issuance_record(2, b"QY\0\0\0\0\0"));
        assert_eq!(issued[0].siblings.len(), ASSETS_DEPTH);
    }

    #[tokio::test]
    async fn owned_and_possessed_assets_keep_peer_order() {
        let (mut peer, _sent) = advanced_peer().await;

        let owned = peer.owned_assets([7; 32]).await.unwrap();
        assert_eq!(owned.len(), 2);
        assert_eq!(owned[0].universe_index, 20);
        assert_eq!(owned[1].universe_index, 21);
        match &owned[0].asset.kind {
            AssetRecordKind::Ownership(record) => assert_eq!(record.shares, 100),
            other => panic!("unexpected owned asset kind: {other:?}"),
        }
        match &owned[1].asset.kind {
            AssetRecordKind::Ownership(record) => assert_eq!(record.shares, 101),
            other => panic!("unexpected owned asset kind: {other:?}"),
        }

        let possessed = peer.possessed_assets([7; 32]).await.unwrap();
        assert_eq!(possessed.len(), 2);
        assert_eq!(possessed[0].universe_index, 30);
        assert_eq!(possessed[1].universe_index, 31);
        match &possessed[1].asset.kind {
            AssetRecordKind::Possession(record) => assert_eq!(record.shares, 201),
            other => panic!("unexpected possessed asset kind: {other:?}"),
        }
    }

    #[tokio::test]
    async fn asset_records_decode_plain_and_sibling_responses() {
        let (mut peer, _sent) = advanced_peer().await;
        let plain = peer
            .asset_records(&RequestAssets::ByUniverseIndex {
                flags: 0,
                universe_index: 1,
            })
            .await
            .unwrap();
        assert_eq!(plain.len(), 2);
        assert!(matches!(
            &plain[0],
            AssetRecordResponse::Asset(asset) if asset.universe_index == 40
        ));
        assert!(matches!(
            &plain[1],
            AssetRecordResponse::Asset(asset) if asset.universe_index == 41
        ));

        let with_siblings = peer
            .asset_records(&RequestAssets::ByFilter {
                request_type: qubic::messages::ASSET_REQUEST_ISSUANCE,
                flags: qubic::messages::ASSET_REQUEST_GET_SIBLINGS,
                ownership_managing_contract: 0,
                possession_managing_contract: 0,
                issuer: [1; 32],
                asset_name: 0,
                owner: [0; 32],
                possessor: [0; 32],
            })
            .await
            .unwrap();
        assert_eq!(with_siblings.len(), 2);
        assert!(matches!(
            &with_siblings[0],
            AssetRecordResponse::WithSiblings(asset)
                if asset.universe_index == 40 && asset.siblings.len() == ASSETS_DEPTH
        ));
        assert_eq!(with_siblings[0].universe_index(), 40);
    }

    #[tokio::test]
    async fn price_series_round_trip_and_clamps_count() {
        let (mut peer, _sent) = advanced_peer().await;
        let series = peer.price_series(500, 3).await.unwrap();
        assert_eq!(series.tick, 1000);
        assert_eq!(series.epoch, 240);
        assert!(series.synced);
        assert!(!series.bound);
        assert_eq!(series.count, 3);
        assert_eq!(series.points.len(), 3);
        assert_eq!(series.points[0].hour, 500);
        assert_eq!(series.points[0].flags, 1);
        assert_eq!(series.points[1].hour, 501);
        assert_eq!(series.points[1].append_tick, 1001);
        assert_eq!(series.points[2].price_e8, 1_000_000_002);

        let clamped = peer.price_series(500, 10_000).await.unwrap();
        assert_eq!(clamped.count, PRICE_SERIES_MAX_POINTS as u32);
        assert_eq!(clamped.points.len(), PRICE_SERIES_MAX_POINTS);
    }

    async fn malformed_price_peer() -> Peer {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            send(
                &mut stream,
                MessageType::ExchangePublicPeers,
                77,
                vec![0u8; 16],
            )
            .await;
            let mut served = 0;
            while let Some((kind, dejavu, _payload)) = read_request(&mut stream).await {
                if kind != MessageType::RequestPriceSeries {
                    continue;
                }
                served += 1;
                let count_offset = RESPOND_PRICE_SERIES_HEADER_SIZE - 4;
                let payload = if served == 1 {
                    let count = PRICE_SERIES_MAX_POINTS + 1;
                    let mut bytes = vec![
                        0u8;
                        RESPOND_PRICE_SERIES_HEADER_SIZE
                            + count * RESPOND_PRICE_SERIES_POINT_SIZE
                    ];
                    bytes[count_offset..count_offset + 4]
                        .copy_from_slice(&(count as u32).to_le_bytes());
                    bytes
                } else {
                    // Claims two points but only one is present.
                    let mut bytes = vec![
                        0u8;
                        RESPOND_PRICE_SERIES_HEADER_SIZE
                            + RESPOND_PRICE_SERIES_POINT_SIZE
                    ];
                    bytes[count_offset..count_offset + 4].copy_from_slice(&2u32.to_le_bytes());
                    bytes
                };
                send(
                    &mut stream,
                    MessageType::RespondPriceSeries,
                    dejavu,
                    payload,
                )
                .await;
            }
        });
        Peer::connect(
            PeerConfig::new(addr)
                .with_timeout(Duration::from_secs(2))
                .with_connect_timeout(Duration::from_secs(2)),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn price_series_rejects_oversized_and_short_responses() {
        let mut peer = malformed_price_peer().await;
        for _ in 0..2 {
            assert!(matches!(
                peer.price_series(500, 2).await,
                Err(Error::Wire(qubic::Error::InvalidPacket(_)))
            ));
        }
    }

    #[tokio::test]
    async fn broadcast_writes_signed_transaction_with_dejavu_zero() {
        let (mut peer, mut sent) = advanced_peer().await;
        let transaction = fixture_transaction();
        let receipt = peer.broadcast_transaction(&transaction).await.unwrap();
        assert_eq!(receipt.digest, transaction.hash());
        assert_eq!(receipt.tick, transaction.tick);
        let (kind, dejavu, payload) = sent.recv().await.unwrap();
        assert_eq!(kind, MessageType::BroadcastTransaction);
        assert_eq!(
            dejavu, 0,
            "a core only relays broadcast transactions with dejavu zero"
        );
        assert_eq!(payload, transaction.encode());
    }

    #[tokio::test]
    async fn broadcast_rejects_invalid_transaction_before_writing() {
        let (mut peer, mut sent) = advanced_peer().await;

        let mut bad_signature = fixture_transaction();
        bad_signature.signature[0] ^= 1;
        assert!(matches!(
            peer.broadcast_transaction(&bad_signature).await,
            Err(Error::Verification(_))
        ));

        let mut bad_amount = fixture_transaction();
        bad_amount.amount = -1;
        assert!(matches!(
            peer.broadcast_transaction(&bad_amount).await,
            Err(Error::Verification(_))
        ));

        assert!(sent.try_recv().is_err(), "nothing should reach the peer");
    }

    fn live_addr() -> SocketAddr {
        std::env::var("GLYPH_LIVE_PEER")
            .unwrap_or_else(|_| "157.180.10.49:21841".into())
            .parse()
            .expect("GLYPH_LIVE_PEER must be host:port")
    }

    async fn live_peer() -> Peer {
        Peer::connect(PeerConfig::new(live_addr()).with_timeout(Duration::from_secs(10)))
            .await
            .expect("live connect")
    }

    #[tokio::test]
    #[ignore = "requires a reachable Qubic peer; run with --ignored"]
    async fn live_entity_smoke() {
        let identity = std::env::var("GLYPH_LIVE_ENTITY").unwrap_or_else(|_| {
            "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK".into()
        });
        let expected = qubic::PublicKey::from_identity(&identity)
            .expect("GLYPH_LIVE_ENTITY must be a valid identity")
            .0;
        let mut peer = live_peer().await;
        let entity = peer
            .entity_by_identity(&identity)
            .await
            .expect("live entity");
        assert_eq!(entity.entity.public_key, expected);
        assert!(entity.tick > 0, "entity response should carry a peer tick");
        eprintln!(
            "live entity {identity}: tick={} spectrum_index={} incoming={} outgoing={}",
            entity.tick,
            entity.spectrum_index,
            entity.entity.incoming_amount,
            entity.entity.outgoing_amount
        );
    }

    #[tokio::test]
    #[ignore = "requires a reachable Qubic peer; run with --ignored"]
    async fn live_contract_function_smoke() {
        let contract_index = std::env::var("GLYPH_LIVE_CONTRACT_INDEX")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1u32);
        let input_type = std::env::var("GLYPH_LIVE_FUNCTION_INPUT_TYPE")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1u16);
        let mut peer = live_peer().await;
        let output = peer
            .contract_function(contract_index, input_type, &[])
            .await
            .expect("live contract function");
        eprintln!(
            "live contract function {contract_index}:{input_type} -> {} bytes",
            output.len()
        );
    }

    #[tokio::test]
    #[ignore = "requires GLYPH_LIVE_BROADCAST_TX and a reachable Qubic peer"]
    async fn live_broadcast_smoke() {
        let Ok(path) = std::env::var("GLYPH_LIVE_BROADCAST_TX") else {
            eprintln!("GLYPH_LIVE_BROADCAST_TX not set; skipping");
            return;
        };
        let bytes = std::fs::read(&path).expect("read GLYPH_LIVE_BROADCAST_TX");
        let transaction = Transaction::decode(&bytes).expect("signed transaction decodes");
        let mut peer = live_peer().await;
        let receipt = peer
            .broadcast_transaction(&transaction)
            .await
            .expect("live broadcast");
        assert_eq!(receipt.digest, transaction.hash());
        eprintln!(
            "live broadcast tick {} digest {} to {}",
            receipt.tick,
            qubic::encode_tx_hash(&receipt.digest),
            receipt.peer
        );
    }

    fn empty_vote(computor_index: u16, tick_number: u32) -> Tick {
        let mut vote = tick(computor_index, tick_number);
        vote.transaction_digest = [0; 32];
        vote
    }

    #[test]
    fn empty_tick_requires_a_full_aligned_quorum() {
        let votes = (0..QUORUM as u16)
            .map(|index| empty_vote(index, TICK))
            .collect::<Vec<_>>();
        let data = empty_tick_from_votes(&votes, TICK).expect("empty quorum");
        assert_eq!(data.tick, TICK);
        assert_eq!(data.computor_index, (TICK % COMPUTOR_COUNT as u32) as u16);
        assert_eq!(data.non_empty_transactions().count(), 0);

        let short = &votes[..QUORUM - 1];
        assert!(matches!(
            empty_tick_from_votes(short, TICK),
            Err(Error::MissingNonEmptyTick { .. })
        ));
    }

    #[test]
    fn empty_tick_rejects_split_timestamps() {
        let mut votes = (0..QUORUM as u16)
            .map(|index| empty_vote(index, TICK))
            .collect::<Vec<_>>();
        for (index, vote) in votes.iter_mut().enumerate().skip(QUORUM / 2) {
            vote.millisecond = index as u16;
        }
        assert!(matches!(
            empty_tick_from_votes(&votes, TICK),
            Err(Error::MissingNonEmptyTick { .. })
        ));
    }
}
