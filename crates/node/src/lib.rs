use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use qubic::LogEvent;
use qubic::crypto::schnorrq;
use qubic::messages::{
    COMPUTOR_COUNT, Computors, CurrentTickInfo, LOG_TX_PER_TICK, LogRanges, MAX_AMOUNT,
    MAX_INPUT_SIZE, PeerAddress, RespondContractFunction, RespondEntity, RespondTxStatus,
    TX_PER_TICK, Tick, TickData, Transaction, request_all_log_ranges, request_computors,
    request_contract_function, request_current_tick_info, request_entity, request_logs,
    request_quorum_tick, request_tick_data, request_tick_transactions, request_transaction_info,
    request_tx_status,
};
use qubic::wire::{HEADER_SIZE, MessageType, Packet};
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

/// How long a peer may take to answer one packet of a request.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(8);
/// How long establishing the TCP connection may take.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Extra attempts after the first one for a request.
pub const DEFAULT_RETRIES: u32 = 2;
/// Timeout for the all-ranges log request. Logs are optional, so this must not
/// dominate a tick fetch on peers without logging.
const LOG_RANGE_TIMEOUT: Duration = Duration::from_secs(4);
/// Timeout for one per-transaction log range probe during fallback.
const LOG_FALLBACK_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Guard against a peer that keeps returning a partial log page forever.
const MAX_LOG_PAGES: u32 = 1024;

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
    pub log_ranges: LogRanges,
    pub logs: Vec<LogEvent>,
    /// Non-fatal problems, such as a peer that has logging disabled.
    pub warnings: Vec<String>,
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

/// A broadcast transaction that passed local validation and was written to the
/// peer socket. Qubic core does not acknowledge broadcasts, so receipt means
/// "sent", not "included".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BroadcastReceipt {
    pub digest: [u8; 32],
    pub tick: u32,
    pub peer: SocketAddr,
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
}

impl Peer {
    /// Connects to a peer and completes the exchange-public-peers handshake.
    pub async fn connect(config: PeerConfig) -> Result<Self> {
        let stream = Self::open(&config).await?;
        let (reader, writer) = stream.into_split();
        let mut peer = Self {
            config,
            reader: BufReader::new(reader),
            writer,
            next_dejavu: random_dejavu(),
            pending: HashMap::new(),
            public_peers: Vec::new(),
            desynced: false,
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
        let peers = [PeerAddress { ip: [0, 0, 0, 0] }; 4];
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
        Ok(Head {
            epoch: info.epoch,
            tick: info.tick,
            initial_tick: info.initial_tick,
            tick_duration: info.tick_duration,
        })
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
        let mut votes = Vec::with_capacity(packets.len());
        for packet in packets {
            let vote = Tick::decode(&packet.payload)?;
            if vote.tick == tick {
                votes.push(vote);
            }
        }
        Ok(votes)
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
    /// Peers without logging either answer `EndResponse` or time out. Both are
    /// reported as [`Error::LogsUnavailable`] so callers can continue.
    pub async fn log_ranges(&mut self, tick: u32) -> Result<LogRanges> {
        let result = self
            .request_single_any_bounded(
                MessageType::RequestAllLogIdRangesFromTx,
                request_all_log_ranges(tick, self.config.passcode),
                &[MessageType::RespondAllLogIdRangesFromTx],
                LOG_RANGE_TIMEOUT,
                0,
            )
            .await;
        match result {
            Ok(packet) => Ok(LogRanges::decode(&packet.payload)?),
            Err(Error::EndResponse(_)) => Err(Error::LogsUnavailable(format!(
                "peer {} does not expose log ranges for tick {tick} \
                 (logging disabled or passcode mismatch)",
                self.config.addr
            ))),
            Err(Error::Timeout(elapsed)) => Err(Error::LogsUnavailable(format!(
                "peer {} did not answer log range request for tick {tick} after {elapsed:?}",
                self.config.addr
            ))),
            Err(error) => Err(error),
        }
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
        tracing::debug!(tick, "bundle: tick data");
        let tick_data = self.tick_data(tick).await?;
        if tick_data.tick != tick {
            return Err(Error::Verification(format!(
                "tick data is for tick {}, requested {tick}",
                tick_data.tick
            )));
        }

        tracing::debug!(tick, "bundle: quorum votes");
        let votes = self.votes(tick, &[false; COMPUTOR_COUNT]).await?;
        tracing::debug!(tick, votes = votes.len(), "bundle: transactions");
        let transactions = self.transactions(tick, &[false; TX_PER_TICK]).await?;

        // Historical or unindexed ticks may not have log data; that is not fatal.
        tracing::debug!(
            tick,
            transactions = transactions.len(),
            "bundle: log ranges"
        );
        let mut warnings = Vec::new();
        let (log_ranges, logs) = match self.fetch_log_ranges(tick, &tick_data).await {
            Ok(ranges) => {
                if ranges.is_pending() {
                    tracing::debug!(
                        peer = %self.config.addr,
                        tick,
                        "log generation still pending"
                    );
                    warnings.push(format!(
                        "peer {} reports log generation still pending for tick {tick}",
                        self.config.addr
                    ));
                }
                let logs = match ranges.range() {
                    Some((from, to)) if from >= 0 && to >= from => {
                        match self.logs(from as u64, to as u64).await {
                            Ok(logs) => logs,
                            Err(error) => {
                                tracing::warn!(
                                    peer = %self.config.addr,
                                    tick,
                                    %error,
                                    "peer returned log ranges but not log records"
                                );
                                warnings.push(format!(
                                    "peer {} returned log ranges but not log records: {error}",
                                    self.config.addr
                                ));
                                Vec::new()
                            }
                        }
                    }
                    _ => Vec::new(),
                };
                (ranges, logs)
            }
            Err(error) => {
                tracing::warn!(
                    peer = %self.config.addr,
                    tick,
                    %error,
                    "log data unavailable; continuing without logs"
                );
                warnings.push(format!(
                    "log data unavailable from peer {}: {error}",
                    self.config.addr
                ));
                (empty_log_ranges(), Vec::new())
            }
        };

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
            warnings,
        })
    }

    /// Fetches log ranges, falling back to the per-transaction request when the
    /// peer does not implement the all-ranges request.
    async fn fetch_log_ranges(&mut self, tick: u32, tick_data: &TickData) -> Result<LogRanges> {
        match self.log_ranges(tick).await {
            Ok(ranges) => Ok(ranges),
            Err(Error::LogsUnavailable(_)) => self.log_ranges_fallback(tick, tick_data).await,
            Err(error) => Err(error),
        }
    }

    /// Probes `RequestLogIdRangeFromTx` (type 48) for every non-empty
    /// transaction plus the six special-event slots.
    async fn log_ranges_fallback(&mut self, tick: u32, tick_data: &TickData) -> Result<LogRanges> {
        let mut ranges = empty_log_ranges();
        let mut indices: Vec<usize> = tick_data
            .non_empty_transactions()
            .map(|(index, _)| index)
            .collect();
        indices.extend(TX_PER_TICK..LOG_TX_PER_TICK);
        indices.sort_unstable();
        indices.dedup();

        let deadline = std::time::Instant::now() + self.config.timeout;
        let passcode = self.config.passcode;
        let mut answered = 0usize;
        let mut last_error = None;

        for index in indices {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let budget = remaining.min(LOG_FALLBACK_PROBE_TIMEOUT);
            let result = self
                .request_single_any_bounded(
                    MessageType::RequestLogIdRangeFromTx,
                    request_log_id_range_from_tx(tick, index as u32, passcode),
                    &[MessageType::RespondLogIdRangeFromTx],
                    budget,
                    0,
                )
                .await;
            match result {
                Ok(packet) => {
                    let (from, length) = decode_log_id_range(&packet.payload)?;
                    ranges.from_log_id[index] = from;
                    ranges.length[index] = length;
                    answered += 1;
                }
                // This slot has no range on this peer.
                Err(Error::EndResponse(_)) => {}
                Err(error) => {
                    last_error = Some(error);
                    break;
                }
            }
        }

        if answered > 0 {
            Ok(ranges)
        } else {
            Err(Error::LogsUnavailable(match last_error {
                Some(error) => format!(
                    "peer {} rejects both all-ranges and per-transaction log range requests: {error}",
                    self.config.addr
                ),
                None => format!(
                    "peer {} returned no log ranges for tick {tick} \
                     (logging disabled or passcode mismatch)",
                    self.config.addr
                ),
            }))
        }
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
        let attempts = self.config.retries.saturating_add(1);
        let mut attempt = 0;
        loop {
            attempt += 1;
            let dejavu = self.send(request, payload.clone()).await?;
            match self.receive_stream(dejavu, request, expected).await {
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
                kind if kind == expected => packets.push(packet),
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

/// A pool of independent peer connections.
pub struct NodePool {
    peers: Vec<PeerConfig>,
}

impl NodePool {
    pub fn new(peers: Vec<PeerConfig>) -> Self {
        Self { peers }
    }

    pub fn peer_configs(&self) -> &[PeerConfig] {
        &self.peers
    }

    /// Queries peers in order until one reports its head.
    pub async fn head(&self) -> Result<Head> {
        if self.peers.is_empty() {
            return Err(Error::NoPeers);
        }
        let mut last_error = None;
        for config in &self.peers {
            match Peer::connect(config.clone()).await {
                Ok(mut peer) => match peer.head().await {
                    Ok(head) => return Ok(head),
                    Err(error) => last_error = Some(error),
                },
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or(Error::NoPeers))
    }

    /// Looks up a spectrum entity on the best currently reachable peer.
    pub async fn entity(&self, public_key: [u8; 32]) -> Result<RespondEntity> {
        self.connect_best().await?.entity(public_key).await
    }

    /// Looks up a spectrum entity by its 60-character identity.
    pub async fn entity_by_identity(&self, identity: &str) -> Result<RespondEntity> {
        self.connect_best()
            .await?
            .entity_by_identity(identity)
            .await
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

    /// Connects with failover and keeps the peer with the fastest handshake.
    pub async fn connect_best(&self) -> Result<Peer> {
        if self.peers.is_empty() {
            return Err(Error::NoPeers);
        }
        let mut best: Option<(Duration, Peer)> = None;
        let mut last_error = None;
        for config in &self.peers {
            let started = std::time::Instant::now();
            match Peer::connect(config.clone()).await {
                Ok(peer) => {
                    let latency = started.elapsed();
                    if best
                        .as_ref()
                        .is_none_or(|(best_latency, _)| latency < *best_latency)
                    {
                        best = Some((latency, peer));
                    }
                }
                Err(error) => last_error = Some(error),
            }
        }
        match best {
            Some((_, peer)) => Ok(peer),
            None => Err(last_error.unwrap_or(Error::NoPeers)),
        }
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

/// Log ranges with every slot marked unavailable.
fn empty_log_ranges() -> LogRanges {
    LogRanges {
        from_log_id: vec![-1; LOG_TX_PER_TICK],
        length: vec![-1; LOG_TX_PER_TICK],
    }
}

/// Node-local helper for `RequestLogIdRangeFromTx` (type 48), which the codec
/// crate does not expose.
fn request_log_id_range_from_tx(tick: u32, tx_id: u32, passcode: [u64; 4]) -> Vec<u8> {
    let mut out = Vec::with_capacity(40);
    for part in passcode {
        out.extend_from_slice(&part.to_le_bytes());
    }
    out.extend_from_slice(&tick.to_le_bytes());
    out.extend_from_slice(&tx_id.to_le_bytes());
    out
}

/// Node-local decoder for `RespondLogIdRangeFromTx` (type 49).
fn decode_log_id_range(payload: &[u8]) -> Result<(i64, i64)> {
    if payload.len() != 16 {
        return Err(qubic::Error::InvalidPacket(format!(
            "log id range needs 16 bytes, got {}",
            payload.len()
        ))
        .into());
    }
    let from = i64::from_le_bytes(payload[0..8].try_into().expect("8 bytes"));
    let length = i64::from_le_bytes(payload[8..16].try_into().expect("8 bytes"));
    Ok((from, length))
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

#[cfg(test)]
mod tests {
    use super::*;
    use qubic::messages::{
        EntityRecord, LOG_TX_PER_TICK, MAX_CONTRACTS, SPECTRUM_DEPTH, TX_PER_TICK,
    };
    use qubic::wire::{Header, MessageType, Packet};
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
                    for index in [7u16, 9] {
                        send(
                            &mut stream,
                            MessageType::BroadcastTick,
                            dejavu,
                            tick(index, TICK).encode().to_vec(),
                        )
                        .await;
                    }
                    send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                }
                MessageType::RequestTickTransactions => {
                    let transaction = Transaction {
                        source: [1; 32],
                        destination: [2; 32],
                        amount: 123,
                        tick: TICK,
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
                _ => {}
            }
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

    #[tokio::test]
    async fn handshake_learns_public_peers() {
        let peer = peer_and_server().await;
        let peers: Vec<String> = peer
            .public_peers()
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(peers, ["1.2.3.4", "5.6.7.8", "9.10.11.12", "13.14.15.16"]);
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
    async fn fake_peer_optional_logs(listener: TcpListener, fallback: bool) {
        let (mut stream, _) = listener.accept().await.unwrap();
        send(
            &mut stream,
            MessageType::ExchangePublicPeers,
            77,
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        )
        .await;

        while let Some((kind, dejavu, payload)) = read_request(&mut stream).await {
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
                    send(&mut stream, MessageType::EndResponse, dejavu, Vec::new()).await;
                }
                MessageType::RequestLogIdRangeFromTx => {
                    let tx_id = u32::from_le_bytes(payload[36..40].try_into().unwrap());
                    if fallback && tx_id == TX_PER_TICK as u32 {
                        let mut range = Vec::with_capacity(16);
                        range.extend_from_slice(&5i64.to_le_bytes());
                        range.extend_from_slice(&2i64.to_le_bytes());
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
                    if fallback && from <= to {
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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(fake_peer_optional_logs(listener, fallback));
        Peer::connect(
            PeerConfig::new(addr)
                .with_timeout(Duration::from_secs(2))
                .with_connect_timeout(Duration::from_secs(2)),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn bundle_falls_back_to_per_transaction_log_ranges() {
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
    async fn bundle_soft_fails_when_peer_has_no_logging() {
        let mut peer = peer_with_optional_logs(false).await;
        let bundle = peer.bundle(TICK).await.unwrap();
        assert_eq!(bundle.tick.tick, TICK);
        assert_eq!(bundle.votes.len(), 1);
        assert!(bundle.logs.is_empty());
        assert_eq!(bundle.log_ranges.count(), 0);
        assert!(
            bundle
                .warnings
                .iter()
                .any(|warning| warning.contains("log data unavailable")),
            "warnings: {:?}",
            bundle.warnings
        );
    }

    #[tokio::test]
    async fn log_ranges_reports_unavailable_with_clear_message() {
        let mut peer = peer_with_optional_logs(false).await;
        match peer.log_ranges(TICK).await {
            Err(Error::LogsUnavailable(message)) => {
                assert!(message.contains("logging disabled") || message.contains("passcode"));
            }
            other => panic!("expected LogsUnavailable, got {other:?}"),
        }
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
            "live tick {tick_number}: votes={} transactions={} logs={} warnings={:?}",
            bundle.votes.len(),
            bundle.transactions.len(),
            bundle.logs.len(),
            bundle.warnings
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
}
