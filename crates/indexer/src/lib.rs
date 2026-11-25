//! Verification and sync pipeline for the Glyph Qubic index.

pub mod archive_policy;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use archive::{ArchivePolicy, SharedArchive};
use chrono::{DateTime, Utc};
use node::{Head, NodePool, Peer, PeerConfig, TickBundle};
use qubic::crypto::k12::k12_32;
use qubic::crypto::schnorrq;
use qubic::logs::{CONTRACT_ERROR_MESSAGE, EventBody};
use qubic::messages::{
    COMPUTOR_COUNT, Computors, EMPTY_TICK_QUORUM, LOG_TX_PER_TICK, QUORUM, TICK_DATA_SIZE,
    TX_PER_TICK, Tick, TickData, Transaction, ZERO_DIGEST,
};
use qubic::{LogEvent, PublicKey};
use serde::Serialize;
use store::{
    EventParticipantRow, EventRow, IdentityRow, QuorumEvidenceRow, Store, TickBatch, TransactionRow,
};
use tokio::sync::{Mutex as AsyncMutex, RwLock as AsyncRwLock, mpsc};
use tokio_util::sync::CancellationToken;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("node: {0}")]
    Node(#[from] node::Error),
    #[error("store: {0}")]
    Store(#[from] store::Error),
    #[error("verification: {0}")]
    Verification(String),
    #[error("configuration: {0}")]
    Config(String),
    #[error("archive: {0}")]
    Archive(#[from] archive::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Ticks this close to the head may not be archived yet. Never skip them;
/// wait for a peer or the archive to publish the data instead.
const LIVE_HEAD_MARGIN: u32 = 256;
/// Quorum votes for tick `N` complete while `N + 1` runs. Stay a few ticks
/// behind the reported head so live ticks always have their full vote set.
const LIVE_HEAD_LAG: u32 = 8;

#[derive(Clone)]
pub struct IndexerConfig {
    pub peers: Vec<PeerConfig>,
    /// First tick to fetch when there is no stored cursor. Defaults to the
    /// current epoch's initial tick.
    pub start_tick: Option<u32>,
    /// How often to poll the head when caught up.
    pub poll_interval: Duration,
    /// Verify SchnorrQ signatures in addition to quorum digests. Off by
    /// default: per-vote FourQ verification caps catch-up at roughly one tick
    /// per second on a small host. The quorum check still requires a distinct
    /// supermajority of computor indices to agree on one transaction digest.
    pub verify_signatures: bool,
    /// Log progress every N ticks.
    pub progress_every: u64,
    /// Give up on a tick after this many fetch attempts when every peer says
    /// the tick data does not exist. Retries still apply to transient errors.
    pub max_tick_attempts: u32,
    /// Optional raw bundle archive (filesystem or S3/MinIO).
    pub archive: Option<SharedArchive>,
    /// When true, an archive failure prevents the tick from being indexed.
    pub archive_fail_closed: bool,
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            peers: Vec::new(),
            start_tick: None,
            poll_interval: Duration::from_millis(750),
            verify_signatures: false,
            progress_every: 100,
            max_tick_attempts: 4,
            archive: None,
            archive_fail_closed: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verification {
    Quorum,
    Data,
    State,
}

impl Verification {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Quorum => "quorum",
            Self::Data => "data",
            Self::State => "state",
        }
    }
}

/// Everything derived from one verified tick.
#[derive(Debug, Clone)]
pub struct VerifiedTick {
    pub head: Head,
    pub tick: Tick,
    pub tick_data: TickData,
    pub votes: Vec<Tick>,
    pub transactions: Vec<Transaction>,
    pub computors: Computors,
    pub log_ranges: qubic::messages::LogRanges,
    pub logs: Vec<LogEvent>,
    pub verification: Verification,
    pub warnings: Vec<String>,
}

/// Build the event and participant rows for one verified tick without touching
/// any other table. Log repair uses this when logs became available after the
/// tick was first persisted.
pub fn event_rows(verified: &VerifiedTick) -> (Vec<EventRow>, Vec<EventParticipantRow>) {
    let timestamp = verified_timestamp(verified);
    let tx_hashes = transaction_hashes(verified);
    let log_slots = LogSlotLookup::new(&verified.log_ranges);
    let mut events = Vec::with_capacity(verified.logs.len());
    let mut participants = Vec::new();
    for event in &verified.logs {
        let tx_index = log_slots.slot(event.log_id);
        let (tx_hash, tx_index_i16) = match tx_index {
            Some(index) if index < TX_PER_TICK => (
                tx_hashes
                    .get(&(index as u16))
                    .cloned()
                    .unwrap_or_else(|| format!("slot:{index:?}")),
                index as i16,
            ),
            Some(index) => (format!("special:{}:{index}", event.tick), -1),
            None => (String::new(), -1),
        };
        events.push(EventRow {
            epoch: event.epoch,
            tick: event.tick,
            log_id: event.log_id,
            log_type: event.log_type as u16,
            type_name: event.type_name().to_string(),
            tx_hash: tx_hash.clone(),
            tx_index: tx_index_i16,
            body: event_body_json(event),
            log_digest: event.log_digest,
            timestamp,
        });
        for (identity, role) in event.body().participant_identities() {
            participants.push(EventParticipantRow {
                epoch: event.epoch,
                tick: event.tick,
                log_id: event.log_id,
                identity,
                role: role.to_string(),
                tx_hash: tx_hash.clone(),
                tx_index: tx_index_i16,
                timestamp,
            });
        }
    }
    (events, participants)
}

fn verified_timestamp(verified: &VerifiedTick) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(verified.tick.timestamp_millis()).unwrap_or_else(Utc::now)
}

fn transaction_hashes(verified: &VerifiedTick) -> HashMap<u16, String> {
    let mut hashes = HashMap::new();
    let mut by_hash = HashMap::<[u8; 32], &Transaction>::new();
    for transaction in &verified.transactions {
        by_hash.entry(transaction.hash()).or_insert(transaction);
    }
    for (index, digest) in verified.tick_data.non_empty_transactions() {
        if by_hash.contains_key(digest) {
            hashes.insert(index as u16, qubic::encode_tx_hash(digest));
        }
    }
    hashes
}

#[derive(Debug, Clone, Copy)]
struct PipelineTuning {
    fetch_concurrency: usize,
    fetch_window: usize,
    fetch_batch: usize,
    batch_min_lag: usize,
    write_batch: usize,
    write_flush: Duration,
    archive_concurrency: usize,
    verify_concurrency: usize,
}

fn env_usize(name: &str, default: usize, min: usize, max: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
        .clamp(min, max)
}

impl PipelineTuning {
    fn from_env() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(4);
        Self {
            // Fetching is I/O-bound, so a wide worker pool is cheap. The
            // window bounds memory: at most `fetch_window` bundles are in
            // flight or buffered by the ordered writer.
            fetch_concurrency: env_usize("INDEXER_FETCH_CONCURRENCY", 32, 1, 128),
            fetch_window: env_usize("INDEXER_FETCH_WINDOW", 512, 1, 8192),
            fetch_batch: env_usize("INDEXER_FETCH_BATCH", 32, 1, 256),
            batch_min_lag: env_usize("INDEXER_BATCH_MIN_LAG", 8, 1, 100_000),
            write_batch: env_usize("INDEXER_WRITE_BATCH", 128, 1, 4096),
            write_flush: Duration::from_millis(
                env_usize("INDEXER_WRITE_FLUSH_MS", 250, 10, 10_000) as u64,
            ),
            archive_concurrency: env_usize("INDEXER_ARCHIVE_CONCURRENCY", 16, 1, 64),
            // Leave one core for the writer, ClickHouse client, and API.
            verify_concurrency: env_usize(
                "INDEXER_VERIFY_CONCURRENCY",
                cpus.saturating_sub(1).max(1),
                1,
                64,
            ),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct FetchWork {
    start: u32,
    count: u32,
    attempt: u32,
}

struct TickOutcome {
    tick: u32,
    attempt: u32,
    outcome: Result<VerifiedTick>,
}

struct FetchResult {
    start: u32,
    count: u32,
    attempt: u32,
    outcomes: Vec<TickOutcome>,
    batch_error: Option<Error>,
}

struct RetryRequest {
    start: u32,
    count: u32,
    attempt: u32,
    delay: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchAction {
    Retry,
    SkipTo(u32),
}

/// Bounded out-of-order buffer that the ordered writer drains from `next`.
struct OrderedBuffer<T> {
    entries: BTreeMap<u32, T>,
    capacity: usize,
}

impl<T> OrderedBuffer<T> {
    fn new(capacity: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            capacity: capacity.max(1),
        }
    }

    fn insert(&mut self, tick: u32, value: T) {
        self.entries.insert(tick, value);
    }

    fn remove_next(&mut self, tick: u32) -> Option<T> {
        self.entries.remove(&tick)
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_full(&self) -> bool {
        self.entries.len() >= self.capacity
    }

    /// Drops every buffered result below `tick`, returning how many were
    /// discarded after the writer moved its cursor forward.
    fn retain_from(&mut self, tick: u32) -> usize {
        let stale = self
            .entries
            .keys()
            .take_while(|buffered| **buffered < tick)
            .count();
        for _ in 0..stale {
            if let Some(key) = self.entries.keys().next().copied() {
                self.entries.remove(&key);
            }
        }
        stale
    }
}

fn dispatch_upper_bound(next_write: u32, head_tick: u32, window: usize) -> Option<u32> {
    let head_tick = head_tick.saturating_sub(LIVE_HEAD_LAG);
    if head_tick < next_write {
        return None;
    }
    let span = u32::try_from(window.saturating_sub(1)).unwrap_or(u32::MAX);
    Some(head_tick.min(next_write.saturating_add(span)))
}

fn should_flush(
    buffered: usize,
    write_batch: usize,
    elapsed: Duration,
    write_flush: Duration,
) -> bool {
    // The timer must not be starved by a steady result stream. Otherwise a
    // backfill would expose the head only every `write_batch` ticks, which
    // looks like long stalls between bursts even though fetching is steady.
    buffered >= write_batch || (buffered > 0 && elapsed >= write_flush)
}

fn next_after_flush(current: u32, persisted: Option<u32>) -> u32 {
    match persisted {
        Some(tick) if tick >= current => tick.saturating_add(1),
        _ => current,
    }
}

/// Epoch start ticks keyed by epoch, ordered by epoch.
///
type EpochBoundaries = BTreeMap<u16, u32>;

/// A restart-based epoch boundary can skip a range of tick numbers. Qubic
/// publishes a new `TICK` when the network restarts from scratch, so the first
/// tick of the new epoch is not `old_last + 1`. The dispatcher stops feeding
/// ticks beyond the old epoch's last observed tick, and the writer jumps over
/// the gap once the old tail is drained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EpochRollover {
    old_epoch: u16,
    old_last: u32,
    new_epoch: u16,
    new_initial: u32,
}

impl EpochRollover {
    fn contains(self, tick: u32) -> bool {
        tick > self.old_last && tick < self.new_initial
    }
}

/// Build the skip range when a peer moves the current epoch's published start
/// forward. The previous start was provisional, so its tick numbers must not be
/// fetched or indexed.
fn moved_boundary_rollover(
    epoch: u16,
    previous_initial: u32,
    previous_epoch_last: Option<u32>,
    new_initial: u32,
) -> Option<EpochRollover> {
    if new_initial <= previous_initial {
        return None;
    }
    Some(EpochRollover {
        old_epoch: epoch.saturating_sub(1),
        old_last: previous_epoch_last.unwrap_or_else(|| previous_initial.saturating_sub(1)),
        new_epoch: epoch,
        new_initial,
    })
}

/// Resolve the epoch that owns `tick` from observed boundaries.
///
/// Returns `None` when the tick predates every observed epoch start. Callers
/// must skip such ticks instead of guessing an epoch from a fixed tick count.
fn epoch_for_tick(boundaries: &EpochBoundaries, tick: u32) -> Option<u16> {
    let mut entries = boundaries
        .iter()
        .map(|(epoch, initial_tick)| (*epoch, *initial_tick))
        .collect::<Vec<_>>();
    entries.sort_by_key(|(_, initial_tick)| *initial_tick);
    let index = entries
        .iter()
        .rposition(|(_, initial_tick)| *initial_tick <= tick)?;
    let (epoch, initial_tick) = entries[index];
    // A gap between observed boundaries means ticks in between could belong to
    // an epoch we never observed. Refuse to guess instead of assigning them to
    // the preceding epoch. The boundary tick itself is unambiguous.
    if let Some((next_epoch, _)) = entries.get(index + 1) {
        if *next_epoch != epoch.saturating_add(1) && tick != initial_tick {
            return None;
        }
    }
    Some(epoch)
}

fn boundary_map(boundaries: Vec<store::EpochBoundary>) -> EpochBoundaries {
    boundaries
        .into_iter()
        .map(|boundary| (boundary.epoch, boundary.initial_tick))
        .collect()
}

/// Close open epochs below `head` whose successor boundary is known.
///
async fn close_stale_epochs<S: Store>(store: &S, boundaries: &EpochBoundaries, head: &Head) {
    let Ok(metadata) = store.epoch_metadata().await else {
        return;
    };
    let mut starts = boundaries
        .iter()
        .map(|(epoch, initial_tick)| (*epoch, *initial_tick))
        .collect::<Vec<_>>();
    starts.sort_by_key(|(_, initial_tick)| *initial_tick);
    let mut open = metadata
        .into_iter()
        .filter(|row| !row.closed && row.epoch < head.epoch)
        .collect::<Vec<_>>();
    open.sort_by_key(|row| row.epoch);
    for row in open {
        let successor = starts
            .iter()
            .find(|(epoch, _)| *epoch == row.epoch.saturating_add(1))
            .map(|(_, initial_tick)| *initial_tick);
        if successor.is_none() {
            continue;
        }
        let last_tick = match store.last_tick_for_epoch(row.epoch).await {
            Ok(Some(tick)) => tick,
            Ok(None) => continue,
            Err(error) => {
                tracing::warn!(epoch = row.epoch, %error, "last indexed tick was not read");
                continue;
            }
        };
        if last_tick < row.initial_tick {
            continue;
        }
        if let Err(error) = store
            .close_epoch(row.epoch, row.initial_tick, last_tick)
            .await
        {
            tracing::warn!(epoch = row.epoch, %error, "stale epoch was not closed");
        }
    }
}

pub struct Indexer<S: Store + 'static> {
    store: Arc<S>,
    pool: NodePool,
    config: IndexerConfig,
    archive_policy: ArchivePolicy,
    /// Absolute identity snapshots as of the last persisted tick.
    identities: HashMap<[u8; 32], IdentityRow>,
}

impl<S: Store + 'static> Indexer<S> {
    pub fn new(store: Arc<S>, mut config: IndexerConfig) -> Result<Self> {
        if config.peers.is_empty() {
            return Err(Error::Config("at least one peer is required".into()));
        }
        // Live ingestion must fail over fast. The API can afford the longer
        // defaults for one-off reads; the indexer cannot let a dead peer hold
        // a worker for 8 seconds while the chain advances.
        for peer in &mut config.peers {
            peer.connect_timeout = peer.connect_timeout.min(Duration::from_millis(1_500));
            peer.timeout = peer.timeout.min(Duration::from_secs(4));
            peer.retries = 0;
        }
        let pool = NodePool::new(config.peers.clone());
        Ok(Self {
            store,
            pool,
            config,
            archive_policy: ArchivePolicy::from_env(),
            identities: HashMap::new(),
        })
    }

    /// Runs until `cancel` is triggered.
    pub async fn run(&mut self, cancel: CancellationToken) -> Result<()> {
        self.store.init().await?;
        let mut peer = self.pool.connect_best().await?;
        let mut head = peer.head().await?;
        let mut current = peer.config().addr;
        let mut probes = tokio::task::JoinSet::new();
        for config in self.config.peers.clone() {
            if config.addr == current {
                continue;
            }
            probes.spawn(async move {
                let mut candidate = Peer::connect(config).await.ok()?;
                let candidate_head = candidate.head().await.ok()?;
                Some((candidate, candidate_head))
            });
        }
        while let Some(joined) = probes.join_next().await {
            let Ok(Some((candidate, candidate_head))) = joined else {
                continue;
            };
            let newer = candidate_head.epoch > head.epoch
                || (candidate_head.epoch == head.epoch && candidate_head.tick > head.tick);
            if newer {
                tracing::info!(
                    from = %current,
                    to = %candidate.config().addr,
                    old_head = head.tick,
                    new_head = candidate_head.tick,
                    "using more advanced peer head"
                );
                current = candidate.config().addr;
                peer = candidate;
                head = candidate_head;
            }
        }
        let mut next = self.next_tick(&head).await?;
        let tuning = PipelineTuning::from_env();

        // Load observed boundaries and record this head before any worker can
        // see it. A tick is only assigned to an epoch whose boundary is known.
        let mut boundaries = match self.store.epoch_boundaries().await {
            Ok(rows) => boundary_map(rows),
            Err(error) => {
                tracing::warn!(%error, "epoch boundaries could not be loaded");
                EpochBoundaries::new()
            }
        };
        // A provisional boundary may already have been persisted before a
        // restart. Keep the later value so the writer never moves backwards
        // into a tick range the network has abandoned.
        let stored_initial = boundaries.get(&head.epoch).copied();
        let initial_tick =
            stored_initial.map_or(head.initial_tick, |stored| stored.max(head.initial_tick));
        if initial_tick != head.initial_tick {
            tracing::warn!(
                epoch = head.epoch,
                reported_initial = head.initial_tick,
                stored_initial,
                using_initial = initial_tick,
                "stored epoch boundary is ahead of the peer head"
            );
        }
        boundaries.insert(head.epoch, initial_tick);
        next = next.max(initial_tick);
        if let Err(error) = self.store.write_epoch_start(head.epoch, initial_tick).await {
            tracing::warn!(epoch = head.epoch, %error, "epoch start was not persisted");
        }
        close_stale_epochs(self.store.as_ref(), &boundaries, &head).await;
        let shared_boundaries = Arc::new(AsyncRwLock::new(boundaries));

        tracing::info!(
            epoch = head.epoch,
            head = head.tick,
            initial_tick = head.initial_tick,
            next,
            fetch_concurrency = tuning.fetch_concurrency,
            fetch_window = tuning.fetch_window,
            write_batch = tuning.write_batch,
            write_flush_ms = tuning.write_flush.as_millis() as u64,
            verify_concurrency = tuning.verify_concurrency,
            "indexer started"
        );

        // Seed the shared state with the initial computor set. The coordinator
        // owns refreshes and epoch rollovers from here on.
        let mut initial_computors = HashMap::new();
        match peer.computors().await {
            Ok(computors) => {
                if let Err(error) = self
                    .store
                    .write_computors(computors.epoch, &computors, Utc::now())
                    .await
                {
                    tracing::warn!(%error, "computor set was not persisted");
                }
                initial_computors.insert(computors.epoch, computors);
            }
            Err(error) => {
                tracing::warn!(%error, "initial computor fetch failed; retrying in background");
            }
        }

        let shared_head = Arc::new(AsyncRwLock::new(head.clone()));
        let shared_computors = Arc::new(AsyncRwLock::new(initial_computors));
        let rollover = Arc::new(AsyncRwLock::new(None));
        let next_write = Arc::new(AtomicU32::new(next));
        let pending = Arc::new(AtomicUsize::new(0));
        let pipeline_cancel = CancellationToken::new();
        let channel_capacity = tuning.fetch_window.max(tuning.fetch_concurrency);

        let (work_tx, work_rx) = mpsc::channel::<FetchWork>(channel_capacity);
        let work_rx = Arc::new(AsyncMutex::new(work_rx));
        let (result_tx, mut result_rx) = mpsc::channel::<FetchResult>(channel_capacity);
        let (retry_tx, retry_rx) = mpsc::channel::<RetryRequest>(channel_capacity);
        let mut peer_configs = self.pool.peer_configs().to_vec();
        if std::env::var("INDEXER_DISCOVER_PEERS").is_ok_and(|value| value == "true") {
            let mut seen = peer_configs
                .iter()
                .map(|config| config.addr)
                .collect::<HashSet<_>>();
            let before = peer_configs.len();
            for address in peer.public_peers() {
                if peer_configs.len() >= 32 {
                    break;
                }
                let socket = std::net::SocketAddr::from((address.ip, 21841));
                if socket.ip().is_unspecified() || !seen.insert(socket) {
                    continue;
                }
                peer_configs.push(
                    PeerConfig::new(socket)
                        .with_connect_timeout(Duration::from_secs(2))
                        .with_timeout(Duration::from_secs(4))
                        .with_retries(0),
                );
            }
            tracing::info!(
                configured = before,
                discovered = peer_configs.len() - before,
                total = peer_configs.len(),
                "peer discovery complete"
            );
            self.pool.set_peers(peer_configs.clone());
        }

        let head_poll = Duration::from_millis(100).min(self.config.poll_interval);
        let prune_keep = std::env::var("PRUNE_KEEP_EPOCHS")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .filter(|keep| *keep > 0);
        let coordinator = tokio::spawn(coordinate(
            peer,
            peer_configs.to_vec(),
            shared_head.clone(),
            shared_computors.clone(),
            shared_boundaries.clone(),
            rollover.clone(),
            self.store.clone(),
            prune_keep,
            pipeline_cancel.clone(),
            head_poll,
        ));
        let dispatcher = tokio::spawn(dispatch_loop(
            next,
            shared_head.clone(),
            rollover.clone(),
            next_write.clone(),
            pending.clone(),
            work_tx,
            retry_rx,
            pipeline_cancel.clone(),
            tuning.fetch_window,
            tuning.fetch_batch,
            tuning.batch_min_lag,
        ));
        let verify_semaphore = Arc::new(tokio::sync::Semaphore::new(tuning.verify_concurrency));

        let mut workers = Vec::with_capacity(tuning.fetch_concurrency);
        for _ in 0..tuning.fetch_concurrency {
            workers.push(tokio::spawn(fetch_worker(
                None,
                self.pool.clone(),
                work_rx.clone(),
                result_tx.clone(),
                shared_head.clone(),
                shared_computors.clone(),
                shared_boundaries.clone(),
                self.config.verify_signatures,
                verify_semaphore.clone(),
                pipeline_cancel.clone(),
            )));
        }
        drop(result_tx);

        let mut buffer = OrderedBuffer::new(tuning.fetch_window);
        let mut consume_next = next;
        let mut backoff = Duration::from_millis(250);
        let mut since_progress = 0u64;
        let mut consecutive_failures = 0u32;
        let mut write_batches = Vec::<TickBatch>::with_capacity(tuning.write_batch);
        let mut archive_tasks =
            Vec::<Vec<tokio::task::JoinHandle<archive::Result<()>>>>::with_capacity(
                tuning.write_batch,
            );
        let archive_semaphore = self
            .config
            .archive
            .as_ref()
            .map(|_| Arc::new(tokio::sync::Semaphore::new(tuning.archive_concurrency)));
        let mut stored_epochs = HashSet::new();
        let mut last_flush = Instant::now();
        let mut run_result = Ok(());

        'writer: loop {
            if let Some(state) = *rollover.read().await {
                if state.contains(consume_next) {
                    tracing::info!(
                        from = consume_next,
                        to = state.new_initial,
                        old_epoch = state.old_epoch,
                        new_epoch = state.new_epoch,
                        "skipping restart tick gap"
                    );
                    next = state.new_initial;
                    consume_next = next;
                    next_write.store(next, Ordering::Release);
                    let dropped = buffer.retain_from(consume_next);
                    pending.fetch_sub(dropped, Ordering::Release);
                }
            }
            let (received, timed_out) = tokio::select! {
                _ = cancel.cancelled() => break,
                _ = pipeline_cancel.cancelled() => break,
                result = result_rx.recv() => (Some(result), false),
                _ = tokio::time::sleep(tuning.write_flush) => (None, true),
            };
            if let Some(result) = received {
                let Some(result) = result else {
                    break;
                };
                if let Some(error) = result.batch_error {
                    pending.fetch_sub(result.count as usize, Ordering::Release);
                    tracing::warn!(
                        start = result.start,
                        count = result.count,
                        %error,
                        "batch fetch failed; retrying"
                    );
                    retry_tx
                        .send(RetryRequest {
                            start: result.start,
                            count: result.count,
                            attempt: result.attempt.saturating_add(1),
                            delay: Duration::from_millis(250),
                        })
                        .await
                        .ok();
                    continue 'writer;
                }
                for tick in result.outcomes {
                    if tick.tick < consume_next {
                        pending.fetch_sub(1, Ordering::Release);
                        continue;
                    }
                    buffer.insert(tick.tick, tick);
                }

                while let Some(result) = buffer.remove_next(consume_next) {
                    pending.fetch_sub(1, Ordering::Release);
                    let head_snapshot = shared_head.read().await.clone();
                    let fetched = match result.outcome {
                        Ok(fetched) => fetched,
                        Err(error) => {
                            let action = self
                                .handle_fetch_error(
                                    consume_next,
                                    result.attempt,
                                    error,
                                    &head_snapshot,
                                    &retry_tx,
                                    &mut backoff,
                                    &mut consecutive_failures,
                                )
                                .await;
                            if let FetchAction::SkipTo(skip_to) = action {
                                tracing::warn!(
                                    from = consume_next,
                                    to = skip_to,
                                    "skipping unavailable ticks"
                                );
                                next = skip_to;
                                consume_next = next;
                                next_write.store(next, Ordering::Release);
                                let dropped = buffer.retain_from(consume_next);
                                pending.fetch_sub(dropped, Ordering::Release);
                            }
                            continue 'writer;
                        }
                    };

                    let verified = fetched;

                    let mut archives = Vec::new();
                    if let (Some(archive), Some(semaphore)) =
                        (self.config.archive.clone(), archive_semaphore.clone())
                    {
                        let tick = verified.tick.tick;
                        let epoch = verified.tick.epoch;
                        let raw = archive_policy::raw_bundle(&verified, &self.archive_policy);
                        archives.push(tokio::spawn(async move {
                            let _permit = semaphore
                                .acquire_owned()
                                .await
                                .expect("archive semaphore is never closed");
                            let started = Instant::now();
                            let result = archive::put_bundle(archive.as_ref(), &raw).await;
                            tracing::debug!(
                                tick,
                                epoch,
                                ok = result.is_ok(),
                                elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0,
                                "raw bundle archived"
                            );
                            result.map(|_| ())
                        }));
                    }

                    let build_started = Instant::now();
                    let batch = match self.build_batch(&verified).await {
                        Ok(batch) => batch,
                        Err(error) => {
                            run_result = Err(error);
                            break 'writer;
                        }
                    };
                    tracing::debug!(
                        tick = consume_next,
                        elapsed_ms = build_started.elapsed().as_secs_f64() * 1_000.0,
                        "tick batch built"
                    );
                    let epoch = verified.tick.epoch;
                    if stored_epochs.insert(epoch) {
                        match self
                            .store
                            .write_computors(epoch, &verified.computors, Utc::now())
                            .await
                        {
                            Ok(()) => {
                                // The sidecar is written once per epoch and is
                                // idempotent. Await it so failures are retried
                                // on the next tick, or stop the writer when the
                                // archive is configured to fail closed.
                                if let (Some(archive), Some(semaphore)) =
                                    (self.config.archive.clone(), archive_semaphore.clone())
                                {
                                    let bytes = verified.computors.encode();
                                    let permit = semaphore
                                        .acquire_owned()
                                        .await
                                        .expect("archive semaphore is never closed");
                                    let result =
                                        archive::put_computors(archive.as_ref(), epoch, &bytes)
                                            .await;
                                    drop(permit);
                                    if let Err(error) = result {
                                        if self.config.archive_fail_closed {
                                            run_result = Err(Error::Archive(error));
                                            break 'writer;
                                        }
                                        stored_epochs.remove(&epoch);
                                        tracing::warn!(
                                            epoch,
                                            %error,
                                            "computor sidecar was not archived; retrying on the next tick"
                                        );
                                    }
                                }
                            }
                            Err(error) => {
                                stored_epochs.remove(&epoch);
                                tracing::warn!(epoch, %error, "computor set was not persisted");
                            }
                        }
                    }
                    for event in &verified.logs {
                        match event.custom_op_code() {
                            Some(qubic::logs::CUSTOM_OP_END_EPOCH) => tracing::info!(
                                epoch = verified.tick.epoch,
                                tick = verified.tick.tick,
                                log_id = event.log_id,
                                "epoch end event observed"
                            ),
                            Some(qubic::logs::CUSTOM_OP_START_EPOCH) => tracing::info!(
                                epoch = verified.tick.epoch,
                                tick = verified.tick.tick,
                                log_id = event.log_id,
                                "epoch start event observed"
                            ),
                            _ => {}
                        }
                    }

                    write_batches.push(batch);
                    archive_tasks.push(archives);
                    consume_next = consume_next.saturating_add(1);
                    consecutive_failures = 0;
                    backoff = Duration::from_millis(250);

                    // While ticks are flowing, fill the batch: per-tick inserts
                    // are the bottleneck. The timer branch below only flushes
                    // when the result channel went idle.
                    let flush_due = should_flush(
                        write_batches.len(),
                        tuning.write_batch,
                        last_flush.elapsed(),
                        tuning.write_flush,
                    );
                    if flush_due {
                        let stored = loop {
                            match self
                                .flush_batches(&mut write_batches, &mut archive_tasks, &mut next)
                                .await
                            {
                                Ok(stored) => break stored,
                                Err(Error::Archive(error)) => {
                                    run_result = Err(Error::Archive(error));
                                    break 'writer;
                                }
                                Err(error) => {
                                    if cancel.is_cancelled() {
                                        run_result = Err(error);
                                        break 'writer;
                                    }
                                    tracing::error!(
                                        tick = next,
                                        %error,
                                        "batched store write failed; retrying same range"
                                    );
                                    tokio::time::sleep(Duration::from_secs(2)).await;
                                }
                            }
                        };
                        last_flush = Instant::now();
                        next_write.store(next, Ordering::Release);
                        since_progress += stored as u64;
                        if since_progress >= self.config.progress_every {
                            since_progress = 0;
                            let head_snapshot = shared_head.read().await.clone();
                            tracing::info!(
                                tick = next.saturating_sub(1),
                                epoch = head_snapshot.epoch,
                                stored,
                                lag = head_snapshot.tick.saturating_sub(next),
                                "indexed"
                            );
                        }
                    }
                }
            }

            // A bounded window means this should not happen; if it does, stop
            // accepting new work until the missing next tick arrives.
            if buffer.is_full() {
                tracing::warn!(next, buffered = buffer.len(), "ordered buffer is full");
            }

            if timed_out
                && !write_batches.is_empty()
                && should_flush(
                    write_batches.len(),
                    tuning.write_batch,
                    last_flush.elapsed(),
                    tuning.write_flush,
                )
            {
                let stored = loop {
                    match self
                        .flush_batches(&mut write_batches, &mut archive_tasks, &mut next)
                        .await
                    {
                        Ok(stored) => break stored,
                        Err(Error::Archive(error)) => {
                            run_result = Err(Error::Archive(error));
                            break 'writer;
                        }
                        Err(error) => {
                            if cancel.is_cancelled() {
                                run_result = Err(error);
                                break 'writer;
                            }
                            tracing::error!(
                                tick = next,
                                %error,
                                "timed batch write failed; retrying same range"
                            );
                            tokio::time::sleep(Duration::from_secs(2)).await;
                        }
                    }
                };
                last_flush = Instant::now();
                next_write.store(next, Ordering::Release);
                since_progress += stored as u64;
                if since_progress >= self.config.progress_every {
                    since_progress = 0;
                    let head_snapshot = shared_head.read().await.clone();
                    tracing::info!(
                        tick = next.saturating_sub(1),
                        epoch = head_snapshot.epoch,
                        stored,
                        lag = head_snapshot.tick.saturating_sub(next),
                        "indexed"
                    );
                }
            }
        }

        // Flush on graceful shutdown. Failures here must not skip the range;
        // the next run resumes from the persisted cursor.
        if !write_batches.is_empty() && run_result.is_ok() {
            if let Err(error) = self
                .flush_batches(&mut write_batches, &mut archive_tasks, &mut next)
                .await
            {
                tracing::error!(tick = next, %error, "shutdown batch write failed");
                run_result = Err(error);
            }
        }

        pipeline_cancel.cancel();
        drop(result_rx);
        drop(retry_tx);
        let _ = coordinator.await;
        let _ = dispatcher.await;
        for worker in workers {
            let _ = worker.await;
        }
        if run_result.is_ok() {
            tracing::info!(next, "indexer stopping");
        }
        run_result
    }

    async fn flush_batches(
        &self,
        batches: &mut Vec<TickBatch>,
        archives: &mut Vec<Vec<tokio::task::JoinHandle<archive::Result<()>>>>,
        next: &mut u32,
    ) -> Result<usize> {
        if batches.is_empty() {
            return Ok(0);
        }
        for tasks in archives.iter_mut() {
            for task in tasks.drain(..) {
                match task.await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) if self.config.archive_fail_closed => {
                        return Err(Error::Archive(error));
                    }
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "raw archive failed; continuing with indexed rows");
                    }
                    Err(error) if self.config.archive_fail_closed => {
                        return Err(Error::Archive(archive::Error::InvalidBundle(format!(
                            "archive task failed: {error}"
                        ))));
                    }
                    Err(error) => {
                        tracing::warn!(%error, "raw archive task failed; continuing");
                    }
                }
            }
        }
        let started = Instant::now();
        let outcomes = self.store.write_ticks(batches).await?;
        let stored = batches.len();
        *next = next_after_flush(*next, outcomes.iter().map(|outcome| outcome.tick).max());
        batches.clear();
        archives.clear();
        tracing::debug!(
            ticks = stored,
            next = *next,
            elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0,
            "tick batches stored"
        );
        Ok(stored)
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_fetch_error(
        &self,
        next: u32,
        attempt: u32,
        error: Error,
        head: &Head,
        retry: &mpsc::Sender<RetryRequest>,
        backoff: &mut Duration,
        consecutive_failures: &mut u32,
    ) -> FetchAction {
        if let Error::Node(node::Error::NotSynchronized {
            head: peer_head, ..
        }) = &error
        {
            tracing::warn!(
                tick = next,
                peer_head = *peer_head,
                "peer is not synchronized to the requested tick"
            );
            *consecutive_failures = 0;
            retry
                .send(RetryRequest {
                    start: next,
                    count: 1,
                    attempt: attempt.saturating_add(1),
                    delay: self.config.poll_interval,
                })
                .await
                .ok();
            return FetchAction::Retry;
        }

        if matches!(
            &error,
            Error::Node(node::Error::EndResponse(
                qubic::wire::MessageType::RequestTickData
            ))
        ) {
            *consecutive_failures += 1;
            let near_head = next.saturating_add(LIVE_HEAD_MARGIN) >= head.tick;
            if !near_head && attempt.saturating_add(1) >= self.config.max_tick_attempts {
                tracing::error!(
                    tick = next,
                    attempts = attempt.saturating_add(1),
                    "tick data is unavailable from every peer, skipping tick"
                );
                *consecutive_failures = 0;
                return FetchAction::SkipTo(next.saturating_add(1));
            }
            retry
                .send(RetryRequest {
                    start: next,
                    count: 1,
                    attempt: attempt.saturating_add(1),
                    delay: if near_head {
                        Duration::from_secs(1)
                    } else {
                        Duration::ZERO
                    },
                })
                .await
                .ok();
            return FetchAction::Retry;
        }

        if let Error::Node(node::Error::ArchiveNotReady(_)) = &error {
            retry
                .send(RetryRequest {
                    start: next,
                    count: 1,
                    attempt: attempt.saturating_add(1),
                    delay: Duration::from_millis(500),
                })
                .await
                .ok();
            return FetchAction::Retry;
        }

        if let Error::Node(node::Error::Archive(_)) = &error {
            *consecutive_failures += 1;
            if attempt.saturating_add(1) >= self.config.max_tick_attempts.saturating_mul(5) {
                tracing::error!(
                    tick = next,
                    attempts = attempt.saturating_add(1),
                    "archive tick data is unavailable, skipping tick"
                );
                *consecutive_failures = 0;
                return FetchAction::SkipTo(next.saturating_add(1));
            }
            retry
                .send(RetryRequest {
                    start: next,
                    count: 1,
                    attempt: attempt.saturating_add(1),
                    delay: *backoff,
                })
                .await
                .ok();
            *backoff = (*backoff * 2).min(Duration::from_secs(2));
            return FetchAction::Retry;
        }

        if let Error::Node(node::Error::Verification(message)) = &error {
            if message.starts_with("unknown epoch boundary") {
                tracing::warn!(tick = next, %message, "skipping a tick with no known epoch");
                *consecutive_failures = 0;
                return FetchAction::SkipTo(next.saturating_add(1));
            }
            if message.starts_with("no votes for tick")
                || message.starts_with("tick data digest does not match")
            {
                tracing::debug!(tick = next, "waiting for complete quorum votes");
                retry
                    .send(RetryRequest {
                        start: next,
                        count: 1,
                        attempt: attempt.saturating_add(1),
                        delay: Duration::from_secs(1),
                    })
                    .await
                    .ok();
                return FetchAction::Retry;
            }
        }

        if let Error::Verification(message) = &error {
            tracing::error!(
                tick = next,
                %message,
                "verification failed, refusing to index"
            );
            *consecutive_failures += 1;
            if next < head.initial_tick && *consecutive_failures >= 3 {
                tracing::warn!(
                    tick = next,
                    initial_tick = head.initial_tick,
                    skipped_from = next,
                    skipped_to = head.initial_tick,
                    "previous epoch tail did not verify, skipping range"
                );
                *consecutive_failures = 0;
                return FetchAction::SkipTo(head.initial_tick);
            }
            retry
                .send(RetryRequest {
                    start: next,
                    count: 1,
                    attempt: attempt.saturating_add(1),
                    delay: Duration::from_millis(50),
                })
                .await
                .ok();
            return FetchAction::Retry;
        }

        if matches!(
            &error,
            Error::Node(
                node::Error::Io(_) | node::Error::Timeout(_) | node::Error::RetriesExhausted { .. }
            )
        ) {
            // A reset or timeout is a peer problem, not a reason to pause the
            // tick. Retry immediately and let health-aware selection pick a
            // different peer.
            *consecutive_failures += 1;
            retry
                .send(RetryRequest {
                    start: next,
                    count: 1,
                    attempt: attempt.saturating_add(1),
                    delay: Duration::ZERO,
                })
                .await
                .ok();
            return FetchAction::Retry;
        }

        tracing::warn!(tick = next, %error, "tick fetch failed, retrying");
        *consecutive_failures += 1;
        retry
            .send(RetryRequest {
                start: next,
                count: 1,
                attempt: attempt.saturating_add(1),
                delay: *backoff,
            })
            .await
            .ok();
        *backoff = (*backoff * 2).min(Duration::from_secs(1));
        if next < head.initial_tick && *consecutive_failures >= 5 {
            tracing::warn!(
                tick = next,
                initial_tick = head.initial_tick,
                skipped_from = next,
                skipped_to = head.initial_tick,
                "previous epoch tail is unavailable, skipping range"
            );
            *consecutive_failures = 0;
            return FetchAction::SkipTo(head.initial_tick);
        }
        FetchAction::Retry
    }

    async fn next_tick(&self, head: &Head) -> Result<u32> {
        let cursor = self.store.ingest_cursor().await?;
        let next = match cursor.last_indexed_tick {
            Some(tick) => tick.saturating_add(1),
            None => self.config.start_tick.unwrap_or(head.initial_tick),
        };
        // An epoch rollover can leave the stored cursor below the new epoch's
        // first tick, so move forward. Never clamp backwards: a stale peer head
        // must not make us refetch ticks that are already persisted.
        Ok(next.max(head.initial_tick))
    }

    async fn build_batch(&mut self, verified: &VerifiedTick) -> Result<TickBatch> {
        let timestamp = verified_timestamp(verified);
        let mut batch = TickBatch::new(
            verified.tick.epoch,
            verified.tick.tick,
            timestamp,
            verified.tick.computor_index,
            verified.verification.as_str(),
        );
        batch.signature = verified.tick.signature;
        batch.tick_digest = verified.tick_data.digest();

        let mut error_txs = HashMap::<u16, bool>::new();
        let mut tx_hashes = HashMap::<u16, String>::new();

        // Hash every fetched transaction once instead of re-encoding it for
        // each non-empty digest slot.
        let mut transactions_by_hash = HashMap::<[u8; 32], &Transaction>::new();
        for transaction in &verified.transactions {
            transactions_by_hash
                .entry(transaction.hash())
                .or_insert(transaction);
        }

        for (index, digest) in verified.tick_data.non_empty_transactions() {
            if let Some(transaction) = transactions_by_hash.get(digest) {
                let hash = qubic::encode_tx_hash(digest);
                tx_hashes.insert(index as u16, hash.clone());
                batch.transactions.push(TransactionRow {
                    epoch: verified.tick.epoch,
                    tick: verified.tick.tick,
                    tx_index: index as u16,
                    hash,
                    source: PublicKey(transaction.source).to_identity(),
                    destination: PublicKey(transaction.destination).to_identity(),
                    amount: transaction.amount,
                    input_type: transaction.input_type,
                    input_data: hex::encode(&transaction.input),
                    status: "success".into(),
                    timestamp,
                });
            }
        }

        let log_slots = LogSlotLookup::new(&verified.log_ranges);
        for event in &verified.logs {
            let tx_index = log_slots.slot(event.log_id);
            let (tx_hash, tx_index_i16) = match tx_index {
                Some(index) if index < TX_PER_TICK => (
                    tx_hashes
                        .get(&(index as u16))
                        .cloned()
                        .unwrap_or_else(|| format!("slot:{:?}", index)),
                    index as i16,
                ),
                Some(index) => (format!("special:{}:{}", verified.tick.tick, index), -1),
                None => (String::new(), -1),
            };
            if event.log_type == CONTRACT_ERROR_MESSAGE {
                if let Some(index) = tx_index {
                    error_txs.insert(index as u16, true);
                }
            }
            let participants = event.body().participant_identities();
            batch.events.push(EventRow {
                epoch: event.epoch,
                tick: event.tick,
                log_id: event.log_id,
                log_type: event.log_type as u16,
                type_name: event.type_name().to_string(),
                tx_hash: tx_hash.clone(),
                tx_index: tx_index_i16,
                body: event_body_json(event),
                log_digest: event.log_digest,
                timestamp,
            });
            for (identity, role) in participants {
                batch.event_participants.push(EventParticipantRow {
                    epoch: event.epoch,
                    tick: event.tick,
                    log_id: event.log_id,
                    identity,
                    role: role.to_string(),
                    tx_hash: tx_hash.clone(),
                    tx_index: tx_index_i16,
                    timestamp,
                });
            }
        }

        for transaction in &mut batch.transactions {
            if error_txs.contains_key(&transaction.tx_index) {
                transaction.status = "failed".into();
            }
        }

        let touched = self.apply_identity_changes(verified).await?;
        batch.identities = touched;
        batch.evidence = Some(quorum_evidence(verified, &self.config, timestamp));

        Ok(batch)
    }

    async fn apply_identity_changes(
        &mut self,
        verified: &VerifiedTick,
    ) -> Result<Vec<IdentityRow>> {
        let tick = verified.tick.tick;
        let mut ops = Vec::<([u8; 32], IdentityChange)>::new();
        for event in &verified.logs {
            match event.body() {
                EventBody::QuTransfer { from, to, amount } => {
                    for (key, incoming) in [(from, false), (to, true)] {
                        if key == [0u8; 32] {
                            continue;
                        }
                        ops.push((key, IdentityChange::Transfer { incoming, amount }));
                    }
                }
                EventBody::Burning { from, amount, .. } => {
                    if from != [0u8; 32] {
                        ops.push((from, IdentityChange::Burn { amount }));
                    }
                }
                EventBody::DustBurning { burns } => {
                    for (key, amount) in burns {
                        if key == [0u8; 32] {
                            continue;
                        }
                        ops.push((key, IdentityChange::Burn { amount }));
                    }
                }
                _ => {}
            }
        }

        // Load every first-seen identity for this tick in one query. Doing one
        // identity lookup per transfer turns a busy tick into dozens of serial
        // ClickHouse round trips.
        let mut missing = BTreeMap::<[u8; 32], String>::new();
        for (key, _) in &ops {
            if !self.identities.contains_key(key) {
                missing
                    .entry(*key)
                    .or_insert_with(|| PublicKey(*key).to_identity());
            }
        }
        if !missing.is_empty() {
            let identities = missing.values().cloned().collect::<Vec<_>>();
            let loaded = self.store.identities_by_id(&identities).await?;
            for (key, identity) in missing {
                if let Some(row) = loaded.get(&identity) {
                    self.identities.insert(key, row.clone());
                }
            }
        }

        let mut touched = BTreeMap::<[u8; 32], IdentityRow>::new();
        for (key, change) in ops {
            let row = touched
                .get(&key)
                .cloned()
                .or_else(|| self.identities.get(&key).cloned())
                .unwrap_or_else(|| empty_identity(key));
            let mut row = row;
            match change {
                IdentityChange::Transfer { incoming, amount } => {
                    apply_transfer(&mut row, incoming, amount, tick)
                }
                IdentityChange::Burn { amount } => apply_burn(&mut row, amount, tick),
            }
            touched.insert(key, row);
        }

        let mut rows = Vec::with_capacity(touched.len());
        for (key, row) in touched {
            self.identities.insert(key, row.clone());
            rows.push(row);
        }
        Ok(rows)
    }
}

#[derive(Debug, Clone, Copy)]
enum IdentityChange {
    Transfer { incoming: bool, amount: i64 },
    Burn { amount: i64 },
}

fn empty_identity(key: [u8; 32]) -> IdentityRow {
    IdentityRow {
        identity: PublicKey(key).to_identity(),
        public_key: PublicKey(key).to_hex(),
        balance: 0,
        incoming_amount: 0,
        outgoing_amount: 0,
        transfer_count: 0,
        first_seen_tick: 0,
        last_seen_tick: 0,
    }
}

/// Apply one side of a QU transfer to a materialized identity row.
fn apply_transfer(row: &mut IdentityRow, incoming: bool, amount: i64, tick: u32) {
    if incoming {
        row.incoming_amount = row.incoming_amount.saturating_add(amount);
        row.balance = row.balance.saturating_add(amount);
    } else {
        row.outgoing_amount = row.outgoing_amount.saturating_add(amount);
        row.balance = row.balance.saturating_sub(amount);
    }
    row.transfer_count = row.transfer_count.saturating_add(1);
    touch_identity(row, tick);
}

/// Burns remove QU without a matching transfer, so they only increase the
/// outgoing side. They are not transfer events and do not bump the count.
fn apply_burn(row: &mut IdentityRow, amount: i64, tick: u32) {
    row.outgoing_amount = row.outgoing_amount.saturating_add(amount);
    row.balance = row.balance.saturating_sub(amount);
    touch_identity(row, tick);
}

fn touch_identity(row: &mut IdentityRow, tick: u32) {
    row.last_seen_tick = row.last_seen_tick.max(tick);
    row.first_seen_tick = if row.first_seen_tick == 0 {
        tick
    } else {
        row.first_seen_tick.min(tick)
    };
}

fn quorum_evidence(
    verified: &VerifiedTick,
    config: &IndexerConfig,
    timestamp: DateTime<Utc>,
) -> QuorumEvidenceRow {
    let tick = verified.tick.tick;
    let tick_data_digest = verified.tick_data.digest();
    let quorum_digest = if tick_data_digest == ZERO_DIGEST {
        ZERO_DIGEST
    } else {
        tick_data_digest
    };
    let mut unique = HashSet::new();
    let mut aligned = HashSet::new();
    for vote in &verified.votes {
        if vote.epoch != verified.tick.epoch || vote.tick != tick {
            continue;
        }
        unique.insert(vote.computor_index);
        if vote.transaction_digest == quorum_digest {
            aligned.insert(vote.computor_index);
        }
    }
    let required = required_quorum(&quorum_digest) as u32;
    let aligned_votes = aligned.len() as u32;
    QuorumEvidenceRow {
        epoch: verified.tick.epoch,
        tick,
        quorum_digest,
        tick_data_digest,
        required_votes: required,
        aligned_votes,
        fetched_votes: verified.votes.len() as u32,
        unique_votes: unique.len() as u32,
        verified_votes: if config.verify_signatures {
            aligned_votes.min(required)
        } else {
            0
        },
        verification: verified.verification.as_str().to_string(),
        warning_count: verified.warnings.len() as u32,
        warnings: serde_json::to_string(&verified.warnings).unwrap_or_default(),
        timestamp,
        version: u64::from(tick),
    }
}

/// Votes agree on the transaction digest. Per-computor salted header fields
/// differ by design, so the digest is the only field a quorum can share.
type VoteGroupKey = [u8; 32];

/// Verifies quorum, transaction inclusion, log digests, and optionally
/// SchnorrQ signatures.
pub fn verify_bundle(bundle: TickBundle, verify_signatures: bool) -> Result<VerifiedTick> {
    verify_bundle_with(bundle, verify_signatures, true)
}

/// Verify without spawning per-tick verification threads. The indexer runs
/// many verifications concurrently on its fetch workers, so an inner thread
/// pool only adds contention and startup cost.
pub fn verify_bundle_sequential(
    bundle: TickBundle,
    verify_signatures: bool,
) -> Result<VerifiedTick> {
    verify_bundle_with(bundle, verify_signatures, false)
}

fn verify_bundle_with(
    mut bundle: TickBundle,
    verify_signatures: bool,
    parallel_votes: bool,
) -> Result<VerifiedTick> {
    let tick_data = bundle.tick_data.clone();
    let tick_number = tick_data.tick;
    let epoch = if tick_data.epoch == 0 {
        bundle.tick.epoch
    } else {
        tick_data.epoch
    };
    let mut warnings = std::mem::take(&mut bundle.warnings);
    let mut complete = warnings.is_empty();

    if bundle.tick.tick != tick_number {
        return Err(Error::Verification(format!(
            "candidate tick {} does not match tick data {tick_number}",
            bundle.tick.tick
        )));
    }
    if tick_data.epoch != epoch {
        return Err(Error::Verification(format!(
            "tick data epoch {} does not match tick epoch {epoch}",
            tick_data.epoch
        )));
    }
    if bundle.computors.epoch != epoch {
        return Err(Error::Verification(format!(
            "computor set is for epoch {}, tick is epoch {epoch}",
            bundle.computors.epoch
        )));
    }

    let vote_started = Instant::now();

    // One vote per computor index. A peer that repeats a single valid vote
    // must not be able to reach quorum on its own, so deduplicate before
    // counting anything. Signatures are verified later, and only for groups
    // that can actually reach quorum.
    let mut by_computor = BTreeMap::<u16, &Tick>::new();
    for vote in &bundle.votes {
        if vote.epoch != epoch || vote.tick != tick_number {
            continue;
        }
        if vote.computor_index as usize >= COMPUTOR_COUNT {
            continue;
        }
        by_computor.entry(vote.computor_index).or_insert(vote);
    }
    if by_computor.is_empty() {
        return Err(Error::Verification(format!(
            "no votes for tick {tick_number}"
        )));
    }

    let unique_votes: Vec<&Tick> = by_computor.values().copied().collect();
    let (consensus_digest, consensus_count) = select_consensus_group(
        tick_number,
        &unique_votes,
        required_quorum,
        |group, required| {
            if verify_signatures {
                verify_vote_group(group, &bundle.computors, required, parallel_votes).valid
            } else {
                group.len()
            }
        },
    )?;
    let required = required_quorum(&consensus_digest);
    tracing::debug!(
        tick = tick_number,
        fetched = bundle.votes.len(),
        unique = unique_votes.len(),
        consensus = consensus_count,
        required,
        verify_signatures,
        elapsed_ms = vote_started.elapsed().as_secs_f64() * 1_000.0,
        "vote quorum verified"
    );

    if consensus_digest != ZERO_DIGEST && tick_data.digest() != consensus_digest {
        return Err(Error::Verification(format!(
            "tick data digest does not match quorum vote for tick {tick_number}"
        )));
    }
    if consensus_digest == ZERO_DIGEST && tick_data.non_empty_transactions().next().is_some() {
        return Err(Error::Verification(format!(
            "empty tick {tick_number} carries non-empty transaction slots"
        )));
    }

    if verify_signatures && consensus_digest != ZERO_DIGEST {
        verify_tick_data_signature(&bundle, &bundle.computors)?;
    }

    // The stored header is the requested candidate, not one computor's vote.
    // Votes carry per-computor salted fields that are not chain state.
    let candidate = bundle.tick.clone();
    debug_assert!(consensus_count >= required);

    let digest_to_index: HashMap<[u8; 32], u16> = tick_data
        .non_empty_transactions()
        .map(|(index, digest)| (*digest, index as u16))
        .collect();
    let tx_started = Instant::now();
    let mut seen = 0usize;
    for transaction in &bundle.transactions {
        if transaction.tick != tick_number {
            return Err(Error::Verification(format!(
                "transaction for tick {} appears in tick {tick_number}",
                transaction.tick
            )));
        }
        // Encode once: the hash covers the signature, the signable digest does
        // not, and both are slices of the same buffer.
        let bytes = transaction.encode();
        let hash = k12_32(&bytes);
        if !digest_to_index.contains_key(&hash) {
            complete = false;
            warnings.push(format!(
                "tick {tick_number}: transaction {} is not in the tick data digest set",
                qubic::encode_tx_hash(&hash)
            ));
            continue;
        }
        seen += 1;
        if verify_signatures
            && !verify_transaction_signature_bytes(
                &bytes,
                &transaction.source,
                &transaction.signature,
            )
        {
            return Err(Error::Verification(format!(
                "invalid transaction signature for {}",
                qubic::encode_tx_hash(&hash)
            )));
        }
    }
    if seen != digest_to_index.len() {
        complete = false;
        warnings.push(format!(
            "tick {tick_number}: fetched {seen}/{} transactions",
            digest_to_index.len()
        ));
    }
    tracing::debug!(
        tick = tick_number,
        transactions = bundle.transactions.len(),
        verify_signatures,
        elapsed_ms = tx_started.elapsed().as_secs_f64() * 1_000.0,
        "transactions verified"
    );

    let log_started = Instant::now();
    let log_slots = LogSlotLookup::new(&bundle.log_ranges);
    let mut last_log_id = None;
    for event in &bundle.logs {
        if event.tick != tick_number || event.epoch != epoch {
            return Err(Error::Verification(format!(
                "log {} is for epoch {}/tick {}, expected {epoch}/{tick_number}",
                event.log_id, event.epoch, event.tick
            )));
        }
        if !event.verify_digest() {
            return Err(Error::Verification(format!(
                "log {} digest mismatch",
                event.log_id
            )));
        }
        if let Some(previous) = last_log_id {
            if event.log_id <= previous {
                return Err(Error::Verification(format!(
                    "log ids are not strictly increasing at {}",
                    event.log_id
                )));
            }
        }
        last_log_id = Some(event.log_id);
        if log_slots.slot(event.log_id).is_none() {
            // Peers occasionally return a log that their own range table does
            // not cover (truncated fallback answers, stale ranges). The log
            // still has to carry this tick and a matching K12 digest, so keep
            // it and downgrade the tick instead of stalling the pipeline on a
            // peer bookkeeping mismatch. The slot is unknown, so the writer
            // stores an empty transaction link for this event.
            complete = false;
            warnings.push(format!(
                "tick {tick_number}: log {} lies outside the peer's declared log ranges",
                event.log_id
            ));
        }
    }
    let expected_logs = bundle.log_ranges.count();
    if (bundle.logs.len() as i64) != expected_logs {
        complete = false;
        warnings.push(format!(
            "tick {tick_number}: fetched {}/{} logs",
            bundle.logs.len(),
            expected_logs
        ));
    }
    tracing::debug!(
        tick = tick_number,
        logs = bundle.logs.len(),
        elapsed_ms = log_started.elapsed().as_secs_f64() * 1_000.0,
        "logs verified"
    );

    let verification = if complete && verify_signatures {
        Verification::Data
    } else {
        Verification::Quorum
    };

    Ok(VerifiedTick {
        head: bundle.head,
        tick: candidate,
        tick_data,
        votes: bundle.votes,
        transactions: bundle.transactions,
        computors: bundle.computors,
        log_ranges: bundle.log_ranges,
        logs: bundle.logs,
        verification,
        warnings,
    })
}

async fn connect_from(configs: &[PeerConfig], start: usize) -> Option<(Peer, usize)> {
    if configs.is_empty() {
        return None;
    }
    for offset in 0..configs.len() {
        let index = (start + offset) % configs.len();
        if let Ok(peer) = Peer::connect(configs[index].clone()).await {
            return Some((peer, index));
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
async fn coordinate<S: Store + 'static>(
    mut peer: Peer,
    configs: Vec<PeerConfig>,
    head: Arc<AsyncRwLock<Head>>,
    computors: Arc<AsyncRwLock<HashMap<u16, Computors>>>,
    boundaries: Arc<AsyncRwLock<EpochBoundaries>>,
    rollover: Arc<AsyncRwLock<Option<EpochRollover>>>,
    store: Arc<S>,
    prune_keep: Option<u16>,
    cancel: CancellationToken,
    poll: Duration,
) {
    let mut cursor = 0usize;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(poll) => {}
        }
        let head_result = tokio::select! {
            _ = cancel.cancelled() => return,
            result = peer.head() => result,
        };
        match head_result {
            Ok(updated) => {
                let old_head = head.read().await.clone();
                let regressed = updated.epoch < old_head.epoch
                    || (updated.epoch == old_head.epoch && updated.tick < old_head.tick);
                if regressed {
                    tracing::warn!(
                        epoch = updated.epoch,
                        tick = updated.tick,
                        known_epoch = old_head.epoch,
                        known_tick = old_head.tick,
                        "head refresh regressed; rotating peer"
                    );
                    if let Some((replacement, index)) =
                        connect_from(&configs, cursor.saturating_add(1)).await
                    {
                        peer = replacement;
                        cursor = index;
                    }
                    continue;
                }
                let epoch_changed = updated.epoch != old_head.epoch;
                let missing = !computors.read().await.contains_key(&updated.epoch);
                // Cache the new epoch's computors before workers can observe
                // the new head. Publishing first would let a worker fetch with
                // an epoch that has no computor set.
                if epoch_changed || missing {
                    let computors_result = tokio::select! {
                        _ = cancel.cancelled() => return,
                        result = peer.computors() => result,
                    };
                    match computors_result {
                        Ok(set) if set.epoch == updated.epoch => {
                            computors.write().await.insert(set.epoch, set);
                        }
                        Ok(set) => {
                            tracing::warn!(
                                epoch = updated.epoch,
                                computors_epoch = set.epoch,
                                "peer returned computors for the wrong epoch"
                            );
                            continue;
                        }
                        Err(error) => {
                            tracing::warn!(%error, "computor refresh failed, reconnecting");
                            if let Some((replacement, index)) =
                                connect_from(&configs, cursor.saturating_add(1)).await
                            {
                                peer = replacement;
                                cursor = index;
                            }
                            continue;
                        }
                    }
                }
                // A peer can report an epoch once with a provisional initial
                // tick and later with the final restart tick. Never move the
                // boundary backwards; a stale peer must not rewind the
                // indexer or make already indexed ticks look like a new epoch.
                let previous_initial = boundaries.read().await.get(&updated.epoch).copied();
                if previous_initial.is_some_and(|initial| updated.initial_tick < initial) {
                    tracing::warn!(
                        epoch = updated.epoch,
                        previous_initial = previous_initial.unwrap_or_default(),
                        reported_initial = updated.initial_tick,
                        "ignoring backward epoch boundary"
                    );
                    continue;
                }
                // Persist and publish the boundary before the head so a
                // worker never sees a head whose epoch has no boundary.
                let boundary_changed = {
                    let mut known = boundaries.write().await;
                    let previous = known.insert(updated.epoch, updated.initial_tick);
                    previous != Some(updated.initial_tick)
                };
                if boundary_changed {
                    if let Err(error) = store
                        .write_epoch_start(updated.epoch, updated.initial_tick)
                        .await
                    {
                        tracing::warn!(
                            epoch = updated.epoch,
                            %error,
                            "epoch start was not persisted"
                        );
                    }
                }
                if epoch_changed {
                    let old_last = old_head.tick.max(old_head.initial_tick);
                    let skipped = updated
                        .initial_tick
                        .saturating_sub(old_last)
                        .saturating_sub(1);
                    *rollover.write().await = Some(EpochRollover {
                        old_epoch: old_head.epoch,
                        old_last,
                        new_epoch: updated.epoch,
                        new_initial: updated.initial_tick,
                    });
                    if skipped > 0 {
                        tracing::info!(
                            old_epoch = old_head.epoch,
                            old_last,
                            new_epoch = updated.epoch,
                            new_initial = updated.initial_tick,
                            skipped,
                            "restart tick gap observed"
                        );
                    }
                    if updated.epoch != old_head.epoch.saturating_add(1) {
                        tracing::warn!(
                            old_epoch = old_head.epoch,
                            new_epoch = updated.epoch,
                            "epoch number gap observed"
                        );
                    }
                    if let Err(error) = store
                        .close_epoch(old_head.epoch, old_head.initial_tick, old_last)
                        .await
                    {
                        tracing::warn!(
                            epoch = old_head.epoch,
                            %error,
                            "previous epoch was not closed"
                        );
                    }
                } else if let Some(previous_initial) = previous_initial {
                    if updated.initial_tick > previous_initial {
                        // The same epoch moved its published start forward.
                        // This happens when a peer announces a provisional
                        // restart tick and the network restarts again before
                        // the new epoch produces a verifiable tick. Move the
                        // writer past the abandoned range instead of trying
                        // those tick numbers forever.
                        let old_epoch = updated.epoch.saturating_sub(1);
                        let previous_epoch_last = match store.last_tick_for_epoch(old_epoch).await {
                            Ok(tick) => tick,
                            Err(error) => {
                                tracing::warn!(
                                    epoch = old_epoch,
                                    %error,
                                    "last indexed tick was not read"
                                );
                                None
                            }
                        };
                        if let Some(state) = moved_boundary_rollover(
                            updated.epoch,
                            previous_initial,
                            previous_epoch_last,
                            updated.initial_tick,
                        ) {
                            let skipped = updated
                                .initial_tick
                                .saturating_sub(state.old_last)
                                .saturating_sub(1);
                            *rollover.write().await = Some(state);
                            tracing::warn!(
                                epoch = updated.epoch,
                                previous_initial,
                                new_initial = updated.initial_tick,
                                skipped,
                                "epoch initial tick moved forward"
                            );
                            if let Ok(Some(metadata)) = store.epoch_metadata_for(old_epoch).await {
                                if let Err(error) = store
                                    .close_epoch(old_epoch, metadata.initial_tick, state.old_last)
                                    .await
                                {
                                    tracing::warn!(
                                        epoch = old_epoch,
                                        %error,
                                        "previous epoch was not closed after boundary move"
                                    );
                                }
                            }
                        }
                    }
                }
                *head.write().await = updated.clone();
                if epoch_changed {
                    tracing::info!(
                        old_epoch = old_head.epoch,
                        new_epoch = updated.epoch,
                        initial_tick = updated.initial_tick,
                        "epoch changed"
                    );
                    if let Some(keep) = prune_keep {
                        let store = store.clone();
                        tokio::spawn(async move {
                            match store.prune_epochs(keep).await {
                                Ok(pruned) if !pruned.is_empty() => tracing::info!(
                                    keep,
                                    pruned = pruned.len(),
                                    oldest = pruned.first().copied(),
                                    newest = pruned.last().copied(),
                                    "pruned old epochs"
                                ),
                                Ok(_) => {}
                                Err(error) => {
                                    tracing::warn!(keep, %error, "epoch pruning failed")
                                }
                            }
                        });
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, "head refresh failed, reconnecting");
                if let Some((replacement, index)) =
                    connect_from(&configs, cursor.saturating_add(1)).await
                {
                    peer = replacement;
                    cursor = index;
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_loop(
    initial: u32,
    head: Arc<AsyncRwLock<Head>>,
    rollover: Arc<AsyncRwLock<Option<EpochRollover>>>,
    next_write: Arc<AtomicU32>,
    pending: Arc<AtomicUsize>,
    work_tx: mpsc::Sender<FetchWork>,
    mut retry_rx: mpsc::Receiver<RetryRequest>,
    cancel: CancellationToken,
    window: usize,
    batch_max: usize,
    batch_min_lag: usize,
) {
    let mut retries = std::collections::VecDeque::<(Instant, u32, u32, u32)>::new();
    let mut assigned_upto = None;

    loop {
        if cancel.is_cancelled() {
            return;
        }
        let now = Instant::now();

        if let Some(position) = retries.iter().position(|(at, _, _, _)| *at <= now) {
            let (_, mut start, mut count, attempt) = retries.remove(position).expect("retry entry");
            let next = next_write.load(Ordering::Acquire);
            if start.saturating_add(count) <= next {
                continue;
            }
            if start < next {
                count = count.saturating_sub(next - start);
                start = next;
            }
            if let Some(state) = *rollover.read().await {
                if start > state.old_last && start < state.new_initial {
                    continue;
                }
                if start <= state.old_last
                    && start.saturating_add(count) > state.old_last.saturating_add(1)
                {
                    count = state.old_last.saturating_sub(start) + 1;
                }
            }
            let available = window.saturating_sub(pending.load(Ordering::Acquire));
            let count = count.min(available as u32);
            if count > 0 {
                pending.fetch_add(count as usize, Ordering::Release);
                if work_tx
                    .send(FetchWork {
                        start,
                        count,
                        attempt,
                    })
                    .await
                    .is_err()
                {
                    pending.fetch_sub(count as usize, Ordering::Release);
                    return;
                }
                continue;
            }
        }

        let next_write_tick = next_write.load(Ordering::Acquire);
        if let Some(upto) = assigned_upto {
            if upto < next_write_tick {
                assigned_upto = Some(next_write_tick.saturating_sub(1));
            }
        }
        let head_tick = head.read().await.tick;
        let next_tick = match assigned_upto {
            Some(upto) => upto.saturating_add(1),
            None => initial,
        };
        let rollover_state = *rollover.read().await;
        if let Some(state) = rollover_state {
            if state.contains(next_tick) {
                // The writer drains the old epoch tail, then jumps to the new
                // initial tick. Do not queue skipped tick numbers.
                assigned_upto = Some(state.new_initial.saturating_sub(1));
                continue;
            }
        }
        if pending.load(Ordering::Acquire) < window {
            if let Some(upper) = dispatch_upper_bound(next_write_tick, head_tick, window) {
                if next_tick >= next_write_tick && next_tick <= upper {
                    let lag = head_tick.saturating_sub(next_write_tick) as usize;
                    let desired = if lag >= batch_min_lag { batch_max } else { 1 };
                    let available = window.saturating_sub(pending.load(Ordering::Acquire));
                    let mut count = desired
                        .min(available)
                        .min((upper.saturating_sub(next_tick) + 1) as usize)
                        as u32;
                    // A restart gap can start in the middle of a batch.
                    if let Some(state) = rollover_state {
                        if next_tick <= state.old_last
                            && next_tick.saturating_add(count) > state.old_last.saturating_add(1)
                        {
                            count = state.old_last.saturating_sub(next_tick) + 1;
                        }
                    }
                    if count == 0 {
                        assigned_upto = Some(next_tick);
                        continue;
                    }
                    pending.fetch_add(count as usize, Ordering::Release);
                    if work_tx
                        .send(FetchWork {
                            start: next_tick,
                            count,
                            attempt: 0,
                        })
                        .await
                        .is_err()
                    {
                        pending.fetch_sub(count as usize, Ordering::Release);
                        return;
                    }
                    assigned_upto = Some(next_tick.saturating_add(count - 1));
                    continue;
                }
            }
        }

        let wait = retries
            .front()
            .map(|(at, _, _, _)| at.saturating_duration_since(now))
            .unwrap_or(Duration::from_millis(50))
            .min(Duration::from_millis(50));
        tokio::select! {
            _ = cancel.cancelled() => return,
            request = retry_rx.recv() => {
                if let Some(request) = request {
                    retries.push_back((
                        Instant::now() + request.delay,
                        request.start,
                        request.count,
                        request.attempt,
                    ));
                }
            }
            _ = tokio::time::sleep(wait) => {}
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn fetch_worker(
    mut peer: Option<node::PooledPeer>,
    pool: NodePool,
    work: Arc<AsyncMutex<mpsc::Receiver<FetchWork>>>,
    results: mpsc::Sender<FetchResult>,
    head: Arc<AsyncRwLock<Head>>,
    computors: Arc<AsyncRwLock<HashMap<u16, Computors>>>,
    boundaries: Arc<AsyncRwLock<EpochBoundaries>>,
    verify_signatures: bool,
    verify_semaphore: Arc<tokio::sync::Semaphore>,
    cancel: CancellationToken,
) {
    loop {
        let item = {
            let mut receiver = work.lock().await;
            tokio::select! {
                _ = cancel.cancelled() => return,
                item = receiver.recv() => match item {
                    Some(item) => item,
                    None => return,
                },
            }
        };

        if peer.is_none() {
            let connection = tokio::select! {
                _ = cancel.cancelled() => return,
                connection = pool.connect_balanced() => connection,
            };
            match connection {
                Ok(connected) => peer = Some(connected),
                Err(error) => {
                    tracing::debug!(%error, "no peer available for fetch worker");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    if results
                        .send(FetchResult {
                            start: item.start,
                            count: item.count,
                            attempt: item.attempt,
                            outcomes: Vec::new(),
                            batch_error: Some(Error::Node(error)),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                    continue;
                }
            }
        }
        let started = Instant::now();
        let head_snapshot = head.read().await.clone();
        let ticks = (item.start..item.start.saturating_add(item.count)).collect::<Vec<_>>();
        let known = boundaries.read().await.clone();
        let mut runs = Vec::<BatchRun>::new();
        let mut outcomes = (0..ticks.len()).map(|_| None).collect::<Vec<_>>();
        for (position, tick) in ticks.iter().enumerate() {
            let Some(epoch) = epoch_for_tick(&known, *tick) else {
                tracing::warn!(
                    tick,
                    "epoch boundary for tick is unknown; skipping instead of guessing"
                );
                outcomes[position] = Some(Err(Error::Node(node::Error::Verification(format!(
                    "unknown epoch boundary for tick {tick}"
                )))));
                continue;
            };
            match runs.last_mut() {
                Some(run) if run.epoch == epoch => {
                    run.positions.push(position);
                    run.ticks.push(*tick);
                }
                _ => runs.push(BatchRun {
                    epoch,
                    positions: vec![position],
                    ticks: vec![*tick],
                }),
            }
        }

        let mut peer_failed = false;
        let mut batch_error: Option<Error> = None;
        for run in runs {
            if batch_error.is_some() {
                break;
            }
            if peer_failed {
                for position in run.positions {
                    outcomes[position] = Some(Err(Error::Node(node::Error::NoPeers)));
                }
                continue;
            }
            let cached = computors.read().await.get(&run.epoch).cloned();
            let Some(cached) = cached else {
                for position in run.positions {
                    outcomes[position] =
                        Some(Err(Error::Node(node::Error::Verification(format!(
                            "computors for epoch {} are unavailable for tick {}",
                            run.epoch, run.ticks[0]
                        )))));
                }
                continue;
            };
            let fetched = tokio::select! {
                _ = cancel.cancelled() => return,
                outcome = peer.as_mut().expect("worker peer").prefetch_bundles(
                    &run.ticks,
                    head_snapshot.clone(),
                    cached,
                    Some(&pool),
                ) => outcome,
            };
            let fetched = match fetched {
                Ok(fetched) => fetched,
                Err(error) => {
                    if node_error_is_peer_specific(&error) {
                        if let Some(peer) = peer.as_ref() {
                            pool.record_failure(peer.addr());
                        }
                        peer = None;
                    }
                    batch_error = Some(Error::Node(error));
                    break;
                }
            };
            if fetched.len() != run.ticks.len() {
                batch_error = Some(Error::Verification(format!(
                    "prefetch returned {} bundles for {} ticks",
                    fetched.len(),
                    run.ticks.len()
                )));
                break;
            }

            for (offset, bundle) in fetched.into_iter().enumerate() {
                let position = run.positions[offset];
                let outcome = match bundle {
                    Ok(bundle) => {
                        match verify_fetched(bundle, verify_signatures, &verify_semaphore, &cancel)
                            .await
                        {
                            Some(outcome) => outcome,
                            None => return,
                        }
                    }
                    Err(error) => {
                        if node_error_is_peer_specific(&error) {
                            peer_failed = true;
                        }
                        Err(Error::Node(error))
                    }
                };
                outcomes[position] = Some(outcome);
            }
            if peer_failed {
                if let Some(peer) = peer.as_ref() {
                    pool.record_failure(peer.addr());
                }
                peer = None;
            } else if run.positions.iter().any(|position| {
                outcomes[*position]
                    .as_ref()
                    .is_some_and(|outcome| outcome.is_ok())
            }) {
                if let Some(peer) = peer.as_ref() {
                    pool.record_healthy(peer.addr());
                }
            }
        }

        if let Some(error) = batch_error {
            if results
                .send(FetchResult {
                    start: item.start,
                    count: item.count,
                    attempt: item.attempt,
                    outcomes: Vec::new(),
                    batch_error: Some(error),
                })
                .await
                .is_err()
            {
                return;
            }
            continue;
        }
        let outcomes = outcomes
            .into_iter()
            .enumerate()
            .map(|(position, outcome)| TickOutcome {
                tick: item.start.saturating_add(position as u32),
                attempt: item.attempt,
                outcome: outcome.unwrap_or_else(|| {
                    Err(Error::Verification(
                        "batch fetch produced no result for a tick".into(),
                    ))
                }),
            })
            .collect::<Vec<_>>();
        tracing::debug!(
            start = item.start,
            count = item.count,
            ok = outcomes
                .iter()
                .filter(|outcome| outcome.outcome.is_ok())
                .count(),
            elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0,
            "batch fetched and verified"
        );
        if results
            .send(FetchResult {
                start: item.start,
                count: item.count,
                attempt: item.attempt,
                outcomes,
                batch_error: None,
            })
            .await
            .is_err()
        {
            return;
        }
    }
}

struct BatchRun {
    epoch: u16,
    positions: Vec<usize>,
    ticks: Vec<u32>,
}

async fn verify_fetched(
    bundle: node::TickBundle,
    verify_signatures: bool,
    semaphore: &Arc<tokio::sync::Semaphore>,
    cancel: &CancellationToken,
) -> Option<Result<VerifiedTick>> {
    if !verify_signatures {
        return Some(verify_bundle_sequential(bundle, false));
    }
    let permit = tokio::select! {
        _ = cancel.cancelled() => return None,
        permit = semaphore.acquire() => match permit {
            Ok(permit) => permit,
            Err(_) => return None,
        },
    };
    let result = tokio::task::spawn_blocking(move || verify_bundle_sequential(bundle, true)).await;
    drop(permit);
    Some(match result {
        Ok(result) => result,
        Err(error) => Err(Error::Verification(format!(
            "verification task failed: {error}"
        ))),
    })
}

/// A peer-level failure means this connection should not receive the same
/// request again. Verification failures are deliberately excluded: they can
/// be a real protocol disagreement and the writer has its own handling.
fn node_error_is_unusable(error: &node::Error) -> bool {
    matches!(
        error,
        node::Error::Io(_)
            | node::Error::Wire(_)
            | node::Error::Timeout(_)
            | node::Error::RetriesExhausted { .. }
            | node::Error::NotSynchronized { .. }
            | node::Error::TryAgain(_)
            | node::Error::EndResponse(_)
            | node::Error::UnexpectedPacket { .. }
            | node::Error::InvalidRequest(_)
    )
}

/// A peer that has no quorum votes for a tick cannot serve it even though the
/// connection is healthy. Rotate instead of retrying the same peer forever.
fn node_error_is_peer_specific(error: &node::Error) -> bool {
    node_error_is_unusable(error)
        || matches!(
            error,
            node::Error::Verification(message) if message.starts_with("no votes for tick")
        )
}

fn required_quorum(digest: &[u8; 32]) -> usize {
    if digest == &ZERO_DIGEST {
        EMPTY_TICK_QUORUM
    } else {
        QUORUM
    }
}

/// Picks the quorum group after deduplication without verifying signatures for
/// groups that cannot reach the threshold.
///
/// Votes are grouped by transaction digest and tried in descending size order.
/// `verified_count` must return only the votes whose signatures actually
/// verified, so an unverified vote can never contribute to quorum. If the
/// largest candidate falls short, the next candidate is tried. Multiple
/// verified candidates are a protocol error rather than a silent winner.
#[doc(hidden)]
pub fn select_consensus_group<'a>(
    tick_number: u32,
    votes: &[&'a Tick],
    required_for: impl Fn(&[u8; 32]) -> usize,
    verified_count: impl Fn(&[&'a Tick], usize) -> usize,
) -> Result<([u8; 32], usize)> {
    let mut groups = BTreeMap::<VoteGroupKey, Vec<&'a Tick>>::new();
    for vote in votes {
        groups
            .entry(vote.transaction_digest)
            .or_default()
            .push(*vote);
    }

    let mut candidates = groups
        .into_iter()
        .map(|(digest, votes)| {
            let required = required_for(&digest);
            (votes.len(), required, digest, votes)
        })
        .collect::<Vec<_>>();
    // Descending size; the digest breaks ties deterministically.
    candidates.sort_by(|a, b| b.0.cmp(&a.0).then(a.2.cmp(&b.2)));

    let mut winner = None;
    let mut winners = 0usize;
    for (raw, required, digest, group) in &candidates {
        if *raw < *required {
            continue;
        }
        let valid = verified_count(group, *required);
        if valid < *required {
            continue;
        }
        winners += 1;
        if winner.is_none() {
            winner = Some((*digest, valid));
        }
    }
    if winners > 1 {
        return Err(Error::Verification(format!(
            "tick {tick_number} has {winners} conflicting quorum candidates"
        )));
    }
    winner.ok_or_else(|| {
        let (best, required) = candidates
            .first()
            .map(|(raw, required, _, _)| (*raw, *required))
            .unwrap_or((0, QUORUM));
        Error::Verification(format!(
            "tick {tick_number} has {best} aligned votes, needs {required}"
        ))
    })
}

/// Deterministic signature-verification result for one vote group.
///
/// `valid` is capped at the required threshold: verification stops as soon as
/// the group is known to reach quorum. `first_invalid` is only reported when
/// the group fell short; a group that reached quorum reports `None`.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteVerification {
    pub valid: usize,
    pub first_invalid: Option<usize>,
}

#[doc(hidden)]
pub fn verify_vote_group_sequential(
    votes: &[&Tick],
    computors: &Computors,
    required: usize,
) -> VoteVerification {
    let mut result = VoteVerification {
        valid: 0,
        first_invalid: None,
    };
    if required == 0 {
        return result;
    }
    for (index, vote) in votes.iter().enumerate() {
        if result.valid >= required {
            break;
        }
        if verify_vote_signature(vote, computors) {
            result.valid += 1;
        } else if result.first_invalid.is_none() {
            result.first_invalid = Some(index);
        }
    }
    if result.valid >= required {
        result.first_invalid = None;
    }
    result
}

#[doc(hidden)]
pub fn verify_vote_group_parallel(
    votes: &[&Tick],
    computors: &Computors,
    required: usize,
) -> VoteVerification {
    let workers = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1);
    if workers <= 1 || votes.len() < 2 || required == 0 {
        return verify_vote_group_sequential(votes, computors, required);
    }

    let chunk_size = votes.len().div_ceil(workers.min(votes.len()));
    let found = std::sync::atomic::AtomicUsize::new(0);
    let stop = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        let handles = votes
            .chunks(chunk_size)
            .map(|chunk| {
                scope.spawn(|| {
                    let mut part = VoteVerification {
                        valid: 0,
                        first_invalid: None,
                    };
                    for (offset, vote) in chunk.iter().enumerate() {
                        if stop.load(std::sync::atomic::Ordering::Relaxed) {
                            break;
                        }
                        if verify_vote_signature(vote, computors) {
                            part.valid += 1;
                            let total =
                                found.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                            if total >= required {
                                stop.store(true, std::sync::atomic::Ordering::Relaxed);
                                break;
                            }
                        } else if part.first_invalid.is_none() {
                            part.first_invalid = Some(offset);
                        }
                    }
                    part
                })
            })
            .collect::<Vec<_>>();
        let mut result = VoteVerification {
            valid: 0,
            first_invalid: None,
        };
        for (chunk_index, handle) in handles.into_iter().enumerate() {
            let part = handle.join().expect("vote verification thread panicked");
            result.valid += part.valid;
            if let Some(offset) = part.first_invalid {
                let index = chunk_index * chunk_size + offset;
                if result.first_invalid.is_none() {
                    result.first_invalid = Some(index);
                }
            }
        }
        if result.valid >= required {
            result.valid = required;
            result.first_invalid = None;
        }
        result
    })
}

fn verify_vote_group(
    votes: &[&Tick],
    computors: &Computors,
    required: usize,
    parallel: bool,
) -> VoteVerification {
    // Thread startup is not worth it for the handful of votes in a tiny group.
    const PARALLEL_MIN: usize = 64;
    if parallel && votes.len() >= PARALLEL_MIN {
        verify_vote_group_parallel(votes, computors, required)
    } else {
        verify_vote_group_sequential(votes, computors, required)
    }
}

fn verify_vote_signature(vote: &Tick, computors: &Computors) -> bool {
    let Some(key) = computors.public_keys.get(vote.computor_index as usize) else {
        return false;
    };
    // Core signs the vote with `computorIndex ^ BROADCAST_TICK` in the hashed
    // bytes (see broadcastTickVotes/processBroadcastTick), then sends the
    // un-XORed index on the wire.
    const BROADCAST_TICK: u16 = 3;
    let mut bytes = vote.encode();
    bytes[..2].copy_from_slice(&(vote.computor_index ^ BROADCAST_TICK).to_le_bytes());
    let digest = k12_32(&bytes[..bytes.len() - 64]);
    schnorrq::verify_digest(key, &digest, &vote.signature)
}

fn verify_tick_data_signature(bundle: &TickBundle, computors: &Computors) -> Result<()> {
    let Some(key) = computors
        .public_keys
        .get(bundle.tick_data.computor_index as usize)
    else {
        return Err(Error::Verification(
            "tick data computor index is out of range".into(),
        ));
    };
    // Same domain separation as votes: the digest covers the computor index
    // XORed with BROADCAST_FUTURE_TICK_DATA.
    const BROADCAST_FUTURE_TICK_DATA: u16 = 8;
    let mut bytes = bundle.tick_data.encode();
    bytes[..2].copy_from_slice(
        &(bundle.tick_data.computor_index ^ BROADCAST_FUTURE_TICK_DATA).to_le_bytes(),
    );
    let digest = k12_32(&bytes[..TICK_DATA_SIZE - 64]);
    if schnorrq::verify_digest(key, &digest, &bundle.tick_data.signature) {
        Ok(())
    } else {
        Err(Error::Verification("invalid tick data signature".into()))
    }
}

#[cfg(test)]
fn verify_transaction_signature(transaction: &Transaction) -> bool {
    let bytes = transaction.encode();
    verify_transaction_signature_bytes(&bytes, &transaction.source, &transaction.signature)
}

fn verify_transaction_signature_bytes(
    bytes: &[u8],
    source: &[u8; 32],
    signature: &[u8; 64],
) -> bool {
    let digest = k12_32(&bytes[..bytes.len() - 64]);
    schnorrq::verify_digest(source, &digest, signature)
}

/// Precomputed lookup for log ranges. Slots 0..4096 are transaction indices;
/// 4096..4102 are the six special system-event slots.
///
/// Ranges are normally sorted and disjoint, so a binary search finds the slot
/// in O(log n) instead of rescanning all 4098 ranges for every log. If a peer
/// supplies overlapping ranges, fall back to the original first-index scan.
struct LogSlotLookup {
    ordered: Vec<(i64, i64, usize)>,
    sorted: Vec<(i64, i64, usize)>,
    overlapping: bool,
}

impl LogSlotLookup {
    fn new(ranges: &qubic::messages::LogRanges) -> Self {
        let mut ordered = Vec::new();
        for (index, (from, length)) in ranges
            .from_log_id
            .iter()
            .zip(ranges.length.iter())
            .enumerate()
        {
            if index >= LOG_TX_PER_TICK {
                break;
            }
            if *from >= 0 && *length > 0 {
                ordered.push((*from, from.saturating_add(*length), index));
            }
        }
        let mut sorted = ordered.clone();
        sorted.sort_by_key(|(start, _, _)| *start);
        let overlapping = sorted.windows(2).any(|pair| pair[1].0 < pair[0].1);
        Self {
            ordered,
            sorted,
            overlapping,
        }
    }

    fn slot(&self, log_id: u64) -> Option<usize> {
        let id = log_id as i64;
        if self.overlapping {
            return self
                .ordered
                .iter()
                .find(|(start, end, _)| id >= *start && id < *end)
                .map(|(_, _, slot)| *slot);
        }
        let next = self.sorted.partition_point(|(start, _, _)| *start <= id);
        let found = next.checked_sub(1).and_then(|index| {
            let (start, end, slot) = self.sorted[index];
            (id >= start && id < end).then_some(slot)
        });
        // A peer can declare ranges that are sorted but not disjoint in a way
        // the adjacency check misses. Never reject a valid log because of the
        // index shape; fall back to the original scan.
        found.or_else(|| {
            self.ordered
                .iter()
                .find(|(start, end, _)| id >= *start && id < *end)
                .map(|(_, _, slot)| *slot)
        })
    }
}

fn event_body_json(event: &LogEvent) -> String {
    let key = |bytes: [u8; 32]| PublicKey(bytes).to_identity();
    let value = match event.body() {
        EventBody::QuTransfer { from, to, amount } => serde_json::json!({
            "from": key(from),
            "to": key(to),
            "amount": amount,
        }),
        EventBody::AssetIssuance { asset, shares } => serde_json::json!({
            "issuer": key(asset.issuer),
            "name": asset.name,
            "decimals": asset.decimals,
            "unit": asset.unit,
            "managing_contract": asset.managing_contract,
            "shares": shares,
        }),
        EventBody::AssetOwnershipChange {
            from,
            to,
            asset,
            shares,
        } => serde_json::json!({
            "from": key(from),
            "to": key(to),
            "issuer": key(asset.issuer),
            "name": asset.name,
            "managing_contract": asset.managing_contract,
            "shares": shares,
        }),
        EventBody::AssetPossessionChange {
            from,
            to,
            asset,
            shares,
        } => serde_json::json!({
            "from": key(from),
            "to": key(to),
            "issuer": key(asset.issuer),
            "name": asset.name,
            "managing_contract": asset.managing_contract,
            "shares": shares,
        }),
        EventBody::Burning {
            from,
            amount,
            contract_index,
        } => serde_json::json!({
            "from": key(from),
            "amount": amount,
            "contract_index": contract_index,
        }),
        EventBody::DustBurning { burns } => serde_json::json!({
            "burns": burns
                .into_iter()
                .map(|(who, amount)| serde_json::json!({ "identity": key(who), "amount": amount }))
                .collect::<Vec<_>>(),
        }),
        EventBody::ContractMessage {
            contract_index,
            message_type,
            data,
        } => serde_json::json!({
            "contract_index": contract_index,
            "message_type": message_type,
            "data": hex::encode(data),
        }),
        EventBody::Custom { op_code, data } => serde_json::json!({
            "op_code": op_code.to_string(),
            "data": hex::encode(data),
        }),
        EventBody::Unknown { raw } => serde_json::json!({ "raw": hex::encode(raw) }),
        _ => serde_json::json!({ "raw": hex::encode(&event.body) }),
    };
    value.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verification_order_is_monotonic() {
        assert!(Verification::Quorum < Verification::Data);
        assert!(Verification::Data < Verification::State);
    }

    fn golden_computors() -> Computors {
        Computors::decode(&include_bytes!("../../../fixtures/computors.bin")[8..])
            .expect("computors fixture")
    }

    #[test]
    fn golden_vote_signature_verifies() {
        let computors = golden_computors();
        let vote = Tick::decode(&include_bytes!("../../../fixtures/quorum_tick.bin")[8..])
            .expect("vote fixture");
        assert!(verify_vote_signature(&vote, &computors));

        // The same signed bytes must not verify against a neighbouring key.
        let mut wrong = vote.clone();
        wrong.computor_index = (wrong.computor_index + 1) % COMPUTOR_COUNT as u16;
        assert!(!verify_vote_signature(&wrong, &computors));
    }

    #[test]
    fn golden_tick_data_signature_verifies() {
        let computors = golden_computors();
        let tick_data = TickData::decode(&include_bytes!("../../../fixtures/tick_data.bin")[8..])
            .expect("tick data fixture");
        let vote = Tick::decode(&include_bytes!("../../../fixtures/quorum_tick.bin")[8..])
            .expect("vote fixture");
        let bundle = TickBundle {
            head: Head {
                epoch: tick_data.epoch,
                tick: tick_data.tick,
                initial_tick: tick_data.tick,
                tick_duration: 1_000,
            },
            computors: computors.clone(),
            tick: vote,
            tick_data,
            votes: Vec::new(),
            transactions: Vec::new(),
            log_ranges: qubic::messages::LogRanges {
                from_log_id: vec![-1; LOG_TX_PER_TICK],
                length: vec![0; LOG_TX_PER_TICK],
            },
            logs: Vec::new(),
            warnings: Vec::new(),
        };
        verify_tick_data_signature(&bundle, &computors).expect("tick data signature");
    }

    #[test]
    fn golden_transaction_hash_matches_tick_data() {
        let tick_data = TickData::decode(&include_bytes!("../../../fixtures/tick_data.bin")[8..])
            .expect("tick data fixture");
        let transaction =
            Transaction::decode(&include_bytes!("../../../fixtures/tick_transactions.bin")[8..])
                .expect("transaction fixture");
        let hash = transaction.hash();
        assert!(
            tick_data
                .non_empty_transactions()
                .any(|(_, digest)| *digest == hash)
        );
        assert!(verify_transaction_signature(&transaction));
    }

    #[test]
    fn identity_round_trip_in_event_body() {
        let key = [7u8; 32];
        let event = LogEvent {
            epoch: 1,
            tick: 2,
            log_type: qubic::logs::QU_TRANSFER,
            log_id: 3,
            log_digest: 0,
            body: {
                let mut body = Vec::new();
                body.extend_from_slice(&key);
                body.extend_from_slice(&[8u8; 32]);
                body.extend_from_slice(&42i64.to_le_bytes());
                body
            },
        };
        let json = event_body_json(&event);
        assert!(json.contains(&PublicKey(key).to_identity()));
        assert!(json.contains("\"amount\":42"));
    }

    #[test]
    fn ordered_buffer_holds_later_results_until_the_next_tick() {
        let mut buffer = OrderedBuffer::new(8);
        buffer.insert(12, "twelve");
        buffer.insert(10, "ten");
        buffer.insert(11, "eleven");
        assert_eq!(buffer.remove_next(10), Some("ten"));
        assert_eq!(buffer.remove_next(10), None);
        assert_eq!(buffer.remove_next(11), Some("eleven"));
        assert_eq!(buffer.remove_next(12), Some("twelve"));
        assert!(!buffer.is_full());
    }

    #[test]
    fn dispatch_window_never_runs_past_the_head_or_window() {
        assert_eq!(dispatch_upper_bound(100, 1_000, 64), Some(163));
        assert_eq!(dispatch_upper_bound(100, 120, 64), Some(112));
        assert_eq!(
            dispatch_upper_bound(117, 125, 1),
            Some(117),
            "the live vote lag is reserved from the head"
        );
        assert_eq!(dispatch_upper_bound(100, 99, 64), None);
        assert_eq!(dispatch_upper_bound(u32::MAX - 1, u32::MAX, 64), None);
        assert_eq!(
            dispatch_upper_bound(u32::MAX - 9, u32::MAX, 64),
            Some(u32::MAX - 8)
        );
    }

    #[test]
    fn ordered_buffer_retains_only_the_live_window() {
        let mut buffer = OrderedBuffer::new(8);
        for tick in 10..15 {
            buffer.insert(tick, tick);
        }
        assert_eq!(buffer.retain_from(12), 2);
        assert_eq!(buffer.remove_next(12), Some(12));
        assert_eq!(buffer.remove_next(13), Some(13));
    }

    #[test]
    fn flush_policy_fills_batches_and_honors_the_time_bound() {
        let flush = Duration::from_millis(250);
        assert!(should_flush(127, 128, Duration::from_secs(10), flush));
        assert!(should_flush(128, 128, Duration::ZERO, flush));
        assert!(!should_flush(1, 128, Duration::from_millis(100), flush));
        assert!(should_flush(1, 128, Duration::from_millis(250), flush));
        assert!(!should_flush(0, 128, Duration::from_secs(10), flush));
    }

    #[test]
    fn failed_flush_does_not_advance_the_cursor() {
        assert_eq!(next_after_flush(100, None), 100);
        assert_eq!(next_after_flush(100, Some(99)), 100);
        assert_eq!(next_after_flush(100, Some(127)), 128);
    }

    #[test]
    fn epoch_mapping_uses_observed_boundaries() {
        // The current network runs roughly 330k ticks per epoch, not the
        // historical 604_800. Boundaries are learned per epoch.
        let mut boundaries = EpochBoundaries::new();
        boundaries.insert(237, 100_000_000);
        boundaries.insert(238, 100_330_000);
        boundaries.insert(239, 100_661_000);
        boundaries.insert(240, 100_991_500);

        assert_eq!(epoch_for_tick(&boundaries, 100_991_500), Some(240));
        assert_eq!(epoch_for_tick(&boundaries, 100_991_499), Some(239));
        assert_eq!(epoch_for_tick(&boundaries, 100_661_000), Some(239));
        assert_eq!(epoch_for_tick(&boundaries, 100_330_000), Some(238));
        assert_eq!(epoch_for_tick(&boundaries, 100_000_000), Some(237));
        // The tick predates every observed boundary. Guessing a fixed-size
        // epoch would silently assign it to the wrong epoch.
        assert_eq!(epoch_for_tick(&boundaries, 99_999_999), None);
    }

    #[test]
    fn epoch_mapping_refuses_unobserved_gaps() {
        let mut boundaries = EpochBoundaries::new();
        boundaries.insert(238, 100_000_000);
        boundaries.insert(240, 100_660_000);
        assert_eq!(epoch_for_tick(&boundaries, 100_000_000), Some(238));
        assert_eq!(epoch_for_tick(&boundaries, 100_659_999), None);
        assert_eq!(epoch_for_tick(&boundaries, 100_660_000), Some(240));
    }

    #[test]
    fn boundary_map_keeps_epoch_order() {
        let boundaries = boundary_map(vec![
            store::EpochBoundary {
                epoch: 2,
                initial_tick: 220,
            },
            store::EpochBoundary {
                epoch: 1,
                initial_tick: 110,
            },
        ]);
        assert_eq!(boundaries.get(&1), Some(&110));
        assert_eq!(boundaries.get(&2), Some(&220));
        assert_eq!(epoch_for_tick(&boundaries, 219), Some(1));
        assert_eq!(epoch_for_tick(&boundaries, 220), Some(2));
    }

    #[test]
    fn restart_gap_contains_only_skipped_ticks() {
        let rollover = EpochRollover {
            old_epoch: 240,
            old_last: 100,
            new_epoch: 241,
            new_initial: 200,
        };
        assert!(!rollover.contains(100));
        assert!(rollover.contains(101));
        assert!(rollover.contains(199));
        assert!(!rollover.contains(200));
    }

    #[test]
    fn moved_boundary_skips_the_provisional_epoch_start() {
        let rollover = moved_boundary_rollover(241, 92_241_594, Some(92_241_593), 92_260_000)
            .expect("a forward boundary move needs a skip range");
        assert_eq!(rollover.old_epoch, 240);
        assert_eq!(rollover.old_last, 92_241_593);
        assert!(rollover.contains(92_241_594));
        assert!(rollover.contains(92_259_999));
        assert!(!rollover.contains(92_260_000));

        let without_prior_epoch = moved_boundary_rollover(241, 92_241_594, None, 92_260_000)
            .expect("a forward boundary move needs a skip range");
        assert_eq!(without_prior_epoch.old_last, 92_241_593);

        assert!(moved_boundary_rollover(241, 92_260_000, Some(92_241_593), 92_241_594).is_none());
        assert!(moved_boundary_rollover(241, 92_260_000, Some(92_241_593), 92_260_000).is_none());
    }

    #[test]
    fn peers_that_cannot_serve_a_tick_are_rotated() {
        let unavailable = [
            node::Error::NotSynchronized {
                head: 100,
                requested: 200,
            },
            node::Error::TryAgain(qubic::wire::MessageType::RequestTickData),
            node::Error::EndResponse(qubic::wire::MessageType::RequestTickData),
        ];
        for error in unavailable {
            assert!(node_error_is_unusable(&error));
        }

        assert!(!node_error_is_unusable(&node::Error::Verification(
            "bad tick data".into()
        )));
    }

    #[tokio::test]
    async fn dispatcher_cancels_cleanly() {
        let head = Arc::new(AsyncRwLock::new(Head {
            epoch: 1,
            tick: 1_000,
            initial_tick: 100,
            tick_duration: 1_000,
        }));
        let next_write = Arc::new(AtomicU32::new(100));
        let pending = Arc::new(AtomicUsize::new(0));
        let (work_tx, _work_rx) = mpsc::channel::<FetchWork>(4);
        let (_retry_tx, retry_rx) = mpsc::channel::<RetryRequest>(4);
        let cancel = CancellationToken::new();
        let task = tokio::spawn(dispatch_loop(
            100,
            head,
            Arc::new(AsyncRwLock::new(None)),
            next_write,
            pending,
            work_tx,
            retry_rx,
            cancel.clone(),
            4,
            1,
            1,
        ));
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("dispatcher must stop promptly")
            .expect("dispatcher task");
    }

    #[tokio::test]
    async fn dispatcher_skips_restart_tick_gap() {
        let head = Arc::new(AsyncRwLock::new(Head {
            epoch: 2,
            tick: 300,
            initial_tick: 200,
            tick_duration: 1_000,
        }));
        let rollover = Arc::new(AsyncRwLock::new(Some(EpochRollover {
            old_epoch: 1,
            old_last: 105,
            new_epoch: 2,
            new_initial: 200,
        })));
        let next_write = Arc::new(AtomicU32::new(100));
        let pending = Arc::new(AtomicUsize::new(0));
        let (work_tx, mut work_rx) = mpsc::channel::<FetchWork>(256);
        let (_retry_tx, retry_rx) = mpsc::channel::<RetryRequest>(4);
        let cancel = CancellationToken::new();
        let task = tokio::spawn(dispatch_loop(
            100,
            head,
            rollover,
            next_write,
            pending,
            work_tx,
            retry_rx,
            cancel.clone(),
            256,
            16,
            1,
        ));

        let mut seen = Vec::new();
        for _ in 0..16 {
            let Ok(Some(item)) = tokio::time::timeout(Duration::from_secs(1), work_rx.recv()).await
            else {
                break;
            };
            seen.push((item.start, item.count));
            if item.start == 200 {
                break;
            }
        }
        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), task).await;
        assert!(
            seen.iter()
                .any(|(start, count)| { *start <= 105 && start.saturating_add(*count) > 105 })
        );
        assert!(seen.iter().any(|(start, _)| *start == 200));
        assert!(!seen.iter().any(|(start, count)| {
            (*start..start.saturating_add(*count)).any(|tick| (106..200).contains(&tick))
        }));
    }

    #[tokio::test]
    async fn dispatcher_batches_contiguous_ticks_while_behind() {
        let head = Arc::new(AsyncRwLock::new(Head {
            epoch: 1,
            tick: 1_000,
            initial_tick: 100,
            tick_duration: 1_000,
        }));
        let next_write = Arc::new(AtomicU32::new(100));
        let pending = Arc::new(AtomicUsize::new(0));
        let (work_tx, mut work_rx) = mpsc::channel::<FetchWork>(64);
        let (_retry_tx, retry_rx) = mpsc::channel::<RetryRequest>(4);
        let cancel = CancellationToken::new();
        let task = tokio::spawn(dispatch_loop(
            100,
            head,
            Arc::new(AsyncRwLock::new(None)),
            next_write,
            pending,
            work_tx,
            retry_rx,
            cancel.clone(),
            256,
            16,
            32,
        ));

        let mut seen = Vec::new();
        for _ in 0..4 {
            let Ok(Some(item)) = tokio::time::timeout(Duration::from_secs(1), work_rx.recv()).await
            else {
                break;
            };
            seen.push((item.start, item.count));
        }
        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), task).await;
        assert_eq!(seen, [(100, 16), (116, 16), (132, 16), (148, 16)]);
    }

    #[tokio::test]
    async fn dispatcher_uses_single_ticks_at_the_head() {
        let head = Arc::new(AsyncRwLock::new(Head {
            epoch: 1,
            tick: 108,
            initial_tick: 100,
            tick_duration: 1_000,
        }));
        let next_write = Arc::new(AtomicU32::new(100));
        let pending = Arc::new(AtomicUsize::new(0));
        let (work_tx, mut work_rx) = mpsc::channel::<FetchWork>(16);
        let (_retry_tx, retry_rx) = mpsc::channel::<RetryRequest>(4);
        let cancel = CancellationToken::new();
        let task = tokio::spawn(dispatch_loop(
            100,
            head,
            Arc::new(AsyncRwLock::new(None)),
            next_write,
            pending,
            work_tx,
            retry_rx,
            cancel.clone(),
            256,
            16,
            32,
        ));

        let item = tokio::time::timeout(Duration::from_secs(1), work_rx.recv())
            .await
            .expect("dispatch timeout")
            .expect("work item");
        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), task).await;
        assert_eq!((item.start, item.count), (100, 1));
    }
}
