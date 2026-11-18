//! Verification and sync pipeline for the Glyph Qubic index.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use node::{Head, NodePool, Peer, PeerConfig, TickBundle};
use qubic::crypto::k12::k12_32;
use qubic::crypto::schnorrq;
use qubic::logs::{CONTRACT_ERROR_MESSAGE, EventBody};
use qubic::messages::{
    Computors, EMPTY_TICK_QUORUM, QUORUM, TICK_DATA_SIZE, TX_PER_TICK, Tick, TickData, Transaction,
    ZERO_DIGEST,
};
use qubic::{LogEvent, PublicKey};
use serde::Serialize;
use store::{EventRow, IdentityRow, Store, TickBatch, TransactionRow};
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
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct IndexerConfig {
    pub peers: Vec<PeerConfig>,
    /// First tick to fetch when there is no stored cursor. Defaults to the
    /// current epoch's initial tick.
    pub start_tick: Option<u32>,
    /// How often to poll the head when caught up.
    pub poll_interval: Duration,
    /// Verify SchnorrQ signatures in addition to quorum digests. This is
    /// CPU-heavy for backfills but should be enabled for live ingestion.
    pub verify_signatures: bool,
    /// Log progress every N ticks.
    pub progress_every: u64,
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            peers: Vec::new(),
            start_tick: None,
            poll_interval: Duration::from_millis(750),
            verify_signatures: true,
            progress_every: 100,
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
    pub log_ranges: qubic::messages::LogRanges,
    pub logs: Vec<LogEvent>,
    pub verification: Verification,
    pub warnings: Vec<String>,
}

pub struct Indexer<S: Store + 'static> {
    store: Arc<S>,
    pool: NodePool,
    config: IndexerConfig,
    /// Absolute identity snapshots as of the last persisted tick.
    identities: HashMap<[u8; 32], IdentityRow>,
    /// Computors keyed by epoch. A peer only serves the current epoch's set, so
    /// keep the old set while the tail of a rolled-over epoch is still being
    /// indexed.
    computors: HashMap<u16, Computors>,
}

impl<S: Store + 'static> Indexer<S> {
    pub fn new(store: Arc<S>, config: IndexerConfig) -> Result<Self> {
        if config.peers.is_empty() {
            return Err(Error::Config("at least one peer is required".into()));
        }
        let pool = NodePool::new(config.peers.clone());
        Ok(Self {
            store,
            pool,
            config,
            identities: HashMap::new(),
            computors: HashMap::new(),
        })
    }

    /// Runs until `cancel` is triggered.
    pub async fn run(&mut self, cancel: CancellationToken) -> Result<()> {
        self.store.init().await?;
        let mut peer = self.pool.connect_best().await?;
        let mut head = peer.head().await?;
        let mut next = self.next_tick(&head).await?;
        let mut backoff = Duration::from_millis(250);
        let mut since_progress = 0u64;
        let mut consecutive_failures = 0u32;

        tracing::info!(
            epoch = head.epoch,
            head = head.tick,
            initial_tick = head.initial_tick,
            next,
            "indexer started"
        );

        loop {
            if cancel.is_cancelled() {
                tracing::info!(next, "indexer stopping");
                return Ok(());
            }

            if next > head.tick {
                tokio::select! {
                    _ = cancel.cancelled() => continue,
                    _ = tokio::time::sleep(self.config.poll_interval) => {}
                }
                match peer.head().await {
                    Ok(updated) => {
                        if updated.epoch != head.epoch {
                            tracing::info!(
                                old_epoch = head.epoch,
                                new_epoch = updated.epoch,
                                initial_tick = updated.initial_tick,
                                "epoch changed"
                            );
                        }
                        head = updated;
                    }
                    Err(error) => {
                        tracing::warn!(%error, "head refresh failed, reconnecting");
                        peer = self.reconnect(peer).await?;
                    }
                }
                continue;
            }

            let mut fetched = match peer.bundle(next).await {
                Ok(bundle) => bundle,
                Err(error) => {
                    tracing::warn!(tick = next, %error, "tick fetch failed, retrying");
                    consecutive_failures += 1;
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                    if matches!(
                        error,
                        node::Error::Io(_)
                            | node::Error::Timeout(_)
                            | node::Error::RetriesExhausted { .. }
                    ) {
                        peer = self.reconnect(peer).await?;
                    }
                    if next < head.initial_tick && consecutive_failures >= 5 {
                        tracing::warn!(
                            tick = next,
                            initial_tick = head.initial_tick,
                            "previous epoch tail is unavailable, moving to the new epoch"
                        );
                        next = head.initial_tick;
                        consecutive_failures = 0;
                    }
                    continue;
                }
            };
            backoff = Duration::from_millis(250);

            let epoch = fetched.tick.epoch;
            if let Some(cached) = self.computors.get(&epoch) {
                fetched.computors = cached.clone();
            } else if fetched.computors.epoch == epoch {
                self.computors.insert(epoch, fetched.computors.clone());
            } else {
                tracing::error!(
                    tick = next,
                    epoch,
                    computors_epoch = fetched.computors.epoch,
                    "peer returned computors for the wrong epoch"
                );
                tokio::time::sleep(Duration::from_secs(2)).await;
                peer = self.reconnect(peer).await?;
                continue;
            }

            let verified = match verify_bundle(fetched, self.config.verify_signatures) {
                Ok(verified) => verified,
                Err(error) => {
                    tracing::error!(tick = next, %error, "verification failed, refusing to index");
                    consecutive_failures += 1;
                    if next < head.initial_tick && consecutive_failures >= 3 {
                        tracing::warn!(
                            tick = next,
                            initial_tick = head.initial_tick,
                            "previous epoch tail did not verify, moving to the new epoch"
                        );
                        next = head.initial_tick;
                        consecutive_failures = 0;
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    peer = self.reconnect(peer).await?;
                    continue;
                }
            };

            let batch = self.build_batch(&verified).await?;
            let outcome = self.store.write_tick(&batch).await?;
            consecutive_failures = 0;
            if outcome.tick >= next {
                next = outcome.tick.saturating_add(1);
            }
            since_progress += 1;
            if since_progress >= self.config.progress_every {
                since_progress = 0;
                tracing::info!(
                    tick = outcome.tick,
                    epoch = batch.epoch,
                    transactions = outcome.transactions,
                    events = outcome.events,
                    identities = outcome.identities,
                    verification = batch.verification,
                    lag = head.tick.saturating_sub(outcome.tick),
                    "indexed"
                );
            }
        }
    }

    async fn next_tick(&self, head: &Head) -> Result<u32> {
        let cursor = self.store.ingest_cursor().await?;
        let next = match cursor.last_indexed_tick {
            Some(tick) => tick.saturating_add(1),
            None => self.config.start_tick.unwrap_or(head.initial_tick),
        };
        // An epoch rollover can leave the stored cursor below the new epoch's
        // first tick. Never ask peers for ticks from a pruned epoch.
        Ok(next.max(head.initial_tick).min(head.tick.saturating_add(1)))
    }

    async fn reconnect(&self, peer: Peer) -> Result<Peer> {
        drop(peer);
        let mut last_error = None;
        for config in self.pool.peer_configs() {
            match Peer::connect(config.clone()).await {
                Ok(peer) => return Ok(peer),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error
            .map(Error::Node)
            .unwrap_or_else(|| Error::Config("no peers available".into())))
    }

    async fn build_batch(&mut self, verified: &VerifiedTick) -> Result<TickBatch> {
        let timestamp = DateTime::from_timestamp_millis(verified.tick.timestamp_millis())
            .unwrap_or_else(Utc::now);
        let mut batch = TickBatch::new(
            verified.tick.epoch,
            verified.tick.tick,
            timestamp,
            verified.tick.computor_index,
            verified.verification.as_str(),
        );
        batch.signature = verified.tick.signature;
        batch.tick_digest = verified.tick_data.digest();

        let mut digest_to_index = HashMap::with_capacity(TX_PER_TICK);
        for (index, digest) in verified.tick_data.non_empty_transactions() {
            digest_to_index.insert(*digest, index as u16);
        }

        let mut error_txs = HashMap::<u16, bool>::new();
        let mut tx_hashes = HashMap::<u16, String>::new();

        for (index, digest) in verified.tick_data.non_empty_transactions() {
            if let Some(transaction) = verified.transactions.iter().find(|tx| tx.hash() == *digest)
            {
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

        for event in &verified.logs {
            let tx_index = verified
                .log_ranges
                .from_log_id
                .iter()
                .zip(verified.log_ranges.length.iter())
                .position(|(from, len)| {
                    *from >= 0
                        && *len > 0
                        && (event.log_id as i64) >= *from
                        && ((event.log_id as i64) < *from + *len)
                });
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
            batch.events.push(EventRow {
                epoch: event.epoch,
                tick: event.tick,
                log_id: event.log_id,
                log_type: event.log_type as u16,
                type_name: event.type_name().to_string(),
                tx_hash,
                tx_index: tx_index_i16,
                body: event_body_json(event),
                log_digest: event.log_digest,
                timestamp,
            });
        }

        for transaction in &mut batch.transactions {
            if error_txs.contains_key(&transaction.tx_index) {
                transaction.status = "failed".into();
            }
        }

        let touched = self.apply_identity_changes(verified).await?;
        batch.identities = touched;

        Ok(batch)
    }

    async fn apply_identity_changes(
        &mut self,
        verified: &VerifiedTick,
    ) -> Result<Vec<IdentityRow>> {
        let mut touched = BTreeMap::<[u8; 32], IdentityRow>::new();
        for event in &verified.logs {
            if let EventBody::QuTransfer { from, to, amount } = event.body() {
                for (key, incoming) in [(from, false), (to, true)] {
                    if key == [0u8; 32] {
                        continue;
                    }
                    let row = self.load_identity(key).await?;
                    let row = touched.entry(key).or_insert(row);
                    if incoming {
                        row.incoming_amount = row.incoming_amount.saturating_add(amount);
                        row.balance = row.balance.saturating_add(amount);
                    } else {
                        row.outgoing_amount = row.outgoing_amount.saturating_add(amount);
                        row.balance = row.balance.saturating_sub(amount);
                    }
                    row.transfer_count = row.transfer_count.saturating_add(1);
                    row.last_seen_tick = row.last_seen_tick.max(verified.tick.tick);
                    if row.first_seen_tick == 0 {
                        row.first_seen_tick = verified.tick.tick;
                    }
                }
            }
        }

        let mut rows = Vec::with_capacity(touched.len());
        for (key, row) in touched {
            self.identities.insert(key, row.clone());
            rows.push(row);
        }
        Ok(rows)
    }

    async fn load_identity(&mut self, key: [u8; 32]) -> Result<IdentityRow> {
        if let Some(row) = self.identities.get(&key) {
            return Ok(row.clone());
        }
        let identity = PublicKey(key).to_identity();
        let row = match self.store.identity(&identity).await? {
            Some(row) => row,
            None => IdentityRow {
                identity: identity.clone(),
                public_key: PublicKey(key).to_hex(),
                balance: 0,
                incoming_amount: 0,
                outgoing_amount: 0,
                transfer_count: 0,
                first_seen_tick: 0,
                last_seen_tick: 0,
            },
        };
        self.identities.insert(key, row.clone());
        Ok(row)
    }
}

/// Verifies quorum, transaction inclusion, log digests, and optionally
/// SchnorrQ signatures.
pub fn verify_bundle(mut bundle: TickBundle, verify_signatures: bool) -> Result<VerifiedTick> {
    let tick = bundle.tick.clone();
    let tick_data = bundle.tick_data.clone();
    let epoch = tick.epoch;
    let tick_number = tick.tick;
    let mut warnings = std::mem::take(&mut bundle.warnings);
    let mut complete = true;

    if bundle.tick_data.tick != tick_number {
        return Err(Error::Verification(format!(
            "tick data is for {}, requested {tick_number}",
            bundle.tick_data.tick
        )));
    }
    if bundle.tick_data.epoch != epoch && bundle.tick_data.epoch != 0 {
        return Err(Error::Verification(format!(
            "tick data epoch {} does not match tick epoch {epoch}",
            bundle.tick_data.epoch
        )));
    }

    let mut groups = BTreeMap::<[u8; 32], usize>::new();
    for vote in &bundle.votes {
        if vote.tick == tick_number && vote.epoch == epoch {
            *groups.entry(vote.transaction_digest).or_default() += 1;
        }
    }
    let (consensus_digest, consensus_count) = groups
        .iter()
        .max_by_key(|(_, count)| **count)
        .map(|(digest, count)| (*digest, *count))
        .ok_or_else(|| Error::Verification(format!("no votes for tick {tick_number}")))?;

    let required = if consensus_digest == ZERO_DIGEST {
        EMPTY_TICK_QUORUM
    } else {
        QUORUM
    };
    if consensus_count < required {
        return Err(Error::Verification(format!(
            "tick {tick_number} has {consensus_count} matching votes, needs {required}"
        )));
    }
    if consensus_digest != ZERO_DIGEST && tick_data.digest() != consensus_digest {
        return Err(Error::Verification(format!(
            "tick data digest does not match quorum vote for tick {tick_number}"
        )));
    }

    if verify_signatures {
        verify_signatures_for_quorum(&bundle, &consensus_digest, required)?;
        if consensus_digest != ZERO_DIGEST {
            verify_tick_data_signature(&bundle, &bundle.computors)?;
        }
    }

    let digest_to_index: HashMap<[u8; 32], u16> = tick_data
        .non_empty_transactions()
        .map(|(index, digest)| (*digest, index as u16))
        .collect();
    let mut seen = 0usize;
    for digest in tick_data.non_empty_transactions().map(|(_, digest)| digest) {
        if let Some(transaction) = bundle.transactions.iter().find(|tx| tx.hash() == *digest) {
            seen += 1;
            if verify_signatures && !verify_transaction_signature(transaction) {
                return Err(Error::Verification(format!(
                    "invalid transaction signature for {}",
                    qubic::encode_tx_hash(digest)
                )));
            }
        }
    }
    if seen != digest_to_index.len() {
        complete = false;
        warnings.push(format!(
            "tick {tick_number}: fetched {seen}/{} transactions",
            digest_to_index.len()
        ));
    }

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
    }
    let expected_logs = bundle.log_ranges.count();
    if (bundle.logs.len() as i64) < expected_logs {
        complete = false;
        warnings.push(format!(
            "tick {tick_number}: fetched {}/{} logs",
            bundle.logs.len(),
            expected_logs
        ));
    }

    if complete && verify_signatures {
        Ok(VerifiedTick {
            head: bundle.head,
            tick,
            tick_data,
            votes: bundle.votes,
            transactions: bundle.transactions,
            log_ranges: bundle.log_ranges,
            logs: bundle.logs,
            verification: Verification::Data,
            warnings,
        })
    } else {
        Ok(VerifiedTick {
            head: bundle.head,
            tick,
            tick_data,
            votes: bundle.votes,
            transactions: bundle.transactions,
            log_ranges: bundle.log_ranges,
            logs: bundle.logs,
            verification: Verification::Quorum,
            warnings,
        })
    }
}

fn verify_signatures_for_quorum(
    bundle: &TickBundle,
    consensus_digest: &[u8; 32],
    required: usize,
) -> Result<()> {
    if consensus_digest == &ZERO_DIGEST {
        // Empty ticks still need 226 valid signatures.
        let mut valid = 0usize;
        for vote in bundle
            .votes
            .iter()
            .filter(|vote| vote.tick == bundle.tick.tick && vote.transaction_digest == ZERO_DIGEST)
        {
            if verify_vote_signature(vote, &bundle.computors) {
                valid += 1;
            }
            if valid >= required {
                return Ok(());
            }
        }
        return Err(Error::Verification(format!(
            "only {valid} valid empty-tick signatures, need {required}"
        )));
    }

    let mut valid = 0usize;
    for vote in bundle.votes.iter().filter(|vote| {
        vote.tick == bundle.tick.tick && vote.transaction_digest == *consensus_digest
    }) {
        if verify_vote_signature(vote, &bundle.computors) {
            valid += 1;
        }
        if valid >= required {
            return Ok(());
        }
    }
    Err(Error::Verification(format!(
        "only {valid} valid quorum signatures, need {required}"
    )))
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

fn verify_transaction_signature(transaction: &Transaction) -> bool {
    let bytes = transaction.encode();
    let digest = k12_32(&bytes[..bytes.len() - 64]);
    schnorrq::verify_digest(&transaction.source, &digest, &transaction.signature)
}

fn event_body_json(event: &LogEvent) -> String {
    let key = |bytes: [u8; 32]| PublicKey(bytes).to_identity();
    let value = match event.body() {
        EventBody::QuTransfer { from, to, amount } => serde_json::json!({
            "from": key(from),
            "to": key(to),
            "amount": amount.to_string(),
        }),
        EventBody::AssetIssuance { asset, shares } => serde_json::json!({
            "issuer": key(asset.issuer),
            "name": asset.name,
            "decimals": asset.decimals,
            "unit": asset.unit,
            "managingContract": asset.managing_contract,
            "shares": shares.to_string(),
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
            "managingContract": asset.managing_contract,
            "shares": shares.to_string(),
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
            "managingContract": asset.managing_contract,
            "shares": shares.to_string(),
        }),
        EventBody::Burning {
            from,
            amount,
            contract_index,
        } => serde_json::json!({
            "from": key(from),
            "amount": amount.to_string(),
            "contractIndex": contract_index,
        }),
        EventBody::DustBurning { burns } => serde_json::json!({
            "burns": burns
                .into_iter()
                .map(|(who, amount)| serde_json::json!({ "identity": key(who), "amount": amount.to_string() }))
                .collect::<Vec<_>>(),
        }),
        EventBody::ContractMessage {
            contract_index,
            message_type,
            data,
        } => serde_json::json!({
            "contractIndex": contract_index,
            "messageType": message_type,
            "data": hex::encode(data),
        }),
        EventBody::Custom { op_code, data } => serde_json::json!({
            "opCode": op_code.to_string(),
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
        assert!(json.contains("\"42\""));
    }
}
