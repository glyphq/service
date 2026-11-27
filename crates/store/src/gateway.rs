//! Operational ClickHouse state for the API mempool and webhooks.
//!
//! This module is intentionally separate from the canonical chain schema. It
//! stores best-effort gateway state, not indexed Qubic data.
//!
//! Every table is a `ReplacingMergeTree(version)` keyed by one logical row:
//! `hash` for mempool and tracked transaction entries, `id` for webhooks, and
//! `id` for delivery attempts. Updates append a new millisecond version, so
//! reads resolve the newest row with `LIMIT 1 BY` and replaying an update is
//! idempotent.
//!
//! Optional timestamps and counters use zero sentinels instead of ClickHouse
//! `Nullable` columns. That keeps the RowBinary shape simple and avoids a
//! nullable-`DateTime64` adapter for every `Option<DateTime<Utc>>`.

use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use clickhouse::Row;
use serde::{Deserialize, Serialize};

use crate::{ClickHouseStore, Error, Result};

/// Lifecycle of one mempool entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MempoolStatus {
    Queued,
    Broadcast,
    Confirmed,
    Expired,
    Failed,
    Cancelled,
}

impl MempoolStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Broadcast => "broadcast",
            Self::Confirmed => "confirmed",
            Self::Expired => "expired",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "broadcast" => Some(Self::Broadcast),
            "confirmed" => Some(Self::Confirmed),
            "expired" => Some(Self::Expired),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    /// Statuses the background tracker still has work for.
    pub fn is_open(self) -> bool {
        matches!(self, Self::Queued | Self::Broadcast | Self::Failed)
    }
}

/// One submitted transaction and its best-effort broadcast state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MempoolEntry {
    pub hash: String,
    /// Signed transaction bytes as lowercase hex.
    pub raw: String,
    pub source: String,
    pub destination: String,
    pub amount: i64,
    /// Target tick from the transaction header.
    pub tick: u32,
    /// Number of peers requested for the first broadcast.
    pub peers: u8,
    pub status: MempoolStatus,
    /// Total broadcast attempts, including failed attempts.
    pub attempts: u8,
    /// Peers that accepted the packet during the most recent attempt.
    pub broadcast_peers: u8,
    pub broadcast_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub confirmed_tick: Option<u32>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MempoolEntry {
    pub fn is_open(&self) -> bool {
        self.status.is_open()
    }
}

/// Durable public projection of a transaction submitted through the API.
///
/// Unlike [`MempoolEntry`], this row never carries signed bytes and outlives
/// the operational mempool. The index remains authoritative: a public read
/// prefers an indexed row and only falls back here while the transaction is
/// pending or terminal without inclusion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrackedTransaction {
    pub hash: String,
    pub source: String,
    pub destination: String,
    pub amount: i64,
    pub target_tick: u32,
    pub input_type: u16,
    pub input_data: String,
    pub status: MempoolStatus,
    pub attempts: u8,
    pub broadcast_peers: u8,
    pub broadcast_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub confirmed_tick: Option<u32>,
    pub expiry_tick: u32,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// One registered webhook endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Webhook {
    pub id: String,
    /// Stored target, encrypted when the API has a key. Never serialized.
    #[serde(skip_serializing)]
    pub url: String,
    /// HMAC signing key. Never serialized into API responses.
    #[serde(skip_serializing)]
    pub secret: String,
    pub events: Vec<String>,
    /// Empty means every transaction hash.
    pub transaction_hashes: Vec<String>,
    /// Empty means every source identity.
    pub identity: String,
    /// Optional `{issuer}/{name}` asset filter for on-chain events.
    pub asset: String,
    /// Optional contract index filter for on-chain events. Zero means any.
    pub contract_index: u32,
    pub active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// One persisted webhook delivery attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookDelivery {
    pub id: String,
    pub webhook_id: String,
    pub event: String,
    pub transaction_hash: String,
    /// `pending`, `delivered`, or `failed` (dead-lettered).
    pub status: String,
    /// Number of completed attempts.
    pub attempt: u32,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub response_status: Option<u16>,
    pub error: Option<String>,
    /// Exact JSON body sent to the endpoint, so retries are byte-identical.
    pub payload: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct MempoolRow {
    hash: String,
    raw: String,
    source: String,
    destination: String,
    amount: i64,
    tick: u32,
    peers: u8,
    status: String,
    attempts: u8,
    broadcast_peers: u8,
    broadcast_at: u64,
    confirmed_at: u64,
    confirmed_tick: u32,
    error: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    created_at: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    updated_at: DateTime<Utc>,
    version: u64,
}

impl From<&MempoolEntry> for MempoolRow {
    fn from(entry: &MempoolEntry) -> Self {
        Self {
            hash: entry.hash.clone(),
            raw: entry.raw.clone(),
            source: entry.source.clone(),
            destination: entry.destination.clone(),
            amount: entry.amount,
            tick: entry.tick,
            peers: entry.peers,
            status: entry.status.as_str().to_owned(),
            attempts: entry.attempts,
            broadcast_peers: entry.broadcast_peers,
            broadcast_at: millis(entry.broadcast_at),
            confirmed_at: millis(entry.confirmed_at),
            confirmed_tick: entry.confirmed_tick.unwrap_or(0),
            error: entry.error.clone().unwrap_or_default(),
            created_at: entry.created_at,
            updated_at: entry.updated_at,
            version: 0,
        }
    }
}

impl From<MempoolRow> for MempoolEntry {
    fn from(row: MempoolRow) -> Self {
        Self {
            hash: row.hash,
            raw: row.raw,
            source: row.source,
            destination: row.destination,
            amount: row.amount,
            tick: row.tick,
            peers: row.peers,
            status: MempoolStatus::parse(&row.status).unwrap_or(MempoolStatus::Queued),
            attempts: row.attempts,
            broadcast_peers: row.broadcast_peers,
            broadcast_at: from_millis(row.broadcast_at),
            confirmed_at: from_millis(row.confirmed_at),
            confirmed_tick: (row.confirmed_tick != 0).then_some(row.confirmed_tick),
            error: (!row.error.is_empty()).then_some(row.error),
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct TrackedTransactionRow {
    hash: String,
    source: String,
    destination: String,
    amount: i64,
    target_tick: u32,
    input_type: u16,
    input_data: String,
    status: String,
    attempts: u8,
    broadcast_peers: u8,
    broadcast_at: u64,
    confirmed_at: u64,
    confirmed_tick: u32,
    expiry_tick: u32,
    error: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    created_at: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    updated_at: DateTime<Utc>,
    version: u64,
}

impl From<&TrackedTransaction> for TrackedTransactionRow {
    fn from(transaction: &TrackedTransaction) -> Self {
        Self {
            hash: transaction.hash.clone(),
            source: transaction.source.clone(),
            destination: transaction.destination.clone(),
            amount: transaction.amount,
            target_tick: transaction.target_tick,
            input_type: transaction.input_type,
            input_data: transaction.input_data.clone(),
            status: transaction.status.as_str().to_owned(),
            attempts: transaction.attempts,
            broadcast_peers: transaction.broadcast_peers,
            broadcast_at: millis(transaction.broadcast_at),
            confirmed_at: millis(transaction.confirmed_at),
            confirmed_tick: transaction.confirmed_tick.unwrap_or(0),
            expiry_tick: transaction.expiry_tick,
            error: transaction.error.clone().unwrap_or_default(),
            created_at: transaction.created_at,
            updated_at: transaction.updated_at,
            version: 0,
        }
    }
}

impl From<TrackedTransactionRow> for TrackedTransaction {
    fn from(row: TrackedTransactionRow) -> Self {
        Self {
            hash: row.hash,
            source: row.source,
            destination: row.destination,
            amount: row.amount,
            target_tick: row.target_tick,
            input_type: row.input_type,
            input_data: row.input_data,
            status: MempoolStatus::parse(&row.status).unwrap_or(MempoolStatus::Queued),
            attempts: row.attempts,
            broadcast_peers: row.broadcast_peers,
            broadcast_at: from_millis(row.broadcast_at),
            confirmed_at: from_millis(row.confirmed_at),
            confirmed_tick: (row.confirmed_tick != 0).then_some(row.confirmed_tick),
            expiry_tick: row.expiry_tick,
            error: (!row.error.is_empty()).then_some(row.error),
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct WebhookRow {
    id: String,
    url: String,
    secret: String,
    events: String,
    transaction_hashes: String,
    identity: String,
    asset: String,
    contract_index: u32,
    active: u8,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    created_at: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    updated_at: DateTime<Utc>,
    version: u64,
}

impl From<&Webhook> for WebhookRow {
    fn from(webhook: &Webhook) -> Self {
        Self {
            id: webhook.id.clone(),
            url: webhook.url.clone(),
            secret: webhook.secret.clone(),
            events: serde_json::to_string(&webhook.events).unwrap_or_else(|_| "[]".into()),
            transaction_hashes: serde_json::to_string(&webhook.transaction_hashes)
                .unwrap_or_else(|_| "[]".into()),
            identity: webhook.identity.clone(),
            asset: webhook.asset.clone(),
            contract_index: webhook.contract_index,
            active: u8::from(webhook.active),
            created_at: webhook.created_at,
            updated_at: webhook.updated_at,
            version: 0,
        }
    }
}

impl From<WebhookRow> for Webhook {
    fn from(row: WebhookRow) -> Self {
        Self {
            id: row.id,
            url: row.url,
            secret: row.secret,
            events: serde_json::from_str(&row.events).unwrap_or_default(),
            transaction_hashes: serde_json::from_str(&row.transaction_hashes).unwrap_or_default(),
            identity: row.identity,
            asset: row.asset,
            contract_index: row.contract_index,
            active: row.active != 0,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct WebhookCursorRow {
    id: String,
    tick: u32,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    updated_at: DateTime<Utc>,
    version: u64,
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct DeliveryRow {
    id: String,
    webhook_id: String,
    event: String,
    transaction_hash: String,
    status: String,
    attempt: u32,
    next_attempt_at: u64,
    response_status: u16,
    error: String,
    payload: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    created_at: DateTime<Utc>,
    version: u64,
}

impl From<&WebhookDelivery> for DeliveryRow {
    fn from(delivery: &WebhookDelivery) -> Self {
        Self {
            id: delivery.id.clone(),
            webhook_id: delivery.webhook_id.clone(),
            event: delivery.event.clone(),
            transaction_hash: delivery.transaction_hash.clone(),
            status: delivery.status.clone(),
            attempt: delivery.attempt,
            next_attempt_at: millis(delivery.next_attempt_at),
            response_status: delivery.response_status.unwrap_or(0),
            error: delivery.error.clone().unwrap_or_default(),
            payload: delivery.payload.clone(),
            created_at: delivery.created_at,
            version: 0,
        }
    }
}

impl From<DeliveryRow> for WebhookDelivery {
    fn from(row: DeliveryRow) -> Self {
        Self {
            id: row.id,
            webhook_id: row.webhook_id,
            event: row.event,
            transaction_hash: row.transaction_hash,
            status: row.status,
            attempt: row.attempt,
            next_attempt_at: from_millis(row.next_attempt_at),
            response_status: (row.response_status != 0).then_some(row.response_status),
            error: (!row.error.is_empty()).then_some(row.error),
            payload: row.payload,
            created_at: row.created_at,
        }
    }
}

#[derive(Debug, Row, Deserialize)]
struct CountRow {
    n: u64,
}

#[derive(Debug, Row, Deserialize)]
struct MempoolCountsRow {
    source_count: u64,
    total: u64,
}

fn millis(value: Option<DateTime<Utc>>) -> u64 {
    value
        .and_then(|value| u64::try_from(value.timestamp_millis()).ok())
        .unwrap_or(0)
}

fn from_millis(value: u64) -> Option<DateTime<Utc>> {
    if value == 0 {
        return None;
    }
    let millis = i64::try_from(value).ok()?;
    DateTime::from_timestamp_millis(millis)
}

/// ClickHouse-backed gateway state shared by the mempool and webhook services.
pub struct GatewayStore {
    client: clickhouse::Client,
    database: String,
    /// Makes versions strictly increasing even when two writes land in the
    /// same millisecond. `ReplacingMergeTree` resolves equal versions
    /// arbitrarily, which would make an update look lost until the next one.
    last_version: AtomicU64,
}

impl GatewayStore {
    /// Reads `CLICKHOUSE_URL` and `CLICKHOUSE_DB` with the same defaults as the
    /// API and indexer binaries.
    pub fn from_env() -> Result<Self> {
        let url =
            std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".into());
        let database = std::env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "glyph".into());
        Self::new(&url, &database)
    }

    /// Builds a gateway client over the same connection settings as the
    /// canonical store, including URL-embedded credentials and database
    /// validation.
    pub fn new(url: &str, database: &str) -> Result<Self> {
        let store = ClickHouseStore::new(url, database)?;
        Ok(Self {
            client: store.client.clone(),
            database: database.to_owned(),
            last_version: AtomicU64::new(0),
        })
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    /// Creates the gateway tables. Safe to call on every startup.
    pub async fn init(&self) -> Result<()> {
        for statement in create_statements(&self.database) {
            self.client.query(&statement).execute().await?;
        }
        for statement in retention_statements(&self.database) {
            self.client.query(&statement).execute().await?;
        }
        Ok(())
    }

    fn next_version(&self) -> u64 {
        let now = u64::try_from(Utc::now().timestamp_millis())
            .unwrap_or(0)
            .max(1);
        let mut current = self.last_version.load(Ordering::Relaxed);
        loop {
            let next = now.max(current + 1);
            match self.last_version.compare_exchange_weak(
                current,
                next,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return next,
                Err(observed) => current = observed,
            }
        }
    }

    /// Appends the entry with a fresh version. Later reads resolve it.
    pub async fn insert_mempool(&self, entry: &MempoolEntry) -> Result<()> {
        let mut insert = self.client.insert::<MempoolRow>("mempool")?;
        insert.write(&self.mempool_row(entry)).await?;
        insert.end().await?;
        Ok(())
    }

    /// Replaces an existing mempool entry with a fresh version.
    pub async fn update_mempool(&self, entry: &MempoolEntry) -> Result<()> {
        self.insert_mempool(entry).await
    }

    fn mempool_row(&self, entry: &MempoolEntry) -> MempoolRow {
        let mut row = MempoolRow::from(entry);
        row.version = self.next_version();
        row
    }

    pub async fn mempool_entry(&self, hash: &str) -> Result<Option<MempoolEntry>> {
        let row = self
            .client
            .query("SELECT ?fields FROM mempool WHERE hash = ? ORDER BY version DESC LIMIT 1")
            .bind(hash)
            .fetch_optional::<MempoolRow>()
            .await?;
        Ok(row.map(Into::into))
    }

    /// Appends a durable public projection of one API transaction.
    pub async fn put_tracked_transaction(&self, transaction: &TrackedTransaction) -> Result<()> {
        let mut row = TrackedTransactionRow::from(transaction);
        row.version = self.next_version();
        let mut insert = self
            .client
            .insert::<TrackedTransactionRow>("tracked_transactions")?;
        insert.write(&row).await?;
        insert.end().await?;
        Ok(())
    }

    pub async fn tracked_transaction(&self, hash: &str) -> Result<Option<TrackedTransaction>> {
        let row = self
            .client
            .query(
                "SELECT ?fields FROM tracked_transactions WHERE hash = ? \
                 ORDER BY version DESC LIMIT 1",
            )
            .bind(hash)
            .fetch_optional::<TrackedTransactionRow>()
            .await?;
        Ok(row.map(Into::into))
    }

    /// Newest version of every entry, newest submission first.
    pub async fn mempool_entries(
        &self,
        status: Option<MempoolStatus>,
        limit: u32,
    ) -> Result<Vec<MempoolEntry>> {
        let limit = limit.clamp(1, 500);
        let rows = match status {
            Some(status) => {
                self.client
                    .query(
                        "SELECT ?fields FROM (\
                             SELECT * FROM mempool \
                             ORDER BY version DESC LIMIT 1 BY hash\
                         ) WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                    )
                    .bind(status.as_str())
                    .bind(limit)
                    .fetch_all::<MempoolRow>()
                    .await?
            }
            None => {
                self.client
                    .query(
                        "SELECT ?fields FROM (\
                             SELECT * FROM mempool \
                             ORDER BY version DESC LIMIT 1 BY hash\
                         ) ORDER BY created_at DESC LIMIT ?",
                    )
                    .bind(limit)
                    .fetch_all::<MempoolRow>()
                    .await?
            }
        };
        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Entries the background tracker should still consider, oldest first.
    pub async fn open_mempool_entries(
        &self,
        limit: u32,
        max_attempts: u8,
    ) -> Result<Vec<MempoolEntry>> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM (\
                     SELECT * FROM mempool \
                     ORDER BY version DESC LIMIT 1 BY hash\
                 ) WHERE (status IN ('queued', 'broadcast') \
                     OR (status = 'failed' AND attempts < ?)) \
                 ORDER BY created_at ASC LIMIT ?",
            )
            .bind(max_attempts)
            .bind(limit.clamp(1, 10_000))
            .fetch_all::<MempoolRow>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Open entries overall and for one source, used to enforce both capacity
    /// caps in a single scan.
    pub async fn open_mempool_counts(&self, source: &str, max_attempts: u8) -> Result<(u64, u64)> {
        let row = self
            .client
            .query(
                "SELECT countIf(source = ?) AS source_count, count() AS total FROM (\
                     SELECT source, status, attempts FROM mempool \
                     ORDER BY version DESC LIMIT 1 BY hash\
                 ) WHERE (status IN ('queued', 'broadcast') \
                     OR (status = 'failed' AND attempts < ?))",
            )
            .bind(source)
            .bind(max_attempts)
            .fetch_one::<MempoolCountsRow>()
            .await?;
        Ok((row.source_count, row.total))
    }

    pub async fn insert_webhook(&self, webhook: &Webhook) -> Result<()> {
        let mut row = WebhookRow::from(webhook);
        row.version = self.next_version();
        let mut insert = self.client.insert::<WebhookRow>("webhooks")?;
        insert.write(&row).await?;
        insert.end().await?;
        Ok(())
    }

    pub async fn update_webhook(&self, webhook: &Webhook) -> Result<()> {
        self.insert_webhook(webhook).await
    }

    pub async fn webhook(&self, id: &str) -> Result<Option<Webhook>> {
        let row = self
            .client
            .query("SELECT ?fields FROM webhooks WHERE id = ? ORDER BY version DESC LIMIT 1")
            .bind(id)
            .fetch_optional::<WebhookRow>()
            .await?;
        Ok(row.map(Into::into))
    }

    /// Active webhooks, newest first.
    pub async fn active_webhooks(&self) -> Result<Vec<Webhook>> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM (\
                     SELECT * FROM webhooks \
                     ORDER BY version DESC LIMIT 1 BY id\
                 ) WHERE active = 1 ORDER BY created_at DESC",
            )
            .fetch_all::<WebhookRow>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Closed webhooks, newest first. The dispatcher deletes them once their
    /// deliveries are terminal.
    pub async fn inactive_webhooks(&self, limit: u32) -> Result<Vec<Webhook>> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM (\
                     SELECT * FROM webhooks \
                     ORDER BY version DESC LIMIT 1 BY id\
                 ) WHERE active = 0 ORDER BY updated_at ASC LIMIT ?",
            )
            .bind(limit.clamp(1, 1_000))
            .fetch_all::<WebhookRow>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn has_pending_delivery(&self, webhook_id: &str) -> Result<bool> {
        let row = self
            .client
            .query(
                "SELECT count() AS n FROM (\
                     SELECT webhook_id, status FROM webhook_deliveries \
                     ORDER BY version DESC LIMIT 1 BY id\
                ) WHERE webhook_id = ? AND status IN ('pending', 'sending')",
            )
            .bind(webhook_id)
            .fetch_one::<CountRow>()
            .await?;
        Ok(row.n > 0)
    }

    /// Last tick fully processed by the on-chain webhook source.
    pub async fn webhook_cursor(&self, id: &str) -> Result<Option<u32>> {
        let row = self
            .client
            .query(
                "SELECT ?fields FROM webhook_cursor WHERE id = ? \
                 ORDER BY version DESC LIMIT 1",
            )
            .bind(id)
            .fetch_optional::<WebhookCursorRow>()
            .await?;
        Ok(row.map(|row| row.tick))
    }

    pub async fn set_webhook_cursor(&self, id: &str, tick: u32) -> Result<()> {
        let row = WebhookCursorRow {
            id: id.to_owned(),
            tick,
            updated_at: Utc::now(),
            version: self.next_version(),
        };
        let mut insert = self.client.insert::<WebhookCursorRow>("webhook_cursor")?;
        insert.write(&row).await?;
        insert.end().await?;
        Ok(())
    }

    pub async fn delete_webhook(&self, id: &str) -> Result<()> {
        self.client
            .query("ALTER TABLE webhooks DELETE WHERE id = ? SETTINGS mutations_sync = 2")
            .bind(id)
            .execute()
            .await?;
        self.client
            .query(
                "ALTER TABLE webhook_deliveries DELETE WHERE webhook_id = ? \
                 SETTINGS mutations_sync = 2",
            )
            .bind(id)
            .execute()
            .await?;
        Ok(())
    }

    /// Appends one delivery attempt. The same delivery id keeps its logical
    /// identity while its attempt counter and next attempt advance.
    pub async fn insert_delivery(&self, delivery: &WebhookDelivery) -> Result<()> {
        let mut row = DeliveryRow::from(delivery);
        row.version = self.next_version();
        let mut insert = self.client.insert::<DeliveryRow>("webhook_deliveries")?;
        insert.write(&row).await?;
        insert.end().await?;
        Ok(())
    }

    /// Recent attempts for one webhook, newest first.
    pub async fn deliveries(&self, webhook_id: &str, limit: u32) -> Result<Vec<WebhookDelivery>> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM (\
                     SELECT * FROM webhook_deliveries \
                     ORDER BY version DESC LIMIT 1 BY id\
                 ) WHERE webhook_id = ? ORDER BY created_at DESC LIMIT ?",
            )
            .bind(webhook_id)
            .bind(limit.clamp(1, 500))
            .fetch_all::<DeliveryRow>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Deliveries that are due for their next attempt.
    pub async fn due_deliveries(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<WebhookDelivery>> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM (\
                     SELECT * FROM webhook_deliveries \
                     ORDER BY version DESC LIMIT 1 BY id\
                 ) WHERE status = 'pending' AND next_attempt_at > 0 AND next_attempt_at <= ? \
                 ORDER BY next_attempt_at ASC LIMIT ?",
            )
            .bind(millis(Some(now)))
            .bind(limit.clamp(1, 500))
            .fetch_all::<DeliveryRow>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Deliveries claimed by a dispatcher that never wrote an outcome.
    pub async fn stale_sending_deliveries(
        &self,
        before: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<WebhookDelivery>> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM (\
                     SELECT * FROM webhook_deliveries \
                     ORDER BY version DESC LIMIT 1 BY id\
                 ) WHERE status = 'sending' AND next_attempt_at > 0 AND next_attempt_at <= ? \
                 ORDER BY next_attempt_at ASC LIMIT ?",
            )
            .bind(millis(Some(before)))
            .bind(limit.clamp(1, 500))
            .fetch_all::<DeliveryRow>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }
}

/// DDL for the operational tables. Kept as SQL so migrations stay reviewable.
///
/// The database name is interpolated as an identifier and arrives from
/// [`ClickHouseStore::new`], which validates it before a client exists.
fn create_statements(database: &str) -> Vec<String> {
    vec![
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.mempool\n(\n\
                 hash String,\n\
                 raw String CODEC(ZSTD(3)),\n\
                 source String,\n\
                 destination String,\n\
                 amount Int64,\n\
                 tick UInt32,\n\
                 peers UInt8,\n\
                 status LowCardinality(String),\n\
                 attempts UInt8,\n\
                 broadcast_peers UInt8,\n\
                 broadcast_at UInt64,\n\
                 confirmed_at UInt64,\n\
                 confirmed_tick UInt32,\n\
                 error String,\n\
                 created_at DateTime64(3, 'UTC'),\n\
                 updated_at DateTime64(3, 'UTC'),\n\
                 version UInt64\n\
             )\n\
             ENGINE = ReplacingMergeTree(version)\n\
             ORDER BY hash\n\
             TTL updated_at + INTERVAL 7 DAY DELETE"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.tracked_transactions\n(\n\
                 hash String,\n\
                 source String,\n\
                 destination String,\n\
                 amount Int64,\n\
                 target_tick UInt32,\n\
                 input_type UInt16,\n\
                 input_data String CODEC(ZSTD(3)),\n\
                 status LowCardinality(String),\n\
                 attempts UInt8,\n\
                 broadcast_peers UInt8,\n\
                 broadcast_at UInt64,\n\
                 confirmed_at UInt64,\n\
                 confirmed_tick UInt32,\n\
                 expiry_tick UInt32,\n\
                 error String,\n\
                 created_at DateTime64(3, 'UTC'),\n\
                 updated_at DateTime64(3, 'UTC'),\n\
                 version UInt64\n\
             )\n\
             ENGINE = ReplacingMergeTree(version)\n\
             ORDER BY hash\n\
             TTL updated_at + INTERVAL 30 DAY DELETE WHERE status != 'failed'"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.webhooks\n(\n\
                 id String,\n\
                 url String,\n\
                 secret String,\n\
                 events String,\n\
                 transaction_hashes String,\n\
                 identity String,\n\
                 asset String,\n\
                 contract_index UInt32,\n\
                 active UInt8,\n\
                 created_at DateTime64(3, 'UTC'),\n\
                 updated_at DateTime64(3, 'UTC'),\n\
                 version UInt64\n\
             )\n\
             ENGINE = ReplacingMergeTree(version)\n\
             ORDER BY id\n\
             TTL updated_at + INTERVAL 7 DAY DELETE WHERE active = 0"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.webhook_deliveries\n(\n\
                 id String,\n\
                 webhook_id String,\n\
                 event String,\n\
                 transaction_hash String,\n\
                 status LowCardinality(String),\n\
                 attempt UInt32,\n\
                 next_attempt_at UInt64,\n\
                 response_status UInt16,\n\
                 error String,\n\
                 payload String CODEC(ZSTD(3)),\n\
                 created_at DateTime64(3, 'UTC'),\n\
                 version UInt64\n\
             )\n\
             ENGINE = ReplacingMergeTree(version)\n\
             ORDER BY id\n\
             TTL created_at + INTERVAL 1 DAY DELETE \
                 WHERE status IN ('delivered', 'failed')"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {database}.webhook_cursor\n(\n\
                 id String,\n\
                 tick UInt32,\n\
                 updated_at DateTime64(3, 'UTC'),\n\
                 version UInt64\n\
             )\n\
             ENGINE = ReplacingMergeTree(version)\n\
             ORDER BY id"
        ),
    ]
}

/// Applies retention to tables created before the TTL clauses were added.
fn retention_statements(database: &str) -> Vec<String> {
    vec![
        format!("ALTER TABLE {database}.mempool MODIFY TTL updated_at + INTERVAL 7 DAY DELETE"),
        format!(
            "ALTER TABLE {database}.tracked_transactions MODIFY TTL \
             updated_at + INTERVAL 30 DAY DELETE WHERE status != 'failed'"
        ),
        format!(
            "ALTER TABLE {database}.webhooks MODIFY TTL updated_at + INTERVAL 7 DAY DELETE \
             WHERE active = 0"
        ),
        format!(
            "ALTER TABLE {database}.webhook_deliveries MODIFY TTL created_at + INTERVAL 1 DAY \
             DELETE WHERE status IN ('delivered', 'failed')"
        ),
        format!(
            "ALTER TABLE {database}.webhooks ADD COLUMN IF NOT EXISTS asset String \
             AFTER identity"
        ),
        format!(
            "ALTER TABLE {database}.webhooks ADD COLUMN IF NOT EXISTS contract_index UInt32 \
             AFTER asset"
        ),
    ]
}

/// Validates a hash before it is used as a primary-key filter.
pub fn normalize_hash(hash: &str) -> Result<String> {
    let hash = hash.trim().to_ascii_lowercase();
    if hash.is_empty() {
        return Err(Error::Config("transaction hash is empty".into()));
    }
    Ok(hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_creates_all_gateway_tables_with_versions() {
        let statements = create_statements("glyph").join("\n");
        for table in [
            "mempool",
            "tracked_transactions",
            "webhooks",
            "webhook_deliveries",
            "webhook_cursor",
        ] {
            assert!(
                statements.contains(&format!("glyph.{table}")),
                "missing table {table}"
            );
        }
        assert_eq!(statements.matches("ReplacingMergeTree(version)").count(), 5);
        assert!(statements.contains("created_at DateTime64(3, 'UTC')"));
        assert!(statements.contains("ORDER BY hash"));
        assert!(statements.contains("ORDER BY id"));
        assert_eq!(statements.matches("TTL ").count(), 4);
        assert!(statements.contains("WHERE active = 0"));
        assert!(statements.contains("WHERE status IN ('delivered', 'failed')"));
        assert!(statements.contains("TTL updated_at + INTERVAL 7 DAY DELETE"));
        assert!(
            statements.contains("TTL updated_at + INTERVAL 30 DAY DELETE WHERE status != 'failed'")
        );
        assert!(statements.contains("TTL updated_at + INTERVAL 7 DAY DELETE WHERE active = 0"));
        assert!(statements.contains("TTL created_at + INTERVAL 1 DAY DELETE"));
    }

    #[test]
    fn retention_statements_apply_to_existing_tables() {
        let statements = retention_statements("glyph").join("\n");
        for table in [
            "mempool",
            "tracked_transactions",
            "webhooks",
            "webhook_deliveries",
        ] {
            assert!(
                statements.contains(&format!("ALTER TABLE glyph.{table} MODIFY TTL")),
                "missing retention for {table}"
            );
        }
        assert!(statements.contains("mempool MODIFY TTL updated_at + INTERVAL 7 DAY DELETE"));
        assert!(statements.contains(
            "tracked_transactions MODIFY TTL updated_at + INTERVAL 30 DAY DELETE \
                     WHERE status != 'failed'"
        ));
        assert!(statements.contains("webhooks MODIFY TTL updated_at + INTERVAL 7 DAY DELETE"));
        assert!(
            statements.contains("webhook_deliveries MODIFY TTL created_at + INTERVAL 1 DAY DELETE")
        );
    }

    #[test]
    fn status_round_trips_and_marks_open_states() {
        for status in [
            MempoolStatus::Queued,
            MempoolStatus::Broadcast,
            MempoolStatus::Confirmed,
            MempoolStatus::Expired,
            MempoolStatus::Failed,
            MempoolStatus::Cancelled,
        ] {
            assert_eq!(MempoolStatus::parse(status.as_str()), Some(status));
        }
        assert!(MempoolStatus::Queued.is_open());
        assert!(MempoolStatus::Broadcast.is_open());
        assert!(MempoolStatus::Failed.is_open());
        assert!(!MempoolStatus::Confirmed.is_open());
        assert!(!MempoolStatus::Expired.is_open());
        assert!(!MempoolStatus::Cancelled.is_open());
        assert_eq!(MempoolStatus::parse("unknown"), None);
    }

    #[test]
    fn mempool_row_round_trips_zero_sentinels() {
        let entry = MempoolEntry {
            hash: "ab".into(),
            raw: "00".into(),
            source: "SRC".into(),
            destination: "DST".into(),
            amount: 10,
            tick: 42,
            peers: 3,
            status: MempoolStatus::Queued,
            attempts: 0,
            broadcast_peers: 0,
            broadcast_at: None,
            confirmed_at: None,
            confirmed_tick: None,
            error: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let row = MempoolRow::from(&entry);
        assert_eq!(row.broadcast_at, 0);
        assert_eq!(row.confirmed_tick, 0);
        assert!(row.error.is_empty());
        let back: MempoolEntry = row.into();
        assert_eq!(back.status, MempoolStatus::Queued);
        assert_eq!(back.broadcast_at, None);
        assert_eq!(back.confirmed_tick, None);
        assert_eq!(back.error, None);
    }

    #[test]
    fn tracked_transaction_row_round_trips_without_raw_bytes() {
        let transaction = TrackedTransaction {
            hash: "ab".into(),
            source: "SRC".into(),
            destination: "DST".into(),
            amount: 10,
            target_tick: 42,
            input_type: 1,
            input_data: "deadbeef".into(),
            status: MempoolStatus::Failed,
            attempts: 10,
            broadcast_peers: 0,
            broadcast_at: None,
            confirmed_at: None,
            confirmed_tick: None,
            expiry_tick: 52,
            error: Some("no peer accepted the packet".into()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let row = TrackedTransactionRow::from(&transaction);
        let back: TrackedTransaction = row.into();
        assert_eq!(back.status, MempoolStatus::Failed);
        assert_eq!(back.input_type, 1);
        assert_eq!(back.input_data, "deadbeef");
        assert_eq!(back.expiry_tick, 52);
        assert_eq!(back.error.as_deref(), Some("no peer accepted the packet"));
    }

    #[test]
    fn webhook_url_and_secret_are_never_serialized() {
        let webhook = Webhook {
            id: "id".into(),
            url: "https://example.com/hook".into(),
            secret: "0123456789abcdef".into(),
            events: vec!["transaction.confirmed".into()],
            transaction_hashes: Vec::new(),
            identity: String::new(),
            asset: String::new(),
            contract_index: 0,
            active: true,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let encoded = serde_json::to_string(&webhook).unwrap();
        assert!(!encoded.contains("0123456789abcdef"));
        assert!(!encoded.contains("example.com"));
        assert!(encoded.contains("transaction.confirmed"));
    }

    #[test]
    fn normalize_hash_lowercases_and_rejects_empty() {
        assert_eq!(normalize_hash(" ABCD ").unwrap(), "abcd");
        assert!(normalize_hash("  ").is_err());
    }

    #[test]
    fn constructor_reuses_database_validation() {
        assert!(GatewayStore::new("http://127.0.0.1:8123", "glyph").is_ok());
        assert!(GatewayStore::new("http://127.0.0.1:8123", "glyph; DROP").is_err());
        assert!(GatewayStore::new("", "glyph").is_err());
    }
}
