//! ClickHouse-backed storage for the glyph indexer and API.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub mod rows;
pub mod schema;
pub mod writer;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("clickhouse: {0}")]
    ClickHouse(#[from] clickhouse::error::Error),
    #[error("configuration: {0}")]
    Config(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("invalid batch: {0}")]
    InvalidBatch(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Cursor key advanced after a tick has been fully persisted.
pub const CURSOR_HEAD: &str = "head";
/// Cursor key holding the typed [`IngestCursor`] JSON document.
pub const CURSOR_INGEST: &str = "ingest";

/// JSON value stored under [`CURSOR_HEAD`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadCursor {
    pub epoch: u16,
    pub tick: u32,
    pub timestamp: DateTime<Utc>,
}

/// Typed ingest progress stored under [`CURSOR_INGEST`].
///
/// `last_indexed_tick` is the highest fully persisted tick. Writers never move
/// it backwards, so replaying an older tick cannot corrupt the cursor.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestCursor {
    pub last_indexed_tick: Option<u32>,
    pub epoch: Option<u16>,
    pub verification: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickRow {
    pub epoch: u16,
    pub tick: u32,
    pub timestamp: DateTime<Utc>,
    pub computor_index: u16,
    pub transaction_count: u32,
    pub event_count: u32,
    pub verification: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionRow {
    pub epoch: u16,
    pub tick: u32,
    pub tx_index: u16,
    pub hash: String,
    pub source: String,
    pub destination: String,
    pub amount: i64,
    pub input_type: u16,
    pub input_data: String,
    pub status: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventRow {
    pub epoch: u16,
    pub tick: u32,
    pub log_id: u64,
    pub log_type: u16,
    pub type_name: String,
    pub tx_hash: String,
    pub tx_index: i16,
    pub body: String,
    pub log_digest: u64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityRow {
    pub identity: String,
    pub public_key: String,
    pub balance: i64,
    pub incoming_amount: i64,
    pub outgoing_amount: i64,
    pub transfer_count: u64,
    pub first_seen_tick: u32,
    pub last_seen_tick: u32,
}

/// One `(event log, identity, role)` pair extracted from a decoded event body.
///
/// The events table stores participants inside the JSON `body`; this row is the
/// normalized index that makes per-identity event lookups a primary-key read.
/// Rows are keyed by `(identity, tick, log_id)` and versioned like the rest of
/// the store, so re-indexing a tick is idempotent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventParticipantRow {
    pub epoch: u16,
    pub tick: u32,
    pub log_id: u64,
    pub identity: String,
    pub role: String,
    pub tx_hash: String,
    pub tx_index: i16,
    pub timestamp: DateTime<Utc>,
}

/// One elected computor for an epoch.
///
/// `public_key` is the 0x-prefixed hex form of the raw 32-byte key and
/// `identity` is the canonical uppercase base-26 form. `computor_index` is the
/// position in the epoch's computor message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputorRow {
    pub epoch: u16,
    pub computor_index: u16,
    pub public_key: String,
    pub identity: String,
    pub version: u64,
    pub timestamp: DateTime<Utc>,
}

/// Vote evidence behind a verified tick.
///
/// `quorum_digest` is the digest the winning vote group agreed on and
/// `tick_data_digest` is the digest of the tick data that was persisted. Vote
/// counts describe how the quorum was assembled. `warnings` is the producer's
/// serialized warning list; `warning_count` is its length.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuorumEvidenceRow {
    pub epoch: u16,
    pub tick: u32,
    pub quorum_digest: [u8; 32],
    pub tick_data_digest: [u8; 32],
    pub required_votes: u32,
    pub aligned_votes: u32,
    pub fetched_votes: u32,
    pub unique_votes: u32,
    pub verified_votes: u32,
    pub verification: String,
    pub warning_count: u32,
    pub warnings: String,
    pub timestamp: DateTime<Utc>,
    pub version: u64,
}

/// One row of a per-identity history.
///
/// Direct transactions (`kind = "transaction"`) carry the transaction fields
/// and leave `log_id`, `type_name`, `body`, and `role` empty. Event-derived rows
/// (`kind = "event"`) carry the event identity and the participant `role`, and
/// leave the transaction input/status fields empty. Transfer-shaped event
/// bodies (`from`, `to`, `amount`) also populate `source`, `destination`, and
/// `amount` so callers can render both kinds with the same code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityActivityRow {
    pub kind: String,
    pub epoch: u16,
    pub tick: u32,
    pub timestamp: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tx_index: Option<i16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_id: Option<u64>,
    pub hash: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub destination: String,
    pub amount: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_type: Option<u16>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub input_data: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub type_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub body: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub role: String,
}

/// Aggregated view of one epoch derived from the `ticks` table.
///
/// Epochs are not stored separately yet, so the summary is computed on read.
/// `closed` is true when a later epoch exists in the index; the last indexed
/// epoch may still be receiving ticks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochRow {
    pub epoch: u16,
    pub first_tick: u32,
    pub last_tick: u32,
    pub first_timestamp: DateTime<Utc>,
    pub last_timestamp: DateTime<Utc>,
    pub tick_count: u64,
    pub transaction_count: u64,
    pub event_count: u64,
    pub closed: bool,
}

/// Asset issuance metadata derived from `asset_issuance` events.
///
/// The planned `assets` table does not exist yet, so reads aggregate the
/// decoded issuance logs. `issued_tick` and `log_id` identify the latest
/// issuance observed for the `(issuer, name)` pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetRow {
    pub issuer: String,
    pub name: String,
    pub decimals: u8,
    pub unit: String,
    pub managing_contract: i64,
    pub shares: i64,
    pub issued_tick: u32,
    pub log_id: u64,
    pub timestamp: DateTime<Utc>,
}

/// Index-wide counts and progress for the network stats endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkStats {
    pub head: Option<TickRow>,
    pub epochs: u64,
    pub ticks: u64,
    pub transactions: u64,
    pub events: u64,
    pub identities: u64,
    pub latest_timestamp: Option<DateTime<Utc>>,
    pub ingest: IngestCursor,
}

/// One time bucket produced by [`Store::activity_analytics`].
///
/// `bucket` is the start of the interval. Counts are deduplicated across
/// replayed ticks, and `volume` is the sum of transaction amounts in the
/// bucket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityBucket {
    pub bucket: DateTime<Utc>,
    pub ticks: u64,
    pub transactions: u64,
    pub events: u64,
    pub volume: i64,
}

/// Number of events of one `type_name` produced by
/// [`Store::event_type_counts`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventTypeCount {
    pub type_name: String,
    pub count: u64,
}

/// One hour of the QUBIC/USDT oracle price.
///
/// `hour` is the logical key: a later `version` for the same hour replaces the
/// earlier row. `price_e8` is the price scaled by 1e8, and `series_digest` is
/// the raw K12 digest of the series state that produced the value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PricePointRow {
    pub hour: u32,
    pub append_tick: u32,
    pub source_tick: u32,
    pub epoch: u16,
    pub flags: u32,
    pub price_e8: i64,
    pub source: u32,
    pub synced: u8,
    pub bound: u8,
    pub series_digest: [u8; 32],
    pub observed_at: DateTime<Utc>,
    pub version: u64,
}

/// Latest stored price alongside the series metadata callers display.
///
/// `points` counts distinct hours, not physical rows, so replayed writes do not
/// inflate it. The metadata fields mirror `latest` and default to zero when the
/// series is empty, but [`Store::price_snapshot`] returns `None` in that case.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceSnapshot {
    pub latest: Option<PricePointRow>,
    pub points: u64,
    pub synced: bool,
    pub bound: bool,
    pub source: u32,
    pub series_digest: [u8; 32],
    pub epoch: u16,
    pub append_tick: u32,
    pub source_tick: u32,
    pub observed_at: DateTime<Utc>,
}

/// Filters accepted by the transaction collection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransactionFilter {
    pub epoch: Option<u16>,
    pub tick: Option<u32>,
    /// Canonical uppercase identity matched against source or destination.
    pub identity: Option<String>,
}

/// Filters accepted by the event collection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventFilter {
    pub epoch: Option<u16>,
    pub tick: Option<u32>,
    pub type_name: Option<String>,
}

/// A verified tick and everything derived from it.
///
/// Identity rows are absolute snapshots as of `tick`, not deltas, so replaying
/// the same batch is a no-op. Transaction and event rows must carry the same
/// tick as the batch; the writer uses the batch version for all of them.
#[derive(Debug, Clone)]
pub struct TickBatch {
    pub epoch: u16,
    pub tick: u32,
    pub timestamp: DateTime<Utc>,
    pub computor_index: u16,
    pub verification: String,
    /// ReplacingMergeTree version. Defaults to the tick number; bump it when
    /// replacing a tick whose contents changed (for example after a reorg).
    pub version: u64,
    /// Raw SchnorrQ signature over the tick.
    pub signature: [u8; 64],
    /// K12 digest of the tick.
    pub tick_digest: [u8; 32],
    pub transactions: Vec<TransactionRow>,
    pub events: Vec<EventRow>,
    /// Normalized participants of `events`. Empty when the producer does not
    /// publish participant identities.
    pub event_participants: Vec<EventParticipantRow>,
    pub identities: Vec<IdentityRow>,
    /// Optional quorum evidence for this tick. Written in the same batch,
    /// before the tick row, so a visible tick always has its audit row.
    pub evidence: Option<QuorumEvidenceRow>,
}

impl TickBatch {
    pub fn new(
        epoch: u16,
        tick: u32,
        timestamp: DateTime<Utc>,
        computor_index: u16,
        verification: impl Into<String>,
    ) -> Self {
        Self {
            epoch,
            tick,
            timestamp,
            computor_index,
            verification: verification.into(),
            version: u64::from(tick),
            signature: [0; 64],
            tick_digest: [0; 32],
            transactions: Vec::new(),
            events: Vec::new(),
            event_participants: Vec::new(),
            identities: Vec::new(),
            evidence: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteOutcome {
    pub tick: u32,
    pub version: u64,
    pub transactions: usize,
    pub events: usize,
    pub identities: usize,
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn init(&self) -> Result<()>;
    async fn drop_database(&self) -> Result<()>;
    async fn head(&self) -> Result<Option<TickRow>>;
    async fn tick(&self, tick: u32) -> Result<Option<TickRow>>;
    async fn transactions_for_tick(
        &self,
        tick: u32,
        limit: u32,
        offset: u64,
    ) -> Result<Vec<TransactionRow>>;
    async fn transaction(&self, hash: &str) -> Result<Option<TransactionRow>>;
    /// Batch transaction lookup by hash. At most one row per hash is returned,
    /// newest tick first. Unknown hashes are omitted.
    ///
    /// The default implementation issues one point read per hash. Stores with
    /// native batch support override it.
    async fn transactions_by_hashes(&self, hashes: &[String]) -> Result<Vec<TransactionRow>> {
        let mut seen = HashSet::new();
        let mut rows = Vec::new();
        for hash in hashes {
            if !seen.insert(hash.as_str()) {
                continue;
            }
            if let Some(row) = self.transaction(hash).await? {
                rows.push(row);
            }
        }
        rows.sort_by(|a, b| {
            b.tick
                .cmp(&a.tick)
                .then_with(|| a.tx_index.cmp(&b.tx_index))
        });
        Ok(rows)
    }
    /// Transfers where `identity` is either the source or the destination.
    async fn transactions_for_identity(
        &self,
        identity: &str,
        limit: u32,
        offset: u64,
    ) -> Result<Vec<TransactionRow>>;
    /// Most recent events across all epochs, newest tick first.
    async fn recent_events(&self, limit: u32, offset: u64) -> Result<Vec<EventRow>>;
    /// Most recent transactions across all epochs, newest tick first.
    async fn recent_transactions(&self, limit: u32, offset: u64) -> Result<Vec<TransactionRow>>;
    async fn events_for_tick(&self, tick: u32, limit: u32, offset: u64) -> Result<Vec<EventRow>>;
    /// Events where `identity` is a participant, newest tick and log first.
    ///
    /// `limit` is clamped to `1..=500`. The default implementation reports the
    /// capability as unsupported.
    async fn events_for_identity(
        &self,
        identity: &str,
        limit: u32,
        offset: u64,
    ) -> Result<Vec<EventRow>> {
        let _ = (identity, limit, offset);
        Err(Error::Config("activity storage unsupported".into()))
    }
    /// Linear per-identity history: direct transactions and event-derived rows
    /// in one shape, newest tick first.
    ///
    /// `limit` is clamped to `1..=200`. The default implementation reports the
    /// capability as unsupported.
    async fn activity_for_identity(
        &self,
        identity: &str,
        limit: u32,
        offset: u64,
    ) -> Result<Vec<IdentityActivityRow>> {
        let _ = (identity, limit, offset);
        Err(Error::Config("activity storage unsupported".into()))
    }
    /// Upsert normalized event participants. Duplicates inside one call
    /// collapse by `(epoch, log_id, identity, role)`. The tick writer calls
    /// this between the event insert and the tick row.
    async fn write_event_participants(&self, rows: &[EventParticipantRow]) -> Result<()> {
        let _ = rows;
        Err(Error::Config("activity storage unsupported".into()))
    }
    async fn identity(&self, identity: &str) -> Result<Option<IdentityRow>>;
    /// Identities with the highest transfer counts, then highest balance,
    /// then canonical identity.
    async fn top_identities(&self, limit: u32) -> Result<Vec<IdentityRow>> {
        let _ = limit;
        Err(Error::Config(
            "top_identities is not supported by this store".into(),
        ))
    }
    async fn stats(&self) -> Result<Value>;

    /// Newest ticks first, optionally restricted to one epoch.
    async fn ticks(&self, epoch: Option<u16>, limit: u32, offset: u64) -> Result<Vec<TickRow>>;
    /// Ticks strictly newer than `tick`, oldest first. Used by streaming to
    /// resume from the last delivered tick.
    async fn ticks_after(&self, tick: u32, limit: u32) -> Result<Vec<TickRow>> {
        let _ = (tick, limit);
        Err(Error::Config(
            "ticks_after is not supported by this store".into(),
        ))
    }
    /// Epoch summaries newest first, derived from `ticks`.
    async fn epochs(&self, limit: u32, offset: u64) -> Result<Vec<EpochRow>>;
    async fn epoch(&self, epoch: u16) -> Result<Option<EpochRow>>;
    /// Identities ordered by most recent activity, then canonical identity.
    async fn identities(&self, limit: u32, offset: u64) -> Result<Vec<IdentityRow>>;
    /// Assets ordered by `(issuer, name)`, optionally filtered by issuer.
    async fn assets(&self, issuer: Option<&str>, limit: u32, offset: u64) -> Result<Vec<AssetRow>>;
    async fn asset(&self, issuer: &str, name: &str) -> Result<Option<AssetRow>>;
    /// Filtered transaction collection ordered by `(tick DESC, tx_index ASC)`.
    async fn transactions_page(
        &self,
        filter: &TransactionFilter,
        limit: u32,
        offset: u64,
    ) -> Result<Vec<TransactionRow>>;
    /// Filtered event collection ordered by `(tick DESC, log_id DESC)`.
    async fn events_page(
        &self,
        filter: &EventFilter,
        limit: u32,
        offset: u64,
    ) -> Result<Vec<EventRow>>;
    /// Event counts grouped by `type_name`, most frequent first.
    async fn event_type_counts(&self, limit: u32) -> Result<Vec<EventTypeCount>> {
        let _ = limit;
        Err(Error::Config(
            "event_type_counts is not supported by this store".into(),
        ))
    }
    /// Activity aggregated into `interval_seconds` buckets, newest first.
    ///
    /// A zero interval is treated as one second so callers cannot accidentally
    /// request an unbounded number of buckets.
    async fn activity_analytics(
        &self,
        interval_seconds: u32,
        limit: u32,
    ) -> Result<Vec<ActivityBucket>> {
        let _ = (interval_seconds, limit);
        Err(Error::Config(
            "activity_analytics is not supported by this store".into(),
        ))
    }
    async fn network_stats(&self) -> Result<NetworkStats>;

    /// Persist the elected computor set for `epoch`. Replaying the same epoch
    /// replaces its rows; later timestamps win. The default implementation
    /// reports the capability as unsupported.
    async fn write_computors(
        &self,
        epoch: u16,
        computors: &qubic::messages::Computors,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        let _ = (epoch, computors, timestamp);
        Err(Error::Config("computor storage unsupported".into()))
    }
    /// The elected computor set for `epoch`, ordered by `computor_index`.
    /// Unknown epochs return an empty list. The default implementation reports
    /// the capability as unsupported.
    async fn computors(&self, epoch: u16) -> Result<Vec<ComputorRow>> {
        let _ = epoch;
        Err(Error::Config("computor storage unsupported".into()))
    }
    /// Quorum evidence for a stored tick. `None` when the tick has no evidence
    /// or is not indexed. The default implementation reports the capability as
    /// unsupported.
    async fn evidence_for_tick(&self, tick: u32) -> Result<Option<QuorumEvidenceRow>> {
        let _ = tick;
        Err(Error::Config("evidence storage unsupported".into()))
    }

    /// Upsert price points keyed by `hour`. Later versions win, and duplicate
    /// hours inside one batch collapse to the highest version. Writes are
    /// idempotent, so replaying a batch is safe.
    async fn write_price_points(&self, points: &[PricePointRow]) -> Result<()> {
        let _ = points;
        Err(Error::Config("price storage unsupported".into()))
    }
    /// Distinct price points with `hour >= from_hour`, oldest hour first.
    /// `limit` is clamped to `1..=1000`.
    async fn price_points(&self, from_hour: u32, limit: u32) -> Result<Vec<PricePointRow>> {
        let _ = (from_hour, limit);
        Err(Error::Config("price storage unsupported".into()))
    }
    /// Highest stored hour after per-hour version resolution.
    async fn latest_price(&self) -> Result<Option<PricePointRow>> {
        Err(Error::Config("price storage unsupported".into()))
    }
    /// Latest point plus series metadata. `None` when no points are stored.
    async fn price_snapshot(&self) -> Result<Option<PriceSnapshot>> {
        Err(Error::Config("price storage unsupported".into()))
    }

    /// Persist a fully verified tick and advance the head cursor.
    async fn write_tick(&self, batch: &TickBatch) -> Result<WriteOutcome>;
    /// Persist ordered ticks with one insert per table and one cursor advance.
    ///
    /// The default implementation calls [`Store::write_tick`] for each batch.
    /// Stores that can batch should override it; backfill throughput depends on
    /// it because per-tick inserts are the dominant cost.
    async fn write_ticks(&self, batches: &[TickBatch]) -> Result<Vec<WriteOutcome>> {
        let mut outcomes = Vec::with_capacity(batches.len());
        for batch in batches {
            outcomes.push(self.write_tick(batch).await?);
        }
        Ok(outcomes)
    }
    /// Read an ingest cursor. Values are opaque strings owned by the caller.
    async fn cursor(&self, key: &str) -> Result<Option<String>>;
    async fn set_cursor(&self, key: &str, value: &str) -> Result<()>;
    /// Typed getter for the last indexed tick, epoch, and verification level.
    async fn ingest_cursor(&self) -> Result<IngestCursor>;
    async fn set_ingest_cursor(&self, cursor: &IngestCursor) -> Result<()>;
}

pub struct ClickHouseStore {
    pub client: clickhouse::Client,
    bootstrap: clickhouse::Client,
    database: String,
}

impl ClickHouseStore {
    pub fn new(url: &str, database: &str) -> Result<Self> {
        if url.is_empty() {
            return Err(Error::Config("clickhouse url is empty".into()));
        }
        schema::validate_database(database).map_err(Error::Config)?;
        let (url, user, password) = split_url_credentials(url)?;
        // ClickHouse 26 answers `compress=1` with ZSTD while clickhouse-rs 0.13
        // can only decode LZ4. Leave the wire uncompressed until the client
        // supports ZSTD responses.
        let mut client = clickhouse::Client::default()
            .with_url(&url)
            .with_database(database)
            .with_compression(clickhouse::Compression::None);
        if let Some(user) = user.clone() {
            client = client.with_user(user);
        }
        if let Some(password) = password.clone() {
            client = client.with_password(password);
        }
        // DDL that creates the database cannot be scoped to a database that
        // does not exist yet.
        let mut bootstrap = clickhouse::Client::default()
            .with_url(&url)
            .with_compression(clickhouse::Compression::None);
        if let Some(user) = user {
            bootstrap = bootstrap.with_user(user);
        }
        if let Some(password) = password {
            bootstrap = bootstrap.with_password(password);
        }
        Ok(Self {
            client,
            bootstrap,
            database: database.to_owned(),
        })
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    async fn count(&self, query: &str) -> Result<u64> {
        let row = self
            .client
            .query(query)
            .fetch_one::<rows::CountRow>()
            .await?;
        Ok(row.n)
    }

    /// Derive a `ReplacingMergeTree` version from the observation timestamp.
    /// Microseconds keep two writes in the same millisecond ordered while the
    /// value stays small enough for `UInt64`.
    fn timestamp_version(timestamp: DateTime<Utc>) -> u64 {
        u64::try_from(timestamp.timestamp_micros()).unwrap_or(0)
    }
}

/// Splits `http://user:password@host:port` into the bare URL and credentials.
///
/// Percent-encoded userinfo is decoded, so a password can contain `:`, `@`, or
/// `/` when it is encoded. A URL without userinfo is returned unchanged.
fn split_url_credentials(url: &str) -> Result<(String, Option<String>, Option<String>)> {
    let Some(scheme_end) = url.find("://") else {
        return Err(Error::Config(
            "clickhouse url must start with http:// or https://".into(),
        ));
    };
    let authority_start = scheme_end + 3;
    let Some(at) = url[authority_start..].rfind('@') else {
        return Ok((url.to_owned(), None, None));
    };
    let at = authority_start + at;
    let userinfo = &url[authority_start..at];
    let (user, password) = match userinfo.split_once(':') {
        Some((user, password)) => (user, Some(password)),
        None => (userinfo, None),
    };
    if user.is_empty() {
        return Err(Error::Config(
            "clickhouse url contains credentials without a user name".into(),
        ));
    }
    let user = percent_decode(user)?;
    let password = password.map(percent_decode).transpose()?;
    let host = &url[at + 1..];
    if host.is_empty() {
        return Err(Error::Config(
            "clickhouse url contains credentials but no host".into(),
        ));
    }
    let base = format!("{}://{}", &url[..scheme_end], host);
    Ok((base, Some(user), password))
}

fn percent_decode(value: &str) -> Result<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hex = bytes.get(index + 1..index + 3).ok_or_else(|| {
                Error::Config("clickhouse credentials contain a truncated percent escape".into())
            })?;
            let hex = std::str::from_utf8(hex).map_err(|_| {
                Error::Config("clickhouse credentials contain an invalid percent escape".into())
            })?;
            let byte = u8::from_str_radix(hex, 16).map_err(|_| {
                Error::Config("clickhouse credentials contain an invalid percent escape".into())
            })?;
            decoded.push(byte);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded)
        .map_err(|_| Error::Config("clickhouse credentials are not valid UTF-8".into()))
}

#[async_trait]
impl Store for ClickHouseStore {
    async fn init(&self) -> Result<()> {
        self.bootstrap
            .query(&schema::create_database(&self.database))
            .execute()
            .await?;
        // Early deployments stored the BTC/USDT mining series in
        // `price_points`. Keep that history under a distinct table and create a
        // fresh table for QUBIC/USDT instead of serving BTC numbers as QUBIC.
        let legacy_table = format!("EXISTS TABLE {}.price_points", self.database);
        let backup_table = format!("EXISTS TABLE {}.btc_price_points", self.database);
        let has_legacy = self
            .client
            .query(&legacy_table)
            .fetch_one::<u8>()
            .await
            .unwrap_or(0);
        let has_backup = self
            .client
            .query(&backup_table)
            .fetch_one::<u8>()
            .await
            .unwrap_or(0);
        if has_legacy != 0 && has_backup == 0 {
            self.client
                .query(&format!(
                    "RENAME TABLE {}.price_points TO {}.btc_price_points",
                    self.database, self.database
                ))
                .execute()
                .await?;
            tracing::warn!(
                database = %self.database,
                "moved legacy BTC price series to btc_price_points; starting a fresh QUBIC/USDT series"
            );
        }
        let statements = schema::statements(&self.database).map_err(Error::Config)?;
        for statement in statements {
            self.client.query(&statement).execute().await?;
        }
        Ok(())
    }

    async fn drop_database(&self) -> Result<()> {
        self.bootstrap
            .query(&schema::drop_database(&self.database))
            .execute()
            .await?;
        Ok(())
    }

    async fn head(&self) -> Result<Option<TickRow>> {
        let row = self
            .client
            .query("SELECT ?fields FROM ticks ORDER BY epoch DESC, tick DESC, version DESC LIMIT 1")
            .fetch_optional::<rows::TickSelect>()
            .await?;
        Ok(row.map(Into::into))
    }

    async fn tick(&self, tick: u32) -> Result<Option<TickRow>> {
        let row = self
            .client
            .query("SELECT ?fields FROM ticks WHERE tick = ? ORDER BY version DESC LIMIT 1")
            .bind(tick)
            .fetch_optional::<rows::TickSelect>()
            .await?;
        Ok(row.map(Into::into))
    }

    async fn transactions_for_tick(
        &self,
        tick: u32,
        limit: u32,
        offset: u64,
    ) -> Result<Vec<TransactionRow>> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM (\
                     SELECT * FROM transactions WHERE tick = ? \
                     ORDER BY version DESC LIMIT 1 BY tick, tx_index\
                 ) ORDER BY tx_index LIMIT ? OFFSET ?",
            )
            .bind(tick)
            .bind(limit)
            .bind(offset)
            .fetch_all::<rows::TransactionSelect>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn transaction(&self, hash: &str) -> Result<Option<TransactionRow>> {
        let row = self
            .client
            .query("SELECT ?fields FROM transactions WHERE hash = ? ORDER BY version DESC LIMIT 1")
            .bind(hash)
            .fetch_optional::<rows::TransactionSelect>()
            .await?;
        Ok(row.map(Into::into))
    }

    async fn transactions_by_hashes(&self, hashes: &[String]) -> Result<Vec<TransactionRow>> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self
            .client
            .query(
                r#"
SELECT ?fields FROM (
    SELECT * FROM transactions
    WHERE hash IN (?)
    ORDER BY version DESC
    LIMIT 1 BY hash
)
ORDER BY tick DESC, tx_index ASC
"#,
            )
            .bind(hashes)
            .fetch_all::<rows::TransactionSelect>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn transactions_for_identity(
        &self,
        identity: &str,
        limit: u32,
        offset: u64,
    ) -> Result<Vec<TransactionRow>> {
        // The two projections are used by the union branches. ClickHouse still
        // has to merge both branches and sort before applying the page, so deep
        // offsets remain proportional to the identity's history size.
        let rows = self
            .client
            .query(
                r#"
SELECT ?fields FROM (
    SELECT * FROM (
        SELECT * FROM transactions WHERE source = ?
        UNION ALL
        SELECT * FROM transactions WHERE destination = ?
    )
    ORDER BY version DESC
    LIMIT 1 BY tick, tx_index
)
ORDER BY tick DESC, tx_index ASC
LIMIT ? OFFSET ?
"#,
            )
            .bind(identity)
            .bind(identity)
            .bind(limit)
            .bind(offset)
            .fetch_all::<rows::TransactionSelect>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn recent_events(&self, limit: u32, offset: u64) -> Result<Vec<EventRow>> {
        // `by_recent` orders by (tick, log_id), so a reverse scan already
        // produces the requested order. LIMIT 1 BY collapses replayed rows
        // before the page is cut.
        let rows = self
            .client
            .query(
                r#"
SELECT ?fields FROM events
ORDER BY tick DESC, log_id DESC, version DESC
LIMIT 1 BY epoch, log_id
LIMIT ? OFFSET ?
"#,
            )
            .bind(limit)
            .bind(offset)
            .fetch_all::<rows::EventSelect>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn recent_transactions(&self, limit: u32, offset: u64) -> Result<Vec<TransactionRow>> {
        // The table order is (tick, tx_index), which the mixed DESC/ASC sort
        // cannot scan directly. ClickHouse uses it to bound the top-K sort;
        // deep offsets still read more of the table.
        let rows = self
            .client
            .query(
                r#"
SELECT ?fields FROM transactions
ORDER BY tick DESC, tx_index ASC, version DESC
LIMIT 1 BY tick, tx_index
LIMIT ? OFFSET ?
"#,
            )
            .bind(limit)
            .bind(offset)
            .fetch_all::<rows::TransactionSelect>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn events_for_tick(&self, tick: u32, limit: u32, offset: u64) -> Result<Vec<EventRow>> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM (\
                     SELECT * FROM events WHERE tick = ? \
                     ORDER BY version DESC LIMIT 1 BY epoch, log_id\
                 ) ORDER BY log_id LIMIT ? OFFSET ?",
            )
            .bind(tick)
            .bind(limit)
            .bind(offset)
            .fetch_all::<rows::EventSelect>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn events_for_identity(
        &self,
        identity: &str,
        limit: u32,
        offset: u64,
    ) -> Result<Vec<EventRow>> {
        // Both sides of the join read a version-deduped subquery instead of
        // scanning with FINAL. The participant primary key starts with
        // `identity`, so the left side is a bounded key read. An identity can
        // hold several roles on one log, hence the final `LIMIT 1 BY` on the
        // event key.
        let limit = limit.clamp(1, 500);
        let rows = self
            .client
            .query(
                r#"
SELECT ?fields FROM (
    SELECT e.epoch AS epoch,
           e.tick AS tick,
           e.log_id AS log_id,
           e.log_type AS log_type,
           e.type_name AS type_name,
           e.tx_hash AS tx_hash,
           e.tx_index AS tx_index,
           e.body AS body,
           e.log_digest AS log_digest,
           e.timestamp AS timestamp
    FROM (
        SELECT * FROM event_participants
        WHERE identity = ?
        ORDER BY version DESC
        LIMIT 1 BY epoch, log_id, identity, role
    ) AS p
    INNER JOIN (
        SELECT * FROM events
        ORDER BY version DESC
        LIMIT 1 BY epoch, log_id
    ) AS e ON e.epoch = p.epoch AND e.log_id = p.log_id
    ORDER BY e.tick DESC, e.log_id DESC, p.role DESC
    LIMIT 1 BY e.epoch, e.log_id
)
ORDER BY tick DESC, log_id DESC
LIMIT ? OFFSET ?
"#,
            )
            .bind(identity)
            .bind(limit)
            .bind(offset)
            .fetch_all::<rows::EventSelect>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn activity_for_identity(
        &self,
        identity: &str,
        limit: u32,
        offset: u64,
    ) -> Result<Vec<IdentityActivityRow>> {
        // One linear history over two sources: direct transfers and events the
        // identity participated in. Both branches are version-deduped before
        // the union so replayed ticks contribute once. The union columns have a
        // common type and order; `kind` tells callers which source produced the
        // row. For event rows the transfer-shaped fields are read from the
        // decoded JSON body and default to empty/zero for other event types.
        let limit = limit.clamp(1, 200);
        let rows = self
            .client
            .query(
                r#"
SELECT kind, epoch, tick, timestamp, tx_index, log_id, hash, source,
       destination, amount, input_type, input_data, status, type_name, body, role
FROM (
    SELECT
        'transaction' AS kind,
        t.epoch AS epoch,
        t.tick AS tick,
        t.timestamp AS timestamp,
        toNullable(toInt16(t.tx_index)) AS tx_index,
        CAST(NULL AS Nullable(UInt64)) AS log_id,
        t.hash AS hash,
        t.source AS source,
        t.destination AS destination,
        t.amount AS amount,
        t.input_type AS input_type,
        t.input_data AS input_data,
        t.status AS status,
        '' AS type_name,
        '' AS body,
        '' AS role
    FROM (
        SELECT * FROM transactions
        WHERE source = ? OR destination = ?
        ORDER BY version DESC
        LIMIT 1 BY tick, tx_index
    ) AS t
    UNION ALL
    SELECT
        'event' AS kind,
        e.epoch AS epoch,
        e.tick AS tick,
        e.timestamp AS timestamp,
        toNullable(e.tx_index) AS tx_index,
        toNullable(e.log_id) AS log_id,
        e.tx_hash AS hash,
        JSONExtractString(e.body, 'from') AS source,
        JSONExtractString(e.body, 'to') AS destination,
        toInt64OrZero(JSONExtractString(e.body, 'amount')) AS amount,
        toUInt16(0) AS input_type,
        '' AS input_data,
        '' AS status,
        e.type_name AS type_name,
        e.body AS body,
        p.role AS role
    FROM (
        SELECT * FROM event_participants
        WHERE identity = ?
        ORDER BY version DESC
        LIMIT 1 BY epoch, log_id, identity, role
    ) AS p
    INNER JOIN (
        SELECT * FROM events
        ORDER BY version DESC
        LIMIT 1 BY epoch, log_id
    ) AS e ON e.epoch = p.epoch AND e.log_id = p.log_id
)
ORDER BY tick DESC, log_id DESC NULLS LAST, tx_index DESC, hash DESC
LIMIT ? OFFSET ?
"#,
            )
            .bind(identity)
            .bind(identity)
            .bind(identity)
            .bind(limit)
            .bind(offset)
            .fetch_all::<rows::IdentityActivitySelect>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn write_event_participants(&self, participants: &[EventParticipantRow]) -> Result<()> {
        if participants.is_empty() {
            return Ok(());
        }
        // The natural batch key includes the role, so one event can contribute
        // several rows for the same identity. The table's sort key omits the
        // role, so a merge keeps the highest version per
        // `(identity, tick, log_id)`.
        let mut seen = HashSet::with_capacity(participants.len());
        let mut rows = Vec::with_capacity(participants.len());
        for participant in participants {
            if !seen.insert((
                participant.epoch,
                participant.log_id,
                participant.identity.as_str(),
                participant.role.as_str(),
            )) {
                continue;
            }
            rows.push(rows::EventParticipantInsert {
                epoch: participant.epoch,
                tick: participant.tick,
                log_id: participant.log_id,
                identity: participant.identity.clone(),
                role: participant.role.clone(),
                tx_hash: participant.tx_hash.clone(),
                tx_index: participant.tx_index,
                timestamp: participant.timestamp,
                // Participants carry no version of their own. The tick number
                // is the store-wide default and matches `TickBatch::new`.
                version: u64::from(participant.tick),
            });
        }
        let mut insert = self
            .client
            .insert::<rows::EventParticipantInsert>("event_participants")?;
        for row in &rows {
            insert.write(row).await?;
        }
        insert.end().await?;
        Ok(())
    }

    async fn identity(&self, identity: &str) -> Result<Option<IdentityRow>> {
        let row = self
            .client
            .query(
                "SELECT ?fields FROM identities WHERE identity = ? ORDER BY version DESC LIMIT 1",
            )
            .bind(identity)
            .fetch_optional::<rows::IdentitySelect>()
            .await?;
        Ok(row.map(Into::into))
    }

    async fn top_identities(&self, limit: u32) -> Result<Vec<IdentityRow>> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM (\
                     SELECT * FROM identities \
                     ORDER BY version DESC LIMIT 1 BY identity\
                 ) ORDER BY transfer_count DESC, balance DESC, identity ASC LIMIT ?",
            )
            .bind(limit)
            .fetch_all::<rows::IdentitySelect>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn stats(&self) -> Result<Value> {
        let head = self.head().await?;
        // uniqExact on the sorting keys is exact and still avoids FINAL, so
        // replayed ticks are not double counted before merges run.
        let ticks = self
            .count("SELECT uniqExact(epoch, tick) AS n FROM ticks")
            .await?;
        let transactions = self
            .count("SELECT uniqExact(tick, tx_index) AS n FROM transactions")
            .await?;
        let events = self
            .count("SELECT uniqExact(epoch, log_id) AS n FROM events")
            .await?;
        let identities = self
            .count("SELECT uniqExact(identity) AS n FROM identities")
            .await?;
        let cursor = match self.cursor(CURSOR_HEAD).await? {
            Some(value) => serde_json::from_str(&value).unwrap_or_else(|_| json!(value)),
            None => Value::Null,
        };
        let ingest = self.ingest_cursor().await?;
        Ok(json!({
            "head": head,
            "counts": {
                "ticks": ticks,
                "transactions": transactions,
                "events": events,
                "identities": identities,
            },
            "cursor": cursor,
            "ingest": ingest,
        }))
    }

    async fn ticks(&self, epoch: Option<u16>, limit: u32, offset: u64) -> Result<Vec<TickRow>> {
        let rows = match epoch {
            Some(epoch) => {
                self.client
                    .query(
                        "SELECT ?fields FROM (\
                             SELECT * FROM ticks WHERE epoch = ? \
                             ORDER BY version DESC LIMIT 1 BY epoch, tick\
                         ) ORDER BY tick DESC LIMIT ? OFFSET ?",
                    )
                    .bind(epoch)
                    .bind(limit)
                    .bind(offset)
                    .fetch_all::<rows::TickSelect>()
                    .await?
            }
            None => {
                self.client
                    .query(
                        "SELECT ?fields FROM (\
                             SELECT * FROM ticks \
                             ORDER BY version DESC LIMIT 1 BY epoch, tick\
                         ) ORDER BY tick DESC LIMIT ? OFFSET ?",
                    )
                    .bind(limit)
                    .bind(offset)
                    .fetch_all::<rows::TickSelect>()
                    .await?
            }
        };
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn ticks_after(&self, tick: u32, limit: u32) -> Result<Vec<TickRow>> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM (\
                     SELECT * FROM ticks WHERE tick > ? \
                     ORDER BY version DESC LIMIT 1 BY epoch, tick\
                 ) ORDER BY tick ASC, epoch ASC LIMIT ?",
            )
            .bind(tick)
            .bind(limit)
            .fetch_all::<rows::TickSelect>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn epochs(&self, limit: u32, offset: u64) -> Result<Vec<EpochRow>> {
        let head_epoch = self.head().await?.map(|row| row.epoch);
        let rows = self
            .client
            .query(
                "SELECT epoch, min(tick) AS first_tick, max(tick) AS last_tick, \
                     min(timestamp) AS first_timestamp, max(timestamp) AS last_timestamp, \
                     count() AS tick_count, sum(transaction_count) AS transaction_count, \
                     sum(event_count) AS event_count \
                 FROM (\
                     SELECT * FROM ticks \
                     ORDER BY version DESC LIMIT 1 BY epoch, tick\
                 ) GROUP BY epoch ORDER BY epoch DESC LIMIT ? OFFSET ?",
            )
            .bind(limit)
            .bind(offset)
            .fetch_all::<rows::EpochSelect>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| row.into_row(head_epoch))
            .collect())
    }

    async fn epoch(&self, epoch: u16) -> Result<Option<EpochRow>> {
        let head_epoch = self.head().await?.map(|row| row.epoch);
        let row = self
            .client
            .query(
                "SELECT epoch, min(tick) AS first_tick, max(tick) AS last_tick, \
                     min(timestamp) AS first_timestamp, max(timestamp) AS last_timestamp, \
                     count() AS tick_count, sum(transaction_count) AS transaction_count, \
                     sum(event_count) AS event_count \
                 FROM (\
                     SELECT * FROM ticks WHERE epoch = ? \
                     ORDER BY version DESC LIMIT 1 BY epoch, tick\
                 ) GROUP BY epoch LIMIT 1",
            )
            .bind(epoch)
            .fetch_optional::<rows::EpochSelect>()
            .await?;
        Ok(row.map(|row| row.into_row(head_epoch)))
    }

    async fn identities(&self, limit: u32, offset: u64) -> Result<Vec<IdentityRow>> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM (\
                     SELECT * FROM identities \
                     ORDER BY version DESC LIMIT 1 BY identity\
                 ) ORDER BY last_seen_tick DESC, identity ASC LIMIT ? OFFSET ?",
            )
            .bind(limit)
            .bind(offset)
            .fetch_all::<rows::IdentitySelect>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn assets(&self, issuer: Option<&str>, limit: u32, offset: u64) -> Result<Vec<AssetRow>> {
        const ASSET_SELECT: &str = "\
            SELECT issuer, name, decimals, unit, managing_contract, shares, \
                   issued_tick, log_id, timestamp \
            FROM (\
                SELECT JSONExtractString(body, 'issuer') AS issuer, \
                       JSONExtractString(body, 'name') AS name, \
                       toUInt8(JSONExtractUInt(body, 'decimals')) AS decimals, \
                       JSONExtractString(body, 'unit') AS unit, \
                       JSONExtractInt(body, 'managingContract') AS managing_contract, \
                       toInt64OrZero(JSONExtractString(body, 'shares')) AS shares, \
                       tick AS issued_tick, \
                       log_id AS log_id, \
                       timestamp AS timestamp \
                FROM (\
                    SELECT * FROM events WHERE type_name = 'asset_issuance' \
                    ORDER BY version DESC LIMIT 1 BY epoch, log_id\
                )\
            ) WHERE issuer != ''";
        let rows = match issuer {
            Some(issuer) => {
                let sql = format!(
                    "{ASSET_SELECT} AND issuer = ? \
                     ORDER BY issuer ASC, name ASC, issued_tick DESC, log_id DESC \
                     LIMIT 1 BY issuer, name LIMIT ? OFFSET ?"
                );
                self.client
                    .query(&sql)
                    .bind(issuer)
                    .bind(limit)
                    .bind(offset)
                    .fetch_all::<rows::AssetSelect>()
                    .await?
            }
            None => {
                let sql = format!(
                    "{ASSET_SELECT} \
                     ORDER BY issuer ASC, name ASC, issued_tick DESC, log_id DESC \
                     LIMIT 1 BY issuer, name LIMIT ? OFFSET ?"
                );
                self.client
                    .query(&sql)
                    .bind(limit)
                    .bind(offset)
                    .fetch_all::<rows::AssetSelect>()
                    .await?
            }
        };
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn asset(&self, issuer: &str, name: &str) -> Result<Option<AssetRow>> {
        let sql = "\
            SELECT issuer, name, decimals, unit, managing_contract, shares, \
                   issued_tick, log_id, timestamp \
            FROM (\
                SELECT JSONExtractString(body, 'issuer') AS issuer, \
                       JSONExtractString(body, 'name') AS name, \
                       toUInt8(JSONExtractUInt(body, 'decimals')) AS decimals, \
                       JSONExtractString(body, 'unit') AS unit, \
                       JSONExtractInt(body, 'managingContract') AS managing_contract, \
                       toInt64OrZero(JSONExtractString(body, 'shares')) AS shares, \
                       tick AS issued_tick, \
                       log_id AS log_id, \
                       timestamp AS timestamp \
                FROM (\
                    SELECT * FROM events WHERE type_name = 'asset_issuance' \
                    ORDER BY version DESC LIMIT 1 BY epoch, log_id\
                )\
            ) WHERE issuer = ? AND name = ? \
            ORDER BY issued_tick DESC, log_id DESC \
            LIMIT 1 BY issuer, name LIMIT 1";
        let row = self
            .client
            .query(sql)
            .bind(issuer)
            .bind(name)
            .fetch_optional::<rows::AssetSelect>()
            .await?;
        Ok(row.map(Into::into))
    }

    async fn transactions_page(
        &self,
        filter: &TransactionFilter,
        limit: u32,
        offset: u64,
    ) -> Result<Vec<TransactionRow>> {
        let mut sql = String::from("SELECT ?fields FROM (SELECT * FROM transactions");
        let mut conditions = Vec::new();
        if filter.epoch.is_some() {
            conditions.push("epoch = ?");
        }
        if filter.tick.is_some() {
            conditions.push("tick = ?");
        }
        if filter.identity.is_some() {
            conditions.push("(source = ? OR destination = ?)");
        }
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }
        sql.push_str(
            " ORDER BY version DESC LIMIT 1 BY tick, tx_index\
             ) ORDER BY tick DESC, tx_index ASC LIMIT ? OFFSET ?",
        );

        let mut query = self.client.query(&sql);
        if let Some(epoch) = filter.epoch {
            query = query.bind(epoch);
        }
        if let Some(tick) = filter.tick {
            query = query.bind(tick);
        }
        if let Some(identity) = &filter.identity {
            query = query.bind(identity.as_str()).bind(identity.as_str());
        }
        let rows = query
            .bind(limit)
            .bind(offset)
            .fetch_all::<rows::TransactionSelect>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn events_page(
        &self,
        filter: &EventFilter,
        limit: u32,
        offset: u64,
    ) -> Result<Vec<EventRow>> {
        let mut sql = String::from("SELECT ?fields FROM (SELECT * FROM events");
        let mut conditions = Vec::new();
        if filter.epoch.is_some() {
            conditions.push("epoch = ?");
        }
        if filter.tick.is_some() {
            conditions.push("tick = ?");
        }
        if filter.type_name.is_some() {
            conditions.push("type_name = ?");
        }
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }
        sql.push_str(
            " ORDER BY version DESC LIMIT 1 BY epoch, log_id\
             ) ORDER BY tick DESC, log_id DESC LIMIT ? OFFSET ?",
        );

        let mut query = self.client.query(&sql);
        if let Some(epoch) = filter.epoch {
            query = query.bind(epoch);
        }
        if let Some(tick) = filter.tick {
            query = query.bind(tick);
        }
        if let Some(type_name) = &filter.type_name {
            query = query.bind(type_name.as_str());
        }
        let rows = query
            .bind(limit)
            .bind(offset)
            .fetch_all::<rows::EventSelect>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn event_type_counts(&self, limit: u32) -> Result<Vec<EventTypeCount>> {
        let rows = self
            .client
            .query(
                "SELECT type_name, count() AS n FROM (\
                     SELECT type_name FROM events \
                     ORDER BY version DESC LIMIT 1 BY epoch, log_id\
                 ) GROUP BY type_name ORDER BY n DESC, type_name ASC LIMIT ?",
            )
            .bind(limit)
            .fetch_all::<rows::EventTypeCountSelect>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn activity_analytics(
        &self,
        interval_seconds: u32,
        limit: u32,
    ) -> Result<Vec<ActivityBucket>> {
        // The three branches read one deduplicated projection of each table so
        // a replayed tick contributes exactly once. The interval is bound as a
        // value (not interpolated) and each count is widened to UInt64 so the
        // union has one type per column.
        let interval = i64::from(interval_seconds.max(1));
        let rows = self
            .client
            .query(
                r#"
SELECT bucket,
       sum(ticks) AS ticks,
       sum(transactions) AS transactions,
       sum(events) AS events,
       sum(volume) AS volume
FROM (
    SELECT toStartOfInterval(timestamp, toIntervalSecond(?)) AS bucket,
           count() AS ticks,
           toUInt64(0) AS transactions,
           toUInt64(0) AS events,
           toInt64(0) AS volume
    FROM (
        SELECT timestamp FROM ticks
        ORDER BY version DESC LIMIT 1 BY epoch, tick
    )
    GROUP BY bucket
    UNION ALL
    SELECT toStartOfInterval(timestamp, toIntervalSecond(?)) AS bucket,
           toUInt64(0) AS ticks,
           count() AS transactions,
           toUInt64(0) AS events,
           sum(amount) AS volume
    FROM (
        SELECT timestamp, amount FROM transactions
        ORDER BY version DESC LIMIT 1 BY tick, tx_index
    )
    GROUP BY bucket
    UNION ALL
    SELECT toStartOfInterval(timestamp, toIntervalSecond(?)) AS bucket,
           toUInt64(0) AS ticks,
           toUInt64(0) AS transactions,
           count() AS events,
           toInt64(0) AS volume
    FROM (
        SELECT timestamp FROM events
        ORDER BY version DESC LIMIT 1 BY epoch, log_id
    )
    GROUP BY bucket
)
GROUP BY bucket
ORDER BY bucket DESC
LIMIT ?
"#,
            )
            .bind(interval)
            .bind(interval)
            .bind(interval)
            .bind(limit)
            .fetch_all::<rows::ActivityBucketSelect>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn network_stats(&self) -> Result<NetworkStats> {
        let head = self.head().await?;
        let epochs = self
            .count("SELECT uniqExact(epoch) AS n FROM ticks")
            .await?;
        let ticks = self
            .count("SELECT uniqExact(epoch, tick) AS n FROM ticks")
            .await?;
        let transactions = self
            .count("SELECT uniqExact(tick, tx_index) AS n FROM transactions")
            .await?;
        let events = self
            .count("SELECT uniqExact(epoch, log_id) AS n FROM events")
            .await?;
        let identities = self
            .count("SELECT uniqExact(identity) AS n FROM identities")
            .await?;
        let ingest = self.ingest_cursor().await?;
        Ok(NetworkStats {
            latest_timestamp: head.as_ref().map(|row| row.timestamp),
            head,
            epochs,
            ticks,
            transactions,
            events,
            identities,
            ingest,
        })
    }

    async fn write_computors(
        &self,
        epoch: u16,
        computors: &qubic::messages::Computors,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        if computors.epoch != epoch {
            return Err(Error::InvalidBatch(format!(
                "computor message is for epoch {}, not {epoch}",
                computors.epoch
            )));
        }
        if computors.public_keys.is_empty() {
            return Ok(());
        }
        let version = Self::timestamp_version(timestamp);
        let mut insert = self.client.insert::<rows::ComputorInsert>("computors")?;
        for (index, key) in computors.public_keys.iter().enumerate() {
            let index = u16::try_from(index)
                .map_err(|_| Error::InvalidBatch("computor index does not fit in UInt16".into()))?;
            let public_key = qubic::PublicKey::new(*key);
            insert
                .write(&rows::ComputorInsert {
                    epoch,
                    computor_index: index,
                    public_key: public_key.to_hex(),
                    identity: public_key.to_identity(),
                    version,
                    timestamp,
                })
                .await?;
        }
        insert.end().await?;
        Ok(())
    }

    async fn computors(&self, epoch: u16) -> Result<Vec<ComputorRow>> {
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM (\
                     SELECT * FROM computors WHERE epoch = ? \
                     ORDER BY version DESC LIMIT 1 BY epoch, computor_index\
                 ) ORDER BY computor_index ASC",
            )
            .bind(epoch)
            .fetch_all::<rows::ComputorSelect>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn evidence_for_tick(&self, tick: u32) -> Result<Option<QuorumEvidenceRow>> {
        let row = self
            .client
            .query(
                "SELECT ?fields FROM quorum_evidence WHERE tick = ? \
                 ORDER BY version DESC LIMIT 1",
            )
            .bind(tick)
            .fetch_optional::<rows::QuorumEvidenceSelect>()
            .await?;
        Ok(row.map(Into::into))
    }

    async fn write_price_points(&self, points: &[PricePointRow]) -> Result<()> {
        if points.is_empty() {
            return Ok(());
        }
        // Collapse duplicate hours inside the batch before writing. A higher
        // version always wins; equal versions keep the later occurrence so the
        // last item in the batch is authoritative.
        let mut by_hour: HashMap<u32, &PricePointRow> = HashMap::with_capacity(points.len());
        for point in points {
            let replaced = by_hour
                .get(&point.hour)
                .is_none_or(|existing| existing.version <= point.version);
            if replaced {
                by_hour.insert(point.hour, point);
            }
        }
        let mut rows: Vec<&PricePointRow> = by_hour.into_values().collect();
        rows.sort_by_key(|point| point.hour);

        let mut insert = self
            .client
            .insert::<rows::PricePointInsert>("price_points")?;
        for point in rows {
            insert
                .write(&rows::PricePointInsert {
                    hour: point.hour,
                    append_tick: point.append_tick,
                    source_tick: point.source_tick,
                    epoch: point.epoch,
                    flags: point.flags,
                    price_e8: point.price_e8,
                    source: point.source,
                    synced: point.synced,
                    bound: point.bound,
                    series_digest: point.series_digest,
                    observed_at: point.observed_at,
                    version: point.version,
                })
                .await?;
        }
        insert.end().await?;
        Ok(())
    }

    async fn price_points(&self, from_hour: u32, limit: u32) -> Result<Vec<PricePointRow>> {
        let limit = limit.clamp(1, 1000);
        let rows = self
            .client
            .query(
                "SELECT ?fields FROM (\
                     SELECT * FROM price_points WHERE hour >= ? \
                     ORDER BY version DESC LIMIT 1 BY hour\
                 ) ORDER BY hour ASC LIMIT ?",
            )
            .bind(from_hour)
            .bind(limit)
            .fetch_all::<rows::PricePointSelect>()
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn latest_price(&self) -> Result<Option<PricePointRow>> {
        let row = self
            .client
            .query(
                "SELECT ?fields FROM (\
                     SELECT * FROM price_points \
                     ORDER BY version DESC LIMIT 1 BY hour\
                 ) ORDER BY hour DESC LIMIT 1",
            )
            .fetch_optional::<rows::PricePointSelect>()
            .await?;
        Ok(row.map(Into::into))
    }

    async fn price_snapshot(&self) -> Result<Option<PriceSnapshot>> {
        let Some(latest) = self.latest_price().await? else {
            return Ok(None);
        };
        let points = self
            .count("SELECT uniqExact(hour) AS n FROM price_points")
            .await?;
        Ok(Some(PriceSnapshot {
            synced: latest.synced != 0,
            bound: latest.bound != 0,
            source: latest.source,
            series_digest: latest.series_digest,
            epoch: latest.epoch,
            append_tick: latest.append_tick,
            source_tick: latest.source_tick,
            observed_at: latest.observed_at,
            latest: Some(latest),
            points,
        }))
    }

    async fn write_tick(&self, batch: &TickBatch) -> Result<WriteOutcome> {
        writer::write_tick(self, batch).await
    }

    async fn write_ticks(&self, batches: &[TickBatch]) -> Result<Vec<WriteOutcome>> {
        writer::write_ticks(self, batches).await
    }

    async fn cursor(&self, key: &str) -> Result<Option<String>> {
        let row = self
            .client
            .query("SELECT value FROM ingest_state WHERE key = ? ORDER BY version DESC LIMIT 1")
            .bind(key)
            .fetch_optional::<rows::CursorSelect>()
            .await?;
        Ok(row.map(|row| row.value))
    }

    async fn set_cursor(&self, key: &str, value: &str) -> Result<()> {
        rows::insert_cursor(&self.client, key, value, Utc::now()).await
    }

    async fn ingest_cursor(&self) -> Result<IngestCursor> {
        match self.cursor(CURSOR_INGEST).await? {
            Some(value) => serde_json::from_str(&value)
                .map_err(|err| Error::Config(format!("invalid ingest cursor: {err}"))),
            None => Ok(IngestCursor::default()),
        }
    }

    async fn set_ingest_cursor(&self, cursor: &IngestCursor) -> Result<()> {
        let mut cursor = cursor.clone();
        if cursor.updated_at.is_none() {
            cursor.updated_at = Some(Utc::now());
        }
        let value = serde_json::to_string(&cursor)
            .map_err(|err| Error::Config(format!("serialize ingest cursor: {err}")))?;
        self.set_cursor(CURSOR_INGEST, &value).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_url_and_unsafe_database() {
        assert!(ClickHouseStore::new("", "glyph").is_err());
        assert!(ClickHouseStore::new("http://127.0.0.1:8123", "glyph; DROP").is_err());
        assert!(ClickHouseStore::new("http://127.0.0.1:8123", "glyph").is_ok());
    }

    #[test]
    fn version_tracks_tick() {
        let batch = TickBatch::new(1, 42, Utc::now(), 3, "quorum");
        assert_eq!(batch.version, 42);
    }

    #[test]
    fn splits_clickhouse_url_credentials() {
        let (url, user, password) =
            split_url_credentials("http://glyph:s3cret@127.0.0.1:8123").unwrap();
        assert_eq!(url, "http://127.0.0.1:8123");
        assert_eq!(user.as_deref(), Some("glyph"));
        assert_eq!(password.as_deref(), Some("s3cret"));

        let (url, user, password) =
            split_url_credentials("https://user:p%40ss%3Aword@db.example:8443").unwrap();
        assert_eq!(url, "https://db.example:8443");
        assert_eq!(user.as_deref(), Some("user"));
        assert_eq!(password.as_deref(), Some("p@ss:word"));

        let (url, user, password) = split_url_credentials("http://127.0.0.1:8123").unwrap();
        assert_eq!(url, "http://127.0.0.1:8123");
        assert_eq!(user, None);
        assert_eq!(password, None);
    }
}
